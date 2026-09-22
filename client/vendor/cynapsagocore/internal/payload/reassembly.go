package payload

import (
	"bytes"
	"context"
	"sync"
	"time"
)

type reassemblyEntry struct {
	manifest      TransferManifest
	binding       TransferBinding
	carriers      map[CarrierKind]struct{}
	data          []byte
	canonical     []byte
	received      []bool
	receivedCount uint32
	completing    bool
	completingVia CarrierKind
	completed     bool
	aborted       bool
	expired       bool
	cancel        context.CancelFunc
	objectRunning bool
	objectCancel  context.CancelFunc
	objectDone    chan struct{}
	objectAttempt uint64
	objectAborted bool
	completingObj uint64
	chargedBytes  int64
}

// transferWait is a bounded, authenticated rendezvous for the case where the
// logical envelope is delivered before its private carrier. The channel is
// replaced on every state transition so waiters never poll.
type transferWait struct {
	binding    TransferBinding
	changed    chan struct{}
	waiters    int
	terminal   error
	expiresAt  time.Time
	terminalAt uint64
}

// Reassembler owns exact reserved byte buffers for bounded incomplete transfers.
type Reassembler struct {
	mu            sync.Mutex
	limits        Limits
	now           func() time.Time
	cipher        PayloadCipher
	lifecycle     context.Context
	cancelLife    context.CancelFunc
	entries       map[string]*reassemblyEntry
	waits         map[string]*transferWait
	terminalClock uint64
	peerTransfers map[string]int
	peerBytes     map[string]int64
	revokedPeers  map[string]struct{}
	totalBytes    int64
	closed        bool
	active        sync.WaitGroup
}

func NewReassembler(limits Limits, cipher PayloadCipher, now func() time.Time) (*Reassembler, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	lifecycle, cancelLife := context.WithCancel(context.Background())
	return &Reassembler{limits: limits, cipher: cipher, now: now, lifecycle: lifecycle, cancelLife: cancelLife, entries: make(map[string]*reassemblyEntry), waits: make(map[string]*transferWait), peerTransfers: make(map[string]int), peerBytes: make(map[string]int64), revokedPeers: make(map[string]struct{})}, nil
}

func reassemblyPeerKey(meshID, peerID string) string { return meshID + "\x00" + peerID }

func (r *Reassembler) peerRevokedLocked(binding TransferBinding) bool {
	_, sender := r.revokedPeers[reassemblyPeerKey(binding.MeshID, binding.SenderID)]
	_, recipient := r.revokedPeers[reassemblyPeerKey(binding.MeshID, binding.RecipientID)]
	return sender || recipient
}

// Begin authenticates a manifest against trusted context before reserving exact quota.
func (r *Reassembler) Begin(manifest TransferManifest, binding TransferBinding, carrier CarrierKind) error {
	if r == nil {
		return ErrInvalidLimits
	}
	if carrier != CarrierDirectChunks && carrier != CarrierObjectUpload && carrier != CarrierMessageChunks {
		return ErrInvalidManifest
	}
	now := r.now().UTC()
	if err := ValidateTransferManifest(manifest, binding, r.limits, now); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrInvalidHandleState
	}
	if r.peerRevokedLocked(binding) {
		return ErrAuthentication
	}
	r.expireWaitsLocked(now)
	if wait := r.waits[manifest.TransferID]; wait != nil {
		if wait.binding != binding {
			return ErrAuthentication
		}
		if wait.terminal != nil {
			return ErrTransferExists
		}
	}
	if existing, ok := r.entries[manifest.TransferID]; ok {
		if existing.expired {
			return ErrTransferExpired
		}
		if manifestsEqual(existing.manifest, manifest) && existing.binding == binding {
			existing.carriers[carrier] = struct{}{}
			if carrier != CarrierObjectUpload && !existing.completed && !existing.completing && existing.data == nil {
				existing.data = make([]byte, int(manifest.TransferredSize))
				existing.received = make([]bool, int(manifest.ChunkCount))
			}
			return nil
		}
		return ErrTransferExists
	}
	_, reservedByWait := r.waits[manifest.TransferID]
	if !reservedByWait && (r.logicalTransfersLocked() >= r.limits.MaximumTransfers || r.logicalPeerTransfersLocked(binding.SenderID) >= r.limits.TransfersPerPeer) {
		return ErrReassemblyQuota
	}
	if !r.canReserveLocked(binding.SenderID, manifest.TransferredSize) {
		return ErrReassemblyQuota
	}
	// The exact allocation occurs only after all authenticated bounds and quota checks.
	var data []byte
	var received []bool
	if carrier != CarrierObjectUpload {
		data = make([]byte, int(manifest.TransferredSize))
		received = make([]bool, int(manifest.ChunkCount))
	}
	r.entries[manifest.TransferID] = &reassemblyEntry{manifest: cloneManifest(manifest), binding: binding, carriers: map[CarrierKind]struct{}{carrier: {}}, data: data, received: received, chargedBytes: manifest.TransferredSize}
	r.peerTransfers[binding.SenderID]++
	r.reserveLocked(binding.SenderID, manifest.TransferredSize)
	r.notifyLocked(manifest.TransferID)
	return nil
}

// Accept validates and stores one chunk idempotently.
func (r *Reassembler) Accept(carrier CarrierKind, chunk TransferChunk) error {
	if r == nil {
		return ErrInvalidLimits
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[chunk.TransferID]
	if !ok {
		return ErrTransferNotFound
	}
	if _, authorized := entry.carriers[carrier]; !authorized || carrier == CarrierObjectUpload {
		return ErrAuthentication
	}
	if entry.completed {
		return ErrTransferCompleted
	}
	if entry.completing {
		return ErrInvalidHandleState
	}
	if !entry.manifest.ExpiresAt.After(r.now().UTC()) {
		r.terminalLocked(chunk.TransferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferExpired)
		r.removeLocked(chunk.TransferID)
		return ErrTransferExpired
	}
	if err := ValidateTransferChunk(entry.manifest, chunk); err != nil {
		return err
	}
	start := int(chunk.Offset)
	end := start + len(chunk.Bytes)
	if entry.received[chunk.Index] {
		if bytes.Equal(entry.data[start:end], chunk.Bytes) {
			return nil
		}
		return ErrChunkConflict
	}
	copy(entry.data[start:end], chunk.Bytes)
	entry.received[chunk.Index] = true
	entry.receivedCount++
	return nil
}

// Complete verifies transferred bytes, decrypts/authenticates if required,
// retains exactly one quota-charged canonical buffer, and returns metadata
// only. Canonical ownership transfers later through Consume or WaitConsume.
func (r *Reassembler) Complete(ctx context.Context, transferID string, carrier CarrierKind) (CompletionEvidence, error) {
	if r == nil || ctx == nil {
		return CompletionEvidence{}, ErrInvalidLimits
	}
	r.mu.Lock()
	entry, ok := r.entries[transferID]
	if !ok {
		r.mu.Unlock()
		return CompletionEvidence{}, ErrTransferNotFound
	}
	if entry.expired {
		r.mu.Unlock()
		return CompletionEvidence{}, ErrTransferExpired
	}
	if _, authorized := entry.carriers[carrier]; !authorized || carrier == CarrierObjectUpload {
		r.mu.Unlock()
		return CompletionEvidence{}, ErrAuthentication
	}
	if entry.completing {
		r.mu.Unlock()
		return CompletionEvidence{}, ErrInvalidHandleState
	}
	if entry.completed {
		evidence := completionEvidence(entry)
		r.mu.Unlock()
		return evidence, nil
	}
	if !entry.manifest.ExpiresAt.After(r.now().UTC()) {
		r.terminalLocked(transferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferExpired)
		r.removeLocked(transferID)
		r.mu.Unlock()
		return CompletionEvidence{}, ErrTransferExpired
	}
	if entry.receivedCount != entry.manifest.ChunkCount {
		r.mu.Unlock()
		return CompletionEvidence{}, ErrTransferIncomplete
	}
	manifest := cloneManifest(entry.manifest)
	binding := entry.binding
	entry.completing = true
	entry.completingVia = carrier
	workCtx, cancel := context.WithTimeout(ctx, manifest.ExpiresAt.Sub(r.now().UTC()))
	entry.cancel = cancel
	transferred := entry.data
	entry.data = nil
	r.active.Add(1)
	r.mu.Unlock()
	return r.finalize(ctx, workCtx, cancel, transferID, entry, manifest, binding, transferred)
}

// CompleteObject takes ownership of ciphertext downloaded for an authorized
// object attempt, authenticates it, and stores exactly one logical completion.
func (r *Reassembler) CompleteObject(ctx context.Context, transferID string, ciphertext []byte) error {
	if r == nil || ctx == nil {
		zero(ciphertext)
		return ErrInvalidLimits
	}
	r.mu.Lock()
	entry, ok := r.entries[transferID]
	if !ok {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrTransferNotFound
	}
	if entry.expired {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrTransferExpired
	}
	if entry.objectAborted {
		r.mu.Unlock()
		zero(ciphertext)
		return context.Canceled
	}
	if _, authorized := entry.carriers[CarrierObjectUpload]; !authorized {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrAuthentication
	}
	if entry.completed {
		r.mu.Unlock()
		zero(ciphertext)
		return nil
	}
	if !entry.objectRunning {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrInvalidHandleState
	}
	if entry.completing {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrInvalidHandleState
	}
	if !entry.manifest.ExpiresAt.After(r.now().UTC()) {
		r.terminalLocked(transferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferExpired)
		r.removeLocked(transferID)
		r.mu.Unlock()
		zero(ciphertext)
		return ErrTransferExpired
	}
	if int64(len(ciphertext)) != entry.manifest.TransferredSize {
		r.mu.Unlock()
		zero(ciphertext)
		return ErrIntegrity
	}
	manifest := cloneManifest(entry.manifest)
	binding := entry.binding
	entry.completing = true
	entry.completingVia = CarrierObjectUpload
	entry.completingObj = entry.objectAttempt
	zero(entry.data)
	entry.data = nil
	entry.received = nil
	workCtx, cancel := context.WithTimeout(ctx, manifest.ExpiresAt.Sub(r.now().UTC()))
	entry.cancel = cancel
	r.active.Add(1)
	r.mu.Unlock()
	_, err := r.finalize(ctx, workCtx, cancel, transferID, entry, manifest, binding, ciphertext)
	return err
}

func (r *Reassembler) finalize(_ context.Context, workCtx context.Context, cancel context.CancelFunc, transferID string, entry *reassemblyEntry, manifest TransferManifest, binding TransferBinding, transferred []byte) (CompletionEvidence, error) {
	succeeded := false
	defer func() {
		zero(manifest.CanonicalDigest)
		zero(manifest.TransferredDigest)
		cancel()
		zero(transferred)
		r.mu.Lock()
		if !succeeded {
			r.resetAfterFailureLocked(transferID, entry)
		}
		r.mu.Unlock()
		r.active.Done()
	}()

	if err := Verify(transferred, manifest.TransferredSize, manifest.TransferredDigest); err != nil {
		return CompletionEvidence{}, err
	}
	canonical := transferred
	if manifest.EncryptionRef != "" {
		if r.cipher == nil {
			return CompletionEvidence{}, ErrEncryption
		}
		// Reserve the complete canonical output before the cipher may allocate
		// it. The cipher contract permits one newly owned output and no retained
		// payload-sized workspace beyond this reservation.
		r.mu.Lock()
		current, exists := r.entries[transferID]
		if !exists || current != entry || r.closed || entry.aborted {
			r.mu.Unlock()
			return CompletionEvidence{}, context.Canceled
		}
		if !r.canReserveLocked(binding.SenderID, manifest.CanonicalSize) {
			r.mu.Unlock()
			return CompletionEvidence{}, ErrReassemblyQuota
		}
		r.reserveLocked(binding.SenderID, manifest.CanonicalSize)
		entry.chargedBytes += manifest.CanonicalSize
		r.mu.Unlock()
		var err error
		canonical, err = r.cipher.Decrypt(workCtx, binding, transferred, manifest.EncryptionRef)
		if err != nil {
			zero(canonical)
			if contextErr := dependencyContextError(workCtx, err); contextErr != nil {
				if r.entryExpired(transferID, entry) {
					return CompletionEvidence{}, ErrTransferExpired
				}
				return CompletionEvidence{}, contextErr
			}
			return CompletionEvidence{}, ErrAuthentication
		}
	}
	if err := Verify(canonical, manifest.CanonicalSize, manifest.CanonicalDigest); err != nil {
		if manifest.EncryptionRef != "" {
			zero(canonical)
		}
		return CompletionEvidence{}, err
	}
	if err := validateCanonicalProfileOnly(canonical, binding.Profile, r.limits.MaximumPayloadBytes); err != nil {
		if manifest.EncryptionRef != "" {
			zero(canonical)
		}
		return CompletionEvidence{}, err
	}
	r.mu.Lock()
	current, exists := r.entries[transferID]
	objectAttemptInvalid := entry.completingVia == CarrierObjectUpload && (entry.objectAborted || entry.completingObj == 0 || entry.completingObj != entry.objectAttempt || !carrierAuthorized(entry, CarrierObjectUpload))
	if !exists || current != entry || r.closed || entry.aborted || objectAttemptInvalid {
		r.mu.Unlock()
		if manifest.EncryptionRef != "" {
			zero(canonical)
		}
		if r.closed {
			return CompletionEvidence{}, context.Canceled
		}
		if entry.expired {
			return CompletionEvidence{}, ErrTransferExpired
		}
		if entry.aborted {
			return CompletionEvidence{}, context.Canceled
		}
		if objectAttemptInvalid {
			return CompletionEvidence{}, context.Canceled
		}
		return CompletionEvidence{}, ErrTransferNotFound
	}
	if manifest.EncryptionRef == "" {
		// The assembled plaintext becomes the retained canonical value. Remove
		// local ownership so the deferred cleanup cannot zero the moved buffer.
		entry.canonical = transferred
		transferred = nil
	} else {
		entry.canonical = canonical
		entry.chargedBytes -= manifest.TransferredSize
		r.releaseBytesLocked(binding.SenderID, manifest.TransferredSize)
	}
	entry.received = nil
	entry.completed = true
	entry.completing = false
	entry.completingVia = 0
	entry.completingObj = 0
	entry.cancel = nil
	r.notifyLocked(transferID)
	evidence := completionEvidence(entry)
	succeeded = true
	r.mu.Unlock()
	return evidence, nil
}

func (r *Reassembler) entryExpired(transferID string, entry *reassemblyEntry) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.entries[transferID]
	return ok && current == entry && entry.expired
}

// Consume returns and removes canonical bytes only when a matching trusted
// logical-envelope binding has arrived. Completed bytes otherwise remain
// quota-charged until consumption, expiry, abort, or shutdown.
func (r *Reassembler) Consume(transferID string, binding TransferBinding) ([]byte, error) {
	if r == nil {
		return nil, ErrInvalidLimits
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrInvalidHandleState
	}
	entry, ok := r.entries[transferID]
	if !ok {
		return nil, ErrTransferNotFound
	}
	if entry.expired {
		return nil, ErrTransferExpired
	}
	if entry.binding != binding {
		return nil, ErrAuthentication
	}
	if !entry.completed {
		return nil, ErrTransferIncomplete
	}
	// Detach the sole canonical buffer before removal. removeLocked therefore
	// releases quota but cannot zero bytes whose ownership has moved to caller.
	result := entry.canonical
	entry.canonical = nil
	r.terminalLocked(transferID, binding, entry.manifest.ExpiresAt, ErrTransferNotFound)
	r.removeLocked(transferID)
	return result, nil
}

// WaitConsume waits for authenticated transfer registration and completion,
// then gives exactly one waiter ownership of the retained canonical bytes.
// It is bounded by caller cancellation, the logical request expiry (when
// present), the transfer lifetime/manifest expiry, abort, and shutdown.
func (r *Reassembler) WaitConsume(ctx context.Context, transferID string, binding TransferBinding, requestExpiry time.Time) ([]byte, error) {
	if r == nil || ctx == nil || !validTransferID(transferID) {
		return nil, ErrInvalidLimits
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := r.now().UTC()
	bound := now.Add(r.limits.TransferLifetime)
	if !requestExpiry.IsZero() && requestExpiry.Before(bound) {
		bound = requestExpiry
	}
	if !bound.After(now) {
		return nil, ErrTransferExpired
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, context.Canceled
	}
	r.expireWaitsLocked(now)
	if r.peerRevokedLocked(binding) {
		r.mu.Unlock()
		return nil, ErrAuthentication
	}
	wait := r.waits[transferID]
	if wait == nil {
		entry := r.entries[transferID]
		if entry != nil && entry.binding != binding {
			r.mu.Unlock()
			return nil, ErrAuthentication
		}
		if entry == nil && (r.logicalTransfersLocked() >= r.limits.MaximumTransfers || r.logicalPeerTransfersLocked(binding.SenderID) >= r.limits.TransfersPerPeer) {
			r.mu.Unlock()
			return nil, ErrReassemblyQuota
		}
		wait = &transferWait{binding: binding, changed: make(chan struct{}), expiresAt: bound}
		r.waits[transferID] = wait
	} else {
		if wait.binding != binding {
			r.mu.Unlock()
			return nil, ErrAuthentication
		}
		if bound.Before(wait.expiresAt) {
			wait.expiresAt = bound
		}
	}
	wait.waiters++
	r.active.Add(1)
	r.mu.Unlock()
	defer func() {
		r.releaseWaiter(transferID, wait)
		r.active.Done()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, context.Canceled
		}
		current := r.waits[transferID]
		if current != wait {
			r.mu.Unlock()
			return nil, ErrTransferNotFound
		}
		if wait.terminal != nil {
			err := wait.terminal
			r.mu.Unlock()
			if contextErr := ctx.Err(); contextErr != nil {
				return nil, contextErr
			}
			return nil, err
		}
		entry := r.entries[transferID]
		if entry != nil {
			if entry.binding != binding {
				r.mu.Unlock()
				return nil, ErrAuthentication
			}
			if entry.manifest.ExpiresAt.Before(wait.expiresAt) {
				wait.expiresAt = entry.manifest.ExpiresAt
			}
			if entry.completed {
				if err := ctx.Err(); err != nil {
					r.mu.Unlock()
					return nil, err
				}
				// Completion wins cancellation once this mutex-protected ownership
				// transfer linearizes. No second full-payload allocation is made.
				result := entry.canonical
				entry.canonical = nil
				expiresAt := entry.manifest.ExpiresAt
				r.terminalLocked(transferID, binding, expiresAt, ErrTransferNotFound)
				r.removeLocked(transferID)
				r.mu.Unlock()
				return result, nil
			}
		}
		remaining := wait.expiresAt.Sub(r.now().UTC())
		if remaining <= 0 {
			r.setWaitTerminalLocked(wait, ErrTransferExpired)
			if entry != nil {
				r.removeLocked(transferID)
			}
			r.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, ErrTransferExpired
		}
		changed := wait.changed
		r.mu.Unlock()

		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			stopReassemblyTimer(timer)
			return nil, ctx.Err()
		case <-r.lifecycle.Done():
			stopReassemblyTimer(timer)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, context.Canceled
		case <-changed:
			stopReassemblyTimer(timer)
		case <-timer.C:
			r.mu.Lock()
			if current := r.waits[transferID]; current == wait && wait.terminal == nil {
				r.setWaitTerminalLocked(wait, ErrTransferExpired)
				if entry := r.entries[transferID]; entry != nil {
					r.removeLocked(transferID)
				}
			}
			r.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, ErrTransferExpired
		}
	}
}

func stopReassemblyTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// Completion reports authenticated retained completion without exposing bytes.
func (r *Reassembler) Completion(transferID string, binding TransferBinding) (CompletionEvidence, bool, error) {
	if r == nil {
		return CompletionEvidence{}, false, ErrInvalidLimits
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[transferID]
	if !ok {
		return CompletionEvidence{}, false, ErrTransferNotFound
	}
	if entry.expired {
		return CompletionEvidence{}, false, ErrTransferExpired
	}
	if entry.binding != binding {
		return CompletionEvidence{}, false, ErrAuthentication
	}
	if !entry.completed {
		return CompletionEvidence{}, false, nil
	}
	return completionEvidence(entry), true, nil
}

// beginObjectOperation registers one bounded object-ingress owner with the
// reassembler lifecycle. The returned release function must be called once.
func (r *Reassembler) beginObjectOperation(manifest TransferManifest, binding TransferBinding) (context.Context, func(), func(), error) {
	if r == nil {
		return nil, nil, nil, ErrInvalidLimits
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, nil, nil, ErrInvalidHandleState
	}
	entry, ok := r.entries[manifest.TransferID]
	if !ok || !manifestsEqual(entry.manifest, manifest) || entry.binding != binding {
		r.mu.Unlock()
		return nil, nil, nil, ErrAuthentication
	}
	if entry.expired {
		r.mu.Unlock()
		return nil, nil, nil, ErrTransferExpired
	}
	if _, authorized := entry.carriers[CarrierObjectUpload]; !authorized {
		r.mu.Unlock()
		return nil, nil, nil, ErrAuthentication
	}
	if entry.objectRunning {
		r.mu.Unlock()
		return nil, nil, nil, ErrInvalidHandleState
	}
	if entry.objectAttempt == ^uint64(0) {
		r.mu.Unlock()
		return nil, nil, nil, ErrInvalidHandleState
	}
	remaining := entry.manifest.ExpiresAt.Sub(r.now().UTC())
	if remaining <= 0 {
		r.terminalLocked(manifest.TransferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferExpired)
		r.removeLocked(manifest.TransferID)
		r.mu.Unlock()
		return nil, nil, nil, ErrTransferExpired
	}
	operation, cancel := context.WithTimeout(r.lifecycle, remaining)
	objectDone := make(chan struct{})
	entry.objectAttempt++
	entry.objectAborted = false
	entry.objectRunning = true
	entry.objectCancel = cancel
	entry.objectDone = objectDone
	r.active.Add(1)
	r.mu.Unlock()
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			r.mu.Lock()
			if current, exists := r.entries[manifest.TransferID]; exists && current == entry {
				if len(entry.carriers) == 0 && !entry.completed {
					r.removeLocked(manifest.TransferID)
				}
			}
			r.mu.Unlock()
		})
	}
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			cleanup()
			r.mu.Lock()
			if current, exists := r.entries[manifest.TransferID]; exists && current == entry {
				entry.objectRunning = false
				entry.objectCancel = nil
				entry.objectDone = nil
				if len(entry.carriers) == 0 && !entry.completed {
					r.removeLocked(manifest.TransferID)
				}
			}
			r.mu.Unlock()
			close(objectDone)
			r.active.Done()
		})
	}
	return operation, cleanup, finish, nil
}

// Abort releases one authorized carrier attempt idempotently without
// destroying another concurrent attempt or a retained logical completion.
func (r *Reassembler) Abort(transferID string, carrier CarrierKind) error {
	if r == nil {
		return ErrInvalidLimits
	}
	r.mu.Lock()
	entry, ok := r.entries[transferID]
	if !ok {
		if wait := r.waits[transferID]; wait != nil && wait.terminal == nil {
			r.setWaitTerminalLocked(wait, ErrTransferNotFound)
		}
		r.mu.Unlock()
		return nil
	}
	if carrier == CarrierObjectUpload && entry.objectRunning {
		delete(entry.carriers, carrier)
		entry.objectAborted = true
		cancel := entry.objectCancel
		done := entry.objectDone
		r.mu.Unlock()
		cancel()
		<-done
		r.mu.Lock()
		if current, exists := r.entries[transferID]; exists && current == entry && len(entry.carriers) == 0 && !entry.completed {
			r.removeLocked(transferID)
		}
		r.mu.Unlock()
		return nil
	}
	if _, authorized := entry.carriers[carrier]; !authorized {
		r.mu.Unlock()
		return nil
	}
	delete(entry.carriers, carrier)
	if entry.completing && entry.completingVia == carrier {
		entry.aborted = true
		if entry.cancel != nil {
			entry.cancel()
		}
		r.mu.Unlock()
		return nil
	}
	if len(entry.carriers) == 0 && !entry.completed {
		r.terminalLocked(transferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferNotFound)
		r.removeLocked(transferID)
	}
	r.mu.Unlock()
	return nil
}

// abortObjectFromOwner releases a failed object authorization without waiting
// on the owner that is making this call. External Abort remains the barrier.
func (r *Reassembler) abortObjectFromOwner(transferID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[transferID]
	if !ok {
		return
	}
	delete(entry.carriers, CarrierObjectUpload)
	if len(entry.carriers) == 0 && !entry.completed && !entry.objectRunning {
		r.terminalLocked(transferID, entry.binding, entry.manifest.ExpiresAt, ErrTransferNotFound)
		r.removeLocked(transferID)
	}
}

func (r *Reassembler) Expire(now time.Time) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for id, entry := range r.entries {
		if entry.expired || entry.manifest.ExpiresAt.After(now) {
			continue
		}
		entry.expired = true
		entry.aborted = true
		entry.objectAborted = true
		clear(entry.carriers)
		r.terminalLocked(id, entry.binding, entry.manifest.ExpiresAt, ErrTransferExpired)
		count++
		if entry.cancel != nil {
			entry.cancel()
		}
		if entry.objectCancel != nil {
			entry.objectCancel()
		}
		// A finalize/object owner keeps its reserved buffers charged until it
		// observes cancellation and runs its one cleanup path. Expire never
		// blocks while holding r.mu and never reallocates an expired entry.
		if !entry.completing && !entry.objectRunning {
			r.removeLocked(id)
		}
	}
	r.expireWaitsLocked(now)
	return count
}

// RetirePeer revokes every incomplete or completed transfer bound to one
// current-membership peer. It installs the terminal authorization result and
// cancels active cipher/object owners under the registry lock; those owners
// keep their buffers charged until their existing cleanup path joins. Stable
// buffers are zeroed immediately when no active owner can still reference
// them.
func (r *Reassembler) RetirePeer(meshID, peerID string) int {
	if r == nil || meshID == "" || peerID == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revokedPeers[reassemblyPeerKey(meshID, peerID)] = struct{}{}
	retired := 0
	for id, entry := range r.entries {
		if entry.binding.MeshID != meshID || entry.binding.SenderID != peerID && entry.binding.RecipientID != peerID {
			continue
		}
		entry.aborted = true
		entry.objectAborted = true
		clear(entry.carriers)
		r.terminalLocked(id, entry.binding, entry.manifest.ExpiresAt, ErrAuthentication)
		retired++
		if entry.cancel != nil {
			entry.cancel()
		}
		if entry.objectCancel != nil {
			entry.objectCancel()
		}
		if !entry.completing && !entry.objectRunning {
			r.removeLocked(id)
		}
	}
	for id, wait := range r.waits {
		if wait.binding.MeshID != meshID || wait.binding.SenderID != peerID && wait.binding.RecipientID != peerID {
			continue
		}
		r.setWaitTerminalLocked(wait, ErrAuthentication)
		if wait.waiters == 0 {
			delete(r.waits, id)
		}
	}
	return retired
}

// AllowPeer removes the fail-closed transfer fence only when a fresh complete
// current-membership snapshot contains the peer. It does not resurrect any
// transfer retired by an earlier snapshot.
func (r *Reassembler) AllowPeer(meshID, peerID string) {
	if r == nil || meshID == "" || peerID == "" {
		return
	}
	r.mu.Lock()
	delete(r.revokedPeers, reassemblyPeerKey(meshID, peerID))
	r.mu.Unlock()
}

// CurrentBytes reports aggregate private reassembly ownership without
// exposing transfer or peer identifiers.
func (r *Reassembler) CurrentBytes() int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalBytes
}

func (r *Reassembler) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.active.Wait()
		return
	}
	r.closed = true
	r.cancelLife()
	for id, entry := range r.entries {
		if entry.completing {
			if entry.cancel != nil {
				entry.cancel()
			}
		} else {
			r.removeLocked(id)
		}
	}
	for id, wait := range r.waits {
		r.setWaitTerminalLocked(wait, context.Canceled)
		delete(r.waits, id)
	}
	r.mu.Unlock()
	r.active.Wait()
}

func (r *Reassembler) releaseWaiter(transferID string, wait *transferWait) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.waits[transferID]; current != wait {
		return
	}
	wait.waiters--
	if wait.waiters == 0 {
		if wait.terminal == nil {
			delete(r.waits, transferID)
		} else {
			r.trimTerminalWaitsLocked()
		}
	}
}

func (r *Reassembler) notifyLocked(transferID string) {
	if wait := r.waits[transferID]; wait != nil {
		close(wait.changed)
		wait.changed = make(chan struct{})
	}
}

func (r *Reassembler) setWaitTerminalLocked(wait *transferWait, err error) {
	if wait == nil || wait.terminal != nil {
		return
	}
	wait.terminal = err
	r.terminalClock++
	wait.terminalAt = r.terminalClock
	close(wait.changed)
	wait.changed = make(chan struct{})
}

func (r *Reassembler) terminalLocked(transferID string, binding TransferBinding, expiresAt time.Time, err error) {
	wait := r.waits[transferID]
	if wait == nil {
		return
	}
	if wait.binding != binding {
		r.setWaitTerminalLocked(wait, ErrAuthentication)
		return
	}
	if expiresAt.Before(wait.expiresAt) {
		wait.expiresAt = expiresAt
	}
	r.setWaitTerminalLocked(wait, err)
}

func (r *Reassembler) expireWaitsLocked(now time.Time) {
	for id, wait := range r.waits {
		if wait.waiters == 0 && !wait.expiresAt.After(now) {
			delete(r.waits, id)
		}
	}
}

func (r *Reassembler) logicalTransfersLocked() int {
	total := len(r.entries)
	for id, wait := range r.waits {
		if wait.terminal == nil {
			if _, exists := r.entries[id]; !exists {
				total++
			}
		}
	}
	return total
}

func (r *Reassembler) logicalPeerTransfersLocked(senderID string) int {
	total := r.peerTransfers[senderID]
	for id, wait := range r.waits {
		if wait.terminal != nil || wait.binding.SenderID != senderID {
			continue
		}
		if _, exists := r.entries[id]; !exists {
			total++
		}
	}
	return total
}

// trimTerminalWaitsLocked keeps recent terminal identities only as a bounded
// defense-in-depth replay cache. Stable-message dedupe remains authoritative;
// terminal cache entries never reserve transfer count or byte quota.
func (r *Reassembler) trimTerminalWaitsLocked() {
	for {
		count := 0
		oldestID := ""
		oldest := ^uint64(0)
		for id, wait := range r.waits {
			if wait.terminal == nil || wait.waiters != 0 {
				continue
			}
			count++
			if wait.terminalAt < oldest {
				oldest = wait.terminalAt
				oldestID = id
			}
		}
		if count <= r.limits.MaximumTransfers || oldestID == "" {
			return
		}
		delete(r.waits, oldestID)
	}
}

func (r *Reassembler) removeLocked(id string) {
	entry, ok := r.entries[id]
	if !ok {
		return
	}
	r.terminalLocked(id, entry.binding, entry.manifest.ExpiresAt, ErrTransferNotFound)
	if entry.objectCancel != nil {
		entry.objectCancel()
	}
	zero(entry.data)
	zero(entry.canonical)
	delete(r.entries, id)
	r.releaseBytesLocked(entry.binding.SenderID, entry.chargedBytes)
	entry.chargedBytes = 0
	r.peerTransfers[entry.binding.SenderID]--
	if r.peerBytes[entry.binding.SenderID] == 0 {
		delete(r.peerBytes, entry.binding.SenderID)
	}
	if r.peerTransfers[entry.binding.SenderID] == 0 {
		delete(r.peerTransfers, entry.binding.SenderID)
	}
}

func (r *Reassembler) resetAfterFailureLocked(id string, entry *reassemblyEntry) {
	current, ok := r.entries[id]
	if !ok || current != entry || r.closed || entry.expired || len(entry.carriers) == 0 {
		r.removeLocked(id)
		return
	}
	entry.completing = false
	entry.completingVia = 0
	entry.completingObj = 0
	entry.cancel = nil
	entry.aborted = false
	entry.receivedCount = 0
	if entry.chargedBytes > entry.manifest.TransferredSize {
		extra := entry.chargedBytes - entry.manifest.TransferredSize
		r.releaseBytesLocked(entry.binding.SenderID, extra)
		entry.chargedBytes = entry.manifest.TransferredSize
	}
	zero(entry.data)
	entry.data = nil
	entry.received = nil
	if _, direct := entry.carriers[CarrierDirectChunks]; direct {
		entry.data = make([]byte, int(entry.manifest.TransferredSize))
		entry.received = make([]bool, int(entry.manifest.ChunkCount))
	} else if _, message := entry.carriers[CarrierMessageChunks]; message {
		entry.data = make([]byte, int(entry.manifest.TransferredSize))
		entry.received = make([]bool, int(entry.manifest.ChunkCount))
	}
}

func (r *Reassembler) canReserveLocked(senderID string, amount int64) bool {
	return amount > 0 && r.peerBytes[senderID] <= r.limits.ReassemblyBytesPerPeer && amount <= r.limits.ReassemblyBytesPerPeer-r.peerBytes[senderID] && r.totalBytes <= r.limits.ReassemblyBytes && amount <= r.limits.ReassemblyBytes-r.totalBytes
}

func (r *Reassembler) reserveLocked(senderID string, amount int64) {
	r.peerBytes[senderID] += amount
	r.totalBytes += amount
}

func (r *Reassembler) releaseBytesLocked(senderID string, amount int64) {
	if amount <= 0 {
		return
	}
	peer := r.peerBytes[senderID]
	if amount > peer {
		amount = peer
	}
	r.peerBytes[senderID] = peer - amount
	if amount > r.totalBytes {
		r.totalBytes = 0
	} else {
		r.totalBytes -= amount
	}
}

func completionEvidence(entry *reassemblyEntry) CompletionEvidence {
	if entry == nil {
		return CompletionEvidence{}
	}
	return CompletionEvidence{TransferID: entry.manifest.TransferID, MessageID: entry.manifest.MessageID, Digest: entry.binding.CanonicalDigest}
}

func carrierAuthorized(entry *reassemblyEntry, carrier CarrierKind) bool {
	_, ok := entry.carriers[carrier]
	return ok
}

func cloneManifest(value TransferManifest) TransferManifest {
	value.CanonicalDigest = clone(value.CanonicalDigest)
	value.TransferredDigest = clone(value.TransferredDigest)
	return value
}
func manifestsEqual(a, b TransferManifest) bool {
	return a.Version == b.Version && a.TransferID == b.TransferID && a.MessageID == b.MessageID && a.MeshID == b.MeshID && a.SenderID == b.SenderID && a.RecipientID == b.RecipientID && a.CanonicalSize == b.CanonicalSize && bytes.Equal(a.CanonicalDigest, b.CanonicalDigest) && a.TransferredSize == b.TransferredSize && bytes.Equal(a.TransferredDigest, b.TransferredDigest) && a.ChunkSize == b.ChunkSize && a.ChunkCount == b.ChunkCount && a.EncryptionRef == b.EncryptionRef && a.ExpiresAt.Equal(b.ExpiresAt)
}
