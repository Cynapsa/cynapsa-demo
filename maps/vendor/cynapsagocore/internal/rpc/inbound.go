package rpc

import (
	"crypto/rand"
	"io"
	"math"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type InboundConfig struct {
	Capacity int
	// ByteCapacity is an internal standalone-test seam; composed services inject
	// one shared derived budget instead.
	ByteCapacity uint64
	Now          func() time.Time
	Entropy      io.Reader
	LeaseEntropy io.Reader
}

const MaxRequestHandleIssuance = 1_048_576

type inboundEntry struct {
	request        protocol.Envelope
	ownedHandle    string
	conversationID string
	deadline       time.Time
	retainedBytes  uint64
	reserved       bool
	leaseToken     [16]byte
	leaseEpoch     uint64
}

type replyBorrow struct {
	envelope protocol.Envelope
	bytes    uint64
}

type retiredInbound struct {
	deadline time.Time
	cause    error
}

// ReplyLease is a package-owned reservation held across local reply admission.
type ReplyLease struct {
	handle string
	token  [16]byte
	epoch  uint64
}

// InboundTable maps opaque local request handles to private reply metadata.
type InboundTable struct {
	mu            sync.Mutex
	capacity      int
	now           func() time.Time
	entropy       io.Reader
	leaseEntropy  io.Reader
	entries       map[string]inboundEntry
	active        map[string]map[string]struct{}
	retired       map[string]retiredInbound
	issued        map[string]struct{}
	issuedCount   uint64
	identityBytes uint64
	budget        *ByteBudget
	borrows       map[ReplyLease]*replyBorrow
	destroyed     bool
}

func NewInboundTable(config InboundConfig) (*InboundTable, error) {
	var budget *ByteBudget
	var err error
	if config.ByteCapacity != 0 {
		budget, err = newByteBudget(config.ByteCapacity)
	} else {
		budget, err = NewByteBudget(config.Capacity)
	}
	if err != nil {
		return nil, err
	}
	return NewInboundTableWithBudget(config, budget)
}

// NewInboundTableWithBudget binds a table to a service-level shared budget.
func NewInboundTableWithBudget(config InboundConfig, budget *ByteBudget) (*InboundTable, error) {
	if config.Capacity < 1 || config.Capacity > MaxTableCapacity {
		return nil, ErrInvalidConfig
	}
	if budget == nil {
		return nil, ErrInvalidConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	leaseEntropy := config.LeaseEntropy
	if leaseEntropy == nil {
		leaseEntropy = rand.Reader
	}
	return &InboundTable{
		capacity:     config.Capacity,
		now:          now,
		entropy:      config.Entropy,
		leaseEntropy: leaseEntropy,
		entries:      make(map[string]inboundEntry, config.Capacity),
		active:       make(map[string]map[string]struct{}, config.Capacity),
		retired:      make(map[string]retiredInbound, config.Capacity),
		issued:       make(map[string]struct{}),
		budget:       budget,
		borrows:      make(map[ReplyLease]*replyBorrow, config.Capacity),
	}, nil
}

// CreateHandle registers one inbound request and returns a local opaque handle.
func (t *InboundTable) CreateHandle(request protocol.Envelope, deadline time.Time) (string, error) {
	if t == nil || deadline.IsZero() {
		return "", ErrInvalidConfig
	}
	if err := protocol.ValidateEnvelope(request); err != nil {
		return "", err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if request.Mode != protocol.ModeRequest || !deadline.After(now) {
		return "", ErrExpired
	}
	t.pruneLocked(now)
	if len(t.entries)+len(t.retired) >= t.capacity {
		return "", ErrCapacity
	}
	if t.issuedCount >= MaxRequestHandleIssuance {
		return "", ErrIssuanceExhausted
	}
	requestBytes := envelopeDynamicBytes(request)
	retainedBytes := uint64(requestHandleLength) + requestBytes
	// Reserve the exact fixed-length handle and request graph before entropy or
	// any owned clone allocation.
	if !t.budget.reserve(retainedBytes) {
		return "", ErrCapacity
	}
	handle, err := (HandleGenerator{
		Reader: t.entropy,
		Exists: func(candidate string) bool {
			_, issued := t.issued[candidate]
			return issued
		},
	}).Generate()
	if err != nil {
		t.budget.release(retainedBytes)
		return "", err
	}
	ownedBytes := requestBytes
	owned := cloneEnvelopeExact(request)
	conversationID := owned.ConversationID
	t.entries[handle] = inboundEntry{request: owned, ownedHandle: handle, conversationID: conversationID, deadline: deadline, retainedBytes: ownedBytes}
	t.trackConversationLocked(conversationID, handle)
	t.issued[handle] = struct{}{}
	t.issuedCount++
	t.identityBytes += uint64(len(handle))
	return handle, nil
}

// BeginReply atomically reserves a handle and returns immutable request
// metadata. A concurrent reply cannot obtain a second lease.
func (t *InboundTable) BeginReply(handle string) (protocol.Envelope, ReplyLease, error) {
	if t == nil || ValidateRequestHandle(handle) != nil {
		return protocol.Envelope{}, ReplyLease{}, ErrInvalidHandle
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.pruneLocked(now)
	entry, ok := t.entries[handle]
	if !ok {
		if retired, ok := t.retired[handle]; ok {
			if retired.cause != nil {
				return protocol.Envelope{}, ReplyLease{}, retired.cause
			}
			return protocol.Envelope{}, ReplyLease{}, ErrCompleted
		}
		return protocol.Envelope{}, ReplyLease{}, ErrInvalidHandle
	}
	if !now.Before(entry.deadline) {
		t.removeEntryLocked(handle, entry, true)
		return protocol.Envelope{}, ReplyLease{}, ErrExpired
	}
	if entry.reserved {
		return protocol.Envelope{}, ReplyLease{}, ErrReplyInProgress
	}
	if entry.leaseEpoch == math.MaxUint64 {
		return protocol.Envelope{}, ReplyLease{}, ErrInvalidLease
	}
	borrowBytes := envelopeDynamicBytes(entry.request)
	if !t.budget.reserve(borrowBytes) {
		return protocol.Envelope{}, ReplyLease{}, ErrCapacity
	}
	if _, err := io.ReadFull(t.leaseEntropy, entry.leaseToken[:]); err != nil {
		t.budget.release(borrowBytes)
		return protocol.Envelope{}, ReplyLease{}, ErrHandleEntropy
	}
	entry.leaseEpoch++
	entry.reserved = true
	borrow := &replyBorrow{envelope: cloneEnvelopeExact(entry.request), bytes: borrowBytes}
	lease := ReplyLease{handle: entry.ownedHandle, token: entry.leaseToken, epoch: entry.leaseEpoch}
	t.borrows[lease] = borrow
	t.entries[handle] = entry
	return borrow.envelope, lease, nil
}

// CommitReply consumes a reserved handle only after local reply acceptance.
func (t *InboundTable) CommitReply(lease ReplyLease) error {
	if t == nil {
		return ErrInvalidLease
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[lease.handle]
	if !ok || !entry.reserved || entry.leaseToken != lease.token || entry.leaseEpoch != lease.epoch {
		if retired, exists := t.retired[lease.handle]; exists && retired.cause != nil {
			return retired.cause
		}
		return ErrInvalidLease
	}
	if !t.now().Before(entry.deadline) {
		t.removeEntryLocked(lease.handle, entry, true)
		return ErrExpired
	}
	t.removeEntryLocked(lease.handle, entry, true)
	return nil
}

// ReleaseReply restores a reserved handle after local admission failure.
func (t *InboundTable) ReleaseReply(lease ReplyLease) error {
	if t == nil {
		return ErrInvalidLease
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[lease.handle]
	if !ok || !entry.reserved || entry.leaseToken != lease.token || entry.leaseEpoch != lease.epoch {
		if retired, exists := t.retired[lease.handle]; exists && retired.cause != nil {
			return retired.cause
		}
		return ErrInvalidLease
	}
	if !t.now().Before(entry.deadline) {
		t.removeEntryLocked(lease.handle, entry, true)
		return ErrExpired
	}
	entry.reserved = false
	entry.leaseToken = [16]byte{}
	t.entries[lease.handle] = entry
	return nil
}

// ConsumeHandle is a convenience for a caller that already has trusted local
// acceptance. Reply pipelines should use BeginReply/CommitReply/ReleaseReply.
func (t *InboundTable) ConsumeHandle(handle string) (protocol.Envelope, error) {
	request, lease, err := t.BeginReply(handle)
	if err != nil {
		return protocol.Envelope{}, err
	}
	if err := t.CommitReply(lease); err != nil {
		clearEnvelope(&request)
		_ = t.FinalizeReply(lease)
		return protocol.Envelope{}, err
	}
	if !t.transferBorrow(lease) {
		clearEnvelope(&request)
		return protocol.Envelope{}, ErrInvalidLease
	}
	return request, nil
}

func (t *InboundTable) Cancel(handle string) error {
	if t == nil || ValidateRequestHandle(handle) != nil {
		return ErrInvalidHandle
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[handle]
	if !ok {
		return ErrInvalidHandle
	}
	t.removeEntryLocked(handle, entry, true)
	return nil
}

func (t *InboundTable) removeEntryLocked(handle string, entry inboundEntry, retire bool) {
	t.releaseConversationLocked(entry.conversationID, handle)
	delete(t.entries, handle)
	if retire {
		t.retired[handle] = retiredInbound{deadline: entry.deadline}
	}
	clearEnvelope(&entry.request)
	t.budget.release(entry.retainedBytes)
}

func (t *InboundTable) finalizeBorrowLocked(lease ReplyLease) bool {
	borrow := t.borrows[lease]
	if borrow == nil {
		return false
	}
	clearEnvelope(&borrow.envelope)
	t.budget.release(borrow.bytes)
	delete(t.borrows, lease)
	return true
}

// FinalizeReply releases one lease-owned borrow after the holder has scrubbed
// its returned Envelope. It is idempotent and independent of reply authority.
func (t *InboundTable) FinalizeReply(lease ReplyLease) error {
	if t == nil {
		return ErrInvalidLease
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finalizeBorrowLocked(lease)
	t.releaseDestroyedIdentitiesLocked()
	return nil
}

func (t *InboundTable) transferBorrow(lease ReplyLease) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	borrow := t.borrows[lease]
	if borrow == nil {
		return false
	}
	borrow.envelope = protocol.Envelope{}
	t.budget.release(borrow.bytes)
	delete(t.borrows, lease)
	t.releaseDestroyedIdentitiesLocked()
	return true
}

func (t *InboundTable) trackConversationLocked(conversationID, handle string) {
	entries := t.active[conversationID]
	if entries == nil {
		entries = make(map[string]struct{})
		t.active[conversationID] = entries
	}
	entries[handle] = struct{}{}
}

func (t *InboundTable) releaseConversationLocked(conversationID, handle string) {
	entries := t.active[conversationID]
	delete(entries, handle)
	if len(entries) == 0 {
		delete(t.active, conversationID)
	}
}

func (t *InboundTable) pruneLocked(now time.Time) int {
	removed := 0
	for handle, retired := range t.retired {
		if !now.Before(retired.deadline) {
			delete(t.retired, handle)
			removed++
		}
	}
	for handle, entry := range t.entries {
		if !now.Before(entry.deadline) {
			t.removeEntryLocked(handle, entry, false)
			removed++
		}
	}
	return removed
}

func (t *InboundTable) Prune(now time.Time) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pruneLocked(now)
}

func (t *InboundTable) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

// OwnedBytes returns the race-safe aggregate budget usage for internal tests.
func (t *InboundTable) OwnedBytes() uint64 {
	if t == nil {
		return 0
	}
	return t.budget.ownedBytes()
}

func (t *InboundTable) Stats() Stats {
	if t == nil {
		return Stats{}
	}
	return t.budget.stats()
}

// Destroy clears every request graph and releases this table's issued handle
// ownership after all service operations and workers have joined.
func (t *InboundTable) Destroy() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for handle, entry := range t.entries {
		t.removeEntryLocked(handle, entry, false)
	}
	t.destroyed = true
	clear(t.active)
	clear(t.retired)
	t.issuedCount = MaxRequestHandleIssuance
	t.releaseDestroyedIdentitiesLocked()
}

func (t *InboundTable) releaseDestroyedIdentitiesLocked() {
	if !t.destroyed || len(t.borrows) != 0 {
		return
	}
	if t.identityBytes != 0 {
		t.budget.release(t.identityBytes)
		t.identityBytes = 0
	}
	clear(t.issued)
}

// ActiveConversation reports whether one live inbound request handle is owned
// by the exact conversation. Expiry and the bounded index are linearized under
// the table lock, so callers never scan the global handle registry.
func (t *InboundTable) ActiveConversation(conversationID string) bool {
	if t == nil || conversationID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for handle := range t.active[conversationID] {
		entry, ok := t.entries[handle]
		if !ok {
			t.releaseConversationLocked(conversationID, handle)
			continue
		}
		if !now.Before(entry.deadline) {
			t.removeEntryLocked(handle, entry, false)
		}
	}
	return len(t.active[conversationID]) > 0
}

// RetainsConversation is the side-effect-free ownership query used at an
// enclosing service's final close linearization point. Unlike
// ActiveConversation it does not consult the injected clock or prune state.
func (t *InboundTable) RetainsConversation(conversationID string) bool {
	if t == nil || conversationID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active[conversationID]) != 0
}

// CancelAll expires all local reply authority during service shutdown.
func (t *InboundTable) CancelAll() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	count := len(t.entries)
	for handle, entry := range t.entries {
		t.removeEntryLocked(handle, entry, true)
	}
	return count
}

// CancelPeer permanently invalidates inbound request/reply authority from one
// removed peer. Active reply borrows remain holder-owned until FinalizeReply;
// revocation never mutates memory concurrently borrowed by the SDK. Every
// later Begin/Commit/Release attempt receives ErrAuthorizationRejected without
// transport I/O.
func (t *InboundTable) CancelPeer(meshID, peerID string) int {
	if t == nil || meshID == "" || peerID == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for handle, entry := range t.entries {
		if entry.request.MeshID != meshID || entry.request.Sender != peerID {
			continue
		}
		t.removeEntryLocked(handle, entry, false)
		t.retired[handle] = retiredInbound{deadline: entry.deadline, cause: ErrAuthorizationRejected}
		count++
	}
	return count
}
