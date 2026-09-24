// Package rpc owns private request correlation and local request handles.
package rpc

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	MaxTableCapacity        = 65_536
	MaxCorrelationIssuance  = 1_048_576
	lateResponseDigestBytes = uint64(sha256.Size)
)

type OutboundConfig struct {
	Capacity int
	// ByteCapacity is an internal test seam. Services always inject one shared
	// derived budget and do not expose this as public configuration.
	ByteCapacity uint64
	Now          func() time.Time
	After        func(time.Duration) <-chan time.Time
}

type outboundResult struct {
	envelope protocol.Envelope
	err      error
}

type outboundEntry struct {
	request        protocol.Envelope
	correlationID  string
	conversationID string
	meshID         string
	peerID         string
	responsePath   string
	commandID      string // Deliberately always empty; command data is not retained.
	deadline       time.Time
	done           chan *outboundResult
	terminalResult *outboundResult
	terminal       bool
	payloadOwned   bool
	waiting        bool
	retainedBytes  uint64 // Excludes process-lifetime issued correlation metadata.
}

type retiredOutbound struct {
	deadline time.Time
	cause    error
}

type issuedOutbound struct {
	lateResponseDigest [sha256.Size]byte
	cause              error
}

// OutboundTable maps private correlation values to bounded local waits.
type OutboundTable struct {
	mu          sync.Mutex
	capacity    int
	now         func() time.Time
	after       func(time.Duration) <-chan time.Time
	entries     map[string]*outboundEntry
	active      map[string]map[string]struct{}
	retired     map[string]retiredOutbound
	issued      map[string]issuedOutbound
	issuedCount uint64
	issuedBytes uint64
	budget      *ByteBudget
}

func NewOutboundTable(config OutboundConfig) (*OutboundTable, error) {
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
	return NewOutboundTableWithBudget(config, budget)
}

// NewOutboundTableWithBudget binds a table to a service-level shared budget.
func NewOutboundTableWithBudget(config OutboundConfig, budget *ByteBudget) (*OutboundTable, error) {
	if config.Capacity < 1 || config.Capacity > MaxTableCapacity || budget == nil {
		return nil, ErrInvalidConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	after := config.After
	if after == nil {
		after = time.After
	}
	return &OutboundTable{
		capacity: config.Capacity,
		now:      now,
		after:    after,
		entries:  make(map[string]*outboundEntry, config.Capacity),
		active:   make(map[string]map[string]struct{}, config.Capacity),
		retired:  make(map[string]retiredOutbound, config.Capacity),
		issued:   make(map[string]issuedOutbound),
		budget:   budget,
	}, nil
}

// Register stores one validated outbound request before send.
func (t *OutboundTable) Register(request protocol.Envelope, commandID string, deadline time.Time) error {
	return t.RegisterWithResponsePath(request, commandID, "", deadline)
}

// RegisterWithResponsePath stores the bounded application path needed for
// early authenticated-response policy without retaining a second envelope.
func (t *OutboundTable) RegisterWithResponsePath(request protocol.Envelope, commandID, responsePath string, deadline time.Time) error {
	if t == nil || commandID == "" || deadline.IsZero() {
		return ErrInvalidConfig
	}
	if err := protocol.ValidateEnvelope(request); err != nil {
		return err
	}
	if request.Mode != protocol.ModeRequest {
		return ErrInvalidResponse
	}
	requestBytes := envelopeDynamicBytes(request) + uint64(len(responsePath))
	admissionBytes := requestBytes + lateResponseDigestBytes
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if !deadline.After(now) {
		return ErrExpired
	}
	t.pruneLocked(now)
	if _, used := t.issued[request.CorrelationID]; used {
		return ErrDuplicate
	}
	if _, ok := t.entries[request.CorrelationID]; ok {
		return ErrDuplicate
	}
	if _, ok := t.retired[request.CorrelationID]; ok {
		return ErrDuplicate
	}
	if t.trackedCountLocked() >= t.capacity {
		return ErrCapacity
	}
	if t.issuedCount >= MaxCorrelationIssuance {
		return ErrIssuanceExhausted
	}
	// Reserve from the borrowed graph before allocating any owned clone.
	if !t.budget.reserve(admissionBytes) {
		return ErrCapacity
	}
	owned := cloneEnvelopeExact(request)
	correlationID := owned.CorrelationID
	conversationID := owned.ConversationID
	meshID := owned.MeshID
	peerID := owned.Recipient
	ownedPath := strings.Clone(responsePath)
	t.entries[correlationID] = &outboundEntry{
		request:        owned,
		correlationID:  correlationID,
		conversationID: conversationID,
		meshID:         meshID,
		peerID:         peerID,
		responsePath:   ownedPath,
		deadline:       deadline,
		done:           make(chan *outboundResult, 1),
		retainedBytes:  requestBytes - uint64(len(correlationID)),
	}
	t.trackConversationLocked(conversationID, correlationID)
	t.issued[correlationID] = issuedOutbound{lateResponseDigest: lateResponseBinding(
		owned.MessageID,
		owned.CorrelationID,
		owned.ConversationID,
		owned.MeshID,
		owned.Sender,
		owned.Recipient,
	)}
	t.issuedCount++
	t.issuedBytes += uint64(len(correlationID)) + lateResponseDigestBytes
	_ = commandID // Validated for the caller contract but intentionally not retained.
	return nil
}

// Complete atomically matches one authenticated response. Duplicate, late,
// peer-swapped, or correlation-confused responses fail closed.
func (t *OutboundTable) Complete(response protocol.Envelope) error {
	return t.CompleteAccepted(response, nil)
}

// CompleteAccepted atomically matches one response and, while the exact
// request is still protected by the table lock, applies optional terminal
// delivery evidence before publishing RPC completion. The callback borrows
// both envelopes only for its invocation and must not retain or mutate them.
func (t *OutboundTable) CompleteAccepted(response protocol.Envelope, accept func(protocol.Envelope, protocol.Envelope) error) error {
	if t == nil {
		return ErrUnknown
	}
	if err := protocol.ValidateEnvelope(response); err != nil {
		return err
	}
	if response.Mode != protocol.ModeResponse {
		return ErrInvalidResponse
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.pruneLocked(now)
	entry, ok := t.entries[response.CorrelationID]
	if !ok {
		if _, issued := t.issued[response.CorrelationID]; issued {
			return ErrCompleted
		}
		return ErrUnknown
	}
	if entry.terminal {
		return ErrCompleted
	}
	if !now.Before(entry.deadline) {
		t.finishErrorLocked(entry.correlationID, entry, ErrExpired)
		return ErrExpired
	}
	request := entry.request
	if response.ReplyTo != request.MessageID || response.MeshID != entry.meshID || response.ConversationID != entry.conversationID || response.Sender != request.Recipient || response.Recipient != request.Sender {
		return ErrInvalidResponse
	}
	stableBytes := entry.stableBytes()
	terminalStableBytes := entry.terminalStableBytes()
	responseNonStable := envelopeDynamicBytes(response) - uint64(len(response.CorrelationID)+len(response.ConversationID)+len(response.MeshID))
	// The old request and new response overlap during freezing. Reserve the new
	// non-stable graph first; stable identity backing is moved, not recopied.
	if !t.budget.reserve(responseNonStable) {
		t.finishErrorLocked(entry.correlationID, entry, ErrCapacity)
		return ErrCapacity
	}
	if accept != nil {
		if err := accept(request, response); err != nil {
			t.budget.release(responseNonStable)
			return err
		}
	}
	owned := cloneResponseExact(response, entry.correlationID, entry.conversationID, entry.meshID)
	oldNonStable := entry.retainedBytes - stableBytes
	clearEnvelope(&entry.request)
	t.budget.release(oldNonStable + stableBytes - terminalStableBytes)
	entry.responsePath = ""
	entry.retainedBytes = terminalStableBytes + responseNonStable
	entry.terminal = true
	entry.payloadOwned = true
	result := &outboundResult{envelope: owned}
	entry.terminalResult = result
	entry.done <- result
	t.retired[entry.correlationID] = retiredOutbound{deadline: entry.deadline}
	return nil
}

func cloneResponseExact(response protocol.Envelope, correlationID, conversationID, meshID string) protocol.Envelope {
	response.CorrelationID = ""
	response.ConversationID = ""
	response.MeshID = ""
	owned := cloneEnvelopeExact(response)
	owned.CorrelationID = correlationID
	owned.ConversationID = conversationID
	owned.MeshID = meshID
	return owned
}

// PendingResponsePath validates scalar correlation metadata and returns the
// associated policy path without cloning or exposing the retained request.
func (t *OutboundTable) PendingResponsePath(response protocol.Envelope) (string, bool) {
	if t == nil || response.Mode != protocol.ModeResponse {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entries[response.CorrelationID]
	if entry == nil || entry.terminal {
		return "", false
	}
	request := entry.request
	if response.ReplyTo != request.MessageID || response.MeshID != entry.meshID || response.ConversationID != entry.conversationID || response.Sender != request.Recipient || response.Recipient != request.Sender {
		return "", false
	}
	return entry.responsePath, true
}

// ValidateLateResponse authenticates an exact response against immutable,
// process-lifetime metadata captured when its correlation was issued. It does
// not consult live request/outbox ownership and therefore remains valid after
// expiry, cancellation, completion, and pruning. No payload or response path
// is retained by this index.
func (t *OutboundTable) ValidateLateResponse(response protocol.Envelope) bool {
	if t == nil || response.Mode != protocol.ModeResponse {
		return false
	}
	if err := protocol.ValidateEnvelope(response); err != nil {
		return false
	}
	want := lateResponseBinding(
		response.ReplyTo,
		response.CorrelationID,
		response.ConversationID,
		response.MeshID,
		response.Recipient,
		response.Sender,
	)
	t.mu.Lock()
	issued, ok := t.issued[response.CorrelationID]
	t.mu.Unlock()
	return ok && subtle.ConstantTimeCompare(issued.lateResponseDigest[:], want[:]) == 1
}

func lateResponseBinding(requestMessageID, correlationID, conversationID, meshID, sender, recipient string) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("cynapsa-rpc-late-response-v1"))
	var length [8]byte
	for _, field := range []string{requestMessageID, correlationID, conversationID, meshID, sender, recipient} {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(field))
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func (t *OutboundTable) Cancel(correlationID string) error {
	return t.fail(correlationID, ErrCancelled)
}

func (t *OutboundTable) Fail(correlationID string, cause error) error {
	canonical, ok := canonicalTerminalCause(cause)
	if !ok {
		return ErrInvalidConfig
	}
	return t.fail(correlationID, canonical)
}

func (t *OutboundTable) fail(correlationID string, cause error) error {
	if t == nil {
		return ErrUnknown
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(t.now())
	entry, ok := t.entries[correlationID]
	if !ok {
		if _, issued := t.issued[correlationID]; issued {
			return ErrCompleted
		}
		return ErrUnknown
	}
	if entry.terminal {
		return ErrCompleted
	}
	t.finishErrorLocked(entry.correlationID, entry, cause)
	return nil
}

// Wait consumes exactly one registered request result. A context already
// cancelled when admission obtains the table lock deterministically wins over
// a previously queued completion; later races linearize at the table lock.
func (t *OutboundTable) Wait(ctx context.Context, correlationID string) (protocol.Envelope, error) {
	if t == nil || ctx == nil {
		return protocol.Envelope{}, ErrUnknown
	}
	t.mu.Lock()
	entry, ok := t.entries[correlationID]
	if !ok {
		err := t.missingErrorLocked(correlationID)
		t.mu.Unlock()
		return protocol.Envelope{}, err
	}
	if entry.waiting {
		t.mu.Unlock()
		return protocol.Envelope{}, ErrDuplicate
	}
	if ctx.Err() != nil {
		if entry.terminal {
			t.replaceTerminalErrorLocked(entry.correlationID, entry, ErrCancelled)
		} else {
			t.finishErrorLocked(entry.correlationID, entry, ErrCancelled)
		}
		result := <-entry.done
		clearEnvelope(&result.envelope)
		t.removeEntryLocked(entry.correlationID, entry, false)
		t.mu.Unlock()
		return protocol.Envelope{}, ErrCancelled
	}
	entry.waiting = true
	duration := entry.deadline.Sub(t.now())
	done := entry.done
	t.mu.Unlock()
	if duration <= 0 {
		_ = t.fail(correlationID, ErrExpired)
	}

	var result *outboundResult
	select {
	case result = <-done:
	case <-ctx.Done():
		if err := t.fail(correlationID, ErrCancelled); err != nil && !errors.Is(err, ErrCompleted) && !errors.Is(err, ErrUnknown) {
			return protocol.Envelope{}, err
		}
		result = <-done
	case <-t.after(duration):
		if err := t.fail(correlationID, ErrExpired); err != nil && !errors.Is(err, ErrCompleted) && !errors.Is(err, ErrUnknown) {
			return protocol.Envelope{}, err
		}
		result = <-done
	}

	t.mu.Lock()
	if current := t.entries[entry.correlationID]; current == entry {
		t.removeEntryLocked(entry.correlationID, entry, false)
	}
	t.mu.Unlock()
	if result == nil {
		return protocol.Envelope{}, ErrUnknown
	}
	if result.err != nil {
		clearEnvelope(&result.envelope)
		return protocol.Envelope{}, result.err
	}
	// Move the sole owned result to the caller. No payload-sized clone exists.
	response := result.envelope
	result.envelope = protocol.Envelope{}
	return response, nil
}

func (t *OutboundTable) finishErrorLocked(correlationID string, entry *outboundEntry, cause error) {
	stableBytes := entry.terminalStableBytes()
	clearEnvelope(&entry.request)
	t.budget.release(entry.retainedBytes - stableBytes)
	entry.responsePath = ""
	entry.retainedBytes = stableBytes
	entry.terminal = true
	entry.payloadOwned = false
	result := &outboundResult{err: cause}
	entry.terminalResult = result
	entry.done <- result
	t.retired[correlationID] = retiredOutbound{deadline: entry.deadline, cause: cause}
	t.setIssuedCauseLocked(correlationID, cause)
}

func (t *OutboundTable) replaceTerminalErrorLocked(correlationID string, entry *outboundEntry, cause error) {
	result := entry.terminalResult
	if result == nil {
		result = &outboundResult{}
		entry.terminalResult = result
		entry.done <- result
	}
	clearEnvelope(&result.envelope)
	result.err = cause
	stableBytes := entry.terminalStableBytes()
	clearEnvelope(&entry.request)
	t.budget.release(entry.retainedBytes - stableBytes)
	entry.responsePath = ""
	entry.retainedBytes = stableBytes
	entry.terminal = true
	entry.payloadOwned = false
	t.retired[correlationID] = retiredOutbound{deadline: entry.deadline, cause: cause}
	t.setIssuedCauseLocked(correlationID, cause)
}

func (t *OutboundTable) retireEntryLocked(correlationID string, entry *outboundEntry, cause error, drain bool) {
	if drain {
		if entry.terminalResult != nil {
			clearEnvelope(&entry.terminalResult.envelope)
			entry.terminalResult.err = nil
		}
		select {
		case <-entry.done:
		default:
		}
	}
	t.releaseConversationLocked(entry.conversationID, correlationID)
	delete(t.entries, correlationID)
	clearEnvelope(&entry.request)
	t.budget.release(entry.retainedBytes)
	entry.retainedBytes = 0
	entry.conversationID = ""
	entry.meshID = ""
	entry.peerID = ""
	entry.responsePath = ""
	entry.correlationID = ""
	t.retired[correlationID] = retiredOutbound{deadline: entry.deadline, cause: cause}
	if cause != nil {
		t.setIssuedCauseLocked(correlationID, cause)
	}
}

func (t *OutboundTable) setIssuedCauseLocked(correlationID string, cause error) {
	issued, ok := t.issued[correlationID]
	if !ok {
		return
	}
	issued.cause = permanentCause(cause)
	t.issued[correlationID] = issued
}

func permanentCause(cause error) error {
	canonical, _ := canonicalTerminalCause(cause)
	return canonical
}

func canonicalTerminalCause(cause error) (error, bool) {
	if cause == nil {
		return nil, false
	}
	for _, sentinel := range []error{ErrExpired, ErrCancelled, ErrAuthorizationRejected, ErrServerUnavailable, ErrCapacity, ErrInvalidResponse} {
		if errors.Is(cause, sentinel) {
			return sentinel, true
		}
	}
	return nil, false
}

func (entry *outboundEntry) stableBytes() uint64 {
	return uint64(len(entry.conversationID) + len(entry.meshID) + len(entry.peerID) + len(entry.responsePath))
}

func (entry *outboundEntry) terminalStableBytes() uint64 {
	return uint64(len(entry.conversationID) + len(entry.meshID) + len(entry.peerID))
}

func (t *OutboundTable) removeEntryLocked(correlationID string, entry *outboundEntry, drain bool) {
	if drain {
		if entry.terminalResult != nil {
			clearEnvelope(&entry.terminalResult.envelope)
			entry.terminalResult.err = nil
		}
		select {
		case <-entry.done:
		default:
		}
	}
	t.releaseConversationLocked(entry.conversationID, correlationID)
	delete(t.entries, correlationID)
	clearEnvelope(&entry.request)
	t.budget.release(entry.retainedBytes)
	entry.retainedBytes = 0
}

func (t *OutboundTable) missingErrorLocked(correlationID string) error {
	if retired, ok := t.retired[correlationID]; ok {
		if retired.cause != nil {
			return retired.cause
		}
		return ErrCompleted
	}
	if issued, ok := t.issued[correlationID]; ok {
		if issued.cause != nil {
			return issued.cause
		}
		return ErrCompleted
	}
	return ErrUnknown
}

func (t *OutboundTable) trackConversationLocked(conversationID, correlationID string) {
	entries := t.active[conversationID]
	if entries == nil {
		entries = make(map[string]struct{})
		t.active[conversationID] = entries
	}
	entries[correlationID] = struct{}{}
}

func (t *OutboundTable) releaseConversationLocked(conversationID, correlationID string) {
	entries := t.active[conversationID]
	delete(entries, correlationID)
	if len(entries) == 0 {
		delete(t.active, conversationID)
	}
}

func (t *OutboundTable) pruneLocked(now time.Time) int {
	removed := 0
	for correlation, retired := range t.retired {
		if !now.Before(retired.deadline) {
			delete(t.retired, correlation)
			removed++
		}
	}
	for correlation, entry := range t.entries {
		if now.Before(entry.deadline) {
			continue
		}
		if entry.waiting {
			if !entry.terminal {
				t.finishErrorLocked(correlation, entry, ErrExpired)
			}
			continue
		}
		t.retireEntryLocked(correlation, entry, ErrExpired, true)
		removed++
	}
	return removed
}

func (t *OutboundTable) trackedCountLocked() int {
	count := len(t.entries)
	for correlation := range t.retired {
		if _, active := t.entries[correlation]; !active {
			count++
		}
	}
	return count
}

func (t *OutboundTable) Prune(now time.Time) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pruneLocked(now)
}

func (t *OutboundTable) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for _, entry := range t.entries {
		if !entry.terminal || entry.payloadOwned {
			count++
		}
	}
	return count
}

// OwnedBytes returns the race-safe aggregate budget usage for internal tests
// and diagnostics.
func (t *OutboundTable) OwnedBytes() uint64 {
	if t == nil {
		return 0
	}
	return t.budget.ownedBytes()
}

func (t *OutboundTable) Stats() Stats {
	if t == nil {
		return Stats{}
	}
	return t.budget.stats()
}

// Destroy clears every request/result graph and releases this table's issued
// identity ownership. It is idempotent and is called only after service joins.
func (t *OutboundTable) Destroy() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for correlationID, entry := range t.entries {
		t.removeEntryLocked(correlationID, entry, true)
	}
	if t.issuedBytes != 0 {
		t.budget.release(t.issuedBytes)
		t.issuedBytes = 0
	}
	for correlationID, issued := range t.issued {
		issued.lateResponseDigest = [sha256.Size]byte{}
		issued.cause = nil
		t.issued[correlationID] = issued
	}
	clear(t.active)
	clear(t.retired)
	clear(t.issued)
	t.issuedCount = MaxCorrelationIssuance
}

func (t *OutboundTable) ActiveConversation(conversationID string) bool {
	if t == nil || conversationID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(t.now())
	return len(t.active[conversationID]) > 0
}

func (t *OutboundTable) RetainsConversation(conversationID string) bool {
	if t == nil || conversationID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active[conversationID]) != 0
}

// FailAll resolves waiters and destroys every unclaimed terminal result during
// shutdown. A later Wait observes the retained scalar cause, never payload.
func (t *OutboundTable) FailAll(cause error) int {
	canonical, ok := canonicalTerminalCause(cause)
	if t == nil || !ok {
		return 0
	}
	cause = canonical
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for correlation, entry := range t.entries {
		if entry.terminal && !entry.payloadOwned {
			continue
		}
		if entry.terminal && entry.waiting && len(entry.done) == 0 {
			// Receiving the sole result pointer is the waiter-success
			// linearization point. It now owns the response bytes.
			continue
		}
		if entry.terminal {
			t.replaceTerminalErrorLocked(correlation, entry, cause)
		} else {
			t.finishErrorLocked(correlation, entry, cause)
		}
		count++
	}
	return count
}

// FailPeer atomically resolves every unconsumed request to one currently
// unauthorized destination. A queued successful response is scrubbed and
// replaced; a result already received by its waiter cannot be retracted.
func (t *OutboundTable) FailPeer(meshID, peerID string) int {
	return len(t.FailPeerCorrelations(meshID, peerID))
}

// FailMessageID resolves only the request named by a trusted server routing
// error. A queued response may be replaced, but one already taken by its
// waiter cannot be retracted. The caller owns any separately decoded value.
func (t *OutboundTable) FailMessageID(messageID string, cause error) string {
	canonical, ok := canonicalTerminalCause(cause)
	if t == nil || messageID == "" || !ok {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for correlation, entry := range t.entries {
		if entry.request.MessageID != messageID && (entry.terminalResult == nil || entry.terminalResult.envelope.ReplyTo != messageID) {
			continue
		}
		if entry.terminal && (!entry.payloadOwned || entry.waiting && len(entry.done) == 0) {
			return ""
		}
		if entry.terminal {
			t.replaceTerminalErrorLocked(correlation, entry, canonical)
		} else {
			t.finishErrorLocked(correlation, entry, canonical)
		}
		return correlation
	}
	return ""
}

// FailPeerCorrelations returns the bounded scalar keys whose terminal result
// changed, allowing the composing service to scrub its separately owned
// decoded response value without touching a success already consumed.
func (t *OutboundTable) FailPeerCorrelations(meshID, peerID string) []string {
	if t == nil || meshID == "" || peerID == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	failed := make([]string, 0)
	for correlation, entry := range t.entries {
		if entry.meshID != meshID || entry.peerID != peerID {
			continue
		}
		if entry.terminal && !entry.payloadOwned {
			continue
		}
		if entry.terminal && entry.waiting && len(entry.done) == 0 {
			// The waiter already owns the only result pointer.
			continue
		}
		if entry.terminal {
			t.replaceTerminalErrorLocked(correlation, entry, ErrAuthorizationRejected)
		} else {
			t.finishErrorLocked(correlation, entry, ErrAuthorizationRejected)
		}
		failed = append(failed, correlation)
	}
	return failed
}
