// Package outbox owns private pending durable-delivery state.
package outbox

import (
	"crypto/sha256"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type Config struct {
	MessageCapacity int
	ByteCapacity    int64
	Now             func() time.Time
}

type RetiredEnvelope struct {
	MessageID, ConversationID string
}

// Outbox assigns stable local ordinals and stores unsafe process-lifetime
// envelopes. It is not a separate transport rank.
type Outbox struct {
	mu              sync.Mutex
	store           *MemoryStore
	now             func() time.Time
	nextOrdinal     uint64
	nextReservation uint64
	scheduleCursor  uint64
	handled         uint64
	destroyed       bool
}

func New(config Config) (*Outbox, error) {
	store, err := NewMemoryStore(config.MessageCapacity, config.ByteCapacity)
	if err != nil {
		return nil, err
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Outbox{store: store, now: now, nextOrdinal: 1, nextReservation: 1}, nil
}

type Reservation struct {
	MessageID    string
	Envelope     protocol.Envelope
	FallbackOnly bool
	token        uint64
}

// Rank1ReceiptEvidence binds pending ownership and terminal evidence to one
// exact logical envelope and authenticated live-session channel binding.
type Rank1ReceiptEvidence struct {
	MessageID, ConversationID, Sender, Recipient, MeshID string
	ChannelBinding                                       [sha256.Size]byte
}

type Rank1PendingACK struct {
	MessageID string
	token     uint64
}

type Provisional struct {
	MessageID string
	token     uint64
}

// Enqueue retains the exact envelope and returns its stable local ordinal.
// Carrier replacement for the same logical message retains the prior ordinal.
func (outbox *Outbox) Enqueue(envelope protocol.Envelope) (uint64, error) {
	if outbox == nil {
		return 0, ErrInvalidConfig
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return 0, err
	}

	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return 0, ErrInvalidConfig
	}
	return outbox.enqueueLocked(envelope)
}

func (outbox *Outbox) enqueueLocked(envelope protocol.Envelope) (uint64, error) {
	// Put handles an existing logical message and preserves its ordinal. Find
	// that ordinal first so callers receive the stable value.
	if ordinal, queuedAt, exists := outbox.store.lookupMetadata(envelope); exists {
		candidate := Entry{Envelope: envelope.Clone(), QueuedAt: queuedAt, Ordinal: ordinal}
		if err := outbox.store.Put(candidate); err != nil {
			return 0, err
		}
		return ordinal, nil
	}
	if outbox.nextOrdinal == 0 {
		return 0, ErrOrdinalExhausted
	}
	ordinal := outbox.nextOrdinal
	entry := Entry{Envelope: envelope.Clone(), QueuedAt: outbox.now().UTC(), Ordinal: ordinal}
	if err := outbox.store.Put(entry); err != nil {
		return 0, err
	}
	if ordinal == math.MaxUint64 {
		outbox.nextOrdinal = 0
	} else {
		outbox.nextOrdinal++
	}
	return ordinal, nil
}

// EnqueueReserved atomically makes an envelope visible and reserves it when it
// is unowned. A zero reservation means another owner already holds it; enqueue
// succeeded and automatic draining will revisit it.
func (outbox *Outbox) EnqueueReserved(envelope protocol.Envelope) (Reservation, error) {
	if outbox == nil || protocol.ValidateEnvelope(envelope) != nil {
		return Reservation{}, ErrInvalidConfig
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return Reservation{}, ErrInvalidConfig
	}
	if _, err := outbox.enqueueLocked(envelope); err != nil {
		return Reservation{}, err
	}
	return outbox.reserveLocked(envelope.MessageID)
}

func (outbox *Outbox) EnqueueProvisional(envelope protocol.Envelope) (Provisional, error) {
	if outbox == nil || protocol.ValidateEnvelope(envelope) != nil {
		return Provisional{}, ErrInvalidConfig
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed || outbox.nextReservation == 0 {
		return Provisional{}, ErrInvalidConfig
	}
	if outbox.store.exists(envelope) {
		return Provisional{}, ErrDuplicateConflict
	}
	if _, err := outbox.enqueueLocked(envelope); err != nil {
		return Provisional{}, err
	}
	token := outbox.nextReservation
	if err := outbox.store.markProvisional(envelope.MessageID, token); err != nil {
		return Provisional{}, err
	}
	if token == math.MaxUint64 {
		outbox.nextReservation = 0
	} else {
		outbox.nextReservation++
	}
	return Provisional{MessageID: envelope.MessageID, token: token}, nil
}

func (outbox *Outbox) CommitProvisional(provisional Provisional) (Reservation, error) {
	if outbox == nil || provisional.token == 0 {
		return Reservation{}, ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if err := outbox.store.commitProvisional(provisional.MessageID, provisional.token); err != nil {
		return Reservation{}, err
	}
	return outbox.reserveLocked(provisional.MessageID)
}

func (outbox *Outbox) CommitProvisionalQueued(provisional Provisional) error {
	if outbox == nil || provisional.token == 0 {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.commitProvisional(provisional.MessageID, provisional.token)
}

func (outbox *Outbox) AbortProvisional(provisional Provisional) error {
	if outbox == nil || provisional.token == 0 {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.abortProvisional(provisional.MessageID, provisional.token)
}

func (outbox *Outbox) reserveLocked(messageID string) (Reservation, error) {
	if outbox.nextReservation == 0 {
		return Reservation{}, ErrOrdinalExhausted
	}
	token := outbox.nextReservation
	envelope, err := outbox.store.reserveEligible(messageID, token)
	if err != nil {
		if err == ErrOwnedEntry {
			return Reservation{}, nil
		}
		return Reservation{}, err
	}
	if token == math.MaxUint64 {
		outbox.nextReservation = 0
	} else {
		outbox.nextReservation++
	}
	return Reservation{MessageID: messageID, Envelope: envelope.Envelope, FallbackOnly: envelope.FallbackOnly, token: token}, nil
}

func (outbox *Outbox) ReserveEligible(messageID string) (Reservation, error) {
	if outbox == nil || messageID == "" {
		return Reservation{}, ErrUnknownEntry
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return Reservation{}, ErrUnknownEntry
	}
	return outbox.reserveLocked(messageID)
}

// ReserveEligibleBatch returns at most limit independently eligible entries.
// It copies only reserved envelopes, never the entire byte-bounded queue.
func (outbox *Outbox) ReserveEligibleBatch(limit int) []Reservation {
	if outbox == nil || limit <= 0 {
		return nil
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return nil
	}
	ids, cursor := outbox.store.reservableIDs(limit, outbox.scheduleCursor)
	outbox.scheduleCursor = cursor
	reservations := make([]Reservation, 0, len(ids))
	for _, id := range ids {
		reservation, err := outbox.reserveLocked(id)
		if err == nil && reservation.MessageID != "" {
			reservations = append(reservations, reservation)
		}
	}
	return reservations
}

// EligibleMessageIDs returns bounded identity-only scheduling hints.
// ReserveEligible remains the authoritative transaction and revalidates each hint.
func (outbox *Outbox) EligibleMessageIDs(limit int) []string {
	if outbox == nil || limit <= 0 {
		return nil
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return nil
	}
	ids, cursor := outbox.store.reservableIDs(limit, outbox.scheduleCursor)
	outbox.scheduleCursor = cursor
	return ids
}

// Destroy idempotently zeroizes and releases every retained envelope.
func (outbox *Outbox) Destroy() {
	if outbox == nil {
		return
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return
	}
	outbox.destroyed = true
	outbox.store.destroy()
}

func (outbox *Outbox) Release(reservation Reservation) bool {
	if outbox == nil || reservation.token == 0 {
		return false
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.releaseReservation(reservation.MessageID, reservation.token)
}

func (outbox *Outbox) MarkReservedTerminal(reservation Reservation) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.deleteReserved(reservation.MessageID, reservation.token)
}

// ClaimRank1PendingACK moves the exact active reservation into authenticated
// Rank1 receipt ownership. A successful DataChannel write alone is not
// terminal evidence.
func (outbox *Outbox) ClaimRank1PendingACK(reservation Reservation, evidence Rank1ReceiptEvidence) (Rank1PendingACK, error) {
	if outbox == nil || reservation.token == 0 {
		return Rank1PendingACK{}, ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if err := outbox.store.claimRank1Pending(reservation.MessageID, reservation.token, evidence); err != nil {
		return Rank1PendingACK{}, err
	}
	return Rank1PendingACK{MessageID: reservation.MessageID, token: reservation.token}, nil
}

// MarkRank1Acknowledged retires only an active exact-match pending receipt.
// Retired-peer, duplicate, late, wrong-session, and forged evidence is
// rejected without changing queue ownership.
func (outbox *Outbox) MarkRank1Acknowledged(pending Rank1PendingACK, evidence Rank1ReceiptEvidence) error {
	if outbox == nil || pending.token == 0 {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.acknowledgeRank1(pending.MessageID, pending.token, evidence)
}

// MakeRank2FallbackEligible closes exact Rank1 pending ownership while
// retaining the same immutable queue entry for a fresh Rank2-only attempt.
func (outbox *Outbox) MakeRank2FallbackEligible(pending Rank1PendingACK, evidence Rank1ReceiptEvidence) error {
	if outbox == nil || pending.token == 0 {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.releaseRank1Pending(pending.MessageID, pending.token, evidence)
}

// MarkHandled applies trusted cumulative local handled-through evidence.
func (outbox *Outbox) MarkHandled(handledThrough uint64) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if handledThrough < outbox.handled {
		return ErrHandledRegression
	}
	lastAssigned := outbox.nextOrdinal - 1
	if outbox.nextOrdinal == 0 {
		lastAssigned = math.MaxUint64
	}
	if handledThrough > lastAssigned {
		return ErrInvalidEvidence
	}
	outbox.store.deleteThrough(handledThrough)
	outbox.handled = handledThrough
	return nil
}

// MarkTerminal removes exactly one message after trusted terminal evidence.
func (outbox *Outbox) MarkTerminal(messageID string) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.Delete(messageID)
}

// MarkUnownedTerminal removes an entry only while no durable carrier owns it.
// It is the atomic terminal edge used by Rank1 acceptance and delivery.drop.
func (outbox *Outbox) MarkUnownedTerminal(messageID string) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.deleteUnowned(messageID)
}

// RetireAcceptedRPCResponse removes the exact request proven delivered by an
// authenticated, application-accepted RPC response. Unlike generic terminal
// deletion, this evidence may cross Rank1-pending or Rank2 ownership because
// the response can only exist after the remote application accepted the exact
// request. The request is supplied by the RPC table's still-locked match so a
// forged, late, or correlation-confused response cannot select queue state.
func (outbox *Outbox) RetireAcceptedRPCResponse(request, response protocol.Envelope) error {
	if outbox == nil || protocol.ValidateEnvelope(request) != nil || protocol.ValidateEnvelope(response) != nil || !acceptedRPCResponseMatches(request, response) {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return ErrUnknownEntry
	}
	return outbox.store.retireAcceptedRPCResponse(request)
}

// ValidateLateRPCResponse reports whether an exact request is still physically
// retained in this outbox. It is not the late-response authentication source:
// trusted carrier custody may legitimately retire the payload before a delayed
// response arrives. The RPC table owns that process-lifetime scalar proof.
func (outbox *Outbox) ValidateLateRPCResponse(response protocol.Envelope) bool {
	if outbox == nil || protocol.ValidateEnvelope(response) != nil || response.Mode != protocol.ModeResponse {
		return false
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return !outbox.destroyed && outbox.store.matchesLateRPCResponse(response)
}

// RetireLateRPCResponse removes a still-retained request after the RPC table
// has authenticated an exact delayed response. A concurrent trusted carrier
// receipt is idempotent; message identifiers cannot be rebound to new work.
func (outbox *Outbox) RetireLateRPCResponse(response protocol.Envelope) error {
	if outbox == nil || protocol.ValidateEnvelope(response) != nil || response.Mode != protocol.ModeResponse {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return ErrUnknownEntry
	}
	return outbox.store.retireLateRPCResponse(response)
}

func acceptedRPCResponseMatches(request, response protocol.Envelope) bool {
	return request.Mode == protocol.ModeRequest && response.Mode == protocol.ModeResponse &&
		request.MessageID == response.ReplyTo && request.CorrelationID == response.CorrelationID &&
		request.ConversationID == response.ConversationID && request.MeshID == response.MeshID &&
		request.Sender == response.Recipient && request.Recipient == response.Sender
}

// DropUnowned atomically requests retraction before Rank2 ownership. A
// non-nil wait channel closes after the exact active borrower returns and the
// entry has been zeroized.
func (outbox *Outbox) DropUnowned(messageID string) (<-chan error, error) {
	if outbox == nil {
		return nil, ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if outbox.destroyed {
		return nil, ErrUnknownEntry
	}
	return outbox.store.requestDrop(messageID)
}

// CancelDrop revokes the exact pending drop only if no terminal carrier
// outcome has already linearized.
func (outbox *Outbox) CancelDrop(messageID string, wait <-chan error) bool {
	if outbox == nil || wait == nil {
		return false
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.cancelDrop(messageID, wait)
}

// ClaimRank2 atomically transfers an existing unowned entry to Rank2. The
// transport ordinal is assigned by Rank2's serialized wire gate.
func (outbox *Outbox) ClaimRank2(messageID string, transportOrdinal uint64) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.claimRank2(messageID, transportOrdinal)
}

func (outbox *Outbox) OwnsRank2(messageID string) bool {
	if outbox == nil {
		return false
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.ownsRank2(messageID)
}

func (outbox *Outbox) Rank2Metadata() []Rank2Metadata {
	if outbox == nil {
		return nil
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	entries := outbox.store.rank2Metadata()
	sort.Slice(entries, func(i, j int) bool { return entries[i].TransportOrdinal < entries[j].TransportOrdinal })
	return entries
}

func (outbox *Outbox) GetRank2(messageID string, transportOrdinal uint64) (protocol.Envelope, error) {
	if outbox == nil {
		return protocol.Envelope{}, ErrUnknownEntry
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.getRank2(messageID, transportOrdinal)
}

// RetireRank2Owned removes exactly the entry owned by the supplied Rank2
// transport ordinal. Generic terminal/drop paths remain unable to delete
// carrier-owned entries.
func (outbox *Outbox) RetireRank2Owned(messageID string, transportOrdinal uint64) error {
	if outbox == nil {
		return ErrInvalidEvidence
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.retireRank2(messageID, transportOrdinal)
}

// MarkRank2Handled atomically retires only Rank2-owned entries covered by the
// carrier's transport ordinal evidence.
func (outbox *Outbox) MarkRank2Handled(handled uint64) []string {
	if outbox == nil || handled == 0 {
		return nil
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.deleteRank2Through(handled)
}

// Pending returns immutable identity-preserving replay copies in ordinal order.
func (outbox *Outbox) Pending() ([]protocol.Envelope, error) {
	if outbox == nil {
		return nil, ErrInvalidConfig
	}
	outbox.mu.Lock()
	entries := outbox.store.snapshot()
	outbox.mu.Unlock()
	defer func() {
		for index := range entries {
			clearEntry(&entries[index])
		}
	}()
	sort.Slice(entries, func(left, right int) bool { return entries[left].Ordinal < entries[right].Ordinal })
	pending := make([]protocol.Envelope, len(entries))
	for index, entry := range entries {
		pending[index] = entry.Envelope.Clone()
	}
	return pending, nil
}

// Get returns the one immutable pending envelope with messageID. Message IDs
// are process-unique; multiple matches fail closed as invalid evidence.
func (outbox *Outbox) Get(messageID string) (protocol.Envelope, error) {
	if outbox == nil || messageID == "" {
		return protocol.Envelope{}, ErrUnknownEntry
	}
	outbox.mu.Lock()
	entries := outbox.store.snapshot()
	outbox.mu.Unlock()
	defer func() {
		for index := range entries {
			clearEntry(&entries[index])
		}
	}()
	var found *protocol.Envelope
	for _, entry := range entries {
		if entry.Envelope.MessageID != messageID {
			continue
		}
		if found != nil {
			return protocol.Envelope{}, ErrInvalidEvidence
		}
		copy := entry.Envelope.Clone()
		found = &copy
	}
	if found == nil {
		return protocol.Envelope{}, ErrUnknownEntry
	}
	result := found.Clone()
	for index := range found.Payload.Inline {
		found.Payload.Inline[index] = 0
	}
	for index := range found.CredentialProof {
		found.CredentialProof[index] = 0
	}
	return result, nil
}

func (outbox *Outbox) Usage() (messages int, bytes int64) {
	if outbox == nil {
		return 0, 0
	}
	return outbox.store.usage()
}

// HasConversation reports whether the bounded queue retains an envelope for
// the exact conversation. It does not clone payload or proof ownership.
func (outbox *Outbox) HasConversation(conversationID string) bool {
	if outbox == nil || conversationID == "" {
		return false
	}
	return outbox.store.hasConversation(conversationID)
}

// RetirePeerOwned removes every pending outbound envelope for one peer that is
// absent from the installed current-membership snapshot. Borrowed reservations
// become terminal retired ownership and are scrubbed by their final release.
func (outbox *Outbox) RetirePeerOwned(meshID, peerID string) []RetiredEnvelope {
	if outbox == nil || meshID == "" || peerID == "" {
		return nil
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	return outbox.store.retirePeerOwned(meshID, peerID)
}
