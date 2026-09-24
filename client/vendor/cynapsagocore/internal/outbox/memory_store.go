package outbox

import (
	"crypto/sha256"
	"sort"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	MaxMessageCapacity       = 65_536
	MaxByteCapacity    int64 = 256 << 20
)

type storedEntry struct {
	entry        Entry
	logical      [sha256.Size]byte
	messageKey   string
	rank2Ordinal uint64
	rank1Pending *rank1PendingState
	fallbackOnly bool
	reservation  uint64
	provisional  uint64
	dropWait     chan error
}

type rank1PendingState struct {
	token    uint64
	evidence Rank1ReceiptEvidence
}

type reservedEnvelope struct {
	Envelope     protocol.Envelope
	FallbackOnly bool
}

type Rank2Metadata struct {
	MessageID, ConversationID, Sender, Recipient, MeshID string
	TransportOrdinal                                     uint64
}

// MemoryStore is bounded process-lifetime-only pending storage.
type MemoryStore struct {
	mu                  sync.Mutex
	maxMessages         int
	maxBytes            int64
	bytes               int64
	byOrdinal           map[uint64]storedEntry
	byMessage           map[string]uint64
	byID                map[string]uint64
	retiredReservations map[uint64]Entry
}

func NewMemoryStore(maxMessages int, maxBytes int64) (*MemoryStore, error) {
	if maxMessages < 1 || maxMessages > MaxMessageCapacity || maxBytes < 1 || maxBytes > MaxByteCapacity {
		return nil, ErrInvalidConfig
	}
	return &MemoryStore{
		maxMessages:         maxMessages,
		maxBytes:            maxBytes,
		byOrdinal:           make(map[uint64]storedEntry, maxMessages),
		byMessage:           make(map[string]uint64, maxMessages),
		byID:                make(map[string]uint64, maxMessages),
		retiredReservations: make(map[uint64]Entry),
	}, nil
}

// Put stores a new ordinal or converges a carrier replacement for the same
// logical message without allocating a second queue item.
func (store *MemoryStore) Put(entry Entry) error {
	if store == nil || entry.Ordinal == 0 || entry.QueuedAt.IsZero() {
		return ErrInvalidConfig
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		return err
	}
	encoded, err := codec.Encode(entry.Envelope)
	if err != nil {
		return err
	}
	// Byte accounting is always derived from the canonical private encoding;
	// callers cannot understate queue usage through Entry metadata.
	entry.Bytes = int64(len(encoded))
	logical, err := protocol.LogicalMessageDigest(entry.Envelope)
	if err != nil {
		return err
	}
	key := messageKey(entry.Envelope)

	store.mu.Lock()
	defer store.mu.Unlock()
	if ordinal, exists := store.byMessage[key]; exists {
		existing := store.byOrdinal[ordinal]
		if store.byID[entry.Envelope.MessageID] != ordinal {
			return ErrInvalidEvidence
		}
		if existing.logical != logical {
			return ErrDuplicateConflict
		}
		// An active borrow or carrier-owned entry has immutable backing. An
		// idempotent duplicate converges without replacing or clearing it.
		if existing.reservation != 0 || existing.rank2Ordinal != 0 || existing.rank1Pending != nil {
			return nil
		}
		delta := entry.Bytes - existing.entry.Bytes
		if delta > 0 && store.bytes > store.maxBytes-delta {
			return ErrCapacity
		}
		entry.Ordinal = existing.entry.Ordinal
		entry.QueuedAt = existing.entry.QueuedAt
		store.bytes += delta
		clearEntry(&existing.entry)
		store.byOrdinal[ordinal] = storedEntry{entry: entry.clone(), logical: logical, messageKey: key, rank2Ordinal: existing.rank2Ordinal, rank1Pending: existing.rank1Pending, fallbackOnly: existing.fallbackOnly, reservation: existing.reservation, provisional: existing.provisional, dropWait: existing.dropWait}
		return nil
	}
	if _, duplicateOrdinal := store.byOrdinal[entry.Ordinal]; duplicateOrdinal {
		return ErrDuplicateConflict
	}
	if _, duplicateID := store.byID[entry.Envelope.MessageID]; duplicateID {
		return ErrDuplicateConflict
	}
	if len(store.byOrdinal)+len(store.retiredReservations) >= store.maxMessages || entry.Bytes > store.maxBytes || store.bytes > store.maxBytes-entry.Bytes {
		return ErrCapacity
	}
	store.byOrdinal[entry.Ordinal] = storedEntry{entry: entry.clone(), logical: logical, messageKey: key}
	store.byMessage[key] = entry.Ordinal
	store.byID[entry.Envelope.MessageID] = entry.Ordinal
	store.bytes += entry.Bytes
	return nil
}

func (store *MemoryStore) findMessageLocked(messageID string) (uint64, storedEntry, error) {
	ordinal, ok := store.byID[messageID]
	if !ok || ordinal == 0 {
		return 0, storedEntry{}, ErrUnknownEntry
	}
	entry, ok := store.byOrdinal[ordinal]
	if !ok || entry.entry.Envelope.MessageID != messageID {
		return 0, storedEntry{}, ErrInvalidEvidence
	}
	return ordinal, entry, nil
}

func (store *MemoryStore) Delete(messageID string) error {
	if store == nil || messageID == "" {
		return ErrUnknownEntry
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	found, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.rank1Pending != nil || entry.rank2Ordinal != 0 {
		return ErrOwnedEntry
	}
	store.retireOrdinalLocked(found)
	return nil
}

// retireServerRejected uses a validated server-originated routing error as
// terminal evidence. Unlike local deletion it may retire a Rank1/Rank2-owned
// entry; an active borrower keeps its bytes until its reservation is released.
func (store *MemoryStore) retireServerRejected(messageID string) error {
	if store == nil || messageID == "" {
		return ErrUnknownEntry
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, _, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	store.retireOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) deleteUnowned(messageID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.reservation != 0 || entry.provisional != 0 || entry.dropWait != nil {
		return ErrOwnedEntry
	}
	store.deleteOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) retireAcceptedRPCResponse(request protocol.Envelope) error {
	logical, err := protocol.LogicalMessageDigest(request)
	if err != nil {
		return ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(request.MessageID)
	if err != nil {
		// A transport receipt/custody result may have won the terminal race.
		// The same accepted response remains idempotent and must not affect any
		// other entry.
		if err == ErrUnknownEntry {
			return nil
		}
		return err
	}
	if entry.logical != logical || entry.entry.Envelope.Mode != protocol.ModeRequest {
		return ErrInvalidEvidence
	}
	// retireOrdinalLocked preserves bytes borrowed by an active reservation;
	// Rank1-pending and released Rank2 ownership have no such borrower and are
	// zeroized immediately. Later carrier evidence observes an absent entry.
	store.retireOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) matchesLateRPCResponse(response protocol.Envelope) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, entry, err := store.findMessageLocked(response.ReplyTo)
	return err == nil && acceptedRPCResponseMatches(entry.entry.Envelope, response)
}

func (store *MemoryStore) retireLateRPCResponse(response protocol.Envelope) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(response.ReplyTo)
	if err != nil {
		// Validation happened before the peer-lane commit. A trusted transport
		// receipt may have retired the exact request in the meantime.
		if err == ErrUnknownEntry {
			return nil
		}
		return err
	}
	if !acceptedRPCResponseMatches(entry.entry.Envelope, response) {
		return ErrInvalidEvidence
	}
	store.retireOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) markProvisional(messageID string, token uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if token == 0 || entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.reservation != 0 || entry.provisional != 0 {
		return ErrOwnedEntry
	}
	entry.provisional = token
	store.byOrdinal[ordinal] = entry
	return nil
}

func (store *MemoryStore) commitProvisional(messageID string, token uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if token == 0 || entry.provisional != token {
		return ErrInvalidEvidence
	}
	entry.provisional = 0
	store.byOrdinal[ordinal] = entry
	return nil
}

func (store *MemoryStore) abortProvisional(messageID string, token uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if token == 0 || entry.provisional != token || entry.reservation != 0 || entry.rank2Ordinal != 0 || entry.rank1Pending != nil {
		return ErrInvalidEvidence
	}
	store.deleteOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) claimRank2(messageID string, transportOrdinal uint64) error {
	if transportOrdinal == 0 {
		return ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.rank2Ordinal != 0 {
		if entry.rank2Ordinal == transportOrdinal {
			return nil
		}
		return ErrInvalidEvidence
	}
	if entry.reservation == 0 || entry.rank1Pending != nil {
		return ErrInvalidEvidence
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	entry.rank2Ordinal = transportOrdinal
	store.byOrdinal[ordinal] = entry
	return nil
}

func (store *MemoryStore) claimRank1Pending(messageID string, token uint64, evidence Rank1ReceiptEvidence) error {
	if token == 0 || !validRank1Evidence(evidence) {
		return ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.reservation != token || entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.fallbackOnly || !rank1EvidenceMatchesEnvelope(evidence, entry.entry.Envelope) {
		return ErrInvalidEvidence
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	entry.rank1Pending = &rank1PendingState{token: token, evidence: evidence}
	entry.reservation = 0
	store.byOrdinal[ordinal] = entry
	return nil
}

func (store *MemoryStore) acknowledgeRank1(messageID string, token uint64, evidence Rank1ReceiptEvidence) error {
	if token == 0 || !validRank1Evidence(evidence) {
		return ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.reservation != 0 || entry.rank1Pending == nil || entry.rank1Pending.token != token || entry.rank1Pending.evidence != evidence || !rank1EvidenceMatchesEnvelope(evidence, entry.entry.Envelope) {
		return ErrInvalidEvidence
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	store.byOrdinal[ordinal] = entry
	store.deleteOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) releaseRank1Pending(messageID string, token uint64, evidence Rank1ReceiptEvidence) error {
	if token == 0 || !validRank1Evidence(evidence) {
		return ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if entry.reservation != 0 || entry.rank1Pending == nil || entry.rank1Pending.token != token || entry.rank1Pending.evidence != evidence || !rank1EvidenceMatchesEnvelope(evidence, entry.entry.Envelope) {
		return ErrInvalidEvidence
	}
	entry.rank1Pending = nil
	entry.fallbackOnly = true
	store.byOrdinal[ordinal] = entry
	return nil
}

func validRank1Evidence(evidence Rank1ReceiptEvidence) bool {
	return evidence.ChannelBinding != [sha256.Size]byte{} && evidence.MessageID != "" && evidence.ConversationID != "" && evidence.Sender != "" && evidence.Recipient != "" && evidence.MeshID != ""
}

func rank1EvidenceMatchesEnvelope(evidence Rank1ReceiptEvidence, envelope protocol.Envelope) bool {
	return evidence.MessageID == envelope.MessageID && evidence.ConversationID == envelope.ConversationID && evidence.Sender == envelope.Sender && evidence.Recipient == envelope.Recipient && evidence.MeshID == envelope.MeshID
}

func (store *MemoryStore) reserveEligible(messageID string, token uint64) (reservedEnvelope, error) {
	if token == 0 {
		return reservedEnvelope{}, ErrInvalidEvidence
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return reservedEnvelope{}, err
	}
	if entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.reservation != 0 || entry.provisional != 0 || entry.dropWait != nil {
		return reservedEnvelope{}, ErrOwnedEntry
	}
	entry.reservation = token
	store.byOrdinal[ordinal] = entry
	// Reservation borrowers receive the store-owned immutable value. Its slices
	// remain owned and charged by the store until the reservation is resolved;
	// callers must not retain or mutate them.
	return reservedEnvelope{Envelope: entry.entry.Envelope, FallbackOnly: entry.fallbackOnly}, nil
}

func (store *MemoryStore) reservableIDs(limit int, after uint64) ([]string, uint64) {
	if limit <= 0 {
		return nil, after
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinals := make([]uint64, 0, len(store.byOrdinal))
	for ordinal := range store.byOrdinal {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	type candidate struct {
		ordinal uint64
		id      string
	}
	candidates := make([]candidate, 0, len(ordinals))
	for _, ordinal := range ordinals {
		entry := store.byOrdinal[ordinal]
		if entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.reservation != 0 || entry.provisional != 0 || entry.dropWait != nil {
			continue
		}
		candidates = append(candidates, candidate{ordinal: ordinal, id: entry.entry.Envelope.MessageID})
	}
	if len(candidates) == 0 {
		return nil, after
	}
	start := sort.Search(len(candidates), func(index int) bool { return candidates[index].ordinal > after })
	if start == len(candidates) {
		start = 0
	}
	count := min(limit, len(candidates))
	ids := make([]string, 0, count)
	cursor := after
	for offset := 0; offset < count; offset++ {
		entry := candidates[(start+offset)%len(candidates)]
		ids = append(ids, entry.id)
		cursor = entry.ordinal
	}
	return ids, cursor
}

func (store *MemoryStore) releaseReservation(messageID string, token uint64) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		retired, ok := store.retiredReservations[token]
		if !ok || retired.Envelope.MessageID != messageID {
			return false
		}
		delete(store.retiredReservations, token)
		delete(store.byID, messageID)
		store.bytes -= retired.Bytes
		clearEntry(&retired)
		return true
	}
	if token == 0 || entry.reservation != token || entry.rank1Pending != nil {
		return false
	}
	if entry.dropWait != nil {
		store.resolveDropLocked(&entry, nil)
		store.byOrdinal[ordinal] = entry
		store.deleteOrdinalLocked(ordinal)
		return true
	}
	entry.reservation = 0
	store.byOrdinal[ordinal] = entry
	return true
}

func (store *MemoryStore) deleteReserved(messageID string, token uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		retired, ok := store.retiredReservations[token]
		if !ok || retired.Envelope.MessageID != messageID {
			return err
		}
		delete(store.retiredReservations, token)
		delete(store.byID, messageID)
		store.bytes -= retired.Bytes
		clearEntry(&retired)
		return nil
	}
	if token == 0 || entry.reservation != token || entry.rank2Ordinal != 0 || entry.rank1Pending != nil {
		return ErrInvalidEvidence
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	store.byOrdinal[ordinal] = entry
	store.deleteOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) ownsRank2(messageID string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, entry, err := store.findMessageLocked(messageID)
	return err == nil && entry.rank2Ordinal != 0
}

func (store *MemoryStore) requestDrop(messageID string) (<-chan error, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return nil, err
	}
	if entry.rank2Ordinal != 0 || entry.rank1Pending != nil || entry.provisional != 0 {
		return nil, ErrOwnedEntry
	}
	if entry.reservation == 0 {
		store.deleteOrdinalLocked(ordinal)
		return nil, nil
	}
	if entry.dropWait != nil {
		return nil, ErrOwnedEntry
	}
	if entry.dropWait == nil {
		entry.dropWait = make(chan error, 1)
		store.byOrdinal[ordinal] = entry
	}
	return entry.dropWait, nil
}

func (store *MemoryStore) cancelDrop(messageID string, wait <-chan error) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil || entry.dropWait == nil || (<-chan error)(entry.dropWait) != wait {
		return false
	}
	entry.dropWait = nil
	store.byOrdinal[ordinal] = entry
	return true
}

func (store *MemoryStore) rank2Metadata() []Rank2Metadata {
	store.mu.Lock()
	defer store.mu.Unlock()
	entries := make([]Rank2Metadata, 0)
	for _, stored := range store.byOrdinal {
		if stored.rank2Ordinal == 0 {
			continue
		}
		envelope := stored.entry.Envelope
		entries = append(entries, Rank2Metadata{
			MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
			Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
			TransportOrdinal: stored.rank2Ordinal,
		})
	}
	return entries
}

func (store *MemoryStore) getRank2(messageID string, transportOrdinal uint64) (protocol.Envelope, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return protocol.Envelope{}, err
	}
	if entry.rank2Ordinal != transportOrdinal || transportOrdinal == 0 {
		return protocol.Envelope{}, ErrInvalidEvidence
	}
	return entry.entry.Envelope.Clone(), nil
}

func (store *MemoryStore) retireRank2(messageID string, transportOrdinal uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if transportOrdinal == 0 || entry.rank2Ordinal != transportOrdinal || entry.rank1Pending != nil {
		return ErrInvalidEvidence
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	store.byOrdinal[ordinal] = entry
	store.deleteOrdinalLocked(ordinal)
	return nil
}

func (store *MemoryStore) releaseRank2(messageID string, transportOrdinal uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, entry, err := store.findMessageLocked(messageID)
	if err != nil {
		return err
	}
	if transportOrdinal == 0 || entry.rank2Ordinal != transportOrdinal || entry.rank1Pending != nil {
		return ErrInvalidEvidence
	}
	entry.rank2Ordinal = 0
	store.byOrdinal[ordinal] = entry
	return nil
}

func (store *MemoryStore) deleteRank2Through(handled uint64) []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := make([]string, 0)
	for ordinal, stored := range store.byOrdinal {
		if stored.rank2Ordinal != 0 && stored.rank2Ordinal <= handled {
			removed = append(removed, stored.entry.Envelope.MessageID)
			if stored.reservation != 0 {
				store.resolveDropLocked(&stored, ErrOwnedEntry)
				delete(store.byOrdinal, ordinal)
				delete(store.byMessage, stored.messageKey)
				store.byID[stored.entry.Envelope.MessageID] = 0
				store.retiredReservations[stored.reservation] = stored.entry
			} else {
				store.deleteOrdinalLocked(ordinal)
			}
		}
	}
	sort.Strings(removed)
	return removed
}

func (store *MemoryStore) lookup(envelope protocol.Envelope) (Entry, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, ok := store.byMessage[messageKey(envelope)]
	if !ok {
		return Entry{}, false
	}
	stored, ok := store.byOrdinal[ordinal]
	if !ok {
		return Entry{}, false
	}
	return stored.entry.clone(), true
}

func (store *MemoryStore) lookupMetadata(envelope protocol.Envelope) (uint64, time.Time, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	ordinal, ok := store.byMessage[messageKey(envelope)]
	if !ok {
		return 0, time.Time{}, false
	}
	stored, ok := store.byOrdinal[ordinal]
	if !ok {
		return 0, time.Time{}, false
	}
	return ordinal, stored.entry.QueuedAt, true
}

func (store *MemoryStore) exists(envelope protocol.Envelope) bool {
	if store == nil {
		return false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.byMessage[messageKey(envelope)]
	return ok
}

func (store *MemoryStore) deleteThrough(handled uint64) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := 0
	for ordinal, entry := range store.byOrdinal {
		if ordinal <= handled {
			if entry.rank1Pending != nil || entry.rank2Ordinal != 0 {
				continue
			}
			store.retireOrdinalLocked(ordinal)
			removed++
		}
	}
	return removed
}

func (store *MemoryStore) retireOrdinalLocked(ordinal uint64) {
	entry, ok := store.byOrdinal[ordinal]
	if !ok {
		return
	}
	if entry.reservation == 0 {
		store.deleteOrdinalLocked(ordinal)
		return
	}
	delete(store.byOrdinal, ordinal)
	delete(store.byMessage, entry.messageKey)
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	store.byID[entry.entry.Envelope.MessageID] = 0
	store.retiredReservations[entry.reservation] = entry.entry
}

func (store *MemoryStore) deleteOrdinalLocked(ordinal uint64) {
	entry, ok := store.byOrdinal[ordinal]
	if !ok {
		return
	}
	store.resolveDropLocked(&entry, ErrOwnedEntry)
	delete(store.byOrdinal, ordinal)
	delete(store.byMessage, entry.messageKey)
	delete(store.byID, entry.entry.Envelope.MessageID)
	store.bytes -= entry.entry.Bytes
	clearEntry(&entry.entry)
}

func (store *MemoryStore) resolveDropLocked(entry *storedEntry, result error) {
	if entry != nil && entry.dropWait != nil {
		entry.dropWait <- result
		close(entry.dropWait)
		entry.dropWait = nil
	}
}

func clearEntry(entry *Entry) {
	if entry == nil {
		return
	}
	for index := range entry.Envelope.Payload.Inline {
		entry.Envelope.Payload.Inline[index] = 0
	}
	for index := range entry.Envelope.CredentialProof {
		entry.Envelope.CredentialProof[index] = 0
	}
	*entry = Entry{}
}

func (store *MemoryStore) snapshot() []Entry {
	store.mu.Lock()
	defer store.mu.Unlock()
	entries := make([]Entry, 0, len(store.byOrdinal))
	for _, stored := range store.byOrdinal {
		entries = append(entries, stored.entry.clone())
	}
	return entries
}

func (store *MemoryStore) usage() (int, int64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.byOrdinal) + len(store.retiredReservations), store.bytes
}

func (store *MemoryStore) destroy() {
	if store == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for ordinal, stored := range store.byOrdinal {
		if stored.reservation != 0 {
			store.resolveDropLocked(&stored, ErrOwnedEntry)
			delete(store.byOrdinal, ordinal)
			delete(store.byMessage, stored.messageKey)
			store.byID[stored.entry.Envelope.MessageID] = 0
			store.retiredReservations[stored.reservation] = stored.entry
		} else {
			store.deleteOrdinalLocked(ordinal)
		}
	}
	// Active retired reservations are borrowed by admitted callbacks and are
	// zeroized by their later Release/MarkReservedTerminal transaction.
}

func (store *MemoryStore) hasConversation(conversationID string) bool {
	if store == nil || conversationID == "" {
		return false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, stored := range store.byOrdinal {
		if stored.entry.Envelope.ConversationID == conversationID {
			return true
		}
	}
	for _, entry := range store.retiredReservations {
		if entry.Envelope.ConversationID == conversationID {
			return true
		}
	}
	return false
}

func (store *MemoryStore) retirePeerOwned(meshID, peerID string) []RetiredEnvelope {
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := make([]RetiredEnvelope, 0)
	for ordinal, stored := range store.byOrdinal {
		envelope := stored.entry.Envelope
		if envelope.MeshID != meshID || envelope.Recipient != peerID {
			continue
		}
		if stored.provisional != 0 {
			store.deleteOrdinalLocked(ordinal)
			continue
		}
		removed = append(removed, RetiredEnvelope{MessageID: envelope.MessageID, ConversationID: envelope.ConversationID})
		if stored.reservation != 0 {
			store.resolveDropLocked(&stored, ErrOwnedEntry)
			delete(store.byOrdinal, ordinal)
			delete(store.byMessage, stored.messageKey)
			store.byID[envelope.MessageID] = 0
			store.retiredReservations[stored.reservation] = stored.entry
		} else {
			store.deleteOrdinalLocked(ordinal)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].MessageID < removed[j].MessageID })
	return removed
}
func messageKey(envelope protocol.Envelope) string {
	return envelope.MeshID + "\x00" + envelope.Sender + "\x00" + envelope.MessageID
}
