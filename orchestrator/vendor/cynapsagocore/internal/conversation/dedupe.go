package conversation

import (
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	// Dedupe capacities bound process-lifetime identity state independently of
	// application queue capacity. Terminal identities remain resident for the
	// replay window after their application work has completed, so coupling
	// these limits to concurrent mailbox/outbox slots would turn throughput into
	// artificial backpressure.
	DefaultDedupeGlobalCapacity  = 65_536
	DefaultDedupePerPeerCapacity = 4_096
	// MaximumMailboxReplayRetention is the longest exact-resource mailbox
	// replay window accepted by the bundled ejabberd authority. Terminal
	// message identity must outlive that window so a lost destination SM
	// acknowledgement cannot invoke the application handler twice.
	MaximumMailboxReplayRetention = 24 * time.Hour
	DedupeReplaySafetyMargin      = time.Hour
	DefaultDedupeRetention        = MaximumMailboxReplayRetention + DedupeReplaySafetyMargin
)

// DedupeResult classifies one stable logical message before handler delivery.
type DedupeResult string

const (
	DedupeNew               DedupeResult = "new"
	DedupeDuplicateInFlight DedupeResult = "duplicate_in_flight"
	DedupeDuplicateTerminal DedupeResult = "duplicate_terminal"
)

type DedupeConfig struct {
	GlobalCapacity  int
	PerPeerCapacity int
	Retention       time.Duration
	Now             func() time.Time
}

type dedupeState uint8

const (
	dedupeInFlight dedupeState = iota
	dedupeTerminal
)

type dedupeEntry struct {
	peerKey        string
	meshID         string
	conversationID string
	recipient      string
	state          dedupeState
	expiresAt      time.Time
}

// Deduper tracks bounded delivered and in-flight logical message identity.
// One instance is scoped to a receiving process; V1 restart clears it.
type Deduper struct {
	mu              sync.Mutex
	globalCapacity  int
	perPeerCapacity int
	retention       time.Duration
	now             func() time.Time
	entries         map[string]dedupeEntry
	peerCounts      map[string]int
	nextPrune       time.Time
}

func NewDeduper(config DedupeConfig) (*Deduper, error) {
	global := config.GlobalCapacity
	if global == 0 {
		global = DefaultDedupeGlobalCapacity
	}
	perPeer := config.PerPeerCapacity
	if perPeer == 0 {
		perPeer = DefaultDedupePerPeerCapacity
	}
	retention := config.Retention
	if retention == 0 {
		retention = DefaultDedupeRetention
	}
	if global < 1 || global > DefaultDedupeGlobalCapacity || perPeer < 1 || perPeer > DefaultDedupePerPeerCapacity || perPeer > global || retention < 0 {
		return nil, ErrDedupeCapacity
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Deduper{
		globalCapacity:  global,
		perPeerCapacity: perPeer,
		retention:       retention,
		now:             now,
		entries:         make(map[string]dedupeEntry),
		peerCounts:      make(map[string]int),
	}, nil
}

// Check registers a new envelope or classifies a scoped message-ID replay.
// Authenticated sender + mesh + message_id is the complete duplicate key.
func (d *Deduper) Check(lane LaneID, envelope protocol.Envelope) (DedupeResult, error) {
	if d == nil {
		return "", ErrDedupeCapacity
	}
	// Authenticated binding is checked before digesting, pruning, or capacity.
	if err := lane.validateEnvelope(envelope); err != nil {
		return "", err
	}
	now := d.now()
	messageKey, peerKey := dedupeKeys(envelope)

	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLocked(now, false)
	if existing, ok := d.entries[messageKey]; ok {
		if existing.state == dedupeTerminal {
			return DedupeDuplicateTerminal, nil
		}
		return DedupeDuplicateInFlight, nil
	}
	if len(d.entries) >= d.globalCapacity || d.peerCounts[peerKey] >= d.perPeerCapacity {
		d.pruneLocked(now, true)
	}
	if len(d.entries) >= d.globalCapacity || d.peerCounts[peerKey] >= d.perPeerCapacity {
		return "", ErrDedupeCapacity
	}
	d.entries[messageKey] = dedupeEntry{
		peerKey: peerKey, meshID: envelope.MeshID,
		conversationID: envelope.ConversationID, recipient: envelope.Recipient,
		state: dedupeInFlight,
	}
	d.peerCounts[peerKey]++
	return DedupeNew, nil
}

// AbandonLane removes only in-flight identities owned by one authenticated
// sender lane after its peer lane becomes terminal. Terminal replay evidence
// is retained for the full dedupe window.
func (d *Deduper) AbandonLane(lane LaneID) int {
	if d == nil || !lane.verified {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	removed := 0
	for key, entry := range d.entries {
		if entry.state != dedupeInFlight || entry.meshID != lane.meshID ||
			entry.conversationID != lane.conversationID ||
			entry.peerKey != lane.meshID+"\x00"+lane.sender || entry.recipient != lane.recipient {
			continue
		}
		delete(d.entries, key)
		d.decrementPeerLocked(entry.peerKey)
		removed++
	}
	return removed
}

// RetirePeer removes all replay and in-flight identity owned by one peer that
// is absent from the current complete membership snapshot. A later re-add is
// new current authority and does not resurrect this retired state.
func (d *Deduper) RetirePeer(meshID, peerID string) int {
	if d == nil || meshID == "" || peerID == "" {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	peerKey := meshID + "\x00" + peerID
	removed := 0
	for key, entry := range d.entries {
		if entry.meshID != meshID || entry.peerKey != peerKey {
			continue
		}
		delete(d.entries, key)
		d.decrementPeerLocked(entry.peerKey)
		removed++
	}
	return removed
}

// MarkTerminal retains trusted terminal identity for the full replay window.
func (d *Deduper) MarkTerminal(lane LaneID, envelope protocol.Envelope) error {
	if d == nil {
		return ErrUnknownMessage
	}
	if err := lane.validateEnvelope(envelope); err != nil {
		return err
	}
	now := d.now()
	messageKey, _ := dedupeKeys(envelope)
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.entries[messageKey]
	if !ok {
		return ErrUnknownMessage
	}
	entry.state = dedupeTerminal
	entry.expiresAt = now.Add(d.retention)
	d.entries[messageKey] = entry
	return nil
}

// Remove abandons one in-flight registration after trusted local failure. A
// terminal/delivered entry cannot be removed early because that would reopen a
// duplicate-delivery window.
func (d *Deduper) Remove(lane LaneID, envelope protocol.Envelope) error {
	if d == nil {
		return ErrUnknownMessage
	}
	if err := lane.validateEnvelope(envelope); err != nil {
		return err
	}
	messageKey, _ := dedupeKeys(envelope)
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.entries[messageKey]
	if !ok {
		return ErrUnknownMessage
	}
	if entry.state == dedupeTerminal {
		return ErrMessageConflict
	}
	delete(d.entries, messageKey)
	d.decrementPeerLocked(entry.peerKey)
	return nil
}

// Prune removes expired terminal entries. In-flight registrations remain until
// trusted terminal handling or explicit local abandonment; capacity fails
// closed instead of allowing a slow handler to be invoked twice.
func (d *Deduper) Prune(now time.Time) int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pruneLocked(now, true)
}

func (d *Deduper) pruneLocked(now time.Time, force bool) int {
	if !force && !d.nextPrune.IsZero() && now.Before(d.nextPrune) {
		return 0
	}
	removed := 0
	for key, entry := range d.entries {
		if entry.state == dedupeTerminal && !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
			delete(d.entries, key)
			d.decrementPeerLocked(entry.peerKey)
			removed++
		}
	}
	d.nextPrune = now.Add(time.Second)
	return removed
}

func (d *Deduper) decrementPeerLocked(peerKey string) {
	remaining := d.peerCounts[peerKey] - 1
	if remaining <= 0 {
		delete(d.peerCounts, peerKey)
		return
	}
	d.peerCounts[peerKey] = remaining
}

func dedupeKeys(envelope protocol.Envelope) (messageKey, peerKey string) {
	peerKey = envelope.MeshID + "\x00" + envelope.Sender
	messageKey = peerKey + "\x00" + envelope.MessageID
	return messageKey, peerKey
}

func (d *Deduper) Len() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}
