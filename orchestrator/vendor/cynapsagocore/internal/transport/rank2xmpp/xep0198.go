package rank2xmpp

import (
	"math"
	"sync"
)

const (
	minimumRetainedStanzaBytes   int64 = 256
	MaximumStreamManagementBytes int64 = 256 << 20
)

// StreamManagement is the bounded AZTM-owned XEP-0198 accounting core. XML
// negotiation is performed by the Mellium session adapter.
type StreamManagement struct {
	mu             sync.Mutex
	capacity       int
	byteCapacity   int64
	pendingBytes   int64
	enabled        bool
	resumable      bool
	resumeID       string
	outbound       uint32
	acked          uint32
	serverAcked    uint32
	inbound        uint32
	inboundSeen    uint32
	inboundReject  bool
	inboundPending []*inboundAcceptance
	pending        []managedStanza
}
type managedStanza struct {
	stanza            Stanza
	sequence          uint32
	bytes             int64
	correlatedClaimed bool
}

func NewStreamManagement(capacity int, byteCapacity int64) (*StreamManagement, error) {
	if capacity <= 0 || capacity > 65536 || byteCapacity < minimumRetainedStanzaBytes || byteCapacity > MaximumStreamManagementBytes {
		return nil, ErrInvalidConfig
	}
	return &StreamManagement{capacity: capacity, byteCapacity: byteCapacity}, nil
}
func (sm *StreamManagement) Enable(resumeID string, resumable bool) error {
	if sm == nil || resumeID == "" || !resumable {
		return ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.enabled {
		return ErrProtocol
	}
	sm.enabled = true
	sm.resumable = true
	sm.resumeID = resumeID
	return nil
}
func (sm *StreamManagement) RecordSent(stanza Stanza) error {
	_, err := sm.RecordSentTracked(stanza)
	return err
}

// RecordSentTracked records a stanza and returns its exact XEP-0198 sequence.
// The sequence is used only by correlated protocol IQs whose result itself is
// authoritative evidence that the server handled every prior stream stanza.
func (sm *StreamManagement) RecordSentTracked(stanza Stanza) (uint32, error) {
	if sm == nil {
		return 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, ErrStreamManagement
	}
	if len(sm.pending) >= sm.capacity {
		return 0, ErrQueueFull
	}
	charge, ok := retainedStanzaBytes(stanza)
	if !ok || charge > sm.byteCapacity-sm.pendingBytes {
		return 0, ErrQueueFull
	}
	sm.outbound++
	sm.pending = append(sm.pending, managedStanza{stanza: stanza.streamManagementRecord(), sequence: sm.outbound, bytes: charge})
	sm.pendingBytes += charge
	return sm.outbound, nil
}

// ConfirmCorrelatedHandled advances the cumulative handled position through
// sequence after a strictly correlated IQ result has been validated. XMPP
// stream ordering makes that result proof that all prior stanzas were handled.
// A preceding XEP-0198 acknowledgement of the same sequence is idempotent.
func (sm *StreamManagement) ConfirmCorrelatedHandled(sequence uint32) (uint64, uint32, error) {
	if sm == nil {
		return 0, 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, 0, ErrStreamManagement
	}
	index := -1
	for i := range sm.pending {
		if sm.pending[i].sequence == sequence {
			index = i
			break
		}
	}
	if index < 0 {
		// With a bounded pending window, a sequence at or behind acked has
		// already been cumulatively acknowledged. Anything ahead is invalid.
		if uint32(sm.acked-sequence) < 1<<31 {
			return 0, 0, nil
		}
		return 0, 0, ErrProtocol
	}
	var ordinal uint64
	for _, entry := range sm.pending[:index+1] {
		if entry.stanza.Ordinal > ordinal {
			ordinal = entry.stanza.Ordinal
		}
		clear(entry.stanza.Data)
		sm.pendingBytes -= entry.bytes
	}
	count := uint32(index + 1)
	sm.pending = append([]managedStanza(nil), sm.pending[index+1:]...)
	sm.acked = sequence
	return ordinal, count, nil
}

// ConfirmCorrelatedHandledRecord applies the same cumulative proof using an
// exact retained request descriptor. It is used by a resumed parser after the
// original caller and Mellium IQ waiter were retired with the old socket.
func (sm *StreamManagement) ConfirmCorrelatedHandledRecord(record Stanza) (uint64, uint32, error) {
	if sm == nil || !isSessionControlStanza(record.Kind) {
		return 0, 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, 0, ErrStreamManagement
	}
	index := -1
	for candidate := range sm.pending {
		if !sameManagedStanza(sm.pending[candidate].stanza, record) {
			continue
		}
		if index >= 0 {
			return 0, 0, ErrProtocol
		}
		index = candidate
	}
	if index < 0 {
		return 0, 0, nil
	}
	var ordinal uint64
	for _, entry := range sm.pending[:index+1] {
		if entry.stanza.Ordinal > ordinal {
			ordinal = entry.stanza.Ordinal
		}
		clear(entry.stanza.Data)
		sm.pendingBytes -= entry.bytes
	}
	count := uint32(index + 1)
	sm.acked = sm.pending[index].sequence
	sm.pending = append([]managedStanza(nil), sm.pending[index+1:]...)
	return ordinal, count, nil
}

// ConfirmEnvelopeCustody advances the transient outbound ledger when the
// trusted Mesh Server positively confirms that this envelope's mailbox commit
// succeeded. If a preceding raw SM acknowledgement already removed the record,
// the confirmation remains valid and idempotent at this layer.
func (sm *StreamManagement) ConfirmEnvelopeCustody(messageID string) (uint64, uint32, error) {
	if sm == nil || messageID == "" {
		return 0, 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, 0, ErrStreamManagement
	}
	index := -1
	for i := range sm.pending {
		if sm.pending[i].stanza.Kind == StanzaEnvelope && sm.pending[i].stanza.MessageID == messageID {
			index = i
			break
		}
	}
	if index < 0 {
		return 0, 0, nil
	}
	var ordinal uint64
	for _, entry := range sm.pending[:index+1] {
		if entry.stanza.Ordinal > ordinal {
			ordinal = entry.stanza.Ordinal
		}
		clear(entry.stanza.Data)
		sm.pendingBytes -= entry.bytes
	}
	count := uint32(index + 1)
	sm.acked = sm.pending[index].sequence
	sm.pending = append([]managedStanza(nil), sm.pending[index+1:]...)
	return ordinal, count, nil
}

// EnvelopeCustodyPosition validates a custody receipt against the current
// ledger without mutating it. The adapter uses this preview so a failed event
// handoff cannot erase replay metadata before Core observes the cumulative
// envelope ordinal.
func (sm *StreamManagement) EnvelopeCustodyPosition(messageID string) (uint64, uint32, error) {
	if sm == nil || messageID == "" {
		return 0, 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, 0, ErrStreamManagement
	}
	index := -1
	for i := range sm.pending {
		if sm.pending[i].stanza.Kind == StanzaEnvelope && sm.pending[i].stanza.MessageID == messageID {
			index = i
			break
		}
	}
	if index < 0 {
		return 0, 0, nil
	}
	var ordinal uint64
	for _, entry := range sm.pending[:index+1] {
		if entry.stanza.Ordinal > ordinal {
			ordinal = entry.stanza.Ordinal
		}
	}
	return ordinal, uint32(index + 1), nil
}

// RollbackLast removes a stanza whose serialized write failed before Send
// committed it. Callers serialize RecordSent/write/RollbackLast, so only the
// exact most recently admitted stanza can be removed.
func (sm *StreamManagement) RollbackLast(stanza Stanza) error {
	if sm == nil {
		return ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled || len(sm.pending) == 0 {
		return ErrProtocol
	}
	last := sm.pending[len(sm.pending)-1]
	if !sameManagedStanza(last.stanza, stanza) || last.sequence != sm.outbound {
		return ErrProtocol
	}
	clear(last.stanza.Data)
	sm.pendingBytes -= last.bytes
	sm.pending = sm.pending[:len(sm.pending)-1]
	sm.outbound--
	return nil
}

func sameManagedStanza(a, b Stanza) bool {
	if a.Kind != b.Kind || a.From != b.From || a.To != b.To || a.MeshID != b.MeshID || a.Ordinal != b.Ordinal || a.AttemptID != b.AttemptID || a.TransferID != b.TransferID || a.MessageID != b.MessageID || a.Evidence != b.Evidence {
		return false
	}
	if a.Kind == StanzaEnvelope {
		return true
	}
	if len(a.Data) != len(b.Data) {
		return false
	}
	for i := range a.Data {
		if a.Data[i] != b.Data[i] {
			return false
		}
	}
	return true
}

// ApplyAck accepts only forward modulo-2^32 progress within the exact pending
// count and returns the greatest local envelope ordinal proven handled.
func (sm *StreamManagement) ApplyAck(handled uint32) (uint64, error) {
	ordinal, _, err := sm.applyAck(handled)
	return ordinal, err
}

func (sm *StreamManagement) ApplyAckDetailed(handled uint32) (uint64, uint32, error) {
	return sm.applyAck(handled)
}

func (sm *StreamManagement) applyAck(handled uint32) (uint64, uint32, error) {
	if sm == nil {
		return 0, 0, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, 0, ErrStreamManagement
	}
	// Custody receipts and strictly correlated IQ results can prove a prefix
	// handled before ejabberd's asynchronously queued XEP-0198 acknowledgement
	// for an earlier prefix reaches this stream. Keep the server-reported
	// watermark separate so that earlier-but-monotonic wire evidence is
	// idempotent instead of looking like a uint32-wrapped forward jump.
	serverDelta := uint32(handled - sm.serverAcked)
	if serverDelta >= 1<<31 {
		emitRank2Evidence(rank2EvidenceRecord{Event: "sm_ack_rejected", Source: "sm", Stage: "server_watermark", WireGeneration: uint64(handled), ClientGeneration: uint64(sm.outbound), StateEpoch: uint64(sm.acked), SessionEpoch: uint64(sm.serverAcked), Attempt: len(sm.pending)}, ErrProtocol)
		return 0, 0, ErrProtocol
	}
	delta := uint32(handled - sm.acked)
	if delta >= 1<<31 {
		sm.serverAcked = handled
		return 0, 0, nil
	}
	if uint64(delta) > uint64(len(sm.pending)) {
		emitRank2Evidence(rank2EvidenceRecord{Event: "sm_ack_rejected", Source: "sm", Stage: "untracked_outbound", WireGeneration: uint64(handled), ClientGeneration: uint64(sm.outbound), StateEpoch: uint64(sm.acked), SessionEpoch: uint64(sm.serverAcked), Attempt: len(sm.pending)}, ErrProtocol)
		return 0, 0, ErrProtocol
	}
	var ordinal uint64
	for _, entry := range sm.pending[:int(delta)] {
		if entry.stanza.Ordinal > ordinal {
			ordinal = entry.stanza.Ordinal
		}
		clear(entry.stanza.Data)
		sm.pendingBytes -= entry.bytes
	}
	sm.pending = append([]managedStanza(nil), sm.pending[int(delta):]...)
	sm.acked = handled
	sm.serverAcked = handled
	return ordinal, delta, nil
}

// DeferHandledInbound assigns one wire-order position to an application
// envelope without advancing h. Only unresolved envelopes consume bounded
// queue entries; immediately handled controls are represented by inboundSeen
// and therefore cannot exhaust this queue while an application is blocked.
func (sm *StreamManagement) DeferHandledInbound(onReject func()) (*inboundAcceptance, error) {
	if sm == nil {
		return nil, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled || sm.inboundReject {
		return nil, ErrStreamManagement
	}
	if len(sm.inboundPending) >= sm.capacity {
		return nil, ErrQueueFull
	}
	sm.inboundSeen++
	acceptance := &inboundAcceptance{
		management: sm,
		sequence:   sm.inboundSeen,
		onReject:   onReject,
	}
	sm.inboundPending = append(sm.inboundPending, acceptance)
	return acceptance, nil
}

// MarkHandledInbound records one immediately handled stanza in wire order.
// If a deferred predecessor exists, h remains immediately before that first
// unresolved position even though this later control stanza is complete.
func (sm *StreamManagement) MarkHandledInbound() uint32 {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled || sm.inboundReject {
		return sm.inbound
	}
	sm.inboundSeen++
	sm.advanceInboundLocked()
	return sm.inbound
}

// resolveDeferredInbound applies the exact once-only SDK decision. It returns
// true only for an accepted capability still owned by this ledger. Rejection
// permanently fences resumption and returns the exact-generation fail-stop
// callback for invocation outside the stream-management lock.
func (sm *StreamManagement) resolveDeferredInbound(acceptance *inboundAcceptance, accepted bool) (bool, func()) {
	if sm == nil || acceptance == nil {
		return false, nil
	}
	sm.mu.Lock()
	index := -1
	for i, pending := range sm.inboundPending {
		if pending == acceptance {
			index = i
			break
		}
	}
	if !sm.enabled || index < 0 || acceptance.resolved {
		sm.mu.Unlock()
		return false, nil
	}
	acceptance.resolved = true
	acceptance.rejected = !accepted
	if !accepted {
		sm.inboundReject = true
	}
	sm.advanceInboundLocked()
	reject := acceptance.onReject
	sm.mu.Unlock()
	if accepted {
		return true, nil
	}
	return false, reject
}

func (sm *StreamManagement) advanceInboundLocked() {
	for len(sm.inboundPending) > 0 && sm.inboundPending[0].resolved && !sm.inboundPending[0].rejected {
		sm.inboundPending[0] = nil
		sm.inboundPending = sm.inboundPending[1:]
	}
	if len(sm.inboundPending) == 0 {
		sm.inbound = sm.inboundSeen
		return
	}
	sm.inbound = sm.inboundPending[0].sequence - 1
}

// HandledInbound reports h. XEP-0198 nonzas such as <r/> are not stanzas and
// must never advance it.
func (sm *StreamManagement) HandledInbound() uint32 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.inbound
}
func (sm *StreamManagement) ResumeState() (string, uint32, bool) {
	if sm == nil {
		return "", 0, false
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.resumeID, sm.inbound, sm.enabled && sm.resumable && !sm.inboundReject
}
func (sm *StreamManagement) ResumeAccepted(serverHandled uint32) ([]Stanza, uint64, error) {
	ordinal, err := sm.ApplyAck(serverHandled)
	if err != nil {
		return nil, 0, err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	replay := make([]Stanza, len(sm.pending))
	for i, entry := range sm.pending {
		replay[i] = entry.stanza.clone()
	}
	return replay, ordinal, nil
}
func (sm *StreamManagement) ResumeRejected() []Stanza {
	if sm == nil {
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	replay := make([]Stanza, len(sm.pending))
	for i, entry := range sm.pending {
		// Move the sole retained Data ownership into replay. The rejected ledger
		// no longer retains or charges these bytes.
		replay[i] = entry.stanza
		sm.pending[i].stanza = Stanza{}
	}
	sm.enabled = false
	sm.resumable = false
	sm.resumeID = ""
	sm.outbound = 0
	sm.acked = 0
	sm.serverAcked = 0
	sm.inbound = 0
	sm.inboundSeen = 0
	sm.inboundReject = false
	sm.inboundPending = nil
	sm.pending = nil
	sm.pendingBytes = 0
	return replay
}
func (sm *StreamManagement) Pending() int {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.pending)
}

func (sm *StreamManagement) PendingBytes() int64 {
	if sm == nil {
		return 0
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.pendingBytes
}

func (sm *StreamManagement) PendingSnapshot() []Stanza {
	if sm == nil {
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	result := make([]Stanza, len(sm.pending))
	for i, entry := range sm.pending {
		result[i] = entry.stanza.clone()
	}
	return result
}

func (sm *StreamManagement) pendingContains(record Stanza) bool {
	if sm == nil {
		return false
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for index := range sm.pending {
		if sameManagedStanza(sm.pending[index].stanza, record) {
			return true
		}
	}
	return false
}

// claimPendingCorrelatedSequence assigns one late IQ result to the exact
// session-control request still retained by XEP-0198. The claim is one-use:
// concurrent or duplicate delivery cannot advance inbound h twice, while a
// claim interrupted by session replacement remains a pending control fence.
func (sm *StreamManagement) claimPendingCorrelatedSequence(record Stanza) (uint32, bool, error) {
	if sm == nil || !isSessionControlStanza(record.Kind) || record.MessageID == "" {
		return 0, false, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return 0, false, ErrStreamManagement
	}
	index := -1
	for candidate := range sm.pending {
		entry := sm.pending[candidate]
		if !sameManagedStanza(entry.stanza, record) {
			continue
		}
		if index >= 0 {
			return 0, false, ErrProtocol
		}
		index = candidate
	}
	if index < 0 || sm.pending[index].correlatedClaimed {
		return 0, false, nil
	}
	sm.pending[index].correlatedClaimed = true
	return sm.pending[index].sequence, true, nil
}

// Close terminally destroys the stream-management ledger. It is distinct
// from a resumable transport disconnect: only the owning session's explicit
// terminal Close invokes it. Repeated calls are idempotent.
func (sm *StreamManagement) Close() {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i := range sm.pending {
		clear(sm.pending[i].stanza.Data)
		sm.pending[i] = managedStanza{}
	}
	sm.pending = nil
	sm.pendingBytes = 0
	sm.enabled = false
	sm.resumable = false
	sm.resumeID = ""
	sm.outbound = 0
	sm.acked = 0
	sm.serverAcked = 0
	sm.inbound = 0
	sm.inboundSeen = 0
	sm.inboundReject = false
	sm.inboundPending = nil
}

func retainedStanzaBytes(stanza Stanza) (int64, bool) {
	// Include a conservative fixed allowance for the retained struct, slice
	// header, and digest, then every variable byte owned by the clone. Envelope
	// records retain only metadata; the immutable application bytes live in the
	// shared Core outbox until explicit server custody retires them.
	total := minimumRetainedStanzaBytes
	dataLen := len(stanza.Data)
	if stanza.Kind == StanzaEnvelope {
		dataLen = 0
	}
	for _, size := range []int{
		len(stanza.From), len(stanza.To), len(stanza.MeshID), len(stanza.AttemptID),
		len(stanza.TransferID), len(stanza.MessageID), dataLen,
		len(stanza.Evidence.TransferID), len(stanza.Evidence.MessageID),
	} {
		if size < 0 || int64(size) > math.MaxInt64-total {
			return 0, false
		}
		total += int64(size)
	}
	return total, true
}

// pendingKindAfterAck validates a prospective resumed h without mutating the
// old ledger and reports whether that h would leave kind unacknowledged. This
// lets control-plane fences force a clean session while retaining the complete
// authoritative application order for duplicate-safe replay.
func (sm *StreamManagement) pendingKindAfterAck(handled uint32, kind StanzaKind) (bool, error) {
	if sm == nil {
		return false, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return false, ErrStreamManagement
	}
	delta := uint32(handled - sm.acked)
	if uint64(delta) > uint64(len(sm.pending)) {
		return false, ErrProtocol
	}
	for _, entry := range sm.pending[int(delta):] {
		if entry.stanza.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

// pendingControlAfterAck reports whether a prospective resumed h would leave
// any non-replayable session-local correlated IQ marker ambiguous. The sole
// replay-safe control is an exact XEP-0199 result for the authenticated server
// route; every other session control continues to fence resumption.
func (sm *StreamManagement) pendingControlAfterAck(handled uint32, local, server, meshID string) (bool, error) {
	if sm == nil {
		return false, ErrStreamManagement
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.enabled {
		return false, ErrStreamManagement
	}
	delta := uint32(handled - sm.acked)
	if uint64(delta) > uint64(len(sm.pending)) {
		return false, ErrProtocol
	}
	pingIDs := make(map[string]struct{})
	externalIDs := make(map[string]struct{})
	for _, entry := range sm.pending[int(delta):] {
		if isSessionControlStanza(entry.stanza.Kind) {
			if validSessionPingResultReplay(entry.stanza, local, server, meshID) {
				if _, duplicate := pingIDs[entry.stanza.AttemptID]; duplicate {
					return true, nil
				}
				pingIDs[entry.stanza.AttemptID] = struct{}{}
				continue
			}
			if validExternalServiceQueryReplay(entry.stanza, local, server, meshID) {
				if _, duplicate := externalIDs[entry.stanza.MessageID]; duplicate {
					return true, nil
				}
				externalIDs[entry.stanza.MessageID] = struct{}{}
				continue
			}
			return true, nil
		}
	}
	return false, nil
}
