package rank2xmpp

import (
	"context"
	"encoding/xml"
	"io"
	"strings"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

type issuedJingleState uint8

const (
	issuedJingleActive issuedJingleState = iota + 1
	issuedJingleTombstone
)

// issuedJingle is payload-free transport correlation state. Active entries are
// bounded separately from late-response tombstones. A tombstone intentionally
// outlives both an ExchangeJingle waiter and an XMPP stream generation, while
// still authenticating only the exact ID and endpoint pair originally issued.
type issuedJingle struct {
	from      string
	to        string
	state     issuedJingleState
	responded bool
}

func (s *melliumSession) registerIssuedJingle(record Stanza, tracked bool) error {
	if s == nil || record.Kind != StanzaSignal || record.AttemptID == "" ||
		record.MeshID == "" || !validJingleBoundIdentity(record.From, record.MeshID) ||
		!validJingleBoundIdentity(record.To, record.MeshID) || record.From == record.To {
		return ErrProtocol
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if current, ok := s.issuedJingles[record.AttemptID]; ok {
		if current.from != record.From || current.to != record.To {
			return ErrUnavailable
		}
		if !tracked {
			return nil
		}
		// A Jingle SID identifies the negotiated session, not one individual
		// action. The same endpoint therefore legitimately publishes
		// session-accept, transport-info, and later restart actions under the
		// same SID. Admit a new wire issuance only after the preceding one has
		// quiesced; a concurrent duplicate remains ambiguous and is rejected.
		if current.state != issuedJingleTombstone {
			return ErrUnavailable
		}
		if s.issuedJingleCapacity <= 0 || s.issuedJingleActive >= s.issuedJingleCapacity {
			return ErrQueueFull
		}
		s.removeIssuedJingleTombstoneLocked(record.AttemptID)
		current.state = issuedJingleActive
		current.responded = false
		s.issuedJingles[record.AttemptID] = current
		s.issuedJingleActive++
		return nil
	}
	if !tracked {
		return ErrStreamManagement
	}
	if s.issuedJingleCapacity <= 0 || s.issuedJingleActive >= s.issuedJingleCapacity {
		return ErrQueueFull
	}
	if s.issuedJingles == nil {
		s.issuedJingles = make(map[string]issuedJingle)
	}
	s.issuedJingles[strings.Clone(record.AttemptID)] = issuedJingle{
		from: strings.Clone(record.From), to: strings.Clone(record.To), state: issuedJingleActive,
	}
	s.issuedJingleActive++
	return nil
}

func (s *melliumSession) lookupIssuedJingle(attemptID string) (issuedJingle, bool) {
	if s == nil || attemptID == "" {
		return issuedJingle{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	issued, ok := s.issuedJingles[attemptID]
	return issued, ok
}

// quiesceIssuedJingle converts an in-progress publication into bounded
// late-response state. It is called after every wire attempt, including
// cancellation and ambiguous failure, so abandoned exchanges cannot pin active
// capacity forever. Exact replays remain recognizable through the tombstone.
func (s *melliumSession) quiesceIssuedJingle(attemptID, from, to string) bool {
	if s == nil || attemptID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	issued, ok := s.issuedJingles[attemptID]
	if !ok || issued.from != from || issued.to != to {
		return false
	}
	s.quiesceIssuedJingleLocked(attemptID, issued)
	return true
}

func (s *melliumSession) quiesceIssuedJingleLocked(attemptID string, issued issuedJingle) {
	if issued.state == issuedJingleTombstone {
		return
	}
	issued.state = issuedJingleTombstone
	s.issuedJingles[attemptID] = issued
	if s.issuedJingleActive > 0 {
		s.issuedJingleActive--
	}
	s.issuedJingleTombstones = append(s.issuedJingleTombstones, attemptID)
	for len(s.issuedJingleTombstones) > s.issuedJingleCapacity {
		oldest := s.issuedJingleTombstones[0]
		copy(s.issuedJingleTombstones, s.issuedJingleTombstones[1:])
		s.issuedJingleTombstones[len(s.issuedJingleTombstones)-1] = ""
		s.issuedJingleTombstones = s.issuedJingleTombstones[:len(s.issuedJingleTombstones)-1]
		if old, exists := s.issuedJingles[oldest]; exists && old.state == issuedJingleTombstone {
			delete(s.issuedJingles, oldest)
		}
	}
}

// removeIssuedJingleTombstoneLocked removes the queue position owned by an
// exact SID before that SID is reactivated for a later Jingle action. Keeping
// the stale queue position would let unrelated tombstone eviction delete the
// newly active correlation entry.
func (s *melliumSession) removeIssuedJingleTombstoneLocked(attemptID string) {
	for index, candidate := range s.issuedJingleTombstones {
		if candidate != attemptID {
			continue
		}
		copy(s.issuedJingleTombstones[index:], s.issuedJingleTombstones[index+1:])
		last := len(s.issuedJingleTombstones) - 1
		s.issuedJingleTombstones[last] = ""
		s.issuedJingleTombstones = s.issuedJingleTombstones[:last]
		return
	}
}

// completeIssuedJingle authenticates and records an exact response without
// deleting its tombstone. This makes duplicate delivery idempotent and keeps a
// replay of the same unacknowledged IQ recognizable. first is false for an
// already consumed response, allowing callers to suppress duplicate events.
func (s *melliumSession) completeIssuedJingle(attemptID string, expected issuedJingle) (first, ok bool) {
	if s == nil || attemptID == "" {
		return false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	issued, exists := s.issuedJingles[attemptID]
	if !exists || issued.from != expected.from || issued.to != expected.to {
		return false, false
	}
	s.quiesceIssuedJingleLocked(attemptID, issued)
	issued = s.issuedJingles[attemptID]
	first = !issued.responded
	issued.responded = true
	s.issuedJingles[attemptID] = issued
	return first, true
}

func (s *melliumSession) clearIssuedJinglesLocked() {
	for id := range s.issuedJingles {
		delete(s.issuedJingles, id)
	}
	s.issuedJingleActive = 0
	clear(s.issuedJingleTombstones)
	s.issuedJingleTombstones = nil
}

func (s *melliumSession) handleJingleIQResult(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ) error {
	if s == nil || ctx == nil || source == nil || iq.ID == "" {
		return ErrProtocol
	}
	issued, ok := s.lookupIssuedJingle(iq.ID)
	if !ok {
		return ErrProtocol
	}
	peer, peerErr := jid.Parse(issued.to)
	local, localErr := jid.Parse(issued.from)
	if peerErr != nil || localErr != nil || iq.From.String() != peer.String() || iq.To.String() != local.String() {
		emitInboundAuthRejection(ctx, outer, "jingle_result_address")
		return ErrAuthentication
	}
	if iqType, attrsOK := validCorrelatedIQAttrs(outer.Attr, iq.ID, peer, local); !attrsOK || iqType != stanza.ResultIQ {
		return ErrProtocol
	}
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, maximumJingleIQErrorBytes)
	if err != nil {
		return ErrProtocol
	}
	decoder := xml.NewTokenDecoder(bounded)
	for {
		token, readErr := decoder.Token()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return ErrProtocol
		}
		if chars, whitespace := token.(xml.CharData); !whitespace || strings.TrimSpace(string(chars)) != "" {
			return ErrProtocol
		}
	}
	if !children.done {
		return ErrProtocol
	}
	s.mu.Lock()
	management, generation := s.management, s.generation
	s.mu.Unlock()
	contextGeneration, generationOK := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	if management == nil || !generationOK || contextGeneration == 0 || contextGeneration != generation {
		return ErrStreamManagement
	}
	if _, ok = s.completeIssuedJingle(iq.ID, issued); !ok {
		return ErrProtocol
	}
	management.MarkHandledInbound()
	return nil
}
