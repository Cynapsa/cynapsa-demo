package rank2xmpp

import (
	"context"
	"sync"
	"time"
)

// inboundAcceptance is a payload-free capability for one deferred inbound
// stream position. StreamManagement owns its ordered queue entry; copied
// capabilities converge on one terminal decision through decisionOnce.
type inboundAcceptance struct {
	decisionOnce sync.Once
	accepted     bool
	management   *StreamManagement
	sequence     uint32
	onReject     func()

	// resolved and rejected are protected by management.mu. They are kept on
	// the capability so the ordered ledger needs only one pointer per deferred
	// envelope and never retains application payload bytes.
	resolved bool
	rejected bool
}

func (acceptance *inboundAcceptance) decide(accepted bool) bool {
	if acceptance == nil {
		return false
	}
	acceptance.decisionOnce.Do(func() {
		var reject func()
		if acceptance.management != nil {
			acceptance.accepted, reject = acceptance.management.resolveDeferredInbound(acceptance, accepted)
		}
		if reject != nil {
			reject()
		}
	})
	return acceptance.accepted
}

func (acceptance *inboundAcceptance) accept(ctx context.Context) error {
	if acceptance == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidConfig
	}
	// MessagingService has already committed terminal SDK ownership before it
	// calls Accept. Caller cancellation can stop later pump work, but cannot
	// roll back that ownership or turn this exact stanza back into a rejection.
	// Publish the irreversible decision without waiting for socket progress.
	// The ordered StreamManagement ledger advances h across the contiguous
	// accepted prefix; later accepted envelopes and controls cannot overtake an
	// unresolved predecessor.
	if !acceptance.decide(true) {
		return ErrUnavailable
	}
	return nil
}

func (acceptance *inboundAcceptance) reject() { _ = acceptance.decide(false) }

// rejectInboundGeneration fail-stops only the stream generation which issued
// a rejected acceptance capability. A late rejection from a retired event can
// never close a resumed or clean replacement connection.
func (s *melliumSession) rejectInboundGeneration(management *StreamManagement, generation uint64) {
	if s == nil || management == nil || generation == 0 {
		return
	}
	s.mu.Lock()
	if s.closed || s.management != management || s.generation != generation {
		s.mu.Unlock()
		return
	}
	s.suspended = true
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.SetWriteDeadline(time.Now())
		_ = conn.Close()
	}
}
