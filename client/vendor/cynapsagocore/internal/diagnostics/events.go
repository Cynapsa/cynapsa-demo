// Package diagnostics owns private diagnostics and support-safe snapshots.
package diagnostics

import (
	"errors"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var (
	ErrInvalidEvent  = errors.New("diagnostics: invalid event")
	ErrUnknownMetric = errors.New("diagnostics: unknown metric")
	ErrInvalidMetric = errors.New("diagnostics: invalid metric value")
)

// Event constructs a typed private event. The name and closed value variant
// must agree; arbitrary diagnostic payloads are rejected.
func Event(id, name string, createdAt time.Time, value model.EventValue) (model.Event, error) {
	if id == "" || name == "" || createdAt.IsZero() || !validEventPair(name, value) {
		return model.Event{}, ErrInvalidEvent
	}
	return model.Event{ID: id, Name: name, CreatedAt: createdAt, Value: value}, nil
}

func validEventPair(name string, value model.EventValue) bool {
	switch value.(type) {
	case model.SessionStateChangedEvent:
		return name == "session.state_changed"
	case model.ConnectivityChangedEvent:
		return name == "connectivity.state_changed"
	case model.PeerReachabilityEvent:
		return name == "peer.reachable" || name == "peer.unreachable"
	case model.MessageReceivedEvent:
		return name == "message.received"
	case model.MessageStateEvent:
		return name == "message.queued" || name == "message.retried" || name == "message.deduplicated" || name == "delivery.resumed" || name == "delivery.replayed"
	case model.PayloadTransferEvent:
		return name == "payload.transfer_started" || name == "payload.transfer_progress" || name == "payload.transfer_completed" || name == "payload.transfer_failed"
	case model.PolicyRejectedEvent:
		return name == "policy.rejected"
	case model.RPCTimeoutEvent:
		return name == "rpc.timeout"
	case model.QueueCapacityEvent:
		return name == "command.queue_full" || name == "event.queue_full"
	case model.CoreErrorEvent:
		return name == "core.error"
	case model.DiagnosticLogEvent:
		return name == "diagnostics.log"
	default:
		return false
	}
}
