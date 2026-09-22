package v1

import "time"

// EventName identifies one allowlisted public event payload type.
type EventName string

const (
	EventSessionStateChanged      EventName = "session.state_changed"
	EventConnectivityStateChanged EventName = "connectivity.state_changed"
	EventPeerReachable            EventName = "peer.reachable"
	EventPeerUnreachable          EventName = "peer.unreachable"
	EventMessageReceived          EventName = "message.received"
	EventMessageQueued            EventName = "message.queued"
	EventMessageRetried           EventName = "message.retried"
	EventMessageDeduplicated      EventName = "message.deduplicated"
	EventDeliveryResumed          EventName = "delivery.resumed"
	EventDeliveryReplayed         EventName = "delivery.replayed"
	EventPayloadTransferStarted   EventName = "payload.transfer_started"
	EventPayloadTransferProgress  EventName = "payload.transfer_progress"
	EventPayloadTransferCompleted EventName = "payload.transfer_completed"
	EventPayloadTransferFailed    EventName = "payload.transfer_failed"
	EventPolicyRejected           EventName = "policy.rejected"
	EventRPCTimeout               EventName = "rpc.timeout"
	EventCommandQueueFull         EventName = "command.queue_full"
	EventEventQueueFull           EventName = "event.queue_full"
	EventCoreError                EventName = "core.error"
	EventDiagnosticLog            EventName = "diagnostics.log"
)

// EventPayload is the closed marker interface for versioned public event payloads.
type EventPayload interface {
	eventPayloadType()
}

// Event wraps one typed public event without a catch-all metadata map.
type Event struct {
	ID        EventID
	Name      EventName
	CreatedAt time.Time
	Payload   EventPayload
}

// EventResult is the pull-polling projection of the same normalized event
// queue consumed by push callbacks. It does not create a second delivery queue.
type EventResult struct {
	Event Event
}

func (EventResult) resultType() {}

// CoreErrorEvent reports a normalized runtime failure.
type CoreErrorEvent struct {
	Error Error
}

// eventPayloadType keeps event payload construction inside the versioned API contract.
func (CoreErrorEvent) eventPayloadType() {
}
