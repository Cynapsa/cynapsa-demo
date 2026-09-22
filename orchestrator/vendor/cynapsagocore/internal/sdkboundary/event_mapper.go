package sdkboundary

import (
	"fmt"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapEvent explicitly projects one allowlisted semantic event.
func (a *Adapter) MapEvent(event model.Event) (v1.Event, error) {
	if err := validateProjectedIdentifier("event_id", event.ID); err != nil || event.CreatedAt.IsZero() || event.Value == nil {
		return v1.Event{}, fmt.Errorf("%w: incomplete internal event", ErrMalformedInput)
	}
	name := v1.EventName(event.Name)
	payload, err := a.mapEventValue(name, event.Value)
	if err != nil {
		return v1.Event{}, err
	}
	return v1.Event{ID: v1.EventID(event.ID), Name: name, CreatedAt: event.CreatedAt, Payload: payload}, nil
}

func (a *Adapter) mapEventValue(name v1.EventName, input model.EventValue) (v1.EventPayload, error) {
	switch value := input.(type) {
	case model.SessionStateChangedEvent:
		if name != v1.EventSessionStateChanged {
			break
		}
		return v1.SessionStateChangedEvent{Previous: publicLifecycle(value.Previous), Current: publicLifecycle(value.Current)}, nil
	case model.ConnectivityChangedEvent:
		if name != v1.EventConnectivityStateChanged {
			break
		}
		return v1.ConnectivityChangedEvent{Previous: publicConnectivity(value.Previous), Current: publicConnectivity(value.Current)}, nil
	case model.PeerReachabilityEvent:
		if name != v1.EventPeerReachable && name != v1.EventPeerUnreachable {
			break
		}
		if (name == v1.EventPeerReachable) != value.Reachable {
			return nil, fmt.Errorf("%w: reachability mismatch", ErrMalformedInput)
		}
		if err := validateProjectedIdentifier("peer", value.Peer); err != nil {
			return nil, err
		}
		return v1.PeerReachabilityEvent{Peer: v1.AgentID(value.Peer), Reachable: value.Reachable}, nil
	case model.MessageReceivedEvent:
		if name != v1.EventMessageReceived {
			break
		}
		mode := v1.MessageMode(value.Mode)
		if mode != v1.MessageModeOneWay && mode != v1.MessageModeRequest {
			return nil, fmt.Errorf("%w: unknown message mode", ErrMalformedInput)
		}
		if (mode == v1.MessageModeRequest && value.RequestHandle == "") ||
			(mode == v1.MessageModeOneWay && value.RequestHandle != "") {
			return nil, fmt.Errorf("%w: request handle mismatch", ErrMalformedInput)
		}
		for field, identifier := range map[string]string{"message_id": value.MessageID, "conversation_id": value.ConversationID, "from_agent_id": value.FromAgentID, "mesh_id": value.MeshID} {
			if err := validateProjectedIdentifier(field, identifier); err != nil {
				return nil, err
			}
		}
		if mode == v1.MessageModeRequest {
			if err := validateRequestHandle(v1.RequestHandle(value.RequestHandle)); err != nil {
				return nil, err
			}
		}
		payload, err := a.MapPayload(value.Payload)
		if err != nil {
			return nil, err
		}
		return v1.MessageReceivedEvent{MessageID: v1.MessageID(value.MessageID), ConversationID: v1.ConversationID(value.ConversationID), FromAgentID: v1.AgentID(value.FromAgentID), MeshID: v1.MeshID(value.MeshID), Mode: mode, RequestHandle: v1.RequestHandle(value.RequestHandle), Payload: payload}, nil
	case model.MessageStateEvent:
		if expected, known := expectedMessageEventState(name); known {
			state, ok := publicDeliveryState(value.State)
			if !ok || state != expected {
				return nil, fmt.Errorf("%w: message event state mismatch", ErrMalformedInput)
			}
			if err := validateProjectedIdentifier("message_id", value.MessageID); err != nil {
				return nil, err
			}
			if err := validateProjectedIdentifier("conversation_id", value.ConversationID); err != nil {
				return nil, err
			}
			return v1.MessageStateEvent{MessageID: v1.MessageID(value.MessageID), ConversationID: v1.ConversationID(value.ConversationID), State: state}, nil
		}
	case model.PayloadTransferEvent:
		if expected, known := expectedPayloadEventState(name); known {
			if value.Completed > value.Total || value.Total > maxCanonicalPayloadSize {
				return nil, fmt.Errorf("%w: invalid payload progress", ErrMalformedInput)
			}
			state, ok := publicPayloadTransferState(value.State)
			if !ok || state != expected {
				return nil, fmt.Errorf("%w: payload event state mismatch", ErrMalformedInput)
			}
			if err := validatePayloadHandle(v1.PayloadHandle(value.Handle)); err != nil {
				return nil, err
			}
			return v1.PayloadTransferEvent{Handle: v1.PayloadHandle(value.Handle), State: state, Completed: value.Completed, Total: value.Total}, nil
		}
	case model.PolicyRejectedEvent:
		if name != v1.EventPolicyRejected {
			break
		}
		if err := validateProjectedIdentifier("message_id", value.MessageID); err != nil {
			return nil, err
		}
		if err := validateProjectedIdentifier("peer", value.Peer); err != nil {
			return nil, err
		}
		if err := validatePath(value.Path); err != nil {
			return nil, err
		}
		return v1.PolicyRejectedEvent{MessageID: v1.MessageID(value.MessageID), Peer: v1.AgentID(value.Peer), Path: value.Path}, nil
	case model.RPCTimeoutEvent:
		if name != v1.EventRPCTimeout {
			break
		}
		if err := validateProjectedIdentifier("command_id", value.CommandID); err != nil {
			return nil, err
		}
		return v1.RPCTimeoutEvent{CommandID: v1.CommandID(value.CommandID)}, nil
	case model.QueueCapacityEvent:
		if name != v1.EventCommandQueueFull && name != v1.EventEventQueueFull {
			break
		}
		if value.Capacity == 0 || value.Capacity > maxQueueCapacity {
			return nil, fmt.Errorf("%w: invalid queue capacity", ErrMalformedInput)
		}
		queue, ok := publicQueueName(value.Queue)
		if !ok {
			return nil, fmt.Errorf("%w: unknown queue", ErrMalformedInput)
		}
		if (name == v1.EventCommandQueueFull && queue != v1.QueueNameCommand) ||
			(name == v1.EventEventQueueFull && queue != v1.QueueNameEvent) {
			return nil, fmt.Errorf("%w: queue event mismatch", ErrMalformedInput)
		}
		return v1.QueueCapacityEvent{Queue: queue, Capacity: value.Capacity}, nil
	case model.CoreErrorEvent:
		if name != v1.EventCoreError {
			break
		}
		public := a.MapError(value.Err)
		if public == nil {
			return nil, fmt.Errorf("%w: missing event error", ErrMalformedInput)
		}
		return v1.CoreErrorEvent{Error: *public}, nil
	case model.DiagnosticLogEvent:
		if name != v1.EventDiagnosticLog {
			break
		}
		level := v1.DiagnosticLogLevel(value.Level)
		code, message, ok := publicDiagnosticLog(value.Code)
		if !isDiagnosticLogLevel(level) || !ok {
			return nil, fmt.Errorf("%w: invalid diagnostic log", ErrMalformedInput)
		}
		diagnosticID := publicDiagnosticID(value.DiagnosticID)
		if value.DiagnosticID != "" && diagnosticID == "" {
			return nil, fmt.Errorf("%w: invalid diagnostic id", ErrMalformedInput)
		}
		return v1.DiagnosticLogEvent{Level: level, Code: code, Message: message, DiagnosticID: diagnosticID}, nil
	}
	return nil, fmt.Errorf("%w: unknown internal event or payload mismatch", ErrMalformedInput)
}

func publicDiagnosticLog(code string) (v1.DiagnosticLogCode, string, bool) {
	switch code {
	case "core_state":
		return v1.DiagnosticLogCoreState, "Core state changed", true
	case "connectivity":
		return v1.DiagnosticLogConnectivity, "Connectivity state changed", true
	case "queue_pressure":
		return v1.DiagnosticLogQueuePressure, "A local queue reached capacity", true
	case "payload_transfer":
		return v1.DiagnosticLogPayloadTransfer, "Payload transfer state changed", true
	default:
		return "", "", false
	}
}

func validateProjectedIdentifier(name, value string) error {
	return requiredBounded(name, value, maxIdentifierLength)
}

func publicDeliveryState(value string) (v1.DeliveryState, bool) {
	state := v1.DeliveryState(value)
	switch state {
	case v1.DeliveryStateReady, v1.DeliveryStateQueued, v1.DeliveryStateBlocked, v1.DeliveryStateFailed:
		return state, true
	}
	return "", false
}
func publicPayloadTransferState(value string) (v1.PayloadTransferState, bool) {
	state := v1.PayloadTransferState(value)
	switch state {
	case v1.PayloadTransferStarted, v1.PayloadTransferProgress, v1.PayloadTransferCompleted, v1.PayloadTransferFailed:
		return state, true
	}
	return "", false
}

func expectedMessageEventState(name v1.EventName) (v1.DeliveryState, bool) {
	switch name {
	case v1.EventMessageQueued, v1.EventMessageRetried, v1.EventDeliveryReplayed:
		return v1.DeliveryStateQueued, true
	case v1.EventMessageDeduplicated, v1.EventDeliveryResumed:
		return v1.DeliveryStateReady, true
	default:
		return "", false
	}
}

func expectedPayloadEventState(name v1.EventName) (v1.PayloadTransferState, bool) {
	switch name {
	case v1.EventPayloadTransferStarted:
		return v1.PayloadTransferStarted, true
	case v1.EventPayloadTransferProgress:
		return v1.PayloadTransferProgress, true
	case v1.EventPayloadTransferCompleted:
		return v1.PayloadTransferCompleted, true
	case v1.EventPayloadTransferFailed:
		return v1.PayloadTransferFailed, true
	default:
		return "", false
	}
}
func publicQueueName(value string) (v1.QueueName, bool) {
	queue := v1.QueueName(value)
	switch queue {
	case v1.QueueNameCommand, v1.QueueNameEvent:
		return queue, true
	}
	return "", false
}
