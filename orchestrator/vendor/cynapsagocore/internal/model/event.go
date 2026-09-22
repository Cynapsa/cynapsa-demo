package model

import "time"

// Event is an internal runtime event that cannot be sent directly to an SDK.
type Event struct {
	ID        string
	Name      string
	CreatedAt time.Time
	Value     EventValue
}

type EventValue interface{ eventValue() }

type SessionStateChangedEvent struct{ Previous, Current LifecycleState }

func (SessionStateChangedEvent) eventValue() {}

type ConnectivityChangedEvent struct{ Previous, Current string }

func (ConnectivityChangedEvent) eventValue() {}

type PeerReachabilityEvent struct {
	Peer      string
	Reachable bool
}

func (PeerReachabilityEvent) eventValue() {}

type MessageReceivedEvent struct {
	MessageID, ConversationID, FromAgentID, MeshID, Mode, RequestHandle string
	Payload                                                             Payload
}

func (MessageReceivedEvent) eventValue() {}

type MessageStateEvent struct{ MessageID, ConversationID, State string }

func (MessageStateEvent) eventValue() {}

type PayloadTransferEvent struct {
	Handle, State    string
	Completed, Total uint64
}

func (PayloadTransferEvent) eventValue() {}

type PolicyRejectedEvent struct{ MessageID, Peer, Path string }

func (PolicyRejectedEvent) eventValue() {}

type RPCTimeoutEvent struct{ CommandID string }

func (RPCTimeoutEvent) eventValue() {}

type QueueCapacityEvent struct {
	Queue    string
	Capacity uint32
}

func (QueueCapacityEvent) eventValue() {}

type CoreErrorEvent struct{ Err *Error }

func (CoreErrorEvent) eventValue() {}

type DiagnosticLogEvent struct{ Level, Code, DiagnosticID string }

func (DiagnosticLogEvent) eventValue() {}
