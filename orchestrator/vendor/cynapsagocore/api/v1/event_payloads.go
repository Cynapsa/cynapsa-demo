package v1

// SessionStateChangedEvent reports a normalized lifecycle transition.
type SessionStateChangedEvent struct {
	Previous LifecycleState
	Current  LifecycleState
}

// eventPayloadType marks the payload as public and versioned.
func (SessionStateChangedEvent) eventPayloadType() {
}

// ConnectivityChangedEvent reports normalized application connectivity.
type ConnectivityChangedEvent struct {
	Previous ConnectivityState
	Current  ConnectivityState
}

// eventPayloadType marks the payload as public and versioned.
func (ConnectivityChangedEvent) eventPayloadType() {
}

// PeerReachabilityEvent reports semantic reachability for one opaque peer.
type PeerReachabilityEvent struct {
	Peer      AgentID
	Reachable bool
}

func (PeerReachabilityEvent) eventPayloadType() {}

// MessageReceivedEvent carries one materialized application message.
type MessageReceivedEvent struct {
	MessageID      MessageID
	ConversationID ConversationID
	FromAgentID    AgentID
	MeshID         MeshID
	Mode           MessageMode
	RequestHandle  RequestHandle
	Payload        Payload
}

// eventPayloadType marks the payload as public and versioned.
func (MessageReceivedEvent) eventPayloadType() {
}

// MessageStateEvent reports one normalized message state transition.
type MessageStateEvent struct {
	MessageID      MessageID
	ConversationID ConversationID
	State          DeliveryState
}

func (MessageStateEvent) eventPayloadType() {}

// PayloadTransferEvent reports application-level payload transfer progress.
type PayloadTransferEvent struct {
	Handle    PayloadHandle
	State     PayloadTransferState
	Completed uint64
	Total     uint64
}

type PayloadTransferState string

const (
	PayloadTransferStarted   PayloadTransferState = "started"
	PayloadTransferProgress  PayloadTransferState = "progress"
	PayloadTransferCompleted PayloadTransferState = "completed"
	PayloadTransferFailed    PayloadTransferState = "failed"
)

// eventPayloadType marks the payload as public and versioned.
func (PayloadTransferEvent) eventPayloadType() {
}

// PolicyRejectedEvent identifies rejected application intent without private policy data.
type PolicyRejectedEvent struct {
	MessageID MessageID
	Peer      AgentID
	// Path is a nonempty absolute application path of at most 2,048 bytes;
	// query and fragment delimiters are forbidden.
	Path string
}

func (PolicyRejectedEvent) eventPayloadType() {}

// RPCTimeoutEvent identifies one timed-out local request.
type RPCTimeoutEvent struct {
	CommandID CommandID
}

func (RPCTimeoutEvent) eventPayloadType() {}

// QueueCapacityEvent reports normalized bounded-queue pressure.
type QueueCapacityEvent struct {
	Queue QueueName
	// Capacity is always in the inclusive range 1..65,536.
	Capacity uint32
}

type QueueName string

const (
	QueueNameCommand QueueName = "command"
	QueueNameEvent   QueueName = "event"
)

func (QueueCapacityEvent) eventPayloadType() {}

// DiagnosticLogLevel is a closed support-safe severity vocabulary.
type DiagnosticLogLevel string

const (
	DiagnosticLogDebug DiagnosticLogLevel = "debug"
	DiagnosticLogInfo  DiagnosticLogLevel = "info"
	DiagnosticLogWarn  DiagnosticLogLevel = "warn"
	DiagnosticLogError DiagnosticLogLevel = "error"
)

type DiagnosticLogCode string

const (
	DiagnosticLogCoreState       DiagnosticLogCode = "core_state"
	DiagnosticLogConnectivity    DiagnosticLogCode = "connectivity"
	DiagnosticLogQueuePressure   DiagnosticLogCode = "queue_pressure"
	DiagnosticLogPayloadTransfer DiagnosticLogCode = "payload_transfer"
)

// DiagnosticLogEvent carries a normalized message without arbitrary fields or
// private dependency text.
type DiagnosticLogEvent struct {
	Level        DiagnosticLogLevel
	Code         DiagnosticLogCode
	Message      string
	DiagnosticID DiagnosticID
}

func (DiagnosticLogEvent) eventPayloadType() {}
