package v1

// ConnectivityState is an implementation-neutral view of application connectivity.
type ConnectivityState string

const (
	ConnectivityUnknown     ConnectivityState = "unknown"
	ConnectivityAvailable   ConnectivityState = "available"
	ConnectivityDegraded    ConnectivityState = "degraded"
	ConnectivityUnavailable ConnectivityState = "unavailable"
)

// SDKPersonality identifies the one application personality selected for a Core.
type SDKPersonality string

const (
	SDKPersonalityUnset      SDKPersonality = "unset"
	SDKPersonalityHTTPBridge SDKPersonality = "http_bridge"
	SDKPersonalityNative     SDKPersonality = "native"
)

// Status is the normalized public runtime status.
type Status struct {
	Lifecycle          LifecycleState
	Connectivity       ConnectivityState
	Personality        SDKPersonality
	AgentID            AgentID
	MeshID             MeshID
	MeshEndpoint       string
	QueuedMessageCount uint64
}

// resultType marks Status as an allowlisted result.
func (Status) resultType() {}
