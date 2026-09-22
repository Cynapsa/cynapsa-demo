package model

// LifecycleState is the detailed internal runtime lifecycle.
type LifecycleState string

const (
	LifecycleCreated          LifecycleState = "created"
	LifecycleAuthenticating   LifecycleState = "authenticating"
	LifecycleMeshConnected    LifecycleState = "mesh_connected"
	LifecycleDurableReady     LifecycleState = "durable_ready"
	LifecyclePeerLinkBuilding LifecycleState = "peer_link_building"
	LifecycleReady            LifecycleState = "ready"
	LifecycleDegraded         LifecycleState = "degraded"
	LifecycleClosing          LifecycleState = "closing"
	LifecycleClosed           LifecycleState = "closed"
	LifecycleFailed           LifecycleState = "failed"
)

// Status contains detailed private state that requires boundary normalization.
type Status struct {
	Lifecycle          LifecycleState
	ConnectivityDetail string
	Personality        string
	AgentID            string
	MeshID             string
	MeshEndpoint       string
	QueuedMessageCount uint64
}
