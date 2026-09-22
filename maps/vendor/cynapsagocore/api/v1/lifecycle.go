package v1

// LifecycleState is the bounded public runtime lifecycle vocabulary.
type LifecycleState string

const (
	LifecycleCreated    LifecycleState = "created"
	LifecycleConnecting LifecycleState = "connecting"
	LifecycleReady      LifecycleState = "ready"
	LifecycleDegraded   LifecycleState = "degraded"
	LifecycleClosing    LifecycleState = "closing"
	LifecycleClosed     LifecycleState = "closed"
	LifecycleFailed     LifecycleState = "failed"
)
