package commandgate

// Admission is the internal local admission result.
type Admission struct {
	CommandID     string
	CommandHandle CommandHandle
	Accepted      bool
	Reason        string
}

// Admission reason codes are private command-gate values. SDKBoundaryAdapter
// owns any projection into the public error vocabulary.
const (
	ReasonInvalidCommandID  = "invalid_command_id"
	ReasonDuplicate         = "duplicate_command_id"
	ReasonCapacityReached   = "command_capacity_reached"
	ReasonQueueFull         = "command_queue_full"
	ReasonContextCancelled  = "admission_cancelled"
	ReasonShuttingDown      = "shutdown_in_progress"
	ReasonHandleUnavailable = "command_handle_unavailable"
)
