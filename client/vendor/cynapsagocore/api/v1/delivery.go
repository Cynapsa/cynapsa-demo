package v1

const (
	CommandDeliveryNext        CommandName = "delivery.next"
	CommandDeliveryAccept      CommandName = "delivery.accept"
	CommandDeliveryQueueStatus CommandName = "delivery.queue.status"
	CommandDeliveryRetry       CommandName = "delivery.retry"
	CommandDeliveryPause       CommandName = "delivery.pause"
	CommandDeliveryResume      CommandName = "delivery.resume"
	CommandDeliveryDrop        CommandName = "delivery.drop"
)

// DeliveryNextCommand polls the same normalized event queue used by push delivery.
type DeliveryNextCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (DeliveryNextCommand) Name() CommandName {
	return CommandDeliveryNext
}
func (DeliveryNextCommand) commandType() {}

// DeliveryAcceptCommand releases local backpressure for one consumed event.
type DeliveryAcceptCommand struct {
	CommandBase
	EventID EventID
}

// Name returns the allowlisted command name.
func (DeliveryAcceptCommand) Name() CommandName {
	return CommandDeliveryAccept
}
func (DeliveryAcceptCommand) commandType() {}

// DeliveryQueueStatusCommand requests normalized local queue state.
type DeliveryQueueStatusCommand struct {
	CommandBase
}

func (DeliveryQueueStatusCommand) Name() CommandName { return CommandDeliveryQueueStatus }
func (DeliveryQueueStatusCommand) commandType()      {}

// DeliveryRetryCommand retries one queued logical message.
type DeliveryRetryCommand struct {
	CommandBase
	MessageID MessageID
}

func (DeliveryRetryCommand) Name() CommandName { return CommandDeliveryRetry }
func (DeliveryRetryCommand) commandType()      {}

// DeliveryPauseCommand pauses local dispatch for the authenticated session.
type DeliveryPauseCommand struct{ CommandBase }

func (DeliveryPauseCommand) Name() CommandName { return CommandDeliveryPause }
func (DeliveryPauseCommand) commandType()      {}

// DeliveryResumeCommand resumes local dispatch for the authenticated session.
type DeliveryResumeCommand struct{ CommandBase }

func (DeliveryResumeCommand) Name() CommandName { return CommandDeliveryResume }
func (DeliveryResumeCommand) commandType()      {}

// DeliveryDropCommand drops one queued logical message locally.
type DeliveryDropCommand struct {
	CommandBase
	MessageID MessageID
}

func (DeliveryDropCommand) Name() CommandName { return CommandDeliveryDrop }
func (DeliveryDropCommand) commandType()      {}

// DeliveryQueueStatus reports bounded process-local delivery state.
type DeliveryQueueStatus struct {
	Queued uint64
	Paused bool
}

func (DeliveryQueueStatus) resultType() {}
