package v1

const (
	CommandChannelRegister   CommandName = "command.channel.register"
	CommandChannelClear      CommandName = "command.channel.clear"
	CommandCancel            CommandName = "command.cancel"
	CommandEventSinkRegister CommandName = "event.sink.register"
	CommandEventSinkClear    CommandName = "event.sink.clear"
	CommandEventSinkBind     CommandName = "event.sink.bind"
)

type CommandChannelRegisterCommand struct {
	CommandBase
	// Capacity is always in the inclusive range 1..65,536.
	Capacity uint32
}

func (CommandChannelRegisterCommand) Name() CommandName { return CommandChannelRegister }
func (CommandChannelRegisterCommand) commandType()      {}

type CommandChannelClearCommand struct {
	CommandBase
	ChannelID CompletionChannelID
}

func (CommandChannelClearCommand) Name() CommandName { return CommandChannelClear }
func (CommandChannelClearCommand) commandType()      {}

type CommandCancelCommand struct {
	CommandBase
	CommandHandle CommandHandle
}

func (CommandCancelCommand) Name() CommandName { return CommandCancel }
func (CommandCancelCommand) commandType()      {}

type EventSinkRegisterCommand struct {
	CommandBase
	// Capacity is always in the inclusive range 1..65,536.
	Capacity uint32
}

func (EventSinkRegisterCommand) Name() CommandName { return CommandEventSinkRegister }
func (EventSinkRegisterCommand) commandType()      {}

type EventSinkClearCommand struct {
	CommandBase
	SinkID EventSinkID
}

func (EventSinkClearCommand) Name() CommandName { return CommandEventSinkClear }
func (EventSinkClearCommand) commandType()      {}

type EventSinkBindCommand struct {
	CommandBase
	SinkID EventSinkID
}

func (EventSinkBindCommand) Name() CommandName { return CommandEventSinkBind }
func (EventSinkBindCommand) commandType()      {}

type CompletionChannelResult struct {
	ChannelID CompletionChannelID
	// MaxInFlight is always in the inclusive range 1..65,536.
	MaxInFlight uint32
}

func (CompletionChannelResult) resultType() {}

type EventSinkResult struct {
	SinkID EventSinkID
}

func (EventSinkResult) resultType() {}
