package v1

const (
	CommandHandlerRegister   CommandName = "handler.register"
	CommandHandlerUnregister CommandName = "handler.unregister"
)

// HandlerRegisterCommand registers application routing intent. The language
// SDK retains the actual callable; no function pointer crosses the ABI.
type HandlerRegisterCommand struct {
	CommandBase
	// Path is a nonempty absolute application path of at most 2,048 bytes;
	// query and fragment delimiters are forbidden.
	Path string
}

func (HandlerRegisterCommand) Name() CommandName { return CommandHandlerRegister }
func (HandlerRegisterCommand) commandType()      {}

type HandlerUnregisterCommand struct {
	CommandBase
	// Path has the same application-path contract as HandlerRegisterCommand.Path.
	Path string
}

func (HandlerUnregisterCommand) Name() CommandName { return CommandHandlerUnregister }
func (HandlerUnregisterCommand) commandType()      {}
