package v1

// CommandName identifies one allowlisted public command schema.
type CommandName string

// Command is implemented only by versioned public command types.
type Command interface {
	Name() CommandName
	commandType()
}

// CommandBase carries identifiers common to every public command.
type CommandBase struct {
	CommandID    CommandID
	SDKSessionID SDKSessionID
}

// CommandBase deliberately does not implement Command. Only concrete,
// versioned command structs in this package satisfy the closed interface.
