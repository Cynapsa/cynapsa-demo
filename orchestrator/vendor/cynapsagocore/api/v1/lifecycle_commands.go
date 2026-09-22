package v1

const (
	CommandCoreInit         CommandName = "core.init"
	CommandCoreCapabilities CommandName = "core.capabilities"
	CommandCoreStatus       CommandName = "core.status"
	CommandCoreShutdown     CommandName = "core.shutdown"
	CommandConfigGet        CommandName = "session.config.get"
	CommandConfigUpdate     CommandName = "session.config.update"
)

// CoreInitCommand initializes the embedded runtime for one SDK process.
type CoreInitCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (CoreInitCommand) Name() CommandName {
	return CommandCoreInit
}
func (CoreInitCommand) commandType() {}

// CoreCapabilitiesCommand requests the public application capability allowlist.
type CoreCapabilitiesCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (CoreCapabilitiesCommand) Name() CommandName {
	return CommandCoreCapabilities
}
func (CoreCapabilitiesCommand) commandType() {}

// CoreStatusCommand requests normalized application-level status.
type CoreStatusCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (CoreStatusCommand) Name() CommandName {
	return CommandCoreStatus
}
func (CoreStatusCommand) commandType() {}

// CoreShutdownCommand requests graceful runtime shutdown.
type CoreShutdownCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (CoreShutdownCommand) Name() CommandName {
	return CommandCoreShutdown
}
func (CoreShutdownCommand) commandType() {}

// ConfigGetCommand requests allowlisted public configuration only.
type ConfigGetCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (ConfigGetCommand) Name() CommandName {
	return CommandConfigGet
}
func (ConfigGetCommand) commandType() {}

// ConfigUpdateCommand requests a safe public configuration update.
type ConfigUpdateCommand struct {
	CommandBase
	Update ConfigUpdate
}

// Name returns the allowlisted command name.
func (ConfigUpdateCommand) Name() CommandName {
	return CommandConfigUpdate
}
func (ConfigUpdateCommand) commandType() {}

// CoreInitResult identifies one initialized local SDK/Core session.
type CoreInitResult struct {
	SDKSessionID SDKSessionID
}

func (CoreInitResult) resultType() {}

// ConfigResult returns process-local limits only.
type ConfigResult struct {
	Config Config
}

func (ConfigResult) resultType() {}
