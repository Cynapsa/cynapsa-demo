package v1

import "time"

const (
	CommandAuthLogin               CommandName = "auth.login"
	CommandAuthConnect             CommandName = "auth.connect"
	CommandAuthTokenLogin          CommandName = "auth.token_login"
	CommandAuthTokenConnect        CommandName = "auth.token_connect"
	CommandAuthInstallationLogin   CommandName = "auth.installation_login"
	CommandAuthInstallationConnect CommandName = "auth.installation_connect"
	CommandAuthLogout              CommandName = "auth.logout"
	CommandAuthAgentID             CommandName = "auth.agent_id"
)

// AuthInput contains the login-session credentials for one mesh.
type AuthInput struct {
	// MeshEndpoint is an explicit host:port authority. It has no default port
	// or service-discovery behavior and does not accept URI components.
	MeshEndpoint    string
	Username        AgentID
	Password        string
	MeshID          MeshID
	AgentInstanceID string
}

// AuthTokenInput contains an opaque enrollment token and the selected mesh.
// The enrollment service supplies all private connectivity credentials.
type AuthTokenInput struct {
	Token  string
	MeshID MeshID
	// ProfileID selects a process-local installation namespace. Empty retains
	// the legacy default profile. It is never an authorization identity.
	ProfileID string
}

// AuthInstallationInput selects an already-enrolled local installation and
// mesh without requiring or retaining the bootstrap enrollment token.
type AuthInstallationInput struct {
	ProfileID string
	MeshID    MeshID
}

// AuthTokenLoginCommand authenticates with a token and selects HTTP Bridge behavior.
type AuthTokenLoginCommand struct {
	CommandBase
	Auth AuthTokenInput
}

func (AuthTokenLoginCommand) Name() CommandName { return CommandAuthTokenLogin }
func (AuthTokenLoginCommand) commandType()      {}

// AuthTokenConnectCommand authenticates with a token and selects native behavior.
type AuthTokenConnectCommand struct {
	CommandBase
	Auth AuthTokenInput
}

func (AuthTokenConnectCommand) Name() CommandName { return CommandAuthTokenConnect }
func (AuthTokenConnectCommand) commandType()      {}

type AuthInstallationLoginCommand struct {
	CommandBase
	Auth AuthInstallationInput
}

func (AuthInstallationLoginCommand) Name() CommandName { return CommandAuthInstallationLogin }
func (AuthInstallationLoginCommand) commandType()      {}

type AuthInstallationConnectCommand struct {
	CommandBase
	Auth AuthInstallationInput
}

func (AuthInstallationConnectCommand) Name() CommandName { return CommandAuthInstallationConnect }
func (AuthInstallationConnectCommand) commandType()      {}

// AuthLoginCommand authenticates and selects HTTP Bridge SDK behavior.
type AuthLoginCommand struct {
	CommandBase
	Auth AuthInput
}

// Name returns the allowlisted command name.
func (AuthLoginCommand) Name() CommandName {
	return CommandAuthLogin
}
func (AuthLoginCommand) commandType() {}

// AuthConnectCommand authenticates for native AZTM SDK behavior.
type AuthConnectCommand struct {
	CommandBase
	Auth AuthInput
}

// Name returns the allowlisted command name.
func (AuthConnectCommand) Name() CommandName {
	return CommandAuthConnect
}
func (AuthConnectCommand) commandType() {}

// AuthLogoutCommand closes authenticated application state.
type AuthLogoutCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (AuthLogoutCommand) Name() CommandName {
	return CommandAuthLogout
}
func (AuthLogoutCommand) commandType() {}

// AuthAgentIDCommand requests the current opaque agent identifier.
type AuthAgentIDCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (AuthAgentIDCommand) Name() CommandName {
	return CommandAuthAgentID
}
func (AuthAgentIDCommand) commandType() {}

// AuthResult reports the selected personality and opaque authenticated context.
// Credential material is never returned.
type AuthResult struct {
	AgentID                       AgentID
	MeshID                        MeshID
	AgentInstanceID               string
	Personality                   SDKPersonality
	ProfileID                     string
	CredentialExpiresAt           time.Time
	OfflineStartDeadline          time.Time
	OfflineColdStartTargetSeconds int64
	OfflineTargetSatisfied        bool
	PolicyRevision                int64
	SessionExpiryMode             string
	PreparationStatus             string
}

func (AuthResult) resultType() {}

// AgentIDResult returns the current opaque agent identity.
type AgentIDResult struct {
	AgentID AgentID
}

func (AgentIDResult) resultType() {}
