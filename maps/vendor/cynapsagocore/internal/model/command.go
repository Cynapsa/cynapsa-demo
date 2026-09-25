package model

import "time"

// Command is the typed internal command produced only by SDKBoundaryAdapter.
type Command struct {
	ID        string
	Name      string
	SessionID string
	Args      CommandArgs
	ownership *commandOwnership
	canonical bool
}

// CommandArgs is a closed set of boundary-facing internal command variants.
type CommandArgs interface{ commandArgs() }

type EmptyArgs struct{}

func (EmptyArgs) commandArgs() {}

type ConfigUpdateArgs struct {
	CommandTimeout *time.Duration
	RPCTimeout     *time.Duration
	QueueLimit     *uint32
	PayloadLimit   *uint64
}

func (ConfigUpdateArgs) commandArgs() {}

type TokenAuthArgs struct {
	Token       []byte
	MeshID      string
	ProfileID   string
	ForceEnroll bool
}

func (TokenAuthArgs) commandArgs() {}

type InstallationAuthArgs struct {
	ProfileID string
	MeshID    string
}

func (InstallationAuthArgs) commandArgs() {}

type AuthArgs struct {
	MeshEndpoint    string
	Username        string
	Password        []byte
	MeshID          string
	AgentInstanceID string
}

func (AuthArgs) commandArgs() {}

type AddressMapping struct{ VirtualOrigin, Recipient string }
type AddressPutArgs struct{ Mapping AddressMapping }

func (AddressPutArgs) commandArgs() {}

type AddressRemoveArgs struct{ VirtualOrigin string }

func (AddressRemoveArgs) commandArgs() {}

type AddressResolveArgs struct{ URL string }

func (AddressResolveArgs) commandArgs() {}

type Header struct{ Name, Value string }

type NativePayload struct {
	// Path is boundary-validated as an absolute application path (maximum 2,048 bytes).
	ContentType, Path string
	Body              []byte
}

func (NativePayload) payloadValue() {}

type HTTPRequestPayload struct {
	// Path is boundary-validated as an absolute application path (maximum 2,048 bytes).
	Method, Path, Query string
	Headers             []Header
	Body                []byte
}

func (HTTPRequestPayload) payloadValue() {}

type ApplicationError struct {
	Code        string
	Detail      string
	DetailsJSON string
}

type HTTPResponsePayload struct {
	StatusCode uint16
	Reason     string
	Headers    []Header
	Body       []byte
	Error      *ApplicationError
}

func (HTTPResponsePayload) payloadValue() {}

type PayloadHandle struct{ Handle string }

func (PayloadHandle) payloadValue() {}

type PayloadValue interface{ payloadValue() }

// Payload has exactly one populated variant; Handle identifies the complete
// canonical snapshot.
type Payload struct {
	Value PayloadValue
}

type MessageSendArgs struct {
	To      string
	Payload Payload
}

func (MessageSendArgs) commandArgs() {}

type MessageRequestArgs struct {
	To      string
	Payload Payload
	TTL     time.Duration
}

func (MessageRequestArgs) commandArgs() {}

type MessageReplyArgs struct {
	RequestHandle string
	Payload       Payload
}

func (MessageReplyArgs) commandArgs() {}

type DeliveryAcceptArgs struct{ EventID string }

func (DeliveryAcceptArgs) commandArgs() {}

type MessageIDArgs struct{ MessageID string }

func (MessageIDArgs) commandArgs() {}

// HandlerPathArgs carries a boundary-validated absolute application path.
type HandlerPathArgs struct{ Path string }

func (HandlerPathArgs) commandArgs() {}

type PayloadHandleArgs struct{ Handle string }

func (PayloadHandleArgs) commandArgs() {}

type PayloadWriteArgs struct {
	Handle string
	Chunk  []byte
}

func (PayloadWriteArgs) commandArgs() {}

type PayloadReadArgs struct {
	Handle string
	Offset uint64
	Limit  uint32
}

func (PayloadReadArgs) commandArgs() {}

type ConversationIDArgs struct{ ConversationID string }

func (ConversationIDArgs) commandArgs() {}

// PolicyRule.Path is either the empty whole-field wildcard or a
// boundary-validated absolute application path.
type PolicyRule struct{ Action, Path, AgentID string }
type PolicySetArgs struct{ Rules []PolicyRule }

func (PolicySetArgs) commandArgs() {}

type PolicyTestArgs struct{ Input MessageSendArgs }

func (PolicyTestArgs) commandArgs() {}

type DiagnosticsPeerArgs struct{ Peer string }

func (DiagnosticsPeerArgs) commandArgs() {}

type DiagnosticsLogsArgs struct{ Enabled bool }

func (DiagnosticsLogsArgs) commandArgs() {}

type ChannelRegisterArgs struct{ Capacity uint32 }

func (ChannelRegisterArgs) commandArgs() {}

type ChannelIDArgs struct{ ChannelID string }

func (ChannelIDArgs) commandArgs() {}

type CommandCancelArgs struct{ CommandHandle string }

func (CommandCancelArgs) commandArgs() {}

type EventSinkRegisterArgs struct{ Capacity uint32 }

func (EventSinkRegisterArgs) commandArgs() {}

type EventSinkIDArgs struct{ SinkID string }

func (EventSinkIDArgs) commandArgs() {}
