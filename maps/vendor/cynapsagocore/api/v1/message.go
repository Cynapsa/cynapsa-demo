package v1

import "time"

const (
	CommandMessageSend    CommandName = "message.send"
	CommandMessageRequest CommandName = "message.request"
	CommandMessageReply   CommandName = "message.reply"
)

// MessageMode describes application request/reply semantics.
type MessageMode string

const (
	MessageModeOneWay   MessageMode = "msg"
	MessageModeRequest  MessageMode = "rpc"
	MessageModeResponse MessageMode = "rpc_response"
)

// Header preserves one HTTP field exactly as intercepted.
type Header struct {
	// Name is a valid HTTP token of at most 512 bytes.
	Name string
	// Value is at most 8,192 bytes and contains no CR or LF.
	Value string
}

// NativePayload is one canonical native application payload.
type NativePayload struct {
	// ContentType is valid UTF-8 text of at most 512 bytes with no CR or LF.
	ContentType string
	// Path is a nonempty absolute application path of at most 2,048 bytes;
	// query and fragment delimiters are forbidden.
	Path string
	Body []byte
}

func (NativePayload) payloadVariant() {}

// HTTPRequestPayload is one canonical prepared HTTP request.
type HTTPRequestPayload struct {
	// Method is a valid HTTP token of at most 32 bytes.
	Method string
	// Path has the same application-path contract as NativePayload.Path.
	Path string
	// Query is at most 8,192 bytes.
	Query string
	// Headers contains at most 4,096 duplicate-preserving fields.
	Headers []Header
	Body    []byte
}

func (HTTPRequestPayload) payloadVariant() {}

// ApplicationError is optional application-declared error metadata. DetailsJSON
// is a bounded JSON object encoded as UTF-8 text; it is data, never a Go map.
type ApplicationError struct {
	// Code is nonempty valid UTF-8 text of at most 512 bytes.
	Code string
	// Detail is nonempty valid UTF-8 text of at most 8,192 bytes.
	Detail string
	// DetailsJSON is a valid UTF-8 JSON object of at most 8,192 bytes.
	DetailsJSON string
}

// HTTPResponsePayload is one canonical HTTP response.
type HTTPResponsePayload struct {
	StatusCode uint16
	// Reason is valid UTF-8 text of at most 512 bytes with no CR or LF.
	Reason string
	// Headers contains at most 4,096 duplicate-preserving fields.
	Headers []Header
	Body    []byte
	// Error is optional and valid only when StatusCode is in 400..599.
	// A 4xx or 5xx response does not require application-error metadata.
	Error *ApplicationError
}

func (HTTPResponsePayload) payloadVariant() {}

// PayloadVariant is the closed set of canonical application payloads.
type PayloadVariant interface{ payloadVariant() }

// Payload contains exactly one closed canonical payload variant. A Handle
// refers to the entire canonical payload snapshot, never only its body.
type Payload struct {
	Value PayloadVariant
}

// MessageSendCommand submits a one-way application message. When a destination
// has multiple active instances, Core attempts every current instance. A failed
// completion can be an uncertain outcome, so an application must not replay a
// semantic action without its own idempotency key.
type MessageSendCommand struct {
	CommandBase
	To      AgentID
	Payload Payload
}

// Name returns the allowlisted command name.
func (MessageSendCommand) Name() CommandName {
	return CommandMessageSend
}
func (MessageSendCommand) commandType() {}

// MessageRequestCommand submits an application RPC request.
type MessageRequestCommand struct {
	CommandBase
	To      AgentID
	Payload Payload
	// TTL is the one request lifetime. Zero selects the Core's configured RPC
	// timeout; negative values are invalid.
	TTL time.Duration
}

// Name returns the allowlisted command name.
func (MessageRequestCommand) Name() CommandName {
	return CommandMessageRequest
}
func (MessageRequestCommand) commandType() {}

// MessageReplyCommand replies through one local opaque request handle.
type MessageReplyCommand struct {
	CommandBase
	RequestHandle RequestHandle
	Payload       Payload
}

// Name returns the allowlisted command name.
func (MessageReplyCommand) Name() CommandName {
	return CommandMessageReply
}
func (MessageReplyCommand) commandType() {}

// SendResult reports local core acceptance without implying remote processing.
type SendResult struct {
	MessageID      MessageID
	ConversationID ConversationID
	Accepted       bool
}

// resultType marks SendResult as an allowlisted command result.
func (SendResult) resultType() {
}

// ResponseResult contains a normalized application response.
type ResponseResult struct {
	MessageID      MessageID
	ConversationID ConversationID
	FromAgentID    AgentID
	MeshID         MeshID
	Payload        Payload
}

// resultType marks ResponseResult as an allowlisted command result.
func (ResponseResult) resultType() {
}
