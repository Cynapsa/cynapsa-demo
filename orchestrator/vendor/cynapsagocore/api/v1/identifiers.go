package v1

// CommandID identifies one local SDK command.
type CommandID string

// CommandHandle is a cmdh_-prefixed opaque Core-scoped capability containing
// the frozen 40-byte scope/generation/nonce layout. SDKs retain it internally;
// it is not an application correlation identifier.
type CommandHandle string

// SDKSessionID identifies one local SDK session.
type SDKSessionID string

// AgentID is an opaque agent identifier. Public input must be nonempty valid
// UTF-8 of at most 256 bytes and contain no NUL or Unicode control characters;
// its internal representation is otherwise unspecified.
type AgentID string

// MeshID identifies an application mesh scope.
type MeshID string

// ConversationID identifies an application conversation.
type ConversationID string

// MessageID identifies one stable application message.
type MessageID string

// RequestHandle is a reqh_-prefixed opaque Core-scoped capability containing
// 32 random bytes for replying once to an inbound request.
type RequestHandle string

// PayloadHandle is a payh_-prefixed opaque Core-scoped handle containing 32
// random bytes for one immutable complete canonical payload snapshot.
type PayloadHandle string

func (PayloadHandle) payloadVariant() {}

// DiagnosticID is an opaque operator-correlation identifier.
type DiagnosticID string

// EventID identifies one SDK-visible event.
type EventID string

// CompletionChannelID identifies local completion plumbing.
type CompletionChannelID string

// EventSinkID identifies local event plumbing.
type EventSinkID string
