package model

import "time"

// Result is an internal command result that must be explicitly projected.
type Result struct {
	CommandID string
	Value     ResultValue
	Err       *Error
}

type ResultValue interface{ resultValue() }

type EmptyResult struct{}

func (EmptyResult) resultValue() {}

type CapabilitiesResult struct{ Commands, Features []string }

func (CapabilitiesResult) resultValue() {}

// StatusResult carries one private runtime status snapshot to the sole public
// boundary adapter.
type StatusResult struct{ Status Status }

func (StatusResult) resultValue() {}

type ConfigResult struct{ Config RuntimeConfig }

func (ConfigResult) resultValue() {}

type AuthResult struct {
	AgentID, MeshID, AgentInstanceID, Personality   string
	ProfileID, SessionExpiryMode, PreparationStatus string
	CredentialExpiresAt, OfflineStartDeadline       time.Time
	OfflineColdStartTargetSeconds                   int64
	OfflineTargetSatisfied                          bool
	PolicyRevision                                  int64
}

func (AuthResult) resultValue() {}

type AgentIDResult struct{ AgentID string }

func (AgentIDResult) resultValue() {}

type MeshSummary struct {
	MeshID string
	Active bool
}
type MeshListResult struct{ Meshes []MeshSummary }

func (MeshListResult) resultValue() {}

type AddressMappingsResult struct{ Mappings []AddressMapping }

func (AddressMappingsResult) resultValue() {}

type AddressResolution struct{ Recipient, Path, Query string }

func (AddressResolution) resultValue() {}

type SendResult struct {
	MessageID, ConversationID string
	Accepted                  bool
}

func (SendResult) resultValue() {}

type ResponseResult struct {
	MessageID, ConversationID, FromAgentID, MeshID string
	Payload                                        Payload
}

func (ResponseResult) resultValue() {}

type EventResult struct{ Event Event }

func (EventResult) resultValue() {}

type DeliveryQueueStatus struct {
	Queued uint64
	Paused bool
}

func (DeliveryQueueStatus) resultValue() {}

type PayloadHandleResult struct {
	Handle string
	Size   uint64
	EOF    bool
	Chunk  []byte
}

func (PayloadHandleResult) resultValue() {}

type ConversationStatus struct {
	ConversationID, MeshID, Peer, DeliveryState string
	QueuedMessageCount                          uint64
	Blocked                                     bool
}

func (ConversationStatus) resultValue() {}

type ConversationListResult struct{ Conversations []ConversationStatus }

func (ConversationListResult) resultValue() {}

type PolicyResult struct {
	Rules   []PolicyRule
	Allowed bool
}

func (PolicyResult) resultValue() {}

type PeerStatus struct {
	Peer, Connectivity            string
	Reachable, RecoveryInProgress bool
}

func (PeerStatus) resultValue() {}

type ConnectivityStatus struct{ State string }

func (ConnectivityStatus) resultValue() {}

type DiagnosticSnapshot struct {
	Status                                                                                                   Status
	CommandQueueDepth, EventQueueDepth, PeerCount, QueuedMessageCount, PendingRPCCount, PayloadTransferCount uint64
}

func (DiagnosticSnapshot) resultValue() {}

type CoreInitResult struct{ SessionID string }

func (CoreInitResult) resultValue() {}

type CompletionChannelResult struct {
	ChannelID   string
	MaxInFlight uint32
}

func (CompletionChannelResult) resultValue() {}

type EventSinkResult struct{ SinkID string }

func (EventSinkResult) resultValue() {}
