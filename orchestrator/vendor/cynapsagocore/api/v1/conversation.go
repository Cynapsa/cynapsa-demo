package v1

const (
	CommandConversationList   CommandName = "conversation.list"
	CommandConversationStatus CommandName = "conversation.status"
	CommandConversationClose  CommandName = "conversation.close"
)

type ConversationListCommand struct{ CommandBase }

func (ConversationListCommand) Name() CommandName { return CommandConversationList }
func (ConversationListCommand) commandType()      {}

type ConversationStatusCommand struct {
	CommandBase
	ConversationID ConversationID
}

func (ConversationStatusCommand) Name() CommandName { return CommandConversationStatus }
func (ConversationStatusCommand) commandType()      {}

type ConversationCloseCommand struct {
	CommandBase
	ConversationID ConversationID
}

func (ConversationCloseCommand) Name() CommandName { return CommandConversationClose }
func (ConversationCloseCommand) commandType()      {}

// DeliveryState is a bounded public view of logical message delivery.
type DeliveryState string

const (
	DeliveryStateReady   DeliveryState = "ready"
	DeliveryStateQueued  DeliveryState = "queued"
	DeliveryStateBlocked DeliveryState = "blocked"
	DeliveryStateFailed  DeliveryState = "failed"
)

// ConversationStatus is a normalized public conversation view.
type ConversationStatus struct {
	ConversationID     ConversationID
	MeshID             MeshID
	Peer               AgentID
	DeliveryState      DeliveryState
	QueuedMessageCount uint64
	Blocked            bool
}

func (ConversationStatus) resultType() {}

type ConversationListResult struct {
	Conversations []ConversationStatus
}

func (ConversationListResult) resultType() {}
