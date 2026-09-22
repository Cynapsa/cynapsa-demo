package conversation

import "errors"

var (
	ErrInvalidConversation = errors.New("conversation: invalid identity")
	ErrDedupeCapacity      = errors.New("conversation: dedupe capacity exhausted")
	ErrMessageConflict     = errors.New("conversation: conflicting message identifier reuse")
	ErrUnknownMessage      = errors.New("conversation: message is not registered")
	ErrUnverifiedLane      = errors.New("conversation: lane is not verified")
	ErrLaneMismatch        = errors.New("conversation: envelope does not match authenticated lane")
)
