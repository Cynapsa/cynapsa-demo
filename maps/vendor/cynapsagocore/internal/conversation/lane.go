package conversation

import "github.com/Cynapsa/cynapsagocore/internal/protocol"

// AuthenticatedBinding is the trusted receiver-side session context. Its
// values come from authentication and local routing, never from the envelope.
type AuthenticatedBinding struct {
	MeshID              string
	AuthenticatedSender string
	Recipient           string
}

// LaneID is a verified V3 participant-pair lane. Fields are private so
// arbitrary wire data cannot manufacture a lane or consume dedupe capacity.
type LaneID struct {
	meshID         string
	conversationID string
	sender         string
	recipient      string
	verified       bool
}

// NewLaneID binds trusted session values to their derived conversation. The
// caller must source all fields from authenticated/local context.
func NewLaneID(binding AuthenticatedBinding) (LaneID, error) {
	if err := protocol.ValidateMeshID(binding.MeshID); err != nil {
		return LaneID{}, ErrInvalidConversation
	}
	if err := protocol.ValidateAgentIdentity(binding.AuthenticatedSender); err != nil {
		return LaneID{}, ErrInvalidConversation
	}
	if err := protocol.ValidateAgentIdentity(binding.Recipient); err != nil {
		return LaneID{}, ErrInvalidConversation
	}
	conversationID, err := DeriveID(binding.MeshID, binding.AuthenticatedSender, binding.Recipient)
	if err != nil {
		return LaneID{}, err
	}
	return LaneID{
		meshID:         binding.MeshID,
		conversationID: conversationID,
		sender:         binding.AuthenticatedSender,
		recipient:      binding.Recipient,
		verified:       true,
	}, nil
}

// VerifyEnvelopeBinding validates the envelope and checks it against trusted
// mesh and participant context before any stateful receive operation.
func VerifyEnvelopeBinding(envelope protocol.Envelope, binding AuthenticatedBinding) (LaneID, error) {
	lane, err := NewLaneID(binding)
	if err != nil {
		return LaneID{}, err
	}
	if err := lane.validateEnvelope(envelope); err != nil {
		return LaneID{}, err
	}
	return lane, nil
}

func (lane LaneID) validateEnvelope(envelope protocol.Envelope) error {
	if !lane.verified {
		return ErrUnverifiedLane
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return err
	}
	if envelope.MeshID != lane.meshID ||
		envelope.ConversationID != lane.conversationID ||
		envelope.Sender != lane.sender ||
		envelope.Recipient != lane.recipient {
		return ErrLaneMismatch
	}
	return nil
}

// MeshID returns the authenticated mesh scope.
func (lane LaneID) MeshID() string { return lane.meshID }

// ConversationID returns the derived conversation identity.
func (lane LaneID) ConversationID() string { return lane.conversationID }

// Sender returns the authenticated sender for this lane.
func (lane LaneID) Sender() string { return lane.sender }

// Recipient returns the trusted local recipient for this lane.
func (lane LaneID) Recipient() string { return lane.recipient }
