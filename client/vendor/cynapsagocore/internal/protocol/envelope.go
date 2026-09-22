// Package protocol defines private AZTM wire and storage models.
package protocol

import (
	"strings"
	"time"
)

// Version2 is the only accepted private envelope version. Version 2 is an
// intentional clean wire cut: application sequence and mesh-generation fields
// from the legacy version are neither emitted nor accepted.
const Version2 uint16 = 2

// Envelope is private and must never be serialized directly to an SDK.
// CredentialProof remains in this private Go shape only for migration safety;
// V1 requires it to be empty because carrier provenance is out of band.
type Envelope struct {
	Version          uint16
	MessageID        string
	ConversationID   string
	Sender           string
	Recipient        string
	MeshID           string
	Mode             Mode
	CorrelationID    string
	ReplyTo          string
	CreatedAt        time.Time
	ExpiresAt        time.Time
	ClockUncertainty time.Duration
	Payload          PayloadDescriptor
	CredentialProof  []byte
}

// EnvelopeInput contains stable identity allocated by the owning session and
// sender lane. MessageID and request CorrelationID may be generated when empty.
type EnvelopeInput struct {
	MessageID        string
	ConversationID   string
	Sender           string
	Recipient        string
	MeshID           string
	Mode             Mode
	CorrelationID    string
	ReplyTo          string
	CreatedAt        time.Time
	ExpiresAt        time.Time
	ClockUncertainty time.Duration
	Payload          PayloadDescriptor
	CredentialProof  []byte
}

// NewEnvelope constructs a V2 envelope without deriving transport state. The
// caller owns conversation identity allocation; replay must reuse the returned
// Envelope instead of calling NewEnvelope again.
func NewEnvelope(in EnvelopeInput) (Envelope, error) {
	messageID := in.MessageID
	if messageID == "" {
		var err error
		messageID, err = NewMessageID()
		if err != nil {
			return Envelope{}, err
		}
	}
	correlationID := in.CorrelationID
	if in.Mode == ModeRequest && correlationID == "" {
		var err error
		correlationID, err = NewCorrelationID()
		if err != nil {
			return Envelope{}, err
		}
	}
	createdAt := in.CreatedAt
	createdAt = createdAt.UTC().Truncate(time.Millisecond)
	expiresAt := in.ExpiresAt
	if !expiresAt.IsZero() {
		expiresAt = expiresAt.UTC().Truncate(time.Millisecond)
	}
	clockUncertainty := in.ClockUncertainty
	if clockUncertainty > 0 && clockUncertainty <= MaxClockUncertainty {
		clockUncertainty = ((clockUncertainty + time.Microsecond - 1) / time.Microsecond) * time.Microsecond
	}

	envelope := Envelope{
		Version:          Version2,
		MessageID:        messageID,
		ConversationID:   in.ConversationID,
		Sender:           in.Sender,
		Recipient:        in.Recipient,
		MeshID:           in.MeshID,
		Mode:             in.Mode,
		CorrelationID:    correlationID,
		ReplyTo:          in.ReplyTo,
		CreatedAt:        createdAt,
		ExpiresAt:        expiresAt,
		ClockUncertainty: clockUncertainty,
		Payload:          in.Payload,
		CredentialProof:  append([]byte(nil), in.CredentialProof...),
	}
	if err := validateEnvelope(envelope, false); err != nil {
		return Envelope{}, err
	}
	return envelope.Clone(), nil
}

// Clone returns an ownership-safe copy suitable for path replay or snapshots.
func (e Envelope) Clone() Envelope {
	e.MessageID = strings.Clone(e.MessageID)
	e.ConversationID = strings.Clone(e.ConversationID)
	e.Sender = strings.Clone(e.Sender)
	e.Recipient = strings.Clone(e.Recipient)
	e.MeshID = strings.Clone(e.MeshID)
	e.CorrelationID = strings.Clone(e.CorrelationID)
	e.ReplyTo = strings.Clone(e.ReplyTo)
	e.Payload = e.Payload.clone()
	e.CredentialProof = append([]byte(nil), e.CredentialProof...)
	return e
}
