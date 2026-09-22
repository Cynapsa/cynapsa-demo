package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MaxEnvelopeBytes         = 1 << 20
	MaxInlinePayloadBytes    = 256 << 10
	MaxAgentIdentityBytes    = 256
	MaxMeshIDBytes           = 256
	MaxWireIdentifierBytes   = 128
	MaxPayloadProfileBytes   = 64
	MaxPrivateReferenceBytes = 4 << 10
	MaxEncryptionRefBytes    = 256
	MaxCredentialProofBytes  = 4 << 10
	MaxClockUncertainty      = time.Second
)

var maximumWireTime = time.Date(9999, 12, 31, 23, 59, 59, 999_000_000, time.UTC)

// ValidateEnvelope validates a complete private wire envelope before policy,
// dedupe or handler delivery.
func ValidateEnvelope(envelope Envelope) error {
	return validateEnvelope(envelope, true)
}

// ValidRequestTimeWindow reports whether a request's creation and expiry
// timestamps can be represented exactly by the private millisecond UTC wire
// contract and describe a positive lifetime.
func ValidRequestTimeWindow(createdAt, expiresAt time.Time) bool {
	return validWireTime(createdAt) && validWireTime(expiresAt) && expiresAt.After(createdAt)
}

func validWireTime(value time.Time) bool {
	return !value.IsZero() && !value.Before(time.Unix(0, 0)) && !value.After(maximumWireTime) && value.Location() == time.UTC && value.Nanosecond()%int(time.Millisecond) == 0
}

func validateEnvelope(envelope Envelope, requireProof bool) error {
	if envelope.Version != Version2 {
		return &FieldError{Field: "version", Cause: ErrUnsupportedVersion}
	}
	if err := validateTypedIdentifier("message_id", envelope.MessageID, "msg_", 16); err != nil {
		return err
	}
	if err := validateTypedIdentifier("conversation_id", envelope.ConversationID, "conv_", sha256.Size); err != nil {
		return err
	}
	if err := validateOpaqueIdentity("sender", envelope.Sender, MaxAgentIdentityBytes); err != nil {
		return err
	}
	if err := validateOpaqueIdentity("recipient", envelope.Recipient, MaxAgentIdentityBytes); err != nil {
		return err
	}
	if err := validateOpaqueIdentity("mesh_id", envelope.MeshID, MaxMeshIDBytes); err != nil {
		return err
	}
	if err := envelope.Mode.Validate(); err != nil {
		return &FieldError{Field: "mode", Cause: err}
	}
	if err := validateModeMetadata(envelope); err != nil {
		return err
	}
	if !validWireTime(envelope.CreatedAt) {
		return &FieldError{Field: "created_at", Cause: ErrInvalidEnvelope}
	}
	if envelope.Mode == ModeRequest {
		if !ValidRequestTimeWindow(envelope.CreatedAt, envelope.ExpiresAt) {
			return &FieldError{Field: "expires_at", Cause: ErrInvalidEnvelope}
		}
	} else if !envelope.ExpiresAt.IsZero() {
		return &FieldError{Field: "expires_at", Cause: ErrInvalidEnvelope}
	}
	if envelope.ClockUncertainty <= 0 || envelope.ClockUncertainty > MaxClockUncertainty || envelope.ClockUncertainty%time.Microsecond != 0 {
		return &FieldError{Field: "clock_uncertainty", Cause: ErrInvalidEnvelope}
	}
	if err := ValidatePayloadDescriptor(envelope.Payload); err != nil {
		return err
	}
	// CredentialProof is a bounded in-memory migration member. V1 encoders
	// never serialize it and authenticated messaging rejects it at ingress.
	if len(envelope.CredentialProof) > MaxCredentialProofBytes {
		return &FieldError{Field: "credential_proof", Cause: ErrInvalidEnvelope}
	}
	_ = requireProof
	return nil
}

func validateModeMetadata(envelope Envelope) error {
	switch envelope.Mode {
	case ModeMessage:
		if envelope.CorrelationID != "" || envelope.ReplyTo != "" {
			return &FieldError{Field: "rpc_metadata", Cause: ErrInvalidEnvelope}
		}
	case ModeRequest:
		if err := validateTypedIdentifier("correlation_id", envelope.CorrelationID, "cor_", 16); err != nil {
			return err
		}
		if envelope.ReplyTo != "" {
			return &FieldError{Field: "reply_to", Cause: ErrInvalidEnvelope}
		}
	case ModeResponse:
		if err := validateTypedIdentifier("correlation_id", envelope.CorrelationID, "cor_", 16); err != nil {
			return err
		}
		if err := validateTypedIdentifier("reply_to", envelope.ReplyTo, "msg_", 16); err != nil {
			return err
		}
	default:
		return &FieldError{Field: "mode", Cause: ErrInvalidMode}
	}
	return nil
}

// ValidatePayloadDescriptor enforces the frozen V1 descriptor contract.
func ValidatePayloadDescriptor(payload PayloadDescriptor) error {
	if err := validateOpaqueIdentity("payload.profile", payload.Profile, MaxPayloadProfileBytes); err != nil {
		return errors.Join(ErrInvalidPayload, err)
	}
	if payload.Size < 0 {
		return &FieldError{Field: "payload.size", Cause: ErrInvalidPayload}
	}
	switch payload.Kind {
	case PayloadInline:
		if len(payload.Inline) > MaxInlinePayloadBytes || payload.Size != int64(len(payload.Inline)) || payload.Reference != "" || payload.EncryptionRef != "" || sha256.Sum256(payload.Inline) != payload.Digest {
			return &FieldError{Field: "payload.inline", Cause: ErrInvalidPayload}
		}
	case PayloadObjectReference, PayloadTransferReference:
		if len(payload.Inline) != 0 || payload.Reference == "" || len(payload.Reference) > MaxPrivateReferenceBytes || !validText(payload.Reference) || len(payload.EncryptionRef) > MaxEncryptionRefBytes || !validOptionalText(payload.EncryptionRef) {
			return &FieldError{Field: "payload.reference", Cause: ErrInvalidPayload}
		}
	default:
		return &FieldError{Field: "payload.kind", Cause: ErrInvalidPayload}
	}
	return nil
}

func validateTypedIdentifier(field, value, prefix string, entropyBytes int) error {
	if len(value) > MaxWireIdentifierBytes || !strings.HasPrefix(value, prefix) {
		return &FieldError{Field: field, Cause: ErrInvalidIdentifier}
	}
	encoded := value[len(prefix):]
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != entropyBytes || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return &FieldError{Field: field, Cause: ErrInvalidIdentifier}
	}
	return nil
}

func validateOpaqueIdentity(field, value string, limit int) error {
	if value == "" || len(value) > limit || !validText(value) {
		return &FieldError{Field: field, Cause: ErrInvalidIdentifier}
	}
	return nil
}

// ValidateAgentIdentity validates one opaque canonical participant identity.
// It does not parse, case-fold, or otherwise reinterpret the trusted string.
func ValidateAgentIdentity(value string) error {
	return validateOpaqueIdentity("agent_identity", value, MaxAgentIdentityBytes)
}

// ValidateMeshID validates the authenticated session mesh identifier without
// defining how a server binds or routes that mesh.
func ValidateMeshID(value string) error {
	return validateOpaqueIdentity("mesh_id", value, MaxMeshIDBytes)
}

// ValidateConversationID validates the exact private conv_ plus SHA-256 form.
func ValidateConversationID(value string) error {
	return validateTypedIdentifier("conversation_id", value, "conv_", sha256.Size)
}

func validOptionalText(value string) bool {
	return value == "" || validText(value)
}

func validText(value string) bool {
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func checkedUnixMillis(createdAt time.Time) (int64, error) {
	if createdAt.Before(time.Unix(0, 0)) || createdAt.After(maximumWireTime) {
		return 0, ErrInvalidEnvelope
	}
	return createdAt.UnixMilli(), nil
}
