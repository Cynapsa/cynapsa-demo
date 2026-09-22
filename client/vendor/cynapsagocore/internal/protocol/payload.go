package protocol

import (
	"crypto/sha256"
	"errors"
	"strings"
)

// PayloadKind is a carrier-neutral private descriptor kind.
type PayloadKind uint8

const (
	PayloadInline PayloadKind = iota
	PayloadObjectReference
	PayloadTransferReference
)

// PayloadDescriptor binds one canonical payload digest to either inline bytes
// or a private object/transfer reference. References never cross the SDK
// boundary and their interpretation belongs to the payload system.
type PayloadDescriptor struct {
	Kind          PayloadKind
	Profile       string
	Inline        []byte
	Reference     string
	Size          int64
	Digest        [sha256.Size]byte
	EncryptionRef string
}

// NewInlinePayload snapshots canonical bytes and their V1 SHA-256 identity.
func NewInlinePayload(profile string, canonical []byte) (PayloadDescriptor, error) {
	if len(canonical) > MaxInlinePayloadBytes {
		return PayloadDescriptor{}, &FieldError{Field: "payload.inline", Cause: ErrInvalidPayload}
	}
	if err := validateOpaqueIdentity("payload.profile", profile, MaxPayloadProfileBytes); err != nil {
		return PayloadDescriptor{}, errors.Join(ErrInvalidPayload, err)
	}
	descriptor := PayloadDescriptor{
		Kind:    PayloadInline,
		Profile: profile,
		Inline:  append([]byte(nil), canonical...),
		Size:    int64(len(canonical)),
		Digest:  sha256.Sum256(canonical),
	}
	if err := ValidatePayloadDescriptor(descriptor); err != nil {
		return PayloadDescriptor{}, err
	}
	return descriptor, nil
}

// NewReferencedPayload creates a bounded private descriptor for canonical
// bytes materialized by Pod 5. Size is descriptive and must not be allocated
// before Pod 5 applies its independently configured payload limit.
func NewReferencedPayload(kind PayloadKind, profile, reference string, size int64, digest [sha256.Size]byte, encryptionRef string) (PayloadDescriptor, error) {
	descriptor := PayloadDescriptor{
		Kind:          kind,
		Profile:       profile,
		Reference:     reference,
		Size:          size,
		Digest:        digest,
		EncryptionRef: encryptionRef,
	}
	if err := ValidatePayloadDescriptor(descriptor); err != nil {
		return PayloadDescriptor{}, err
	}
	return descriptor, nil
}

// Materialize returns an ownership-safe copy only for an inline descriptor.
// Object and transfer I/O is deliberately owned by internal/payload.
func (p PayloadDescriptor) Materialize() ([]byte, error) {
	if err := ValidatePayloadDescriptor(p); err != nil {
		return nil, err
	}
	if p.Kind != PayloadInline {
		return nil, ErrPayloadNotMaterialized
	}
	return append([]byte(nil), p.Inline...), nil
}

// IsMaterialized reports whether canonical bytes are already carried inline.
func (p PayloadDescriptor) IsMaterialized() bool {
	return p.Kind == PayloadInline
}

func (p PayloadDescriptor) clone() PayloadDescriptor {
	p.Profile = strings.Clone(p.Profile)
	p.Reference = strings.Clone(p.Reference)
	p.EncryptionRef = strings.Clone(p.EncryptionRef)
	p.Inline = append([]byte(nil), p.Inline...)
	return p
}

var (
	ErrPayloadNotMaterialized = errors.New("protocol: referenced payload is not materialized")
	ErrPayloadIntegrity       = errors.New("protocol: payload integrity mismatch")
)
