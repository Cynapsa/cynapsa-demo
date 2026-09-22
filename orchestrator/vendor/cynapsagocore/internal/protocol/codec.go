package protocol

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// Codec serializes and parses private AZTM envelopes.
type Codec interface {
	Encode(Envelope) ([]byte, error)
	Decode([]byte) (Envelope, error)
	EncodeForIntegrity(Envelope) ([]byte, error)
}

type canonicalCodec struct {
	encode cbor.EncMode
	decode cbor.DecMode
}

var (
	defaultCodecOnce sync.Once
	defaultCodec     Codec
	defaultCodecErr  error
)

// Version 2 deliberately leaves the legacy sequence (key 6), proof (key 12),
// and mesh-generation (key 15) positions unused. Unknown-field rejection makes
// this a fail-closed cut instead of a mixed-version interpretation.
type wireEnvelopeV2 struct {
	Version            uint16        `cbor:"0,keyasint"`
	MessageID          string        `cbor:"1,keyasint"`
	ConversationID     string        `cbor:"2,keyasint"`
	Sender             string        `cbor:"3,keyasint"`
	Recipient          string        `cbor:"4,keyasint"`
	MeshID             string        `cbor:"5,keyasint"`
	Mode               uint8         `cbor:"7,keyasint"`
	CorrelationID      string        `cbor:"8,keyasint,omitempty"`
	ReplyTo            string        `cbor:"9,keyasint,omitempty"`
	CreatedAtMS        int64         `cbor:"10,keyasint"`
	Payload            wirePayloadV1 `cbor:"11,keyasint"`
	Proof              []byte        `cbor:"12,keyasint,omitempty"`
	ExpiresAtMS        int64         `cbor:"13,keyasint,omitempty"`
	ClockUncertaintyUS uint64        `cbor:"14,keyasint"`
}

type wirePayloadV1 struct {
	Kind          uint8  `cbor:"0,keyasint"`
	Profile       string `cbor:"1,keyasint"`
	Inline        []byte `cbor:"2,keyasint,omitempty"`
	Reference     string `cbor:"3,keyasint,omitempty"`
	Size          uint64 `cbor:"4,keyasint"`
	Digest        []byte `cbor:"5,keyasint"`
	EncryptionRef string `cbor:"6,keyasint,omitempty"`
}

// logicalEnvelopeV2 is the carrier-invariant message identity used only for
// replay, dedupe, and message-ID conflict checks. Carrier kind, inline bytes,
// private references, encryption references, and carrier provenance are deliberately not
// present. Canonical size and SHA-256 bind the same payload across carriers.
type logicalEnvelopeV2 struct {
	Version            uint16 `cbor:"0,keyasint"`
	MessageID          string `cbor:"1,keyasint"`
	ConversationID     string `cbor:"2,keyasint"`
	Sender             string `cbor:"3,keyasint"`
	Recipient          string `cbor:"4,keyasint"`
	MeshID             string `cbor:"5,keyasint"`
	Mode               uint8  `cbor:"7,keyasint"`
	CorrelationID      string `cbor:"8,keyasint,omitempty"`
	ReplyTo            string `cbor:"9,keyasint,omitempty"`
	CreatedAtMS        int64  `cbor:"10,keyasint"`
	PayloadProfile     string `cbor:"11,keyasint"`
	PayloadSize        uint64 `cbor:"12,keyasint"`
	PayloadDigest      []byte `cbor:"13,keyasint"`
	ExpiresAtMS        int64  `cbor:"14,keyasint,omitempty"`
	ClockUncertaintyUS uint64 `cbor:"15,keyasint"`
}

// NewCodec creates the strict deterministic RFC 8949 private version 2 codec.
func NewCodec() (Codec, error) {
	defaultCodecOnce.Do(func() {
		defaultCodec, defaultCodecErr = newCanonicalCodec()
	})
	return defaultCodec, defaultCodecErr
}

func newCanonicalCodec() (Codec, error) {
	encodeOptions := cbor.CoreDetEncOptions()
	encodeOptions.IndefLength = cbor.IndefLengthForbidden
	encodeOptions.TagsMd = cbor.TagsForbidden
	encodeMode, err := encodeOptions.EncMode()
	if err != nil {
		return nil, errors.Join(ErrMalformedEncoding, err)
	}
	decodeMode, err := (cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		MaxNestedLevels:   4,
		MaxArrayElements:  16,
		MaxMapPairs:       16,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
		UTF8:              cbor.UTF8RejectInvalid,
	}).DecMode()
	if err != nil {
		return nil, errors.Join(ErrMalformedEncoding, err)
	}
	return &canonicalCodec{encode: encodeMode, decode: decodeMode}, nil
}

func (c *canonicalCodec) Encode(envelope Envelope) ([]byte, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	return c.encodeWire(envelope, true)
}

func (c *canonicalCodec) EncodeForIntegrity(envelope Envelope) ([]byte, error) {
	if err := validateEnvelope(envelope, false); err != nil {
		return nil, err
	}
	return c.encodeWire(envelope, false)
}

func (c *canonicalCodec) encodeWire(envelope Envelope, includeProof bool) ([]byte, error) {
	wire, err := envelopeToWire(envelope, includeProof)
	if err != nil {
		return nil, err
	}
	defer clearWireEnvelope(&wire)
	encoded, err := c.encode.Marshal(wire)
	if err != nil {
		clear(encoded)
		return nil, ErrMalformedEncoding
	}
	if len(encoded) > MaxEnvelopeBytes {
		clear(encoded)
		return nil, ErrEnvelopeTooLarge
	}
	return encoded, nil
}

func clearWireEnvelope(wire *wireEnvelopeV2) {
	if wire == nil {
		return
	}
	clear(wire.Payload.Inline)
	clear(wire.Payload.Digest)
	clear(wire.Proof)
	wire.Payload.Inline = nil
	wire.Payload.Digest = nil
	wire.Proof = nil
}

func (c *canonicalCodec) Decode(encoded []byte) (Envelope, error) {
	if len(encoded) == 0 {
		return Envelope{}, ErrMalformedEncoding
	}
	if len(encoded) > MaxEnvelopeBytes {
		return Envelope{}, ErrEnvelopeTooLarge
	}
	var wire wireEnvelopeV2
	defer clearWireEnvelope(&wire)
	if err := c.decode.Unmarshal(encoded, &wire); err != nil {
		return Envelope{}, ErrMalformedEncoding
	}
	envelope, err := wireToEnvelope(wire)
	if err != nil {
		return Envelope{}, err
	}
	return c.validateDecodedEnvelope(envelope, encoded)
}

// validateDecodedEnvelope owns envelope's byte slices until every canonical
// validation step succeeds. A decoded value is transferred to the caller only
// on success; all failure and panic paths clear the independently allocated
// payload bytes first.
func (c *canonicalCodec) validateDecodedEnvelope(envelope Envelope, encoded []byte) (Envelope, error) {
	return c.validateDecodedEnvelopeWith(envelope, encoded, ValidateEnvelope)
}

func (c *canonicalCodec) validateDecodedEnvelopeWith(envelope Envelope, encoded []byte, validate func(Envelope) error) (Envelope, error) {
	valid := false
	defer func() {
		if !valid {
			clearEnvelopeOwned(&envelope)
		}
	}()
	if validate == nil {
		return Envelope{}, ErrMalformedEncoding
	}
	if err := validate(envelope); err != nil {
		return Envelope{}, err
	}
	reencoded, err := c.encodeWire(envelope, true)
	if err != nil {
		return Envelope{}, err
	}
	defer clear(reencoded)
	if !bytes.Equal(encoded, reencoded) {
		return Envelope{}, ErrNonCanonical
	}
	valid = true
	return envelope, nil
}

func clearEnvelopeOwned(envelope *Envelope) {
	if envelope == nil {
		return
	}
	clear(envelope.Payload.Inline)
	clear(envelope.Payload.Digest[:])
	clear(envelope.CredentialProof)
	*envelope = Envelope{}
}

func envelopeToWire(envelope Envelope, includeProof bool) (wireEnvelopeV2, error) {
	mode, err := envelope.Mode.wireValue()
	if err != nil {
		return wireEnvelopeV2{}, err
	}
	createdAtMS, err := checkedUnixMillis(envelope.CreatedAt)
	if err != nil {
		return wireEnvelopeV2{}, err
	}
	expiresAtMS := int64(0)
	if !envelope.ExpiresAt.IsZero() {
		expiresAtMS, err = checkedUnixMillis(envelope.ExpiresAt)
		if err != nil {
			return wireEnvelopeV2{}, err
		}
	}
	wire := wireEnvelopeV2{
		Version:            envelope.Version,
		MessageID:          envelope.MessageID,
		ConversationID:     envelope.ConversationID,
		Sender:             envelope.Sender,
		Recipient:          envelope.Recipient,
		MeshID:             envelope.MeshID,
		Mode:               mode,
		CorrelationID:      envelope.CorrelationID,
		ReplyTo:            envelope.ReplyTo,
		CreatedAtMS:        createdAtMS,
		ExpiresAtMS:        expiresAtMS,
		ClockUncertaintyUS: uint64(envelope.ClockUncertainty / time.Microsecond),
		Payload: wirePayloadV1{
			Kind:          uint8(envelope.Payload.Kind),
			Profile:       envelope.Payload.Profile,
			Inline:        append([]byte(nil), envelope.Payload.Inline...),
			Reference:     envelope.Payload.Reference,
			Size:          uint64(envelope.Payload.Size),
			Digest:        append([]byte(nil), envelope.Payload.Digest[:]...),
			EncryptionRef: envelope.Payload.EncryptionRef,
		},
	}
	_ = includeProof // map key 12 is reserved and never emitted in V2.
	return wire, nil
}

func wireToEnvelope(wire wireEnvelopeV2) (Envelope, error) {
	if wire.SizeOverflow() || len(wire.Payload.Digest) != sha256.Size {
		return Envelope{}, &FieldError{Field: "payload", Cause: ErrInvalidPayload}
	}
	if wire.ClockUncertaintyUS == 0 || wire.ClockUncertaintyUS > uint64(MaxClockUncertainty/time.Microsecond) {
		return Envelope{}, &FieldError{Field: "clock_uncertainty", Cause: ErrInvalidEnvelope}
	}
	mode, err := modeFromWire(wire.Mode)
	if err != nil {
		return Envelope{}, &FieldError{Field: "mode", Cause: err}
	}
	var digest [sha256.Size]byte
	copy(digest[:], wire.Payload.Digest)
	expiresAt := time.Time{}
	if wire.ExpiresAtMS != 0 {
		expiresAt = time.UnixMilli(wire.ExpiresAtMS).UTC()
	}
	return Envelope{
		Version:          wire.Version,
		MessageID:        wire.MessageID,
		ConversationID:   wire.ConversationID,
		Sender:           wire.Sender,
		Recipient:        wire.Recipient,
		MeshID:           wire.MeshID,
		Mode:             mode,
		CorrelationID:    wire.CorrelationID,
		ReplyTo:          wire.ReplyTo,
		CreatedAt:        time.UnixMilli(wire.CreatedAtMS).UTC(),
		ExpiresAt:        expiresAt,
		ClockUncertainty: time.Duration(wire.ClockUncertaintyUS) * time.Microsecond,
		Payload: PayloadDescriptor{
			Kind:          PayloadKind(wire.Payload.Kind),
			Profile:       wire.Payload.Profile,
			Inline:        append([]byte(nil), wire.Payload.Inline...),
			Reference:     wire.Payload.Reference,
			Size:          int64(wire.Payload.Size),
			Digest:        digest,
			EncryptionRef: wire.Payload.EncryptionRef,
		},
		CredentialProof: append([]byte(nil), wire.Proof...),
	}, nil
}

func (wire wireEnvelopeV2) SizeOverflow() bool {
	return wire.Payload.Size > math.MaxInt64
}

// CanonicalIntegrityBytes returns the complete deterministic version 2 envelope.
// Reserved map key 12 and out-of-band carrier provenance are absent.
func CanonicalIntegrityBytes(envelope Envelope) ([]byte, error) {
	codec, err := NewCodec()
	if err != nil {
		return nil, err
	}
	return codec.EncodeForIntegrity(envelope)
}

// EnvelopeIntegrityDigest covers the exact carrier envelope.
// It changes when a private carrier descriptor changes.
func EnvelopeIntegrityDigest(envelope Envelope) ([sha256.Size]byte, error) {
	canonical, err := CanonicalIntegrityBytes(envelope)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer clear(canonical)
	return sha256.Sum256(canonical), nil
}

// LogicalMessageDigest is the carrier-invariant replay/conflict identity. It
// binds stable application metadata plus canonical payload size and SHA-256,
// while excluding all storage and transfer representation.
func LogicalMessageDigest(envelope Envelope) ([sha256.Size]byte, error) {
	if err := validateEnvelope(envelope, false); err != nil {
		return [sha256.Size]byte{}, err
	}
	mode, err := envelope.Mode.wireValue()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	createdAtMS, err := checkedUnixMillis(envelope.CreatedAt)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	codec, err := NewCodec()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	canonical, ok := codec.(*canonicalCodec)
	if !ok {
		return [sha256.Size]byte{}, ErrMalformedEncoding
	}
	expiresAtMS := int64(0)
	if !envelope.ExpiresAt.IsZero() {
		expiresAtMS, err = checkedUnixMillis(envelope.ExpiresAt)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	wire := logicalEnvelopeV2{
		Version:            envelope.Version,
		MessageID:          envelope.MessageID,
		ConversationID:     envelope.ConversationID,
		Sender:             envelope.Sender,
		Recipient:          envelope.Recipient,
		MeshID:             envelope.MeshID,
		Mode:               mode,
		CorrelationID:      envelope.CorrelationID,
		ReplyTo:            envelope.ReplyTo,
		CreatedAtMS:        createdAtMS,
		PayloadProfile:     envelope.Payload.Profile,
		PayloadSize:        uint64(envelope.Payload.Size),
		PayloadDigest:      append([]byte(nil), envelope.Payload.Digest[:]...),
		ExpiresAtMS:        expiresAtMS,
		ClockUncertaintyUS: uint64(envelope.ClockUncertainty / time.Microsecond),
	}
	defer clear(wire.PayloadDigest)
	encoded, err := canonical.encode.Marshal(wire)
	if err != nil {
		clear(encoded)
		return [sha256.Size]byte{}, ErrMalformedEncoding
	}
	defer clear(encoded)
	return sha256.Sum256(encoded), nil
}
