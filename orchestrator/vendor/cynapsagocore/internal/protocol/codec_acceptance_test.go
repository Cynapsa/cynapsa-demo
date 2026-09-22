package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

const qaGoldenEnvelopeHex = goldenEnvelopeHex

func qaGoldenEnvelope(t *testing.T) []byte {
	t.Helper()
	encoded, err := hex.DecodeString(qaGoldenEnvelopeHex)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestQACodecGoldenVectorAndCanonicalIntegrity(t *testing.T) {
	codec, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	golden := qaGoldenEnvelope(t)
	envelope, err := codec.Decode(golden)
	if err != nil {
		t.Fatalf("decode accepted vector: %v", err)
	}
	reencoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatalf("encode accepted vector: %v", err)
	}
	if !bytes.Equal(reencoded, golden) {
		t.Fatalf("canonical bytes changed\n got: %x\nwant: %x", reencoded, golden)
	}

	integrity, err := EnvelopeIntegrityDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	wantIntegrity, _ := hex.DecodeString(goldenIntegrityDigestHex)
	if !bytes.Equal(integrity[:], wantIntegrity) {
		t.Fatalf("integrity digest=%x", integrity)
	}
	logical, err := LogicalMessageDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	wantLogical, _ := hex.DecodeString(goldenLogicalDigestHex)
	if !bytes.Equal(logical[:], wantLogical) {
		t.Fatalf("logical digest=%x", logical)
	}
}

func TestQACodecRejectsEveryTruncationAndNonCanonicalForm(t *testing.T) {
	codec, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	golden := qaGoldenEnvelope(t)
	for length := 0; length < len(golden); length++ {
		if _, err := codec.Decode(golden[:length]); err == nil {
			t.Fatalf("accepted truncated prefix length %d", length)
		}
	}

	nonMinimalMapLength := append([]byte{0xb8, 0x0c}, golden[1:]...)
	unknownField := append(append([]byte{0xad}, golden[1:]...), 0x0f, 0x00)
	duplicateField := append(append([]byte{0xad}, golden[1:]...), 0x00, 0x01)
	reservedProofEmpty := append(append([]byte{0xad}, golden[1:]...), 0x0c, 0x40)
	reservedProofNonempty := append(append([]byte{0xad}, golden[1:]...), 0x0c, 0x41, 0x01)
	malformed := map[string][]byte{
		"trailing data":            append(append([]byte(nil), golden...), 0x00),
		"tagged envelope":          append([]byte{0xc0}, golden...),
		"top-level array":          {0x80},
		"indefinite empty map":     {0xbf, 0xff},
		"non-minimal map":          nonMinimalMapLength,
		"unknown field":            unknownField,
		"duplicate field":          duplicateField,
		"reserved key 12 empty":    reservedProofEmpty,
		"reserved key 12 nonempty": reservedProofNonempty,
	}
	for name, encoded := range malformed {
		t.Run(name, func(t *testing.T) {
			if _, err := codec.Decode(encoded); err == nil {
				t.Fatal("accepted malformed or non-canonical CBOR")
			}
		})
	}

	oversized := make([]byte, MaxEnvelopeBytes+1)
	if _, err := codec.Decode(oversized); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("oversized decode error=%v", err)
	}
}

func TestQAIdentifierCanonicalFormsAndSecretFreeErrors(t *testing.T) {
	validMessage := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0xa5}, 16))
	validCorrelation := "cor_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 16))
	validConversation := "conv_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x33}, sha256.Size))
	if err := validateTypedIdentifier("message_id", validMessage, "msg_", 16); err != nil {
		t.Fatal(err)
	}
	if err := validateTypedIdentifier("correlation_id", validCorrelation, "cor_", 16); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConversationID(validConversation); err != nil {
		t.Fatal(err)
	}

	privateCanary := "PRIVATE-CREDENTIAL-CANARY"
	invalid := []string{
		validMessage + "=",
		strings.Replace(validMessage, "msg_", "cor_", 1),
		"msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 15)),
		"msg_" + privateCanary,
	}
	for _, candidate := range invalid {
		err := validateTypedIdentifier("message_id", candidate, "msg_", 16)
		if err == nil {
			t.Fatalf("accepted %q", candidate)
		}
		if strings.Contains(err.Error(), candidate) || strings.Contains(err.Error(), privateCanary) {
			t.Fatalf("error leaked rejected value: %v", err)
		}
	}
}

func TestQAPayloadDescriptorBoundsAndCarrierInvariantIdentity(t *testing.T) {
	canonical := []byte("same canonical payload")
	digest := sha256.Sum256(canonical)
	inline, err := NewInlinePayload("aztm.native", canonical)
	if err != nil {
		t.Fatal(err)
	}
	object, err := NewReferencedPayload(PayloadObjectReference, "aztm.native", strings.Repeat("o", MaxPrivateReferenceBytes), int64(len(canonical)), digest, strings.Repeat("e", MaxEncryptionRefBytes))
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := NewReferencedPayload(PayloadTransferReference, "aztm.native", "transfer-private-reference", int64(len(canonical)), digest, "transfer-encryption")
	if err != nil {
		t.Fatal(err)
	}

	base := qaProtocolEnvelope(t, inline)
	digests := make([][sha256.Size]byte, 0, 3)
	for _, payload := range []PayloadDescriptor{inline, object, transfer} {
		candidate := base.Clone()
		candidate.Payload = payload
		logical, err := LogicalMessageDigest(candidate)
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, logical)
	}
	if digests[0] != digests[1] || digests[1] != digests[2] {
		t.Fatal("carrier representation changed logical identity")
	}

	tooLongReference := object
	tooLongReference.Reference += "x"
	if err := ValidatePayloadDescriptor(tooLongReference); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("oversized reference error=%v", err)
	}
	tooLongEncryption := object
	tooLongEncryption.EncryptionRef += "x"
	if err := ValidatePayloadDescriptor(tooLongEncryption); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("oversized encryption reference error=%v", err)
	}
	tamperedInline := inline
	tamperedInline.Inline = append([]byte(nil), inline.Inline...)
	tamperedInline.Inline[0] ^= 1
	if err := ValidatePayloadDescriptor(tamperedInline); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("tampered inline error=%v", err)
	}
}

func qaProtocolEnvelope(t *testing.T, payload PayloadDescriptor) Envelope {
	t.Helper()
	goldenCodec, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := goldenCodec.Decode(qaGoldenEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	envelope.Payload = payload
	return envelope
}
