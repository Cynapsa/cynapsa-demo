package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

const goldenEnvelopeHex = goldenIntegrityHex
const goldenIntegrityHex = "ac000201781a6d73675f41414141414141414141414141414141414141414141027830636f6e765f7153434139334449416d7a464639507a4d537a3549674d334b795570706a696c5f46726d58545f4a654d3403781d6167656e742d6140786d70702e6578616d706c652f6d6573682d31323304781d6167656e742d6240786d70702e6578616d706c652f6d6573682d31323305686d6573682d313233070108781a636f725f434343434343434343434343434343434343434343410a1b0000019ff1e06e030ba50000016b617a746d2e6e61746976650258187b226f70223a2273756d222c2261223a312c2262223a327d0418180558202fa85d7d03a324260269ac10b2ea2b3df4d930a221004382eed3c773980944100d1b0000019ff1e0e3330e1a0003d090"
const goldenIntegrityDigestHex = "ce26fb5b65ecb42d37be45d9a650d4d0ae48311e9c6aae36455590da09cb7252"
const goldenLogicalDigestHex = "1969aaa189863ee421318afaaa361e3d22af7de4071c2aa36a957ff69d100e0d"

func testEnvelope(t *testing.T) Envelope {
	t.Helper()
	payload, err := NewInlinePayload("aztm.native", []byte(`{"op":"sum","a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	return Envelope{
		Version:        Version2,
		MessageID:      "msg_AAAAAAAAAAAAAAAAAAAAAA",
		ConversationID: "conv_qSCA93DIAmzFF9PzMSz5IgM3KyUppjil_FrmXT_JeM4",
		Sender:         "agent-a@xmpp.example/mesh-123",
		Recipient:      "agent-b@xmpp.example/mesh-123",
		MeshID:         "mesh-123", Mode: ModeRequest,
		CorrelationID:    "cor_CCCCCCCCCCCCCCCCCCCCCA",
		CreatedAt:        time.Date(2026, 8, 11, 17, 30, 45, 123_000_000, time.UTC),
		ExpiresAt:        time.Date(2026, 8, 11, 17, 31, 15, 123_000_000, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
	}
}

func testTypedID(prefix, seed string, size int) string {
	digest := sha256.Sum256([]byte(seed))
	return prefix + base64.RawURLEncoding.EncodeToString(digest[:size])
}

func testMessageID(seed string) string { return testTypedID("msg_", seed, 16) }

func testCorrelationID(seed string) string { return testTypedID("cor_", seed, 16) }

func testConversationID(seed string) string { return testTypedID("conv_", seed, sha256.Size) }

func mustCodec(t *testing.T) Codec {
	t.Helper()
	codec, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func TestCodecGoldenVector(t *testing.T) {
	codec := mustCodec(t)
	envelope := testEnvelope(t)
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	actualHex := hex.EncodeToString(encoded)
	if actualHex != goldenEnvelopeHex {
		t.Fatalf("golden mismatch\ngot:  %s\nwant: %s", actualHex, goldenEnvelopeHex)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, envelope) {
		t.Fatalf("round trip mismatch\ngot:  %#v\nwant: %#v", decoded, envelope)
	}
	reencoded, err := codec.Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("deterministic re-encoding changed bytes")
	}
	integrity, err := codec.EncodeForIntegrity(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if actual := hex.EncodeToString(integrity); actual != goldenIntegrityHex {
		t.Fatalf("integrity golden mismatch\ngot:  %s\nwant: %s", actual, goldenIntegrityHex)
	}
	digest, err := EnvelopeIntegrityDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if actual := hex.EncodeToString(digest[:]); actual != goldenIntegrityDigestHex {
		t.Fatalf("integrity digest mismatch: got %s want %s", actual, goldenIntegrityDigestHex)
	}
	logical, err := LogicalMessageDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if actual := hex.EncodeToString(logical[:]); actual != goldenLogicalDigestHex {
		t.Fatalf("logical digest mismatch: got %s want %s", actual, goldenLogicalDigestHex)
	}
}

func TestCodecStrictMalformedBoundaries(t *testing.T) {
	codec := mustCodec(t)
	canonical, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}

	for length := 0; length < len(canonical); length++ {
		if _, err := codec.Decode(canonical[:length]); err == nil {
			t.Fatalf("accepted truncated envelope at length %d", length)
		}
	}

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "trailing", data: append(append([]byte(nil), canonical...), 0xf6), want: ErrMalformedEncoding},
		{name: "tag", data: append([]byte{0xc0}, canonical...), want: ErrMalformedEncoding},
		{name: "oversized", data: make([]byte, MaxEnvelopeBytes+1), want: ErrEnvelopeTooLarge},
	}

	duplicate := append([]byte(nil), canonical...)
	duplicate[0]++ // the canonical fixture has a one-byte, 12-pair map header
	duplicate = append(duplicate, 0x00, 0x01)
	tests = append(tests, struct {
		name string
		data []byte
		want error
	}{name: "duplicate field", data: duplicate, want: ErrMalformedEncoding})

	indefinite := append([]byte(nil), canonical...)
	indefinite[0] = 0xbf
	indefinite = append(indefinite, 0xff)
	tests = append(tests, struct {
		name string
		data []byte
		want error
	}{name: "indefinite map", data: indefinite, want: ErrMalformedEncoding})

	nonMinimal := make([]byte, 0, len(canonical)+1)
	nonMinimal = append(nonMinimal, canonical[:2]...)
	nonMinimal = append(nonMinimal, 0x18, canonical[2])
	nonMinimal = append(nonMinimal, canonical[3:]...)
	tests = append(tests, struct {
		name string
		data []byte
		want error
	}{name: "non-minimal integer", data: nonMinimal, want: ErrNonCanonical})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := codec.Decode(test.data)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
		})
	}
}

func TestCodecRejectsUnknownFieldAndVersion(t *testing.T) {
	codec := mustCodec(t).(*canonicalCodec)
	envelope := testEnvelope(t)
	wire, err := envelopeToWire(envelope, true)
	if err != nil {
		t.Fatal(err)
	}

	wire.Version = 1
	unsupported, err := codec.encode.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(unsupported); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("unsupported version: got %v", err)
	}

	valid, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[uint64]any
	if err := codec.decode.Unmarshal(valid, &fields); err != nil {
		t.Fatal(err)
	}
	fields[99] = uint64(1)
	unknown, err := codec.encode.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(unknown); !errors.Is(err, ErrMalformedEncoding) {
		t.Fatalf("unknown field: got %v", err)
	}

	var top map[uint64]cbor.RawMessage
	if err := codec.decode.Unmarshal(valid, &top); err != nil {
		t.Fatal(err)
	}
	var payload map[uint64]cbor.RawMessage
	if err := codec.decode.Unmarshal(top[11], &payload); err != nil {
		t.Fatal(err)
	}
	payload[99] = cbor.RawMessage{0x01}
	top[11], err = codec.encode.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	unknownNested, err := codec.encode.Marshal(top)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(unknownNested); !errors.Is(err, ErrMalformedEncoding) {
		t.Fatalf("unknown nested field: got %v", err)
	}

	var originalTop map[uint64]cbor.RawMessage
	if err := codec.decode.Unmarshal(valid, &originalTop); err != nil {
		t.Fatal(err)
	}
	duplicatePayload := append([]byte(nil), originalTop[11]...)
	duplicatePayload[0]++ // fixture payload is a one-byte, five-pair map header
	duplicatePayload = append(duplicatePayload, 0x00, 0x00)
	originalTop[11] = cbor.RawMessage(duplicatePayload)
	duplicateNested, err := codec.encode.Marshal(originalTop)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(duplicateNested); !errors.Is(err, ErrMalformedEncoding) {
		t.Fatalf("duplicate nested field: got %v", err)
	}
}

func TestCodecOmitsCompatibilityProofAndRejectsInvalidDigestLength(t *testing.T) {
	codec := mustCodec(t).(*canonicalCodec)
	envelope := testEnvelope(t)
	withoutProof, err := codec.EncodeForIntegrity(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(withoutProof); err != nil {
		t.Fatalf("proof-free envelope: got %v", err)
	}
	envelope.CredentialProof = []byte("forbidden")
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatalf("compatibility proof encoding: %v", err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || len(decoded.CredentialProof) != 0 {
		t.Fatalf("compatibility proof reached wire: %#v %v", decoded.CredentialProof, err)
	}
	envelope.CredentialProof = nil

	wire, err := envelopeToWire(envelope, true)
	if err != nil {
		t.Fatal(err)
	}
	wire.Payload.Digest = wire.Payload.Digest[:len(wire.Payload.Digest)-1]
	invalid, err := codec.encode.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(invalid); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("digest length: got %v", err)
	}
}

func TestIntegrityIgnoresCompatibilityProof(t *testing.T) {
	envelope := testEnvelope(t)
	firstDigest, err := EnvelopeIntegrityDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.CredentialProof = []byte("refreshed-private-proof")
	secondDigest, err := EnvelopeIntegrityDigest(envelope)
	if err != nil || secondDigest != firstDigest {
		t.Fatalf("compatibility proof changed integrity: %v", err)
	}
	envelope.CredentialProof = nil

	mutations := []func(*Envelope){
		func(e *Envelope) { e.MessageID = testMessageID("mutated-message") },
		func(e *Envelope) { e.ConversationID = testConversationID("mutated-conversation") },
		func(e *Envelope) { e.Sender = "agent-c@xmpp.example/mesh-123" },
		func(e *Envelope) { e.Recipient = "agent-c@xmpp.example/mesh-123" },
		func(e *Envelope) { e.MeshID = "mesh-456" },
		func(e *Envelope) { e.CorrelationID = testCorrelationID("mutated-correlation") },
		func(e *Envelope) { e.CreatedAt = e.CreatedAt.Add(time.Millisecond) },
		func(e *Envelope) { e.ExpiresAt = e.ExpiresAt.Add(time.Millisecond) },
		func(e *Envelope) { e.ClockUncertainty += time.Microsecond },
		func(e *Envelope) { e.Payload.Profile = "http.request" },
	}
	for index, mutate := range mutations {
		candidate := envelope.Clone()
		mutate(&candidate)
		digest, err := EnvelopeIntegrityDigest(candidate)
		if err != nil {
			t.Fatalf("mutation %d: %v", index, err)
		}
		if digest == firstDigest {
			t.Fatalf("mutation %d was not covered", index)
		}
	}
}

func TestLogicalMessageDigestIsCarrierInvariant(t *testing.T) {
	canonical := []byte(`{"op":"sum","a":1,"b":2}`)
	inline := testEnvelope(t)
	first, err := LogicalMessageDigest(inline)
	if err != nil {
		t.Fatal(err)
	}
	integrity, err := EnvelopeIntegrityDigest(inline)
	if err != nil {
		t.Fatal(err)
	}

	for _, descriptor := range []PayloadDescriptor{
		mustReferencedPayload(t, PayloadObjectReference, inline.Payload.Profile, "object://private/one", canonical),
		mustReferencedPayload(t, PayloadTransferReference, inline.Payload.Profile, "direct://transfer/one", canonical),
		mustReferencedPayload(t, PayloadTransferReference, inline.Payload.Profile, "xmpp://chunks/one", canonical),
	} {
		candidate := inline.Clone()
		candidate.Payload = descriptor
		logical, err := LogicalMessageDigest(candidate)
		if err != nil {
			t.Fatal(err)
		}
		carrier, err := EnvelopeIntegrityDigest(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if logical != first {
			t.Fatal("carrier representation changed logical digest")
		}
		if carrier == integrity {
			t.Fatal("carrier representation did not change integrity digest")
		}
	}

	mutated := inline.Clone()
	mutated.Payload.Profile = "http.request"
	logical, err := LogicalMessageDigest(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if logical == first {
		t.Fatal("stable application metadata did not change logical digest")
	}
	mutated = inline.Clone()
	mutated.ExpiresAt = mutated.ExpiresAt.Add(time.Millisecond)
	logical, err = LogicalMessageDigest(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if logical == first {
		t.Fatal("request expiry did not change logical digest")
	}
}

func mustReferencedPayload(t *testing.T, kind PayloadKind, profile, reference string, canonical []byte) PayloadDescriptor {
	t.Helper()
	payload, err := NewReferencedPayload(kind, profile, reference, int64(len(canonical)), sha256.Sum256(canonical), "enc-v1")
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestCodecOwnsDecodedBytes(t *testing.T) {
	codec := mustCodec(t)
	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		encoded[i] = 0
	}
	if string(decoded.Payload.Inline) != `{"op":"sum","a":1,"b":2}` {
		t.Fatal("decoded payload aliases caller input")
	}
}

func TestCodecConcurrentUse(t *testing.T) {
	codec := mustCodec(t)
	envelope := testEnvelope(t)
	const goroutines = 32
	const iterations = 100
	var wait sync.WaitGroup
	for range goroutines {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range iterations {
				encoded, err := codec.Encode(envelope)
				if err != nil {
					t.Errorf("Encode: %v", err)
					return
				}
				if _, err := codec.Decode(encoded); err != nil {
					t.Errorf("Decode: %v", err)
					return
				}
			}
		}()
	}
	wait.Wait()
}
