package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"
)

func FuzzQAStrictEnvelopeDecode(f *testing.F) {
	golden, err := hex.DecodeString(qaGoldenEnvelopeHex)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(golden)
	f.Add([]byte{})
	f.Add([]byte{0xbf, 0xff})
	f.Add(append(append([]byte(nil), golden...), 0x00))

	codec, err := NewCodec()
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, encoded []byte) {
		envelope, err := codec.Decode(encoded)
		if err != nil {
			return
		}
		if err := ValidateEnvelope(envelope); err != nil {
			t.Fatalf("decoder returned invalid envelope: %v", err)
		}
		reencoded, err := codec.Encode(envelope)
		if err != nil {
			t.Fatalf("accepted envelope did not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("decoder accepted non-canonical input\ninput=%x\ncanon=%x", encoded, reencoded)
		}
	})
}

func FuzzQACanonicalEnvelopeRoundTrip(f *testing.F) {
	f.Add([]byte(`{"operation":"sum","a":1,"b":2}`), uint64(1), []byte("proof"))
	f.Add([]byte{}, uint64(42), []byte{0, 1, 2, 3})
	codec, err := NewCodec()
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, canonical []byte, sequence uint64, proof []byte) {
		if len(canonical) > 8<<10 || len(proof) > MaxCredentialProofBytes {
			return
		}
		if sequence == 0 {
			sequence = 1
		}
		if len(proof) == 0 {
			proof = []byte{0}
		}
		payload, err := NewInlinePayload("aztm.native", canonical)
		if err != nil {
			t.Fatal(err)
		}
		messageEntropy := sha256.Sum256(append([]byte("message:"), canonical...))
		conversationEntropy := sha256.Sum256(append([]byte("conversation:"), canonical...))
		envelope, err := NewEnvelope(EnvelopeInput{
			MessageID:      "msg_" + base64.RawURLEncoding.EncodeToString(messageEntropy[:16]),
			ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(conversationEntropy[:]),
			Sender:         "sender@xmpp.example/mesh-fuzz",
			Recipient:      "recipient@xmpp.example/mesh-fuzz",
			MeshID:         "mesh-fuzz", Mode: ModeMessage,
			CreatedAt:        time.UnixMilli(1_700_000_000_000).UTC(),
			ClockUncertainty: 250 * time.Millisecond,
			Payload:          payload,
			CredentialProof:  proof,
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := codec.Encode(envelope)
		if err != nil {
			t.Fatal(err)
		}
		withoutCompatibilityProof := envelope.Clone()
		withoutCompatibilityProof.CredentialProof = nil
		proofFree, err := codec.Encode(withoutCompatibilityProof)
		if err != nil || !bytes.Equal(encoded, proofFree) {
			t.Fatalf("compatibility proof reached canonical wire: %v", err)
		}
		decoded, err := codec.Decode(encoded)
		if err != nil {
			t.Fatalf("canonical encoding rejected: %v", err)
		}
		reencoded, err := codec.Encode(decoded)
		if err != nil || !bytes.Equal(encoded, reencoded) || len(decoded.CredentialProof) != 0 {
			t.Fatalf("round trip instability err=%v", err)
		}
		materialized, err := decoded.Payload.Materialize()
		if err != nil || !bytes.Equal(materialized, canonical) {
			t.Fatalf("payload ownership changed err=%v", err)
		}
	})
}

func FuzzQATypedIdentifierValidation(f *testing.F) {
	f.Add("msg_AAAAAAAAAAAAAAAAAAAAAA", "msg_", uint8(16))
	f.Add("cor_AAAAAAAAAAAAAAAAAAAAAA", "cor_", uint8(16))
	f.Add("msg_AAAAAAAAAAAAAAAAAAAAAA=", "msg_", uint8(16))
	f.Fuzz(func(t *testing.T, value, prefix string, entropy uint8) {
		entropyBytes := int(entropy % 65)
		err := validateTypedIdentifier("qa_identifier", value, prefix, entropyBytes)
		if err != nil {
			return
		}
		if len(value) > MaxWireIdentifierBytes || len(value) < len(prefix) || value[:len(prefix)] != prefix {
			t.Fatal("accepted identifier violates structural bounds")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(value[len(prefix):])
		if err != nil || len(decoded) != entropyBytes {
			t.Fatal("accepted identifier has invalid entropy encoding")
		}
		if base64.RawURLEncoding.EncodeToString(decoded) != value[len(prefix):] {
			t.Fatal("accepted identifier has a non-canonical alias")
		}
	})
}
