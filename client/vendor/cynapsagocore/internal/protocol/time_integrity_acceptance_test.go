package protocol

import (
	"bytes"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

func TestQASignedTimeAndUncertaintyAreCanonicalIntegrityBoundaries(t *testing.T) {
	base := testEnvelope(t)
	baseIntegrity, err := EnvelopeIntegrityDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	baseLogical, err := LogicalMessageDigest(base)
	if err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*Envelope){
		"created at":        func(envelope *Envelope) { envelope.CreatedAt = envelope.CreatedAt.Add(time.Millisecond) },
		"request expiry":    func(envelope *Envelope) { envelope.ExpiresAt = envelope.ExpiresAt.Add(time.Millisecond) },
		"clock uncertainty": func(envelope *Envelope) { envelope.ClockUncertainty += time.Microsecond },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base.Clone()
			mutate(&changed)
			integrity, err := EnvelopeIntegrityDigest(changed)
			if err != nil {
				t.Fatal(err)
			}
			logical, err := LogicalMessageDigest(changed)
			if err != nil {
				t.Fatal(err)
			}
			if integrity == baseIntegrity || logical == baseLogical {
				t.Fatalf("%s was not bound by both digests", name)
			}
		})
	}

	compatibilityOnly := base.Clone()
	compatibilityOnly.CredentialProof = []byte("must-not-reach-key-12")
	proofIntegrity, err := EnvelopeIntegrityDigest(compatibilityOnly)
	if err != nil {
		t.Fatal(err)
	}
	proofLogical, err := LogicalMessageDigest(compatibilityOnly)
	if err != nil {
		t.Fatal(err)
	}
	if proofIntegrity != baseIntegrity || proofLogical != baseLogical {
		t.Fatal("reserved compatibility member changed key-12-free digests")
	}

	carrierOnly := base.Clone()
	carrierOnly.Payload.Reference = ""
	carrierOnly.Payload.EncryptionRef = ""
	carrierOnly.Payload.Kind = PayloadInline
	carrierOnly.Payload.Inline = append([]byte(nil), base.Payload.Inline...)
	carrierIntegrity, err := EnvelopeIntegrityDigest(carrierOnly)
	if err != nil {
		t.Fatal(err)
	}
	carrierLogical, err := LogicalMessageDigest(carrierOnly)
	if err != nil {
		t.Fatal(err)
	}
	if carrierIntegrity != baseIntegrity || carrierLogical != baseLogical {
		// The golden input is already inline, so this verifies an ownership-safe
		// descriptor clone does not perturb either canonical digest.
		t.Fatal("equivalent carrier descriptor changed digest")
	}
}

func TestQAClockUncertaintyWireBoundariesAndNoLegacyFields(t *testing.T) {
	codec := mustCodec(t)
	for _, uncertainty := range []time.Duration{time.Microsecond, MaxClockUncertainty} {
		envelope := testEnvelope(t)
		envelope.ClockUncertainty = uncertainty
		encoded, err := codec.Encode(envelope)
		if err != nil {
			t.Fatalf("encode %v: %v", uncertainty, err)
		}
		decoded, err := codec.Decode(encoded)
		if err != nil || decoded.ClockUncertainty != uncertainty {
			t.Fatalf("round trip %v = %v, %v", uncertainty, decoded.ClockUncertainty, err)
		}
		var fields map[uint64]cbor.RawMessage
		if err := cbor.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		if _, ok := fields[14]; !ok {
			t.Fatal("clock uncertainty wire key 14 absent")
		}
		if _, ok := fields[12]; ok {
			t.Fatal("reserved credential-proof key 12 was emitted")
		}
		if _, ok := fields[15]; ok {
			t.Fatal("reserved generation wire key 15 was emitted")
		}
		for key := range fields {
			if key > 15 {
				t.Fatalf("unexpected legacy/future wire field %d", key)
			}
		}
	}

	for name, uncertainty := range map[string]time.Duration{
		"zero": 0, "sub-microsecond": time.Nanosecond, "over cap": MaxClockUncertainty + time.Microsecond,
	} {
		t.Run(name, func(t *testing.T) {
			envelope := testEnvelope(t)
			envelope.ClockUncertainty = uncertainty
			if encoded, err := codec.Encode(envelope); err == nil || len(encoded) != 0 {
				t.Fatalf("invalid uncertainty encoded: len=%d err=%v", len(encoded), err)
			}
		})
	}

	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[uint64]cbor.RawMessage
	if err := cbor.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	fields[99] = cbor.RawMessage{0x01}
	nonCanonical, err := cbor.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := codec.Decode(nonCanonical); err == nil || !bytes.Equal(decoded.CredentialProof, nil) {
		t.Fatalf("unknown legacy field accepted: %#v %v", decoded, err)
	}

	for name, reservedValue := range map[string]cbor.RawMessage{
		"empty":    {0x40},
		"nonempty": {0x41, 0x01},
	} {
		t.Run("reserved key 12 "+name, func(t *testing.T) {
			withReserved := make(map[uint64]cbor.RawMessage, len(fields)+1)
			for key, value := range fields {
				if key != 99 {
					withReserved[key] = value
				}
			}
			withReserved[12] = reservedValue
			encodedReserved, err := cbor.Marshal(withReserved)
			if err != nil {
				t.Fatal(err)
			}
			if decoded, err := codec.Decode(encodedReserved); err == nil || len(decoded.CredentialProof) != 0 {
				t.Fatalf("reserved key 12 accepted: %#v %v", decoded, err)
			}
		})
	}
}
