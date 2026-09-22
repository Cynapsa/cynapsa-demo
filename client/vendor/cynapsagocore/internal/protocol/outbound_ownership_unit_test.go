package protocol

import (
	"bytes"
	"errors"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

type ownershipEncodingMode struct {
	cbor.EncMode
	inline  []byte
	digest  []byte
	partial []byte
	result  error
	panic   bool
}

func (mode *ownershipEncodingMode) Marshal(value any) ([]byte, error) {
	wire := value.(wireEnvelopeV2)
	mode.inline = wire.Payload.Inline
	mode.digest = wire.Payload.Digest
	if mode.panic {
		panic("encoder panic")
	}
	if mode.result != nil {
		mode.partial = []byte("partial private encoding")
		return mode.partial, mode.result
	}
	return mode.EncMode.Marshal(value)
}

func TestCodecClearsOwnedWireClonesAfterMarshal(t *testing.T) {
	codec, mode := ownershipCodec(t)
	envelope := testEnvelope(t)
	wantInline := append([]byte(nil), envelope.Payload.Inline...)
	wantDigest := envelope.Payload.Digest
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	assertProtocolZero(t, mode.inline)
	assertProtocolZero(t, mode.digest)
	if !bytes.Equal(envelope.Payload.Inline, wantInline) || envelope.Payload.Digest != wantDigest {
		t.Fatal("codec changed caller-owned envelope bytes")
	}
}

func TestCodecClearsOwnedWireAndPartialResultOnMarshalError(t *testing.T) {
	codec, mode := ownershipCodec(t)
	mode.result = errors.New("private encoder error")
	envelope := testEnvelope(t)
	wantInline := append([]byte(nil), envelope.Payload.Inline...)
	if encoded, err := codec.Encode(envelope); !errors.Is(err, ErrMalformedEncoding) || encoded != nil {
		t.Fatalf("Encode() = %x, %v", encoded, err)
	}
	assertProtocolZero(t, mode.inline)
	assertProtocolZero(t, mode.digest)
	assertProtocolZero(t, mode.partial)
	if !bytes.Equal(envelope.Payload.Inline, wantInline) {
		t.Fatal("codec changed caller-owned envelope bytes")
	}
}

func TestCodecClearsOwnedWireClonesOnMarshalPanic(t *testing.T) {
	codec, mode := ownershipCodec(t)
	mode.panic = true
	envelope := testEnvelope(t)
	wantInline := append([]byte(nil), envelope.Payload.Inline...)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Encode did not preserve dependency panic")
			}
		}()
		_, _ = codec.Encode(envelope)
	}()
	assertProtocolZero(t, mode.inline)
	assertProtocolZero(t, mode.digest)
	if !bytes.Equal(envelope.Payload.Inline, wantInline) {
		t.Fatal("codec changed caller-owned envelope bytes")
	}
}

func TestCodecDecodeClearsOwnedEnvelopeOnValidationFailure(t *testing.T) {
	codec, _ := ownershipCodec(t)
	owned := testEnvelope(t).Clone()
	owned.CredentialProof = []byte("private decoded proof")
	inline, proof := owned.Payload.Inline, owned.CredentialProof
	if _, err := codec.validateDecodedEnvelope(owned, nil); err == nil {
		t.Fatal("validation unexpectedly succeeded")
	}
	assertProtocolZero(t, inline)
	assertProtocolZero(t, proof)
}

func TestCodecDecodeClearsOwnedEnvelopeOnValidationPanic(t *testing.T) {
	codec, _ := ownershipCodec(t)
	owned := testEnvelope(t).Clone()
	owned.CredentialProof = []byte("private decoded proof")
	inline, proof := owned.Payload.Inline, owned.CredentialProof
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("validation panic was not preserved")
			}
		}()
		_, _ = codec.validateDecodedEnvelopeWith(owned, nil, func(Envelope) error {
			panic("validation panic")
		})
	}()
	assertProtocolZero(t, inline)
	assertProtocolZero(t, proof)
}

func TestCodecDecodeClearsOwnedEnvelopeAndTemporariesOnReencodeError(t *testing.T) {
	codec, mode := ownershipCodec(t)
	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	mode.result = errors.New("private re-encode error")
	owned := testEnvelope(t).Clone()
	inline := owned.Payload.Inline
	if decoded, err := codec.validateDecodedEnvelope(owned, encoded); !errors.Is(err, ErrMalformedEncoding) || len(decoded.Payload.Inline) != 0 {
		t.Fatalf("validateDecodedEnvelope() = %+v, %v", decoded, err)
	}
	assertProtocolZero(t, inline)
	assertProtocolZero(t, mode.inline)
	assertProtocolZero(t, mode.digest)
	assertProtocolZero(t, mode.partial)
}

func TestCodecDecodeClearsOwnedEnvelopeOnReencodePanic(t *testing.T) {
	codec, mode := ownershipCodec(t)
	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	mode.panic = true
	owned := testEnvelope(t).Clone()
	inline := owned.Payload.Inline
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("decode canonical re-encode did not preserve dependency panic")
			}
		}()
		_, _ = codec.validateDecodedEnvelope(owned, encoded)
	}()
	assertProtocolZero(t, inline)
	assertProtocolZero(t, mode.inline)
	assertProtocolZero(t, mode.digest)
}

func TestCodecDecodeClearsOwnedEnvelopeOnNonCanonicalFailure(t *testing.T) {
	codec, _ := ownershipCodec(t)
	owned := testEnvelope(t).Clone()
	inline := owned.Payload.Inline
	if decoded, err := codec.validateDecodedEnvelope(owned, []byte("non-canonical private envelope")); !errors.Is(err, ErrNonCanonical) || len(decoded.Payload.Inline) != 0 {
		t.Fatalf("validateDecodedEnvelope() = %+v, %v", decoded, err)
	}
	assertProtocolZero(t, inline)
}

func TestCodecDecodeTransfersOwnedEnvelopeOnlyOnSuccess(t *testing.T) {
	codec, _ := ownershipCodec(t)
	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	owned := testEnvelope(t).Clone()
	want := append([]byte(nil), owned.Payload.Inline...)
	decoded, err := codec.validateDecodedEnvelope(owned, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Payload.Inline, want) {
		t.Fatal("successful decode cleared transferred payload ownership")
	}
	clearEnvelopeOwned(&decoded)
}

func ownershipCodec(t *testing.T) (*canonicalCodec, *ownershipEncodingMode) {
	t.Helper()
	created, err := newCanonicalCodec()
	if err != nil {
		t.Fatal(err)
	}
	codec := created.(*canonicalCodec)
	mode := &ownershipEncodingMode{EncMode: codec.encode}
	codec.encode = mode
	return codec, mode
}

func assertProtocolZero(t *testing.T, data []byte) {
	t.Helper()
	if len(data) == 0 {
		t.Fatal("ownership canary did not observe bytes")
	}
	for index, value := range data {
		if value != 0 {
			t.Fatalf("owned temporary remained non-zero at byte %d", index)
		}
	}
}
