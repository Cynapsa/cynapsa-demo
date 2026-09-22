package protocol

import (
	"errors"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestPrivateWireVersionCutRejectsLegacyVersionAndReservedFields(t *testing.T) {
	codec, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(testEnvelope(t))
	if err != nil {
		t.Fatal(err)
	}
	var value map[uint64]any
	if err := cbor.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}

	legacy := cloneWireMap(value)
	legacy[0] = uint64(1)
	legacyBytes, err := cbor.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(legacyBytes); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("legacy version err=%v", err)
	}

	for _, reserved := range []uint64{6, 15} {
		withReserved := cloneWireMap(value)
		withReserved[reserved] = uint64(1)
		candidate, err := cbor.Marshal(withReserved)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := codec.Decode(candidate); !errors.Is(err, ErrMalformedEncoding) {
			t.Fatalf("reserved key %d err=%v", reserved, err)
		}
	}
}

func cloneWireMap(source map[uint64]any) map[uint64]any {
	clone := make(map[uint64]any, len(source)+1)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
