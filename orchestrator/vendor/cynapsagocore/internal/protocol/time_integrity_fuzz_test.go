package protocol

import (
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

func FuzzQAClockUncertaintyWireBoundary(f *testing.F) {
	for _, seed := range []int64{-1, 0, 1, 999_999, 1_000_000, 1_000_001} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, micros int64) {
		if micros < -2_000_000 || micros > 2_000_000 {
			micros %= 2_000_001
		}
		envelope := testEnvelope(t)
		envelope.ClockUncertainty = time.Duration(micros) * time.Microsecond
		codec := mustCodec(t)
		encoded, err := codec.Encode(envelope)
		valid := micros >= 1 && micros <= int64(MaxClockUncertainty/time.Microsecond)
		if !valid {
			if err == nil {
				t.Fatalf("encoded invalid uncertainty %d", micros)
			}
			return
		}
		if err != nil {
			t.Fatalf("valid uncertainty %d rejected: %v", micros, err)
		}
		decoded, err := codec.Decode(encoded)
		if err != nil || decoded.ClockUncertainty != envelope.ClockUncertainty {
			t.Fatalf("round trip %d: %v %v", micros, decoded.ClockUncertainty, err)
		}
		var fields map[uint64]cbor.RawMessage
		if err := cbor.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		fields[12] = cbor.RawMessage{0x40}
		reserved, err := cbor.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := codec.Decode(reserved); err == nil {
			t.Fatal("reserved credential-proof key 12 accepted")
		}
	})
}
