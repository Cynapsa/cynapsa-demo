package sdkboundary

import (
	"errors"
	"math"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func FuzzMessageRequestTTLMilliseconds(f *testing.F) {
	for _, seed := range []int64{-1, 0, 1, 1250, math.MaxInt64 / int64(time.Millisecond), math.MaxInt64/int64(time.Millisecond) + 1, math.MaxInt64} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, milliseconds int64) {
		adapter, err := New()
		if err != nil {
			t.Fatal(err)
		}
		command, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandMessageRequest, map[string]any{
			"to": "peer", "payload": qaNativeWire("/", ""), "ttl_ms": milliseconds,
		}))
		switch {
		case milliseconds < 0:
			if !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("negative ttl_ms %d error = %v", milliseconds, err)
			}
		case milliseconds > math.MaxInt64/int64(time.Millisecond):
			if !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("oversized ttl_ms %d error = %v", milliseconds, err)
			}
		default:
			if err != nil {
				t.Fatalf("ttl_ms %d error = %v", milliseconds, err)
			}
			request, ok := command.(v1.MessageRequestCommand)
			if !ok || request.TTL != time.Duration(milliseconds)*time.Millisecond {
				t.Fatalf("ttl_ms %d command = %#v", milliseconds, command)
			}
		}
	})
}
