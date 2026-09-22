package handshake

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func FuzzQAHandleSignal(f *testing.F) {
	f.Add([]byte("payload"), int64(0), int64(time.Second), byte(1))
	f.Add([]byte{}, int64(-1), int64(0), byte(2))
	f.Fuzz(func(t *testing.T, payload []byte, startOffsetNanos, timeoutNanos int64, fill byte) {
		base := time.Unix(10_000, 0).UTC()
		config := testConfig(&base)
		manager, err := NewManager(config, &testNegotiator{})
		if err != nil {
			t.Fatal(err)
		}
		started := base.Add(time.Duration(startOffsetNanos))
		timeout := time.Duration(timeoutNanos)
		attempt, err := NewAttempt("a", "b", started, timeout, bytes.NewReader(bytes.Repeat([]byte{fill}, 16)))
		if err != nil {
			return
		}
		_ = manager.HandleSignal(context.Background(), Signal{Attempt: attempt, Payload: payload})
		manager.Close()
	})
}
