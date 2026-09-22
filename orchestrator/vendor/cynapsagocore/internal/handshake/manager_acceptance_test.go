package handshake

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestQARemoteAttemptCannotExtendConfiguredLifetime(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	config := testConfig(&now)
	config.MaximumAttempts = 1
	manager, err := NewManager(config, &testNegotiator{})
	if err != nil {
		t.Fatal(err)
	}

	// A peer-controlled deadline must not pin active/retired capacity beyond
	// this manager's configured one-second attempt lifetime.
	remote, err := NewAttempt("a", "b", now, 24*time.Hour, bytes.NewReader(bytes.Repeat([]byte{0x55}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	err = manager.HandleSignal(context.Background(), Signal{Attempt: remote, Payload: []byte("jingle")})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("oversized remote attempt lifetime accepted: %v", err)
	}
}

func TestQAFutureAndExpiredSignalsDoNotConsumeAttemptCapacity(t *testing.T) {
	now := time.Unix(2_000, 0).UTC()
	config := testConfig(&now)
	config.MaximumAttempts = 1
	manager, err := NewManager(config, &testNegotiator{})
	if err != nil {
		t.Fatal(err)
	}

	future, err := NewAttempt("a", "b", now.Add(time.Minute), config.AttemptTimeout, bytes.NewReader(bytes.Repeat([]byte{0x61}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.HandleSignal(context.Background(), Signal{Attempt: future, Payload: []byte("jingle")}); !errors.Is(err, ErrStale) {
		t.Fatalf("future signal error = %v", err)
	}

	local, err := manager.Start(context.Background(), "c")
	if err != nil || local.State != AttemptSucceeded {
		t.Fatalf("invalid signal consumed capacity: attempt=%#v err=%v", local, err)
	}
}
