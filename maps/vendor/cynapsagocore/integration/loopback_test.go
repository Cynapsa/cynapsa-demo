package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/testkit"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/loopback"
)

// TestLoopbackTransportLifecycle verifies the private deterministic transport's
// start, send, receive, fault injection, ownership, and close behavior.
func TestLoopbackTransportLifecycle(t *testing.T) {
	left, right, control, _, err := loopback.NewFaultablePair(2, transport.KindLive)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, endpoint := range []*loopback.Transport{left, right} {
		if err := endpoint.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}

	original := testkit.Envelope()
	if err := left.Send(ctx, original); err != nil {
		t.Fatal(err)
	}
	received, err := right.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if received.MessageID != original.MessageID || received.ConversationID != original.ConversationID {
		t.Fatalf("identity changed in transit: got %q/%q", received.MessageID, received.ConversationID)
	}
	received.Payload.Inline[0] ^= 0xff
	if original.Payload.Inline[0] == received.Payload.Inline[0] {
		t.Fatal("transport did not preserve payload ownership")
	}

	if err := testkit.InjectFault(control, testkit.Fault{Kind: "duplicate", Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, original); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		copy, err := right.Receive(ctx)
		if err != nil || copy.MessageID != original.MessageID {
			t.Fatalf("duplicate %d = %#v, %v", i, copy, err)
		}
	}

	if err := right.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, original); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("send after peer close = %v", err)
	}
	if got := right.Observe().State; got != transport.HealthClosed {
		t.Fatalf("closed observation = %v", got)
	}
	if err := left.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
