package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/testkit"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/loopback"
)

// TestRankTransitionPreservesMessageIdentity verifies that path changes do not
// alter message identity. Application delivery order is deliberately unspecified.
func TestRankTransitionPreservesMessageIdentity(t *testing.T) {
	left, right, control, _, err := loopback.NewFaultablePair(4, transport.KindLive)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = left.Start(ctx)
	_ = right.Start(ctx)
	first, second := integrationEnvelope(t, 1, protocol.ModeMessage), integrationEnvelope(t, 2, protocol.ModeMessage)
	if err := testkit.InjectFault(control, testkit.Fault{Kind: "hold", Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := control.ReleaseHeld(ctx, left); err != nil {
		t.Fatal(err)
	}

	lane, err := conversation.VerifyEnvelopeBinding(first, conversation.AuthenticatedBinding{MeshID: first.MeshID, AuthenticatedSender: first.Sender, Recipient: first.Recipient})
	if err != nil {
		t.Fatal(err)
	}
	deduper, err := conversation.NewDeduper(conversation.DedupeConfig{GlobalCapacity: 8, PerPeerCapacity: 8, Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(map[string]struct{}, 2)
	for range 2 {
		envelope, err := right.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := deduper.Check(lane, envelope)
		if err != nil || result != conversation.DedupeNew {
			t.Fatalf("dedupe=%s err=%v", result, err)
		}
		delivered[envelope.MessageID] = struct{}{}
	}
	if len(delivered) != 2 {
		t.Fatalf("transition lost or rewrote identity: %#v", delivered)
	}
	if _, ok := delivered[first.MessageID]; !ok {
		t.Fatalf("first identity missing: %#v", delivered)
	}
	if _, ok := delivered[second.MessageID]; !ok {
		t.Fatalf("second identity missing: %#v", delivered)
	}
	if err := deduper.MarkTerminal(lane, first); err != nil {
		t.Fatal(err)
	}
	if result, err := deduper.Check(lane, first); err != nil || result != conversation.DedupeDuplicateTerminal {
		t.Fatalf("replay classification=%s err=%v", result, err)
	}
	_ = left.Close(ctx)
	_ = right.Close(ctx)
}
