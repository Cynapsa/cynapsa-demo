package loopback

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	coretransport "github.com/Cynapsa/cynapsagocore/internal/transport"
)

func testEnvelope(t *testing.T, marker uint64) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("native", []byte{byte(marker)})
	if err != nil {
		t.Fatal(err)
	}
	conversation := sha256.Sum256([]byte("a|b|mesh"))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(conversation[:]),
		Sender:         "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh",
		Mode: protocol.ModeMessage, CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond,
		Payload: payload, CredentialProof: []byte("proof"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestPairOrderingCapacityAndClose(t *testing.T) {
	left, right, err := NewPair(1, coretransport.KindLive)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := left.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := right.Start(ctx); err != nil {
		t.Fatal(err)
	}
	first, second := testEnvelope(t, 1), testEnvelope(t, 2)
	if err := left.Send(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, second); !errors.Is(err, coretransport.ErrQueueFull) {
		t.Fatalf("capacity error = %v", err)
	}
	got, err := right.Receive(ctx)
	if err != nil || got.MessageID != first.MessageID {
		t.Fatalf("receive = %#v, %v", got, err)
	}
	if err := right.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, second); !errors.Is(err, coretransport.ErrClosed) {
		t.Fatalf("send to closed peer = %v", err)
	}
}

func TestFaultsAreDeterministic(t *testing.T) {
	left, right, control, _, err := NewFaultablePair(8, coretransport.KindDurable)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = left.Start(ctx)
	_ = right.Start(ctx)
	one, two, three := testEnvelope(t, 1), testEnvelope(t, 2), testEnvelope(t, 3)
	if err := control.Set(FaultHold, FaultDuplicate, FaultDrop, FaultReject, FaultAmbiguous); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, one); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, two); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := right.Receive(ctx)
		if err != nil || got.MessageID != two.MessageID {
			t.Fatalf("duplicate = %#v, %v", got, err)
		}
	}
	if err := left.Send(ctx, three); err != nil {
		t.Fatal(err)
	}
	if err := left.Send(ctx, three); !errors.Is(err, coretransport.ErrSendRejected) {
		t.Fatalf("reject = %v", err)
	}
	if err := left.Send(ctx, three); !errors.Is(err, coretransport.ErrSendAmbiguous) {
		t.Fatalf("ambiguous = %v", err)
	}
	got, err := right.Receive(ctx)
	if err != nil || got.MessageID != three.MessageID {
		t.Fatalf("ambiguous delivery = %#v, %v", got, err)
	}
	if err := control.ReleaseHeld(ctx, left); err != nil {
		t.Fatal(err)
	}
	got, err = right.Receive(ctx)
	if err != nil || got.MessageID != one.MessageID {
		t.Fatalf("held = %#v, %v", got, err)
	}
}

func TestBlockedSendCancels(t *testing.T) {
	left, right, control, _, err := NewFaultablePair(1, coretransport.KindLive)
	if err != nil {
		t.Fatal(err)
	}
	_ = left.Start(context.Background())
	_ = right.Start(context.Background())
	if err := control.Set(FaultBlock); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := left.Send(ctx, testEnvelope(t, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked = %v", err)
	}
	if err := control.Set(FaultBlock); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- left.Send(context.Background(), testEnvelope(t, 2)) }()
	if err := left.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, coretransport.ErrClosed) {
			t.Fatalf("close unblock=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock send")
	}
}
