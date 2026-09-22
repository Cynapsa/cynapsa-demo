package cynapsagocore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestNativeEventLeaseCommitPanicRollsBackTrackedEventAndReleasesCore(t *testing.T) {
	core, err := New(Config{QueueLimit: 2, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	deliveryDone := make(chan error, 1)
	go func() {
		deliveryDone <- core.runtime.DeliverInbound(context.Background(), model.MessageReceivedEvent{
			MessageID: "panic-message", ConversationID: "panic-conversation", FromAgentID: "sender", MeshID: "mesh", Mode: "msg",
			Payload: model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/", Body: []byte("canary")}},
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	encoded, _, lease, err := core.ReserveNativeEventABI(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	lease.commit = func() error { panic("commit invariant") }
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_ = lease.Commit()
	}()
	if !panicked {
		t.Fatal("NativeDeliveryLease.Commit did not preserve the invariant panic")
	}
	if err = lease.Commit(); !errors.Is(err, errNativeDeliveryLeaseFinalized) {
		t.Fatalf("second Commit() = %v", err)
	}
	if err = lease.Rollback(); !errors.Is(err, errNativeDeliveryLeaseFinalized) {
		t.Fatalf("post-panic Rollback() = %v", err)
	}

	event, err := core.NextEvent(ctx)
	if err != nil {
		t.Fatalf("tracked poll fallback: %v", err)
	}
	if event.Name != "message.received" {
		t.Fatalf("poll fallback event = %q", event.Name)
	}
	if err = core.runtime.AcceptDelivery(ctx, model.DeliveryAcceptArgs{EventID: string(event.ID)}); err != nil {
		t.Fatal(err)
	}
	if err = <-deliveryDone; err != nil {
		t.Fatal(err)
	}
	shutdownAndDrain(t, core)
	destroyed := make(chan error, 1)
	go func() { destroyed <- core.Destroy() }()
	select {
	case err = <-destroyed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Core.Destroy remained blocked by a panicked native delivery lease")
	}
}
