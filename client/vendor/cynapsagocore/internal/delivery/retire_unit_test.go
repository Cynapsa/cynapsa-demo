package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestRetireRemovesExactQueuedEventAndPreservesFIFO(t *testing.T) {
	dispatcher := newTestDispatcher(t, 3)
	admissions := make(map[string]AdmissionID)
	for _, id := range []string{"one", "two", "three"} {
		admission, err := dispatcher.DeliverTracked(context.Background(), testEvent(id, model.CoreErrorEvent{}))
		if err != nil {
			t.Fatal(err)
		}
		admissions[id] = admission
	}
	if !dispatcher.RetireAdmission(admissions["two"]) {
		t.Fatal("Retire(two) = false")
	}
	if dispatcher.RetireAdmission(AdmissionID(9999)) {
		t.Fatal("Retire(missing) = true")
	}
	for _, want := range []string{"one", "three"} {
		got, err := dispatcher.Next(context.Background())
		if err != nil || got.ID != want {
			t.Fatalf("Next() = %+v, %v, want %q", got, err, want)
		}
	}
}

func TestRetireDropsRolledBackFrontAndActiveLease(t *testing.T) {
	dispatcher := newTestDispatcher(t, 2)
	admissions := make(map[string]AdmissionID)
	for _, id := range []string{"front", "after"} {
		admission, err := dispatcher.DeliverTracked(context.Background(), testEvent(id, model.CoreErrorEvent{}))
		if err != nil {
			t.Fatal(err)
		}
		admissions[id] = admission
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	if !dispatcher.RetireAdmission(admissions["front"]) {
		t.Fatal("Retire(front) = false")
	}

	event, lease, err := dispatcher.ReserveNextWait(context.Background())
	if err != nil || event.ID != "after" {
		t.Fatalf("ReserveNextWait() = %+v, %v", event, err)
	}
	if !dispatcher.RetireAdmission(admissions["after"]) {
		t.Fatal("Retire(active) = false")
	}
	if err = dispatcher.Rollback(lease); !errors.Is(err, ErrLeaseRetired) {
		t.Fatalf("Rollback(retired) = %v", err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 {
		t.Fatalf("Stats() = %+v", stats)
	}
}

func TestReserveNextWaitCancellationReleasesConsumer(t *testing.T) {
	dispatcher := newTestDispatcher(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := dispatcher.ReserveNextWait(ctx)
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("ReserveNextWait() = %v", err)
	}
	if err := dispatcher.Push(context.Background(), testEvent("next", model.CoreErrorEvent{})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dispatcher.ReserveNext(context.Background()); err != nil {
		t.Fatalf("ReserveNext() after cancellation = %v", err)
	}
}
