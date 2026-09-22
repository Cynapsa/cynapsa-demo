package delivery

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestQAPod6CapacityOneRollbackUsesFixedTerminalReserve(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := qaPod2DeliveryMessage("ordinary", 1)
	closing := qaPod2LifecycleEvent("closing", model.LifecycleReady, model.LifecycleClosing, 2)
	closed := qaPod2LifecycleEvent("closed", model.LifecycleClosing, model.LifecycleClosed, 3)
	if err = dispatcher.Push(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 1 || stats.Capacity != 1 || stats.ShutdownEventDepth != 1 || stats.ShutdownEventCapacity != 2 {
		t.Fatalf("leased closing stats = %+v", stats)
	}
	if err = dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ordinary", "closing"} {
		event, nextErr := dispatcher.Next(context.Background())
		if nextErr != nil || event.ID != want {
			t.Fatalf("Next = %+v, %v, want %s", event, nextErr, want)
		}
	}
	if err = dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	event, err := dispatcher.Next(context.Background())
	if err != nil || event.ID != "closed" {
		t.Fatalf("closed event = %+v, %v", event, err)
	}
	if _, err = dispatcher.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Next = %v", err)
	}
}

func TestQAPod6TerminalPublicationFailureDoesNotCorruptReserve(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	invalid := model.Event{ID: "invalid", Name: "session.state_changed", CreatedAt: time.Unix(1, 0)}
	if err = dispatcher.BeginShutdownWithEvent(context.Background(), invalid); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("invalid begin = %v", err)
	}
	if stats := dispatcher.Stats(); stats.Closing || stats.ShutdownEventDepth != 0 {
		t.Fatalf("invalid begin changed state: %+v", stats)
	}
	ordinary := qaPod2DeliveryMessage("ordinary", 2)
	closing := qaPod2LifecycleEvent("closing", model.LifecycleCreated, model.LifecycleClosing, 3)
	closed := qaPod2LifecycleEvent("closed", model.LifecycleClosing, model.LifecycleClosed, 4)
	if err = dispatcher.Push(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.SealShutdownWithEvent(context.Background(), invalid); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("invalid seal = %v", err)
	}
	if stats := dispatcher.Stats(); !stats.Closing || stats.Closed || stats.ShutdownEventDepth != 1 {
		t.Fatalf("invalid seal changed reserve: %+v", stats)
	}
	if err = dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ordinary", "closing", "closed"} {
		event, nextErr := dispatcher.Next(context.Background())
		if nextErr != nil || event.ID != want {
			t.Fatalf("Next = %+v, %v, want %s", event, nextErr, want)
		}
	}
}

func TestQAPod6ConcurrentLeaseFinalizerConsumerAndShutdown(t *testing.T) {
	for iteration := range 200 {
		dispatcher, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		ordinary := qaPod2DeliveryMessage("ordinary", 1)
		closing := qaPod2LifecycleEvent("closing", model.LifecycleReady, model.LifecycleClosing, 2)
		closed := qaPod2LifecycleEvent("closed", model.LifecycleClosing, model.LifecycleClosed, 3)
		if err = dispatcher.Push(context.Background(), ordinary); err != nil {
			t.Fatal(err)
		}
		_, lease, err := dispatcher.ReserveNext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err = dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		consumer := make(chan []string, 1)
		consumerErr := make(chan error, 1)
		go func() {
			ids := make([]string, 0, 3)
			for len(ids) < 3 {
				event, nextErr := dispatcher.Next(ctx)
				if nextErr != nil {
					consumerErr <- nextErr
					return
				}
				ids = append(ids, event.ID)
			}
			consumer <- ids
		}()
		shutdown := make(chan error, 1)
		go func() { shutdown <- dispatcher.Shutdown(ctx) }()
		seal := make(chan error, 1)
		go func() { seal <- dispatcher.SealShutdownWithEvent(ctx, closed) }()
		if err = dispatcher.Rollback(lease); err != nil {
			cancel()
			t.Fatalf("iteration %d rollback = %v", iteration, err)
		}
		if err = <-seal; err != nil {
			cancel()
			t.Fatalf("iteration %d seal = %v", iteration, err)
		}
		select {
		case ids := <-consumer:
			if !slices.Equal(ids, []string{"ordinary", "closing", "closed"}) {
				cancel()
				t.Fatalf("iteration %d order = %v", iteration, ids)
			}
		case err = <-consumerErr:
			cancel()
			t.Fatalf("iteration %d consumer = %v", iteration, err)
		}
		if err = <-shutdown; err != nil {
			cancel()
			t.Fatalf("iteration %d shutdown = %v", iteration, err)
		}
		cancel()
		if stats := dispatcher.Stats(); !stats.Closed || stats.Depth != 0 || stats.ShutdownEventDepth != 0 {
			t.Fatalf("iteration %d final stats = %+v", iteration, stats)
		}
	}
}

func qaPod2DeliveryMessage(id string, second int64) model.Event {
	return model.Event{ID: id, Name: "message.received", CreatedAt: time.Unix(second, 0), Value: model.MessageReceivedEvent{MessageID: "message-" + id}}
}

func qaPod2LifecycleEvent(id string, previous, current model.LifecycleState, second int64) model.Event {
	return model.Event{ID: id, Name: "session.state_changed", CreatedAt: time.Unix(second, 0), Value: model.SessionStateChangedEvent{Previous: previous, Current: current}}
}
