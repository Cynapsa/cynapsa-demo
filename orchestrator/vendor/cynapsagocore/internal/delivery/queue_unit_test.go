package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	for _, capacity := range []int{-1, 0} {
		if dispatcher, err := New(capacity); dispatcher != nil || !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("New(%d) = (%v, %v)", capacity, dispatcher, err)
		}
	}
}

func TestPushNextBoundedOwnershipAndRecovery(t *testing.T) {
	dispatcher := newTestDispatcher(t, 1)
	value := &model.PayloadTransferEvent{Handle: "owned", State: "started"}
	event := testEvent("one", value)
	if err := dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	rejectedValue := &model.PayloadTransferEvent{Handle: "caller", State: "started"}
	rejected := testEvent("two", rejectedValue)
	if err := dispatcher.Push(context.Background(), rejected); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("saturated Push() error = %v", err)
	}
	if rejected.Value != rejectedValue || rejectedValue.Handle != "caller" {
		t.Fatal("rejected Push mutated caller-owned event")
	}

	got, err := dispatcher.Next(context.Background())
	gotValue, ok := got.Value.(model.PayloadTransferEvent)
	if err != nil || !ok || gotValue.Handle != "owned" || gotValue.State != "started" || value.Handle != "owned" {
		t.Fatalf("Next() = (%+v, %v), want independent value snapshot", got, err)
	}
	if err := dispatcher.Push(context.Background(), rejected); err != nil {
		t.Fatalf("Push() after drain = %v", err)
	}
	stats := dispatcher.Stats()
	if stats.Depth != 1 || stats.Capacity != 1 {
		t.Fatalf("Stats() = %+v", stats)
	}
}

func TestDeliverUsesTheSameTypedEventQueue(t *testing.T) {
	dispatcher := newTestDispatcher(t, 1)
	event := testEvent("typed", model.MessageReceivedEvent{MessageID: "message"})
	if err := dispatcher.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	got, err := dispatcher.Next(context.Background())
	if err != nil || got.ID != "typed" {
		t.Fatalf("Next() = (%+v, %v)", got, err)
	}
}

func TestWaitOperationsValidateContextAndEvent(t *testing.T) {
	dispatcher := newTestDispatcher(t, 1)
	if err := dispatcher.Push(nil, testEvent("one", model.CoreErrorEvent{})); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Push(nil) error = %v", err)
	}
	if _, err := dispatcher.Next(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Next(nil) error = %v", err)
	}
	if err := dispatcher.Shutdown(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Shutdown(nil) error = %v", err)
	}
	if err := dispatcher.Push(context.Background(), model.Event{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("invalid Push error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := dispatcher.Push(ctx, testEvent("one", model.CoreErrorEvent{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Push error = %v", err)
	}
	if _, err := dispatcher.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Next error = %v", err)
	}
}

func TestShutdownDrainsAndDeadlineAbandons(t *testing.T) {
	t.Run("drain", func(t *testing.T) {
		dispatcher := newTestDispatcher(t, 2)
		for index := range 2 {
			if err := dispatcher.Push(context.Background(), testEvent(fmt.Sprint(index), model.CoreErrorEvent{})); err != nil {
				t.Fatal(err)
			}
		}
		done := make(chan error, 1)
		go func() { done <- dispatcher.Shutdown(context.Background()) }()
		for range 2 {
			if _, err := dispatcher.Next(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !dispatcher.Stats().Closed {
			t.Fatal("dispatcher did not close after drain")
		}
	})

	t.Run("deadline", func(t *testing.T) {
		dispatcher := newTestDispatcher(t, 1)
		if err := dispatcher.Push(context.Background(), testEvent("one", model.CoreErrorEvent{})); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := dispatcher.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown() error = %v", err)
		}
		if stats := dispatcher.Stats(); !stats.Closed || stats.Depth != 0 {
			t.Fatalf("Stats() = %+v", stats)
		}
	})
}

func TestConcurrentPushNextAndShutdownRemainBounded(t *testing.T) {
	dispatcher := newTestDispatcher(t, 8)
	start := make(chan struct{})
	var group sync.WaitGroup
	for producer := range 16 {
		producer := producer
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_ = dispatcher.Push(context.Background(), testEvent(fmt.Sprint(producer), model.CoreErrorEvent{}))
		}()
	}
	close(start)
	group.Wait()
	if stats := dispatcher.Stats(); stats.Depth > stats.Capacity {
		t.Fatalf("unbounded Stats() = %+v", stats)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = dispatcher.Shutdown(ctx)
	if err := dispatcher.Push(context.Background(), testEvent("closed", model.CoreErrorEvent{})); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push() after close error = %v", err)
	}
}

func TestClearRequiresShutdown(t *testing.T) {
	dispatcher := newTestDispatcher(t, 1)
	if err := dispatcher.Clear(context.Background()); !errors.Is(err, ErrNotClosing) {
		t.Fatalf("Clear() error = %v", err)
	}
	dispatcher.BeginShutdown()
	if err := dispatcher.Clear(context.Background()); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("Clear() after shutdown error = %v", err)
	}
}

func newTestDispatcher(t *testing.T, capacity int) *Dispatcher {
	t.Helper()
	dispatcher, err := New(capacity)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func testEvent(id string, value model.EventValue) model.Event {
	return model.Event{ID: id, Name: "test", CreatedAt: time.Unix(1, 0), Value: value}
}
