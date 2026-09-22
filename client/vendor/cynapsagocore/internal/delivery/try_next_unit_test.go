package delivery

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestTryNextUsesAuthoritativeQueueWithoutWaiting(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.TryNext(context.Background()); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("TryNext(empty) error = %v, want ErrQueueEmpty", err)
	}
	event := model.Event{ID: "event", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
	if err := dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	got, err := dispatcher.TryNext(context.Background())
	if err != nil || got.ID != event.ID {
		t.Fatalf("TryNext() = %+v, %v", got, err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 {
		t.Fatalf("Stats().Depth = %d, want 0", stats.Depth)
	}
}

func TestTryNextHonorsContextAndShutdown(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.TryNext(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("TryNext(nil) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dispatcher.TryNext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("TryNext(cancelled) error = %v", err)
	}
	dispatcher.BeginShutdown()
	if _, err := dispatcher.TryNext(context.Background()); !errors.Is(err, ErrClosed) && !errors.Is(err, ErrClosing) {
		t.Fatalf("TryNext(shutdown) error = %v", err)
	}
}

func TestTryNextReturnsPromptlyWhenBlockingConsumerOwnsQueue(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.consumerMu.Lock()
	if _, err := dispatcher.TryNext(context.Background()); !errors.Is(err, ErrConsumerBusy) {
		t.Fatalf("TryNext(consumer busy) error = %v, want ErrConsumerBusy", err)
	}
	dispatcher.consumerMu.Unlock()
}

func TestEventLeaseRollbackRestoresHeadAndCapacity(t *testing.T) {
	dispatcher, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{"one", "two"} {
		event := model.Event{ID: id, Name: "core.error", CreatedAt: time.Unix(int64(index+1), 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
		if err := dispatcher.Push(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	event, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil || event.ID != "one" {
		t.Fatalf("ReserveNext() = %+v, %v", event, err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 2 {
		t.Fatalf("Stats during lease = %+v, want depth 2", stats)
	}
	third := model.Event{ID: "three", Name: "core.error", CreatedAt: time.Unix(3, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
	if err := dispatcher.Push(context.Background(), third); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Push() while all capacity reserved error = %v", err)
	}
	if err := dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"one", "two"} {
		got, err := dispatcher.TryNext(context.Background())
		if err != nil || got.ID != want {
			t.Fatalf("TryNext() = %+v, %v, want %s", got, err, want)
		}
	}
	if err := dispatcher.Rollback(lease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("Rollback(repeated) error = %v", err)
	}
}

func TestEventLeaseCommitTransfersExactlyOnce(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	event := model.Event{ID: "one", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
	if err := dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Commit(lease); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.TryNext(context.Background()); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("TryNext after commit error = %v", err)
	}
	if err := dispatcher.Commit(lease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("Commit(repeated) error = %v", err)
	}
}

func TestClosedDispatcherRejectsConsumersBeforeBufferedData(t *testing.T) {
	tests := []struct {
		name    string
		consume func(*Dispatcher) error
	}{
		{name: "next", consume: func(dispatcher *Dispatcher) error {
			_, err := dispatcher.Next(context.Background())
			return err
		}},
		{name: "try next", consume: func(dispatcher *Dispatcher) error {
			_, err := dispatcher.TryNext(context.Background())
			return err
		}},
		{name: "reserve", consume: func(dispatcher *Dispatcher) error {
			_, _, err := dispatcher.ReserveNext(context.Background())
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatcher, err := New(1)
			if err != nil {
				t.Fatal(err)
			}
			event := model.Event{ID: "buffered", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
			if err := dispatcher.Push(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			// Model the exact finalizer window: closed is committed under mu
			// before closedCh is published and buffered output is abandoned.
			dispatcher.mu.Lock()
			dispatcher.state = dispatcherClosed
			dispatcher.mu.Unlock()
			if err := test.consume(dispatcher); !errors.Is(err, ErrClosed) {
				t.Fatalf("consumer error = %v, want ErrClosed", err)
			}
			if len(dispatcher.queue) != 1 {
				t.Fatalf("closed consumer removed buffered event; depth = %d", len(dispatcher.queue))
			}
		})
	}
}

func TestFinalizeWithActiveLeaseCannotBeOvertakenByNewConsumer(t *testing.T) {
	for iteration := range 100 {
		dispatcher, err := New(2)
		if err != nil {
			t.Fatal(err)
		}
		for index, id := range []string{"leased", "buffered"} {
			event := model.Event{ID: id, Name: "core.error", CreatedAt: time.Unix(int64(index+1), 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
			if err := dispatcher.Push(context.Background(), event); err != nil {
				t.Fatal(err)
			}
		}
		_, lease, err := dispatcher.ReserveNext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		dispatcher.BeginShutdown()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		clearDone := make(chan error, 1)
		go func() { clearDone <- dispatcher.Clear(ctx) }()
		consumerDone := make(chan error, 1)
		go func() {
			for {
				_, _, reserveErr := dispatcher.ReserveNext(context.Background())
				if errors.Is(reserveErr, ErrConsumerBusy) {
					runtime.Gosched()
					continue
				}
				consumerDone <- reserveErr
				return
			}
		}()

		if err := <-clearDone; err != nil {
			cancel()
			t.Fatalf("iteration %d Clear() error = %v", iteration, err)
		}
		if err := <-consumerDone; !errors.Is(err, ErrClosed) {
			cancel()
			t.Fatalf("iteration %d ReserveNext() during finalization error = %v, want ErrClosed", iteration, err)
		}
		cancel()
		if err := dispatcher.Rollback(lease); !errors.Is(err, ErrInvalidLease) {
			t.Fatalf("iteration %d abandoned lease rollback error = %v", iteration, err)
		}
		stats := dispatcher.Stats()
		if !stats.Closed || stats.Depth != 0 {
			t.Fatalf("iteration %d final Stats() = %+v", iteration, stats)
		}
	}
}

func TestShutdownLifecycleReservePreservesOrderAtCapacityOne(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := model.Event{ID: "ordinary", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	closing := model.Event{ID: "closing", Name: "session.state_changed", CreatedAt: time.Unix(2, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleReady, Current: model.LifecycleClosing}}
	closed := model.Event{ID: "closed", Name: "session.state_changed", CreatedAt: time.Unix(3, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleClosing, Current: model.LifecycleClosed}}
	if err := dispatcher.Push(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Push(context.Background(), ordinary); !errors.Is(err, ErrClosing) {
		t.Fatalf("Push after terminal staging error = %v, want ErrClosing", err)
	}
	for _, want := range []string{"ordinary", "closing"} {
		event, err := dispatcher.Next(context.Background())
		if err != nil || event.ID != want {
			t.Fatalf("Next() = %+v, %v, want %s", event, err, want)
		}
	}
	// Seal after closing has already been consumed. Publication history, not
	// pending slice length, owns the exactly-two-event terminal transaction.
	if err := dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	event, err := dispatcher.Next(context.Background())
	if err != nil || event.ID != "closed" {
		t.Fatalf("Next(closed) = %+v, %v", event, err)
	}
	if _, err := dispatcher.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next(after closed) error = %v, want ErrClosed", err)
	}
	stats := dispatcher.Stats()
	if !stats.Closed || stats.Depth != 0 || stats.ShutdownEventDepth != 0 || stats.Capacity != 1 || stats.ShutdownEventCapacity != 2 {
		t.Fatalf("final Stats() = %+v", stats)
	}
}

func TestShutdownLifecycleReserveIsFixedAndIndependentOfQueueCapacity(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := model.Event{ID: "ordinary", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
	closing := model.Event{ID: "closing", Name: "session.state_changed", CreatedAt: time.Unix(2, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleCreated, Current: model.LifecycleClosing}}
	closed := model.Event{ID: "closed", Name: "session.state_changed", CreatedAt: time.Unix(3, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleClosing, Current: model.LifecycleClosed}}
	if err := dispatcher.Push(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	stats := dispatcher.Stats()
	if stats.Depth != 1 || stats.Capacity != 1 || stats.ShutdownEventDepth != 2 || stats.ShutdownEventCapacity != 2 {
		t.Fatalf("staged Stats() = %+v", stats)
	}
	for _, want := range []string{"ordinary", "closing", "closed"} {
		event, err := dispatcher.Next(context.Background())
		if err != nil || event.ID != want {
			t.Fatalf("Next() = %+v, %v, want %s", event, err, want)
		}
	}
}

func TestShutdownLifecycleReserveConcurrentSealAndConsumption(t *testing.T) {
	type outcome struct {
		ids []string
		err error
	}
	for iteration := 0; iteration < 100; iteration++ {
		dispatcher, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		ordinary := model.Event{ID: "ordinary", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
		closing := model.Event{ID: "closing", Name: "session.state_changed", CreatedAt: time.Unix(2, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleReady, Current: model.LifecycleClosing}}
		closed := model.Event{ID: "closed", Name: "session.state_changed", CreatedAt: time.Unix(3, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleClosing, Current: model.LifecycleClosed}}
		if err := dispatcher.Push(context.Background(), ordinary); err != nil {
			t.Fatal(err)
		}
		if err := dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		result := make(chan outcome, 1)
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ids := make([]string, 0, 3)
			for len(ids) != 3 {
				event, nextErr := dispatcher.Next(ctx)
				if nextErr != nil {
					result <- outcome{ids: ids, err: nextErr}
					return
				}
				ids = append(ids, event.ID)
			}
			result <- outcome{ids: ids}
		}()

		close(start)
		if err := dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
			t.Fatalf("iteration %d: SealShutdownWithEvent() error = %v", iteration, err)
		}
		got := <-result
		if got.err != nil {
			t.Fatalf("iteration %d: Next() error after %v = %v", iteration, got.ids, got.err)
		}
		want := []string{"ordinary", "closing", "closed"}
		if !slices.Equal(got.ids, want) {
			t.Fatalf("iteration %d: delivery order = %v, want %v", iteration, got.ids, want)
		}
	}
}
