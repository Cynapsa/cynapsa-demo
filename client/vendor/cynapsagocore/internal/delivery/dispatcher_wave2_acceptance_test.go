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

func TestAcceptanceManyProducersSlowAndVanishedConsumersStayBounded(t *testing.T) {
	const (
		capacity  = 32
		producers = 512
	)
	dispatcher, err := New(capacity)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, producers)
	var group sync.WaitGroup
	group.Add(producers)
	for index := range producers {
		index := index
		go func() {
			defer group.Done()
			<-start
			results <- dispatcher.Push(context.Background(), qaEvent(fmt.Sprintf("producer-%03d", index)))
		}()
	}
	close(start)
	group.Wait()
	close(results)
	accepted := 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrQueueFull):
		default:
			t.Fatalf("producer error = %v", err)
		}
	}
	if accepted != capacity {
		t.Fatalf("accepted = %d, want bounded capacity %d", accepted, capacity)
	}
	if stats := dispatcher.Stats(); stats.Depth != capacity || stats.Capacity != capacity {
		t.Fatalf("saturated Stats() = %+v", stats)
	}

	// A vanished consumer cannot force blocking admission or an unbounded wait.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := dispatcher.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if stats := dispatcher.Stats(); !stats.Closed || stats.Depth != 0 {
		t.Fatalf("deadline cleanup Stats() = %+v", stats)
	}
}

func TestAcceptanceSlowConsumerObservesAcceptedFIFOAndShutdownWakeup(t *testing.T) {
	const count = 128
	dispatcher, err := New(count)
	if err != nil {
		t.Fatal(err)
	}
	for index := range count {
		if err := dispatcher.Push(context.Background(), qaEvent(fmt.Sprintf("event-%03d", index))); err != nil {
			t.Fatal(err)
		}
	}

	dispatcher.BeginShutdown()
	for index := range count {
		event, err := dispatcher.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(%d) error = %v", index, err)
		}
		if want := fmt.Sprintf("event-%03d", index); event.ID != want {
			t.Fatalf("Next(%d) ID = %q, want %q", index, event.ID, want)
		}
	}
	if _, err := dispatcher.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next() after drain = %v, want ErrClosed", err)
	}
	if err := dispatcher.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptanceBlockedConsumersAllExitDuringShutdown(t *testing.T) {
	dispatcher, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	const consumers = 64
	start := make(chan struct{})
	errs := make(chan error, consumers)
	var group sync.WaitGroup
	group.Add(consumers)
	for range consumers {
		go func() {
			defer group.Done()
			<-start
			_, err := dispatcher.Next(context.Background())
			errs <- err
		}()
	}
	close(start)
	dispatcher.BeginShutdown()
	group.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrClosing) && !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked consumer error = %v", err)
		}
	}
}

func qaEvent(id string) model.Event {
	return model.Event{
		ID:        id,
		Name:      "core.error",
		CreatedAt: time.Unix(1, 0),
		Value:     model.CoreErrorEvent{Err: &model.Error{Code: "qa"}},
	}
}
