package commandgate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestWave2AcceptanceCancellationAtEveryLocalPhase(t *testing.T) {
	t.Run("admission context", func(t *testing.T) {
		gate := acceptanceGate(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		admission, err := gate.Submit(ctx, acceptanceCommand("cancelled-before-admission"))
		if !errors.Is(err, context.Canceled) || admission.Accepted || gate.Stats().RegistryEntries != 0 {
			t.Fatalf("Submit() = (%+v, %v), stats=%+v", admission, err, gate.Stats())
		}
	})

	t.Run("queued before dispatch", func(t *testing.T) {
		gate := acceptanceGate(t, 1)
		admission := acceptanceSubmit(t, gate, "queued")
		if err := gate.Cancel(admission.CommandHandle); err != nil {
			t.Fatal(err)
		}
		completion := acceptanceCompletion(t, gate)
		if completion.Err == nil || completion.Err.Code != codeCancelled {
			t.Fatalf("completion = %+v", completion)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := gate.Next(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Next() after queued cancellation = %v", err)
		}
	})

	t.Run("dispatched handler", func(t *testing.T) {
		gate := acceptanceGate(t, 1)
		admission := acceptanceSubmit(t, gate, "running")
		entered := make(chan struct{})
		handlerDone := make(chan error, 1)
		go func() {
			handlerDone <- gate.ExecuteNext(context.Background(), func(ctx context.Context, _ model.Command) (model.Result, error) {
				close(entered)
				<-ctx.Done()
				return model.Result{}, ctx.Err()
			})
		}()
		<-entered
		if err := gate.Cancel(admission.CommandHandle); err != nil {
			t.Fatal(err)
		}
		if err := <-handlerDone; err != nil {
			t.Fatalf("ExecuteNext() error = %v", err)
		}
		completion := acceptanceCompletion(t, gate)
		if completion.Err == nil || completion.Err.Code != codeCancelled {
			t.Fatalf("completion = %+v", completion)
		}
	})

	t.Run("terminal and consumed", func(t *testing.T) {
		gate := acceptanceGate(t, 1)
		admission := acceptanceSubmit(t, gate, "completed")
		dispatch := acceptanceDispatch(t, gate)
		if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "completed", Value: model.EmptyResult{}}); err != nil {
			t.Fatal(err)
		}
		if err := gate.Cancel(admission.CommandHandle); !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("Cancel(terminal) = %v", err)
		}
		acceptanceCompletion(t, gate)
		if err := gate.Cancel(admission.CommandHandle); !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("Cancel(retained terminal) = %v", err)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		gate := acceptanceGate(t, 1)
		admission := acceptanceSubmit(t, gate, "shutdown")
		gate.BeginShutdown()
		if err := gate.Cancel(admission.CommandHandle); !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("Cancel(shutdown terminal) = %v", err)
		}
		completion := acceptanceCompletion(t, gate)
		if completion.Err == nil || completion.Err.Code != codeShutdown {
			t.Fatalf("completion = %+v", completion)
		}
	})
}

func TestWave2AcceptanceCompletionBackpressureUnderFullWorkerLoad(t *testing.T) {
	const capacity = 64
	gate := acceptanceGate(t, capacity)
	for index := range capacity {
		acceptanceSubmit(t, gate, fmt.Sprintf("load-%03d", index))
	}

	var workers sync.WaitGroup
	workers.Add(capacity)
	for range capacity {
		go func() {
			defer workers.Done()
			if err := gate.ExecuteNext(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
				return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
			}); err != nil {
				t.Errorf("ExecuteNext() error = %v", err)
			}
		}()
	}
	workers.Wait()
	if stats := gate.Stats(); stats.CompletionQueueDepth != capacity || stats.RegistryEntries != capacity {
		t.Fatalf("full completion backpressure Stats() = %+v", stats)
	}
	if _, err := gate.Submit(context.Background(), acceptanceCommand("vanished-consumer")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("Submit() with vanished consumer = %v", err)
	}

	seen := make(map[string]struct{}, capacity)
	for range capacity {
		completion := acceptanceCompletion(t, gate)
		if _, duplicate := seen[completion.CommandID]; duplicate {
			t.Fatalf("duplicate completion %q", completion.CommandID)
		}
		seen[completion.CommandID] = struct{}{}
	}
	if stats := gate.Stats(); stats.RegistryEntries != 0 || stats.CompletionQueueDepth != 0 {
		t.Fatalf("drained Stats() = %+v", stats)
	}
}

func TestWave2AcceptanceHandleRegistryDoesNotReuseAuthority(t *testing.T) {
	const iterations = 4096
	gate := acceptanceGate(t, 2)
	seen := make(map[CommandHandle]struct{}, iterations)
	for iteration := range iterations {
		id := fmt.Sprintf("authority-%05d", iteration)
		admission := acceptanceSubmit(t, gate, id)
		if _, duplicate := seen[admission.CommandHandle]; duplicate {
			t.Fatalf("handle reused at iteration %d", iteration)
		}
		seen[admission.CommandHandle] = struct{}{}
		dispatch := acceptanceDispatch(t, gate)
		if err := gate.Complete(dispatch.Completion, model.Result{CommandID: id, Value: model.EmptyResult{}}); err != nil {
			t.Fatal(err)
		}
		acceptanceCompletion(t, gate)
	}
	if stats := gate.Stats(); stats.RegistryEntries != 0 || stats.CommandQueueDepth != 0 || stats.CompletionQueueDepth != 0 {
		t.Fatalf("final Stats() = %+v", stats)
	}
}
