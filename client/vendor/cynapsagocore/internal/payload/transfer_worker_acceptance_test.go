package payload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type acceptanceJob func(context.Context) (TransferReceipt, error)

func (j acceptanceJob) Execute(ctx context.Context) (TransferReceipt, error) { return j(ctx) }

func TestAcceptanceWorkerExactSaturationCancellationAndPanicRedaction(t *testing.T) {
	const workers = 4
	const capacity = 64
	pool, err := NewTransferWorker(workers, capacity)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(runCtx) }()
	defer cancelRun()
	acceptanceWaitWorkerStarted(t, pool)

	started := make(chan struct{}, workers)
	release := make(chan struct{})
	blockers := make([]<-chan JobResult, 0, workers)
	for range workers {
		result, err := pool.Submit(context.Background(), acceptanceJob(func(ctx context.Context) (TransferReceipt, error) {
			started <- struct{}{}
			select {
			case <-release:
				return TransferReceipt{}, nil
			case <-ctx.Done():
				return TransferReceipt{}, ctx.Err()
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		blockers = append(blockers, result)
	}
	for range workers {
		<-started
	}

	contexts := make([]context.Context, capacity)
	cancels := make([]context.CancelFunc, capacity)
	queued := make([]<-chan JobResult, 0, capacity)
	for i := range capacity {
		contexts[i], cancels[i] = context.WithCancel(context.Background())
		result, err := pool.Submit(contexts[i], acceptanceJob(func(ctx context.Context) (TransferReceipt, error) {
			if err := ctx.Err(); err != nil {
				return TransferReceipt{}, err
			}
			return TransferReceipt{MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA"}, nil
		}))
		if err != nil {
			t.Fatalf("queue slot %d: %v", i, err)
		}
		queued = append(queued, result)
	}
	if _, err := pool.Submit(context.Background(), acceptanceJob(func(context.Context) (TransferReceipt, error) { return TransferReceipt{}, nil })); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("capacity+1 submission = %v", err)
	}
	for i := 0; i < capacity/2; i++ {
		cancels[i]()
	}
	close(release)
	for _, result := range blockers {
		if got := <-result; got.Err != nil {
			t.Fatalf("blocking job: %v", got.Err)
		}
	}
	cancelled, completed := 0, 0
	for _, result := range queued {
		got := <-result
		switch {
		case errors.Is(got.Err, context.Canceled):
			cancelled++
		case got.Err == nil && got.Receipt.MessageID == "msg_AAAAAAAAAAAAAAAAAAAAAA":
			completed++
		default:
			t.Errorf("queued result: %#v", got)
		}
	}
	if cancelled != capacity/2 || completed != capacity/2 {
		t.Fatalf("cancelled=%d completed=%d", cancelled, completed)
	}

	panicResult, err := pool.Submit(context.Background(), acceptanceJob(func(context.Context) (TransferReceipt, error) {
		panic("worker secret canary https://private.invalid/object")
	}))
	if err != nil {
		t.Fatal(err)
	}
	got := <-panicResult
	if !errors.Is(got.Err, ErrWorkerPanic) || strings.Contains(got.Err.Error(), "secret") || strings.Contains(got.Err.Error(), "private.invalid") {
		t.Fatalf("panic normalization leaked or changed: %v", got.Err)
	}

	pool.Shutdown()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker shutdown timed out")
	}
}

func TestAcceptanceWorkerShutdownTerminalizesQueuedExactlyOnce(t *testing.T) {
	pool, err := NewTransferWorker(1, 32)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(context.Background()) }()
	acceptanceWaitWorkerStarted(t, pool)
	started := make(chan struct{})
	var once sync.Once
	active, err := pool.Submit(context.Background(), acceptanceJob(func(ctx context.Context) (TransferReceipt, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return TransferReceipt{}, ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued := make([]<-chan JobResult, 32)
	for i := range queued {
		queued[i], err = pool.Submit(context.Background(), acceptanceJob(func(context.Context) (TransferReceipt, error) {
			return TransferReceipt{}, errors.New("must not execute")
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	pool.Shutdown()
	if result := <-active; !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("active result = %v", result.Err)
	}
	for i, result := range queued {
		select {
		case got, ok := <-result:
			if !ok || !errors.Is(got.Err, ErrWorkerClosed) {
				t.Errorf("queued %d result=%#v open=%v", i, got, ok)
			}
			if _, stillOpen := <-result; stillOpen {
				t.Errorf("queued %d result channel not closed", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("queued %d was not terminalized", i)
		}
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not return")
	}
}

func acceptanceWaitWorkerStarted(t *testing.T, pool *TransferWorker) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		pool.mu.Lock()
		started := pool.started
		pool.mu.Unlock()
		if started {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not start")
		}
		time.Sleep(time.Millisecond)
	}
}
