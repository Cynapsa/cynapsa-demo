package payload

import (
	"context"
	"sync"
)

// Job is private typed work; Execute must honor cancellation and never return
// an SDK-facing value.
type Job interface {
	Execute(context.Context) (TransferReceipt, error)
}

type JobResult struct {
	Receipt TransferReceipt
	Err     error
}

type queuedJob struct {
	ctx    context.Context
	job    Job
	result chan JobResult
}

// TransferWorker is a bounded cancellable worker pool outside peer workers.
type TransferWorker struct {
	mu      sync.Mutex
	workers int
	queue   chan queuedJob
	runCtx  context.Context
	cancel  context.CancelFunc
	started bool
	closed  bool
	wg      sync.WaitGroup
}

func NewTransferWorker(workers, capacity int) (*TransferWorker, error) {
	if workers <= 0 || workers > 1024 || capacity <= 0 || capacity > 65536 {
		return nil, ErrInvalidLimits
	}
	return &TransferWorker{workers: workers, queue: make(chan queuedJob, capacity)}, nil
}

// Run starts the pool and blocks until the supplied context is cancelled and
// every worker has exited. Run may be called exactly once.
func (w *TransferWorker) Run(ctx context.Context) error {
	if w == nil || ctx == nil {
		return ErrInvalidLimits
	}
	w.mu.Lock()
	if w.started || w.closed {
		w.mu.Unlock()
		return ErrInvalidHandleState
	}
	w.started = true
	w.runCtx, w.cancel = context.WithCancel(ctx)
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	w.mu.Unlock()
	<-w.runCtx.Done()
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.wg.Wait()
	return nil
}

// Submit transfers ownership of one job only on successful enqueue.
func (w *TransferWorker) Submit(ctx context.Context, job Job) (<-chan JobResult, error) {
	if w == nil || ctx == nil || job == nil {
		return nil, ErrInvalidLimits
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if !w.started || w.closed {
		w.mu.Unlock()
		return nil, ErrWorkerClosed
	}
	runCtx := w.runCtx
	result := make(chan JobResult, 1)
	item := queuedJob{ctx: ctx, job: job, result: result}
	select {
	case <-ctx.Done():
		w.mu.Unlock()
		return nil, ctx.Err()
	case <-runCtx.Done():
		w.mu.Unlock()
		return nil, ErrWorkerClosed
	case w.queue <- item:
		w.mu.Unlock()
		return result, nil
	default:
		w.mu.Unlock()
		return nil, ErrQueueFull
	}
}

func (w *TransferWorker) Shutdown() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	w.closed = true
	w.mu.Unlock()
}

func (w *TransferWorker) worker() {
	defer w.wg.Done()
	for {
		select {
		case <-w.runCtx.Done():
			w.rejectQueued()
			return
		case item := <-w.queue:
			result := executeSafely(w.runCtx, item)
			item.result <- result
			close(item.result)
		}
	}
}

func executeSafely(poolCtx context.Context, item queuedJob) (result JobResult) {
	if poolCtx.Err() != nil {
		return JobResult{Err: ErrWorkerClosed}
	}
	ctx, cancel := context.WithCancel(item.ctx)
	defer cancel()
	stop := context.AfterFunc(poolCtx, cancel)
	defer stop()
	defer func() {
		if recover() != nil {
			result = JobResult{Err: ErrWorkerPanic}
		}
	}()
	result.Receipt, result.Err = item.job.Execute(ctx)
	if contextErr := dependencyContextError(ctx, result.Err); contextErr != nil {
		result.Err = contextErr
	}
	return result
}

func (w *TransferWorker) rejectQueued() {
	for {
		select {
		case item := <-w.queue:
			item.result <- JobResult{Err: ErrWorkerClosed}
			close(item.result)
		default:
			return
		}
	}
}
