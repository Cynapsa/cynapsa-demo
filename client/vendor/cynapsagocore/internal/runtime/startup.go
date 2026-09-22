package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
)

// startup initializes lifecycle components in order and starts the single
// runtime-owned dispatcher worker only after every component succeeds.
func (r *Runtime) startup(ctx context.Context) error {
	// Root lifetime is the direct parent so BeginShutdown cancellation becomes
	// synchronously observable. The caller remains an additional cancellation
	// source, and explicit checks below do not depend on AfterFunc scheduling.
	startupCtx, cancel := context.WithCancelCause(r.rootCtx)
	stopCaller := context.AfterFunc(ctx, func() {
		cancel(ctx.Err())
	})
	defer func() {
		stopCaller()
		cancel(nil)
	}()

	started := make([]lifecycleComponent, 0, len(r.components))
	for _, component := range r.components {
		if err := r.startComponent(ctx, startupCtx, component); err != nil {
			if errors.Is(err, ErrClosing) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return errors.Join(err, r.rollback(started))
			}
			return errors.Join(fmt.Errorf("%w: %s: %w", ErrComponentStartFailed, component.name, err), r.rollback(started))
		}
		started = append(started, component)
		r.mu.Lock()
		r.started = append(r.started, component)
		r.mu.Unlock()
		if err := r.startupCancellation(ctx, startupCtx); err != nil {
			return errors.Join(err, r.rollback(started))
		}
	}
	if err := r.startupCancellation(ctx, startupCtx); err != nil {
		return errors.Join(err, r.rollback(started))
	}

	r.mu.Lock()
	if err := r.startupCancellationLocked(ctx, startupCtx); err != nil {
		r.mu.Unlock()
		return errors.Join(err, r.rollback(started))
	}
	r.workerStarted = true
	r.mu.Unlock()
	_ = r.metrics.Record(diagnostics.MetricActiveWorkers, 1)
	go r.runDispatcher()
	return nil
}

// startComponent atomically admits one Component.Start against shutdown. The
// admission is the invocation's lifecycle linearization point: after it is set,
// BeginShutdown may return with this call classified as already in flight, but
// the same lock prevents every later component from being admitted. Startup
// retains exclusive cleanup ownership until the call returns and rollback is
// complete.
func (r *Runtime) startComponent(caller, startup context.Context, component lifecycleComponent) error {
	r.mu.Lock()
	if err := r.startupCancellationLocked(caller, startup); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.componentStartInFlight {
		r.mu.Unlock()
		return ErrInvalidTransition
	}
	r.componentStartInFlight = true
	r.mu.Unlock()

	err := invokeComponentStart(startup, component.component)
	r.mu.Lock()
	r.componentStartInFlight = false
	r.mu.Unlock()
	return err
}

func (r *Runtime) startupCancellation(caller, startup context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	err := r.startupCancellationLocked(caller, startup)
	r.mu.RUnlock()
	return err
}

func (r *Runtime) startupCancellationLocked(caller, startup context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if r.shutdownStarted || r.state == stateClosing || r.state == stateClosed {
		return ErrClosing
	}
	if err := r.rootCtx.Err(); err != nil {
		if cause := context.Cause(r.rootCtx); cause != nil {
			return cause
		}
		return err
	}
	if err := startup.Err(); err != nil {
		if cause := context.Cause(startup); cause != nil {
			return cause
		}
		return err
	}
	return nil
}

func (r *Runtime) rollback(started []lifecycleComponent) error {
	var result error
	for index := len(started) - 1; index >= 0; index-- {
		component := started[index]
		if err := invokeComponentShutdown(r.rootCtx, component.component); err != nil {
			result = errors.Join(result, fmt.Errorf("%w: %s: %w", ErrComponentStopFailed, component.name, err))
		}
	}
	r.mu.Lock()
	r.started = nil
	r.mu.Unlock()
	return result
}

func invokeComponentStart(ctx context.Context, component Component) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrComponentPanic
		}
	}()
	return component.Start(ctx)
}

func invokeComponentShutdown(ctx context.Context, component Component) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrComponentPanic
		}
	}()
	return component.Shutdown(ctx)
}
