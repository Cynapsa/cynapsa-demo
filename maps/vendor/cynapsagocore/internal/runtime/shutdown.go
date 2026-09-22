package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// BeginShutdown idempotently stops admission, cancels owned work, publishes
// closing, and starts the one Runtime-owned cleanup operation. It never waits
// for cleanup and has no caller context to transfer into that operation.
func (r *Runtime) BeginShutdown() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	r.mu.Lock()
	if r.shutdownStarted || r.state == stateClosed {
		r.mu.Unlock()
		return
	}
	previous := r.state
	changed := previous != stateClosing
	if changed && !legalTransition(previous, stateClosing) {
		r.mu.Unlock()
		return
	}
	r.shutdownStarted = true
	r.state = stateClosing
	r.status.Lifecycle = stateClosing
	r.mu.Unlock()

	// Inbound application deliveries remain Runtime-owned until their exact
	// delivery.accept. Retire them before canceling command work so a losing
	// transactional lease cannot restore one ahead of lifecycle shutdown.
	r.retireAllInbound(ErrClosing)
	r.gate.BeginShutdown()
	r.rootCancel(ErrClosing)
	var initiationErr error
	if changed {
		r.eventMu.Lock()
		if event, err := r.newLifecycleEvent(previous, stateClosing); err == nil {
			initiationErr = r.beginEventShutdown(event)
		} else {
			initiationErr = err
			r.events.BeginShutdown()
		}
		r.eventMu.Unlock()
	} else {
		r.events.BeginShutdown()
	}
	if initiationErr != nil {
		r.mu.Lock()
		r.shutdownInitErr = errors.Join(r.shutdownInitErr, initiationErr)
		r.mu.Unlock()
	}
	r.shutdownOnce.Do(func() { go r.runShutdown() })
}

// Shutdown starts cleanup exactly once and waits only for this caller's
// context. Caller timeout or cancellation never cancels Runtime cleanup and is
// never stored as the cleanup result observed by later callers.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	r.BeginShutdown()

	// Prefer an already completed cleanup even when the join context is also
	// done, making late joins deterministic.
	select {
	case <-r.shutdownDone:
		r.mu.RLock()
		err := r.shutdownErr
		r.mu.RUnlock()
		return err
	default:
	}
	select {
	case <-r.shutdownDone:
		r.mu.RLock()
		err := r.shutdownErr
		r.mu.RUnlock()
		return err
	case <-ctx.Done():
		return errors.Join(ErrShutdownTimeout, ctx.Err())
	}
}

func (r *Runtime) runShutdown() {
	r.mu.RLock()
	err := r.shutdownInitErr
	r.mu.RUnlock()
	defer func() {
		if recover() != nil {
			err = errors.Join(err, ErrShutdownPanic)
		}
		r.mu.Lock()
		r.shutdownErr = err
		r.mu.Unlock()
		close(r.shutdownDone)
	}()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), r.config.CleanupTimeout)
	defer cancel()
	err = errors.Join(err, r.cleanup(cleanupCtx))
}

func (r *Runtime) cleanup(ctx context.Context) error {
	var result error

	r.mu.RLock()
	phase := r.startPhase
	startDone := r.startDone
	r.mu.RUnlock()
	if phase == startRunning {
		select {
		case <-startDone:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
			// A component Start call cannot be forcibly revoked. Do not claim
			// closed or race startup's reverse rollback: wait until startup has
			// transferred or fully released every component it touched. The
			// expired owned context still bounds every subsequent cleanup call.
			<-startDone
		}
	}

	r.mu.RLock()
	workerStarted := r.workerStarted
	workerDone := r.workerDone
	r.mu.RUnlock()
	if workerStarted {
		select {
		case <-workerDone:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
	}

	r.mu.Lock()
	started := append([]lifecycleComponent(nil), r.started...)
	r.started = nil
	r.mu.Unlock()
	for index := len(started) - 1; index >= 0; index-- {
		component := started[index]
		if err := invokeComponentShutdown(ctx, component.component); err != nil {
			result = errors.Join(result, fmt.Errorf("%w: %s: %w", ErrComponentStopFailed, component.name, err))
		}
	}

	if err := r.gate.Shutdown(ctx); err != nil {
		result = errors.Join(result, err)
	}
	r.clearLocalControls()

	// Component, worker, and gate cleanup precedes the final lifecycle commit.
	// The remaining event drain is post-cleanup delivery disposition, so any
	// observer that receives the closed event also observes Status as closed.
	r.lifecycleMu.Lock()
	if err := r.transitionLocked(stateClosed); err != nil {
		result = errors.Join(result, err)
	}
	r.lifecycleMu.Unlock()
	if err := r.events.Shutdown(ctx); err != nil {
		result = errors.Join(result, err)
	}
	if err := ctx.Err(); err != nil {
		result = errors.Join(result, ErrShutdownTimeout, err)
	}
	return result
}

func (r *Runtime) newLifecycleEvent(previous, next model.LifecycleState) (event model.Event, err error) {
	defer func() {
		if recover() != nil {
			event = model.Event{}
			err = ErrLifecycleEvent
			_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
		}
	}()
	event = model.Event{
		ID:        r.nextEventID(),
		Name:      "session.state_changed",
		CreatedAt: r.clock.Now(),
		Value:     model.SessionStateChangedEvent{Previous: previous, Current: next},
	}
	if event.CreatedAt.IsZero() {
		_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
		return model.Event{}, ErrLifecycleEvent
	}
	return event, nil
}
