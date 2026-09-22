package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// AuthenticatedPublisher publishes a fully constructed authenticated service
// graph and its immutable SDK personality. It must hold its own publication
// lock while calling commitReady. A successful commitReady call is the
// transaction's terminal commit point: the publisher must then return without
// doing fallible work. If commitReady fails, the publisher must restore its
// prior unpublished state before returning the error.
// The publisher must not call any other Runtime service while it holds its
// publication lock.
type AuthenticatedPublisher func(commitReady func() error) error

var authenticatedPath = [...]model.LifecycleState{
	stateAuthenticating,
	stateMeshConnected,
	stateDurableReady,
	statePeerLinkBuilding,
	stateReady,
}

const (
	stateCreated          = model.LifecycleCreated
	stateAuthenticating   = model.LifecycleAuthenticating
	stateMeshConnected    = model.LifecycleMeshConnected
	stateDurableReady     = model.LifecycleDurableReady
	statePeerLinkBuilding = model.LifecyclePeerLinkBuilding
	stateReady            = model.LifecycleReady
	stateDegraded         = model.LifecycleDegraded
	stateClosing          = model.LifecycleClosing
	stateClosed           = model.LifecycleClosed
	stateFailed           = model.LifecycleFailed
)

// Transition validates and applies one internal lifecycle transition. Injected
// authentication handlers use it after kernel Start.
func (r *Runtime) Transition(next model.LifecycleState) error {
	if next == stateClosing || next == stateClosed {
		return ErrInvalidTransition
	}
	r.mu.RLock()
	started := r.startPhase == startComplete
	r.mu.RUnlock()
	if !started {
		return ErrInvalidTransition
	}
	return r.transition(next)
}

func (r *Runtime) transition(next model.LifecycleState) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	return r.transitionLocked(next)
}

func (r *Runtime) transitionLocked(next model.LifecycleState) error {
	r.mu.Lock()
	previous := r.state
	if previous == next {
		r.mu.Unlock()
		return nil
	}
	if !legalTransition(previous, next) {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, previous, next)
	}
	r.state = next
	r.status.Lifecycle = next
	r.mu.Unlock()

	// Lifecycle serialization spans both state mutation and event admission.
	// This prevents a later transition (especially closing) from publishing
	// ahead of an earlier transition whose event construction or admission is
	// temporarily stalled.
	terminal := next == stateClosed
	r.eventMu.Lock()
	event, err := r.newLifecycleEvent(previous, next)
	if err == nil {
		if terminal {
			err = r.sealEventShutdown(event)
		} else {
			err = r.publishEvent(context.Background(), event)
		}
	}
	r.eventMu.Unlock()
	if terminal && err != nil {
		return err
	}
	if err != nil && err != delivery.ErrQueueFull && err != delivery.ErrClosing && err != delivery.ErrClosed {
		return err
	}
	return nil
}

// CommitAuthenticated atomically publishes the authenticated service graph
// with the complete created-to-ready lifecycle path. It serializes against
// shutdown and all lifecycle changes. Until commitReady succeeds, Runtime
// remains created and no lifecycle event is published.
func (r *Runtime) CommitAuthenticated(ctx context.Context, publisher AuthenticatedPublisher) error {
	if ctx == nil {
		return ErrNilContext
	}
	if publisher == nil {
		return ErrAuthenticatedCommit
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	started := r.startPhase == startComplete
	state := r.state
	closing := r.shutdownStarted
	r.mu.RUnlock()
	if !started || state != stateCreated {
		if closing || state == stateClosing {
			return ErrClosing
		}
		if state == stateClosed {
			return ErrClosed
		}
		return ErrInvalidTransition
	}

	// Serialize the ordered lifecycle batch with every other Runtime event
	// producer. Existing bounded nonblocking overflow remains nonfatal: state
	// and publisher visibility are the transaction, while events are bounded
	// observations and cannot make authentication wait for an SDK consumer.
	r.eventMu.Lock()
	defer r.eventMu.Unlock()

	called := false
	committed := false
	commitReady := func() error {
		if committed {
			return nil
		}
		if called {
			return ErrAuthenticatedCommit
		}
		called = true
		if err := ctx.Err(); err != nil {
			return err
		}

		events := make([]model.Event, 0, len(authenticatedPath))
		previous := stateCreated
		for _, next := range authenticatedPath {
			event, err := r.newLifecycleEvent(previous, next)
			if err != nil {
				return err
			}
			events = append(events, event)
			previous = next
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		if r.state != stateCreated || r.shutdownStarted {
			r.mu.Unlock()
			return ErrClosing
		}
		r.state = stateReady
		r.status.Lifecycle = stateReady
		r.mu.Unlock()
		for _, event := range events {
			_ = r.publishEvent(context.Background(), event)
		}
		committed = true
		return nil
	}

	err := invokeAuthenticatedPublisher(publisher, commitReady)
	if committed {
		return nil
	}
	if err == nil {
		err = ErrAuthenticatedCommit
	}
	return errors.Join(ErrAuthenticatedCommit, err)
}

func invokeAuthenticatedPublisher(publisher AuthenticatedPublisher, commitReady func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPublisherPanic
		}
	}()
	return publisher(commitReady)
}

func legalTransition(previous, next model.LifecycleState) bool {
	switch previous {
	case stateCreated:
		return next == stateAuthenticating || next == stateClosing || next == stateFailed
	case stateAuthenticating:
		return next == stateMeshConnected || next == stateClosing || next == stateFailed
	case stateMeshConnected:
		return next == stateDurableReady || next == stateDegraded || next == stateClosing || next == stateFailed
	case stateDurableReady:
		return next == statePeerLinkBuilding || next == stateReady || next == stateDegraded || next == stateClosing || next == stateFailed
	case statePeerLinkBuilding:
		return next == stateReady || next == stateDegraded || next == stateClosing || next == stateFailed
	case stateReady:
		return next == stateDegraded || next == stateClosing || next == stateFailed
	case stateDegraded:
		return next == stateReady || next == stateClosing || next == stateFailed
	case stateFailed:
		return next == stateClosing
	case stateClosing:
		return next == stateClosed
	case stateClosed:
		return false
	default:
		return false
	}
}
