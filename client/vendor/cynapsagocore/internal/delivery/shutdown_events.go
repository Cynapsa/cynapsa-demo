package delivery

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const shutdownEventCapacity = 2

// BeginShutdownWithEvent stops ordinary event admission and stages the first
// ordered shutdown lifecycle event without borrowing configured queue
// capacity. The fixed reserve exists because a rolled-back head event may
// already occupy the final ordinary slot when shutdown begins.
func (d *Dispatcher) BeginShutdownWithEvent(ctx context.Context, event model.Event) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	preflightBytes, sizeErr := eventOwnedBytes(event)
	if sizeErr != nil {
		return ErrInvalidEvent
	}
	if preflightBytes > shutdownEventMaximumBytes {
		return ErrQueueFull
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.state == dispatcherClosed {
		return ErrClosed
	}
	if d.state != dispatcherOpen || d.shutdownStaged != 0 {
		return ErrClosing
	}
	if d.shutdownBytes > shutdownEventByteCapacity-preflightBytes {
		return ErrQueueFull
	}
	d.state = dispatcherClosing
	event = freezeEventDynamicFields(event)
	eventBytes, frozenSizeErr := eventOwnedBytes(event)
	if frozenSizeErr != nil || eventBytes > preflightBytes {
		panic("delivery: frozen shutdown event violated preflight byte invariant")
	}
	d.shutdownBytes += eventBytes
	d.shutdownEvents = append(d.shutdownEvents, queuedEvent{event: event, bytes: eventBytes, shutdown: true})
	d.shutdownStaged = 1
	close(d.shutdownCh)
	d.notifyLocked()
	return nil
}

// SealShutdownWithEvent stages the final ordered shutdown lifecycle event and
// closes the fixed reserve to further publication. Consumers observe ordinary
// queued/rolled-back events first, followed by the two reserved events.
func (d *Dispatcher) SealShutdownWithEvent(ctx context.Context, event model.Event) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	preflightBytes, sizeErr := eventOwnedBytes(event)
	if sizeErr != nil {
		return ErrInvalidEvent
	}
	if preflightBytes > shutdownEventMaximumBytes {
		return ErrQueueFull
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.state == dispatcherClosed {
		return ErrClosed
	}
	if d.state != dispatcherClosing || d.shutdownSealed || d.shutdownStaged != 1 {
		return ErrClosing
	}
	if len(d.shutdownEvents) >= shutdownEventCapacity {
		return ErrQueueFull
	}
	if d.shutdownBytes > shutdownEventByteCapacity-preflightBytes {
		return ErrQueueFull
	}
	event = freezeEventDynamicFields(event)
	eventBytes, frozenSizeErr := eventOwnedBytes(event)
	if frozenSizeErr != nil || eventBytes > preflightBytes {
		panic("delivery: frozen shutdown event violated preflight byte invariant")
	}
	d.shutdownBytes += eventBytes
	d.shutdownEvents = append(d.shutdownEvents, queuedEvent{event: event, bytes: eventBytes, shutdown: true})
	d.shutdownStaged = 2
	d.shutdownSealed = true
	d.notifyLocked()
	return nil
}
