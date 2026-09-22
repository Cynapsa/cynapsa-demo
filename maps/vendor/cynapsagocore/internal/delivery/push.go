package delivery

import (
	"context"
	"math"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Push publishes a private delivery into the bounded stream without invoking
// an SDK callback. Success transfers exclusive ownership of the event and all
// reachable memory to the dispatcher. Rejection retains caller ownership.
func (d *Dispatcher) Push(ctx context.Context, event model.Event) error {
	_, err := d.push(ctx, event, false)
	return err
}

// DeliverTracked admits one event with a private identity suitable for exact
// retirement and acceptance bookkeeping. The identity is independent from the
// caller-visible Event.ID namespace.
func (d *Dispatcher) DeliverTracked(ctx context.Context, event model.Event) (AdmissionID, error) {
	return d.push(ctx, event, true)
}

func (d *Dispatcher) push(ctx context.Context, event model.Event, tracked bool) (AdmissionID, error) {
	if ctx == nil {
		return 0, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	preflightBytes, sizeErr := eventOwnedBytes(event)
	if sizeErr != nil {
		return 0, ErrInvalidEvent
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	switch d.state {
	case dispatcherClosing:
		return 0, ErrClosing
	case dispatcherClosed:
		return 0, ErrClosed
	}
	if d.queueDepthLocked() >= cap(d.queue) {
		return 0, ErrQueueFull
	}
	if preflightBytes > d.ordinaryByteCap || d.ordinaryBytes > d.ordinaryByteCap-preflightBytes {
		return 0, ErrQueueFull
	}
	var admission AdmissionID
	if tracked {
		if d.nextAdmission == AdmissionID(math.MaxUint64) {
			return 0, ErrAdmissionExhausted
		}
		d.nextAdmission++
		admission = d.nextAdmission
	}
	// Count and byte ownership have both been reserved. Consumers receive only
	// while holding d.mu, so the prior depth check guarantees this send cannot
	// block and caller-owned fields are frozen only on the success path.
	event = freezeEventDynamicFields(event)
	eventBytes, frozenSizeErr := eventOwnedBytes(event)
	if frozenSizeErr != nil || eventBytes > preflightBytes {
		panic("delivery: frozen event violated preflight byte invariant")
	}
	d.ordinaryBytes += eventBytes
	d.queue <- queuedEvent{event: event, admission: admission, bytes: eventBytes}
	d.notifyLocked()
	return admission, nil
}

func validEvent(event model.Event) bool {
	_, err := eventOwnedBytes(event)
	return err == nil
}
