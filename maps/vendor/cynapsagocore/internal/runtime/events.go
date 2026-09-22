package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// EventLease retains the exact Dispatcher head until the boundary either
// commits delivery or restores it. Tracked acceptance state is published only
// by CommitEvent.
type EventLease struct {
	lease   *delivery.EventLease
	eventID string
	tracked bool
}

// ReserveEvent reserves the next private runtime event without transferring
// final ownership. When waitForAcceptance is true, a tracked head waits on its
// private acceptance signal instead of returning the transient
// delivery-accept-pending condition.
func (r *Runtime) ReserveEvent(ctx context.Context, waitForAcceptance bool) (model.Event, *EventLease, error) {
	if ctx == nil {
		return model.Event{}, nil, ErrNilContext
	}
	for {
		event, lease, err := r.events.ReserveNextWait(ctx)
		if err != nil {
			return model.Event{}, nil, err
		}
		r.controlMu.Lock()
		if err := ctx.Err(); err != nil {
			_ = r.events.Rollback(lease)
			r.controlMu.Unlock()
			return model.Event{}, nil, err
		}
		admission := lease.AdmissionID()
		_, tracked := r.inboundDeliveries[admission]
		if tracked && r.outstandingDelivery != "" {
			changed := r.deliveryChanged
			_ = r.events.Rollback(lease)
			r.controlMu.Unlock()
			if !waitForAcceptance {
				return model.Event{}, nil, ErrDeliveryAcceptPending
			}
			select {
			case <-ctx.Done():
				return model.Event{}, nil, ctx.Err()
			case <-r.rootCtx.Done():
				return model.Event{}, nil, ErrClosing
			case <-changed:
				continue
			}
		}
		r.controlMu.Unlock()
		return event, &EventLease{lease: lease, eventID: event.ID, tracked: tracked}, nil
	}
}

// CommitEvent transfers a reserved event and publishes tracked acceptance
// ownership in the same control-state critical section.
func (r *Runtime) CommitEvent(lease *EventLease) error {
	if lease == nil || lease.lease == nil {
		return delivery.ErrInvalidLease
	}
	r.controlMu.Lock()
	if lease.tracked && r.outstandingDelivery != "" {
		internal := lease.lease
		lease.lease = nil
		_ = r.events.Rollback(internal)
		r.controlMu.Unlock()
		return ErrDeliveryAcceptPending
	}
	internal := lease.lease
	admission := internal.AdmissionID()
	err := r.events.Commit(internal)
	if err == nil && lease.tracked {
		r.outstandingDelivery = lease.eventID
		r.outstandingInbound = admission
	}
	lease.lease = nil
	r.controlMu.Unlock()
	return normalizeEventLeaseError(r.events, err)
}

// RollbackEvent restores a reserved event to the exact queue head.
func (r *Runtime) RollbackEvent(lease *EventLease) error {
	if lease == nil || lease.lease == nil {
		return delivery.ErrInvalidLease
	}
	internal := lease.lease
	lease.lease = nil
	return normalizeEventLeaseError(r.events, r.events.Rollback(internal))
}

func normalizeEventLeaseError(events *delivery.Dispatcher, err error) error {
	if errors.Is(err, delivery.ErrInvalidLease) {
		if events.Stats().Closed {
			return delivery.ErrClosed
		}
		return delivery.ErrClosing
	}
	return err
}

// NextEvent returns the next typed private runtime event for boundary projection.
func (r *Runtime) NextEvent(ctx context.Context) (model.Event, error) {
	for {
		event, lease, err := r.ReserveEvent(ctx, false)
		if err != nil {
			return model.Event{}, err
		}
		if err = r.CommitEvent(lease); errors.Is(err, delivery.ErrLeaseRetired) {
			continue
		}
		if err != nil {
			return model.Event{}, err
		}
		return event, nil
	}
}

// DeliverInbound admits one typed application delivery to the sole Runtime
// event queue and waits until the exact mandatory delivery.accept transfers
// responsibility to the SDK. Cancellation or shutdown wins by retiring the
// event from every pre-accept queue/lease/outstanding phase.
func (r *Runtime) DeliverInbound(ctx context.Context, value model.MessageReceivedEvent) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	receipt := make(chan error, 1)
	r.eventMu.Lock()
	event := model.Event{
		ID:        r.nextEventID(),
		Name:      "message.received",
		CreatedAt: r.clock.Now(),
		Value:     value,
	}
	r.controlMu.Lock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		r.controlMu.Unlock()
		r.eventMu.Unlock()
		return err
	}
	admission, err := r.events.DeliverTracked(ctx, event)
	if err == nil {
		_ = r.metrics.Add(diagnostics.MetricEventsPublished, 1)
		if r.inboundDeliveries == nil {
			r.inboundDeliveries = make(map[delivery.AdmissionID]chan error)
		}
		r.inboundDeliveries[admission] = receipt
	} else {
		_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
	}
	r.controlMu.Unlock()
	r.eventMu.Unlock()
	if err != nil {
		return err
	}

	for {
		select {
		case result := <-receipt:
			return result
		case <-ctx.Done():
			if r.retireInbound(admission, ctx.Err()) {
				return ctx.Err()
			}
		case <-r.rootCtx.Done():
			if r.retireInbound(admission, ErrClosing) {
				return ErrClosing
			}
		}
	}
}

func (r *Runtime) retireInbound(admission delivery.AdmissionID, result error) bool {
	r.controlMu.Lock()
	receipt, pending := r.inboundDeliveries[admission]
	if !pending {
		r.controlMu.Unlock()
		return false
	}
	delete(r.inboundDeliveries, admission)
	if r.outstandingInbound == admission {
		r.outstandingDelivery = ""
		r.outstandingInbound = 0
		r.notifyDeliveryChangedLocked()
	}
	r.events.RetireAdmission(admission)
	receipt <- result
	r.controlMu.Unlock()
	return true
}

func (r *Runtime) retireAllInbound(result error) {
	r.controlMu.Lock()
	for admission, receipt := range r.inboundDeliveries {
		delete(r.inboundDeliveries, admission)
		if r.outstandingInbound == admission {
			r.outstandingDelivery = ""
			r.outstandingInbound = 0
			r.notifyDeliveryChangedLocked()
		}
		r.events.RetireAdmission(admission)
		receipt <- result
	}
	r.controlMu.Unlock()
}

func (r *Runtime) notifyDeliveryChangedLocked() {
	close(r.deliveryChanged)
	r.deliveryChanged = make(chan struct{})
}

// PublishEvent transfers one already typed, ordered, and materialized event to
// the shared bounded push/pull stream.
func (r *Runtime) PublishEvent(ctx context.Context, event model.Event) error {
	if event.Name == "diagnostics.log" {
		r.controlMu.Lock()
		enabled := r.diagnosticLogs
		r.controlMu.Unlock()
		if !enabled {
			return ErrDiagnosticLogsOff
		}
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	return r.publishEvent(ctx, event)
}

func (r *Runtime) PublishMessageState(ctx context.Context, name string, value model.MessageStateEvent) error {
	if r == nil || ctx == nil {
		return ErrNilContext
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	return r.publishEvent(ctx, model.Event{ID: r.nextEventID(), Name: name, CreatedAt: r.clock.Now(), Value: value})
}

func (r *Runtime) publishEvent(ctx context.Context, event model.Event) error {
	err := r.events.Deliver(ctx, event)
	if err != nil {
		_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
		return err
	}
	_ = r.metrics.Add(diagnostics.MetricEventsPublished, 1)
	return nil
}

func (r *Runtime) beginEventShutdown(event model.Event) error {
	err := r.events.BeginShutdownWithEvent(context.Background(), event)
	if err != nil {
		_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
		return err
	}
	_ = r.metrics.Add(diagnostics.MetricEventsPublished, 1)
	return nil
}

func (r *Runtime) sealEventShutdown(event model.Event) error {
	err := r.events.SealShutdownWithEvent(context.Background(), event)
	if err != nil {
		_ = r.metrics.Add(diagnostics.MetricEventsRejected, 1)
		return err
	}
	_ = r.metrics.Add(diagnostics.MetricEventsPublished, 1)
	return nil
}

func (r *Runtime) nextEventID() string {
	return fmt.Sprintf("event-%d", r.eventSequence.Add(1))
}
