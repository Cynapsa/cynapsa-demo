package delivery

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// EventLease reserves the head event for a command completion transaction.
// Its fields are private so a lease can only be finalized by its Dispatcher.
type EventLease struct {
	dispatcher *Dispatcher
	event      model.Event
	eventID    string
	admission  AdmissionID
	bytes      uint64
	shutdown   bool
	retired    bool
}

// AdmissionID returns the private tracked-admission identity, or zero for an
// ordinary event.
func (lease *EventLease) AdmissionID() AdmissionID {
	if lease == nil {
		return 0
	}
	return lease.admission
}

// EventID returns the exact reserved event identity.
func (lease *EventLease) EventID() string {
	if lease == nil {
		return ""
	}
	return lease.eventID
}

// ReserveNext reserves the authoritative queue head without transferring final
// ownership. Commit completes the transfer; Rollback restores the event ahead
// of every later queued event. Only one consumer can own the queue while a
// lease is active, preserving ordering and preventing duplicate delivery.
func (d *Dispatcher) ReserveNext(ctx context.Context) (model.Event, *EventLease, error) {
	if ctx == nil {
		return model.Event{}, nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Event{}, nil, err
	}
	if err := d.acquireConsumer(ctx, false); err != nil {
		return model.Event{}, nil, ErrConsumerBusy
	}

	d.mu.Lock()
	if err := ctx.Err(); err != nil {
		d.mu.Unlock()
		d.releaseConsumer()
		return model.Event{}, nil, err
	}
	if d.state == dispatcherClosed {
		d.mu.Unlock()
		d.releaseConsumer()
		return model.Event{}, nil, ErrClosed
	}
	var record queuedEvent
	if d.front != nil {
		record = *d.front
		d.front = nil
	} else {
		select {
		case record = <-d.queue:
		default:
			var err error
			switch d.state {
			case dispatcherClosing:
				err = ErrClosing
			case dispatcherClosed:
				err = ErrClosed
			default:
				err = ErrQueueEmpty
			}
			d.mu.Unlock()
			d.releaseConsumer()
			return model.Event{}, nil, err
		}
	}
	lease := &EventLease{dispatcher: d, event: record.event, eventID: record.event.ID, admission: record.admission, bytes: record.bytes, shutdown: record.shutdown}
	d.activeLease = lease
	d.mu.Unlock()
	return record.event, lease, nil
}

// ReserveNextWait waits for and reserves the authoritative queue head without
// transferring final ownership. The returned lease has the same commit and
// rollback contract as ReserveNext.
func (d *Dispatcher) ReserveNextWait(ctx context.Context) (model.Event, *EventLease, error) {
	if ctx == nil {
		return model.Event{}, nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Event{}, nil, err
	}

	if err := d.acquireConsumer(ctx, true); err != nil {
		return model.Event{}, nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			d.releaseConsumer()
			return model.Event{}, nil, err
		}
		d.mu.Lock()
		if d.state == dispatcherClosed {
			d.mu.Unlock()
			d.releaseConsumer()
			return model.Event{}, nil, ErrClosed
		}

		var record queuedEvent
		found := false
		if d.front != nil {
			record = *d.front
			d.front = nil
			found = true
		} else {
			select {
			case record = <-d.queue:
				found = true
			default:
			}
		}
		if !found && len(d.shutdownEvents) != 0 {
			record = d.shutdownEvents[0]
			d.shutdownEvents = d.shutdownEvents[1:]
			found = true
		}
		if found {
			lease := &EventLease{dispatcher: d, event: record.event, eventID: record.event.ID, admission: record.admission, bytes: record.bytes, shutdown: record.shutdown}
			d.activeLease = lease
			d.mu.Unlock()
			return record.event, lease, nil
		}
		if d.state == dispatcherClosing && d.shutdownSealed {
			d.mu.Unlock()
			d.releaseConsumer()
			return model.Event{}, nil, ErrClosing
		}
		changed := d.changed
		d.mu.Unlock()

		select {
		case <-ctx.Done():
			d.releaseConsumer()
			return model.Event{}, nil, ctx.Err()
		case <-d.closedCh:
			d.releaseConsumer()
			return model.Event{}, nil, ErrClosed
		case <-changed:
		}
	}
}

// RetireAdmission removes one exact tracked admission from dispatcher
// ownership. If a consumer currently holds it, the lease is marked so either
// terminal disposition drops it instead of committing or restoring it.
func (d *Dispatcher) RetireAdmission(admission AdmissionID) bool {
	if admission == 0 {
		return false
	}
	d.mu.Lock()
	found := false
	if d.front != nil && d.front.admission == admission {
		d.releaseRecordLocked(*d.front, true)
		d.front = nil
		found = true
	}
	if !found && d.activeLease != nil && d.activeLease.admission == admission {
		d.activeLease.retired = true
		found = true
	}
	if !found {
		queued := len(d.queue)
		for range queued {
			record := <-d.queue
			if !found && record.admission == admission {
				found = true
				d.releaseRecordLocked(record, true)
				continue
			}
			d.queue <- record
		}
	}
	if found {
		d.notifyLocked()
	}
	shouldFinalize := d.state == dispatcherClosing && d.shutdownSealed && d.totalDepthLocked() == 0
	d.mu.Unlock()
	if shouldFinalize {
		d.finalizeClosed()
	}
	return found
}

// Commit transfers the leased event to the command result owner.
func (d *Dispatcher) Commit(lease *EventLease) error {
	return d.finalizeLease(lease, false)
}

// Rollback restores the leased event to the authoritative queue head.
func (d *Dispatcher) Rollback(lease *EventLease) error {
	return d.finalizeLease(lease, true)
}

func (d *Dispatcher) finalizeLease(lease *EventLease, rollback bool) error {
	if lease == nil {
		return ErrInvalidLease
	}
	d.mu.Lock()
	if lease.dispatcher != d || d.activeLease != lease {
		d.mu.Unlock()
		return ErrInvalidLease
	}
	retired := lease.retired
	if rollback && !retired {
		record := queuedEvent{event: lease.event, admission: lease.admission, bytes: lease.bytes, shutdown: lease.shutdown}
		d.front = &record
	} else {
		record := queuedEvent{event: lease.event, admission: lease.admission, bytes: lease.bytes, shutdown: lease.shutdown}
		d.releaseRecordLocked(record, retired)
	}
	d.activeLease = nil
	lease.dispatcher = nil
	lease.event = model.Event{}
	lease.bytes = 0
	d.notifyLocked()
	shouldFinalize := d.state == dispatcherClosing && d.shutdownSealed && d.totalDepthLocked() == 0
	d.mu.Unlock()
	d.releaseConsumer()
	if shouldFinalize {
		d.finalizeClosed()
	}
	if retired {
		return ErrLeaseRetired
	}
	return nil
}
