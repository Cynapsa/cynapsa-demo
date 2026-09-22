package delivery

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Next waits for the next private ordered delivery and transfers exclusive
// ownership to the caller. Push and pull consume this same queue.
func (d *Dispatcher) Next(ctx context.Context) (model.Event, error) {
	if ctx == nil {
		return model.Event{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Event{}, err
	}

	if err := d.acquireConsumer(ctx, true); err != nil {
		return model.Event{}, err
	}
	event, err := d.nextLocked(ctx)
	d.releaseConsumer()
	if err == nil {
		d.afterConsumption()
	}
	return event, err
}

// TryNext consumes the next event from the same authoritative queue without
// waiting. It exists for command handlers that run on Runtime's sole dispatcher
// worker and therefore must never block that worker on SDK consumption.
func (d *Dispatcher) TryNext(ctx context.Context) (model.Event, error) {
	if ctx == nil {
		return model.Event{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Event{}, err
	}

	if err := d.acquireConsumer(ctx, false); err != nil {
		return model.Event{}, ErrConsumerBusy
	}
	var event model.Event
	var err error
	d.mu.Lock()
	if d.state == dispatcherClosed {
		err = ErrClosed
	} else if d.front != nil {
		record := *d.front
		event = record.event
		d.front = nil
		d.releaseRecordLocked(record, false)
	} else {
		select {
		case record := <-d.queue:
			event = record.event
			d.releaseRecordLocked(record, false)
		default:
			if len(d.shutdownEvents) != 0 {
				record := d.shutdownEvents[0]
				event = record.event
				d.shutdownEvents = d.shutdownEvents[1:]
				d.releaseRecordLocked(record, false)
			} else if d.state == dispatcherClosing {
				err = ErrClosing
			} else {
				err = ErrQueueEmpty
			}
		}
	}
	d.mu.Unlock()
	d.releaseConsumer()
	if err == nil {
		d.afterConsumption()
	}
	return event, err
}

func (d *Dispatcher) afterConsumption() {
	d.mu.Lock()
	d.notifyLocked()
	shouldFinalize := d.state == dispatcherClosing && d.shutdownSealed && d.totalDepthLocked() == 0
	d.mu.Unlock()
	if shouldFinalize {
		d.finalizeClosed()
	}
}

func (d *Dispatcher) nextLocked(ctx context.Context) (model.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return model.Event{}, err
		}
		d.mu.Lock()
		if d.state == dispatcherClosed {
			d.mu.Unlock()
			return model.Event{}, ErrClosed
		}
		if d.front != nil {
			record := *d.front
			event := record.event
			d.front = nil
			d.releaseRecordLocked(record, false)
			d.mu.Unlock()
			return event, nil
		}
		select {
		case record := <-d.queue:
			d.releaseRecordLocked(record, false)
			d.mu.Unlock()
			return record.event, nil
		default:
		}
		if len(d.shutdownEvents) != 0 {
			record := d.shutdownEvents[0]
			event := record.event
			d.shutdownEvents = d.shutdownEvents[1:]
			d.releaseRecordLocked(record, false)
			d.mu.Unlock()
			return event, nil
		}
		changed := d.changed
		d.mu.Unlock()

		select {
		case <-ctx.Done():
			return model.Event{}, ctx.Err()
		case <-d.closedCh:
			return model.Event{}, ErrClosed
		case <-changed:
		}
	}
}

// BeginShutdown immediately stops new event admission. Existing events remain
// available to Next until drained or a Shutdown deadline abandons them.
func (d *Dispatcher) BeginShutdown() {
	d.mu.Lock()
	if d.state == dispatcherOpen {
		d.state = dispatcherClosing
		d.shutdownSealed = true
		close(d.shutdownCh)
		d.notifyLocked()
	}
	empty := d.shutdownSealed && d.totalDepthLocked() == 0
	d.mu.Unlock()
	if empty {
		d.finalizeClosed()
	}
}

// Shutdown stops admission and waits for consumers to drain. On deadline it
// abandons only undrained local events and releases dispatcher-owned memory.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	d.BeginShutdown()

	for {
		d.mu.Lock()
		if d.state == dispatcherClosed {
			d.mu.Unlock()
			return nil
		}
		if d.shutdownSealed && d.totalDepthLocked() == 0 {
			d.mu.Unlock()
			d.finalizeClosed()
			return nil
		}
		changed := d.changed
		d.mu.Unlock()

		select {
		case <-ctx.Done():
			d.finalizeClosed()
			return ctx.Err()
		case <-d.closedCh:
			return nil
		case <-changed:
		}
	}
}

// Clear abandons queued output after shutdown has begun.
func (d *Dispatcher) Clear(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	closing := d.state != dispatcherOpen
	d.mu.Unlock()
	if !closing {
		return ErrNotClosing
	}
	d.finalizeClosed()
	return nil
}

func (d *Dispatcher) finalizeClosed() {
	d.finalize.Do(func() {
		// A command lease is prompt but may still be held by a defective handler
		// when the owned shutdown deadline abandons local output. Invalidate that
		// lease and release its consumer ownership before joining other consumers.
		d.mu.Lock()
		d.state = dispatcherClosed
		if d.activeLease != nil {
			record := queuedEvent{event: d.activeLease.event, admission: d.activeLease.admission, bytes: d.activeLease.bytes, shutdown: d.activeLease.shutdown}
			// ReserveNext transferred a borrowed event view to the active
			// consumer. Final shutdown invalidates the lease and releases the
			// dispatcher's accounting/reference, but cannot clear backing memory
			// while the consumer may still be mapping or encoding that view.
			d.releaseRecordLocked(record, false)
			d.activeLease.dispatcher = nil
			d.activeLease.event = model.Event{}
			d.activeLease.bytes = 0
			d.activeLease = nil
			d.releaseConsumerLocked()
		}
		// Wake a blocking consumer before waiting to acquire consumerMu. The
		// consumer rechecks state under mu, returns ErrClosed, and releases its
		// ownership without inspecting buffered or reserved output.
		d.notifyLocked()
		d.mu.Unlock()
		d.consumerMu.Lock()
		d.mu.Lock()
		if d.front != nil {
			d.releaseRecordLocked(*d.front, true)
		}
		d.front = nil
		for index := range d.shutdownEvents {
			d.releaseRecordLocked(d.shutdownEvents[index], true)
		}
		d.shutdownEvents = nil
		d.shutdownSealed = true
		d.shutdownStaged = shutdownEventCapacity
		for {
			select {
			case record := <-d.queue:
				d.releaseRecordLocked(record, true)
			default:
				close(d.closedCh)
				d.releaseConsumerLocked()
				d.notifyLocked()
				d.mu.Unlock()
				return
			}
		}
	})
}

func (d *Dispatcher) notifyLocked() {
	close(d.changed)
	d.changed = make(chan struct{})
}
