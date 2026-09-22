// Package delivery owns normalized internal delivery scheduling before boundary projection.
package delivery

import (
	"context"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Dispatcher publishes ordered materialized private deliveries.
type Dispatcher struct {
	mu              sync.Mutex
	consumerMu      sync.Mutex
	consumerChanged chan struct{}
	state           dispatcherState
	queue           chan queuedEvent
	front           *queuedEvent
	activeLease     *EventLease
	nextAdmission   AdmissionID
	ordinaryBytes   uint64
	ordinaryByteCap uint64
	shutdownBytes   uint64
	// shutdownEvents is a fixed lifecycle reserve, not ordinary event-queue
	// capacity. Runtime stages exactly closing and closed here so configured
	// queue pressure cannot discard terminal lifecycle observation.
	shutdownEvents []queuedEvent
	shutdownSealed bool
	shutdownStaged uint8
	changed        chan struct{}
	shutdownCh     chan struct{}
	closedCh       chan struct{}
	finalize       sync.Once
}

// AdmissionID is a dispatcher-private ownership identity. Zero identifies an
// ordinary untracked event; tracked admissions receive a nonzero value that is
// never derived from or compared with the caller-visible Event.ID.
type AdmissionID uint64

type queuedEvent struct {
	event     model.Event
	admission AdmissionID
	bytes     uint64
	shutdown  bool
}

type dispatcherState uint8

const (
	dispatcherOpen dispatcherState = iota
	dispatcherClosing
	dispatcherClosed
)

// Stats is a race-safe resource snapshot.
type Stats struct {
	Depth                 int
	Capacity              int
	OwnedBytes            uint64
	ByteCapacity          uint64
	OrdinaryEventBytes    uint64
	OrdinaryByteCapacity  uint64
	ShutdownEventDepth    int
	ShutdownEventCapacity int
	ShutdownEventBytes    uint64
	ShutdownByteCapacity  uint64
	Closing               bool
	Closed                bool
}

// New constructs one bounded private event stream.
func New(capacity int) (*Dispatcher, error) {
	return NewWithByteCapacity(capacity, maximumOrdinaryEventBytes)
}

// NewWithByteCapacity constructs a dispatcher with an explicit private
// ordinary-event byte budget. The fixed shutdown reserve is always separate
// from and additional to this ordinary budget, while their sum remains below
// the process event-memory ceiling.
func NewWithByteCapacity(capacity int, byteCapacity uint64) (*Dispatcher, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	if byteCapacity == 0 || byteCapacity > maximumOrdinaryEventBytes {
		return nil, ErrInvalidCapacity
	}
	return &Dispatcher{
		queue:           make(chan queuedEvent, capacity),
		ordinaryByteCap: byteCapacity,
		consumerChanged: make(chan struct{}),
		shutdownEvents:  make([]queuedEvent, 0, shutdownEventCapacity),
		changed:         make(chan struct{}),
		shutdownCh:      make(chan struct{}),
		closedCh:        make(chan struct{}),
	}, nil
}

// Stats returns current bounded queue accounting.
func (d *Dispatcher) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Stats{
		Depth:                 d.queueDepthLocked(),
		Capacity:              cap(d.queue),
		OwnedBytes:            d.ordinaryBytes + d.shutdownBytes,
		ByteCapacity:          d.ordinaryByteCap + shutdownEventByteCapacity,
		OrdinaryEventBytes:    d.ordinaryBytes,
		OrdinaryByteCapacity:  d.ordinaryByteCap,
		ShutdownEventDepth:    len(d.shutdownEvents),
		ShutdownEventCapacity: shutdownEventCapacity,
		ShutdownEventBytes:    d.shutdownBytes,
		ShutdownByteCapacity:  shutdownEventByteCapacity,
		Closing:               d.state != dispatcherOpen,
		Closed:                d.state == dispatcherClosed,
	}
}

func (d *Dispatcher) queueDepthLocked() int {
	depth := len(d.queue)
	if d.front != nil {
		depth++
	}
	if d.activeLease != nil {
		depth++
	}
	return depth
}

func (d *Dispatcher) totalDepthLocked() int {
	return d.queueDepthLocked() + len(d.shutdownEvents)
}

func (d *Dispatcher) releaseRecordLocked(record queuedEvent, clear bool) {
	if record.shutdown {
		if record.bytes > d.shutdownBytes {
			panic("delivery: shutdown event byte accounting underflow")
		}
		d.shutdownBytes -= record.bytes
	} else {
		if record.bytes > d.ordinaryBytes {
			panic("delivery: ordinary event byte accounting underflow")
		}
		d.ordinaryBytes -= record.bytes
	}
	if clear {
		clearOwnedEvent(&record.event)
	}
}

func (d *Dispatcher) acquireConsumer(ctx context.Context, wait bool) error {
	if !wait {
		if d.consumerMu.TryLock() {
			return nil
		}
		return ErrConsumerBusy
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.mu.Lock()
		changed := d.consumerChanged
		closed := d.state == dispatcherClosed
		d.mu.Unlock()
		if closed {
			return ErrClosed
		}
		// Retry after capturing the notification channel so an unlock between
		// the first attempt and this snapshot cannot be missed.
		if d.consumerMu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.closedCh:
			return ErrClosed
		case <-changed:
		}
	}
}

func (d *Dispatcher) releaseConsumer() {
	d.consumerMu.Unlock()
	d.mu.Lock()
	d.notifyConsumerLocked()
	d.mu.Unlock()
}

func (d *Dispatcher) releaseConsumerLocked() {
	d.consumerMu.Unlock()
	d.notifyConsumerLocked()
}

func (d *Dispatcher) notifyConsumerLocked() {
	close(d.consumerChanged)
	d.consumerChanged = make(chan struct{})
}

// Deliver accepts an already ordered and materialized typed private event.
// Envelope parsing and payload materialization remain owned by their semantic
// packages; this dispatcher never guesses a payload variant from wire bytes.
func (d *Dispatcher) Deliver(ctx context.Context, event model.Event) error {
	return d.Push(ctx, event)
}
