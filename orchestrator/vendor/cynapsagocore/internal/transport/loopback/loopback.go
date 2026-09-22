// Package loopback provides deterministic in-memory delivery for tests.
package loopback

import (
	"context"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

// Fault is consumed by exactly one Send call in FIFO order.
type Fault uint8

const (
	FaultNone Fault = iota
	FaultDrop
	FaultDuplicate
	FaultHold
	FaultReject
	FaultAmbiguous
	FaultBlock
)

// Controller exists only when explicitly requested by a test constructor.
// Ordinary NewPair callers cannot inject faults accidentally.
type Controller struct{ state *shared }

type shared struct {
	mu       sync.Mutex
	faults   []Fault
	held     []protocol.Envelope
	closed   bool
	progress time.Time
}

// Transport is one endpoint of a bounded in-memory pair.
type Transport struct {
	kind   transport.Kind
	in     chan protocol.Envelope
	out    chan protocol.Envelope
	state  *shared
	peer   *shared
	mu     sync.Mutex
	ready  bool
	closed bool
	done   chan struct{}
	once   sync.Once
}

// New creates a safe self-loop adapter for focused manager tests.
func New(capacity int) (*Transport, error) {
	left, _, err := NewPair(capacity, transport.KindLive)
	if err != nil {
		return nil, err
	}
	left.out = left.in
	left.peer = left.state
	return left, nil
}

// NewPair creates two bounded endpoints with fault injection disabled.
func NewPair(capacity int, kind transport.Kind) (*Transport, *Transport, error) {
	left, right, _, _, err := newPair(capacity, kind, false)
	return left, right, err
}

// NewFaultablePair makes fault injection an explicit test-only choice.
func NewFaultablePair(capacity int, kind transport.Kind) (*Transport, *Transport, *Controller, *Controller, error) {
	return newPair(capacity, kind, true)
}

func newPair(capacity int, kind transport.Kind, controlled bool) (*Transport, *Transport, *Controller, *Controller, error) {
	if capacity <= 0 || capacity > transport.MaximumReceiveQueue || (kind != transport.KindLive && kind != transport.KindDurable) {
		return nil, nil, nil, nil, transport.ErrInvalidConfig
	}
	leftQueue := make(chan protocol.Envelope, capacity)
	rightQueue := make(chan protocol.Envelope, capacity)
	leftState, rightState := &shared{}, &shared{}
	left := &Transport{kind: kind, in: leftQueue, out: rightQueue, state: leftState, peer: rightState, done: make(chan struct{})}
	right := &Transport{kind: kind, in: rightQueue, out: leftQueue, state: rightState, peer: leftState, done: make(chan struct{})}
	if !controlled {
		return left, right, nil, nil, nil
	}
	return left, right, &Controller{leftState}, &Controller{rightState}, nil
}

func (t *Transport) Kind() transport.Kind { return t.kind }

func (t *Transport) Start(ctx context.Context) error {
	if t == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return transport.ErrClosed
	}
	t.ready = true
	return nil
}

func (t *Transport) Send(ctx context.Context, envelope protocol.Envelope) error {
	if t == nil || ctx == nil || protocol.ValidateEnvelope(envelope) != nil {
		return transport.ErrInvalidEnvelope
	}
	t.mu.Lock()
	ready, closed := t.ready, t.closed
	t.mu.Unlock()
	if closed {
		return transport.ErrClosed
	}
	if !ready {
		return transport.ErrNotStarted
	}
	fault := t.nextFault()
	if fault == FaultReject {
		return transport.ErrSendRejected
	}
	if fault == FaultBlock {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return transport.ErrClosed
		}
	}
	if fault == FaultDrop {
		t.markProgress()
		return nil
	}
	if fault == FaultHold {
		t.state.mu.Lock()
		t.state.held = append(t.state.held, envelope.Clone())
		t.state.mu.Unlock()
		return nil
	}
	if err := t.enqueue(ctx, envelope); err != nil {
		return err
	}
	if fault == FaultDuplicate {
		if err := t.enqueue(ctx, envelope); err != nil {
			return err
		}
	}
	if fault == FaultAmbiguous {
		return transport.ErrSendAmbiguous
	}
	return nil
}

func (t *Transport) enqueue(ctx context.Context, envelope protocol.Envelope) error {
	t.peer.mu.Lock()
	peerClosed := t.peer.closed
	t.peer.mu.Unlock()
	if peerClosed {
		return transport.ErrClosed
	}
	copy := envelope.Clone()
	select {
	case t.out <- copy:
		t.markProgress()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return transport.ErrClosed
	default:
		return transport.ErrQueueFull
	}
}

func (t *Transport) Receive(ctx context.Context) (protocol.Envelope, error) {
	if t == nil || ctx == nil {
		return protocol.Envelope{}, transport.ErrInvalidConfig
	}
	select {
	case envelope := <-t.in:
		t.markProgress()
		return envelope.Clone(), nil
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	case <-t.done:
		return protocol.Envelope{}, transport.ErrClosed
	}
}

func (t *Transport) Observe() transport.Observation {
	if t == nil {
		return transport.Observation{State: transport.HealthClosed}
	}
	t.mu.Lock()
	ready, closed := t.ready, t.closed
	t.mu.Unlock()
	t.state.mu.Lock()
	progress := t.state.progress
	t.state.mu.Unlock()
	switch {
	case closed:
		return transport.Observation{State: transport.HealthClosed, LastProgress: progress}
	case ready:
		return transport.Observation{State: transport.HealthHealthy, LastProgress: progress}
	default:
		return transport.Observation{State: transport.HealthConnecting, LastProgress: progress}
	}
}

func (t *Transport) Close(ctx context.Context) error {
	if t == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.ready = false
	t.mu.Unlock()
	t.once.Do(func() { close(t.done) })
	t.state.mu.Lock()
	t.state.closed = true
	t.state.held = nil
	t.state.faults = nil
	t.state.mu.Unlock()
	return nil
}

func (t *Transport) nextFault() Fault {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	if len(t.state.faults) == 0 {
		return FaultNone
	}
	fault := t.state.faults[0]
	t.state.faults = t.state.faults[1:]
	return fault
}

func (t *Transport) markProgress() {
	now := time.Now().UTC()
	t.state.mu.Lock()
	t.state.progress = now
	t.state.mu.Unlock()
	t.peer.mu.Lock()
	t.peer.progress = now
	t.peer.mu.Unlock()
}

func (c *Controller) Set(faults ...Fault) error {
	if c == nil || c.state == nil {
		return transport.ErrInvalidConfig
	}
	for _, fault := range faults {
		if fault > FaultBlock {
			return transport.ErrInvalidConfig
		}
	}
	c.state.mu.Lock()
	c.state.faults = append([]Fault(nil), faults...)
	c.state.mu.Unlock()
	return nil
}

// ReleaseHeld emits held messages in their original order after later sends,
// producing deterministic reordering without timers.
func (c *Controller) ReleaseHeld(ctx context.Context, endpoint *Transport) error {
	if c == nil || c.state == nil || endpoint == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	c.state.mu.Lock()
	held := c.state.held
	c.state.held = nil
	c.state.mu.Unlock()
	for _, envelope := range held {
		if err := endpoint.enqueue(ctx, envelope); err != nil {
			return err
		}
	}
	return nil
}
