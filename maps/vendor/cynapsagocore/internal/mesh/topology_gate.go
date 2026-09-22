package mesh

import (
	"context"
	"sync"
)

// topologyGate is a fair, context-cancellable reader/writer gate. Waiters use
// one-shot close notifications under the gate mutex, so release and
// cancellation cannot lose a wakeup. Once a writer queues, later readers wait
// behind it; readers already admitted retain their final-handoff ownership.
type topologyGate struct {
	mu      sync.Mutex
	readers int
	writer  bool
	waiters []*topologyGateWaiter
}

type topologyGateWaiter struct {
	writer  bool
	ready   chan struct{}
	granted bool
}

func (gate *topologyGate) readLock() {
	_ = gate.acquire(context.Background(), false)
}

func (gate *topologyGate) readLockContext(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return gate.acquire(ctx, false)
}

func (gate *topologyGate) readUnlock() {
	gate.mu.Lock()
	if gate.readers <= 0 || gate.writer {
		gate.mu.Unlock()
		panic("mesh: topology read admission released without ownership")
	}
	gate.readers--
	gate.advanceLocked()
	gate.mu.Unlock()
}

func (gate *topologyGate) lock(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return gate.acquire(ctx, true)
}

func (gate *topologyGate) unlock() {
	gate.mu.Lock()
	if !gate.writer || gate.readers != 0 {
		gate.mu.Unlock()
		panic("mesh: topology writer released without ownership")
	}
	gate.writer = false
	gate.advanceLocked()
	gate.mu.Unlock()
}

func (gate *topologyGate) acquire(ctx context.Context, writer bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	waiter := &topologyGateWaiter{writer: writer, ready: make(chan struct{})}
	gate.mu.Lock()
	if !gate.writer && len(gate.waiters) == 0 && (writer && gate.readers == 0 || !writer) {
		if writer {
			gate.writer = true
		} else {
			gate.readers++
		}
		gate.mu.Unlock()
		return nil
	}
	gate.waiters = append(gate.waiters, waiter)
	gate.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		gate.mu.Lock()
		if waiter.granted {
			gate.mu.Unlock()
			return nil
		}
		gate.removeWaiterLocked(waiter)
		gate.advanceLocked()
		gate.mu.Unlock()
		return ctx.Err()
	}
}

func (gate *topologyGate) removeWaiterLocked(target *topologyGateWaiter) {
	for index, waiter := range gate.waiters {
		if waiter != target {
			continue
		}
		copy(gate.waiters[index:], gate.waiters[index+1:])
		gate.waiters[len(gate.waiters)-1] = nil
		gate.waiters = gate.waiters[:len(gate.waiters)-1]
		return
	}
}

func (gate *topologyGate) advanceLocked() {
	if gate.writer || len(gate.waiters) == 0 {
		return
	}
	if gate.readers == 0 && gate.waiters[0].writer {
		waiter := gate.popWaiterLocked()
		gate.writer = true
		waiter.granted = true
		close(waiter.ready)
		return
	}
	if gate.waiters[0].writer {
		return
	}
	for len(gate.waiters) > 0 && !gate.waiters[0].writer {
		waiter := gate.popWaiterLocked()
		gate.readers++
		waiter.granted = true
		close(waiter.ready)
	}
}

func (gate *topologyGate) popWaiterLocked() *topologyGateWaiter {
	waiter := gate.waiters[0]
	copy(gate.waiters, gate.waiters[1:])
	gate.waiters[len(gate.waiters)-1] = nil
	gate.waiters = gate.waiters[:len(gate.waiters)-1]
	return waiter
}
