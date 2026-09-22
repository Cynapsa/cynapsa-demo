package transport

import (
	"context"
	"sync"
	"sync/atomic"
)

const MaximumInboundTransportBytes int64 = 256 << 20

// InboundBudget bounds one production transport pipeline across all of its
// internal queue stages. Moving an InboundLease between stages transfers the
// same charge; it never charges the same owned bytes twice.
type InboundBudget struct {
	mu       sync.Mutex
	maximum  int64
	perItem  int64
	maxCount int
	bytes    int64
	count    int
	changed  chan struct{}
}

type InboundBudgetStats struct {
	Count, MaximumCount int
	Bytes, MaximumBytes int64
}

func NewInboundBudget(capacity, perItemMaximum int) (*InboundBudget, error) {
	if capacity <= 0 || capacity > MaximumReceiveQueue || perItemMaximum <= 0 || perItemMaximum > MaximumControlFrameBytes {
		return nil, ErrInvalidConfig
	}
	maximum := int64(capacity) * int64(perItemMaximum)
	if maximum > MaximumInboundTransportBytes {
		maximum = MaximumInboundTransportBytes
	}
	return &InboundBudget{maximum: maximum, perItem: int64(perItemMaximum), maxCount: capacity, changed: make(chan struct{})}, nil
}

func (budget *InboundBudget) Acquire(ctx context.Context, size int) (*InboundLease, error) {
	leases, err := budget.AcquireBatch(ctx, []int{size})
	if err != nil {
		return nil, err
	}
	return leases[0], nil
}

// AcquireBatch admits an already-owned batch as one atomic count-and-byte
// operation. It either returns one lease per input or leaves the budget
// unchanged; callers never need to expose a partially admitted batch.
func (budget *InboundBudget) AcquireBatch(ctx context.Context, sizes []int) ([]*InboundLease, error) {
	if budget == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	if len(sizes) > budget.maxCount {
		return nil, ErrProtocol
	}
	var requested int64
	for _, size := range sizes {
		if size < 0 {
			return nil, ErrInvalidConfig
		}
		item := int64(size)
		if item > budget.perItem || item > budget.maximum || requested > budget.maximum-item {
			return nil, ErrProtocol
		}
		requested += item
	}
	if len(sizes) == 0 {
		return []*InboundLease{}, nil
	}
	for {
		budget.mu.Lock()
		if len(sizes) <= budget.maxCount-budget.count && budget.bytes <= budget.maximum-requested {
			budget.count += len(sizes)
			budget.bytes += requested
			leases := make([]*InboundLease, len(sizes))
			for i, size := range sizes {
				leases[i] = &InboundLease{budget: budget, bytes: int64(size)}
			}
			budget.mu.Unlock()
			return leases, nil
		}
		changed := budget.changed
		budget.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (budget *InboundBudget) Stats() InboundBudgetStats {
	if budget == nil {
		return InboundBudgetStats{}
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return InboundBudgetStats{Count: budget.count, MaximumCount: budget.maxCount, Bytes: budget.bytes, MaximumBytes: budget.maximum}
}

func (budget *InboundBudget) MaximumBytes() int64 {
	if budget == nil {
		return 0
	}
	return budget.maximum
}

type InboundLease struct {
	budget   *InboundBudget
	bytes    int64
	released atomic.Bool
}

func (lease *InboundLease) Bytes() int64 {
	if lease == nil || lease.released.Load() {
		return 0
	}
	return lease.bytes
}

func (lease *InboundLease) Release() {
	if lease == nil || lease.budget == nil || !lease.released.CompareAndSwap(false, true) {
		return
	}
	budget := lease.budget
	budget.mu.Lock()
	budget.count--
	budget.bytes -= lease.bytes
	changed := budget.changed
	budget.changed = make(chan struct{})
	close(changed)
	budget.mu.Unlock()
}
