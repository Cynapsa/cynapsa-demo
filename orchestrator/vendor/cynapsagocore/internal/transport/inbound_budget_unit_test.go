package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInboundBudgetAtomicallyBoundsCountAndBytes(t *testing.T) {
	budget, err := NewInboundBudget(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	first, err := budget.Acquire(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := budget.Acquire(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err = budget.Acquire(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated acquire = %v", err)
	}
	first.Release()
	third, err := budget.Acquire(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	third.Release()
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 || stats.MaximumCount != 2 || stats.MaximumBytes != 20 {
		t.Fatalf("final stats = %+v", stats)
	}
}

func TestInboundBudgetRejectsOversizeAndCapsDerivedMaximum(t *testing.T) {
	budget, err := NewInboundBudget(MaximumReceiveQueue, MaximumControlFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if budget.MaximumBytes() != MaximumInboundTransportBytes {
		t.Fatalf("maximum = %d", budget.MaximumBytes())
	}
	if _, err = budget.Acquire(t.Context(), MaximumControlFrameBytes+1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversize acquire = %v", err)
	}
}

func TestInboundBudgetBatchAdmissionIsAtomic(t *testing.T) {
	budget, err := NewInboundBudget(3, 10)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := budget.AcquireBatch(t.Context(), []int{4, 6})
	if err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.Count != 2 || stats.Bytes != 10 {
		t.Fatalf("admitted stats = %+v", stats)
	}
	for _, lease := range leases {
		lease.Release()
	}
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("released stats = %+v", stats)
	}

	if _, err = budget.AcquireBatch(t.Context(), []int{1, 1, 1, 1}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("impossible count batch = %v", err)
	}
	if _, err = budget.AcquireBatch(t.Context(), []int{10, 10, 10, 1}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("impossible byte batch = %v", err)
	}
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("failed batch partially admitted = %+v", stats)
	}
}

func TestInboundBudgetBatchWaitIsCancellableWithoutPartialAdmission(t *testing.T) {
	budget, err := NewInboundBudget(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	occupied, err := budget.Acquire(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err = budget.AcquireBatch(ctx, []int{1, 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated batch = %v", err)
	}
	if stats := budget.Stats(); stats.Count != 1 || stats.Bytes != 1 {
		t.Fatalf("cancelled batch partially admitted = %+v", stats)
	}
	occupied.Release()
}
