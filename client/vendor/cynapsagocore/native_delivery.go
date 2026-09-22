package cynapsagocore

import (
	"context"
	"errors"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

var errNativeDeliveryLeaseFinalized = errors.New("native delivery lease finalized")

// NativeDeliveryLease is the private transactional seam between the Go facade
// and the native boundary. It is exported only because the shared-library main
// package is a separate Go package; it is not part of the C ABI or SDK model.
// Exactly one Commit or Rollback call releases the Core operation ownership.
type NativeDeliveryLease struct {
	mu       sync.Mutex
	ctx      context.Context
	commit   func() error
	rollback func() error
	release  func()
	mapError func(error) error
	done     bool
}

// Commit transfers the exact reserved completion or event to the native
// descriptor owner. Cancellation before this linearization restores the head.
func (lease *NativeDeliveryLease) Commit() (err error) {
	if lease == nil {
		return errNativeDeliveryLeaseFinalized
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.done {
		return errNativeDeliveryLeaseFinalized
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			// A commit panic has no trustworthy transfer point. Best-effort
			// rollback preserves the authoritative head when the underlying
			// owner has not already committed, while terminalization always
			// releases the Core operation. Preserve the original invariant
			// panic for the outer native recovery boundary.
			rollbackNativeDeliveryNoPanic(lease.rollback)
			finishNativeDeliveryNoPanic(lease)
			panic(recovered)
		}
	}()
	if err = lease.ctx.Err(); err != nil {
		_ = lease.rollback()
		err = lease.normalizeLocked(err)
		lease.finishLocked()
		return err
	}
	err = lease.commit()
	if err != nil && !errors.Is(err, delivery.ErrLeaseRetired) {
		err = lease.normalizeLocked(err)
	}
	lease.finishLocked()
	return err
}

func (lease *NativeDeliveryLease) normalizeLocked(err error) error {
	if err == nil || lease.mapError == nil {
		return err
	}
	return lease.mapError(err)
}

// NormalizeError preserves the Core's delivery-wait taxonomy for a native
// capacity wait that ends after the authoritative lease was rolled back.
func (lease *NativeDeliveryLease) NormalizeError(err error) error {
	if lease == nil || err == nil {
		return err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.normalizeLocked(err)
}

// Rollback restores the exact reserved queue head and releases the Core
// operation without transferring delivery ownership.
func (lease *NativeDeliveryLease) Rollback() (err error) {
	if lease == nil {
		return errNativeDeliveryLeaseFinalized
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.done {
		return errNativeDeliveryLeaseFinalized
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			finishNativeDeliveryNoPanic(lease)
			panic(recovered)
		}
	}()
	err = lease.rollback()
	lease.finishLocked()
	return err
}

func (lease *NativeDeliveryLease) finishLocked() {
	if lease.done {
		return
	}
	release := lease.release
	lease.done = true
	lease.commit = nil
	lease.rollback = nil
	lease.release = nil
	if release != nil {
		release()
	}
}

func rollbackNativeDeliveryNoPanic(rollback func() error) {
	if rollback == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = rollback()
}

func finishNativeDeliveryNoPanic(lease *NativeDeliveryLease) {
	defer func() { _ = recover() }()
	lease.finishLocked()
}

// ReserveNativeCompletionABI reserves and encodes one completion without
// consuming its authoritative Gate ownership.
func (c *Core) ReserveNativeCompletionABI(ctx context.Context) ([]byte, *NativeDeliveryLease, error) {
	if err := c.beginOperation(sdkboundary.FailureDeliveryWait, true); err != nil {
		return nil, nil, err
	}
	if ctx == nil {
		c.end()
		return nil, nil, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureDeliveryWait)
	}
	result, internal, err := c.gate.ReserveCompletion(ctx)
	if err != nil {
		c.end()
		return nil, nil, c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait)
	}
	rollback := func() error { return c.gate.RollbackCompletion(internal) }
	completion, err := c.boundary.MapCompletion(result)
	if err == nil {
		var encoded []byte
		encoded, err = c.boundary.EncodeABICompletion(completion)
		if err == nil {
			return encoded, &NativeDeliveryLease{
				ctx: ctx, commit: func() error { return c.gate.CommitCompletion(internal) },
				rollback: rollback, release: c.end,
				mapError: func(err error) error { return c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait) },
			}, nil
		}
	}
	_ = rollback()
	c.end()
	return nil, nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
}

// ReserveNativeEventABI reserves and encodes one event without consuming its
// authoritative Dispatcher ownership. Callback consumers set
// waitForAcceptance so a second tracked message waits for the exact prior
// delivery.accept rather than terminating the event pump.
func (c *Core) ReserveNativeEventABI(ctx context.Context, waitForAcceptance bool) ([]byte, string, *NativeDeliveryLease, error) {
	if err := c.beginOperation(sdkboundary.FailureDeliveryWait, true); err != nil {
		return nil, "", nil, err
	}
	if ctx == nil {
		c.end()
		return nil, "", nil, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureDeliveryWait)
	}
	event, internal, err := c.runtime.ReserveEvent(ctx, waitForAcceptance)
	if err != nil {
		c.end()
		return nil, "", nil, c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait)
	}
	rollback := func() error { return c.runtime.RollbackEvent(internal) }
	public, err := c.boundary.MapEvent(event)
	if err == nil {
		var encoded []byte
		encoded, err = c.boundary.EncodeABIEvent(public)
		if err == nil {
			return encoded, string(public.Name), &NativeDeliveryLease{
				ctx: ctx, commit: func() error { return c.runtime.CommitEvent(internal) },
				rollback: rollback, release: c.end,
				mapError: func(err error) error { return c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait) },
			}, nil
		}
	}
	_ = rollback()
	c.end()
	return nil, "", nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
}

// NativeDeliveryLeaseRetired reports the one retryable race where tracked
// producer cancellation retired an event while the native boundary held it.
func NativeDeliveryLeaseRetired(err error) bool { return errors.Is(err, delivery.ErrLeaseRetired) }
