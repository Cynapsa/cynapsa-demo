package main

import (
	"context"
	"math"
	"time"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

const defaultNativeTimeout = 30 * time.Second

var processBuffers = newBufferStore()

func withClearedOwnedInput[T any](owned []byte, operation func([]byte) T) T {
	defer clear(owned)
	return operation(owned)
}

func withClearedInputCopy[T any](input []byte, operation func([]byte) T) T {
	return withClearedOwnedInput(append([]byte(nil), input...), operation)
}

// BufferDescriptor is the pointer-free result of an operation that produced
// immutable wrapper-owned bytes.
type BufferDescriptor struct {
	BufferHandle uint64
	ByteLength   uint64
}

// ABIVersion returns the native contract version understood by this library.
func ABIVersion() uint32 { return core.ABIVersion() }

// CoreCreate strictly decodes public configuration and publishes a random
// numeric handle only after complete local construction succeeds.
func CoreCreate(input []byte) (uint64, BufferDescriptor, error) {
	handle, err := processNumericHandles.reserveCore()
	if err != nil {
		return 0, BufferDescriptor{}, err
	}
	created := withClearedInputCopy(input, func(owned []byte) struct {
		value *core.Core
		err   error
	} {
		value, createErr := core.NewFromABI(owned)
		return struct {
			value *core.Core
			err   error
		}{value: value, err: createErr}
	})
	value, err := created.value, created.err
	if err != nil {
		_ = processNumericHandles.abandonCoreReservation(handle)
		return 0, BufferDescriptor{}, err
	}
	record := newABICoreRecord(value)
	if err := processNumericHandles.publishCore(handle, record); err != nil {
		_ = processNumericHandles.abandonCoreReservation(handle)
		return 0, BufferDescriptor{}, err
	}
	return handle, BufferDescriptor{}, nil
}

func borrowCore(handle uint64) (*abiCoreRecord, *core.Core, error) {
	record, err := processNumericHandles.borrowCore(handle)
	if err != nil {
		return nil, nil, err
	}
	value := record.value()
	if value == nil {
		record.release()
		return nil, nil, errInvalidABIHandle
	}
	return record, value, nil
}

func CoreStart(handle uint64) error {
	record, value, err := borrowCore(handle)
	if err != nil {
		return err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	return value.Start(ctx)
}

func CoreSubmit(handle uint64, input []byte) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	result := withClearedInputCopy(input, func(owned []byte) struct {
		encoded []byte
		err     error
	} {
		encoded, submitErr := value.SubmitABI(ctx, owned)
		return struct {
			encoded []byte
			err     error
		}{encoded: encoded, err: submitErr}
	})
	encoded, err := result.encoded, result.err
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func CoreCancel(handle uint64, input []byte) error {
	record, value, err := borrowCore(handle)
	if err != nil {
		return err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	return withClearedInputCopy(input, func(owned []byte) error { return value.CancelABI(ctx, owned) })
}

func CoreNextCompletion(handle uint64, timeoutMillis int64) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel, err := nativeWaitContext(value.DefaultTimeout(), timeoutMillis)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer cancel()
	return reserveNativeDeliveryBuffer(ctx, func() (callbackReservation, error) {
		encoded, lease, reserveErr := value.ReserveNativeCompletionABI(ctx)
		if reserveErr != nil {
			return callbackReservation{}, reserveErr
		}
		return callbackReservation{encoded: encoded, commit: lease.Commit, rollback: lease.Rollback, normalize: lease.NormalizeError}, nil
	})
}

func CoreNextEvent(handle uint64, timeoutMillis int64) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel, err := nativeWaitContext(value.DefaultTimeout(), timeoutMillis)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer cancel()
	return reserveNativeDeliveryBuffer(ctx, func() (callbackReservation, error) {
		encoded, _, lease, reserveErr := value.ReserveNativeEventABI(ctx, false)
		if reserveErr != nil {
			return callbackReservation{}, reserveErr
		}
		return callbackReservation{encoded: encoded, commit: lease.Commit, rollback: lease.Rollback, normalize: lease.NormalizeError}, nil
	})
}

func reserveNativeDeliveryBuffer(ctx context.Context, reserve func() (callbackReservation, error)) (BufferDescriptor, error) {
	return reserveNativeDeliveryBufferFrom(ctx, processBuffers, reserve)
}

func reserveNativeDeliveryBufferFrom(ctx context.Context, store *bufferStore, reserve func() (callbackReservation, error)) (BufferDescriptor, error) {
	for {
		reservation, err := reserve()
		if err != nil {
			return BufferDescriptor{}, err
		}
		descriptor, changed, allocationErr := safeAllocateOrWait(store, reservation.encoded)
		clear(reservation.encoded)
		if allocationErr != nil {
			_ = reservation.rollback()
			if changed == nil {
				return BufferDescriptor{}, allocationErr
			}
			select {
			case <-ctx.Done():
				if reservation.normalize != nil {
					return BufferDescriptor{}, reservation.normalize(ctx.Err())
				}
				return BufferDescriptor{}, ctx.Err()
			case <-changed:
				continue
			}
		}
		if err = commitPreparedReservation(reservation, func() {
			_ = store.free(descriptor.BufferHandle)
		}); err == nil {
			return descriptor, nil
		}
		_ = store.free(descriptor.BufferHandle)
		if core.NativeDeliveryLeaseRetired(err) {
			continue
		}
		return BufferDescriptor{}, err
	}
}

func commitPreparedReservation(reservation callbackReservation, cleanup func()) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			cleanup()
			rollbackReservationNoPanic(reservation.rollback)
			panic(recovered)
		}
	}()
	return reservation.commit()
}

func CoreStatus(handle uint64) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	encoded, err := value.StatusABI(ctx)
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func CoreShutdown(handle uint64, timeoutMillis int64) error {
	if inNativeCallback(handle) {
		return errCallbackReentrant
	}
	record, value, err := borrowCore(handle)
	if err != nil {
		return err
	}
	defer record.release()
	ctx, cancel, err := nativeWaitContext(value.DefaultTimeout(), timeoutMillis)
	if err != nil {
		return err
	}
	defer cancel()
	// Start Core-owned cleanup before callback quiescence so a slow host
	// callback cannot keep command admission open. Shutdown below joins it.
	if err := value.BeginShutdownABI(); err != nil {
		return err
	}
	// Keep an installed callback consumer alive while the Core drains its
	// bounded completion/event queues. Once cleanup reaches closed, prevent
	// any new dispatch and join the final in-flight callback.
	if err := value.Shutdown(ctx); err != nil {
		return err
	}
	return record.stopCallbacks(ctx, true)
}

func CoreDestroy(handle uint64) error {
	if inNativeCallback(handle) {
		return errCallbackReentrant
	}
	// Destruction is legal only after the runtime is closed. Borrowing first
	// prevents a concurrent destroy while this state check is in flight.
	record, value, err := borrowCore(handle)
	if err != nil {
		// A concurrent or repeated destruction joins the same record-owned
		// release before reporting idempotent success.
		retired, _, retireErr := processNumericHandles.retireCore(handle)
		if retireErr != nil {
			return retireErr
		}
		if retired == nil {
			return nil
		}
		destroyErr := retired.destroy()
		processNumericHandles.completeCoreRetirement(handle, retired)
		return destroyErr
	}
	status, statusErr := value.Status(context.Background())
	record.release()
	if statusErr != nil {
		return statusErr
	}
	if status.Lifecycle != v1.LifecycleClosed {
		return abiPublicError(v1.ErrorCodeShutdownInProgress)
	}
	record, _, err = processNumericHandles.retireCore(handle)
	if err != nil {
		return err
	}
	if record == nil {
		// A concurrent destroy completed after this caller released its status
		// lease. The retired tombstone makes repeated destruction idempotent.
		return nil
	}
	destroyErr := record.destroy()
	processNumericHandles.completeCoreRetirement(handle, record)
	return destroyErr
}

func PayloadOpen(handle uint64) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	encoded, err := value.PayloadOpenABI(ctx)
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func PayloadWrite(handle uint64, input []byte) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	result := withClearedInputCopy(input, func(owned []byte) struct {
		encoded []byte
		err     error
	} {
		encoded, callErr := value.PayloadWriteABI(ctx, owned)
		return struct {
			encoded []byte
			err     error
		}{encoded: encoded, err: callErr}
	})
	encoded, err := result.encoded, result.err
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func PayloadFinish(handle uint64, input []byte) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	result := withClearedInputCopy(input, func(owned []byte) struct {
		encoded []byte
		err     error
	} {
		encoded, callErr := value.PayloadFinishABI(ctx, owned)
		return struct {
			encoded []byte
			err     error
		}{encoded: encoded, err: callErr}
	})
	encoded, err := result.encoded, result.err
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func PayloadRead(handle uint64, input []byte) (BufferDescriptor, error) {
	record, value, err := borrowCore(handle)
	if err != nil {
		return BufferDescriptor{}, err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	result := withClearedInputCopy(input, func(owned []byte) struct {
		encoded []byte
		err     error
	} {
		encoded, callErr := value.PayloadReadABI(ctx, owned)
		return struct {
			encoded []byte
			err     error
		}{encoded: encoded, err: callErr}
	})
	encoded, err := result.encoded, result.err
	if err != nil {
		return BufferDescriptor{}, err
	}
	return processBuffers.allocate(encoded)
}

func PayloadCancel(handle uint64, input []byte) error {
	return payloadMutation(handle, input, (*core.Core).PayloadCancelABI)
}

func PayloadRetain(handle uint64, input []byte) error {
	return payloadMutation(handle, input, (*core.Core).PayloadRetainABI)
}

func PayloadRelease(handle uint64, input []byte) error {
	return payloadMutation(handle, input, (*core.Core).PayloadReleaseABI)
}

func payloadMutation(handle uint64, input []byte, operation func(*core.Core, context.Context, []byte) error) error {
	record, value, err := borrowCore(handle)
	if err != nil {
		return err
	}
	defer record.release()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutOrDefault(value.DefaultTimeout()))
	defer cancel()
	return withClearedInputCopy(input, func(owned []byte) error { return operation(value, ctx, owned) })
}

func FreeBuffer(handle uint64) error { return processBuffers.free(handle) }

func BufferRead(handle, offset uint64, destination []byte) (uint64, error) {
	return processBuffers.read(handle, offset, destination)
}

func nativeWaitContext(configured time.Duration, timeoutMillis int64) (context.Context, context.CancelFunc, error) {
	if timeoutMillis < 0 || timeoutMillis > math.MaxInt64/int64(time.Millisecond) {
		return nil, nil, errInvalidABIInput
	}
	timeout := time.Duration(timeoutMillis) * time.Millisecond
	if timeout == 0 {
		timeout = timeoutOrDefault(configured)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return ctx, cancel, nil
}

func timeoutOrDefault(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultNativeTimeout
	}
	return value
}
