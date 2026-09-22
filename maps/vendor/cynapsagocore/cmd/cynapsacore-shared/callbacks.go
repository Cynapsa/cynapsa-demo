package main

import (
	"context"
	"time"
	"unsafe"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

// CallbackSet is the internal Go representation of the frozen native callback
// registration. Callback is a host code address, never a Go pointer or handle.
type CallbackSet struct {
	Callback        unsafe.Pointer
	CompletionToken uint64
	EventToken      uint64
	LogToken        uint64
	Capacity        uint32
}

// RegisterCallbacks associates one host callback with a live Core. The C ABI
// is responsible for validating that Callback came from a function pointer.
func RegisterCallbacks(coreHandle uint64, values CallbackSet) error {
	record, err := processNumericHandles.borrowCore(coreHandle)
	if err != nil {
		return err
	}
	defer record.release()
	status, err := record.value().Status(context.Background())
	if err != nil {
		return err
	}
	if status.Lifecycle == v1.LifecycleClosing || status.Lifecycle == v1.LifecycleClosed {
		return errABIClosing
	}
	allowed, checkpoint := record.allowCallbacks()
	if !allowed {
		return errInvalidABIHandle
	}
	if checkpoint != nil {
		checkpoint()
	}
	manager, err := newCallbackManager(coreHandle, record.value(), values.Callback, values.CompletionToken, values.EventToken, values.LogToken, values.Capacity)
	if err != nil {
		return err
	}
	if err := record.installCallbacks(manager); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = manager.stop(ctx)
		cancel()
		return err
	}
	return nil
}

// ClearCallbacks prevents new callback dispatch and waits for every in-flight
// host callback. Same-Core teardown reentrancy is rejected by the C entrypoint.
func ClearCallbacks(coreHandle uint64, ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	record, err := processNumericHandles.borrowCore(coreHandle)
	if err != nil {
		return err
	}
	defer record.release()
	return record.stopCallbacks(ctx, false)
}
