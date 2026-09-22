package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

const callbackLifecycleTestConfig = `{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":16,"payload_limit":1048576}`

func newCallbackManagerForLifecycleTest() *callbackManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &callbackManager{
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan callbackJob, 1),
		done:   make(chan struct{}),
		state:  newCallbackState(),
	}
}

func TestCallbackManagerStopBeforeStartIsPromptAndIdempotent(t *testing.T) {
	manager := newCallbackManagerForLifecycleTest()
	expired, expire := context.WithCancel(context.Background())
	expire()

	if err := manager.stop(expired); err != nil {
		t.Fatalf("stop before start: %v", err)
	}
	if manager.ctx.Err() == nil {
		t.Fatal("stop before start did not cancel manager context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := manager.stop(ctx); err != nil {
		t.Fatalf("repeated stop before start: %v", err)
	}

	// A rejected manager can never be revived after it has been disposed.
	manager.start()
	select {
	case <-manager.done:
	default:
		t.Fatal("start after stop reopened a disposed callback manager")
	}
}

func TestConcurrentCallbackInstallHasOneWinnerAndPromptlyDisposesLosers(t *testing.T) {
	const contenders = 64
	record := newABICoreRecord(&core.Core{})
	managers := make([]*callbackManager, contenders)
	for index := range managers {
		managers[index] = newCallbackManagerForLifecycleTest()
	}

	start := make(chan struct{})
	type installResult struct {
		installed bool
		err       error
	}
	results := make(chan installResult, contenders)
	var workers sync.WaitGroup
	for _, manager := range managers {
		workers.Add(1)
		go func(candidate *callbackManager) {
			defer workers.Done()
			<-start
			if err := record.installCallbacks(candidate); err != nil {
				if !errors.Is(err, errInvalidABIHandle) {
					results <- installResult{err: err}
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				stopErr := candidate.stop(ctx)
				cancel()
				results <- installResult{err: stopErr}
				return
			}
			results <- installResult{installed: true}
		}(manager)
	}
	close(start)
	workers.Wait()
	close(results)

	winners := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("install or loser disposal: %v", result.err)
		}
		if result.installed {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("successful callback registrations = %d, want 1", winners)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	if err := record.stopCallbacks(ctx, false); err != nil {
		cancel()
		t.Fatalf("stop winning callback manager: %v", err)
	}
	cancel()
	for index, manager := range managers {
		select {
		case <-manager.done:
		default:
			t.Fatalf("callback manager %d did not quiesce", index)
		}
	}
}

func TestRegisterCallbacksPublicPathReleasesLosingLeaseAndDoesNotDelayDestroy(t *testing.T) {
	handle, record := newStartedCallbackLifecycleCore(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRegistrations := func() { releaseOnce.Do(func() { close(release) }) }
	destroyed := false
	t.Cleanup(func() {
		releaseRegistrations()
		record.mu.Lock()
		record.callbackRegistrationCheckpoint = nil
		record.mu.Unlock()
		if !destroyed {
			_ = ClearCallbacks(handle, context.Background())
			drainer := startCallbackLifecycleDrainer(handle)
			_ = CoreShutdown(handle, 2_000)
			_ = drainer.stopAndWait()
			_ = CoreDestroy(handle)
		}
	})

	arrived := make(chan struct{}, 2)
	record.mu.Lock()
	record.callbackRegistrationCheckpoint = func() {
		arrived <- struct{}{}
		<-release
	}
	record.mu.Unlock()

	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- RegisterCallbacks(handle, callbackLifecycleTestSet())
		}()
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for range 2 {
		select {
		case <-arrived:
		case <-deadline.C:
			t.Fatal("two real callback registrations did not reach the post-allow checkpoint")
		}
	}
	releaseRegistrations()

	winners := 0
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				winners++
			case errors.Is(err, errInvalidABIHandle):
			default:
				t.Fatalf("RegisterCallbacks race error = %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("losing RegisterCallbacks call did not dispose promptly")
		}
	}
	if winners != 1 {
		t.Fatalf("successful callback registrations = %d, want 1", winners)
	}
	record.mu.Lock()
	record.callbackRegistrationCheckpoint = nil
	active, manager := record.active, record.callbacks
	record.mu.Unlock()
	if active != 0 {
		t.Fatalf("callback registration retained %d Core leases", active)
	}
	if manager == nil {
		t.Fatal("winning callback registration was not installed")
	}

	drainer := startCallbackLifecycleDrainer(handle)
	if err := CoreShutdown(handle, 2_000); err != nil {
		t.Fatalf("CoreShutdown: %v", err)
	}
	if err := drainer.stopAndWait(); err != nil {
		t.Fatalf("drain Core shutdown: %v", err)
	}
	destroyDone := make(chan error, 1)
	go func() { destroyDone <- CoreDestroy(handle) }()
	select {
	case err := <-destroyDone:
		if err != nil {
			t.Fatalf("CoreDestroy: %v", err)
		}
		destroyed = true
	case <-time.After(time.Second):
		t.Fatal("losing callback registration lease delayed Core destruction")
	}
	assertCallbackLifecycleRecordDestroyed(t, record)
}

func TestRegisterClearShutdownDestroyRaceTerminatesWithoutResidualCallbacks(t *testing.T) {
	handle, record := newStartedCallbackLifecycleCore(t)
	destroyed := false
	t.Cleanup(func() {
		if !destroyed {
			_ = ClearCallbacks(handle, context.Background())
			drainer := startCallbackLifecycleDrainer(handle)
			_ = CoreShutdown(handle, 2_000)
			_ = drainer.stopAndWait()
			_ = CoreDestroy(handle)
		}
	})
	drainer := startCallbackLifecycleDrainer(handle)
	defer func() { _ = drainer.stopAndWait() }()

	const operationsPerKind = 4
	start := make(chan struct{})
	results := make(chan error, operationsPerKind*4)
	var operations sync.WaitGroup
	launch := func(operation func() error) {
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			results <- operation()
		}()
	}
	for range operationsPerKind {
		launch(func() error { return RegisterCallbacks(handle, callbackLifecycleTestSet()) })
		launch(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return ClearCallbacks(handle, ctx)
		})
		launch(func() error { return CoreShutdown(handle, 2_000) })
		launch(func() error { return CoreDestroy(handle) })
	}
	close(start)
	done := make(chan struct{})
	go func() {
		operations.Wait()
		close(results)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent callback lifecycle operations did not terminate")
	}
	for err := range results {
		if !allowedCallbackLifecycleRaceError(err) {
			t.Fatalf("callback lifecycle race error = %v", err)
		}
	}

	finalized := make(chan error, 1)
	go func() {
		if err := CoreShutdown(handle, 2_000); err != nil && !errors.Is(err, errInvalidABIHandle) {
			finalized <- err
			return
		}
		finalized <- CoreDestroy(handle)
	}()
	select {
	case err := <-finalized:
		if err != nil {
			t.Fatalf("final callback lifecycle cleanup: %v", err)
		}
		destroyed = true
	case <-time.After(5 * time.Second):
		t.Fatal("final callback lifecycle cleanup did not terminate")
	}
	if err := drainer.stopAndWait(); err != nil {
		t.Fatalf("drain concurrent callback lifecycle shutdown: %v", err)
	}
	assertCallbackLifecycleRecordDestroyed(t, record)
}

func newStartedCallbackLifecycleCore(t *testing.T) (uint64, *abiCoreRecord) {
	t.Helper()
	handle, _, err := CoreCreate([]byte(callbackLifecycleTestConfig))
	if err != nil {
		t.Fatalf("CoreCreate: %v", err)
	}
	if err := CoreStart(handle); err != nil {
		_ = CoreDestroy(handle)
		t.Fatalf("CoreStart: %v", err)
	}
	record, err := processNumericHandles.borrowCore(handle)
	if err != nil {
		t.Fatalf("borrow callback lifecycle Core: %v", err)
	}
	record.release()
	return handle, record
}

func callbackLifecycleTestSet() CallbackSet {
	return CallbackSet{Callback: unsafe.Pointer(new(byte)), CompletionToken: 1, Capacity: 1}
}

func allowedCallbackLifecycleRaceError(err error) bool {
	if err == nil || errors.Is(err, errInvalidABIHandle) || errors.Is(err, errABIClosing) {
		return true
	}
	var public *v1.Error
	return errors.As(err, &public) && (public.Code == v1.ErrorCodeInvalidHandle || public.Code == v1.ErrorCodeShutdownInProgress || public.Code == v1.ErrorCodeShutdownTimeout)
}

func assertCallbackLifecycleRecordDestroyed(t *testing.T, record *abiCoreRecord) {
	t.Helper()
	select {
	case <-record.destroyDone:
	default:
		t.Fatal("Core destruction did not close destroyDone")
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active != 0 || record.callbacks != nil || record.core != nil {
		t.Fatalf("destroyed callback record retained state: active=%d callbacks=%v core=%v", record.active, record.callbacks != nil, record.core != nil)
	}
}

type callbackLifecycleDrainer struct {
	stop     chan struct{}
	done     chan error
	stopOnce sync.Once
	waitOnce sync.Once
	err      error
}

func startCallbackLifecycleDrainer(handle uint64) *callbackLifecycleDrainer {
	drainer := &callbackLifecycleDrainer{stop: make(chan struct{}), done: make(chan error, 1)}
	go func() {
		for {
			select {
			case <-drainer.stop:
				drainer.done <- nil
				return
			default:
			}
			for _, poll := range []func(uint64, int64) (BufferDescriptor, error){CoreNextCompletion, CoreNextEvent} {
				descriptor, err := poll(handle, 10)
				if err == nil {
					if descriptor.BufferHandle != 0 {
						if freeErr := FreeBuffer(descriptor.BufferHandle); freeErr != nil {
							drainer.done <- freeErr
							return
						}
					}
					continue
				}
				if errors.Is(err, errInvalidABIHandle) {
					drainer.done <- nil
					return
				}
				var public *v1.Error
				if !errors.As(err, &public) || public.Code != v1.ErrorCodeDeliveryTimeout && public.Code != v1.ErrorCodeShutdownInProgress {
					drainer.done <- fmt.Errorf("callback lifecycle drain: %w", err)
					return
				}
			}
		}
	}()
	return drainer
}

func (d *callbackLifecycleDrainer) stopAndWait() error {
	d.stopOnce.Do(func() { close(d.stop) })
	d.waitOnce.Do(func() { d.err = <-d.done })
	return d.err
}
