package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCoordinatedRegistryRejectsZeroCollisionClassConfusionAndReuse(t *testing.T) {
	registry := newNumericHandleRegistry()
	values := []uint64{0, 7, 7, 9, 7, 11}
	index := 0
	registry.next = func() (uint64, error) {
		value := values[index]
		index++
		return value, nil
	}
	coreHandle, err := registry.insert(handleClassCore, "core")
	if err != nil || coreHandle != coreHandleTag|7 {
		t.Fatalf("insert core handle=%d error=%v", coreHandle, err)
	}
	store := newBufferStoreForTest(registry)
	descriptor, err := store.allocate([]byte("buffer"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.get(coreHandle, handleClassBuffer); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("cross-class get error=%v", err)
	}
	if _, _, err := registry.retire(coreHandle, handleClassCore, false); err != nil {
		t.Fatal(err)
	}
	next, err := registry.insert(handleClassCore, "next")
	if err != nil || next != coreHandleTag|9 {
		t.Fatalf("retired reuse was not rejected: handle=%d error=%v", next, err)
	}
	if _, err := store.read(coreHandle, 0, nil); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("core accepted as buffer: %v", err)
	}
	if _, err := registry.get(descriptor.BufferHandle, handleClassCore); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("buffer accepted as core: %v", err)
	}
	if _, _, err := registry.retire(coreHandle, handleClassCore, true); err != nil {
		t.Fatalf("idempotent issued core retirement: %v", err)
	}
	if _, _, err := registry.retire(99, handleClassCore, true); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("unknown retirement error=%v", err)
	}
}

func TestCoordinatedRegistryEnforcesProcessIssuanceCeiling(t *testing.T) {
	registry := newNumericHandleRegistry()
	registry.issued = maxProcessHandleIssuance
	if _, err := registry.insert(handleClassCore, "core"); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("ceiling error=%v", err)
	}
}

func TestCoreReservationPublishesAtomicallyOrRollsBack(t *testing.T) {
	registry := newNumericHandleRegistry()
	registry.next = func() (uint64, error) { return 73, nil }
	handle, err := registry.reserveCore()
	if err != nil || handle != coreHandleTag|73 {
		t.Fatalf("reserve = %d, %v", handle, err)
	}
	if _, err := registry.borrowCore(handle); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("reservation became borrowable: %v", err)
	}
	if err := registry.abandonCoreReservation(handle); err != nil {
		t.Fatal(err)
	}
	if registry.issued != 0 || len(registry.live) != 0 || len(registry.retired) != 0 {
		t.Fatalf("abandon leaked state: issued=%d live=%d retired=%d", registry.issued, len(registry.live), len(registry.retired))
	}
}

func TestBufferStoreDescriptorReadAndFreeAreCopyBased(t *testing.T) {
	registry := newNumericHandleRegistry()
	store := newBufferStoreForTest(registry)
	descriptor, err := store.allocate([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.BufferHandle == 0 || descriptor.ByteLength != 5 {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	destination := make([]byte, 3)
	n, err := store.read(descriptor.BufferHandle, 1, destination)
	if err != nil || n != 3 || string(destination) != "alu" {
		t.Fatalf("read n=%d data=%q error=%v", n, destination, err)
	}
	destination[0] = 'X'
	verify := make([]byte, 5)
	if _, err := store.read(descriptor.BufferHandle, 0, verify); err != nil || string(verify) != "value" {
		t.Fatalf("host destination aliased wrapper bytes: %q error=%v", verify, err)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.read(descriptor.BufferHandle, 0, verify); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("read after free error=%v", err)
	}
	if err := store.free(descriptor.BufferHandle); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("double free error=%v", err)
	}
}

func TestBufferStoreRejectsOversizedAllocation(t *testing.T) {
	store := newBufferStoreForTest(newNumericHandleRegistry())
	if err := validateABIBufferSize(maxABIBufferBytes); err != nil {
		t.Fatalf("exact logical maximum: %v", err)
	}
	if err := validateABIBufferSize(maxABIBufferBytes + 1); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("oversized logical buffer error=%v", err)
	}
	if _, err := store.allocate(make([]byte, 4096)); err != nil {
		t.Fatalf("bounded real allocation error=%v", err)
	}
}

func TestCallbackClearWaitsForInflightAndStopsAdmission(t *testing.T) {
	state := newCallbackState()
	if !state.begin() {
		t.Fatal("first callback not admitted")
	}
	done := make(chan error, 1)
	go func() { done <- state.clear(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	if state.begin() {
		t.Fatal("callback admitted after clear")
	}
	select {
	case <-done:
		t.Fatal("clear returned before quiescence")
	default:
	}
	state.end()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCallbackClearTimeoutLeavesCleanupInProgress(t *testing.T) {
	state := newCallbackState()
	if !state.begin() {
		t.Fatal("callback not admitted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.clear(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("clear error=%v", err)
	}
	state.end()
	if err := state.clear(context.Background()); err != nil {
		t.Fatalf("eventual clear: %v", err)
	}
}

func TestCoordinatedRegistryConcurrentAccess(t *testing.T) {
	registry := newNumericHandleRegistry()
	var wait sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wait.Add(1)
		go func(value int) {
			defer wait.Done()
			handle, err := registry.insert(handleClassCore, value)
			if err != nil {
				t.Errorf("insert: %v", err)
				return
			}
			if _, err := registry.get(handle, handleClassCore); err != nil {
				t.Errorf("get: %v", err)
			}
			if _, _, err := registry.retire(handle, handleClassCore, false); err != nil {
				t.Errorf("retire: %v", err)
			}
		}(worker)
	}
	wait.Wait()
}
