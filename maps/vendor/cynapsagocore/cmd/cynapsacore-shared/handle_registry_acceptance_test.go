package main

import (
	"bytes"
	"errors"
	"testing"
)

func TestNumericHandleRegistryExactLifetimeBoundaryAndClassSafety(t *testing.T) {
	registry := newNumericHandleRegistry()
	values := []uint64{41, 42}
	next := 0
	registry.next = func() (uint64, error) {
		value := values[next]
		next++
		return value, nil
	}
	registry.issued = maxProcessHandleIssuance - 1

	handle, err := registry.insert(handleClassCore, "core")
	if err != nil || handle != coreHandleTag|41 {
		t.Fatalf("last permitted issuance = %d, %v", handle, err)
	}
	if registry.issued != maxProcessHandleIssuance {
		t.Fatalf("issued = %d, want %d", registry.issued, maxProcessHandleIssuance)
	}
	if _, err := registry.insert(handleClassBuffer, []byte("buffer")); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("post-ceiling issuance error = %v, want allocation failure", err)
	}
	if next != 1 {
		t.Fatalf("random source called after ceiling: calls = %d", next)
	}

	if _, err := registry.get(handle, handleClassBuffer); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("cross-class get error = %v", err)
	}
	if _, _, err := registry.retire(handle, handleClassBuffer, true); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("cross-class retire error = %v", err)
	}
	value, removed, err := registry.retire(handle, handleClassCore, false)
	if err != nil || !removed || value != "core" {
		t.Fatalf("first retire = (%v, %v, %v)", value, removed, err)
	}
	if _, _, err := registry.retire(handle, handleClassCore, false); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("non-idempotent double retire error = %v", err)
	}
	if _, _, err := registry.retire(handle, handleClassCore, true); err != nil {
		t.Fatalf("explicit idempotent core retire: %v", err)
	}
	if _, _, err := registry.retire(handle, handleClassBuffer, true); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("retired cross-class confusion error = %v", err)
	}
}

func TestNumericHandleRegistryExhaustsBoundedCollisionAttempts(t *testing.T) {
	registry := newNumericHandleRegistry()
	registry.next = func() (uint64, error) { return 77, nil }
	handle, err := registry.insert(handleClassCore, "first")
	if err != nil || handle != coreHandleTag|77 {
		t.Fatalf("initial insert = %d, %v", handle, err)
	}
	before := registry.issued
	if _, err := registry.insert(handleClassBuffer, []byte("second")); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("collision exhaustion error = %v", err)
	}
	if registry.issued != before || len(registry.live) != 1 {
		t.Fatalf("failed allocation mutated registry: issued=%d live=%d", registry.issued, len(registry.live))
	}
}

func TestABIBufferExactBoundCopyIsolationZeroizationAndRetirement(t *testing.T) {
	registry := newNumericHandleRegistry()
	store := newBufferStoreForTest(registry)
	if err := validateABIBufferSize(maxABIBufferBytes - 1); err != nil {
		t.Fatalf("logical size immediately below maximum: %v", err)
	}
	if err := validateABIBufferSize(maxABIBufferBytes); err != nil {
		t.Fatalf("exact logical maximum: %v", err)
	}

	// Exercise allocation, copying, reading, and zeroization with bounded real
	// memory. The allocation-free checks above retain the exact 64 MiB ABI
	// buffer ceiling.
	source := bytes.Repeat([]byte{0xa5}, 4096)
	descriptor, err := store.allocate(source)
	if err != nil {
		t.Fatalf("allocate bounded buffer: %v", err)
	}
	if descriptor.BufferHandle == 0 || descriptor.ByteLength != uint64(len(source)) {
		t.Fatalf("descriptor = %#v", descriptor)
	}

	source[0] = 0
	destination := make([]byte, 3)
	n, err := store.read(descriptor.BufferHandle, 0, destination)
	if err != nil || n != 3 || !bytes.Equal(destination, []byte{0xa5, 0xa5, 0xa5}) {
		t.Fatalf("read isolated bytes = %x, n=%d, err=%v", destination, n, err)
	}
	if n, err := store.read(descriptor.BufferHandle, descriptor.ByteLength, destination); err != nil || n != 0 {
		t.Fatalf("end-offset read = %d, %v", n, err)
	}
	if _, err := store.read(descriptor.BufferHandle, descriptor.ByteLength+1, destination); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("past-end read error = %v", err)
	}

	slot, generation, ok := decodeBufferHandle(descriptor.BufferHandle)
	if !ok {
		t.Fatal("descriptor did not decode")
	}
	store.mu.RLock()
	if !store.slots[slot].live || store.slots[slot].generation != generation {
		store.mu.RUnlock()
		t.Fatal("descriptor did not identify the live slot generation")
	}
	stored := store.slots[slot].data
	store.mu.RUnlock()
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatalf("free: %v", err)
	}
	if !bytes.Equal(stored, make([]byte, len(stored))) {
		t.Fatal("freed wrapper-owned bytes were not cleared")
	}
	if _, err := store.read(descriptor.BufferHandle, 0, destination); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("read after free error = %v", err)
	}
	if err := store.free(descriptor.BufferHandle); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("double free error = %v", err)
	}
	if _, err := registry.get(descriptor.BufferHandle, handleClassCore); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("freed buffer as core error = %v", err)
	}
}

func TestABIBufferRejectsFirstByteBeyondExactMaximumWithoutIssuance(t *testing.T) {
	registry := newNumericHandleRegistry()
	if err := validateABIBufferSize(maxABIBufferBytes + 1); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("first byte over logical maximum error = %v", err)
	}
	if registry.issued != 0 || len(registry.live) != 0 || len(registry.retired) != 0 {
		t.Fatalf("oversized allocation mutated registry: issued=%d live=%d retired=%d", registry.issued, len(registry.live), len(registry.retired))
	}
}
