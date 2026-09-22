package main

import (
	"errors"
	"testing"
)

func FuzzABIBufferReadBoundsAndHandleClass(f *testing.F) {
	f.Add([]byte("value"), uint64(0), uint16(5), uint64(1))
	f.Add([]byte{}, uint64(0), uint16(0), uint64(0))
	f.Add([]byte{0, 1, 2, 3}, ^uint64(0), uint16(64), ^uint64(0))
	f.Fuzz(func(t *testing.T, data []byte, offset uint64, destinationLength uint16, unknownHandle uint64) {
		if len(data) > 4096 {
			data = data[:4096]
		}
		registry := newNumericHandleRegistry()
		registry.next = func() (uint64, error) { return 123, nil }
		store := newBufferStoreForTest(registry)
		descriptor, err := store.allocate(data)
		if err != nil {
			t.Fatalf("bounded allocation: %v", err)
		}
		destination := make([]byte, int(destinationLength)%4097)
		n, readErr := store.read(descriptor.BufferHandle, offset, destination)
		if offset > uint64(len(data)) {
			if !errors.Is(readErr, errInvalidABIHandle) {
				t.Fatalf("offset %d > size %d error = %v", offset, len(data), readErr)
			}
		} else {
			if readErr != nil {
				t.Fatalf("valid offset %d error = %v", offset, readErr)
			}
			remaining := uint64(len(data)) - offset
			want := uint64(len(destination))
			if remaining < want {
				want = remaining
			}
			if n != want {
				t.Fatalf("copied = %d, want %d", n, want)
			}
			for index := uint64(0); index < n; index++ {
				if destination[index] != data[offset+index] {
					t.Fatalf("copy mismatch at %d", index)
				}
			}
		}

		if unknownHandle != descriptor.BufferHandle {
			if _, err := store.read(unknownHandle, 0, destination); !errors.Is(err, errInvalidABIHandle) {
				t.Fatalf("unknown handle %d error = %v", unknownHandle, err)
			}
		}
		if err := store.free(descriptor.BufferHandle); err != nil {
			t.Fatalf("free: %v", err)
		}
	})
}
