package main

import (
	"fmt"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestNativeCoreCreatePayloadLimitExactBoundary(t *testing.T) {
	atLimit := fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes)
	handle, descriptor, err := CoreCreate([]byte(atLimit))
	if err != nil || handle == 0 || descriptor != (BufferDescriptor{}) {
		t.Fatalf("exact maximum = (%d, %#v, %v)", handle, descriptor, err)
	}
	if err := CoreStart(handle); err != nil {
		_ = CoreDestroy(handle)
		t.Fatalf("start: %v", err)
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- CoreShutdown(handle, 2_000) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case err := <-shutdown:
			if err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			goto closed
		default:
		}
		for _, next := range []func(uint64, int64) (BufferDescriptor, error){CoreNextCompletion, CoreNextEvent} {
			descriptor, err := next(handle, 5)
			if err == nil && descriptor.BufferHandle != 0 {
				if err := FreeBuffer(descriptor.BufferHandle); err != nil {
					t.Fatalf("free drained buffer: %v", err)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("shutdown did not complete")
		}
	}

closed:
	if err := CoreDestroy(handle); err != nil {
		t.Fatalf("destroy: %v", err)
	}

	overLimit := fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes+1)
	if handle, descriptor, err := CoreCreate([]byte(overLimit)); handle != 0 || descriptor != (BufferDescriptor{}) || err == nil {
		t.Fatalf("maximum + 1 = (%d, %#v, %v), want rejection", handle, descriptor, err)
	}
}
