package payload

import (
	"bytes"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func testHandleLimits() HandleLimits {
	return HandleLimits{MaximumHandles: 8, MaximumBytes: 256, MaximumPerHandle: 128, MaximumWriteBytes: 64, MaximumReadBytes: 8}
}
func handleCanonical(t *testing.T, body string) []byte {
	t.Helper()
	serializer, _ := NewSerializer(128)
	value, err := serializer.Serialize(model.Payload{Value: model.NativePayload{Path: "/", Body: []byte(body)}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestHandleLifecycleAndBounds(t *testing.T) {
	t.Parallel()
	store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	canonical := handleCanonical(t, "hello")
	split := len(canonical) / 2
	if err := store.Write(handle, canonical[:split]); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, canonical[split:]); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, make([]byte, 65)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("write bound: %v", err)
	}
	if err := store.Finish(handle); err != nil {
		t.Fatal(err)
	}
	if size, err := store.Size(handle); err != nil || size != int64(len(canonical)) {
		t.Fatalf("size = %d, %v; want %d", size, err, len(canonical))
	}
	if err := store.Finish(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("double finish: %v", err)
	}
	if err := store.Write(handle, []byte("x")); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("write complete: %v", err)
	}
	first, eof, err := store.Read(handle, 0, 3)
	if err != nil || eof || !bytes.Equal(first, canonical[:3]) {
		t.Fatalf("read: %q %v %v", first, eof, err)
	}
	last, eof, err := store.Read(handle, int64(len(canonical)-3), 8)
	if err != nil || !eof || !bytes.Equal(last, canonical[len(canonical)-3:]) {
		t.Fatalf("last: %q %v %v", last, eof, err)
	}
	if err := store.Retain(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Read(handle, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Read(handle, 0, 1); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("stale read: %v", err)
	}
}

func TestHandleSizeRequiresLiveCompletedHandle(t *testing.T) {
	t.Parallel()
	store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewHandleStore(testHandleLimits(), bytes.NewReader(bytes.Repeat([]byte{1}, 256)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("open size error = %v", err)
	}
	canonical := handleCanonical(t, "size states")
	if err := store.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("writing size error = %v", err)
	}

	store.mu.Lock()
	store.entries[handle].state = HandleFinishing
	store.mu.Unlock()
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finishing size error = %v", err)
	}
	if err := store.Release(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finishing release error = %v", err)
	}
	store.mu.Lock()
	store.entries[handle].state = HandleWriting
	store.mu.Unlock()

	if err := store.Cancel(handle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("cancelled size error = %v", err)
	}
	if _, err := other.Size(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("foreign size error = %v", err)
	}
	if _, err := store.Size("not-a-handle"); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("malformed size error = %v", err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("released size error = %v", err)
	}
}

func TestHandleCancelCompletedIsImmutable(t *testing.T) {
	t.Parallel()
	store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	canonical := handleCanonical(t, "completed cancel")
	if err := store.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(handle); err != nil {
		t.Fatal(err)
	}

	store.mu.RLock()
	entry := store.entries[handle]
	beforeData := clone(entry.data)
	beforeDigest := entry.digest
	beforeRefs := entry.refs
	beforeTotal := store.totalBytes
	store.mu.RUnlock()

	if err := store.Cancel(handle); err != nil {
		t.Fatalf("first completed cancel: %v", err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatalf("second completed cancel: %v", err)
	}

	store.mu.RLock()
	entry = store.entries[handle]
	if entry.state != HandleCompleted || entry.refs != beforeRefs || entry.digest != beforeDigest || !bytes.Equal(entry.data, beforeData) || store.totalBytes != beforeTotal {
		store.mu.RUnlock()
		t.Fatal("completed cancel mutated immutable handle state")
	}
	store.mu.RUnlock()
	zero(beforeData)

	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
}

func TestHandleCancelZeroesOwnedBytes(t *testing.T) {
	t.Parallel()
	store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, []byte("sensitive partial bytes")); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	owned := store.entries[handle].chunks[0].data
	store.mu.RUnlock()
	if err := store.Cancel(handle); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, make([]byte, len(owned))) {
		t.Fatal("cancel did not zero the owned backing bytes")
	}
}

func TestHandleCancelStateContract(t *testing.T) {
	t.Parallel()
	store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewHandleStore(testHandleLimits(), bytes.NewReader(bytes.Repeat([]byte{1}, 256)))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Cancel(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("foreign cancel error = %v", err)
	}
	if err := store.Cancel("not-a-handle"); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("malformed cancel error = %v", err)
	}

	store.mu.Lock()
	store.entries[handle].state = HandleFinishing
	store.mu.Unlock()
	if err := store.Cancel(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finishing cancel error = %v", err)
	}
	store.mu.Lock()
	store.entries[handle].state = HandleOpen
	store.mu.Unlock()

	if err := store.Cancel(handle); err != nil {
		t.Fatalf("open cancel: %v", err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatalf("cancelled cancel: %v", err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatalf("cancelled release: %v", err)
	}
	if err := store.Cancel(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("released cancel error = %v", err)
	}
}

func TestHandleConcurrentFinishSizeCancelRelease(t *testing.T) {
	for iteration := 0; iteration < 256; iteration++ {
		store, err := NewHandleStore(testHandleLimits(), bytes.NewReader(bytes.Repeat([]byte{byte(iteration)}, 64)))
		if err != nil {
			t.Fatal(err)
		}
		handle, err := store.Open()
		if err != nil {
			t.Fatal(err)
		}
		canonical := handleCanonical(t, "race")
		if err := store.Write(handle, canonical); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		errs := make(chan error, 4)
		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			<-start
			err := store.Finish(handle)
			if err != nil && !errors.Is(err, ErrInvalidHandle) && !errors.Is(err, ErrInvalidHandleState) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			size, err := store.Size(handle)
			if err == nil && size != int64(len(canonical)) {
				errs <- errors.New("successful size returned a non-canonical byte count")
			} else if err != nil && !errors.Is(err, ErrInvalidHandle) && !errors.Is(err, ErrInvalidHandleState) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			err := store.Cancel(handle)
			if err != nil && !errors.Is(err, ErrInvalidHandle) && !errors.Is(err, ErrInvalidHandleState) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			err := store.Release(handle)
			if err != nil && !errors.Is(err, ErrInvalidHandle) && !errors.Is(err, ErrInvalidHandleState) {
				errs <- err
			}
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("iteration %d: unexpected race error: %v", iteration, err)
		}
		store.Close()
	}
}

func TestHandleCancelCrossCoreAndReferenceOverflow(t *testing.T) {
	t.Parallel()
	store, _ := NewHandleStore(testHandleLimits(), nil)
	other, _ := NewHandleStore(testHandleLimits(), bytes.NewReader(bytes.Repeat([]byte{1}, 512)))
	handle, _ := store.Open()
	_ = store.Write(handle, []byte("secret"))
	if err := store.Finish(handle); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("malformed finish: %v", err)
	}
	if err := other.Write(handle, []byte("x")); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("cross core: %v", err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(handle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finish cancelled: %v", err)
	}
	overflow, _ := store.Open()
	value := handleCanonical(t, "")
	_ = store.Write(overflow, value)
	_ = store.Finish(overflow)
	store.mu.Lock()
	store.entries[overflow].refs = math.MaxUint64
	store.mu.Unlock()
	if err := store.Retain(overflow); !errors.Is(err, ErrReferenceOverflow) {
		t.Fatalf("overflow: %v", err)
	}
}

func TestHandleConcurrentReadsAndRelease(t *testing.T) {
	store, _ := NewHandleStore(testHandleLimits(), bytes.NewReader(make([]byte, 512)))
	handle, _ := store.Open()
	canonical := handleCanonical(t, "concurrent")
	split := len(canonical) / 2
	_ = store.Write(handle, canonical[:split])
	_ = store.Write(handle, canonical[split:])
	_ = store.Finish(handle)
	for i := 0; i < 32; i++ {
		if err := store.Retain(handle); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, _, err := store.Read(handle, 0, 8)
			if err != nil || !bytes.Equal(value, canonical[:8]) {
				t.Errorf("read %q %v", value, err)
			}
			if err := store.Release(handle); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := store.Release(handle); err != nil {
		t.Fatal(err)
	}
}
