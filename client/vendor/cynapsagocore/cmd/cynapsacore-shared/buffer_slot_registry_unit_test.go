package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

func TestBufferHandleClassSlotAndGenerationEncoding(t *testing.T) {
	for _, test := range []struct {
		slot       int
		generation uint64
	}{
		{slot: 0, generation: 1},
		{slot: maxLiveABIBufferCount - 1, generation: maxBufferGeneration},
	} {
		handle := encodeBufferHandle(test.slot, test.generation)
		slot, generation, ok := decodeBufferHandle(handle)
		if !ok || slot != test.slot || generation != test.generation {
			t.Fatalf("decode(%#x) = (%d, %d, %v)", handle, slot, generation, ok)
		}
		if handleClassOf(handle) != handleClassBuffer || handle == 0 {
			t.Fatalf("buffer handle class = %d, handle=%#x", handleClassOf(handle), handle)
		}
	}
	for _, handle := range []uint64{0, coreHandleTag | 1, bufferHandleTag} {
		if _, _, ok := decodeBufferHandle(handle); ok {
			t.Fatalf("invalid buffer handle %#x decoded", handle)
		}
	}
}

func TestEmergencyQueueFullDescriptorIsExactClassSafeAndImmutable(t *testing.T) {
	descriptor := emergencyQueueFullDescriptor()
	if descriptor.BufferHandle != emergencyQueueFullHandle || descriptor.ByteLength != uint64(len(emergencyQueueFullJSON)) || descriptor.BufferHandle == 0 {
		t.Fatalf("emergency descriptor = %+v", descriptor)
	}
	if handleClassOf(descriptor.BufferHandle) != handleClassEmergencyBuffer {
		t.Fatalf("emergency class = %d", handleClassOf(descriptor.BufferHandle))
	}
	if _, _, ok := decodeBufferHandle(descriptor.BufferHandle); ok {
		t.Fatal("emergency descriptor decoded as a reusable buffer slot")
	}

	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	public := abiPublicError(v1.ErrorCodeQueueFull).(*v1.Error)
	encoded, err := adapter.EncodeABIError(*public)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != emergencyQueueFullJSON {
		t.Fatalf("emergency JSON is not the canonical queue_full encoding:\n got %s\nwant %s", encoded, emergencyQueueFullJSON)
	}
	clear(encoded)

	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	destination := make([]byte, descriptor.ByteLength)
	copied, err := store.read(descriptor.BufferHandle, 0, destination)
	if err != nil || copied != descriptor.ByteLength || string(destination) != emergencyQueueFullJSON {
		t.Fatalf("emergency read copied=%d err=%v data=%q", copied, err, destination)
	}
	short := make([]byte, 7)
	copied, err = store.read(descriptor.BufferHandle, 2, short)
	if err != nil || copied != uint64(len(short)) || string(short) != emergencyQueueFullJSON[2:2+len(short)] {
		t.Fatalf("bounded emergency read copied=%d err=%v data=%q", copied, err, short)
	}
	if _, err := store.read(descriptor.BufferHandle, descriptor.ByteLength+1, short); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("past-end emergency read = %v", err)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatalf("first emergency free: %v", err)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatalf("repeated emergency free: %v", err)
	}
	for _, foreign := range []uint64{coreHandleTag | 1, bufferHandleTag | 1<<bufferSlotBits, emergencyBufferTag | 2} {
		if _, err := store.read(foreign, 0, short); !errors.Is(err, errInvalidABIHandle) {
			t.Fatalf("foreign handle %#x read = %v", foreign, err)
		}
		if err := store.free(foreign); !errors.Is(err, errInvalidABIHandle) {
			t.Fatalf("foreign handle %#x free = %v", foreign, err)
		}
	}
}

func TestEncodedErrorFallsBackAtExactCountAndByteSaturation(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 64, item: 64})
		ordinary, err := store.allocate([]byte("held"))
		if err != nil {
			t.Fatal(err)
		}
		fallback := allocateEncodedCError(store, []byte("private allocation canary"))
		if fallback != emergencyQueueFullDescriptor() {
			t.Fatalf("count-saturated fallback = %+v", fallback)
		}
		if store.liveCount != 1 || store.liveBytes != 4 {
			t.Fatalf("emergency descriptor changed count accounting: count=%d bytes=%d", store.liveCount, store.liveBytes)
		}
		if _, err := store.allocate([]byte("ordinary result")); !errors.Is(err, errHandleAllocation) {
			t.Fatalf("ordinary result bypassed count quota: %v", err)
		}
		assertEmergencyQueueFullOnly(t, store, fallback)
		if err := store.free(ordinary.BufferHandle); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("aggregate bytes", func(t *testing.T) {
		store := newBufferStore()
		type reservation struct {
			slot       int
			generation uint64
		}
		reservations := make([]reservation, maxLiveABIBufferBytes/maxABIBufferBytes)
		for index := range reservations {
			slot, generation, err := store.reserve(maxABIBufferBytes)
			if err != nil {
				t.Fatalf("reserve aggregate part %d: %v", index, err)
			}
			reservations[index] = reservation{slot: slot, generation: generation}
		}
		if store.liveBytes != maxLiveABIBufferBytes {
			t.Fatalf("aggregate reservation = %d", store.liveBytes)
		}
		fallback := allocateEncodedCError(store, []byte("private allocation canary"))
		if fallback != emergencyQueueFullDescriptor() {
			t.Fatalf("byte-saturated fallback = %+v", fallback)
		}
		if _, err := store.allocate([]byte{1}); !errors.Is(err, errHandleAllocation) {
			t.Fatalf("ordinary result bypassed byte quota: %v", err)
		}
		assertEmergencyQueueFullOnly(t, store, fallback)
		for _, reserved := range reservations {
			store.rollback(reserved.slot, reserved.generation, maxABIBufferBytes)
		}
		if store.liveCount != 0 || store.liveBytes != 0 {
			t.Fatalf("aggregate rollback = count=%d bytes=%d", store.liveCount, store.liveBytes)
		}
	})
}

func TestEmergencyDescriptorConcurrentReadAndFreeAt4096SlotSaturation(t *testing.T) {
	store := newBufferStore()
	held := make([]BufferDescriptor, maxLiveABIBufferCount)
	for index := range held {
		var err error
		held[index], err = store.allocate(nil)
		if err != nil {
			t.Fatalf("allocate held descriptor %d: %v", index, err)
		}
	}
	fallback := allocateEncodedCError(store, []byte("must never escape"))
	if fallback != emergencyQueueFullDescriptor() {
		t.Fatalf("saturated fallback = %+v", fallback)
	}
	if _, err := store.allocate(nil); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("ordinary zero-byte result bypassed saturated slots: %v", err)
	}

	const workers = 32
	const iterations = 100
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range iterations {
				destination := make([]byte, fallback.ByteLength)
				copied, err := store.read(fallback.BufferHandle, 0, destination)
				if err != nil || copied != fallback.ByteLength || string(destination) != emergencyQueueFullJSON {
					t.Errorf("concurrent emergency read copied=%d err=%v", copied, err)
					return
				}
				if err := store.free(fallback.BufferHandle); err != nil {
					t.Errorf("concurrent emergency free: %v", err)
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	if store.liveCount != maxLiveABIBufferCount || store.liveBytes != 0 {
		t.Fatalf("emergency concurrency changed accounting: count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
	for _, descriptor := range held {
		if err := store.free(descriptor.BufferHandle); err != nil {
			t.Fatal(err)
		}
	}
	replacement, err := store.allocate([]byte("ordinary"))
	if err != nil || handleClassOf(replacement.BufferHandle) != handleClassBuffer {
		t.Fatalf("ordinary allocation after saturation = %+v err=%v", replacement, err)
	}
	if err := store.free(replacement.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if store.liveCount != 0 || store.liveBytes != 0 {
		t.Fatalf("post-saturation accounting: count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
}

func assertEmergencyQueueFullOnly(t *testing.T, store *bufferStore, descriptor BufferDescriptor) {
	t.Helper()
	destination := make([]byte, descriptor.ByteLength)
	copied, err := store.read(descriptor.BufferHandle, 0, destination)
	if err != nil || copied != descriptor.ByteLength || string(destination) != emergencyQueueFullJSON {
		t.Fatalf("emergency descriptor copied=%d err=%v data=%q", copied, err, destination)
	}
	if bytes.Contains(destination, []byte("private")) || bytes.Contains(destination, []byte("canary")) {
		t.Fatalf("private allocation input leaked: %q", destination)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
}

func TestBufferSlotReuseRejectsEveryStaleGeneration(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 32, item: 32})
	first, err := store.allocate([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.free(first.BufferHandle); err != nil {
		t.Fatal(err)
	}
	second, err := store.allocate([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if first.BufferHandle == second.BufferHandle {
		t.Fatal("reused slot repeated a stale handle")
	}
	for _, operation := range []func() error{
		func() error { _, err := store.read(first.BufferHandle, 0, make([]byte, 8)); return err },
		func() error { return store.free(first.BufferHandle) },
	} {
		if err := operation(); !errors.Is(err, errInvalidABIHandle) {
			t.Fatalf("stale generation error = %v", err)
		}
	}
	if err := store.free(second.BufferHandle); err != nil {
		t.Fatal(err)
	}
}

func TestBufferGenerationExhaustionNeverWraps(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 8, item: 8})
	store.slots[0].generation = maxBufferGeneration - 1
	descriptor, err := store.allocate([]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	_, generation, ok := decodeBufferHandle(descriptor.BufferHandle)
	if !ok || generation != maxBufferGeneration {
		t.Fatalf("final generation = %d, valid=%v", generation, ok)
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.allocate([]byte{2}); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("post-exhaustion allocation = %v", err)
	}
	if store.liveCount != 0 || store.liveBytes != 0 || store.slots[0].generation != maxBufferGeneration {
		t.Fatalf("exhaustion mutated accounting: count=%d bytes=%d generation=%d", store.liveCount, store.liveBytes, store.slots[0].generation)
	}
}

func TestBufferQuotasReserveAtomicallyBeforeClone(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 4, item: 4})
	entered := make(chan struct{})
	release := make(chan struct{})
	var clones atomic.Int32
	store.clone = func(data []byte) ([]byte, error) {
		clones.Add(1)
		close(entered)
		<-release
		return bytes.Clone(data), nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.allocate([]byte("1234"))
		result <- err
	}()
	<-entered
	if _, err := store.allocate(nil); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("count quota during clone = %v", err)
	}
	if clones.Load() != 1 {
		t.Fatalf("rejected allocation cloned input; calls=%d", clones.Load())
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if store.liveCount != 1 || store.liveBytes != 4 {
		t.Fatalf("published accounting = (%d,%d)", store.liveCount, store.liveBytes)
	}
}

func TestBufferCloneFailureAndPanicRollbackReservations(t *testing.T) {
	for _, test := range []struct {
		name  string
		clone func([]byte) ([]byte, error)
		panic bool
	}{
		{name: "error", clone: func([]byte) ([]byte, error) { return []byte{9, 9}, errors.New("clone") }},
		{name: "panic", clone: func([]byte) ([]byte, error) { panic("clone") }, panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 8, item: 8})
			store.clone = test.clone
			func() {
				defer func() {
					recovered := recover()
					if test.panic != (recovered != nil) {
						t.Fatalf("panic=%v, recovered=%v", test.panic, recovered)
					}
				}()
				if _, err := store.allocate([]byte{1, 2}); !test.panic && !errors.Is(err, errHandleAllocation) {
					t.Fatalf("clone failure error = %v", err)
				}
			}()
			if store.liveCount != 0 || store.liveBytes != 0 || store.slots[0].reserved || store.slots[0].live || store.slots[0].generation != 0 {
				t.Fatalf("failed clone leaked reservation: %+v count=%d bytes=%d", store.slots[0], store.liveCount, store.liveBytes)
			}
		})
	}
}

func TestBufferCountAndByteQuotaBoundariesReleaseExactlyOnce(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 2, bytes: 5, item: 4})
	first, err := store.allocate([]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.allocate([]byte{4, 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.allocate(nil); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("exact count quota error = %v", err)
	}
	if _, err := store.allocate([]byte{6}); !errors.Is(err, errHandleAllocation) {
		t.Fatalf("exact byte quota error = %v", err)
	}
	if err := store.free(first.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if err := store.free(first.BufferHandle); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("double free = %v", err)
	}
	if store.liveCount != 1 || store.liveBytes != 2 {
		t.Fatalf("double free changed accounting: count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
	if err := store.free(second.BufferHandle); err != nil {
		t.Fatal(err)
	}
}

func TestProcessBufferCountAndAggregateByteLimitsAreExact(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		store := newBufferStore()
		descriptors := make([]BufferDescriptor, maxLiveABIBufferCount)
		for index := range descriptors {
			var err error
			descriptors[index], err = store.allocate(nil)
			if err != nil {
				t.Fatalf("allocate slot %d: %v", index, err)
			}
		}
		if _, err := store.allocate(nil); !errors.Is(err, errHandleAllocation) {
			t.Fatalf("slot %d allocation = %v", maxLiveABIBufferCount+1, err)
		}
		for _, descriptor := range descriptors {
			if err := store.free(descriptor.BufferHandle); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("bytes", func(t *testing.T) {
		store := newBufferStore()
		type reservation struct {
			slot       int
			generation uint64
		}
		reservations := make([]reservation, maxLiveABIBufferBytes/maxABIBufferBytes)
		for index := range reservations {
			slot, generation, err := store.reserve(maxABIBufferBytes)
			if err != nil {
				t.Fatalf("reserve part %d: %v", index, err)
			}
			reservations[index] = reservation{slot: slot, generation: generation}
		}
		if store.liveBytes != maxLiveABIBufferBytes {
			t.Fatalf("aggregate bytes = %d", store.liveBytes)
		}
		if _, _, err := store.reserve(1); !errors.Is(err, errHandleAllocation) {
			t.Fatalf("first aggregate byte over limit = %v", err)
		}
		for _, reserved := range reservations {
			store.rollback(reserved.slot, reserved.generation, maxABIBufferBytes)
		}
		if store.liveCount != 0 || store.liveBytes != 0 {
			t.Fatalf("reservation rollback = count %d bytes %d", store.liveCount, store.liveBytes)
		}
	})
}

func TestBufferMaximumOutputBoundaryIsExactAndAllocationFree(t *testing.T) {
	for _, test := range []struct {
		size uint64
		want error
	}{
		{size: maxABIBufferBytes - 1},
		{size: maxABIBufferBytes},
		{size: maxABIBufferBytes + 1, want: errHandleAllocation},
		{size: ^uint64(0), want: errHandleAllocation},
	} {
		err := validateABIBufferSize(test.size)
		if !errors.Is(err, test.want) {
			t.Fatalf("validate(%d) = %v, want %v", test.size, err, test.want)
		}
	}
}

func TestBufferCapacityFailureNormalizesWithoutPrivateLeakage(t *testing.T) {
	public, ok := normalizeABIError(errors.Join(errors.New("private buffer allocator detail"), errHandleAllocation)).(*v1.Error)
	if !ok {
		t.Fatalf("normalized capacity type = %T", public)
	}
	if public.Code != v1.ErrorCodeQueueFull || public.Stage != v1.ErrorStageSDK || public.Location != v1.ErrorLocationLocal || public.Message != "The local queue is full" {
		t.Fatalf("normalized capacity = %#v", public)
	}
	if bytes.Contains([]byte(public.Message), []byte("allocator")) {
		t.Fatalf("private allocator detail leaked: %q", public.Message)
	}
}

func TestOrdinaryLargestABIOutputFamiliesFitPerBufferCeiling(t *testing.T) {
	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	const maximumPayloadChunkBytes = 1 << 20
	chunk := make([]byte, maximumPayloadChunkBytes)
	payloadRead, err := adapter.EncodeABIPayloadRead(chunk, false)
	if err != nil {
		t.Fatal(err)
	}
	// The payload-read codec is the ordinary byte-heavy result: JSON's []byte
	// encoding is exactly RFC 4648 base64 plus fixed deterministic framing.
	wantPayloadReadBytes := len(`{"abi_version":1,"chunk":"","eof":false}`) + base64.StdEncoding.EncodedLen(maximumPayloadChunkBytes)
	if len(payloadRead) != wantPayloadReadBytes {
		t.Fatalf("maximum payload-read bytes = %d, derived %d", len(payloadRead), wantPayloadReadBytes)
	}

	payloadHandle := v1.PayloadHandle("payh_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
	maximumEscapedIdentifier := strings.Repeat("<", 512)
	completion, err := adapter.EncodeABICompletion(v1.Completion{
		CommandID: v1.CommandID(maximumEscapedIdentifier),
		OK:        true,
		Result:    v1.PayloadHandleResult{Handle: payloadHandle, Size: maximumPayloadChunkBytes, Chunk: chunk},
	})
	if err != nil {
		t.Fatal(err)
	}
	eventBody := make([]byte, 256<<10-len("application/octet-stream")-1)
	event, err := adapter.EncodeABIEvent(v1.Event{
		ID:        v1.EventID(maximumEscapedIdentifier),
		Name:      v1.EventMessageReceived,
		CreatedAt: time.Unix(1, 0),
		Payload: v1.MessageReceivedEvent{
			MessageID:      v1.MessageID(maximumEscapedIdentifier),
			ConversationID: v1.ConversationID(maximumEscapedIdentifier),
			FromAgentID:    v1.AgentID(maximumEscapedIdentifier),
			MeshID:         v1.MeshID(maximumEscapedIdentifier),
			Mode:           v1.MessageModeOneWay,
			Payload: v1.Payload{Value: v1.NativePayload{
				ContentType: "application/octet-stream",
				Path:        "/",
				Body:        eventBody,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := adapter.EncodeABIStatus(v1.Status{
		Lifecycle:    v1.LifecycleReady,
		Connectivity: v1.ConnectivityAvailable,
		Personality:  v1.SDKPersonalityNative,
		AgentID:      v1.AgentID(maximumEscapedIdentifier),
		MeshID:       v1.MeshID(maximumEscapedIdentifier),
		MeshEndpoint: strings.Repeat("<", 8192),
	})
	if err != nil {
		t.Fatal(err)
	}
	abiError, err := adapter.EncodeABIError(v1.Error{
		Code:     v1.ErrorCodeQueueFull,
		Message:  "The local queue is full",
		Stage:    v1.ErrorStageSDK,
		Location: v1.ErrorLocationLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string][]byte{
		"payload read": payloadRead,
		"completion":   completion,
		"event":        event,
		"status":       status,
		"error":        abiError,
	} {
		if uint64(len(output)) > maxABIBufferBytes {
			t.Fatalf("maximum ordinary %s output = %d, ceiling=%d", name, len(output), maxABIBufferBytes)
		}
		clear(output)
	}
}

func TestConcurrentBufferAllocateReadFreeAndStaleReaders(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 32, bytes: 32 << 10, item: 1024})
	const workers = 16
	const iterations = 250
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(value byte) {
			defer wait.Done()
			for range iterations {
				source := bytes.Repeat([]byte{value}, 128)
				descriptor, err := store.allocate(source)
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				destination := make([]byte, len(source))
				if _, err := store.read(descriptor.BufferHandle, 0, destination); err != nil || !bytes.Equal(source, destination) {
					t.Errorf("read: %v equal=%v", err, bytes.Equal(source, destination))
					return
				}
				if err := store.free(descriptor.BufferHandle); err != nil {
					t.Errorf("free: %v", err)
					return
				}
				if _, err := store.read(descriptor.BufferHandle, 0, destination); !errors.Is(err, errInvalidABIHandle) {
					t.Errorf("stale read: %v", err)
					return
				}
			}
		}(byte(worker + 1))
	}
	wait.Wait()
	if store.liveCount != 0 || store.liveBytes != 0 {
		t.Fatalf("concurrent lifecycle leaked count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
}

func TestConcurrentReadsAndFreeAreLinearized(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 1024, item: 1024})
	source := bytes.Repeat([]byte{0xa5}, 1024)
	descriptor, err := store.allocate(source)
	if err != nil {
		t.Fatal(err)
	}
	const readers = 32
	start := make(chan struct{})
	firstRead := make(chan struct{}, readers)
	var wait sync.WaitGroup
	for range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			reported := false
			for {
				destination := make([]byte, len(source))
				n, err := store.read(descriptor.BufferHandle, 0, destination)
				if errors.Is(err, errInvalidABIHandle) {
					return
				}
				if err != nil || n != uint64(len(source)) || !bytes.Equal(source, destination) {
					t.Errorf("concurrent read n=%d err=%v equal=%v", n, err, bytes.Equal(source, destination))
					return
				}
				if !reported {
					reported = true
					firstRead <- struct{}{}
				}
			}
		}()
	}
	close(start)
	for range readers {
		select {
		case <-firstRead:
		case <-time.After(time.Second):
			t.Fatal("reader did not make progress")
		}
	}
	if err := store.free(descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wait.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("readers did not observe retirement")
	}
}

func TestCallbackPanicReleasesUntransferredDescriptorAndDispatcherContinues(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	manager := newCallbackManagerForLifecycleTest()
	manager.buffers = store
	manager.wg.Add(1)
	invoked := make(chan struct{}, 2)
	manager.invoke = func(_ unsafe.Pointer, _ uint64, _ uint64, _ uint32, descriptor BufferDescriptor) {
		invoked <- struct{}{}
		panic(descriptor.BufferHandle)
	}
	go manager.dispatch()
	for _, value := range [][]byte{[]byte("first"), []byte("again")} {
		descriptor, err := store.allocate(value)
		if err != nil {
			t.Fatal(err)
		}
		store.mu.RLock()
		freed := store.capacityChanged
		store.mu.RUnlock()
		manager.jobs <- callbackJob{
			token: 1, kind: callbackKindCompletion, descriptor: descriptor,
			commit: func() error { return nil }, rollback: func() error { return nil },
		}
		select {
		case <-invoked:
		case <-time.After(time.Second):
			t.Fatal("dispatcher did not survive callback panic")
		}
		select {
		case <-freed:
		case <-time.After(time.Second):
			t.Fatal("callback panic did not release descriptor")
		}
	}
	manager.cancel()
	done := make(chan struct{})
	go func() { manager.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop")
	}
	if store.liveCount != 0 || store.liveBytes != 0 {
		t.Fatalf("callback panic leaked count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
}
