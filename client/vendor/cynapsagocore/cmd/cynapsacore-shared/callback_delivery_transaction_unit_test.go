package main

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"
)

type callbackTestSource struct {
	mu        sync.Mutex
	items     [][]byte
	leased    bool
	changed   chan struct{}
	rollbacks chan struct{}
	commits   int
}

func newCallbackTestSource(values ...string) *callbackTestSource {
	items := make([][]byte, len(values))
	for index, value := range values {
		items[index] = []byte(value)
	}
	return &callbackTestSource{items: items, changed: make(chan struct{}), rollbacks: make(chan struct{}, 64)}
}

func (source *callbackTestSource) reserve(ctx context.Context, name string) (callbackReservation, error) {
	for {
		source.mu.Lock()
		if len(source.items) != 0 && !source.leased {
			source.leased = true
			encoded := append([]byte(nil), source.items[0]...)
			source.mu.Unlock()
			return callbackReservation{
				encoded: encoded,
				name:    name,
				commit: func() error {
					source.mu.Lock()
					defer source.mu.Unlock()
					if !source.leased || len(source.items) == 0 {
						return errors.New("test source commit without lease")
					}
					clear(source.items[0])
					source.items = source.items[1:]
					source.leased = false
					source.commits++
					source.notifyLocked()
					return nil
				},
				rollback: func() error {
					source.mu.Lock()
					defer source.mu.Unlock()
					if !source.leased {
						return errors.New("test source rollback without lease")
					}
					source.leased = false
					source.notifyLocked()
					select {
					case source.rollbacks <- struct{}{}:
					default:
					}
					return nil
				},
			}, nil
		}
		changed := source.changed
		source.mu.Unlock()
		select {
		case <-ctx.Done():
			return callbackReservation{}, ctx.Err()
		case <-changed:
		}
	}
}

func (source *callbackTestSource) notifyLocked() {
	close(source.changed)
	source.changed = make(chan struct{})
}

func (source *callbackTestSource) snapshot() (remaining, commits int, leased bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return len(source.items), source.commits, source.leased
}

func newTransactionalCallbackTestManager(store *bufferStore, source *callbackTestSource, kind uint32, invoke func(BufferDescriptor)) *callbackManager {
	manager := newCallbackManagerForLifecycleTest()
	manager.coreHandle = 1
	manager.callback = unsafe.Pointer(new(byte))
	manager.buffers = store
	manager.invoke = func(_ unsafe.Pointer, _ uint64, _ uint64, _ uint32, descriptor BufferDescriptor) { invoke(descriptor) }
	manager.reserveCompletion = func(ctx context.Context) (callbackReservation, error) { return source.reserve(ctx, "") }
	eventName := "message.received"
	if kind == callbackKindDiagnostic {
		eventName = "diagnostics.log"
	}
	manager.reserveEvent = func(ctx context.Context) (callbackReservation, error) { return source.reserve(ctx, eventName) }
	if kind == callbackKindCompletion {
		manager.completion = 11
	} else {
		manager.event = 12
		if kind == callbackKindDiagnostic {
			manager.diagnostic = 13
		}
	}
	return manager
}

func stopTransactionalCallbackTestManager(t *testing.T, manager *callbackManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.stop(ctx); err != nil {
		t.Fatalf("stop callback manager: %v", err)
	}
}

func TestCallbackDeliveryWaitsForCountAndByteCapacityWithoutConsuming(t *testing.T) {
	tests := []struct {
		name   string
		limits bufferLimits
		held   []byte
		value  string
		kind   uint32
	}{
		{name: "completion_count", limits: bufferLimits{count: 1, bytes: 16, item: 16}, held: []byte("held"), value: "completion", kind: callbackKindCompletion},
		{name: "event_bytes", limits: bufferLimits{count: 2, bytes: 8, item: 8}, held: []byte("12345678"), value: "event", kind: callbackKindEvent},
		{name: "diagnostic_count", limits: bufferLimits{count: 1, bytes: 16, item: 16}, held: []byte("held"), value: "diagnostic", kind: callbackKindDiagnostic},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newBufferStoreWithLimits(test.limits)
			held, err := store.allocate(test.held)
			if err != nil {
				t.Fatal(err)
			}
			source := newCallbackTestSource(test.value)
			called := make(chan BufferDescriptor, 1)
			manager := newTransactionalCallbackTestManager(store, source, test.kind, func(descriptor BufferDescriptor) { called <- descriptor })
			manager.start()
			select {
			case <-source.rollbacks:
			case <-time.After(time.Second):
				t.Fatal("capacity saturation did not roll back authoritative delivery")
			}
			select {
			case descriptor := <-called:
				_ = store.free(descriptor.BufferHandle)
				t.Fatal("callback ran while process buffer capacity was saturated")
			default:
			}
			if remaining, commits, leased := source.snapshot(); remaining != 1 || commits != 0 || leased {
				t.Fatalf("saturated source state = remaining %d commits %d leased %v", remaining, commits, leased)
			}
			if err = store.free(held.BufferHandle); err != nil {
				t.Fatal(err)
			}
			var descriptor BufferDescriptor
			select {
			case descriptor = <-called:
			case <-time.After(time.Second):
				t.Fatal("callback did not resume after buffer capacity wake")
			}
			data := make([]byte, descriptor.ByteLength)
			if _, err = store.read(descriptor.BufferHandle, 0, data); err != nil || string(data) != test.value {
				t.Fatalf("callback descriptor = %q, %v", data, err)
			}
			if err = store.free(descriptor.BufferHandle); err != nil {
				t.Fatal(err)
			}
			stopTransactionalCallbackTestManager(t, manager)
			if remaining, commits, leased := source.snapshot(); remaining != 0 || commits != 1 || leased {
				t.Fatalf("completed source state = remaining %d commits %d leased %v", remaining, commits, leased)
			}
		})
	}
}

func TestCallbackDeterministicAllocationFailureRollsBackForPollFallback(t *testing.T) {
	for _, test := range []struct {
		name  string
		clone func([]byte) ([]byte, error)
	}{
		{name: "error", clone: func([]byte) ([]byte, error) { return nil, errors.New("deterministic clone failure") }},
		{name: "panic", clone: func([]byte) ([]byte, error) { panic("deterministic clone panic") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
			store.clone = test.clone
			source := newCallbackTestSource("fallback")
			called := make(chan BufferDescriptor, 1)
			manager := newTransactionalCallbackTestManager(store, source, callbackKindCompletion, func(descriptor BufferDescriptor) { called <- descriptor })
			manager.start()
			select {
			case <-source.rollbacks:
			case <-time.After(time.Second):
				t.Fatal("deterministic allocation failure did not roll back")
			}
			select {
			case descriptor := <-called:
				t.Fatalf("callback received descriptor after deterministic allocation failure: %+v", descriptor)
			default:
			}
			reservation, err := source.reserve(context.Background(), "")
			if err != nil || !bytes.Equal(reservation.encoded, []byte("fallback")) {
				t.Fatalf("poll fallback reservation = %q, %v", reservation.encoded, err)
			}
			clear(reservation.encoded)
			if err = reservation.commit(); err != nil {
				t.Fatal(err)
			}
			stopTransactionalCallbackTestManager(t, manager)
			if store.liveCount != 0 || store.liveBytes != 0 {
				t.Fatalf("failed allocation retained count=%d bytes=%d", store.liveCount, store.liveBytes)
			}
		})
	}
}

func TestCallbackClearRollsBackPreparedJobAndFreesDescriptorExactlyOnce(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	source := newCallbackTestSource("clear-race")
	manager := newTransactionalCallbackTestManager(store, source, callbackKindEvent, func(BufferDescriptor) { t.Error("callback invoked after clear won") })
	arrived := make(chan BufferDescriptor, 1)
	release := make(chan struct{})
	manager.beforeDispatch = func(job callbackJob) {
		arrived <- job.descriptor
		<-release
	}
	manager.start()
	descriptor := <-arrived
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stopped <- manager.stop(ctx)
	}()
	deadline := time.Now().Add(time.Second)
	for manager.state.begin() {
		manager.state.end()
		if time.Now().After(deadline) {
			t.Fatal("callback clear did not close admission")
		}
	}
	select {
	case <-manager.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("callback clear did not cancel prepared-delivery pumps")
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if remaining, commits, leased := source.snapshot(); remaining != 1 || commits != 0 || leased {
		t.Fatalf("clear rollback state = remaining %d commits %d leased %v", remaining, commits, leased)
	}
	if err := store.free(descriptor.BufferHandle); !errors.Is(err, errInvalidABIHandle) {
		t.Fatalf("descriptor second free = %v", err)
	}
	if store.liveCount != 0 || store.liveBytes != 0 {
		t.Fatalf("clear retained count=%d bytes=%d", store.liveCount, store.liveBytes)
	}
}

func TestCallbackDeliveryFIFOAndNoDuplicateDispatch(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 4, bytes: 64, item: 16})
	source := newCallbackTestSource("one", "two", "three")
	values := make(chan string, 3)
	manager := newTransactionalCallbackTestManager(store, source, callbackKindCompletion, func(descriptor BufferDescriptor) {
		data := make([]byte, descriptor.ByteLength)
		if _, err := store.read(descriptor.BufferHandle, 0, data); err != nil {
			values <- "read-error"
		} else {
			values <- string(data)
		}
		_ = store.free(descriptor.BufferHandle)
	})
	manager.start()
	for _, want := range []string{"one", "two", "three"} {
		select {
		case got := <-values:
			if got != want {
				t.Fatalf("callback order got %q want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("callback %q timed out", want)
		}
	}
	select {
	case duplicate := <-values:
		t.Fatalf("duplicate callback = %q", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	stopTransactionalCallbackTestManager(t, manager)
	if remaining, commits, leased := source.snapshot(); remaining != 0 || commits != 3 || leased {
		t.Fatalf("FIFO source state = remaining %d commits %d leased %v", remaining, commits, leased)
	}
}

func TestBufferCapacitySignalCannotLoseFreeBetweenFailureAndWait(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 8, item: 8})
	held, err := store.allocate([]byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, allocationErr := store.allocateOrWait([]byte("next")); !errors.Is(allocationErr, errHandleAllocation) || changed == nil {
		t.Fatalf("capacity failure = changed %v err %v", changed != nil, allocationErr)
	} else {
		if err = store.free(held.BufferHandle); err != nil {
			t.Fatal(err)
		}
		select {
		case <-changed:
		case <-time.After(time.Second):
			t.Fatal("captured capacity generation missed intervening free")
		}
	}
}

func TestNativePollingCapacityRollbackPreservesExactHead(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	held, err := store.allocate([]byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	source := newCallbackTestSource("polled")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		descriptor BufferDescriptor
		err        error
	}, 1)
	go func() {
		descriptor, pollErr := reserveNativeDeliveryBufferFrom(ctx, store, func() (callbackReservation, error) {
			return source.reserve(ctx, "")
		})
		result <- struct {
			descriptor BufferDescriptor
			err        error
		}{descriptor: descriptor, err: pollErr}
	}()
	select {
	case <-source.rollbacks:
	case <-time.After(time.Second):
		t.Fatal("native poll did not restore head under buffer saturation")
	}
	if remaining, commits, leased := source.snapshot(); remaining != 1 || commits != 0 || leased {
		t.Fatalf("poll saturation state = remaining %d commits %d leased %v", remaining, commits, leased)
	}
	if err = store.free(held.BufferHandle); err != nil {
		t.Fatal(err)
	}
	var got struct {
		descriptor BufferDescriptor
		err        error
	}
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("native poll did not resume after capacity release")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	data := make([]byte, got.descriptor.ByteLength)
	if _, err = store.read(got.descriptor.BufferHandle, 0, data); err != nil || string(data) != "polled" {
		t.Fatalf("polled descriptor = %q, %v", data, err)
	}
	if err = store.free(got.descriptor.BufferHandle); err != nil {
		t.Fatal(err)
	}
	if remaining, commits, leased := source.snapshot(); remaining != 0 || commits != 1 || leased {
		t.Fatalf("poll committed state = remaining %d commits %d leased %v", remaining, commits, leased)
	}
}

func TestNativePollingCommitPanicReleasesDescriptorAndRestoresExactHead(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	source := newCallbackTestSource("prepared")
	rollbacks := 0
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = reserveNativeDeliveryBufferFrom(context.Background(), store, func() (callbackReservation, error) {
			reservation, err := source.reserve(context.Background(), "")
			if err != nil {
				return callbackReservation{}, err
			}
			originalRollback := reservation.rollback
			reservation.commit = func() error { panic("commit invariant") }
			reservation.rollback = func() error {
				rollbacks++
				return originalRollback()
			}
			return reservation, nil
		})
	}()
	if !panicked {
		t.Fatal("commit panic was not propagated to the native recovery boundary")
	}
	store.mu.RLock()
	liveCount, liveBytes := store.liveCount, store.liveBytes
	store.mu.RUnlock()
	if liveCount != 0 || liveBytes != 0 || rollbacks != 1 {
		t.Fatalf("commit panic cleanup = count %d bytes %d rollbacks %d", liveCount, liveBytes, rollbacks)
	}
	reservation, err := source.reserve(context.Background(), "")
	if err != nil || string(reservation.encoded) != "prepared" {
		t.Fatalf("poll fallback = %q, %v", reservation.encoded, err)
	}
	clear(reservation.encoded)
	if err = reservation.commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCallbackCommitPanicStopsManagerWithoutInvokeAndRestoresPollFallback(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 1, bytes: 16, item: 16})
	source := newCallbackTestSource("tracked")
	invoked := make(chan struct{}, 1)
	manager := newTransactionalCallbackTestManager(store, source, callbackKindEvent, func(BufferDescriptor) {
		invoked <- struct{}{}
	})
	manager.reserveEvent = func(ctx context.Context) (callbackReservation, error) {
		reservation, err := source.reserve(ctx, "message.received")
		if err != nil {
			return callbackReservation{}, err
		}
		reservation.commit = func() error { panic("commit invariant") }
		return reservation, nil
	}
	manager.start()
	select {
	case <-manager.done:
	case <-time.After(time.Second):
		t.Fatal("callback manager did not stop after internal commit panic")
	}
	select {
	case <-invoked:
		t.Fatal("host callback ran after delivery commit panic")
	default:
	}
	if remaining, commits, leased := source.snapshot(); remaining != 1 || commits != 0 || leased {
		t.Fatalf("callback panic source = remaining %d commits %d leased %v", remaining, commits, leased)
	}
	store.mu.RLock()
	liveCount, liveBytes := store.liveCount, store.liveBytes
	store.mu.RUnlock()
	if liveCount != 0 || liveBytes != 0 {
		t.Fatalf("callback panic retained count=%d bytes=%d", liveCount, liveBytes)
	}
	reservation, err := source.reserve(context.Background(), "")
	if err != nil || string(reservation.encoded) != "tracked" {
		t.Fatalf("callback poll fallback = %q, %v", reservation.encoded, err)
	}
	clear(reservation.encoded)
	if err = reservation.commit(); err != nil {
		t.Fatal(err)
	}
	stopTransactionalCallbackTestManager(t, manager)
}

func TestCallbackCommitPanicClosesAdmissionBeforeDrainingEveryPreparedJob(t *testing.T) {
	store := newBufferStoreWithLimits(bufferLimits{count: 3, bytes: 48, item: 16})
	manager := newCallbackManagerForLifecycleTest()
	manager.jobs = make(chan callbackJob, 2)
	manager.buffers = store
	invoked := make(chan struct{}, 1)
	manager.invoke = func(_ unsafe.Pointer, _, _ uint64, _ uint32, _ BufferDescriptor) {
		invoked <- struct{}{}
	}
	arrived := make(chan struct{})
	release := make(chan struct{})
	manager.beforeDispatch = func(callbackJob) {
		close(arrived)
		<-release
		manager.beforeDispatch = nil
	}

	descriptors := make([]BufferDescriptor, 3)
	for index, value := range []string{"panic", "queued-one", "queued-two"} {
		var err error
		descriptors[index], err = store.allocate([]byte(value))
		if err != nil {
			t.Fatal(err)
		}
	}
	rollbacks := [3]int{}
	manager.wg.Add(1)
	go manager.dispatch()
	manager.jobs <- callbackJob{
		descriptor: descriptors[0], commit: func() error { panic("commit invariant") },
		rollback: func() error { rollbacks[0]++; return nil },
	}
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("panicking job was not selected")
	}
	for index := 1; index < len(descriptors); index++ {
		owned := index
		manager.jobs <- callbackJob{
			descriptor: descriptors[index], commit: func() error { return nil },
			rollback: func() error { rollbacks[owned]++; return nil },
		}
	}
	close(release)
	done := make(chan struct{})
	go func() { manager.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not join after commit panic")
	}
	select {
	case <-invoked:
		t.Fatal("later callback invoked after terminal commit panic")
	default:
	}
	if rollbacks != [3]int{1, 1, 1} {
		t.Fatalf("rollback counts = %v", rollbacks)
	}
	store.mu.RLock()
	liveCount, liveBytes := store.liveCount, store.liveBytes
	store.mu.RUnlock()
	if liveCount != 0 || liveBytes != 0 {
		t.Fatalf("terminal drain retained count=%d bytes=%d", liveCount, liveBytes)
	}
	for _, descriptor := range descriptors {
		if err := store.free(descriptor.BufferHandle); !errors.Is(err, errInvalidABIHandle) {
			t.Fatalf("descriptor was not freed exactly once: %v", err)
		}
	}
	if manager.state.begin() {
		manager.state.end()
		t.Fatal("callback admission reopened after terminal commit panic")
	}
	if err := manager.stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
