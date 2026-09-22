package payload

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestAcceptanceObjectIngress128DuplicateCancellationSingleFlight(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, canonical := newIngressFixture(t, 8<<10, time.Minute)
	const callers = 128
	contexts := make([]context.Context, callers)
	cancels := make([]context.CancelFunc, callers)
	for i := range contexts {
		contexts[i], cancels[i] = context.WithCancel(context.Background())
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	start := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := range callers {
		go func(index int) {
			ready.Done()
			<-start
			_, err := materializer.IngestObject(contexts[index], manifest, binding, privateRef)
			results <- err
		}(i)
	}
	ready.Wait()
	close(start)
	if call := <-store.started; call != 1 {
		t.Fatalf("first physical download = %d", call)
	}
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	for i := 0; i < callers/2; i++ {
		cancels[i]()
	}
	store.releases <- nil

	cancelled, completed := 0, 0
	for range callers {
		switch err := <-results; {
		case errors.Is(err, context.Canceled):
			cancelled++
		case err == nil:
			completed++
		default:
			t.Errorf("duplicate ingress result: %v", err)
		}
	}
	if cancelled != callers/2 || completed != callers/2 {
		t.Fatalf("cancelled=%d completed=%d", cancelled, completed)
	}
	store.mu.Lock()
	downloads := store.downloads
	store.mu.Unlock()
	if downloads != 1 {
		t.Fatalf("physical downloads = %d, want 1", downloads)
	}

	descriptor, err := referencedDescriptor(protocol.PayloadTransferReference, binding.Profile, manifest.TransferID, manifest.EncryptionRef, canonical)
	if err != nil {
		t.Fatal(err)
	}
	value, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("retained completion consume: len=%d err=%v", len(value), err)
	}
	reassembler.Close()
}

func TestAcceptanceObjectIngress128FailureCoalescesThenRetries(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 1024, time.Minute)
	const callers = 128
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			results <- err
		}()
	}
	close(start)
	if call := <-store.started; call != 1 {
		t.Fatalf("initial download = %d", call)
	}
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	store.releases <- errors.New("object canary: secret URL https://private.invalid/key")
	for range callers {
		if err := <-results; !errors.Is(err, ErrCarrierMaterialization) || bytes.Contains([]byte(err.Error()), []byte("private.invalid")) {
			t.Fatalf("coalesced normalized failure = %v", err)
		}
	}

	retry := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		retry <- err
	}()
	if call := <-store.started; call != 2 {
		t.Fatalf("retry download = %d", call)
	}
	store.releases <- nil
	if err := <-retry; err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	store.mu.Lock()
	downloads := store.downloads
	store.mu.Unlock()
	if downloads != 2 {
		t.Fatalf("physical downloads = %d, want exactly 2", downloads)
	}
	reassembler.Close()
}

func TestAcceptanceObjectIngress128WaitersShutdownPromptly(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 1024, time.Minute)
	const callers = 128
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			results <- err
		}()
	}
	<-store.started
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	closed := make(chan struct{})
	go func() {
		reassembler.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the shared object owner")
	}
	for range callers {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Errorf("shutdown waiter result = %v", err)
		}
	}
}

func TestAcceptanceAbortObjectAttemptCancelsOwnerButPreservesDirectAttempt(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 1024, time.Minute)
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		result <- err
	}()
	<-store.started
	if err := reassembler.Abort(manifest.TransferID, CarrierObjectUpload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("aborted object owner result = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		// Unblock a defective owner before failing so the test leaves no goroutine.
		store.releases <- errors.New("test cleanup")
		<-result
		reassembler.Close()
		t.Fatal("aborting an object attempt did not cancel its active download owner")
	}
	reassembler.mu.Lock()
	entry := reassembler.entries[manifest.TransferID]
	directAuthorized, objectAuthorized := false, false
	if entry != nil {
		_, directAuthorized = entry.carriers[CarrierDirectChunks]
		_, objectAuthorized = entry.carriers[CarrierObjectUpload]
	}
	reassembler.mu.Unlock()
	if entry == nil || !directAuthorized || objectAuthorized {
		t.Fatalf("abort damaged surviving carrier state: entry=%v direct=%v object=%v", entry != nil, directAuthorized, objectAuthorized)
	}
	reassembler.Close()
}
