package payload

import (
	"bytes"
	"errors"
	"testing"
)

func chunkTestStore(t testing.TB, maximumBytes, maximumPerHandle int64) *HandleStore {
	t.Helper()
	store, err := NewHandleStore(HandleLimits{
		MaximumHandles:    8,
		MaximumBytes:      maximumBytes,
		MaximumPerHandle:  maximumPerHandle,
		MaximumWriteBytes: 1 << 20,
		MaximumReadBytes:  1 << 20,
	}, &stageBUniqueReader{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestHandleOneByteWritesUseBoundedFixedBlocks(t *testing.T) {
	const writes = handleChunkBytes + 17
	store := chunkTestStore(t, 2*handleChunkBytes, 2*handleChunkBytes)
	defer store.Close()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	source := []byte{0x7b}
	for index := 0; index < writes; index++ {
		if err := store.Write(handle, source); err != nil {
			t.Fatalf("write %d: %v", index, err)
		}
	}
	source[0] = 0

	store.mu.RLock()
	entry := store.entries[handle]
	if len(entry.chunks) != 2 {
		store.mu.RUnlock()
		t.Fatalf("chunk count = %d; want 2 for %d one-byte writes", len(entry.chunks), writes)
	}
	if entry.chunkBytes != 2*handleChunkBytes || store.totalBytes != 2*handleChunkBytes || entry.canonicalBytes != writes {
		store.mu.RUnlock()
		t.Fatalf("accounting: chunks=%d total=%d canonical=%d", entry.chunkBytes, store.totalBytes, entry.canonicalBytes)
	}
	for index, part := range entry.chunks {
		if len(part.data) != cap(part.data) {
			store.mu.RUnlock()
			t.Fatalf("chunk %d has hidden capacity: len=%d cap=%d", index, len(part.data), cap(part.data))
		}
		if !bytes.Equal(part.data[:part.used], bytes.Repeat([]byte{0x7b}, part.used)) {
			store.mu.RUnlock()
			t.Fatalf("chunk %d aliases caller input", index)
		}
	}
	store.mu.RUnlock()
}

func TestHandleZeroWriteAndExactQuota(t *testing.T) {
	const maximum = int64(257)
	store := chunkTestStore(t, maximum, maximum)
	defer store.Close()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, nil); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	entry := store.entries[handle]
	if entry.state != HandleOpen || len(entry.chunks) != 0 || entry.chunkBytes != 0 || store.totalBytes != 0 {
		store.mu.RUnlock()
		t.Fatal("zero-length write allocated storage or changed state")
	}
	store.mu.RUnlock()

	input := bytes.Repeat([]byte{0xa5}, int(maximum))
	if err := store.Write(handle, input); err != nil {
		t.Fatalf("exact-quota write: %v", err)
	}
	input[0] = 0
	if err := store.Write(handle, []byte{1}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("quota + 1 error = %v", err)
	}
	store.mu.RLock()
	entry = store.entries[handle]
	if len(entry.chunks) != 1 || entry.chunkBytes != maximum || store.totalBytes != maximum || entry.canonicalBytes != maximum || entry.chunks[0].data[0] != 0xa5 {
		store.mu.RUnlock()
		t.Fatal("exact or rejected quota write changed owned storage/accounting")
	}
	store.mu.RUnlock()

	other, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(other, []byte{1}); !errors.Is(err, ErrHandleCapacity) {
		t.Fatalf("aggregate quota + 1 error = %v", err)
	}
}

func TestHandleFinishAccountsPeakAndZeroesChunks(t *testing.T) {
	canonical := stageBHandleCanonical(t, bytes.Repeat([]byte{0x31}, 2048))
	store := chunkTestStore(t, int64(len(canonical)), int64(len(canonical)))
	defer store.Close()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	input := clone(canonical)
	if err := store.Write(handle, input); err != nil {
		t.Fatal(err)
	}
	input[0] ^= 0xff
	store.mu.RLock()
	chunkAlias := store.entries[handle].chunks[0].data
	realValidate := store.validate
	store.mu.RUnlock()

	entered := make(chan struct{})
	release := make(chan struct{})
	store.validate = func(encoded []byte) error {
		close(entered)
		<-release
		return realValidate(encoded)
	}
	result := make(chan error, 1)
	go func() { result <- store.Finish(handle) }()
	<-entered
	store.mu.RLock()
	entry := store.entries[handle]
	if entry.state != HandleFinishing || store.totalBytes != int64(len(canonical)) || store.finishingBytes != int64(len(canonical)) {
		store.mu.RUnlock()
		t.Fatalf("peak accounting: state=%d stable=%d workspace=%d", entry.state, store.totalBytes, store.finishingBytes)
	}
	store.mu.RUnlock()
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunkAlias, make([]byte, len(chunkAlias))) {
		t.Fatal("successful Finish did not zero the replaced chunk")
	}
	value, _, err := store.Snapshot(handle)
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("completed snapshot differs from detached input: %v", err)
	}
	zero(value)
	store.mu.RLock()
	entry = store.entries[handle]
	if len(entry.chunks) != 0 || entry.chunkBytes != 0 || store.finishingBytes != 0 || store.totalBytes != int64(len(canonical)) {
		store.mu.RUnlock()
		t.Fatal("Finish did not commit exact stable accounting")
	}
	store.mu.RUnlock()
}

func TestHandleFailedFinishRollsBackAndClearsWorkspace(t *testing.T) {
	const maximum = int64(128)
	store := chunkTestStore(t, maximum, maximum)
	defer store.Close()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	malformed := []byte("private malformed canonical")
	if err := store.Write(handle, malformed); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	chunkAlias := store.entries[handle].chunks[0].data
	realValidate := store.validate
	stableBefore := store.totalBytes
	store.mu.RUnlock()
	var workspaceAlias []byte
	store.validate = func(encoded []byte) error {
		workspaceAlias = encoded
		return realValidate(encoded)
	}
	if err := store.Finish(handle); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("malformed Finish error = %v", err)
	}
	if !bytes.Equal(workspaceAlias, make([]byte, len(workspaceAlias))) {
		t.Fatal("failed Finish did not clear its materialized workspace")
	}
	if !bytes.Equal(chunkAlias[:len(malformed)], malformed) {
		t.Fatal("failed Finish changed the original chunks")
	}
	store.mu.RLock()
	entry := store.entries[handle]
	if entry.state != HandleWriting || entry.canonicalBytes != int64(len(malformed)) || store.totalBytes != stableBefore || store.finishingBytes != 0 {
		store.mu.RUnlock()
		t.Fatal("failed Finish did not restore state and accounting")
	}
	store.mu.RUnlock()

	store.mu.Lock()
	store.finishingBytes = maximum
	store.mu.Unlock()
	if err := store.Finish(handle); !errors.Is(err, ErrHandleCapacity) {
		t.Fatalf("unavailable Finish workspace error = %v", err)
	}
	store.mu.Lock()
	if store.entries[handle].state != HandleWriting || store.totalBytes != stableBefore || store.finishingBytes != maximum {
		store.mu.Unlock()
		t.Fatal("capacity-rejected Finish changed original state")
	}
	store.finishingBytes = 0
	store.mu.Unlock()
}

func TestHandleFinishPanicRollsBackAndCanRetry(t *testing.T) {
	canonical := stageBHandleCanonical(t, []byte("panic rollback"))
	store := chunkTestStore(t, int64(len(canonical)), int64(len(canonical)))
	defer store.Close()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	chunkAlias := store.entries[handle].chunks[0].data
	realValidate := store.validate
	store.mu.RUnlock()
	var workspaceAlias []byte
	panicValue := errors.New("validator panic")
	store.validate = func(encoded []byte) error {
		workspaceAlias = encoded
		panic(panicValue)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("recovered = %v; want sentinel", recovered)
			}
		}()
		_ = store.Finish(handle)
	}()
	if !bytes.Equal(workspaceAlias, make([]byte, len(workspaceAlias))) {
		t.Fatal("panicking Finish did not zero workspace")
	}
	if !bytes.Equal(chunkAlias[:len(canonical)], canonical) {
		t.Fatal("panicking Finish changed original chunks")
	}
	store.mu.RLock()
	entry := store.entries[handle]
	if entry.state != HandleWriting || store.finishingBytes != 0 || store.totalBytes != int64(len(canonical)) {
		store.mu.RUnlock()
		t.Fatal("panicking Finish did not roll back state/accounting")
	}
	store.mu.RUnlock()
	store.validate = realValidate
	if err := store.Finish(handle); err != nil {
		t.Fatalf("retry after panic: %v", err)
	}
}

func TestHandleCompletedStorageKeepsAggregateQuota(t *testing.T) {
	first := stageBHandleCanonical(t, []byte("first"))
	second := stageBHandleCanonical(t, []byte("second"))
	maximum := int64(2 * handleChunkBytes)
	store := chunkTestStore(t, maximum, maximum)
	defer store.Close()
	firstHandle := stageBCompletedHandle(t, store, first)
	secondHandle := stageBCompletedHandle(t, store, second)
	store.mu.RLock()
	if store.totalBytes != int64(len(first)+len(second)) || store.totalBytes > maximum || store.finishingBytes != 0 {
		store.mu.RUnlock()
		t.Fatalf("completed accounting: stable=%d workspace=%d maximum=%d", store.totalBytes, store.finishingBytes, maximum)
	}
	store.mu.RUnlock()
	if err := store.Release(firstHandle); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(secondHandle); err != nil {
		t.Fatal(err)
	}
}

func TestHandleReleaseAndCloseZeroEveryOwnedBuffer(t *testing.T) {
	canonical := stageBHandleCanonical(t, []byte("completed secret"))
	store := chunkTestStore(t, 2*handleChunkBytes, 2*handleChunkBytes)

	unfinished, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	unfinishedSecret := []byte("unfinished secret")
	if err := store.Write(unfinished, unfinishedSecret); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	unfinishedAlias := store.entries[unfinished].chunks[0].data
	store.mu.RUnlock()
	if err := store.Release(unfinished); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(unfinishedAlias, make([]byte, len(unfinishedAlias))) {
		t.Fatal("final Release did not zero unfinished chunk storage")
	}

	completed := stageBCompletedHandle(t, store, canonical)
	store.mu.RLock()
	completedAlias := store.entries[completed].data
	store.mu.RUnlock()
	if err := store.Release(completed); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(completedAlias, make([]byte, len(completedAlias))) {
		t.Fatal("final Release did not zero completed canonical storage")
	}

	closing, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(closing, []byte("close secret")); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	closeAlias := store.entries[closing].chunks[0].data
	store.mu.RUnlock()
	store.Close()
	if !bytes.Equal(closeAlias, make([]byte, len(closeAlias))) {
		t.Fatal("Close did not zero unfinished chunk storage")
	}
	store.mu.RLock()
	if len(store.entries) != 0 || store.totalBytes != 0 || store.finishingBytes != 0 {
		store.mu.RUnlock()
		t.Fatal("Close did not clear storage accounting")
	}
	store.mu.RUnlock()
}
