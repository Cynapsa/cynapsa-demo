package payload

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func stageBHandleCanonical(t testing.TB, body []byte) []byte {
	t.Helper()
	serializer, err := NewSerializer(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := serializer.Serialize(model.Payload{Value: model.NativePayload{
		ContentType: "application/octet-stream",
		Path:        "/stage-b",
		Body:        clone(body),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func stageBHandleStore(t testing.TB, maximumHandles int, maximumBytes int64) *HandleStore {
	t.Helper()
	store, err := NewHandleStore(HandleLimits{
		MaximumHandles:    maximumHandles,
		MaximumBytes:      maximumBytes,
		MaximumPerHandle:  maximumBytes,
		MaximumWriteBytes: 1 << 20,
		MaximumReadBytes:  1 << 20,
	}, &stageBUniqueReader{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type stageBUniqueReader struct {
	mu   sync.Mutex
	next uint64
}

func (r *stageBUniqueReader) Read(output []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], r.next)
	digest := sha256.Sum256(seed[:])
	for offset := 0; offset < len(output); offset += len(digest) {
		copy(output[offset:], digest[:])
	}
	return len(output), nil
}

func stageBCompletedHandle(t testing.TB, store *HandleStore, canonical []byte) string {
	t.Helper()
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(handle); err != nil {
		t.Fatal(err)
	}
	return handle
}

func TestAcceptanceStageBSizeAndCompletedCancelAreObservationallyImmutable(t *testing.T) {
	canonical := stageBHandleCanonical(t, bytes.Repeat([]byte{0xa5}, 4096))
	store := stageBHandleStore(t, 4, int64(len(canonical))*2)
	defer store.Close()

	input := clone(canonical)
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(handle, input); err != nil {
		t.Fatal(err)
	}
	for index := range input {
		input[index] ^= 0xff
	}
	if err := store.Finish(handle); err != nil {
		t.Fatalf("finish after caller mutates write input: %v", err)
	}

	wantDigest := Digest(canonical)
	before, beforeDigest, err := store.Snapshot(handle)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, canonical) || beforeDigest != wantDigest {
		t.Fatal("write input aliases immutable handle content")
	}
	if size, err := store.Size(handle); err != nil || size != int64(len(canonical)) {
		t.Fatalf("initial size = %d, %v; want %d", size, err, len(canonical))
	}

	// A returned snapshot is caller-owned. Mutating it must not affect Size,
	// digest, or the immutable bytes retained by the store.
	for index := range before {
		before[index] ^= 0xff
	}
	if err := store.Retain(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatalf("first completed cancel: %v", err)
	}
	if err := store.Cancel(handle); err != nil {
		t.Fatalf("repeated completed cancel: %v", err)
	}

	after, afterDigest, err := store.Snapshot(handle)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, canonical) || afterDigest != wantDigest {
		t.Fatal("completed cancellation or caller snapshot mutation changed content")
	}
	if size, err := store.Size(handle); err != nil || size != int64(len(canonical)) {
		t.Fatalf("size after cancellation = %d, %v; want %d", size, err, len(canonical))
	}

	// Two cancels must not consume any of the three ownership references.
	for remaining := 2; remaining >= 0; remaining-- {
		if err := store.Release(handle); err != nil {
			t.Fatalf("release with %d retained references remaining: %v", remaining, err)
		}
		if remaining > 0 {
			if size, err := store.Size(handle); err != nil || size != int64(len(canonical)) {
				t.Fatalf("live size with %d references = %d, %v", remaining, size, err)
			}
		}
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("released size error = %v; want ErrInvalidHandle", err)
	}
}

func TestAcceptanceStageBCompletedCancelPreservesCapacityAccounting(t *testing.T) {
	canonical := stageBHandleCanonical(t, []byte("capacity remains charged"))
	store := stageBHandleStore(t, 2, int64(len(canonical)))
	defer store.Close()

	completed := stageBCompletedHandle(t, store, canonical)
	if err := store.Cancel(completed); err != nil {
		t.Fatal(err)
	}
	waiting, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(waiting, []byte{1}); !errors.Is(err, ErrHandleCapacity) {
		t.Fatalf("completed cancel released byte accounting: %v", err)
	}
	if err := store.Release(completed); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(waiting, canonical); err != nil {
		t.Fatalf("final release did not reclaim capacity: %v", err)
	}
	if err := store.Cancel(waiting); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptanceStageBSizeDoesNotMaterializeCompletedContent(t *testing.T) {
	small := stageBHandleCanonical(t, []byte{0x5a})
	large := stageBHandleCanonical(t, bytes.Repeat([]byte{0x5a}, 256<<10))
	store := stageBHandleStore(t, 2, int64(len(small)+len(large)+handleChunkBytes))
	defer store.Close()
	smallHandle := stageBCompletedHandle(t, store, small)
	largeHandle := stageBCompletedHandle(t, store, large)

	var gotSize int64
	var gotErr error
	smallAllocations := testing.AllocsPerRun(1000, func() {
		gotSize, gotErr = store.Size(smallHandle)
	})
	if gotErr != nil || gotSize != int64(len(small)) {
		t.Fatalf("small size = %d, %v; want %d", gotSize, gotErr, len(small))
	}
	largeAllocations := testing.AllocsPerRun(1000, func() {
		gotSize, gotErr = store.Size(largeHandle)
	})
	if gotErr != nil || gotSize != int64(len(large)) {
		t.Fatalf("large size = %d, %v; want %d", gotSize, gotErr, len(large))
	}
	if largeAllocations > smallAllocations {
		t.Fatalf("Size allocations scale with content: small %.2f, large %.2f", smallAllocations, largeAllocations)
	}
}

func TestAcceptanceStageBSizeAndCancelStateErrorMatrix(t *testing.T) {
	canonical := stageBHandleCanonical(t, []byte("state matrix"))
	store := stageBHandleStore(t, 8, 1<<20)
	defer store.Close()
	foreignStore := stageBHandleStore(t, 2, 1<<20)
	defer foreignStore.Close()

	var nilStore *HandleStore
	if _, err := nilStore.Size("anything"); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("nil store size error = %v", err)
	}
	if err := nilStore.Cancel("anything"); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("nil store cancel error = %v", err)
	}
	for _, malformed := range []string{"", "payh_", "payh_not/base64", "other_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
		if _, err := store.Size(malformed); !errors.Is(err, ErrInvalidHandle) {
			t.Errorf("Size(%q) error = %v", malformed, err)
		}
		if err := store.Cancel(malformed); !errors.Is(err, ErrInvalidHandle) {
			t.Errorf("Cancel(%q) error = %v", malformed, err)
		}
	}

	open, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(open); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("open size error = %v", err)
	}
	if err := store.Write(open, canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(open); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("writing size error = %v", err)
	}

	// Finishing is deliberately an internal transient state. This narrow
	// observation covers the state that cannot be deterministically held via
	// the public methods without timing-dependent tests.
	store.mu.Lock()
	store.entries[open].state = HandleFinishing
	store.mu.Unlock()
	if _, err := store.Size(open); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finishing size error = %v", err)
	}
	if err := store.Cancel(open); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("finishing cancel error = %v", err)
	}
	store.mu.Lock()
	store.entries[open].state = HandleWriting
	store.mu.Unlock()

	if err := store.Cancel(open); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(open); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("cancelled size error = %v", err)
	}
	if err := store.Cancel(open); err != nil {
		t.Fatalf("repeated cancelled cancel: %v", err)
	}
	if err := store.Release(open); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(open); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("released size error = %v", err)
	}
	if err := store.Cancel(open); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("released cancel error = %v", err)
	}

	foreign, err := foreignStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size(foreign); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("foreign size error = %v", err)
	}
	if err := store.Cancel(foreign); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("foreign cancel error = %v", err)
	}
	missing := payloadHandlePrefix + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0xee}, payloadHandleBytes))
	if _, err := store.Size(missing); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("missing size error = %v", err)
	}
	if err := store.Cancel(missing); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("missing cancel error = %v", err)
	}

	completed := stageBCompletedHandle(t, store, canonical)
	if size, err := store.Size(completed); err != nil || size != int64(len(canonical)) {
		t.Fatalf("completed size = %d, %v", size, err)
	}
	if err := store.Cancel(completed); err != nil {
		t.Fatalf("completed cancel: %v", err)
	}

	closedStore := stageBHandleStore(t, 1, 1<<20)
	closedHandle := stageBCompletedHandle(t, closedStore, canonical)
	closedStore.Close()
	if _, err := closedStore.Size(closedHandle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("closed store size error = %v", err)
	}
	if err := closedStore.Cancel(closedHandle); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("closed store cancel error = %v", err)
	}
}

func TestAcceptanceStageBCancelZeroizesAndReclaimsUnfinishedStorage(t *testing.T) {
	store := stageBHandleStore(t, 4, 1<<20)
	defer store.Close()

	open, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Cancel(open); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	openEntry := store.entries[open]
	if openEntry.state != HandleCancelled || openEntry.data != nil || store.totalBytes != 0 {
		store.mu.RUnlock()
		t.Fatal("open cancellation did not leave an empty cancelled entry")
	}
	store.mu.RUnlock()

	writing, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	secret := bytes.Repeat([]byte("private-stage-b-canary"), 256)
	if err := store.Write(writing, secret); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	owned := store.entries[writing].chunks[0].data
	charged := store.entries[writing].chunkBytes
	if store.totalBytes != charged || charged < int64(len(secret)) || charged > handleChunkBytes {
		store.mu.RUnlock()
		t.Fatalf("charged bytes = %d; want one exact bounded block covering %d bytes", store.totalBytes, len(secret))
	}
	store.mu.RUnlock()
	if err := store.Cancel(writing); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, make([]byte, len(owned))) {
		t.Fatal("writing cancellation did not zero every owned byte")
	}
	store.mu.RLock()
	writingEntry := store.entries[writing]
	if writingEntry.state != HandleCancelled || writingEntry.data != nil || store.totalBytes != 0 {
		store.mu.RUnlock()
		t.Fatal("writing cancellation did not release storage accounting")
	}
	store.mu.RUnlock()

	if err := store.Cancel(writing); err != nil {
		t.Fatalf("repeated writing cancellation: %v", err)
	}
	if err := store.Release(open); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(writing); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptanceStageBCompletedHandleHighContention(t *testing.T) {
	const contenders = 256
	canonical := stageBHandleCanonical(t, bytes.Repeat([]byte("immutable"), 128))
	store := stageBHandleStore(t, 2, 1<<20)
	defer store.Close()
	handle := stageBCompletedHandle(t, store, canonical)
	wantDigest := Digest(canonical)

	for index := 0; index < contenders; index++ {
		if err := store.Retain(handle); err != nil {
			t.Fatalf("pre-retain %d: %v", index, err)
		}
	}
	start := make(chan struct{})
	errs := make(chan error, contenders*5)
	var wait sync.WaitGroup
	wait.Add(contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wait.Done()
			<-start
			if size, err := store.Size(handle); err != nil || size != int64(len(canonical)) {
				errs <- fmt.Errorf("contender %d size = %d, %w", index, size, err)
			}
			if err := store.Cancel(handle); err != nil {
				errs <- fmt.Errorf("contender %d cancel: %w", index, err)
			}
			value, digest, err := store.Snapshot(handle)
			if err != nil {
				errs <- fmt.Errorf("contender %d snapshot: %w", index, err)
			} else if !bytes.Equal(value, canonical) || digest != wantDigest {
				errs <- fmt.Errorf("contender %d observed mutated content", index)
			}
			zero(value)
			if err := store.Retain(handle); err != nil {
				errs <- fmt.Errorf("contender %d retain: %w", index, err)
			} else if err := store.Release(handle); err != nil {
				errs <- fmt.Errorf("contender %d paired release: %w", index, err)
			}
			if err := store.Release(handle); err != nil {
				errs <- fmt.Errorf("contender %d ownership release: %w", index, err)
			}
		}(index)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	value, digest, err := store.Snapshot(handle)
	if err != nil || !bytes.Equal(value, canonical) || digest != wantDigest {
		t.Fatalf("final immutable snapshot: digest=%x err=%v", digest, err)
	}
	zero(value)
	if err := store.Release(handle); err != nil {
		t.Fatalf("final baseline release: %v", err)
	}
	if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("post-contention released size error = %v", err)
	}
}

func TestAcceptanceStageBFinishSizeCancelRetainReleaseCloseContention(t *testing.T) {
	const handleCount = 64
	canonical := stageBHandleCanonical(t, bytes.Repeat([]byte("race"), 256))
	store := stageBHandleStore(t, handleCount, int64(handleCount*handleChunkBytes))

	handles := make([]string, 0, handleCount)
	aliases := make([][]byte, 0, handleCount)
	for index := 0; index < handleCount; index++ {
		handle, err := store.Open()
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Write(handle, canonical); err != nil {
			t.Fatal(err)
		}
		store.mu.RLock()
		for _, part := range store.entries[handle].chunks {
			aliases = append(aliases, part.data)
		}
		store.mu.RUnlock()
		handles = append(handles, handle)
	}

	start := make(chan struct{})
	errs := make(chan error, handleCount*8)
	var wait sync.WaitGroup
	for _, handle := range handles {
		handle := handle
		for operation := 0; operation < 5; operation++ {
			operation := operation
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				switch operation {
				case 0:
					if err := store.Finish(handle); !stageBExpectedRaceError(err) {
						errs <- fmt.Errorf("finish: %w", err)
					}
				case 1:
					if size, err := store.Size(handle); err == nil {
						if size != int64(len(canonical)) {
							errs <- fmt.Errorf("successful size = %d", size)
						}
					} else if !stageBExpectedRaceError(err) {
						errs <- fmt.Errorf("size: %w", err)
					}
				case 2:
					if err := store.Cancel(handle); !stageBExpectedRaceError(err) {
						errs <- fmt.Errorf("cancel: %w", err)
					}
				case 3:
					if err := store.Retain(handle); !stageBExpectedRaceError(err) {
						errs <- fmt.Errorf("retain: %w", err)
					}
				case 4:
					if err := store.Release(handle); !stageBExpectedRaceError(err) {
						errs <- fmt.Errorf("release: %w", err)
					}
				}
			}()
		}
	}
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			store.Close()
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for index, alias := range aliases {
		if !bytes.Equal(alias, make([]byte, len(alias))) {
			t.Errorf("handle %d retained non-zero bytes after terminal cleanup", index)
		}
	}
	for _, handle := range handles {
		if _, err := store.Size(handle); !errors.Is(err, ErrInvalidHandleState) {
			t.Errorf("closed store size error = %v", err)
		}
		if err := store.Cancel(handle); !errors.Is(err, ErrInvalidHandleState) {
			t.Errorf("closed store cancel error = %v", err)
		}
	}
}

func stageBExpectedRaceError(err error) bool {
	return err == nil || errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrInvalidHandleState)
}
