package payload

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func testLimits() Limits {
	return Limits{InlineBytes: 8, MaximumPayloadBytes: 1024, ChunkBytes: 8, MaximumFrameBytes: 4096, MaximumChunks: 256, InFlightChunks: 2, TransfersPerPeer: 2, MaximumTransfers: 4, ReassemblyBytesPerPeer: 128, ReassemblyBytes: 256, TransferLifetime: time.Minute, CleanupTimeout: 100 * time.Millisecond, WorkerCount: 2, WorkerQueue: 4}
}
func testBinding(id string) TransferBinding {
	return TransferBinding{TransferID: id, MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh", SenderID: "sender", RecipientID: "recipient", Profile: "aztm.native"}
}
func testCanonical(body string) []byte {
	serializer, _ := NewSerializer(1024)
	value, _ := serializer.Serialize(model.Payload{Value: model.NativePayload{Path: "/", Body: []byte(body)}})
	return value
}

func buildTestTransfer(t *testing.T, canonical, transferred []byte, chunkSize int) (TransferBinding, TransferManifest, []TransferChunk) {
	t.Helper()
	id, err := NewTransferID(bytes.NewReader(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	binding := testBinding(id)
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	encryptionRef := ""
	if len(transferred) != len(canonical) {
		encryptionRef = "enc1_AQ"
	}
	manifest, err := BuildManifest(binding, canonical, transferred, chunkSize, encryptionRef, time.Unix(1000, 0).UTC(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []TransferChunk
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		chunks = append(chunks, TransferChunk{chunk.TransferID, chunk.Index, chunk.Offset, clone(chunk.Bytes), clone(chunk.Digest)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return binding, manifest, chunks
}

func TestManifestChunkEncodingAndValidation(t *testing.T) {
	t.Parallel()
	canonical := testCanonical("canonical-payload")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 64
	now := time.Unix(950, 0).UTC()
	if err := ValidateTransferManifest(manifest, binding, limits, now); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !manifestsEqual(manifest, decoded) {
		t.Fatal("manifest round trip")
	}
	for _, chunk := range chunks {
		if err := ValidateTransferChunk(manifest, chunk); err != nil {
			t.Fatal(err)
		}
		text, err := EncodeTextChunk(chunk)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeTextChunk(text, 5)
		if err != nil || !reflect.DeepEqual(chunk, decoded) {
			t.Fatalf("chunk round trip: %v", err)
		}
	}
	bad := chunks[0]
	bad.Bytes = clone(bad.Bytes)
	bad.Bytes[0] ^= 1
	if err := ValidateTransferChunk(manifest, bad); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("tamper: %v", err)
	}
	wrong := binding
	wrong.SenderID = "other"
	if err := ValidateTransferManifest(manifest, wrong, limits, now); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("binding: %v", err)
	}
	if err := ValidateTransferManifest(manifest, binding, limits, manifest.ExpiresAt); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expiry: %v", err)
	}
}

func TestReassemblerOutOfOrderDuplicateConflictAndQuota(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("canonical-payload")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 64
	limits.TransfersPerPeer = 1
	r, err := NewReassembler(limits, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	for i := len(chunks) - 1; i >= 0; i-- {
		if err := r.Accept(CarrierDirectChunks, chunks[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Accept(CarrierDirectChunks, chunks[0]); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	conflict := chunks[0]
	conflict.Bytes = clone(conflict.Bytes)
	conflict.Bytes[0] ^= 1
	conflictDigest := Digest(conflict.Bytes)
	conflict.Digest = clone(conflictDigest[:])
	if err := r.Accept(CarrierDirectChunks, conflict); !errors.Is(err, ErrChunkConflict) {
		t.Fatalf("conflict: %v", err)
	}
	evidence, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks)
	if err != nil || evidence.Digest != binding.CanonicalDigest {
		t.Fatalf("complete %#v %v", evidence, err)
	}
	consumed, err := r.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(consumed, canonical) {
		t.Fatalf("consume %q %v", consumed, err)
	}
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("stale complete: %v", err)
	}

	_, manifest2, _ := buildTestTransfer(t, canonical, canonical, 5)
	manifest2.TransferID = "xfer_AQEBAQEBAQEBAQEBAQEBAQ"
	binding2 := testBinding(manifest2.TransferID)
	binding2.CanonicalSize = manifest2.CanonicalSize
	copy(binding2.CanonicalDigest[:], manifest2.CanonicalDigest)
	manifest2.MessageID = binding2.MessageID
	if err := r.Begin(manifest2, binding2, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	manifest3 := cloneManifest(manifest2)
	manifest3.TransferID = "xfer_AgICAgICAgICAgICAgICAg"
	binding3 := testBinding(manifest3.TransferID)
	binding3.CanonicalSize = manifest3.CanonicalSize
	copy(binding3.CanonicalDigest[:], manifest3.CanonicalDigest)
	if err := r.Begin(manifest3, binding3, CarrierDirectChunks); !errors.Is(err, ErrReassemblyQuota) {
		t.Fatalf("peer quota: %v", err)
	}
	if err := r.Abort(manifest2.TransferID, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	if err := r.Abort(manifest2.TransferID, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
}

func TestReassemblerIntegrityFailureAndExpiry(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("canonical-payload")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 64
	r, _ := NewReassembler(limits, nil, func() time.Time { return now })
	_ = r.Begin(manifest, binding, CarrierDirectChunks)
	for _, chunk := range chunks {
		_ = r.Accept(CarrierDirectChunks, chunk)
	}
	r.mu.Lock()
	r.entries[manifest.TransferID].data[0] ^= 1
	r.mu.Unlock()
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("integrity: %v", err)
	}
	binding, manifest, _ = buildTestTransfer(t, canonical, canonical, 5)
	_ = r.Begin(manifest, binding, CarrierDirectChunks)
	if got := r.Expire(manifest.ExpiresAt); got != 1 {
		t.Fatalf("expired %d", got)
	}
}

type blockingCipher struct {
	started chan struct{}
	once    sync.Once
}

func (b *blockingCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}
func (b *blockingCipher) Decrypt(ctx context.Context, _ TransferBinding, _ []byte, _ string) ([]byte, error) {
	b.once.Do(func() { close(b.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestReassemblerCloseCancelsActiveMaterialization(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("blocking")
	transferred := make([]byte, len(canonical)+encryptionFrameOverhead)
	binding, manifest, chunks := buildTestTransfer(t, canonical, transferred, 5)
	manifest.EncryptionRef = "enc1_AQ"
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	cipher := &blockingCipher{started: make(chan struct{})}
	r, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if err := r.Accept(CarrierDirectChunks, chunk); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks)
		done <- err
	}()
	<-cipher.started
	r.Close()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("complete %v", err)
	}
}

func TestReassemblerAbortCancelsActiveMaterialization(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("blocking abort")
	transferred := make([]byte, len(canonical)+encryptionFrameOverhead)
	binding, manifest, chunks := buildTestTransfer(t, canonical, transferred, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	cipher := &blockingCipher{started: make(chan struct{})}
	r, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		_ = r.Accept(CarrierMessageChunks, chunk)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
		done <- err
	}()
	<-cipher.started
	if err := r.Abort(manifest.TransferID, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("complete %v", err)
	}
	if _, err := r.Consume(manifest.TransferID, binding); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("aborted transfer retained: %v", err)
	}
}

func TestReassemblerBindsCarrierAndAcceptsTransportProtectedPlaintext(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("carrier-binding")
	binding, manifest, _ := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 64
	r, _ := NewReassembler(limits, nil, func() time.Time { return now })
	manifest.EncryptionRef = "enc1_not*canonical"
	if err := r.Begin(manifest, binding, CarrierDirectChunks); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("malformed encryption ref: %v", err)
	}
	r.mu.Lock()
	if r.totalBytes != 0 || len(r.entries) != 0 {
		t.Fatal("rejected manifest reserved quota")
	}
	r.mu.Unlock()
	manifest.EncryptionRef = ""
	transferred := clone(canonical)
	manifest, _ = BuildManifest(binding, canonical, transferred, 5, "", time.Unix(1000, 0).UTC(), nil)
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	var first TransferChunk
	stop := errors.New("captured")
	_ = ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		first = TransferChunk{TransferID: chunk.TransferID, Index: chunk.Index, Offset: chunk.Offset, Bytes: clone(chunk.Bytes), Digest: clone(chunk.Digest)}
		return stop
	})
	if err := r.Accept(CarrierDirectChunks, first); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("cross-carrier chunk: %v", err)
	}
	if err := r.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatalf("authorized carrier transition: %v", err)
	}
	if err := r.Accept(CarrierDirectChunks, first); err != nil {
		t.Fatalf("authorized direct chunk: %v", err)
	}
}

type partialDecryptCipher struct{ output []byte }

func (*partialDecryptCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}
func (p *partialDecryptCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	p.output = []byte("sensitive partial plaintext")
	return p.output, errors.Join(errors.New("PRIVATE_DECRYPT_CANARY"), context.DeadlineExceeded)
}

func TestReassemblerZerosPartialDecryptOutputAndPreservesDeadline(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("expected")
	transferred := make([]byte, len(canonical)+encryptionFrameOverhead)
	binding, manifest, chunks := buildTestTransfer(t, canonical, transferred, 5)
	manifest.EncryptionRef = "enc1_AQ"
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	cipher := &partialDecryptCipher{}
	r, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if err := r.Accept(CarrierMessageChunks, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks); !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("deadline %q", err)
	}
	if !bytes.Equal(cipher.output, make([]byte, len(cipher.output))) {
		t.Fatalf("partial plaintext retained: %q", cipher.output)
	}
	if _, err := r.Consume(manifest.TransferID, binding); !errors.Is(err, ErrTransferIncomplete) {
		t.Fatalf("failed completion retained: %v", err)
	}
	_ = r.Abort(manifest.TransferID, CarrierMessageChunks)
}

func TestReassemblerCompletedDataRequiresMatchingEnvelopeAndExpires(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("retained")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 64
	r, _ := NewReassembler(limits, nil, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		_ = r.Accept(CarrierDirectChunks, chunk)
	}
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	wrong := binding
	wrong.RecipientID = "other"
	if _, err := r.Consume(manifest.TransferID, wrong); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong envelope binding: %v", err)
	}
	if count := r.Expire(manifest.ExpiresAt); count != 1 {
		t.Fatalf("expired completed transfers %d", count)
	}
	if _, err := r.Consume(manifest.TransferID, binding); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("expired consume: %v", err)
	}
}

func TestReassemblerDirectPartialToXMPPAndLateDirectAmbiguity(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("multi-carrier fallback")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	transferred, ref, _ := fakeCipher{}.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, transferred, 5, ref, time.Unix(1000, 0).UTC(), nil)
	var chunks []TransferChunk
	_ = ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		chunks = append(chunks, TransferChunk{TransferID: chunk.TransferID, Index: chunk.Index, Offset: chunk.Offset, Bytes: clone(chunk.Bytes), Digest: clone(chunk.Digest)})
		return nil
	})
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	r, _ := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	if err := r.Accept(CarrierDirectChunks, chunks[0]); err != nil {
		t.Fatal(err)
	}
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if err := r.Accept(CarrierMessageChunks, chunk); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
	if err != nil || evidence.Digest != binding.CanonicalDigest {
		t.Fatalf("complete %#v %v", evidence, err)
	}
	if err := r.Accept(CarrierDirectChunks, chunks[len(chunks)-1]); !errors.Is(err, ErrTransferCompleted) {
		t.Fatalf("late direct chunk: %v", err)
	}
	late, err := r.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks)
	if err != nil || late != evidence {
		t.Fatalf("late completion %#v %v", late, err)
	}
	consumed, err := r.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(consumed, canonical) {
		t.Fatalf("consume %q %v", consumed, err)
	}
}

func TestReassemblerConcurrentCarrierAdmission(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("concurrent carriers")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	transferred, ref, _ := fakeCipher{}.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, transferred, 8, ref, time.Unix(1000, 0).UTC(), nil)
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	r, _ := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		carrier := CarrierDirectChunks
		if i%2 == 1 {
			carrier = CarrierMessageChunks
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- r.Begin(manifest, binding, carrier)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r.mu.Lock()
	entry := r.entries[manifest.TransferID]
	if entry == nil || len(entry.carriers) != 2 || r.peerTransfers[binding.SenderID] != 1 {
		t.Fatalf("entry %#v peer transfers %d", entry, r.peerTransfers[binding.SenderID])
	}
	r.mu.Unlock()
}

func TestReassemblerLateObjectCandidateIsIdempotentAfterRetainedCompletion(t *testing.T) {
	for _, test := range []struct {
		name    string
		carrier CarrierKind
	}{{"direct", CarrierDirectChunks}, {"message", CarrierMessageChunks}} {
		t.Run(test.name, func(t *testing.T) {
			carrier := test.carrier
			now := time.Unix(950, 0).UTC()
			canonical := testCanonical("late object candidate")
			binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
			binding.CanonicalSize = int64(len(canonical))
			binding.CanonicalDigest = Digest(canonical)
			ciphertext, ref, _ := fakeCipher{}.Encrypt(context.Background(), binding, canonical)
			manifest, _ := BuildManifest(binding, canonical, ciphertext, 8, ref, time.Unix(1000, 0).UTC(), nil)
			limits := testLimits()
			limits.ReassemblyBytesPerPeer = 128
			r, _ := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
			if err := r.Begin(manifest, binding, carrier); err != nil {
				t.Fatal(err)
			}
			if err := ForEachChunk(manifest, ciphertext, func(chunk TransferChunk) error { return r.Accept(carrier, chunk) }); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Complete(context.Background(), manifest.TransferID, carrier); err != nil {
				t.Fatal(err)
			}
			if err := r.Begin(manifest, binding, CarrierObjectUpload); err != nil {
				t.Fatal(err)
			}
			candidate := clone(ciphertext)
			if err := r.CompleteObject(context.Background(), manifest.TransferID, candidate); err != nil {
				t.Fatalf("late object candidate %v", err)
			}
			if !bytes.Equal(candidate, make([]byte, len(candidate))) {
				t.Fatal("late candidate not zeroed")
			}
			r.mu.Lock()
			if r.peerTransfers[binding.SenderID] != 1 || r.totalBytes != manifest.CanonicalSize || len(r.entries) != 1 {
				t.Fatalf("retained quota peer=%d bytes=%d entries=%d", r.peerTransfers[binding.SenderID], r.totalBytes, len(r.entries))
			}
			r.mu.Unlock()
			value, err := r.Consume(manifest.TransferID, binding)
			if err != nil || !bytes.Equal(value, canonical) {
				t.Fatalf("retained completion %q %v", value, err)
			}
		})
	}
}

func TestReassemblerAbortedObjectTokenCannotUseRetainedIdempotency(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("aborted late object")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	ciphertext, ref, _ := fakeCipher{}.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, ciphertext, 8, ref, time.Unix(1000, 0).UTC(), nil)
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = 128
	r, _ := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	_ = ForEachChunk(manifest, ciphertext, func(chunk TransferChunk) error { return r.Accept(CarrierMessageChunks, chunk) })
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := r.Begin(manifest, binding, CarrierObjectUpload); err != nil {
		t.Fatal(err)
	}
	_, cleanup, finish, err := r.beginObjectOperation(manifest, binding)
	if err != nil {
		t.Fatal(err)
	}
	aborted := make(chan error, 1)
	go func() { aborted <- r.Abort(manifest.TransferID, CarrierObjectUpload) }()
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		entry := r.entries[manifest.TransferID]
		won := entry != nil && entry.objectAborted
		r.mu.Unlock()
		if won {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("abort did not invalidate object token")
		}
		time.Sleep(time.Millisecond)
	}
	cleanup()
	finish()
	if err := <-aborted; err != nil {
		t.Fatal(err)
	}
	candidate := clone(ciphertext)
	if err := r.CompleteObject(context.Background(), manifest.TransferID, candidate); !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted token idempotency %v", err)
	}
	if !bytes.Equal(candidate, make([]byte, len(candidate))) {
		t.Fatal("aborted candidate not zeroed")
	}
	if _, completed, err := r.Completion(manifest.TransferID, binding); err != nil || !completed {
		t.Fatalf("original retained completion changed: completed=%v err=%v", completed, err)
	}
	conflict := cloneManifest(manifest)
	conflict.EncryptionRef = "enc1_Ag"
	if err := r.Begin(conflict, binding, CarrierObjectUpload); !errors.Is(err, ErrTransferExists) {
		t.Fatalf("conflicting manifest %v", err)
	}
}
