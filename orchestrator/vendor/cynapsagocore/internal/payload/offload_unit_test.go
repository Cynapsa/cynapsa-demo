package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type ingressObjectStore struct {
	mu        sync.Mutex
	value     []byte
	downloads int
	started   chan int
	releases  chan error
}

type beforeCommitCipher struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	output  []byte
}

func (*beforeCommitCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}
func (c *beforeCommitCipher) Decrypt(ctx context.Context, binding TransferBinding, value []byte, ref string) ([]byte, error) {
	output, err := (fakeCipher{}).Decrypt(ctx, binding, value, ref)
	if err != nil {
		return nil, err
	}
	c.output = output
	c.once.Do(func() { close(c.started) })
	<-c.release
	return output, nil
}

func (*ingressObjectStore) Available(context.Context) (bool, error) { return true, nil }
func (*ingressObjectStore) Upload(context.Context, []byte) (string, error) {
	return "", ErrCarrierUpload
}
func (s *ingressObjectStore) Download(ctx context.Context, _ string) ([]byte, error) {
	s.mu.Lock()
	s.downloads++
	call := s.downloads
	s.mu.Unlock()
	select {
	case s.started <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case err := <-s.releases:
		if err != nil {
			return nil, err
		}
		return clone(s.value), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newIngressFixture(t *testing.T, bodyBytes int, lifetime time.Duration) (*Materializer, *Reassembler, *ingressObjectStore, TransferManifest, TransferBinding, string, []byte) {
	t.Helper()
	maximum := int64(bodyBytes + 4096)
	serializer, err := NewSerializer(maximum)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := serializer.Serialize(model.Payload{Value: model.NativePayload{Path: "/", Body: bytes.Repeat([]byte{'p'}, bodyBytes)}})
	if err != nil {
		t.Fatal(err)
	}
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	ciphertext, ref, _ := cipher.Encrypt(context.Background(), binding, canonical)
	now := timeAt950()
	limits := testLimits()
	limits.MaximumPayloadBytes = maximum
	limits.ReassemblyBytesPerPeer = int64(len(ciphertext) + len(canonical))
	limits.ReassemblyBytes = int64(len(ciphertext) + len(canonical))
	limits.TransferLifetime = lifetime
	limits.ChunkBytes = 1 << 20
	limits.MaximumFrameBytes = 2 << 20
	manifest, err := BuildManifest(binding, canonical, ciphertext, limits.ChunkBytes, ref, now.Add(lifetime), nil)
	if err != nil {
		t.Fatal(err)
	}
	store := &ingressObjectStore{value: clone(ciphertext), started: make(chan int, 64), releases: make(chan error, 64)}
	reassembler, err := NewReassembler(limits, cipher, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(maximum, store, cipher, reassembler)
	if err != nil {
		t.Fatal(err)
	}
	privateRef, err := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	if err != nil {
		t.Fatal(err)
	}
	return materializer, reassembler, store, manifest, binding, privateRef, canonical
}

func waitForIngressWaiters(t *testing.T, materializer *Materializer, transferID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		materializer.ingressMu.Lock()
		flight := materializer.ingresses[transferID]
		got := 0
		if flight != nil {
			got = flight.waiters
		}
		materializer.ingressMu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ingress waiters %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForObjectAbort(t *testing.T, reassembler *Reassembler, transferID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		reassembler.mu.Lock()
		entry := reassembler.entries[transferID]
		aborted := entry != nil && entry.objectAborted
		reassembler.mu.Unlock()
		if aborted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("object attempt was not invalidated")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPackageInlineUsesCanonicalSize(t *testing.T) {
	t.Parallel()
	value := testCanonical("12345678")
	descriptor, err := PackageInline("aztm.native", value, int64(len(value)))
	if err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	materialized, err := descriptor.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	if materialized[0] == 'x' || !bytes.Equal(materialized, testCanonical("12345678")) {
		t.Fatal("descriptor retained caller bytes")
	}
	if _, err := PackageInline("aztm.native", append(value, 0), int64(len(value))); !errors.Is(err, ErrInlineLimitExceeded) {
		t.Fatalf("limit %v", err)
	}
}

func TestMaterializerInlineAndEncryptedObject(t *testing.T) {
	t.Parallel()
	canonical := testCanonical("canonical object")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	ciphertext, ref, err := cipher.Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	objects := &fakeObjects{available: true, value: ciphertext}
	privateRef, err := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	descriptor, err := protocol.NewReferencedPayload(protocol.PayloadObjectReference, "aztm.native", privateRef, int64(len(canonical)), digest, ref)
	if err != nil {
		t.Fatal(err)
	}
	materializer, _ := NewMaterializer(1024, objects, cipher, nil)
	result, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("materialize %q %v", result, err)
	}
	wrong := binding
	wrong.SenderID = "other"
	if _, err := materializer.Materialize(context.Background(), descriptor, wrong); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("binding %v", err)
	}
	inline, _ := protocol.NewInlinePayload("aztm.native", canonical)
	result, err = materializer.Materialize(context.Background(), inline, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("inline %v", err)
	}
}

func TestMaterializerV1PlaintextObject(t *testing.T) {
	t.Parallel()
	canonical := testCanonical("V1 server-visible object")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	objects := &fakeObjects{available: true, value: clone(canonical)}
	privateRef, err := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := protocol.NewReferencedPayload(protocol.PayloadObjectReference, "aztm.native", privateRef, int64(len(canonical)), sha256.Sum256(canonical), "")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(1024, objects, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("plaintext materialize %q %v", result, err)
	}
}

func TestEncryptedReassemblyAuthenticatesBeforeDelivery(t *testing.T) {
	now := timeAt950()
	canonical := testCanonical("canonical-payload")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	transferred, ref, _ := cipher.Encrypt(context.Background(), binding, canonical)
	manifest, err := BuildManifest(binding, canonical, transferred, 5, ref, now.Add(testLimits().TransferLifetime), nil)
	if err != nil {
		t.Fatal(err)
	}
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	r, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error { return r.Accept(CarrierMessageChunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	evidence, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
	if err != nil || evidence.Digest != binding.CanonicalDigest {
		t.Fatalf("complete %#v %v", evidence, err)
	}
	if _, err := r.Consume(manifest.TransferID, binding); err != nil {
		t.Fatal(err)
	}
	binding.TransferID = "xfer_AQEBAQEBAQEBAQEBAQEBAQ"
	manifest.TransferID = binding.TransferID
	manifest.EncryptionRef = "enc1_Ag"
	if err := r.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error { return r.Accept(CarrierMessageChunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong ref %v", err)
	}
}

func timeAt950() time.Time { return time.Unix(950, 0).UTC() }

type partialObjectStore struct{ output []byte }

func (*partialObjectStore) Available(context.Context) (bool, error) { return true, nil }
func (*partialObjectStore) Upload(context.Context, []byte) (string, error) {
	return "", ErrCarrierUpload
}

type borrowedObjectStore struct{ output []byte }

func (*borrowedObjectStore) Available(context.Context) (bool, error) { return true, nil }
func (*borrowedObjectStore) Upload(context.Context, []byte) (string, error) {
	return "", ErrCarrierUpload
}
func (b *borrowedObjectStore) Download(context.Context, string) ([]byte, error) { return b.output, nil }

type countingPayloadCipher struct{ decrypts int }

func (*countingPayloadCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}
func (c *countingPayloadCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	c.decrypts++
	return nil, ErrAuthentication
}
func (p *partialObjectStore) Download(context.Context, string) ([]byte, error) {
	p.output = []byte("sensitive partial object")
	return p.output, context.DeadlineExceeded
}

type canaryDownloadStore struct{ output []byte }

func (*canaryDownloadStore) Available(context.Context) (bool, error) { return true, nil }
func (*canaryDownloadStore) Upload(context.Context, []byte) (string, error) {
	return "", ErrCarrierUpload
}
func (s *canaryDownloadStore) Download(context.Context, string) ([]byte, error) {
	s.output = []byte("private object partial")
	return s.output, errors.Join(errors.New("PRIVATE_OBJECT_CANARY"), context.DeadlineExceeded)
}

func TestMaterializerZerosPartialObjectAndPreservesDeadline(t *testing.T) {
	canonical := testCanonical("object")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	reference, _ := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	descriptor, _ := referencedDescriptor(protocol.PayloadObjectReference, binding.Profile, reference, "enc1_AQ", canonical)
	objects := &partialObjectStore{}
	materializer, _ := NewMaterializer(1024, objects, fakeCipher{}, nil)
	if _, err := materializer.Materialize(context.Background(), descriptor, binding); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline %v", err)
	}
	if !bytes.Equal(objects.output, make([]byte, len(objects.output))) {
		t.Fatalf("partial object retained: %q", objects.output)
	}
}

func TestMaterializerNormalizesWrappedObjectDeadline(t *testing.T) {
	canonical := testCanonical("object canary")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	reference, _ := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	descriptor, _ := referencedDescriptor(protocol.PayloadObjectReference, binding.Profile, reference, "enc1_AQ", canonical)
	objects := &canaryDownloadStore{}
	materializer, _ := NewMaterializer(1024, objects, fakeCipher{}, nil)
	_, err := materializer.Materialize(context.Background(), descriptor, binding)
	if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("deadline %q", err)
	}
	if !bytes.Equal(objects.output, make([]byte, len(objects.output))) {
		t.Fatal("partial object not zeroed")
	}
}

func TestObjectMaterializerRejectsWrongGeometryBeforeDecrypt(t *testing.T) {
	canonical := testCanonical("object geometry")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	reference, _ := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	descriptor, _ := referencedDescriptor(protocol.PayloadObjectReference, binding.Profile, reference, "enc1_AQ", canonical)
	objects := &borrowedObjectStore{}
	cipher := &countingPayloadCipher{}
	materializer, _ := NewMaterializer(1024, objects, cipher, nil)
	for index, size := range []int{len(canonical) + encryptionFrameOverhead - 1, len(canonical) + encryptionFrameOverhead + 1, 1 << 20} {
		objects.output = bytes.Repeat([]byte{7}, size)
		if _, err := materializer.Materialize(context.Background(), descriptor, binding); !errors.Is(err, ErrIntegrity) {
			t.Errorf("attack %d: %v", index, err)
		}
		if !bytes.Equal(objects.output, make([]byte, size)) {
			t.Fatalf("attack %d output not zeroed", index)
		}
	}
	if cipher.decrypts != 0 {
		t.Fatalf("decrypt called %d times", cipher.decrypts)
	}
}

func TestTransferMaterializerConsumesRetainedCompletion(t *testing.T) {
	now := timeAt950()
	canonical := testCanonical("transfer materialization")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	transferred, ref, _ := cipher.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, transferred, 8, ref, now.Add(testLimits().TransferLifetime), nil)
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = 128
	reassembler, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := reassembler.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error { return reassembler.Accept(CarrierMessageChunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if _, err := reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	descriptor, _ := referencedDescriptor(protocol.PayloadTransferReference, binding.Profile, manifest.TransferID, ref, canonical)
	materializer, _ := NewMaterializer(1024, nil, cipher, reassembler)
	result, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("materialize %q %v", result, err)
	}
	if _, err := materializer.Materialize(context.Background(), descriptor, binding); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("second consume %v", err)
	}
}

func TestObjectReferenceRejectsOversizedRawURLBeforeEncoding(t *testing.T) {
	if _, err := encodeObjectReference("xfer_AAAAAAAAAAAAAAAAAAAAAA", "https://objects.example/"+strings.Repeat("x", protocol.MaxPrivateReferenceBytes)); !errors.Is(err, ErrInvalidManifest) && !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized URL %v", err)
	}
}

func TestObjectReferenceRejectsBase64IgnoredLineBreaksAndRequiresExactReencoding(t *testing.T) {
	reference, err := encodeObjectReference("xfer_AAAAAAAAAAAAAAAAAAAAAA", "https://objects.example/blob")
	if err != nil {
		t.Fatal(err)
	}
	for _, separator := range []string{"\r", "\n", "\r\n"} {
		mutated := reference[:len(objectReferencePrefix)+8] + separator + reference[len(objectReferencePrefix)+8:]
		if _, _, err := decodeObjectReference(mutated); err == nil {
			t.Fatalf("line break %q accepted in private reference", separator)
		}
	}
	decodedTransfer, decodedURL, err := decodeObjectReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := encodeObjectReference(decodedTransfer, decodedURL)
	if err != nil || reencoded != reference {
		t.Fatalf("canonical re-encode = (%q, %v)", reencoded, err)
	}
}

func TestObjectIngressRegistersOneCompletionForEnvelopeConsumption(t *testing.T) {
	now := timeAt950()
	canonical := testCanonical("object registry ingress")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	ciphertext, ref, _ := cipher.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, ciphertext, 8, ref, now.Add(testLimits().TransferLifetime), nil)
	objects := &fakeObjects{available: true, value: clone(ciphertext)}
	privateRef, _ := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = 128
	reassembler, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	materializer, _ := NewMaterializer(1024, objects, cipher, reassembler)
	evidence, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
	if err != nil || evidence.TransferID != binding.TransferID || evidence.MessageID != binding.MessageID || evidence.Digest != binding.CanonicalDigest {
		t.Fatalf("evidence %#v %v", evidence, err)
	}
	if _, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef); err != nil {
		t.Fatalf("idempotent ingress: %v", err)
	}
	if objects.downloads != 1 {
		t.Fatalf("downloads %d", objects.downloads)
	}
	descriptor, _ := referencedDescriptor(protocol.PayloadTransferReference, binding.Profile, binding.TransferID, ref, canonical)
	result, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("consume %q %v", result, err)
	}
}

func TestObjectIngressCompletesAfterPartialDirectAttempt(t *testing.T) {
	now := timeAt950()
	canonical := testCanonical("object wins carrier race")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	ciphertext, ref, _ := cipher.Encrypt(context.Background(), binding, canonical)
	manifest, _ := BuildManifest(binding, canonical, ciphertext, 8, ref, now.Add(testLimits().TransferLifetime), nil)
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = 128
	reassembler, _ := NewReassembler(limits, cipher, func() time.Time { return now })
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	var first TransferChunk
	if err := ForEachChunk(manifest, ciphertext, func(chunk TransferChunk) error {
		if first.Bytes == nil {
			first = TransferChunk{TransferID: chunk.TransferID, Index: chunk.Index, Offset: chunk.Offset, Bytes: clone(chunk.Bytes), Digest: clone(chunk.Digest)}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Accept(CarrierDirectChunks, first); err != nil {
		t.Fatal(err)
	}
	objects := &fakeObjects{available: true, value: clone(ciphertext)}
	privateRef, _ := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
	materializer, _ := NewMaterializer(1024, objects, cipher, reassembler)
	if _, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Accept(CarrierDirectChunks, first); !errors.Is(err, ErrTransferCompleted) {
		t.Fatalf("late direct chunk: %v", err)
	}
	descriptor, _ := referencedDescriptor(protocol.PayloadTransferReference, binding.Profile, binding.TransferID, ref, canonical)
	result, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(result, canonical) {
		t.Fatalf("consume %q %v", result, err)
	}
}

func TestObjectIngressConcurrentDuplicatesSingleFlight(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, canonical := newIngressFixture(t, 64<<10, time.Minute)
	const callers = 32
	start := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			ready.Done()
			<-start
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			results <- err
		}()
	}
	ready.Wait()
	close(start)
	if call := <-store.started; call != 1 {
		t.Fatalf("first download %d", call)
	}
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	store.mu.Lock()
	if store.downloads != 1 {
		t.Fatalf("duplicate downloads %d", store.downloads)
	}
	store.mu.Unlock()
	reassembler.mu.Lock()
	if len(reassembler.entries) != 1 || reassembler.peerTransfers[binding.SenderID] != 1 || reassembler.totalBytes != manifest.TransferredSize {
		t.Fatalf("quota entries=%d peer=%d bytes=%d", len(reassembler.entries), reassembler.peerTransfers[binding.SenderID], reassembler.totalBytes)
	}
	reassembler.mu.Unlock()
	store.releases <- nil
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	descriptor, _ := referencedDescriptor(protocol.PayloadTransferReference, binding.Profile, binding.TransferID, manifest.EncryptionRef, canonical)
	value, err := materializer.Materialize(context.Background(), descriptor, binding)
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("consume %d bytes: %v", len(value), err)
	}
}

func TestObjectIngressInitiatorCancellationDoesNotPoisonJoiner(t *testing.T) {
	materializer, _, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	initiatorCtx, cancelInitiator := context.WithCancel(context.Background())
	initiator := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(initiatorCtx, manifest, binding, privateRef)
		initiator <- err
	}()
	<-store.started
	joiner := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		joiner <- err
	}()
	waitForIngressWaiters(t, materializer, manifest.TransferID, 2)
	cancelInitiator()
	if err := <-initiator; !errors.Is(err, context.Canceled) {
		t.Fatalf("initiator cancellation %v", err)
	}
	store.releases <- nil
	if err := <-joiner; err != nil {
		t.Fatalf("joiner %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.downloads != 1 {
		t.Fatalf("downloads %d", store.downloads)
	}
}

func TestObjectIngressJoinerCancellationDoesNotPoisonOwner(t *testing.T) {
	materializer, _, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	owner := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		owner <- err
	}()
	<-store.started
	joinerCtx, cancelJoiner := context.WithCancel(context.Background())
	joiner := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(joinerCtx, manifest, binding, privateRef)
		joiner <- err
	}()
	waitForIngressWaiters(t, materializer, manifest.TransferID, 2)
	cancelJoiner()
	if err := <-joiner; !errors.Is(err, context.Canceled) {
		t.Fatalf("joiner cancellation %v", err)
	}
	store.releases <- nil
	if err := <-owner; err != nil {
		t.Fatalf("owner %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.downloads != 1 {
		t.Fatalf("downloads %d", store.downloads)
	}
}

func TestObjectIngressRejectsConflictingDuplicateReference(t *testing.T) {
	materializer, _, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	owner := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		owner <- err
	}()
	<-store.started
	conflict, _ := encodeObjectReference(binding.TransferID, "https://objects.example/other")
	if _, err := materializer.IngestObject(context.Background(), manifest, binding, conflict); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("conflicting reference %v", err)
	}
	store.releases <- nil
	if err := <-owner; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.downloads != 1 {
		t.Fatalf("downloads %d", store.downloads)
	}
}

func TestObjectIngressFailureRetryAndRetainedCompletion(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	const failedCallers = 8
	first := make(chan error, failedCallers)
	for i := 0; i < failedCallers; i++ {
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			first <- err
		}()
	}
	<-store.started
	waitForIngressWaiters(t, materializer, manifest.TransferID, failedCallers)
	store.releases <- ErrCarrierUpload
	for i := 0; i < failedCallers; i++ {
		if err := <-first; !errors.Is(err, ErrCarrierMaterialization) {
			t.Fatalf("coalesced failure %v", err)
		}
	}
	second := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		second <- err
	}()
	if call := <-store.started; call != 2 {
		t.Fatalf("retry download %d", call)
	}
	store.releases <- nil
	if err := <-second; err != nil {
		t.Fatalf("retry %v", err)
	}
	if _, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef); err != nil {
		t.Fatalf("retained completion %v", err)
	}
	store.mu.Lock()
	if store.downloads != 2 {
		t.Fatalf("downloads %d", store.downloads)
	}
	store.mu.Unlock()
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()
	if len(reassembler.entries) != 1 || reassembler.peerTransfers[binding.SenderID] != 1 {
		t.Fatalf("quota entries=%d peer=%d", len(reassembler.entries), reassembler.peerTransfers[binding.SenderID])
	}
}

func TestObjectIngressExpiryAndShutdownCancelBoundedOwner(t *testing.T) {
	t.Run("expiry", func(t *testing.T) {
		materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, 20*time.Millisecond)
		started := time.Now()
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 500*time.Millisecond {
			t.Fatalf("expiry %v after %s", err, time.Since(started))
		}
		store.mu.Lock()
		if store.downloads != 1 {
			t.Fatalf("downloads %d", store.downloads)
		}
		store.mu.Unlock()
		reassembler.mu.Lock()
		if len(reassembler.entries) != 0 {
			t.Fatalf("expired entries %d", len(reassembler.entries))
		}
		reassembler.mu.Unlock()
	})
	t.Run("explicit expiry cleanup", func(t *testing.T) {
		materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
		result := make(chan error, 1)
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			result <- err
		}()
		<-store.started
		started := time.Now()
		if expired := reassembler.Expire(manifest.ExpiresAt); expired != 1 {
			t.Fatalf("expired %d transfers", expired)
		}
		if err := <-result; !errors.Is(err, context.Canceled) || time.Since(started) > 500*time.Millisecond {
			t.Fatalf("expiry cleanup %v after %s", err, time.Since(started))
		}
	})
	t.Run("shutdown", func(t *testing.T) {
		materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
		result := make(chan error, 1)
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			result <- err
		}()
		<-store.started
		closed := make(chan struct{})
		go func() {
			reassembler.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("shutdown did not cancel object owner")
		}
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown result %v", err)
		}
	})
}

func TestAbortObjectAttemptCancelsOwnerAndPreservesDirectAttempt(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, canonical := newIngressFixture(t, 1024, time.Minute)
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	const callers = 6
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			results <- err
		}()
	}
	<-store.started
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	started := time.Now()
	if err := reassembler.Abort(manifest.TransferID, CarrierObjectUpload); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("object abort did not await bounded owner cleanup")
	}
	for i := 0; i < callers; i++ {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("joined result %v", err)
		}
	}
	reassembler.mu.Lock()
	entry := reassembler.entries[manifest.TransferID]
	direct := false
	object := false
	if entry != nil {
		_, direct = entry.carriers[CarrierDirectChunks]
		_, object = entry.carriers[CarrierObjectUpload]
	}
	if entry == nil || !direct || object || entry.objectRunning || reassembler.peerTransfers[binding.SenderID] != 1 || reassembler.totalBytes != manifest.TransferredSize {
		t.Fatalf("preserved entry=%#v peer=%d bytes=%d", entry, reassembler.peerTransfers[binding.SenderID], reassembler.totalBytes)
	}
	reassembler.mu.Unlock()
	if err := ForEachChunk(manifest, store.value, func(chunk TransferChunk) error {
		return reassembler.Accept(CarrierDirectChunks, chunk)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reassembler.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks); err != nil {
		t.Fatalf("direct completion: %v", err)
	}
	value, err := reassembler.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("direct consume %d bytes: %v", len(value), err)
	}
}

func TestAbortObjectAttemptAllowsLaterObjectRetry(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	first := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		first <- err
	}()
	<-store.started
	if err := reassembler.Abort(manifest.TransferID, CarrierObjectUpload); err != nil {
		t.Fatal(err)
	}
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted owner %v", err)
	}
	retry := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		retry <- err
	}()
	if call := <-store.started; call != 2 {
		t.Fatalf("retry download %d", call)
	}
	store.releases <- nil
	if err := <-retry; err != nil {
		t.Fatalf("retry %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.downloads != 2 {
		t.Fatalf("downloads %d", store.downloads)
	}
}

func TestAbortWinsBeforeObjectCommitAndDirectStillCompletes(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, canonical := newIngressFixture(t, 1024, time.Minute)
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	barrier := &beforeCommitCipher{started: make(chan struct{}), release: make(chan struct{})}
	reassembler.cipher = barrier
	const callers = 6
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
			results <- err
		}()
	}
	<-store.started
	waitForIngressWaiters(t, materializer, manifest.TransferID, callers)
	store.releases <- nil
	<-barrier.started
	aborted := make(chan error, 1)
	go func() { aborted <- reassembler.Abort(manifest.TransferID, CarrierObjectUpload) }()
	waitForObjectAbort(t, reassembler, manifest.TransferID)
	close(barrier.release)
	if err := <-aborted; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < callers; i++ {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("joined result %v", err)
		}
	}
	if !bytes.Equal(barrier.output, make([]byte, len(barrier.output))) {
		t.Fatal("aborted pre-commit plaintext was not zeroed")
	}
	if _, completed, err := reassembler.Completion(manifest.TransferID, binding); err != nil || completed {
		t.Fatalf("object completion retained: completed=%v err=%v", completed, err)
	}
	if err := ForEachChunk(manifest, store.value, func(chunk TransferChunk) error {
		return reassembler.Accept(CarrierDirectChunks, chunk)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reassembler.Complete(context.Background(), manifest.TransferID, CarrierDirectChunks); err != nil {
		t.Fatalf("direct completion: %v", err)
	}
	value, err := reassembler.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("direct consume %d bytes: %v", len(value), err)
	}
}

func TestAbortBeforeObjectCommitAllowsFreshObjectAttempt(t *testing.T) {
	materializer, reassembler, store, manifest, binding, privateRef, _ := newIngressFixture(t, 256, time.Minute)
	barrier := &beforeCommitCipher{started: make(chan struct{}), release: make(chan struct{})}
	reassembler.cipher = barrier
	first := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		first <- err
	}()
	<-store.started
	store.releases <- nil
	<-barrier.started
	aborted := make(chan error, 1)
	go func() { aborted <- reassembler.Abort(manifest.TransferID, CarrierObjectUpload) }()
	waitForObjectAbort(t, reassembler, manifest.TransferID)
	close(barrier.release)
	if err := <-aborted; err != nil {
		t.Fatal(err)
	}
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted attempt %v", err)
	}
	retry := make(chan error, 1)
	go func() {
		_, err := materializer.IngestObject(context.Background(), manifest, binding, privateRef)
		retry <- err
	}()
	if call := <-store.started; call != 2 {
		t.Fatalf("retry download %d", call)
	}
	store.releases <- nil
	if err := <-retry; err != nil {
		t.Fatalf("fresh attempt %v", err)
	}
	if _, completed, err := reassembler.Completion(manifest.TransferID, binding); err != nil || !completed {
		t.Fatalf("retry completion: completed=%v err=%v", completed, err)
	}
}
