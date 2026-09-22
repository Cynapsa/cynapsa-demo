package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type pipelinePublisher struct {
	mu           sync.Mutex
	publications []EnvelopePublication
}

func (publisher *pipelinePublisher) PublishPayloadEnvelope(_ context.Context, publication EnvelopePublication) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publication.Descriptor.Inline = clone(publication.Descriptor.Inline)
	publisher.publications = append(publisher.publications, publication)
	return nil
}

func pipelineLimits() Limits {
	limits := testLimits()
	limits.InlineBytes = 256
	limits.MaximumPayloadBytes = 4096
	limits.ReassemblyBytesPerPeer = 4096
	limits.ReassemblyBytes = 4096
	return limits
}

func pipelineHandles(t testing.TB) *HandleStore {
	t.Helper()
	store, err := NewHandleStore(HandleLimits{MaximumHandles: 8, MaximumBytes: 8192, MaximumPerHandle: 4096, MaximumWriteBytes: 4096, MaximumReadBytes: 4096}, bytes.NewReader(make([]byte, 512)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func inlinePipeline(t testing.TB) (*Pipeline, *HandleStore, *pipelinePublisher) {
	t.Helper()
	handles := pipelineHandles(t)
	factory, err := NewPipelineFactory(pipelineLimits(), PipelineDependencies{Handles: handles})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &pipelinePublisher{}
	pipeline, err := factory.Create(publisher)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline, handles, publisher
}

func TestPipelineInlineRoundTripAndOwnership(t *testing.T) {
	t.Parallel()
	pipeline, _, publisher := inlinePipeline(t)
	body := []byte("owned body")
	input := model.Payload{Value: model.NativePayload{ContentType: "text/plain", Path: "/work", Body: body}}
	prepared, err := pipeline.Prepare(input)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	decoded, err := pipeline.Decode(prepared.Canonical)
	if err != nil || decoded.Value.(model.NativePayload).Body[0] != 'o' || prepared.Profile != ProfileNative || prepared.ApplicationPath != "/work" {
		t.Fatalf("prepared=%#v decoded=%#v err=%v", prepared, decoded, err)
	}

	created := time.UnixMilli(1_700_000_000_000).UTC()
	expires := created.Add(5 * time.Second)
	request := TransferRequest{
		PeerID: "peer", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh",
		SenderID: "sender", RecipientID: "recipient",
		ConversationID: "conv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Mode:           protocol.ModeRequest,
		CorrelationID:  "cor_AAAAAAAAAAAAAAAAAAAAAA", CreatedAt: created,
		ExpiresAt: expires, ClockUncertainty: 250 * time.Microsecond,
		Profile: prepared.Profile, Canonical: prepared.Canonical,
	}
	if err := pipeline.Send(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(publisher.publications) != 1 {
		t.Fatalf("publications=%d", len(publisher.publications))
	}
	publication := publisher.publications[0]
	if !publication.ExpiresAt.Equal(expires) || publication.ClockUncertainty != request.ClockUncertainty || !publication.CreatedAt.Equal(created) {
		t.Fatalf("time metadata changed: %#v", publication)
	}
	materialized, err := pipeline.Materialize(context.Background(), MaterializationRequest{
		MessageID: request.MessageID, MeshID: request.MeshID, SenderID: request.SenderID,
		RecipientID: request.RecipientID, Descriptor: publication.Descriptor,
	})
	if err != nil || !bytes.Equal(materialized, prepared.Canonical) {
		t.Fatalf("materialize err=%v equal=%v", err, bytes.Equal(materialized, prepared.Canonical))
	}
	materialized[0] ^= 0xff
	if bytes.Equal(materialized, publication.Descriptor.Inline) {
		t.Fatal("materialization aliased descriptor storage")
	}
}

func TestPipelineCompletedHandleAndResponsePathRules(t *testing.T) {
	t.Parallel()
	pipeline, handles, _ := inlinePipeline(t)
	response := model.Payload{Value: model.HTTPResponsePayload{
		StatusCode: 500, Reason: "Internal Server Error",
		Headers: []model.Header{{Name: "x-test", Value: "one"}, {Name: "x-test", Value: "two"}},
		Body:    []byte("answer"),
		Error:   &model.ApplicationError{Code: "handler_error", Detail: "The handler failed", DetailsJSON: `{"safe":true}`},
	}}
	canonical, err := pipeline.serializer.Serialize(response)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := handles.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err = handles.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	if err = handles.Finish(handle); err != nil {
		t.Fatal(err)
	}
	prepared, err := pipeline.Prepare(model.Payload{Value: model.PayloadHandle{Handle: handle}})
	if err != nil || prepared.Profile != ProfileHTTPResponse || prepared.ApplicationPath != "" || !bytes.Equal(prepared.Canonical, canonical) {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
	canonical[0] ^= 0xff
	if prepared.Canonical[0] == canonical[0] {
		t.Fatal("handle snapshot aliased caller storage")
	}
	if _, err := pipeline.ApplicationPath(ProfileHTTPResponse, prepared.Canonical); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("response invented application path: %v", err)
	}
	decoded, err := pipeline.Decode(prepared.Canonical)
	decodedResponse, ok := decoded.Value.(model.HTTPResponsePayload)
	if err != nil || !ok || decodedResponse.StatusCode != 500 || decodedResponse.Reason != "Internal Server Error" || !bytes.Equal(decodedResponse.Body, []byte("answer")) || len(decodedResponse.Headers) != 2 || decodedResponse.Error == nil || decodedResponse.Error.Code != "handler_error" || decodedResponse.Error.DetailsJSON != `{"safe":true}` {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
}

func TestPipelineFailsClosedWithoutRealOffloadDependencies(t *testing.T) {
	t.Parallel()
	handles := pipelineHandles(t)
	limits := pipelineLimits()
	if _, err := NewPipelineFactory(limits, PipelineDependencies{Handles: handles, Direct: &fakeCarrier{available: true}}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("carrier without reassembler: %v", err)
	}
	if _, err := NewPipelineFactory(limits, PipelineDependencies{Handles: handles, Direct: &fakeCarrier{available: true}, Cipher: fakeCipher{}}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("offload without reassembler: %v", err)
	}
	if _, err := NewPipelineFactory(limits, PipelineDependencies{Handles: handles, Transfers: &Reassembler{}}); err != nil {
		t.Fatalf("plaintext reassembler: %v", err)
	}
	if _, err := NewPipelineFactory(limits, PipelineDependencies{Handles: handles, Objects: &fakeObjects{available: true}}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("object service without evidence: %v", err)
	}

	factory, err := NewPipelineFactory(limits, PipelineDependencies{Handles: handles})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &pipelinePublisher{}
	pipeline, err := factory.Create(publisher)
	if err != nil {
		t.Fatal(err)
	}
	large := model.Payload{Value: model.NativePayload{Path: "/large", Body: bytes.Repeat([]byte{'x'}, 512)}}
	prepared, err := pipeline.Prepare(large)
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest()
	request.Canonical = prepared.Canonical
	request.Profile = prepared.Profile
	if err := pipeline.Send(context.Background(), request); !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("missing offload reported success: %v", err)
	}
	if len(publisher.publications) != 0 {
		t.Fatal("unavailable offload published an envelope")
	}
}

type waitConsumeResult struct {
	value []byte
	err   error
}

// cancelAfterConsumeContext makes cancellation observable only to a context
// check that occurs after the completed entry has been atomically removed. A
// check inside the Reassembler critical section cannot acquire the mutex and
// therefore observes the operation as still live.
type cancelAfterConsumeContext struct {
	reassembler *Reassembler
	transferID  string
	done        chan struct{}
	once        sync.Once
}

func (ctx *cancelAfterConsumeContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterConsumeContext) Done() <-chan struct{}       { return ctx.done }
func (ctx *cancelAfterConsumeContext) Value(any) any               { return nil }
func (ctx *cancelAfterConsumeContext) Err() error {
	if !ctx.reassembler.mu.TryLock() {
		return nil
	}
	_, retained := ctx.reassembler.entries[ctx.transferID]
	ctx.reassembler.mu.Unlock()
	if retained {
		return nil
	}
	ctx.once.Do(func() { close(ctx.done) })
	return context.Canceled
}

func waitForTransferWaiters(t testing.TB, reassembler *Reassembler, transferID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reassembler.mu.Lock()
		wait := reassembler.waits[transferID]
		got := 0
		if wait != nil {
			got = wait.waiters
		}
		reassembler.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("transfer waiters did not reach %d", want)
}

func transferDescriptor(manifest TransferManifest) protocol.PayloadDescriptor {
	var digest [sha256.Size]byte
	copy(digest[:], manifest.CanonicalDigest)
	return protocol.PayloadDescriptor{
		Kind: protocol.PayloadTransferReference, Profile: ProfileNative,
		Size: manifest.CanonicalSize, Digest: digest,
		Reference: manifest.TransferID, EncryptionRef: manifest.EncryptionRef,
	}
}

func TestMaterializerWaitsForEnvelopeFirstTransferAndHasOneConsumeOwner(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("envelope first")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	ciphertext, ref, err := (fakeCipher{}).Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildManifest(binding, canonical, ciphertext, 5, ref, now.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = int64(len(ciphertext) + len(canonical))
	limits.ReassemblyBytes = int64(len(ciphertext) + len(canonical))
	reassembler, err := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reassembler.Close)
	materializer, err := NewMaterializer(limits.MaximumPayloadBytes, nil, fakeCipher{}, reassembler)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := transferDescriptor(manifest)
	results := make(chan waitConsumeResult, 2)
	for range 2 {
		go func() {
			value, materializeErr := materializer.MaterializeUntil(context.Background(), descriptor, binding, now.Add(30*time.Second))
			results <- waitConsumeResult{value: value, err: materializeErr}
		}()
	}
	waitForTransferWaiters(t, reassembler, manifest.TransferID, 2)
	if err := reassembler.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err := ForEachChunk(manifest, ciphertext, func(chunk TransferChunk) error {
		return reassembler.Accept(CarrierMessageChunks, chunk)
	}); err != nil {
		t.Fatal(err)
	}
	_, err = reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
	if err != nil {
		t.Fatal(err)
	}
	successes := 0
	notFound := 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil && bytes.Equal(result.value, canonical):
			successes++
		case errors.Is(result.err, ErrTransferNotFound):
			notFound++
		default:
			t.Fatalf("materialize result len=%d err=%v", len(result.value), result.err)
		}
		zero(result.value)
	}
	if successes != 1 || notFound != 1 {
		t.Fatalf("successes=%d notFound=%d", successes, notFound)
	}
	reassembler.mu.Lock()
	if len(reassembler.entries) != 0 || reassembler.totalBytes != 0 {
		t.Fatalf("consumption retained quota: entries=%d bytes=%d", len(reassembler.entries), reassembler.totalBytes)
	}
	reassembler.mu.Unlock()
}

func TestWaitConsumeCancellationAfterAtomicConsumeDoesNotDestroyWinner(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	limits := testLimits()
	reassembler, err := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer reassembler.Close()
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	canonical := []byte("sole retained completion")
	entry := &reassemblyEntry{
		manifest: TransferManifest{TransferID: binding.TransferID, TransferredSize: int64(len(canonical)), ExpiresAt: now.Add(time.Minute)},
		binding:  binding, carriers: map[CarrierKind]struct{}{CarrierMessageChunks: {}},
		canonical: clone(canonical), completed: true,
	}
	reassembler.mu.Lock()
	reassembler.entries[binding.TransferID] = entry
	reassembler.peerTransfers[binding.SenderID] = 1
	reassembler.peerBytes[binding.SenderID] = int64(len(canonical))
	reassembler.totalBytes = int64(len(canonical))
	reassembler.mu.Unlock()

	ctx := &cancelAfterConsumeContext{reassembler: reassembler, transferID: binding.TransferID, done: make(chan struct{})}
	value, err := reassembler.WaitConsume(ctx, binding.TransferID, binding, time.Time{})
	if err != nil || !bytes.Equal(value, canonical) {
		t.Fatalf("atomic consume value=%q err=%v", value, err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("test context did not expose post-linearization cancellation")
	}
	zero(value)
	reassembler.mu.Lock()
	_, retained := reassembler.entries[binding.TransferID]
	reassembler.mu.Unlock()
	if retained {
		t.Fatal("successful consume retained a second owner")
	}
}

func TestMaterializerEnvelopeFirstWaitCancellationAbortAndExpiry(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	limits := testLimits()
	limits.TransferLifetime = time.Second
	newWait := func(t *testing.T, transferID string) (*Reassembler, *Materializer, TransferBinding, protocol.PayloadDescriptor) {
		t.Helper()
		binding := testBinding(transferID)
		canonical := testCanonical("wait bounds")
		binding.CanonicalSize = int64(len(canonical))
		binding.CanonicalDigest = Digest(canonical)
		reassembler, err := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		materializer, err := NewMaterializer(limits.MaximumPayloadBytes, nil, fakeCipher{}, reassembler)
		if err != nil {
			t.Fatal(err)
		}
		descriptor := protocol.PayloadDescriptor{Kind: protocol.PayloadTransferReference, Profile: ProfileNative, Size: binding.CanonicalSize, Digest: binding.CanonicalDigest, Reference: transferID, EncryptionRef: "enc1_AQ"}
		return reassembler, materializer, binding, descriptor
	}

	t.Run("caller cancellation", func(t *testing.T) {
		r, materializer, binding, descriptor := newWait(t, "xfer_AAAAAAAAAAAAAAAAAAAAAA")
		defer r.Close()
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { _, err := materializer.Materialize(ctx, descriptor, binding); result <- err }()
		waitForTransferWaiters(t, r, binding.TransferID, 1)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel = %v", err)
		}
		r.mu.Lock()
		remaining := len(r.waits)
		r.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("cancel retained %d wait slots", remaining)
		}
	})

	t.Run("carrier abort", func(t *testing.T) {
		r, materializer, binding, descriptor := newWait(t, "xfer_AQEBAQEBAQEBAQEBAQEBAQ")
		defer r.Close()
		result := make(chan error, 1)
		go func() { _, err := materializer.Materialize(context.Background(), descriptor, binding); result <- err }()
		waitForTransferWaiters(t, r, binding.TransferID, 1)
		if err := r.Abort(binding.TransferID, CarrierMessageChunks); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, ErrTransferNotFound) {
			t.Fatalf("abort = %v", err)
		}
	})

	t.Run("shutdown barrier", func(t *testing.T) {
		r, materializer, binding, descriptor := newWait(t, "xfer_AwMDAwMDAwMDAwMDAwMDAw")
		result := make(chan error, 1)
		go func() { _, err := materializer.Materialize(context.Background(), descriptor, binding); result <- err }()
		waitForTransferWaiters(t, r, binding.TransferID, 1)
		r.Close()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown = %v", err)
			}
		default:
			t.Fatal("Close returned before materialization waiter")
		}
	})

	t.Run("authenticated expiry", func(t *testing.T) {
		r, materializer, binding, descriptor := newWait(t, "xfer_AgICAgICAgICAgICAgICAg")
		defer r.Close()
		started := time.Now()
		_, err := materializer.MaterializeUntil(context.Background(), descriptor, binding, now.Add(15*time.Millisecond))
		if !errors.Is(err, ErrTransferExpired) || time.Since(started) > time.Second {
			t.Fatalf("expiry = %v after %v", err, time.Since(started))
		}
	})
}

func TestReassemblerEnvelopeFirstWaitsReserveLogicalQuota(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	limits := testLimits()
	limits.MaximumTransfers = 1
	limits.TransfersPerPeer = 1
	reassembler, err := NewReassembler(limits, fakeCipher{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer reassembler.Close()
	first := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, waitErr := reassembler.WaitConsume(ctx, first.TransferID, first, time.Time{})
		result <- waitErr
	}()
	waitForTransferWaiters(t, reassembler, first.TransferID, 1)
	second := testBinding("xfer_AQEBAQEBAQEBAQEBAQEBAQ")
	if _, err := reassembler.WaitConsume(context.Background(), second.TransferID, second, time.Time{}); !errors.Is(err, ErrReassemblyQuota) {
		t.Fatalf("second envelope-first transfer = %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancellation = %v", err)
	}
}

func TestMaterializerInlineHonorsPreCancelledContext(t *testing.T) {
	pipeline, _, publisher := inlinePipeline(t)
	prepared, err := pipeline.Prepare(model.Payload{Value: model.NativePayload{Path: "/cancel", Body: []byte("body")}})
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest()
	request.Profile = prepared.Profile
	request.Canonical = prepared.Canonical
	if err := pipeline.Send(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	value, err := pipeline.Materialize(ctx, MaterializationRequest{
		MessageID: request.MessageID, MeshID: request.MeshID, SenderID: request.SenderID,
		RecipientID: request.RecipientID, Descriptor: publisher.publications[0].Descriptor,
	})
	if !errors.Is(err, context.Canceled) || value != nil {
		t.Fatalf("pre-cancel value=%x err=%v", value, err)
	}
}
