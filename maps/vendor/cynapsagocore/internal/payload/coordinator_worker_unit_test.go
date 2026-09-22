package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func privateDependencyDeadline() error {
	return errors.Join(errors.New("PRIVATE_COORDINATOR_CANARY"), context.DeadlineExceeded)
}

type canaryCarrier struct {
	phase    string
	manifest TransferManifest
}

func (c *canaryCarrier) Available(context.Context, CarrierRoute) (bool, error) {
	if c.phase == "available" {
		return false, privateDependencyDeadline()
	}
	return true, nil
}
func (c *canaryCarrier) Begin(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	if c.phase == "begin" {
		return privateDependencyDeadline()
	}
	c.manifest, _ = DecodeManifest(frame.Data)
	return nil
}
func (c *canaryCarrier) SendChunk(context.Context, CarrierRoute, CarrierFrame) error {
	if c.phase == "chunk" {
		return privateDependencyDeadline()
	}
	return nil
}
func (c *canaryCarrier) Finish(_ context.Context, route CarrierRoute, _ string) (CompletionEvidence, error) {
	if c.phase == "finish" {
		return CompletionEvidence{}, privateDependencyDeadline()
	}
	var digest [32]byte
	copy(digest[:], c.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: c.manifest.TransferID, MessageID: c.manifest.MessageID, Digest: digest}, nil
}
func (*canaryCarrier) Abort(context.Context, CarrierRoute, string) error { return nil }

type canaryCoordinatorCipher struct{ output []byte }

func (c *canaryCoordinatorCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	c.output = []byte("private partial ciphertext")
	return c.output, "", privateDependencyDeadline()
}
func (*canaryCoordinatorCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	return nil, ErrEncryption
}

type canaryPublisher struct{}

func (canaryPublisher) PublishPayloadEnvelope(context.Context, EnvelopePublication) error {
	return privateDependencyDeadline()
}

type fakeCipher struct{}

func (fakeCipher) Encrypt(_ context.Context, binding TransferBinding, value []byte) ([]byte, string, error) {
	prefix := sha256.Sum256([]byte(binding.SenderID))
	output := make([]byte, encryptionFrameOverhead+len(value))
	copy(output[:encryptionFrameOverhead], prefix[:encryptionFrameOverhead])
	copy(output[encryptionFrameOverhead:], value)
	return output, "enc1_AQ", nil
}

type partialEncryptCipher struct{ output []byte }

func (p *partialEncryptCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	p.output = []byte("sensitive partial ciphertext")
	return p.output, "enc1_AQ", ErrEncryption
}
func (*partialEncryptCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	return nil, ErrEncryption
}
func (fakeCipher) Decrypt(_ context.Context, binding TransferBinding, value []byte, ref string) ([]byte, error) {
	prefix := sha256.Sum256([]byte(binding.SenderID))
	if ref != "enc1_AQ" || binding.SenderID == "" || len(value) < encryptionFrameOverhead || !bytes.Equal(value[:encryptionFrameOverhead], prefix[:encryptionFrameOverhead]) {
		return nil, ErrAuthentication
	}
	return clone(value[encryptionFrameOverhead:]), nil
}

type fakeCarrier struct {
	mu            sync.Mutex
	available     bool
	fail          error
	failAt        string
	calls         []string
	manifest      TransferManifest
	routes        []CarrierRoute
	abortDeadline bool
}

type diagnosticBlockingCarrier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	inner   fakeCarrier
}

func (carrier *diagnosticBlockingCarrier) Available(context.Context, CarrierRoute) (bool, error) {
	return true, nil
}
func (carrier *diagnosticBlockingCarrier) Begin(ctx context.Context, route CarrierRoute, frame CarrierFrame) error {
	carrier.once.Do(func() { close(carrier.entered) })
	select {
	case <-carrier.release:
		return carrier.inner.Begin(ctx, route, frame)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (carrier *diagnosticBlockingCarrier) SendChunk(ctx context.Context, route CarrierRoute, frame CarrierFrame) error {
	return carrier.inner.SendChunk(ctx, route, frame)
}
func (carrier *diagnosticBlockingCarrier) Finish(ctx context.Context, route CarrierRoute, transferID string) (CompletionEvidence, error) {
	return carrier.inner.Finish(ctx, route, transferID)
}
func (carrier *diagnosticBlockingCarrier) Abort(ctx context.Context, route CarrierRoute, transferID string) error {
	return carrier.inner.Abort(ctx, route, transferID)
}

func (f *fakeCarrier) stageError(stage string) error {
	if f.fail != nil && (f.failAt == "" || f.failAt == stage) {
		return f.fail
	}
	return nil
}

func (f *fakeCarrier) Available(context.Context, CarrierRoute) (bool, error) { return f.available, nil }
func (f *fakeCarrier) Begin(_ context.Context, route CarrierRoute, frame CarrierFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "begin")
	f.routes = append(f.routes, route)
	var m TransferManifest
	var err error
	if frame.Encoding == FrameText {
		m, err = DecodeTextManifest(string(frame.Data))
	} else {
		m, err = DecodeManifest(frame.Data)
	}
	if err != nil {
		return err
	}
	f.manifest = cloneManifest(m)
	return f.stageError("begin")
}
func (f *fakeCarrier) SendChunk(_ context.Context, route CarrierRoute, _ CarrierFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "chunk")
	f.routes = append(f.routes, route)
	return f.stageError("chunk")
}
func (f *fakeCarrier) Finish(_ context.Context, route CarrierRoute, id string) (CompletionEvidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "finish")
	f.routes = append(f.routes, route)
	if err := f.stageError("finish"); err != nil {
		return CompletionEvidence{}, err
	}
	var digest [32]byte
	copy(digest[:], f.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: id, MessageID: f.manifest.MessageID, Digest: digest}, nil
}
func (f *fakeCarrier) Abort(ctx context.Context, route CarrierRoute, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "abort")
	f.routes = append(f.routes, route)
	_, f.abortDeadline = ctx.Deadline()
	return nil
}

type fakeObjects struct {
	available bool
	fail      error
	value     []byte
	reference string
	downloads int
}

type fakePreparedUpload struct {
	reference string
	commit    func(context.Context, []byte) (string, error)
	aborted   bool
}

func (upload *fakePreparedUpload) DownloadReference() string { return upload.reference }
func (upload *fakePreparedUpload) Commit(ctx context.Context, value []byte) error {
	_, err := upload.commit(ctx, value)
	return err
}
func (upload *fakePreparedUpload) Abort() { upload.aborted = true }

func (f *fakeObjects) Available(context.Context) (bool, error) { return f.available, nil }
func (f *fakeObjects) Upload(_ context.Context, value []byte) (string, error) {
	if f.fail != nil {
		return "", f.fail
	}
	f.value = clone(value)
	if f.reference != "" {
		return f.reference, nil
	}
	return "https://objects.example/blob", nil
}
func (f *fakeObjects) PrepareUpload(context.Context, int64) (PreparedObjectUpload, error) {
	reference := f.reference
	if reference == "" {
		reference = "https://objects.example/blob"
	}
	return &fakePreparedUpload{reference: reference, commit: f.Upload}, nil
}
func (f *fakeObjects) Download(context.Context, string) ([]byte, error) {
	f.downloads++
	return clone(f.value), f.fail
}

type fakeEvidence struct {
	fail          error
	readinessFail error
	manifest      TransferManifest
}

func (f *fakeEvidence) ConfirmObjectReadiness(context.Context, CarrierRoute, TransferManifest, string) error {
	return f.readinessFail
}

type fakePublisher struct {
	mu           sync.Mutex
	publications []EnvelopePublication
	fail         error
}

type plaintextCarrier struct {
	manifest TransferManifest
	bytes    []byte
}

func (*plaintextCarrier) Available(context.Context, CarrierRoute) (bool, error) { return true, nil }
func (carrier *plaintextCarrier) Begin(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	var err error
	if frame.Encoding == FrameText {
		carrier.manifest, err = DecodeTextManifest(string(frame.Data))
	} else {
		carrier.manifest, err = DecodeManifest(frame.Data)
	}
	return err
}
func (carrier *plaintextCarrier) SendChunk(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	var chunk TransferChunk
	var err error
	if frame.Encoding == FrameText {
		chunk, err = DecodeTextChunk(string(frame.Data), 1<<20)
	} else {
		chunk, err = DecodeChunk(frame.Data, 1<<20)
	}
	if err == nil {
		carrier.bytes = append(carrier.bytes, chunk.Bytes...)
	}
	return err
}
func (carrier *plaintextCarrier) Finish(_ context.Context, route CarrierRoute, transferID string) (CompletionEvidence, error) {
	var digest [32]byte
	copy(digest[:], carrier.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: transferID, MessageID: carrier.manifest.MessageID, Digest: digest}, nil
}
func (*plaintextCarrier) Abort(context.Context, CarrierRoute, string) error { return nil }

func (f *fakePublisher) PublishPayloadEnvelope(_ context.Context, publication EnvelopePublication) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publications = append(f.publications, publication)
	return f.fail
}

func (f *fakePublisher) registered(route CarrierRoute, transferID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, publication := range f.publications {
		if publication.Route == route && publication.Descriptor.Reference == transferID {
			return true
		}
	}
	return false
}

type routingCarrier struct {
	mu        sync.Mutex
	publisher *fakePublisher
	manifests map[string]TransferManifest
	routes    []CarrierRoute
}

func (r *routingCarrier) Available(context.Context, CarrierRoute) (bool, error) { return true, nil }
func (r *routingCarrier) Begin(_ context.Context, route CarrierRoute, frame CarrierFrame) error {
	manifest, err := DecodeManifest(frame.Data)
	if err != nil {
		return err
	}
	if !r.publisher.registered(route, manifest.TransferID) || route.MeshID != manifest.MeshID || route.SenderID != manifest.SenderID || route.RecipientID != manifest.RecipientID || route.MessageID != manifest.MessageID {
		return ErrAuthentication
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.manifests == nil {
		r.manifests = make(map[string]TransferManifest)
	}
	r.manifests[manifest.TransferID] = cloneManifest(manifest)
	r.routes = append(r.routes, route)
	return nil
}
func (r *routingCarrier) SendChunk(_ context.Context, route CarrierRoute, _ CarrierFrame) error {
	r.mu.Lock()
	r.routes = append(r.routes, route)
	r.mu.Unlock()
	return nil
}
func (r *routingCarrier) Finish(_ context.Context, route CarrierRoute, id string) (CompletionEvidence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route)
	manifest, ok := r.manifests[id]
	if !ok {
		return CompletionEvidence{}, ErrTransferNotFound
	}
	var digest [32]byte
	copy(digest[:], manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: id, MessageID: manifest.MessageID, Digest: digest}, nil
}
func (*routingCarrier) Abort(context.Context, CarrierRoute, string) error { return nil }

func waitPastDeadline(ctx context.Context) error {
	<-ctx.Done()
	return errors.New("dependency returned opaque error after deadline")
}

type deadlineCipher struct{ block bool }

func (d deadlineCipher) Encrypt(ctx context.Context, binding TransferBinding, value []byte) ([]byte, string, error) {
	if d.block {
		return nil, "", waitPastDeadline(ctx)
	}
	return fakeCipher{}.Encrypt(ctx, binding, value)
}
func (deadlineCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	return nil, ErrAuthentication
}

type deadlinePublisher struct{ block bool }

func (d deadlinePublisher) PublishPayloadEnvelope(ctx context.Context, _ EnvelopePublication) error {
	if d.block {
		return waitPastDeadline(ctx)
	}
	return nil
}

type deadlineCarrier struct {
	phase         string
	manifest      TransferManifest
	abortDeadline bool
}

func (d *deadlineCarrier) Available(ctx context.Context, _ CarrierRoute) (bool, error) {
	if d.phase == "available" {
		return false, waitPastDeadline(ctx)
	}
	return true, nil
}
func (d *deadlineCarrier) Begin(ctx context.Context, _ CarrierRoute, frame CarrierFrame) error {
	if d.phase == "begin" {
		return waitPastDeadline(ctx)
	}
	d.manifest, _ = DecodeManifest(frame.Data)
	return nil
}
func (d *deadlineCarrier) SendChunk(ctx context.Context, _ CarrierRoute, _ CarrierFrame) error {
	if d.phase == "chunk" {
		return waitPastDeadline(ctx)
	}
	return nil
}
func (d *deadlineCarrier) Finish(ctx context.Context, route CarrierRoute, id string) (CompletionEvidence, error) {
	if d.phase == "finish" {
		return CompletionEvidence{}, waitPastDeadline(ctx)
	}
	var digest [32]byte
	copy(digest[:], d.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: id, MessageID: d.manifest.MessageID, Digest: digest}, nil
}
func (d *deadlineCarrier) Abort(ctx context.Context, _ CarrierRoute, _ string) error {
	_, d.abortDeadline = ctx.Deadline()
	return nil
}

type deadlineObjects struct{ phase string }

func (d deadlineObjects) Available(ctx context.Context) (bool, error) {
	if d.phase == "object_available" {
		return false, waitPastDeadline(ctx)
	}
	return true, nil
}
func (d deadlineObjects) Upload(ctx context.Context, _ []byte) (string, error) {
	if d.phase == "object_upload" {
		return "", waitPastDeadline(ctx)
	}
	return "https://objects.example/blob", nil
}
func (d deadlineObjects) PrepareUpload(context.Context, int64) (PreparedObjectUpload, error) {
	return &fakePreparedUpload{reference: "https://objects.example/blob", commit: d.Upload}, nil
}
func (deadlineObjects) Download(context.Context, string) ([]byte, error) {
	return nil, ErrCarrierMaterialization
}

type deadlineEvidence struct {
	phase         string
	manifest      TransferManifest
	abortDeadline bool
}

func (d *deadlineEvidence) ConfirmObjectReadiness(ctx context.Context, _ CarrierRoute, _ TransferManifest, _ string) error {
	if d.phase == "object_readiness" {
		return waitPastDeadline(ctx)
	}
	return nil
}

func (d *deadlineEvidence) PublishObject(ctx context.Context, _ CarrierRoute, manifest TransferManifest, _ string) error {
	d.manifest = cloneManifest(manifest)
	if d.phase == "object_publish" {
		return waitPastDeadline(ctx)
	}
	return nil
}
func (d *deadlineEvidence) AwaitMaterialization(ctx context.Context, route CarrierRoute, id string) (CompletionEvidence, error) {
	if d.phase == "object_await" {
		return CompletionEvidence{}, waitPastDeadline(ctx)
	}
	var digest [32]byte
	copy(digest[:], d.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: id, MessageID: d.manifest.MessageID, Digest: digest}, nil
}
func (d *deadlineEvidence) AbortMaterialization(ctx context.Context, _ CarrierRoute, _ string) error {
	_, d.abortDeadline = ctx.Deadline()
	return nil
}

func (f *fakeEvidence) PublishObject(_ context.Context, _ CarrierRoute, m TransferManifest, _ string) error {
	f.manifest = cloneManifest(m)
	return f.fail
}
func (f *fakeEvidence) AwaitMaterialization(_ context.Context, _ CarrierRoute, id string) (CompletionEvidence, error) {
	if f.fail != nil {
		return CompletionEvidence{}, f.fail
	}
	var digest [32]byte
	copy(digest[:], f.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: id, MessageID: f.manifest.MessageID, Digest: digest}, nil
}
func (*fakeEvidence) AbortMaterialization(context.Context, CarrierRoute, string) error { return nil }

func transferRequest() TransferRequest {
	return TransferRequest{PeerID: "peer", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh", SenderID: "sender", RecipientID: "recipient", ConversationID: "conv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Mode: protocol.ModeMessage, Profile: "aztm.native", Canonical: testCanonical("payload larger than inline")}
}

func TestCoordinatorActiveTransfersTracksLargeCarrierOwnership(t *testing.T) {
	carrier := &diagnosticBlockingCarrier{entered: make(chan struct{}), release: make(chan struct{})}
	coordinator, err := NewCoordinator(testLimits(), carrier, nil, nil, nil, nil, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	done := make(chan error, 1)
	go func() {
		_, sendErr := coordinator.Send(context.Background(), transferRequest())
		done <- sendErr
	}()
	<-carrier.entered
	if active := coordinator.ActiveTransfers(); active != 1 {
		t.Fatalf("active transfers while carrier owns bytes = %d", active)
	}
	close(carrier.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if active := coordinator.ActiveTransfers(); active != 0 {
		t.Fatalf("active transfers after evidence = %d", active)
	}
}

func TestCoordinatorAuthorityCheckpointBlocksCarrierCallAfterPublication(t *testing.T) {
	carrier := &fakeCarrier{available: true}
	publisher := &fakePublisher{}
	coordinator, err := NewCoordinator(testLimits(), carrier, nil, nil, nil, nil, publisher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	var checkpoints atomic.Int32
	ctx := WithAuthorityCheckpoint(context.Background(), func(_ context.Context, effect func() error) error {
		if checkpoints.Add(1) >= 3 {
			// Direct.Available and logical publication won. The membership fence
			// wins before the next external call, which would be carrier.Begin.
			return ErrAuthentication
		}
		return effect()
	})
	if _, err = coordinator.Send(ctx, transferRequest()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("send=%v", err)
	}
	carrier.mu.Lock()
	calls := append([]string(nil), carrier.calls...)
	carrier.mu.Unlock()
	for _, call := range calls {
		if call == "begin" || call == "chunk" || call == "finish" {
			t.Fatalf("post-fence carrier call=%q in %v", call, calls)
		}
	}
	if len(publisher.publications) != 1 {
		t.Fatalf("publications=%d want 1", len(publisher.publications))
	}
}

func TestCoordinatorV1PlaintextDirectAndMessageCarriers(t *testing.T) {
	for _, test := range []struct {
		name     string
		direct   bool
		wantKind CarrierKind
	}{
		{name: "rank1 data channel", direct: true, wantKind: CarrierDirectChunks},
		{name: "rank2 XMPP chunks", wantKind: CarrierMessageChunks},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := &plaintextCarrier{}
			var direct, messages ChunkCarrier
			if test.direct {
				direct = carrier
			} else {
				messages = carrier
			}
			coordinator, err := NewCoordinator(testLimits(), direct, nil, nil, messages, nil, &fakePublisher{})
			if err != nil {
				t.Fatal(err)
			}
			coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
			request := transferRequest()
			receipt, err := coordinator.Send(context.Background(), request)
			if err != nil || receipt.Carrier != test.wantKind {
				t.Fatalf("receipt=%#v err=%v", receipt, err)
			}
			if carrier.manifest.EncryptionRef != "" || receipt.Descriptor.EncryptionRef != "" || !bytes.Equal(carrier.bytes, request.Canonical) {
				t.Fatalf("V1 transfer was not exact plaintext: manifest=%#v bytes=%x", carrier.manifest, carrier.bytes)
			}
		})
	}
}

func TestCoordinatorV1PlaintextObjectUploadAfterRecipientReadiness(t *testing.T) {
	objects := &fakeObjects{available: true}
	evidence := &fakeEvidence{}
	coordinator, err := NewCoordinator(testLimits(), nil, objects, evidence, nil, nil, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	request := transferRequest()
	receipt, err := coordinator.Send(context.Background(), request)
	if err != nil || receipt.Carrier != CarrierObjectUpload {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	if evidence.manifest.EncryptionRef != "" || receipt.Descriptor.EncryptionRef != "" || !bytes.Equal(objects.value, request.Canonical) {
		t.Fatalf("V1 object was not exact plaintext: manifest=%#v bytes=%x", evidence.manifest, objects.value)
	}
}

func TestCoordinatorCarrierLadder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		direct   *fakeCarrier
		objects  *fakeObjects
		evidence *fakeEvidence
		messages *fakeCarrier
		want     CarrierKind
		wantErr  error
	}{
		{"direct", &fakeCarrier{available: true}, &fakeObjects{available: true}, &fakeEvidence{}, &fakeCarrier{available: true}, CarrierDirectChunks, nil},
		{"object after ambiguous direct completion", &fakeCarrier{available: true, fail: ErrAmbiguousCompletion, failAt: "finish"}, &fakeObjects{available: true}, &fakeEvidence{}, &fakeCarrier{available: true}, CarrierObjectUpload, nil},
		{"message after object failure", &fakeCarrier{available: false}, &fakeObjects{available: true, fail: ErrCarrierUpload}, &fakeEvidence{}, &fakeCarrier{available: true}, CarrierMessageChunks, nil},
		{"message after oversized object URL", &fakeCarrier{available: false}, &fakeObjects{available: true, reference: "https://objects.example/" + string(bytes.Repeat([]byte{'x'}, 5000))}, &fakeEvidence{}, &fakeCarrier{available: true}, CarrierMessageChunks, nil},
		{"message after receiver materialization failure", &fakeCarrier{available: false}, &fakeObjects{available: true}, &fakeEvidence{fail: ErrCarrierMaterialization}, &fakeCarrier{available: true}, CarrierMessageChunks, nil},
		{"message when recipient cannot reach exact object authority", &fakeCarrier{available: false}, &fakeObjects{available: true}, &fakeEvidence{readinessFail: ErrCarrierUnavailable}, &fakeCarrier{available: true}, CarrierMessageChunks, nil},
		{"terminal object authentication failure", &fakeCarrier{available: false}, &fakeObjects{available: true}, &fakeEvidence{fail: ErrAuthentication}, &fakeCarrier{available: true}, 0, ErrAuthentication},
		{"caller deadline is terminal", &fakeCarrier{available: false}, &fakeObjects{available: true}, &fakeEvidence{fail: context.DeadlineExceeded}, &fakeCarrier{available: true}, 0, context.DeadlineExceeded},
		{"all fail", &fakeCarrier{available: true, fail: ErrCarrierUnavailable}, &fakeObjects{available: true, fail: ErrCarrierUpload}, &fakeEvidence{}, &fakeCarrier{available: true, fail: ErrCarrierUnavailable}, 0, ErrAllCarriersFailed},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			limits := testLimits()
			c, err := NewCoordinator(limits, tt.direct, tt.objects, tt.evidence, tt.messages, fakeCipher{}, &fakePublisher{})
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return time.Unix(1000, 0).UTC() }
			receipt, err := c.Send(context.Background(), transferRequest())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err %v want %v", err, tt.wantErr)
			}
			if err == nil && receipt.Carrier != tt.want {
				t.Fatalf("carrier %v", receipt.Carrier)
			}
		})
	}
}

func TestCoordinatorRequiresExactRecipientReadinessBeforeObjectCommit(t *testing.T) {
	objects := &fakeObjects{available: true, reference: "https://objects.example:7443/exact-object"}
	evidence := &fakeEvidence{readinessFail: ErrCarrierUnavailable}
	messages := &fakeCarrier{available: true}
	coordinator, err := NewCoordinator(testLimits(), nil, objects, evidence, messages, fakeCipher{}, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	receipt, err := coordinator.Send(context.Background(), transferRequest())
	if err != nil || receipt.Carrier != CarrierMessageChunks {
		t.Fatalf("fallback = %#v, %v", receipt, err)
	}
	if len(objects.value) != 0 {
		t.Fatal("ciphertext was uploaded after recipient readiness rejection")
	}
}

func TestFallbackPlanAndFailureClassification(t *testing.T) {
	limits := testLimits()
	plan, err := BuildFallbackPlan(CarrierAvailability{DirectChunks: true, ObjectUpload: true, MessageChunks: true}, limits)
	if err != nil || !reflect.DeepEqual(plan, []CarrierKind{CarrierDirectChunks, CarrierObjectUpload, CarrierMessageChunks}) {
		t.Fatalf("plan %v %v", plan, err)
	}
	if _, err := BuildFallbackPlan(CarrierAvailability{}, limits); !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("empty %v", err)
	}
	for _, err := range []error{ErrCarrierUnavailable, ErrCarrierUnsupported, ErrCarrierRejected, ErrCarrierTimeout, ErrCarrierUpload, ErrCarrierMaterialization, ErrAmbiguousCompletion} {
		if got := ClassifyCarrierFailure(CarrierObjectUpload, err); got != FailureFallbackEligible {
			t.Errorf("%v => %v", err, got)
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, ErrIntegrity, ErrAuthentication, ErrAuthorization, ErrPayloadTooLarge, ErrReassemblyQuota} {
		if got := ClassifyCarrierFailure(CarrierObjectUpload, err); got != FailureTerminal {
			t.Errorf("%v => %v", err, got)
		}
	}
}

func TestNormalizedTransferErrorPreservesAuthorization(t *testing.T) {
	if got := normalizedTransferError(ErrAuthorization); !errors.Is(got, ErrAuthorization) {
		t.Fatalf("authorization normalized to %v", got)
	}
}

func TestCoordinatorInlineAndCancellation(t *testing.T) {
	limits := testLimits()
	limits.InlineBytes = 128
	c, _ := NewCoordinator(limits, nil, nil, nil, nil, fakeCipher{}, &fakePublisher{})
	req := transferRequest()
	req.Canonical = testCanonical("small")
	receipt, err := c.Send(context.Background(), req)
	if err != nil || receipt.Descriptor.Kind != 0 {
		t.Fatalf("inline %v %v", receipt.Descriptor.Kind, err)
	}
	req = transferRequest()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Send(ctx, req); !errors.Is(err, context.Canceled) && !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("cancel %v", err)
	}
}

func TestCoordinatorDirectFrameLimitFallsBackToObject(t *testing.T) {
	limits := testLimits()
	limits.MaximumFrameBytes = limits.ChunkBytes
	direct := &fakeCarrier{available: true}
	objects := &fakeObjects{available: true}
	evidence := &fakeEvidence{}
	c, err := NewCoordinator(limits, direct, objects, evidence, &fakeCarrier{available: true}, fakeCipher{}, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	receipt, err := c.Send(context.Background(), transferRequest())
	if err != nil || receipt.Carrier != CarrierObjectUpload {
		t.Fatalf("receipt %v err %v", receipt.Carrier, err)
	}
}

func TestCoordinatorPublishesEnvelopeBeforeRoutedConcurrentTransfers(t *testing.T) {
	limits := testLimits()
	publisher := &fakePublisher{}
	carrier := &routingCarrier{publisher: publisher}
	coordinator, err := NewCoordinator(limits, carrier, nil, nil, nil, fakeCipher{}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	requests := []TransferRequest{transferRequest(), transferRequest()}
	requests[1].PeerID = "peer-2"
	requests[1].RecipientID = "recipient-2"
	requests[1].MessageID = "msg_AQEBAQEBAQEBAQEBAQEBAQ"
	requests[1].ConversationID = "conv_HEyIV7Gv3IniyWcoOTvqqQBeOEGbNgXc8KgJNQAV_xM"
	var wg sync.WaitGroup
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := coordinator.Send(context.Background(), request)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	publisher.mu.Lock()
	if len(publisher.publications) != 2 {
		t.Fatalf("publications %d", len(publisher.publications))
	}
	publisher.mu.Unlock()
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	seen := make(map[string]bool)
	for _, route := range carrier.routes {
		seen[route.PeerID+"/"+route.RecipientID] = true
	}
	if !seen["peer/recipient"] || !seen["peer-2/recipient-2"] {
		t.Fatalf("routes %#v", carrier.routes)
	}
}

func TestCoordinatorPublicationFailurePreventsCarrierAndCleanupIsBounded(t *testing.T) {
	limits := testLimits()
	carrier := &fakeCarrier{available: true, fail: ErrCarrierUnavailable, failAt: "chunk"}
	publisher := &fakePublisher{fail: errors.New("private publication failure")}
	coordinator, _ := NewCoordinator(limits, carrier, nil, nil, nil, fakeCipher{}, publisher)
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrEnvelopePublication) {
		t.Fatalf("publication error %v", err)
	}
	if len(carrier.calls) != 0 {
		t.Fatalf("carrier ran before publication: %v", carrier.calls)
	}
	publisher.fail = nil
	if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("carrier error %v", err)
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	if !carrier.abortDeadline {
		t.Fatal("cleanup abort had no independent deadline")
	}
}

func TestCoordinatorZerosCipherOutputReturnedWithError(t *testing.T) {
	cipher := &partialEncryptCipher{}
	coordinator, _ := NewCoordinator(testLimits(), &fakeCarrier{available: true}, nil, nil, nil, cipher, &fakePublisher{})
	if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrEncryption) {
		t.Fatalf("error %v", err)
	}
	if !bytes.Equal(cipher.output, make([]byte, len(cipher.output))) {
		t.Fatalf("partial ciphertext was retained: %q", cipher.output)
	}
}

func TestCoordinatorSamplesManifestClockOnce(t *testing.T) {
	carrier := &fakeCarrier{available: true}
	coordinator, err := NewCoordinator(testLimits(), carrier, nil, nil, nil, fakeCipher{}, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1000, 0).UTC()
	clockSamples := 0
	coordinator.now = func() time.Time {
		clockSamples++
		return base.Add(time.Duration(clockSamples-1) * 2 * testLimits().TransferLifetime)
	}
	if _, err := coordinator.Send(context.Background(), transferRequest()); err != nil {
		t.Fatal(err)
	}
	if clockSamples != 1 {
		t.Fatalf("manifest clock samples = %d, want 1", clockSamples)
	}
}

func TestCoordinatorOperationDeadlineBoundsEveryPrimaryPhase(t *testing.T) {
	phases := []string{"available", "encrypt", "publish", "begin", "chunk", "finish", "object_available", "object_readiness", "object_upload", "object_publish", "object_await"}
	for _, phase := range phases {
		phase := phase
		t.Run(phase, func(t *testing.T) {
			limits := testLimits()
			// Leave enough setup headroom for race instrumentation and loaded
			// builders so the configured phase, rather than an earlier phase,
			// observes the operation deadline.
			limits.TransferLifetime = 100 * time.Millisecond
			limits.CleanupTimeout = 30 * time.Millisecond
			publisher := deadlinePublisher{block: phase == "publish"}
			cipher := deadlineCipher{block: phase == "encrypt"}
			var direct *deadlineCarrier
			var directCarrier ChunkCarrier
			var objects ObjectStore
			var evidence MaterializationAcknowledger
			var objectEvidence *deadlineEvidence
			if len(phase) >= len("object_") && phase[:len("object_")] == "object_" {
				objects = deadlineObjects{phase: phase}
				objectEvidence = &deadlineEvidence{phase: phase}
				evidence = objectEvidence
			} else {
				direct = &deadlineCarrier{phase: phase}
				directCarrier = direct
			}
			coordinator, err := NewCoordinator(limits, directCarrier, objects, evidence, nil, cipher, publisher)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			_, err = coordinator.Send(context.Background(), transferRequest())
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error %v", err)
			}
			if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
				t.Fatalf("operation exceeded bound: %s", elapsed)
			}
			if direct != nil && (phase == "begin" || phase == "chunk" || phase == "finish") && !direct.abortDeadline {
				t.Fatal("post-timeout cleanup lacked its independent deadline")
			}
			if objectEvidence != nil && (phase == "object_readiness" || phase == "object_publish" || phase == "object_await") && !objectEvidence.abortDeadline {
				t.Fatal("post-timeout object cleanup lacked its independent deadline")
			}
		})
	}
}

func TestCoordinatorRespectsEarlierCallerDeadline(t *testing.T) {
	limits := testLimits()
	limits.TransferLifetime = time.Second
	carrier := &deadlineCarrier{phase: "available"}
	coordinator, _ := NewCoordinator(limits, carrier, nil, nil, nil, deadlineCipher{}, deadlinePublisher{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := coordinator.Send(ctx, transferRequest())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("caller deadline %v elapsed %s", err, time.Since(started))
	}
}

func TestCoordinatorNormalizesWrappedDependencyDeadlines(t *testing.T) {
	for _, phase := range []string{"available", "begin", "chunk", "finish"} {
		t.Run(phase, func(t *testing.T) {
			carrier := &canaryCarrier{phase: phase}
			coordinator, err := NewCoordinator(testLimits(), carrier, nil, nil, nil, fakeCipher{}, deadlinePublisher{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = coordinator.Send(context.Background(), transferRequest())
			if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
				t.Fatalf("dependency error %q", err)
			}
		})
	}
	t.Run("cipher", func(t *testing.T) {
		cipher := &canaryCoordinatorCipher{}
		coordinator, _ := NewCoordinator(testLimits(), &canaryCarrier{}, nil, nil, nil, cipher, deadlinePublisher{})
		_, err := coordinator.Send(context.Background(), transferRequest())
		if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("cipher error %q", err)
		}
		if !bytes.Equal(cipher.output, make([]byte, len(cipher.output))) {
			t.Fatal("partial cipher output not zeroed")
		}
	})
	t.Run("publisher", func(t *testing.T) {
		limits := testLimits()
		limits.InlineBytes = 128
		coordinator, _ := NewCoordinator(limits, nil, nil, nil, nil, fakeCipher{}, canaryPublisher{})
		request := transferRequest()
		request.Canonical = testCanonical("small")
		_, err := coordinator.Send(context.Background(), request)
		if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("publisher error %q", err)
		}
	})
}

type testJob struct {
	panic bool
	wait  <-chan struct{}
}

type canaryJob struct{}

func (canaryJob) Execute(context.Context) (TransferReceipt, error) {
	return TransferReceipt{}, privateDependencyDeadline()
}

func (j testJob) Execute(ctx context.Context) (TransferReceipt, error) {
	if j.panic {
		panic("private canary")
	}
	if j.wait != nil {
		select {
		case <-j.wait:
		case <-ctx.Done():
			return TransferReceipt{}, ctx.Err()
		}
	}
	return TransferReceipt{MessageID: "ok"}, nil
}

func TestTransferWorkerCapacityPanicAndShutdown(t *testing.T) {
	worker, _ := NewTransferWorker(1, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	deadline := time.Now().Add(time.Second)
	for {
		worker.mu.Lock()
		started := worker.started
		worker.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not start")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := worker.Submit(context.Background(), testJob{panic: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-result; !errors.Is(got.Err, ErrWorkerPanic) {
		t.Fatalf("panic %v", got.Err)
	}
	block := make(chan struct{})
	first, _ := worker.Submit(context.Background(), testJob{wait: block})
	time.Sleep(10 * time.Millisecond)
	second, err := worker.Submit(context.Background(), testJob{wait: block})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Submit(context.Background(), testJob{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("capacity %v", err)
	}
	cancel()
	if got := <-first; !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("active cancel %v", got.Err)
	}
	if got := <-second; !errors.Is(got.Err, ErrWorkerClosed) && !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("queued close %v", got.Err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Submit(context.Background(), testJob{}); !errors.Is(err, ErrWorkerClosed) {
		t.Fatalf("submit closed %v", err)
	}
}

func TestTransferWorkerRejectsPreStartCancellationAndPostShutdownRun(t *testing.T) {
	worker, _ := NewTransferWorker(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.Submit(ctx, testJob{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled submit %v", err)
	}
	worker.Shutdown()
	if err := worker.Run(context.Background()); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("run after close %v", err)
	}
}

func TestTransferWorkerNormalizesWrappedJobDeadline(t *testing.T) {
	worker, _ := NewTransferWorker(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		worker.mu.Lock()
		started := worker.started
		worker.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not start")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := worker.Submit(context.Background(), canaryJob{})
	if err != nil {
		t.Fatal(err)
	}
	got := <-result
	if !errors.Is(got.Err, context.DeadlineExceeded) || strings.Contains(got.Err.Error(), "CANARY") {
		t.Fatalf("job error %q", got.Err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
