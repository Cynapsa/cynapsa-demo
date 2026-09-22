package payload

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type acceptanceTrace struct {
	events      []string
	publication EnvelopePublication
	manifests   []TransferManifest
}

type acceptancePublisher struct{ trace *acceptanceTrace }

func (p acceptancePublisher) PublishPayloadEnvelope(_ context.Context, publication EnvelopePublication) error {
	p.trace.events = append(p.trace.events, "publish-envelope")
	p.trace.publication = publication
	return nil
}

type acceptanceCarrier struct {
	trace    *acceptanceTrace
	name     string
	failDone error
	manifest TransferManifest
}

func (c *acceptanceCarrier) Available(context.Context, CarrierRoute) (bool, error) {
	c.trace.events = append(c.trace.events, c.name+"-available")
	return true, nil
}
func (c *acceptanceCarrier) Begin(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	c.trace.events = append(c.trace.events, c.name+"-begin")
	var err error
	if frame.Encoding == FrameText {
		c.manifest, err = DecodeTextManifest(string(frame.Data))
	} else {
		c.manifest, err = DecodeManifest(frame.Data)
	}
	if err == nil {
		c.trace.manifests = append(c.trace.manifests, cloneManifest(c.manifest))
	}
	return err
}
func (c *acceptanceCarrier) SendChunk(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	c.trace.events = append(c.trace.events, c.name+"-chunk")
	if frame.TransferID != c.manifest.TransferID {
		return ErrAuthentication
	}
	return nil
}
func (c *acceptanceCarrier) Finish(_ context.Context, route CarrierRoute, _ string) (CompletionEvidence, error) {
	c.trace.events = append(c.trace.events, c.name+"-finish")
	if c.failDone != nil {
		return CompletionEvidence{}, c.failDone
	}
	var digest [32]byte
	copy(digest[:], c.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: c.manifest.TransferID, MessageID: c.manifest.MessageID, Digest: digest}, nil
}
func (c *acceptanceCarrier) Abort(context.Context, CarrierRoute, string) error {
	c.trace.events = append(c.trace.events, c.name+"-abort")
	return nil
}

type acceptanceFailingObjects struct{ trace *acceptanceTrace }

func (o acceptanceFailingObjects) Available(context.Context) (bool, error) {
	o.trace.events = append(o.trace.events, "object-available")
	return true, nil
}
func (o acceptanceFailingObjects) Upload(context.Context, []byte) (string, error) {
	o.trace.events = append(o.trace.events, "object-upload")
	return "", ErrCarrierUpload
}
func (o acceptanceFailingObjects) PrepareUpload(context.Context, int64) (PreparedObjectUpload, error) {
	o.trace.events = append(o.trace.events, "object-prepare")
	return &fakePreparedUpload{reference: "https://objects.example/blob", commit: o.Upload}, nil
}
func (acceptanceFailingObjects) Download(context.Context, string) ([]byte, error) {
	return nil, ErrCarrierMaterialization
}

type acceptanceEvidence struct{ trace *acceptanceTrace }

func (e acceptanceEvidence) ConfirmObjectReadiness(context.Context, CarrierRoute, TransferManifest, string) error {
	if e.trace != nil {
		e.trace.events = append(e.trace.events, "object-readiness")
	}
	return nil
}

func (acceptanceEvidence) PublishObject(context.Context, CarrierRoute, TransferManifest, string) error {
	return errors.New("unexpected object publication")
}
func (acceptanceEvidence) AwaitMaterialization(context.Context, CarrierRoute, string) (CompletionEvidence, error) {
	return CompletionEvidence{}, errors.New("unexpected materialization wait")
}
func (acceptanceEvidence) AbortMaterialization(context.Context, CarrierRoute, string) error {
	return nil
}

type legacyObjectStore struct{ uploads int }

func (*legacyObjectStore) Available(context.Context) (bool, error) { return true, nil }
func (store *legacyObjectStore) Upload(context.Context, []byte) (string, error) {
	store.uploads++
	return "https://objects.example/blob", nil
}
func (*legacyObjectStore) Download(context.Context, string) ([]byte, error) {
	return nil, ErrCarrierMaterialization
}

func TestAcceptanceCoordinatorPublishesExactStableDescriptorBeforeEveryCarrier(t *testing.T) {
	trace := &acceptanceTrace{}
	direct := &acceptanceCarrier{trace: trace, name: "direct", failDone: ErrAmbiguousCompletion}
	messages := &acceptanceCarrier{trace: trace, name: "message"}
	coordinator, err := NewCoordinator(testLimits(), direct, acceptanceFailingObjects{trace: trace}, acceptanceEvidence{trace: trace}, messages, fakeCipher{}, acceptancePublisher{trace: trace})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	request := transferRequest()
	receipt, err := coordinator.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Carrier != CarrierMessageChunks {
		t.Fatalf("carrier = %d", receipt.Carrier)
	}
	wantPrefix := []string{"direct-available", "object-available", "message-available", "publish-envelope", "direct-begin"}
	if len(trace.events) < len(wantPrefix) || !reflect.DeepEqual(trace.events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("envelope was not registered first: %v", trace.events)
	}
	readiness, upload := -1, -1
	for index, event := range trace.events {
		if event == "object-readiness" {
			readiness = index
		}
		if event == "object-upload" {
			upload = index
		}
	}
	if readiness < 0 || upload < 0 || readiness >= upload {
		t.Fatalf("object bytes were attempted before recipient readiness: %v", trace.events)
	}
	if len(trace.manifests) != 2 || !manifestsEqual(trace.manifests[0], trace.manifests[1]) {
		t.Fatalf("fallback rewrote manifest: count=%d", len(trace.manifests))
	}
	manifest := trace.manifests[0]
	descriptor := trace.publication.Descriptor
	if descriptor.Kind != protocol.PayloadTransferReference || descriptor.Reference != manifest.TransferID || descriptor.EncryptionRef != manifest.EncryptionRef || descriptor.Profile != request.Profile || descriptor.Size != int64(len(request.Canonical)) || !bytes.Equal(descriptor.Digest[:], manifest.CanonicalDigest) {
		t.Fatalf("published descriptor does not exactly bind stable manifest: %#v %#v", descriptor, manifest)
	}
	if receipt.TransferID != descriptor.Reference || receipt.MessageID != request.MessageID || receipt.Descriptor.Reference != descriptor.Reference || receipt.Descriptor.EncryptionRef != descriptor.EncryptionRef || receipt.Descriptor.Digest != descriptor.Digest {
		t.Fatalf("receipt changed logical descriptor: %#v", receipt)
	}
	if trace.publication.Route != routeFor(request) || trace.publication.ConversationID != request.ConversationID || trace.publication.Mode != request.Mode {
		t.Fatalf("logical envelope identity changed: %#v", trace.publication)
	}
}

func TestAcceptanceObjectAdapterCannotBypassRecipientReadiness(t *testing.T) {
	objects := &legacyObjectStore{}
	messages := &acceptanceCarrier{trace: &acceptanceTrace{}, name: "message"}
	coordinator, err := NewCoordinator(testLimits(), nil, objects, acceptanceEvidence{}, messages, fakeCipher{}, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	receipt, err := coordinator.Send(context.Background(), transferRequest())
	if err != nil || receipt.Carrier != CarrierMessageChunks {
		t.Fatalf("fallback = %#v, %v", receipt, err)
	}
	if objects.uploads != 0 {
		t.Fatalf("legacy object adapter uploaded %d times", objects.uploads)
	}
}
