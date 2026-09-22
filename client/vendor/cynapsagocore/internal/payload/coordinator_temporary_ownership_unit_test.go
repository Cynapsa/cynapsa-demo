package payload

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type temporaryOwnershipPublisher struct {
	borrowed   []byte
	retained   []byte
	sawPayload bool
	result     error
	panic      bool
}

type discardingTemporaryPublisher struct{}

func (discardingTemporaryPublisher) PublishPayloadEnvelope(context.Context, EnvelopePublication) error {
	return nil
}

func (publisher *temporaryOwnershipPublisher) PublishPayloadEnvelope(_ context.Context, publication EnvelopePublication) error {
	publisher.borrowed = publication.Descriptor.Inline
	publisher.retained = clone(publication.Descriptor.Inline)
	publisher.sawPayload = anyNonzero(publication.Descriptor.Inline)
	if publisher.panic {
		panic("publisher panic")
	}
	return publisher.result
}

func TestCoordinatorPublicationTemporaryOwnership(t *testing.T) {
	for _, test := range []struct {
		name      string
		result    error
		panic     bool
		wantError error
	}{
		{name: "success"},
		{name: "error", result: ErrPublicationRejected, wantError: ErrPublicationRejected},
		{name: "panic", panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			publisher := &temporaryOwnershipPublisher{result: test.result, panic: test.panic}
			coordinator, err := NewCoordinator(testLimits(), nil, nil, nil, nil, nil, publisher)
			if err != nil {
				t.Fatal(err)
			}
			request := transferRequest()
			wantRequest := clone(request.Canonical)
			descriptor, err := protocol.NewInlinePayload(request.Profile, request.Canonical)
			if err != nil {
				t.Fatal(err)
			}
			wantDescriptor := clone(descriptor.Inline)
			panicked := payloadCaughtPanic(func() {
				err = coordinator.publish(context.Background(), request, routeFor(request), descriptor)
			})
			if panicked != test.panic {
				t.Fatalf("publish panic = %t, want %t", panicked, test.panic)
			}
			if !test.panic && !errors.Is(err, test.wantError) {
				t.Fatalf("publish error = %v, want %v", err, test.wantError)
			}
			if len(publisher.borrowed) == 0 || !publisher.sawPayload {
				t.Fatal("publisher did not observe a live owned snapshot")
			}
			assertTemporaryZero(t, publisher.borrowed)
			if !bytes.Equal(publisher.retained, wantDescriptor) {
				t.Fatal("publisher's explicit retained clone was changed")
			}
			if !bytes.Equal(descriptor.Inline, wantDescriptor) {
				t.Fatal("publish changed caller-owned descriptor bytes")
			}
			if !bytes.Equal(request.Canonical, wantRequest) {
				t.Fatal("publish changed caller-owned request bytes")
			}
			if &publisher.borrowed[0] == &descriptor.Inline[0] || &publisher.borrowed[0] == &request.Canonical[0] {
				t.Fatal("dependency snapshot aliased caller-owned bytes")
			}
			zero(publisher.retained)
			zero(descriptor.Inline)
			zero(wantDescriptor)
			zero(wantRequest)
		})
	}
}

func TestCoordinatorPublicationOwnershipAllocationBound(t *testing.T) {
	coordinator, err := NewCoordinator(testLimits(), nil, nil, nil, nil, nil, discardingTemporaryPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest()
	descriptor, err := protocol.NewInlinePayload(request.Profile, request.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if publishErr := coordinator.publish(context.Background(), request, routeFor(request), descriptor); publishErr != nil {
			panic(publishErr)
		}
	})
	if allocations > 1 {
		t.Fatalf("publication ownership allocated %.2f times, want one dependency snapshot", allocations)
	}
	zero(descriptor.Inline)
	zero(request.Canonical)
}

type temporaryOwnershipCarrier struct {
	borrowed        []byte
	retained        []byte
	manifest        TransferManifest
	sawPayload      bool
	result          error
	panic           bool
	aborts          int
	cleared         bool
	abortSawCleared bool
	abortPanic      any
}

func (*temporaryOwnershipCarrier) Available(context.Context, CarrierRoute) (bool, error) {
	return true, nil
}

func (carrier *temporaryOwnershipCarrier) Begin(_ context.Context, _ CarrierRoute, frame CarrierFrame) error {
	carrier.borrowed = frame.Data
	carrier.retained = clone(frame.Data)
	carrier.sawPayload = anyNonzero(frame.Data)
	carrier.manifest, _ = DecodeManifest(frame.Data)
	if carrier.panic {
		panic("begin panic")
	}
	return carrier.result
}

func (carrier *temporaryOwnershipCarrier) SendChunk(context.Context, CarrierRoute, CarrierFrame) error {
	carrier.cleared = !anyNonzero(carrier.borrowed)
	return nil
}

func (carrier *temporaryOwnershipCarrier) Finish(_ context.Context, route CarrierRoute, transferID string) (CompletionEvidence, error) {
	var digest [32]byte
	copy(digest[:], carrier.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: transferID, MessageID: carrier.manifest.MessageID, Digest: digest}, nil
}

func (carrier *temporaryOwnershipCarrier) Abort(context.Context, CarrierRoute, string) error {
	carrier.aborts++
	carrier.abortSawCleared = !anyNonzero(carrier.borrowed)
	if carrier.abortPanic != nil {
		panic(carrier.abortPanic)
	}
	return nil
}

func TestManifestFrameTemporaryOwnership(t *testing.T) {
	for _, test := range []struct {
		name      string
		result    error
		panic     bool
		wantError error
		wantAbort int
	}{
		{name: "success"},
		{name: "error", result: ErrCarrierRejected, wantError: ErrCarrierRejected, wantAbort: 1},
		{name: "panic", panic: true, wantAbort: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			canonical := testCanonical("manifest temporary " + test.name)
			wantCanonical := clone(canonical)
			_, manifest, _ := buildTestTransfer(t, canonical, canonical, 8)
			wantCanonicalDigest := clone(manifest.CanonicalDigest)
			wantTransferredDigest := clone(manifest.TransferredDigest)
			carrier := &temporaryOwnershipCarrier{result: test.result, panic: test.panic}
			if test.panic {
				carrier.abortPanic = "abort panic"
			}
			var sendErr error
			recovered := payloadRecovered(func() {
				_, sendErr = sendChunks(context.Background(), carrier, CarrierRoute{}, manifest, canonical, FrameBinary, 4096, time.Second)
			})
			panicked := recovered != nil
			if panicked != test.panic {
				t.Fatalf("Begin panic = %t, want %t", panicked, test.panic)
			}
			if test.panic && recovered != "begin panic" {
				t.Fatalf("recovered panic = %v, want original Begin panic", recovered)
			}
			if !test.panic && !errors.Is(sendErr, test.wantError) {
				t.Fatalf("sendChunks error = %v, want %v", sendErr, test.wantError)
			}
			if len(carrier.borrowed) == 0 || !carrier.sawPayload {
				t.Fatal("carrier did not observe a live manifest snapshot")
			}
			assertTemporaryZero(t, carrier.borrowed)
			if !bytes.Equal(carrier.retained, mustEncodeManifest(t, manifest)) {
				t.Fatal("carrier's explicit retained manifest clone was changed")
			}
			if !bytes.Equal(canonical, wantCanonical) || !bytes.Equal(manifest.CanonicalDigest, wantCanonicalDigest) || !bytes.Equal(manifest.TransferredDigest, wantTransferredDigest) {
				t.Fatal("sendChunks changed caller-owned transfer bytes")
			}
			if carrier.aborts != test.wantAbort {
				t.Fatalf("abort count = %d, want %d", carrier.aborts, test.wantAbort)
			}
			if test.wantAbort != 0 && !carrier.abortSawCleared {
				t.Fatal("manifest snapshot remained live when Abort callback began")
			}
			if !test.panic && test.result == nil && !carrier.cleared {
				t.Fatal("manifest snapshot remained live after Begin returned")
			}
			zero(carrier.retained)
			zero(carrier.manifest.CanonicalDigest)
			zero(carrier.manifest.TransferredDigest)
			zero(wantCanonical)
			zero(wantCanonicalDigest)
			zero(wantTransferredDigest)
		})
	}
}

func mustEncodeManifest(t *testing.T, manifest TransferManifest) []byte {
	t.Helper()
	encoded, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { zero(encoded) })
	return encoded
}

func payloadCaughtPanic(call func()) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	call()
	return false
}

func payloadRecovered(call func()) (recovered any) {
	defer func() { recovered = recover() }()
	call()
	return nil
}

func anyNonzero(value []byte) bool {
	for _, octet := range value {
		if octet != 0 {
			return true
		}
	}
	return false
}

func assertTemporaryZero(t *testing.T, value []byte) {
	t.Helper()
	if len(value) == 0 {
		t.Fatal("ownership canary did not capture bytes")
	}
	for index, octet := range value {
		if octet != 0 {
			t.Fatalf("owned temporary remained non-zero at byte %d", index)
		}
	}
}
