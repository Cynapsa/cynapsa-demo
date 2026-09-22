package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

// TestLargePayloadUsesDirectChunks will verify bounded direct transfer over a healthy live link.
func TestLargePayloadUsesDirectChunks(t *testing.T) {
	direct, publisher := &fixtureCarrier{available: true}, &fixturePublisher{}
	coordinator, err := payload.NewCoordinator(integrationLimits(), direct, nil, nil, nil, nil, publisher)
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest()
	receipt, err := coordinator.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Carrier != payload.CarrierDirectChunks || direct.chunks < 2 || direct.aborts != 0 || len(publisher.publications) != 1 {
		t.Fatalf("receipt=%#v chunks=%d aborts=%d publications=%d", receipt, direct.chunks, direct.aborts, len(publisher.publications))
	}
	if receipt.MessageID != request.MessageID || receipt.Descriptor.Digest != payload.Digest(request.Canonical) {
		t.Fatal("direct transfer changed logical identity or digest")
	}
	if direct.manifest.EncryptionRef != "" || receipt.Descriptor.EncryptionRef != "" {
		t.Fatal("V1 direct transfer unexpectedly used application encryption")
	}
}

// TestLargePayloadFallsBackFromObjectUploadToMessageChunks will verify the final ADR 0005 fallback.
func TestLargePayloadFallsBackFromObjectUploadToMessageChunks(t *testing.T) {
	direct := &fixtureCarrier{available: false}
	messages := &fixtureCarrier{available: true}
	publisher, evidence := &fixturePublisher{}, &unusedEvidence{}
	coordinator, err := payload.NewCoordinator(integrationLimits(), direct, failingObjectStore{available: true}, evidence, messages, nil, publisher)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := coordinator.Send(context.Background(), transferRequest())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Carrier != payload.CarrierMessageChunks || messages.manifest.EncryptionRef != "" || receipt.Descriptor.EncryptionRef != "" || messages.chunks < 2 || evidence.aborts != 1 {
		t.Fatalf("fallback receipt=%#v chunks=%d object aborts=%d", receipt, messages.chunks, evidence.aborts)
	}
}

// TestLargePayloadCarrierAmbiguityDeliversOnce will verify exactly-once application delivery across carrier races.
func TestLargePayloadCarrierAmbiguityDeliversOnce(t *testing.T) {
	direct := &fixtureCarrier{available: true, finishErr: payload.ErrAmbiguousCompletion}
	messages := &fixtureCarrier{available: true}
	publisher := &fixturePublisher{}
	coordinator, err := payload.NewCoordinator(integrationLimits(), direct, failingObjectStore{}, &unusedEvidence{}, messages, fixtureCipher{}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := coordinator.Send(context.Background(), transferRequest())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Carrier != payload.CarrierMessageChunks || len(publisher.publications) != 1 || direct.manifest.TransferID != messages.manifest.TransferID || direct.aborts != 1 {
		t.Fatalf("ambiguous fallback receipt=%#v publications=%d direct aborts=%d", receipt, len(publisher.publications), direct.aborts)
	}
}

// TestLargePayloadAllCarriersFail will verify terminal failure and cleanup when no mechanism can finish.
func TestLargePayloadAllCarriersFail(t *testing.T) {
	messages := &fixtureCarrier{available: true, finishErr: payload.ErrCarrierRejected}
	publisher := &fixturePublisher{}
	coordinator, err := payload.NewCoordinator(integrationLimits(), nil, failingObjectStore{available: true}, &unusedEvidence{}, messages, fixtureCipher{}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.Send(context.Background(), transferRequest())
	if !errors.Is(err, payload.ErrAllCarriersFailed) || messages.aborts != 1 || len(publisher.publications) != 1 {
		t.Fatalf("terminal result=%v aborts=%d publications=%d", err, messages.aborts, len(publisher.publications))
	}
}
