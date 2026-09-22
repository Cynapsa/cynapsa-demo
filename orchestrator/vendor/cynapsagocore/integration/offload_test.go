package integration_test

import (
	"context"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// TestLargePayloadOffload will verify integrity and cleanup for payloads that exceed inline limits.
func TestLargePayloadOffload(t *testing.T) {
	carrier, publisher := &fixtureCarrier{available: true}, &fixturePublisher{}
	coordinator, err := payload.NewCoordinator(integrationLimits(), carrier, nil, nil, nil, fixtureCipher{}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest()
	receipt, err := coordinator.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Descriptor.Kind != protocol.PayloadTransferReference || receipt.Descriptor.Size != int64(len(request.Canonical)) || receipt.Descriptor.EncryptionRef == "" {
		t.Fatalf("offload descriptor=%#v", receipt.Descriptor)
	}
	if receipt.Descriptor.Digest != payload.Digest(request.Canonical) || carrier.manifest.CanonicalSize != int64(len(request.Canonical)) {
		t.Fatal("offload integrity metadata does not bind canonical bytes")
	}
	failing := &fixtureCarrier{available: true, finishErr: payload.ErrCarrierRejected}
	failedCoordinator, err := payload.NewCoordinator(integrationLimits(), failing, nil, nil, nil, fixtureCipher{}, &fixturePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failedCoordinator.Send(context.Background(), request); err == nil || failing.aborts != 1 {
		t.Fatalf("failed offload err=%v aborts=%d", err, failing.aborts)
	}
}
