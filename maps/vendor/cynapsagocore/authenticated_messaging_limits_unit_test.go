package cynapsagocore

import (
	"errors"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

func TestAuthenticatedPayloadLimitsExactPublicBoundary(t *testing.T) {
	profile, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	limits, err := authenticatedPayloadLimits(model.RuntimeConfig{PayloadLimit: v1.MaximumPayloadBytes}, profile)
	if err != nil {
		t.Fatalf("exact maximum rejected: %v", err)
	}
	wantReassembly := int64(v1.MaximumPayloadBytes)*2 + payload.MaximumTransferredOverheadBytes
	if wantReassembly != payload.MaximumReassemblyBytes {
		t.Fatalf("public maximum derivation = %d, want reassembly ceiling %d", wantReassembly, payload.MaximumReassemblyBytes)
	}
	if limits.MaximumPayloadBytes != int64(v1.MaximumPayloadBytes) || limits.ReassemblyBytes != payload.MaximumReassemblyBytes {
		t.Fatalf("limits = %#v", limits)
	}
	if _, err := authenticatedPayloadLimits(model.RuntimeConfig{PayloadLimit: v1.MaximumPayloadBytes + 1}, profile); !errors.Is(err, payload.ErrInvalidLimits) {
		t.Fatalf("maximum + 1 error = %v, want invalid limits", err)
	}
}
