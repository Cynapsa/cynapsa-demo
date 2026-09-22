package sessionkernel_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

func FuzzStageBExactBoundIdentity(f *testing.F) {
	for _, seed := range []string{
		"agent@example.test/mesh-one",
		"agent@example.test/mesh-two",
		"agent@example.test",
		"",
		"agent@example.test/mesh-one/extra",
		"agent@example.test/mesh-\x00one",
		"\xff",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, boundIdentity string) {
		service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
			AgentID: "agent@example.test", BoundIdentity: boundIdentity, MeshID: "mesh-one",
		}}
		controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
		result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
		wantSuccess := boundIdentity == "agent@example.test/mesh-one"
		if wantSuccess {
			if err != nil || result.Err != nil || !controller.Snapshot().Authenticated {
				t.Fatalf("exact identity rejected: result=%+v err=%v snapshot=%+v", result, err, controller.Snapshot())
			}
			if text := fmt.Sprintf("%+v", result); text == "" || strings.Contains(text, boundIdentity) {
				t.Fatalf("unexpected result serialization: %q", text)
			}
			return
		}
		if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != "authentication_failed" || controller.Snapshot().Authenticated {
			t.Fatalf("mismatched bound identity accepted: bound=%q result=%+v err=%v snapshot=%+v", boundIdentity, result, err, controller.Snapshot())
		}
	})
}

func FuzzStageBProviderErrorCodeNormalization(f *testing.F) {
	for _, seed := range []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 255} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw uint8) {
		local := sessionkernel.LocalOperations{PayloadOpen: func(context.Context, coreruntime.Services, model.EmptyArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
			return model.PayloadHandleResult{}, &sessionkernel.ProviderError{Code: sessionkernel.ProviderErrorCode(raw)}
		}}
		_, dependencies := qaFactory(t, sessionkernel.Dependencies{Local: local})
		result, err := dependencies.Handlers["payload.open"](context.Background(), qaServices{}, model.Command{ID: "fuzz", Name: "payload.open", Args: model.EmptyArgs{}})
		if err != nil || result.Value != nil || result.Err == nil {
			t.Fatalf("provider code %d was not normalized: result=%+v err=%v", raw, result, err)
		}
		allowed := map[string]bool{
			"request_cancelled": true, "connectivity_unavailable": true, "authentication_failed": true,
			"command_error": true, "authorization_rejected": true, "queue_full": true, "invalid_handle": true,
			"payload_too_large": true, "payload_integrity_failed": true,
			"payload_transfer_failed": true, "core_error": true,
		}
		if !allowed[result.Err.Code] {
			t.Fatalf("provider code %d produced open error code %q", raw, result.Err.Code)
		}
		if raw == 0 || raw > uint8(sessionkernel.ProviderInternal) {
			if result.Err.Code != "core_error" || result.Err.Retryable {
				t.Fatalf("unknown provider code %d = %+v", raw, result.Err)
			}
		}
	})
}
