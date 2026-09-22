package integration_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

// TestSDKBoundaryRejectsUnknownInput will verify that public decoding fails closed.
func TestSDKBoundaryRejectsUnknownInput(t *testing.T) {
	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	tests := []string{
		`{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"unknown":true}`,
		`{"abi_version":1,"command_id":"c","command_name":"future.command","sdk_session_id":"s","args":{}}`,
		`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"agent","payload":{"kind":"future"},"ttl_ms":1,"priority":"normal"}}`,
		`{"abi_version":1,"command_id":"c","command_name":"core.status","command_name":"core.init","sdk_session_id":"s","args":{}}`,
	}
	for _, input := range tests {
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, sdkboundary.ErrMalformedInput) {
			t.Errorf("input %s error=%v", input, err)
		}
	}
}

// TestSDKBoundaryRedactsInternalDetails will verify that every public output path is transport opaque.
func TestSDKBoundaryRedactsInternalDetails(t *testing.T) {
	adapter, _ := sdkboundary.New()
	private := &model.Error{Code: "payload_transfer_failed", Stage: "payload", Location: "remote", Retryable: true, Cause: errors.New("secret://user:pass@xmpp.internal XEP-0363 rank-2 canary")}
	public := adapter.MapError(private)
	encoded, err := adapter.EncodeABIError(*public)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("secret"), []byte("user:pass"), []byte("xmpp"), []byte("XEP"), []byte("rank-2"), []byte("canary")} {
		if bytes.Contains(bytes.ToLower(encoded), bytes.ToLower(forbidden)) {
			t.Errorf("public error leaked %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"code":"payload_transfer_failed"`) || !strings.Contains(string(encoded), `"stage":"payload"`) {
		t.Fatalf("normalized output=%s", encoded)
	}
}

// TestSDKBoundaryNormalizesUnknownState will verify that new internal states cannot leak through the SDK.
func TestSDKBoundaryNormalizesUnknownState(t *testing.T) {
	adapter, _ := sdkboundary.New()
	status := adapter.MapStatus(model.Status{Lifecycle: model.LifecycleState("future-private-state"), ConnectivityDetail: "future-path", Personality: "future-sdk", AgentID: "agent", MeshID: "mesh", MeshEndpoint: "mesh.test:5222", QueuedMessageCount: 7})
	if status.Lifecycle != "failed" || status.Connectivity != "unknown" || status.Personality != "unset" {
		t.Fatalf("unknown state projection=%#v", status)
	}
	private := &model.Error{Code: "future_error", Stage: "future_stage", Location: "future_location", DiagnosticID: "not-canonical"}
	public := adapter.MapError(private)
	if public.Code != "core_error" || public.Stage != "command" || public.Location != "local" || public.DiagnosticID != "" {
		t.Fatalf("unknown error projection=%#v", public)
	}
}
