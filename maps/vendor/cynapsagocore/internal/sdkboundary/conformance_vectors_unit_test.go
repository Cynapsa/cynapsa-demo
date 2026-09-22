package sdkboundary

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

type discriminatorVector struct {
	Name string          `json:"name"`
	JSON json.RawMessage `json:"json"`
}

func assertConformanceVectors(t *testing.T, filename string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("..", "..", "conformance", "v1", filename)
	if os.Getenv("CYNAPSA_UPDATE_VECTORS") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, encoded) {
		t.Fatalf("%s is stale; regenerate with CYNAPSA_UPDATE_VECTORS=1", path)
	}
}

func TestStaticOperationConformanceVectorIsStrictAndComplete(t *testing.T) {
	path := filepath.Join("..", "..", "conformance", "v1", "operations.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := rejectDuplicateJSONFields(data); err != nil {
		t.Fatalf("strict operation vectors: %v", err)
	}
	var operations map[string]json.RawMessage
	if err := json.Unmarshal(data, &operations); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"core_create", "command_cancel", "admission", "failed_completion", "status", "payload_handle", "payload_write", "payload_read", "normalized_error", "authorization_failure"} {
		if len(operations[name]) == 0 {
			t.Errorf("missing operation vector %s", name)
		}
	}
	if len(operations) != 10 {
		t.Errorf("operation vectors=%d want 10", len(operations))
	}
	var coreCreate struct {
		MaximumPayloadBytes uint64 `json:"maximum_payload_bytes"`
	}
	if err := json.Unmarshal(operations["core_create"], &coreCreate); err != nil {
		t.Fatal(err)
	}
	if coreCreate.MaximumPayloadBytes != v1.MaximumPayloadBytes {
		t.Errorf("maximum_payload_bytes=%d want %d", coreCreate.MaximumPayloadBytes, v1.MaximumPayloadBytes)
	}
	var authorization struct {
		OK    bool `json:"ok"`
		Error *struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
			Stage     string `json:"stage"`
			Location  string `json:"local_or_remote"`
		} `json:"error"`
	}
	if err := json.Unmarshal(operations["authorization_failure"], &authorization); err != nil {
		t.Fatal(err)
	}
	if authorization.OK || authorization.Error == nil || authorization.Error.Code != string(v1.ErrorCodeAuthorizationRejected) || authorization.Error.Stage != string(v1.ErrorStagePolicy) || authorization.Error.Location != string(v1.ErrorLocationLocal) || authorization.Error.Retryable {
		t.Errorf("authorization failure vector=%#v", authorization)
	}
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	completion, err := adapter.MapCompletion(model.Result{CommandID: "command-authorization-rejected", Err: &model.Error{Code: "authorization_rejected", Stage: "policy", Location: "local"}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := adapter.EncodeABICompletion(completion)
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, operations["authorization_failure"]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, compact.Bytes()) {
		t.Errorf("authorization failure vector does not match boundary output\ngot:  %s\nwant: %s", encoded, compact.Bytes())
	}
}
