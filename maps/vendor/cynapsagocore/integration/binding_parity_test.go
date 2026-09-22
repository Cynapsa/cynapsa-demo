package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestConformanceVectorIntegrity verifies the language-neutral inputs consumed
// by external SDK qualification. It deliberately makes no claim that absent
// Python or TypeScript repositories have executed these vectors.
func TestConformanceVectorIntegrity(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "conformance", "v1", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no committed binding conformance vectors")
	}
	required := map[string]bool{"operations.json": false}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		firstDigest := sha256.Sum256(data)
		again, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if firstDigest != sha256.Sum256(again) || !bytes.Equal(data, again) {
			t.Errorf("%s changed during conformance read", path)
		}
		if _, ok := required[filepath.Base(path)]; ok {
			required[filepath.Base(path)] = true
		}
	}
	for name, found := range required {
		if !found {
			t.Errorf("missing %s", name)
		}
	}
	operations, err := os.ReadFile(filepath.Join("..", "conformance", "v1", "operations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(operations, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"core_create", "command_cancel", "admission", "failed_completion", "status", "payload_handle", "payload_write", "payload_read", "normalized_error", "authorization_failure"} {
		if len(vectors[name]) == 0 {
			t.Errorf("missing operation vector %s", name)
		}
	}
	if len(vectors) != 10 {
		t.Fatalf("operation vector count=%d want 10", len(vectors))
	}
	var authorization struct {
		OK    bool `json:"ok"`
		Error *struct {
			Code      string `json:"code"`
			Stage     string `json:"stage"`
			Location  string `json:"local_or_remote"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(vectors["authorization_failure"], &authorization); err != nil {
		t.Fatal(err)
	}
	if authorization.OK || authorization.Error == nil || authorization.Error.Code != "authorization_rejected" || authorization.Error.Stage != "policy" || authorization.Error.Location != "local" || authorization.Error.Retryable {
		t.Fatalf("authorization failure vector=%#v", authorization)
	}
}
