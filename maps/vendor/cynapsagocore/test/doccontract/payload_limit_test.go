package doccontract_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestV1PayloadLimitDocumentationAndBindingMetadataStayAligned(t *testing.T) {
	repository := repositoryRoot(t)
	const decimal = "134,217,696 bytes"
	for _, name := range []string{
		"docs/adr/0004-v1-process-lifetime.md",
		"docs/adr/0005-multi-carrier-large-payload-transfer.md",
		"docs/development/V1_TASK_LEDGER.md",
	} {
		data, err := os.ReadFile(filepath.Join(repository, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(data), decimal) {
			t.Errorf("%s is missing exact V1 payload ceiling %q", name, decimal)
		}
	}

	obsoleteCeiling := "1 " + "GiB"
	err := filepath.WalkDir(repository, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".md") {
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if strings.Contains(string(data), obsoleteCeiling) {
				relative, _ := filepath.Rel(repository, path)
				t.Errorf("%s retains obsolete payload ceiling %q", relative, obsoleteCeiling)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(repository, "conformance", "v1", "operations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var operations struct {
		CoreCreate struct {
			MaximumPayloadBytes uint64 `json:"maximum_payload_bytes"`
		} `json:"core_create"`
	}
	if err := json.Unmarshal(data, &operations); err != nil {
		t.Fatal(err)
	}
	if operations.CoreCreate.MaximumPayloadBytes != v1.MaximumPayloadBytes {
		t.Fatalf("binding maximum_payload_bytes=%d want %d", operations.CoreCreate.MaximumPayloadBytes, v1.MaximumPayloadBytes)
	}
}
