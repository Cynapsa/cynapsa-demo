package doccontract

import (
	"os"
	"strings"
	"testing"
)

func TestPreparedUploadOwnershipContract(t *testing.T) {
	coordinator, err := os.ReadFile("../../internal/payload/coordinator.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"defer abortPreparedObjectUpload(prepared)",
		"func abortPreparedObjectUpload(prepared PreparedObjectUpload)",
		"defer func() { _ = recover() }()",
	} {
		if !strings.Contains(string(coordinator), required) {
			t.Fatalf("prepared-upload implementation missing ownership contract %q", required)
		}
	}

	transfer, err := os.ReadFile("../../internal/payload/transfer.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(transfer), "accompanied by an error and must be Abort'ed exactly once") {
		t.Fatal("PreparedObjectUpload interface is missing value-plus-error ownership semantics")
	}

	adr, err := os.ReadFile("../../docs/adr/0005-multi-carrier-large-payload-transfer.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Every non-nil prepared-upload handle transfers to the coordinator even when",
		"invokes `Abort` exactly once after commit or on every",
		"contained so it cannot replace the original carrier result or panic",
	} {
		if !strings.Contains(string(adr), required) {
			t.Fatalf("ADR 0005 missing prepared-upload ownership contract %q", required)
		}
	}
}
