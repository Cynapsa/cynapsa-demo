package doccontract_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeCommandGateDocumentsBorrowedShutdownQuarantine(t *testing.T) {
	document, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docs", "development", "POD_02_RUNTIME_COMMAND_GATE.md"))
	if err != nil {
		t.Fatalf("read runtime command-gate plan: %v", err)
	}
	text := string(document)
	for _, required := range []string{
		"Timed-out shutdown releases queued and otherwise unborrowed resources",
		"a non-cooperative dispatched borrow remains bounded, quarantined, and charged past the caller deadline until the dispatcher returns",
		"then is zeroed and released exactly once",
		"An already-borrowed completion lease similarly delays only the one lifecycle-owned finalizer until exact commit or rollback",
		"the caller deadline still returns",
		"The closed gate admits and dispatches no new work",
		"native unload requires joining the dispatcher",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("runtime command-gate plan is missing shutdown contract %q", required)
		}
	}
	if strings.Contains(text, "timed-out shutdown release all owned resources") {
		t.Error("runtime command-gate plan retains the obsolete immediate-release shutdown claim")
	}
}
