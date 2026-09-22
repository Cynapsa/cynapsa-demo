package coturn_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistAgentLogOutputNeverOverwritesWithEmptyOrCredential(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.log")
	if err := os.WriteFile(path, []byte("retained\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := persistAgentLogOutput(path, "", nil); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(path); err != nil || string(content) != "retained\n" {
		t.Fatalf("empty output changed retained log: %q, %v", content, err)
	}
	if err := persistAgentLogOutput(path, "contains-secret\n", []string{"secret"}); err == nil {
		t.Fatal("credential-bearing output was persisted")
	}
	if content, err := os.ReadFile(path); err != nil || string(content) != "retained\n" {
		t.Fatalf("credential rejection changed retained log: %q, %v", content, err)
	}
	if err := persistAgentLogOutput(path, "fresh-safe\n", []string{"secret"}); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(path); err != nil || string(content) != "fresh-safe\n" {
		t.Fatalf("safe output not persisted: %q, %v", content, err)
	}
}

func TestPersistThenRemoveAgentOrdersPersistenceBeforeCleanup(t *testing.T) {
	var order []string
	err := persistThenRemoveAgent(func() error {
		order = append(order, "persist")
		return nil
	}, func() {
		order = append(order, "remove")
	})
	if err != nil || len(order) != 2 || order[0] != "persist" || order[1] != "remove" {
		t.Fatalf("cleanup order=%v err=%v", order, err)
	}
}

func TestRetainedAgentLogRequiresNonemptyRegularFile(t *testing.T) {
	directory := t.TempDir()
	if retainedAgentLogExists(directory) {
		t.Fatal("directory treated as retained agent evidence")
	}
	path := filepath.Join(directory, "agent.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if retainedAgentLogExists(path) {
		t.Fatal("empty file treated as retained agent evidence")
	}
	if err := os.WriteFile(path, []byte("evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !retainedAgentLogExists(path) {
		t.Fatal("nonempty regular evidence was not retained")
	}
}
