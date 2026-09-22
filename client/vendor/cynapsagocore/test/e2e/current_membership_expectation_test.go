package e2e_test

import (
	"sort"
	"strings"
	"testing"
)

// currentMembershipAdminSnapshot mirrors the generation-free restricted
// server command: one canonical bare JID per line, in lexical order.
func currentMembershipAdminSnapshot(members ...string) string {
	current := append([]string(nil), members...)
	sort.Strings(current)
	return strings.Join(current, "\n")
}

func TestCurrentMembershipAdminSnapshotExpectationIsGenerationFree(t *testing.T) {
	const want = "agent-a@mesh.test\nagent-b@mesh.test"
	got := currentMembershipAdminSnapshot("agent-b@mesh.test", "agent-a@mesh.test")
	if got != want {
		t.Fatalf("current membership admin snapshot = %q, want %q", got, want)
	}
	if strings.ContainsAny(got, "\t,") || strings.Contains(got, "generation") {
		t.Fatalf("current membership admin snapshot retained obsolete generation/list framing: %q", got)
	}
}
