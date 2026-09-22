package doccontract

import (
	"os"
	"strings"
	"testing"
)

func TestPrivatePolicyDependencyPanicContainmentImplementationDocs(t *testing.T) {
	pod3, err := os.ReadFile("../../docs/development/POD_03_MESSAGING_SEMANTICS.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Narrow V1 exception (2026-08-24): fail-closed policy enforcement",
		"exactly `policy.GateConfig.Now`",
		"`policy.MembershipVerifier.RequireAll`",
		"`policy.ApplicationPathExtractor.ExtractApplicationPath`",
		"`policy.RuleAuthorizer.Allows`",
		"Their ordinary result and error handling, bounded error mapping",
		"authenticated identity, fail-closed authorization, caller ownership",
		"All other policy and messaging dependencies, callbacks, goroutines, cleanup",
		"AZTM_FUTURE_ISSUES.md#12-private-policy-dependency-panic-containment",
		"An inbound policy gate that verifies integrity, authenticated identity, mesh scope, membership, and application authorization in a fail-closed order.",
		"Any nondeterministic codec, unbounded decode, authorization bypass",
	} {
		if !strings.Contains(string(pod3), required) {
			t.Fatalf("POD_03_MESSAGING_SEMANTICS.md missing narrow policy panic exception %q", required)
		}
	}
}
