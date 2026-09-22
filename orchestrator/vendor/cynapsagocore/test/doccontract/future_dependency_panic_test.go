package doccontract

import (
	"os"
	"strings"
	"testing"
)

func TestPrivateDependencyPanicContainmentImplementationDocs(t *testing.T) {
	pod4, err := os.ReadFile("../../docs/development/POD_04_CONNECTIVITY.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Narrow V1 exception (2026-08-24): dependency panic and error containment",
		"`xep0363.SlotRequester.RequestSlot`",
		"`xep0363.Resolver.LookupIPAddr`",
		"Their ordinary error normalization, context precedence, result ownership",
		"All\nother connectivity dependency callbacks and goroutines remain subject",
		"- Dependency panic and error containment.",
		"AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment",
	} {
		if !strings.Contains(string(pod4), required) {
			t.Fatalf("POD_04_CONNECTIVITY.md missing narrow V1 exception contract %q", required)
		}
	}

	pod5, err := os.ReadFile("../../docs/development/POD_05_PAYLOAD_SYSTEM.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Narrow V1 exception (2026-08-24): worker and dependency panic containment",
		"exactly `payload.ObjectStore.Download`, both at synchronous materialization",
		"Ordinary download errors, context precedence, owned-result cleanup",
		"All transfer workers and every other payload dependency boundary",
		"- Transfer-worker capacity, shutdown, job cancellation, panic containment, and resource release.",
		"AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment",
	} {
		if !strings.Contains(string(pod5), required) {
			t.Fatalf("POD_05_PAYLOAD_SYSTEM.md missing narrow V1 exception contract %q", required)
		}
	}
}
