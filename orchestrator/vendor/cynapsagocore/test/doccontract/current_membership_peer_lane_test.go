package doccontract_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCurrentMembershipPeerLaneArchitectureIsImplemented(t *testing.T) {
	repository := repositoryRoot(t)
	adr := compactPeerLaneDocument(readPeerLaneDocument(t, repository, "docs", "adr", "0008-current-membership-peer-lanes.md"))

	for _, required := range []string{
		"Status: Accepted",
		"Implementation status: Implemented by the private V2 wire migration",
		"Two authenticated users may communicate exactly when both are current members",
		"A complete snapshot from that session is the sole membership capability used by the application plane",
		"If an identity was removed and re-added while the Core was offline, and is present in the current snapshot, it is authorized now",
		"Membership-change notices are live control wakeups, not durable messages",
		"Snapshot publication and live-update subscription are serialized at the server",
		"Synchronizing: authenticated or recovering, complete snapshot not installed",
		"One goroutine owns each active lane's mutable state",
		"one Core-wide count and byte budget",
		"While membership authority is `Paused` or `Synchronizing`",
		"Locally submitted outbound messages, requests, replies, and payload work may also enter a bounded paused lane",
		"Pending RPC deadlines continue to run while paused",
		"`authorization_rejected`",
		"Duplicate suppression uses the authenticated mesh, authenticated sender, and canonical message ID",
		"preserve stable message IDs, correlation IDs, expiry, integrity, bounded dedupe",
	} {
		if !strings.Contains(adr, required) {
			t.Errorf("ADR 0008 is missing frozen target statement %q", required)
		}
	}

	for name, required := range map[string][]string{
		"docs/adr/0002-envelope-encoding.md": {
			"Status: Accepted",
			"reserved keys are not interpreted",
		},
		"docs/adr/0003-conversation-identity.md": {
			"Status: Accepted",
			"Stable logical-message identity, authenticated binding",
			"authenticated mesh + authenticated sender + canonical message ID",
		},
		"docs/adr/0006-shared-group-authentication.md": {
			"Status: Accepted",
			"snapshot-required control",
			"generated IQ ID, c2s PID, SID, and bound full JID",
		},
		"docs/adr/0005-multi-carrier-large-payload-transfer.md": {
			"Status: Accepted private Core design",
			"Current membership is checked at admission",
			"All attempts for one logical message retain the same",
		},
		"docs/adr/0007-authenticated-external-service-discovery.md": {
			"Status: Accepted",
			"belongs to the exact authenticated logical session",
		},
		"docs/development/POD_03_MESSAGING_SEMANTICS.md": {
			"Status: current implementation and qualification scope",
			"One lazy bounded actor lane owns the active work for each peer",
			"Bounded message-ID duplicate suppression before handler invocation",
		},
		"docs/development/POD_04_CONNECTIVITY.md": {
			"Status: current implementation and qualification scope",
			"Authority controls use a dedicated bounded session lane",
		},
		"docs/development/POD_05_PAYLOAD_SYSTEM.md": {
			"Status: current implementation and qualification scope",
			"Current membership is checked at admission",
		},
		"docs/development/POD_06_SYSTEM_INTEGRATION_QA.md": {
			"Status: current system-integration and release-QA plan",
		},
		"docs/development/POD_07_PRODUCTION_QUALIFICATION.md": {
			"Status: current production-qualification plan",
		},
		"docs/development/PRODUCTION_READINESS_NEXT_STEP.md": {
			"Status: current production-readiness gap register",
		},
		"docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md": {
			"Status: current orchestration contract",
		},
		"docs/development/V1_TASK_LEDGER.md": {
			"Status: current contract and evidence index",
			"One lazy bounded actor lane owns each active peer",
			"One bounded carrier-neutral outbox survives XMPP session replacement",
		},
		"docs/security/V1_SECURITY_MODEL.md": {
			"Accepted current architecture",
			"Current membership in the server's complete freshly installed mesh snapshot",
		},
	} {
		text := compactPeerLaneDocument(readPeerLaneDocument(t, repository, filepath.FromSlash(name)))
		for _, phrase := range required {
			if !strings.Contains(text, phrase) {
				t.Errorf("%s is missing ADR 0008 alignment %q", name, phrase)
			}
		}
	}
}

func TestCurrentNormativeDocsContainNoLegacyApplicationAuthorityClaims(t *testing.T) {
	repository := repositoryRoot(t)
	files := []string{
		"AZTM_COMMAND_GATE.md",
		"AZTM_ENVELOPE_PAYLOAD_PATCHING.md",
		"AZTM_FUTURE_ISSUES.md",
		"AZTM_GO_CORE.md",
		"AZTM_NETWORKING.md",
		"AZTM_NETWORKING_SHORT.md",
		"AZTM_SDK.md",
		"AZTM_SDK_BOUNDARY.md",
		"docs/adr/0002-envelope-encoding.md",
		"docs/adr/0003-conversation-identity.md",
		"docs/adr/0005-multi-carrier-large-payload-transfer.md",
		"docs/adr/0006-shared-group-authentication.md",
		"docs/adr/0007-authenticated-external-service-discovery.md",
		"docs/security/V1_SECURITY_MODEL.md",
		"docs/development/POD_03_MESSAGING_SEMANTICS.md",
		"docs/development/POD_01_CONTRACT_SDK_BOUNDARY.md",
		"docs/development/POD_04_CONNECTIVITY.md",
		"docs/development/POD_05_PAYLOAD_SYSTEM.md",
		"docs/development/POD_06_SYSTEM_INTEGRATION_QA.md",
		"docs/development/POD_07_PRODUCTION_QUALIFICATION.md",
		"docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md",
		"docs/development/PRODUCTION_READINESS_NEXT_STEP.md",
		"docs/development/V1_TASK_LEDGER.md",
		"docs/adr/0004-v1-process-lifetime.md",
		"test/production/coturn/README.md",
	}
	forbidden := []string{
		"sequence starts at 1",
		"sender-lane sequence",
		"group generation is nonzero",
		"mandatory nonzero authoritative group generation",
		"key 15 carries the mandatory",
		"exact authoritative group generation",
		"current durable group generation",
		"generation before and after",
		"sequence allocation, deduplication, bounded reordering",
		"receiver sequencing provides application ordering",
		"per-conversation ordering cannot",
		"sequence number is assigned",
		"messages are delivered in order",
		"ordered delivery resumes",
		"enter sequencer",
		"core emits ordered delivery",
		"conversation sequence",
		"generation-bound datachannel",
		"generation change itself",
		"newly validated generation",
		"ordering is preserved before handler execution",
		"go core delivers only the next ordered message",
		"dedupes, sequences, and materializes",
		"already-ordered request",
		"should not receive out-of-order application messages",
		"sequencer reservation",
		"ordering_epoch",
		"max reorder buffer",
		"cross-process sequence counters",
		"buffered sequence state",
		"sequence counters survive process restart",
		"envelope, sequencing, rpc",
		"handle namespace, sequence state",
		"rpc, sequence, dedupe",
		"generation-bound peer link",
	}
	for _, name := range files {
		text := strings.ToLower(currentNormativePeerLaneText(readPeerLaneDocument(t, repository, filepath.FromSlash(name))))
		for _, claim := range forbidden {
			if strings.Contains(text, claim) {
				t.Errorf("%s retains legacy normative claim %q outside a historical section", name, claim)
			}
		}
	}
}

func currentNormativePeerLaneText(text string) string {
	lower := strings.ToLower(text)
	index := strings.Index(lower, "## historical ")
	if index < 0 {
		return text
	}
	return text[:index]
}

func compactPeerLaneDocument(text string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(text, ">", "")), " ")
}

func readPeerLaneDocument(t *testing.T, repository string, elements ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{repository}, elements...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
