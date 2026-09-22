package policy

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type proofFunc func(protocol.Envelope, []byte) error

func (function proofFunc) Verify(envelope protocol.Envelope, integrity []byte) error {
	return function(envelope, integrity)
}

type membershipFunc func(string, ...string) error

func (function membershipFunc) RequireAll(meshID string, agentIDs ...string) error {
	return function(meshID, agentIDs...)
}

type pathExtractorFunc func(string, []byte) (string, error)

func (function pathExtractorFunc) ExtractApplicationPath(profile string, canonical []byte) (string, error) {
	return function(profile, canonical)
}

func TestCompileImmutableExactWildcardAndDenyOverride(t *testing.T) {
	input := []Rule{
		{Action: ActionAllow, AgentID: "agent-a"},
		{Action: ActionDeny, AgentID: "agent-a", Path: "/private"},
		{Action: ActionAllow, Path: "/health"},
		{Action: ActionAllow, Path: "/health"},
	}
	compiled, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0].Action = ActionDeny
	if !compiled.Allows("agent-a", "/orders") || compiled.Allows("agent-a", "/private") || !compiled.Allows("other", "/health") || compiled.Allows("other", "/orders") {
		t.Fatal("compiled rule decision mismatch")
	}
	snapshot := compiled.Rules()
	if len(snapshot) != 3 {
		t.Fatalf("duplicate was not collapsed: %d", len(snapshot))
	}
	snapshot[0].Action = ActionDeny
	if !compiled.Allows("agent-a", "/orders") {
		t.Fatal("rules snapshot mutated compiled policy")
	}
}

func TestCompileRejectsMalformedConflictingAndCapacity(t *testing.T) {
	invalid := [][]Rule{
		{{Action: "unknown", Path: "/"}},
		{{Action: ActionAllow, Path: "relative"}},
		{{Action: ActionAllow, Path: "/a/../b"}},
		{{Action: ActionAllow, Path: "/a?query"}},
		{{Action: ActionAllow, AgentID: "bad\n"}},
		{{Action: ActionAllow, Path: "/x"}, {Action: ActionDeny, Path: "/x"}},
	}
	for _, rules := range invalid {
		if _, err := Compile(rules); err == nil {
			t.Fatalf("accepted %#v", rules)
		}
	}
	if _, err := Compile(make([]Rule, MaxRules+1)); !errors.Is(err, ErrRuleCapacity) {
		t.Fatalf("capacity: %v", err)
	}
}

func TestPolicyRuleAndPathBoundaries(t *testing.T) {
	maximumRules := make([]Rule, MaxRules)
	for index := range maximumRules {
		maximumRules[index] = Rule{Action: ActionAllow, Path: "/same"}
	}
	if _, err := Compile(maximumRules); err != nil {
		t.Fatalf("maximum rule count rejected: %v", err)
	}
	maximumPath := "/" + strings.Repeat("a", MaxPathBytes-1)
	if err := ValidateRule(Rule{Action: ActionAllow, Path: maximumPath}); err != nil {
		t.Fatalf("maximum path rejected: %v", err)
	}
	if err := ValidateRule(Rule{Action: ActionAllow, Path: maximumPath + "a"}); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("oversized path: %v", err)
	}
}

func TestGateTwoStageOrderPermitAndDerivedPathBinding(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	rules, err := Compile([]Rule{{Action: ActionAllow, AgentID: "agent-a/mesh-123", Path: "/orders"}})
	if err != nil {
		t.Fatal(err)
	}
	envelope := policyEnvelope(t, "/orders")
	var calls []string
	gate, err := NewGate(GateConfig{
		MeshID:        envelope.MeshID,
		LocalIdentity: envelope.Recipient,
		Rules:         rules,
		Now:           func() time.Time { return now },
		Memberships: membershipFunc(func(_ string, _ ...string) error {
			calls = append(calls, "membership")
			return nil
		}),
		Paths: pathExtractorFunc(func(profile string, canonical []byte) (string, error) {
			calls = append(calls, "path")
			return testExtractPath(profile, canonical)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := gate.EvaluateInbound(envelope, testGroupProvenance(t, envelope))
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := gate.VerifyMaterialization(permit, envelope, envelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.AuthorizeApplication(permit, envelope, materialization); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"membership", "path", "membership"}) {
		t.Fatalf("order=%v", calls)
	}

	changed := envelope.Clone()
	changed.MessageID = "msg_AAAAAAAAAAAAAAAAAAAAAB"
	if _, err := gate.VerifyMaterialization(permit, changed, changed.Payload.Inline); !errors.Is(err, ErrInvalidPermit) {
		t.Fatalf("changed envelope: %v", err)
	}
	substituted := materialization
	substituted.path = "/private"
	if err := gate.AuthorizeApplication(permit, envelope, substituted); !errors.Is(err, ErrMaterializationRejected) {
		t.Fatalf("path substitution: %v", err)
	}

	otherGate, err := newTestGate(&now, envelope, rules, allowMembership(now), testPathExtractor)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherGate.AuthorizeApplication(permit, envelope, materialization); !errors.Is(err, ErrInvalidPermit) {
		t.Fatalf("cross-gate permit: %v", err)
	}
}

func TestGateDefersAuthenticatedFutureBoundUntilAfterProof(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	envelope := policyEnvelope(t, "/orders")
	envelope.CreatedAt = now.Add(time.Millisecond)
	rules, err := Compile([]Rule{{Action: ActionAllow, AgentID: envelope.Sender, Path: "/orders"}})
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient, Rules: rules,
		Memberships: membershipFunc(func(string, ...string) error {
			calls = append(calls, "membership")
			return nil
		}),
		Paths: pathExtractorFunc(testExtractPath),
		Now: func() time.Time {
			calls = append(calls, "clock")
			return now
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.EvaluateInbound(envelope, testGroupProvenance(t, envelope)); err != nil {
		t.Fatalf("future creation before composed bound check = %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"clock", "membership"}) {
		t.Fatalf("fail-closed order = %#v", calls)
	}
}

func TestGateMaterializationPreservesAuthenticatedDescriptorAndRejectsMismatch(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	canonical := canonicalPathPayload("/orders", "payload")
	envelope := policyEnvelope(t, "/orders")
	reference, err := protocol.NewReferencedPayload(protocol.PayloadObjectReference, envelope.Payload.Profile, "object://private/one", int64(len(canonical)), sha256.Sum256(canonical), "enc")
	if err != nil {
		t.Fatal(err)
	}
	envelope.Payload = reference
	originalDescriptor := envelope.Payload
	rules, _ := Compile([]Rule{{Action: ActionAllow, AgentID: envelope.Sender, Path: "/orders"}})
	gate, err := newTestGate(&now, envelope, rules, allowMembership(now), testPathExtractor)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := gate.EvaluateInbound(envelope, testGroupProvenance(t, envelope))
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := gate.VerifyMaterialization(permit, envelope, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(envelope.Payload, originalDescriptor) || envelope.Payload.Kind != protocol.PayloadObjectReference {
		t.Fatal("authenticated descriptor was rewritten during materialization")
	}
	if err := gate.AuthorizeApplication(permit, envelope, materialization); err != nil {
		t.Fatal(err)
	}

	digestMismatch := append([]byte(nil), canonical...)
	digestMismatch[len(digestMismatch)-1] ^= 1
	if _, err := gate.VerifyMaterialization(permit, envelope, digestMismatch); !errors.Is(err, ErrMaterializationRejected) {
		t.Fatalf("digest mismatch: %v", err)
	}
	if _, err := gate.VerifyMaterialization(permit, envelope, append(canonical, 'x')); !errors.Is(err, ErrMaterializationRejected) {
		t.Fatalf("size mismatch: %v", err)
	}
	pathSubstitution := canonicalPathPayload("/otherx", "payload")
	if len(pathSubstitution) != len(canonical) {
		t.Fatal("test path substitution must preserve size")
	}
	if _, err := gate.VerifyMaterialization(permit, envelope, pathSubstitution); !errors.Is(err, ErrMaterializationRejected) {
		t.Fatalf("canonical path substitution: %v", err)
	}

	rewritten := envelope.Clone()
	rewritten.Payload, err = protocol.NewInlinePayload(envelope.Payload.Profile, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.VerifyMaterialization(permit, rewritten, canonical); !errors.Is(err, ErrInvalidPermit) {
		t.Fatalf("descriptor rewrite: %v", err)
	}
	if err := gate.AuthorizeApplication(permit, rewritten, materialization); !errors.Is(err, ErrInvalidPermit) {
		t.Fatalf("rewritten descriptor authorization: %v", err)
	}
	changedProof := envelope.Clone()
	changedProof.CredentialProof = []byte("different-proof")
	if _, err := gate.VerifyMaterialization(permit, changedProof, canonical); err != nil {
		t.Fatalf("ignored compatibility proof changed permit: %v", err)
	}
}

func TestGateFinalMembershipRecheckRejectsRevocation(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	membershipErr := error(nil)
	membership := membershipFunc(func(_ string, _ ...string) error {
		return membershipErr
	})
	envelope := policyEnvelope(t, "/orders")
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/orders"}})
	gate, err := newTestGate(&now, envelope, rules, membership, testPathExtractor)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := gate.EvaluateInbound(envelope, testGroupProvenance(t, envelope))
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := gate.VerifyMaterialization(permit, envelope, envelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}

	if err := gate.AuthorizeApplication(permit, envelope, materialization); err != nil {
		t.Fatalf("current authority rejected: %v", err)
	}
	membershipErr = errors.New("revoked")
	if err := gate.AuthorizeApplication(permit, envelope, materialization); !errors.Is(err, ErrMembership) {
		t.Fatalf("membership revocation: %v", err)
	}
}

func TestGateDerivedPathDenyAndTrustFailures(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/orders"}})
	deniedEnvelope := policyEnvelope(t, "/private")
	gate, err := newTestGate(&now, deniedEnvelope, rules, allowMembership(now), testPathExtractor)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := gate.EvaluateInbound(deniedEnvelope, testGroupProvenance(t, deniedEnvelope))
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := gate.VerifyMaterialization(permit, deniedEnvelope, deniedEnvelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.AuthorizeApplication(permit, deniedEnvelope, materialization); !errors.Is(err, ErrApplicationDenied) {
		t.Fatalf("derived path deny: %v", err)
	}

	envelope := policyEnvelope(t, "/orders")
	gate, err = NewGate(GateConfig{
		MeshID:        envelope.MeshID,
		LocalIdentity: envelope.Recipient,
		Rules:         rules,
		Now:           func() time.Time { return now },
		Paths:         testPathExtractor,
		Memberships:   allowMembership(now),
	})
	if err != nil {
		t.Fatal(err)
	}
	attacker, _ := NewGroupRank2Provenance("attacker/"+envelope.MeshID, envelope.Recipient, envelope.MeshID)
	if _, err := gate.EvaluateInbound(envelope, attacker); !errors.Is(err, ErrProvenance) {
		t.Fatalf("identity provenance: %v", err)
	}
	if _, err := gate.EvaluateInbound(envelope, AuthenticatedProvenance{}); !errors.Is(err, ErrProvenance) {
		t.Fatalf("missing provenance: %v", err)
	}
	wrongMesh := envelope.Clone()
	wrongMesh.MeshID = "other"
	if _, err := gate.EvaluateInbound(wrongMesh, testGroupProvenance(t, envelope)); !errors.Is(err, ErrMesh) {
		t.Fatalf("wrong mesh: %v", err)
	}
	malformed := envelope.Clone()
	malformed.Payload.Digest[0] ^= 0xff
	if _, err := gate.EvaluateInbound(malformed, testGroupProvenance(t, malformed)); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("malformed integrity: %v", err)
	}

	gate, err = newTestGate(&now, envelope, rules, membershipFunc(func(string, ...string) error {
		return errors.New("stale")
	}), testPathExtractor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.EvaluateInbound(envelope, testGroupProvenance(t, envelope)); !errors.Is(err, ErrMembership) {
		t.Fatalf("membership: %v", err)
	}
}

func newTestGate(now *time.Time, envelope protocol.Envelope, rules *Compiled, membership MembershipVerifier, paths ApplicationPathExtractor) (*Gate, error) {
	return NewGate(GateConfig{
		MeshID:        envelope.MeshID,
		LocalIdentity: envelope.Recipient,
		Rules:         rules,
		Now:           func() time.Time { return *now },
		Memberships:   membership,
		Paths:         paths,
	})
}

func allowMembership(_ time.Time) membershipFunc {
	return func(string, ...string) error {
		return nil
	}
}

var testPathExtractor = pathExtractorFunc(testExtractPath)

func testExtractPath(profile string, canonical []byte) (string, error) {
	if profile != "aztm.native" {
		return "", errors.New("profile")
	}
	separator := bytes.IndexByte(canonical, '\n')
	if separator <= 0 {
		return "", errors.New("path")
	}
	return string(canonical[:separator]), nil
}

func canonicalPathPayload(path, body string) []byte {
	return []byte(path + "\n" + body)
}

func policyEnvelope(t *testing.T, path string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", canonicalPathPayload(path, "payload"))
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh-123", "agent-a/mesh-123", "agent-b/mesh-123")
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		Version:          protocol.Version2,
		MessageID:        messageID,
		ConversationID:   conversationID,
		Sender:           "agent-a/mesh-123",
		Recipient:        "agent-b/mesh-123",
		MeshID:           "mesh-123",
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
	}
}

func testGroupProvenance(t *testing.T, envelope protocol.Envelope) AuthenticatedProvenance {
	t.Helper()
	provenance, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	return provenance
}
