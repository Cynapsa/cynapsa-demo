package policy

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func FuzzPolicyRuleInputs(f *testing.F) {
	for _, seed := range []struct{ action, path, agent string }{
		{string(ActionAllow), "/", ""},
		{string(ActionDeny), "/private", "agent-a"},
		{"unknown", "relative", "bad\nagent"},
		{string(ActionAllow), "/a/../b", "agent"},
	} {
		f.Add(seed.action, seed.path, seed.agent)
	}
	f.Fuzz(func(t *testing.T, action, path, agent string) {
		rule := Rule{Action: Action(action), Path: path, AgentID: agent}
		compiled, err := Compile([]Rule{rule})
		if err != nil {
			return
		}
		snapshot := compiled.Rules()
		if len(snapshot) != 1 || snapshot[0] != rule {
			t.Fatalf("compiled rule changed: %#v -> %#v", rule, snapshot)
		}
		if rule.Action == ActionAllow && rule.Path != "" && !compiled.Allows(rule.AgentID, rule.Path) {
			t.Fatalf("exact allow rejected: %#v", rule)
		}
		if rule.Action == ActionDeny && rule.Path != "" && compiled.Allows(rule.AgentID, rule.Path) {
			t.Fatalf("exact deny allowed: %#v", rule)
		}
		snapshot[0].Action = "mutated"
		if compiled.Rules()[0] != rule {
			t.Fatal("rules snapshot aliased compiled policy")
		}
	})
}

func FuzzPolicyMaterializationBytes(f *testing.F) {
	for _, seed := range [][]byte{[]byte("/allowed\nbody"), nil, []byte("/otherxx\nbody"), bytes.Repeat([]byte{'x'}, 4096)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, candidate []byte) {
		if len(candidate) > 8<<10 {
			t.Skip()
		}
		now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
		expected := []byte("/allowed\nbody")
		envelope := acceptancePolicyEnvelope(t, "mesh", 1, "agent-a/mesh", "agent-b/mesh", "/allowed")
		reference, err := newAcceptanceReference(envelope.Payload.Profile, expected)
		if err != nil {
			t.Fatal(err)
		}
		envelope.Payload = reference
		rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/allowed"}})
		gate, err := NewGate(GateConfig{
			MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient,
			Memberships: &acceptanceMembership{},
			Paths:       &acceptancePathExtractor{}, Rules: rules, Now: func() time.Time { return now },
			Entropy: bytes.NewReader(bytes.Repeat([]byte{9}, 16)),
		})
		if err != nil {
			t.Fatal(err)
		}
		provenance, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
		if err != nil {
			t.Fatal(err)
		}
		permit, err := gate.EvaluateInbound(envelope, provenance)
		if err != nil {
			t.Fatal(err)
		}
		result, err := gate.VerifyMaterialization(permit, envelope, candidate)
		if bytes.Equal(candidate, expected) {
			if err != nil {
				t.Fatalf("exact bytes rejected: %v", err)
			}
			if err := gate.AuthorizeApplication(permit, envelope, result); err != nil {
				t.Fatalf("verified materialization denied: %v", err)
			}
		} else if !errors.Is(err, ErrMaterializationRejected) {
			t.Fatalf("mismatched bytes error = %v", err)
		}
	})
}

func newAcceptanceReference(profile string, canonical []byte) (protocol.PayloadDescriptor, error) {
	return protocol.NewReferencedPayload(
		protocol.PayloadObjectReference,
		profile,
		"object://private/fuzz",
		int64(len(canonical)),
		sha256.Sum256(canonical),
		"enc-fuzz",
	)
}
