package policy

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type acceptanceMembership struct {
	reject error
}

func (membership *acceptanceMembership) RequireAll(string, ...string) error {
	return membership.reject
}

type acceptancePathExtractor struct{ reject error }

func (extractor *acceptancePathExtractor) ExtractApplicationPath(_ string, canonical []byte) (string, error) {
	if extractor.reject != nil {
		return "", extractor.reject
	}
	separator := bytes.IndexByte(canonical, '\n')
	if separator < 1 {
		return "", errors.New("invalid private payload")
	}
	return string(canonical[:separator]), nil
}

func TestAcceptanceAuthorizationTrustMatrixFailsClosedAtEveryGate(t *testing.T) {
	base := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	for mask := 0; mask < 64; mask++ {
		mask := mask
		t.Run(fmt.Sprintf("mask-%02x", mask), func(t *testing.T) {
			bindingOK := mask&1 != 0
			meshOK := mask&2 != 0
			provenanceOK := mask&4 != 0
			membershipOK := mask&8 != 0
			pathOK := mask&16 != 0
			integrityOK := mask&32 != 0

			path := "/denied"
			if pathOK {
				path = "/allowed"
			}
			envelope := acceptancePolicyEnvelope(t, "mesh-good", 7, "agent-a/mesh-good", "agent-b/mesh-good", path)
			if !meshOK {
				envelope.MeshID = "mesh-other"
				envelope.ConversationID = acceptanceConversationID(t, envelope.MeshID, envelope.Sender, envelope.Recipient)
			}
			if !bindingOK {
				envelope.ConversationID = acceptanceConversationID(t, envelope.MeshID, "other/"+envelope.MeshID, envelope.Recipient)
			}
			if !integrityOK {
				envelope.Payload.Digest[0] ^= 0xff
			}
			var provenance AuthenticatedProvenance
			var err error
			if meshOK {
				provenance, err = NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
				if err != nil {
					t.Fatal(err)
				}
				if !provenanceOK {
					provenance, err = NewGroupRank2Provenance("agent-attacker/"+envelope.MeshID, envelope.Recipient, envelope.MeshID)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			memberships := &acceptanceMembership{}
			if !membershipOK {
				memberships.reject = errors.New("private membership detail")
			}
			rules, err := Compile([]Rule{{Action: ActionAllow, AgentID: envelope.Sender, Path: "/allowed"}})
			if err != nil {
				t.Fatal(err)
			}
			gate, err := NewGate(GateConfig{
				MeshID: "mesh-good", LocalIdentity: "agent-b/mesh-good",
				Memberships: memberships, Paths: &acceptancePathExtractor{}, Rules: rules,
				Now: func() time.Time { return base }, Entropy: bytes.NewReader(bytes.Repeat([]byte{1}, 16)),
			})
			if err != nil {
				t.Fatal(err)
			}

			permit, evaluateErr := gate.EvaluateInbound(envelope, provenance)
			switch {
			case !integrityOK:
				if !errors.Is(evaluateErr, ErrIntegrity) {
					t.Fatalf("integrity failure = %v", evaluateErr)
				}
				return
			case !meshOK:
				if !errors.Is(evaluateErr, ErrMesh) {
					t.Fatalf("mesh failure = %v", evaluateErr)
				}
				return
			case !provenanceOK:
				if !errors.Is(evaluateErr, ErrProvenance) {
					t.Fatalf("provenance failure = %v", evaluateErr)
				}
				return
			case !bindingOK:
				if !errors.Is(evaluateErr, ErrIdentity) {
					t.Fatalf("binding failure = %v", evaluateErr)
				}
				return
			case !membershipOK:
				if !errors.Is(evaluateErr, ErrMembership) {
					t.Fatalf("membership failure = %v", evaluateErr)
				}
				return
			case evaluateErr != nil:
				t.Fatal(evaluateErr)
			}

			materialization, err := gate.VerifyMaterialization(permit, envelope, envelope.Payload.Inline)
			if err != nil {
				t.Fatal(err)
			}
			authorizeErr := gate.AuthorizeApplication(permit, envelope, materialization)
			if pathOK && authorizeErr != nil {
				t.Fatalf("allowed path rejected: %v", authorizeErr)
			}
			if !pathOK && !errors.Is(authorizeErr, ErrApplicationDenied) {
				t.Fatalf("denied path = %v", authorizeErr)
			}
		})
	}
}

func TestAcceptanceMaterializationBindsBytesDigestPathSealAndGateScope(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	canonical := []byte("/allowed\nprivate body")
	envelope := acceptancePolicyEnvelope(t, "mesh", 1, "agent-a/mesh", "agent-b/mesh", "/ignored")
	reference, err := protocol.NewReferencedPayload(
		protocol.PayloadObjectReference, "aztm.native", "object://private/canary",
		int64(len(canonical)), sha256.Sum256(canonical), "encryption-private",
	)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Payload = reference
	rules, err := Compile([]Rule{{Action: ActionAllow, AgentID: envelope.Sender, Path: "/allowed"}})
	if err != nil {
		t.Fatal(err)
	}
	memberships := &acceptanceMembership{}
	newGate := func(scope byte) *Gate {
		gate, err := NewGate(GateConfig{
			MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient,
			Memberships: memberships,
			Paths:       &acceptancePathExtractor{}, Rules: rules, Now: func() time.Time { return now },
			Entropy: bytes.NewReader(bytes.Repeat([]byte{scope}, 16)),
		})
		if err != nil {
			t.Fatal(err)
		}
		return gate
	}
	first := newGate(1)
	second := newGate(2)
	provenance, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := first.EvaluateInbound(envelope, provenance)
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := first.VerifyMaterialization(permit, envelope.Clone(), append([]byte(nil), canonical...))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.AuthorizeApplication(permit, envelope.Clone(), materialization); err != nil {
		t.Fatalf("exact replay rejected: %v", err)
	}
	if err := second.AuthorizeApplication(permit, envelope, materialization); !errors.Is(err, ErrInvalidPermit) {
		t.Fatalf("cross-gate replay = %v", err)
	}

	for name, candidate := range map[string][]byte{
		"short":             canonical[:len(canonical)-1],
		"long":              append(append([]byte(nil), canonical...), '!'),
		"digest mutation":   append([]byte(nil), canonical...),
		"path substitution": []byte("/blocked\nprivate body"),
	} {
		if name == "digest mutation" {
			candidate[len(candidate)-1] ^= 1
		}
		if _, err := first.VerifyMaterialization(permit, envelope, candidate); !errors.Is(err, ErrMaterializationRejected) {
			t.Errorf("%s = %v", name, err)
		}
	}

	for name, mutate := range map[string]func(*MaterializationResult){
		"path": func(result *MaterializationResult) { result.path = "/blocked" },
		"size": func(result *MaterializationResult) { result.payloadSize++ },
		"hash": func(result *MaterializationResult) { result.payloadHash[0] ^= 1 },
		"seal": func(result *MaterializationResult) { result.seal[0] ^= 1 },
	} {
		forged := materialization
		mutate(&forged)
		if err := first.AuthorizeApplication(permit, envelope, forged); !errors.Is(err, ErrMaterializationRejected) {
			t.Errorf("forged %s = %v", name, err)
		}
	}
}

func TestAcceptanceMembershipRevocationBetweenStages(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	envelope := acceptancePolicyEnvelope(t, "mesh", 10, "agent-a/mesh", "agent-b/mesh", "/allowed")
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/allowed"}})
	membership := &acceptanceMembership{}
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient,
		Memberships: membership,
		Paths:       &acceptancePathExtractor{}, Rules: rules, Now: func() time.Time { return now },
		Entropy: bytes.NewReader(bytes.Repeat([]byte{3}, 16)),
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
	materialization, err := gate.VerifyMaterialization(permit, envelope, envelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		reject  error
		wantErr bool
	}{
		{"unchanged", nil, false},
		{"revoked", errors.New("revoked privately"), true},
	}
	for _, test := range tests {
		membership.reject = test.reject
		err := gate.AuthorizeApplication(permit, envelope, materialization)
		if test.wantErr && !errors.Is(err, ErrMembership) {
			t.Errorf("%s = %v", test.name, err)
		}
		if !test.wantErr && err != nil {
			t.Errorf("%s = %v", test.name, err)
		}
	}
}

func TestAcceptanceRank1ProvenanceCannotSubstituteCarrierPermit(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	envelope := acceptancePolicyEnvelope(t, "mesh", 7, "agent-a/mesh", "agent-b/mesh", "/allowed")
	rules, err := Compile([]Rule{{Action: ActionAllow, AgentID: envelope.Sender, Path: "/allowed"}})
	if err != nil {
		t.Fatal(err)
	}
	membership := &acceptanceMembership{}
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient, Memberships: membership,
		Paths: &acceptancePathExtractor{}, Rules: rules, Now: func() time.Time { return now },
		Entropy: bytes.NewReader(bytes.Repeat([]byte{5}, 16)),
	})
	if err != nil {
		t.Fatal(err)
	}
	var binding [sha256.Size]byte
	binding[0] = 1
	current, err := NewBoundRank1Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID, binding)
	if err != nil {
		t.Fatal(err)
	}
	rank1Permit, err := gate.EvaluateInbound(envelope, current)
	if err != nil {
		t.Fatal(err)
	}
	rank2, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	rank2Permit, err := gate.EvaluateInbound(envelope, rank2)
	if err != nil {
		t.Fatal(err)
	}
	rank2Materialization, err := gate.VerifyMaterialization(rank2Permit, envelope, envelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.AuthorizeApplication(rank1Permit, envelope, rank2Materialization); !errors.Is(err, ErrMaterializationRejected) {
		t.Fatalf("cross-carrier permit substitution = %v", err)
	}
	rank1Materialization, err := gate.VerifyMaterialization(rank1Permit, envelope, envelope.Payload.Inline)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.AuthorizeApplication(rank1Permit, envelope, rank1Materialization); err != nil {
		t.Fatalf("current Rank1 provenance = %v", err)
	}
	membership.reject = errors.New("removed from current membership")
	if err := gate.AuthorizeApplication(rank1Permit, envelope, rank1Materialization); !errors.Is(err, ErrMembership) {
		t.Fatalf("Rank1 permit after removal = %v", err)
	}
}

func TestAcceptancePolicyErrorsAndOpaqueStateDoNotLeakSecretCanaries(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	canary := "PRIVATE-POLICY-CANARY-7f15"
	envelope := acceptancePolicyEnvelope(t, "mesh", 1, "agent-a/mesh", "agent-b/mesh", "/allowed")
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/allowed"}})
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient,
		Memberships: &acceptanceMembership{},
		Paths:       &acceptancePathExtractor{reject: errors.New(canary)}, Rules: rules,
		Now: func() time.Time { return now }, Entropy: bytes.NewReader(bytes.Repeat([]byte{4}, 16)),
	})
	if err != nil {
		t.Fatal(err)
	}
	attacker, err := NewGroupRank2Provenance(canary+"/mesh", envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	_, gotErr := gate.EvaluateInbound(envelope, attacker)
	if !errors.Is(gotErr, ErrProvenance) {
		t.Fatalf("provenance rejection = %v", gotErr)
	}
	for _, text := range []string{
		fmt.Sprintf("%v", gotErr), fmt.Sprintf("%+v", gotErr), fmt.Sprintf("%#v", gotErr),
		fmt.Sprintf("%v", InboundPermit{}), fmt.Sprintf("%#v", InboundPermit{}),
		fmt.Sprintf("%v", MaterializationResult{}), fmt.Sprintf("%#v", MaterializationResult{}),
	} {
		if strings.Contains(text, canary) {
			t.Fatalf("secret leaked through policy output: %s", text)
		}
	}
}

func acceptancePolicyEnvelope(t *testing.T, meshID string, _ uint64, sender, recipient, applicationPath string) protocol.Envelope {
	t.Helper()
	canonical := []byte(applicationPath + "\nprivate body")
	payload, err := protocol.NewInlinePayload("aztm.native", canonical)
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		Version: protocol.Version2, MessageID: messageID,
		ConversationID: acceptanceConversationID(t, meshID, sender, recipient),
		Sender:         sender, Recipient: recipient, MeshID: meshID,
		Mode: protocol.ModeMessage, CreatedAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
	}
}

func acceptanceConversationID(t *testing.T, meshID, sender, recipient string) string {
	t.Helper()
	value, err := conversation.DeriveID(meshID, sender, recipient)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
