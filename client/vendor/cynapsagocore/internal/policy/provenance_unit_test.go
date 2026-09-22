package policy

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
)

func FuzzAuthenticatedProvenanceShape(f *testing.F) {
	f.Add("peer@example/mesh", "local@example/mesh", "mesh")
	f.Add("peer@example/other", "local@example/mesh", "mesh")
	f.Fuzz(func(t *testing.T, sender, recipient, meshID string) {
		provenance, err := NewGroupRank2Provenance(sender, recipient, meshID)
		if err != nil {
			return
		}
		if provenance.Sender() != sender || provenance.Recipient() != recipient || provenance.MeshID() != meshID || !validEndpointIdentity(sender) || !validEndpointIdentity(recipient) {
			t.Fatalf("accepted non-exact provenance: %#v", provenance)
		}
	})
}

func TestAuthenticatedProvenanceAcceptsOpaqueEndpointResourcesAndRejectsMalformedShape(t *testing.T) {
	if _, err := NewGroupRank2Provenance("peer@example/mesh", "local@example/mesh", "mesh"); err != nil {
		t.Fatalf("rank2 provenance = %v", err)
	}
	if _, err := NewGroupRank2Provenance(
		"peer@example/r2.00000000-0000-4000-8000-000000000001.AAAAAAAAAAAAAAAA",
		"local@example/r2.00000000-0000-4000-8000-000000000002.BBBBBBBBBBBBBBBB", "mesh",
	); err != nil {
		t.Fatalf("r2 provenance = %v", err)
	}
	// The resource is an opaque exact-session address. It need not equal the
	// mesh because membership authority is checked separately by Gate.
	if _, err := NewGroupRank2Provenance("peer@example/other", "local@example/session", "mesh"); err != nil {
		t.Fatalf("opaque resource provenance = %v", err)
	}
	for _, test := range []struct{ sender, recipient, mesh string }{
		{"peer@example", "local@example/mesh", "mesh"},
		{"peer@example/mesh", "local@example", "mesh"},
		{"peer@example/nested/resource", "local@example/session", "mesh"},
	} {
		if _, err := NewGroupRank2Provenance(test.sender, test.recipient, test.mesh); !errors.Is(err, ErrProvenance) {
			t.Fatalf("accepted sender=%q recipient=%q mesh=%q", test.sender, test.recipient, test.mesh)
		}
	}
	var empty [sha256.Size]byte
	if _, err := NewBoundRank1Provenance("peer@example/mesh", "local@example/mesh", "mesh", empty); !errors.Is(err, ErrProvenance) {
		t.Fatalf("accepted empty Rank1 binding: %v", err)
	}
}

func TestGateBindsR2EndpointsToAuthenticatedMeshAndCurrentMembership(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	envelope := policyEnvelope(t, "/rank1")
	envelope.Sender = "peer@example/r2.00000000-0000-4000-8000-000000000001.AAAAAAAAAAAAAAAA"
	envelope.Recipient = "local@example/r2.00000000-0000-4000-8000-000000000002.BBBBBBBBBBBBBBBB"
	envelope.ConversationID, _ = conversation.DeriveID(envelope.MeshID, envelope.Sender, envelope.Recipient)
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/rank1"}})
	var gotMesh string
	var gotMembers []string
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient, Rules: rules,
		Memberships: membershipFunc(func(meshID string, members ...string) error {
			gotMesh = meshID
			gotMembers = append([]string(nil), members...)
			return nil
		}),
		Paths: testPathExtractor, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.EvaluateInbound(envelope, provenance); err != nil {
		t.Fatalf("r2 gate=%v", err)
	}
	if gotMesh != envelope.MeshID || len(gotMembers) != 2 || gotMembers[0] != envelope.Sender || gotMembers[1] != envelope.Recipient {
		t.Fatalf("membership check mesh=%q members=%q", gotMesh, gotMembers)
	}
	wrongMesh := envelope.Clone()
	wrongMesh.MeshID = "other"
	if _, err = gate.EvaluateInbound(wrongMesh, provenance); !errors.Is(err, ErrMesh) {
		t.Fatalf("wrong mesh=%v", err)
	}
}

func TestGateProvenancePrecedesClockAndRank1BindsChannel(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	envelope := policyEnvelope(t, "/rank1")
	rules, _ := Compile([]Rule{{Action: ActionAllow, Path: "/rank1"}})
	clockReads := 0
	gate, err := NewGate(GateConfig{
		MeshID: envelope.MeshID, LocalIdentity: envelope.Recipient, Rules: rules,
		Memberships: membershipFunc(func(string, ...string) error { return nil }),
		Paths:       testPathExtractor, Now: func() time.Time { clockReads++; return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.EvaluateInbound(envelope, AuthenticatedProvenance{}); !errors.Is(err, ErrProvenance) || clockReads != 0 {
		t.Fatalf("invalid provenance err=%v clock_reads=%d", err, clockReads)
	}
	binding := [sha256.Size]byte{1}
	current, err := NewBoundRank1Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID, binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.EvaluateInbound(envelope, current); err != nil {
		t.Fatalf("current Rank1 channel binding = %v", err)
	}
}
