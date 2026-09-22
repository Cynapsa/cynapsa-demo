package mesh

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCredentialReplacementRemovalAndSecretSafety(t *testing.T) {
	store := NewCredentialStore()
	canary := []byte("PRIVATE-PASSWORD-CANARY")
	if err := store.Replace(CredentialInput{MeshID: "mesh", Username: "agent", Password: canary}); err != nil {
		t.Fatal(err)
	}
	old := store.active
	if err := store.WithPassword("mesh", func(username string, password []byte) error {
		if username != "agent" || string(password) != string(canary) {
			t.Fatal("callback did not receive scoped credential")
		}
		password[0] ^= 0xff
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WithPassword("mesh", func(_ string, password []byte) error {
		if string(password) != string(canary) {
			t.Fatal("callback mutated stored password")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(CredentialInput{MeshID: "mesh-2", Username: "agent-2", Password: []byte("replacement")}); err != nil {
		t.Fatal(err)
	}
	if strings.Trim(string(old.password), "\x00") != "" {
		t.Fatal("replaced credential bytes were not cleared")
	}
	current := store.active
	if err := store.Remove("mesh-2"); err != nil {
		t.Fatal(err)
	}
	if strings.Trim(string(current.password), "\x00") != "" {
		t.Fatal("removed password was not cleared")
	}
	if text := ErrInvalidCredential.Error(); strings.Contains(text, string(canary)) {
		t.Fatal("error exposed secret")
	}
	for _, formatted := range []string{fmt.Sprintf("%v", store), fmt.Sprintf("%#v", store), fmt.Sprintf("%v", CredentialInput{MeshID: "mesh", Username: "agent", Password: canary}), fmt.Sprintf("%#v", CredentialInput{MeshID: "mesh", Username: "agent", Password: canary})} {
		if strings.Contains(formatted, string(canary)) {
			t.Fatalf("formatted credential exposed secret: %s", formatted)
		}
	}
}

func TestCredentialPasswordBoundary(t *testing.T) {
	store := NewCredentialStore()
	if err := store.Replace(CredentialInput{MeshID: "mesh", Username: "agent", Password: make([]byte, MaxCredentialBytes)}); err != nil {
		t.Fatalf("maximum password rejected: %v", err)
	}
	if err := store.Replace(CredentialInput{MeshID: "mesh", Username: "agent", Password: make([]byte, MaxCredentialBytes+1)}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("oversized password: %v", err)
	}
}

type bindingVerifier func(string, string, string) error

func (verifier bindingVerifier) VerifyBoundIdentity(bare, meshID, full string) error {
	return verifier(bare, meshID, full)
}

func TestSessionIdentityRequiresInjectedBoundVerification(t *testing.T) {
	identity, err := NewSessionIdentity("mesh", "agent@example", "agent@example/mesh", bindingVerifier(func(bare, meshID, full string) error {
		if bare != "agent@example" || meshID != "mesh" || full != "agent@example/mesh" {
			t.Fatal("binding arguments changed")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if identity.AgentID() != "agent@example" || identity.MeshID() != "mesh" || identity.BoundFull() != "agent@example/mesh" {
		t.Fatalf("identity=%#v", identity)
	}
	if _, err := NewSessionIdentity("mesh", "agent@example", "agent@example/wrong", bindingVerifier(func(string, string, string) error { return errors.New("rejected") })); !errors.Is(err, ErrIdentityBinding) {
		t.Fatalf("binding rejection: %v", err)
	}
}

func TestDirectoryImmutableExpiryAndCapacity(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	directory, err := NewDirectory(DirectoryConfig{Capacity: 2, MeshID: "mesh", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	identities := []Identity{{AgentID: "agent-a", Internal: "agent-a@example/mesh"}}
	if err := directory.Replace(DirectorySnapshot{MeshID: "mesh", ObservedAt: now, ExpiresAt: now.Add(time.Minute), Identities: identities}); err != nil {
		t.Fatal(err)
	}
	identities[0].Internal = "attacker"
	identity, err := directory.Lookup("agent-a")
	if err != nil || identity.Internal != "agent-a@example/mesh" {
		t.Fatalf("identity=%#v err=%v", identity, err)
	}
	if _, err := directory.Lookup("unknown"); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("unknown: %v", err)
	}
	for _, invalid := range []DirectorySnapshot{
		{MeshID: "mesh", ObservedAt: now.Add(-2 * time.Second), ExpiresAt: now.Add(-time.Second), Identities: []Identity{{AgentID: "attacker", Internal: "attacker@example/mesh"}}},
		{MeshID: "mesh", ObservedAt: now.Add(time.Second), ExpiresAt: now.Add(2 * time.Second), Identities: []Identity{{AgentID: "attacker", Internal: "attacker@example/mesh"}}},
	} {
		if err := directory.Replace(invalid); !errors.Is(err, ErrSnapshotInvalid) {
			t.Fatalf("invalid timestamp snapshot: %v", err)
		}
		preserved, err := directory.Lookup("agent-a")
		if err != nil || preserved.Internal != "agent-a@example/mesh" {
			t.Fatalf("invalid snapshot replaced good directory: %#v %v", preserved, err)
		}
	}
	now = now.Add(time.Minute)
	if _, err := directory.Lookup("agent-a"); !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("stale: %v", err)
	}
	tooMany := make([]Identity, 3)
	for index := range tooMany {
		tooMany[index] = Identity{AgentID: string(rune('a' + index)), Internal: string(rune('x' + index))}
	}
	if err := directory.Replace(DirectorySnapshot{MeshID: "mesh", ObservedAt: now, ExpiresAt: now.Add(time.Minute), Identities: tooMany}); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("capacity: %v", err)
	}
	if err := directory.Replace(DirectorySnapshot{MeshID: "other", ObservedAt: now, ExpiresAt: now.Add(time.Minute)}); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("cross-mesh directory snapshot: %v", err)
	}
}

func TestDirectoryCapacityBoundaries(t *testing.T) {
	if _, err := NewDirectory(DirectoryConfig{Capacity: MaxDirectoryEntries, MeshID: "mesh"}); err != nil {
		t.Fatalf("maximum directory capacity rejected: %v", err)
	}
	if _, err := NewDirectory(DirectoryConfig{Capacity: MaxDirectoryEntries + 1, MeshID: "mesh"}); !errors.Is(err, ErrDirectoryCapacity) {
		t.Fatalf("oversized directory capacity: %v", err)
	}
}
