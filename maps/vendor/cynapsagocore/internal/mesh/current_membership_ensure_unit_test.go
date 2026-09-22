package mesh

import (
	"context"
	"sync"
	"testing"
	"time"
)

type serializedEnsureAuthority struct {
	mu      sync.Mutex
	ready   bool
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (authority *serializedEnsureAuthority) SynchronizeCurrentMembership(ctx context.Context, meshID string) (PeerAuthoritySnapshot, TopologyDisposition) {
	authority.mu.Lock()
	authority.calls++
	call := authority.calls
	authority.ready = false
	authority.mu.Unlock()
	if call == 1 {
		close(authority.entered)
		select {
		case <-authority.release:
		case <-ctx.Done():
			return PeerAuthoritySnapshot{}, TopologyUnavailable
		}
	}
	if meshID != testMesh {
		return PeerAuthoritySnapshot{}, TopologyRejected
	}
	return PeerAuthoritySnapshot{
		MeshID:          testMesh,
		ObservedAt:      time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
		Local:           Identity{AgentID: testLocalBare, Internal: testLocalFull},
		LocalAuthorized: true,
		Peers:           []PeerAuthorityResult{{Identity: Identity{AgentID: testPeerBare, Internal: testPeerFull}, Authorized: true}},
	}, TopologyReady
}

func (authority *serializedEnsureAuthority) PublishPeerAuthority(context.Context) error {
	authority.mu.Lock()
	authority.ready = true
	authority.mu.Unlock()
	return nil
}

func (authority *serializedEnsureAuthority) BlockPeerAuthority() {
	authority.mu.Lock()
	authority.ready = false
	authority.mu.Unlock()
}

func (authority *serializedEnsureAuthority) CurrentMembershipReady() bool {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	return authority.ready
}

func (authority *serializedEnsureAuthority) synchronizationCount() int {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	return authority.calls
}

func TestEnsureCurrentMembershipCoalescesBehindSuccessfulExplicitRefresh(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(bare, meshID, full string) error {
		if bare != testLocalBare || meshID != testMesh || full != testLocalFull {
			return ErrInvalidIdentity
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	authority := &serializedEnsureAuthority{entered: make(chan struct{}), release: make(chan struct{})}
	service := &MessagingService{
		config:        MessagingConfig{OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second},
		identity:      identity,
		peerAuthority: authority,
		topology:      topology,
	}

	explicitDone := make(chan error, 1)
	go func() { explicitDone <- service.synchronizeCurrentMembership(context.Background()) }()
	select {
	case <-authority.entered:
	case <-time.After(time.Second):
		t.Fatal("explicit refresh did not enter authority synchronization")
	}

	ensureStarted := make(chan struct{})
	ensureDone := make(chan error, 1)
	go func() {
		close(ensureStarted)
		ensureDone <- service.ensureCurrentMembership(context.Background())
	}()
	<-ensureStarted
	if calls := authority.synchronizationCount(); calls != 1 {
		t.Fatalf("recovery ensure bypassed serialized refresh lane: calls=%d", calls)
	}

	close(authority.release)
	if err = <-explicitDone; err != nil {
		t.Fatalf("explicit refresh: %v", err)
	}
	if err = <-ensureDone; err != nil {
		t.Fatalf("recovery ensure: %v", err)
	}
	if calls := authority.synchronizationCount(); calls != 1 {
		t.Fatalf("recovery ensure duplicated successful explicit refresh: calls=%d", calls)
	}
}
