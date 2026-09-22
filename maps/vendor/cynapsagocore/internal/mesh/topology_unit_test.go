package mesh

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestTopologyFreezesExactIdentityBacking(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(4, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	largeAgentBacking := testPeerBare + string(make([]byte, 2<<20))
	largeInternalBacking := testPeerFull + string(make([]byte, 2<<20))
	agent := largeAgentBacking[:len(testPeerBare)]
	internal := largeInternalBacking[:len(testPeerFull)]
	local, err := NewIdentity(testLocalBare, testLocalFull)
	if err != nil {
		t.Fatal(err)
	}
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
		Peers: []PeerAuthorityResult{{Identity: Identity{AgentID: agent, Internal: internal}, Authorized: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}
	stored, err := topology.Lookup(testPeerBare)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(stored.AgentID) == unsafe.StringData(agent) || unsafe.StringData(stored.Internal) == unsafe.StringData(internal) {
		t.Fatal("topology retained caller-owned oversized identity backing")
	}
	if stored.AgentID != testPeerBare || stored.Internal != testPeerFull {
		t.Fatalf("stored=%#v", stored)
	}
}

func TestTopologySuspendResumePreservesExactCurrentSnapshot(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	peerIdentity, _ := NewIdentity(testPeerBare, testPeerFull)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
		Peers: []PeerAuthorityResult{{Identity: peerIdentity, Authorized: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}
	suspension := topology.SuspendAdmission()
	if suspension == nil {
		t.Fatal("current topology did not suspend")
	}
	if _, err = topology.Lookup(testPeerBare); !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("suspended lookup=%v", err)
	}
	now = now.Add(24 * time.Hour)
	if err = topology.ResumeAdmission(context.Background(), suspension); err != nil {
		t.Fatalf("resume after former lease duration: %v", err)
	}
	if got, lookupErr := topology.Lookup(testPeerBare); lookupErr != nil || got != peerIdentity {
		t.Fatalf("resumed lookup=%#v, %v", got, lookupErr)
	}
}

func TestTopologyHardFenceSupersedesSuspension(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(1, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}
	suspension := topology.SuspendAdmission()
	if suspension == nil {
		t.Fatal("current topology did not suspend")
	}
	topology.BlockAdmission()
	if err = topology.ResumeAdmission(context.Background(), suspension); !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("resume after hard fence=%v", err)
	}
}

func TestTopologyReadRechecksAuthorityAfterWaitingForWriterGate(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	peerIdentity, _ := NewIdentity(testPeerBare, testPeerFull)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
		Peers: []PeerAuthorityResult{{Identity: peerIdentity, Authorized: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}

	if err = topology.gate.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, lookupErr := topology.Lookup(testPeerBare)
		result <- lookupErr
	}()
	waitTopologyGateQueue(t, &topology.gate, 0, 1)
	topology.BlockAdmission()
	topology.gate.unlock()
	if err = <-result; !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("lookup crossed authority fence: %v", err)
	}
}

func TestStaleReplacementDoesNotClearNewerPeerAuthorityPublication(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	peerIdentity, _ := NewIdentity(testPeerBare, testPeerFull)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
		Peers: []PeerAuthorityResult{{Identity: peerIdentity, Authorized: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}

	if err = topology.gate.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- topology.Replace(AuthoritativeGroupSnapshot{
			MeshID: testMesh, ObservedAt: now,
			Members: []Identity{local},
		})
	}()
	waitTopologyGateQueue(t, &topology.gate, 1, 0)
	// The replacement captured the former live token before it queued. A hard
	// authority fence now makes that writer stale while the gate still excludes
	// its map mutation.
	topology.BlockAdmission()
	topology.gate.unlock()
	if err = <-replaceDone; !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("stale replacement=%v", err)
	}
	if len(topology.byAgent) != 2 || len(topology.byAgent[testPeerBare]) != 1 || topology.byAgent[testPeerBare][0] != peerIdentity {
		t.Fatalf("stale replacement mutated membership: %#v", topology.byAgent)
	}

	now = now.Add(time.Millisecond)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
		Peers: []PeerAuthorityResult{{Identity: peerIdentity, Authorized: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatalf("newer peer authority could not publish: %v", err)
	}
	if _, err = topology.Lookup(testPeerBare); err != nil {
		t.Fatalf("newer peer authority was overwritten: %v", err)
	}
}

func TestTopologyContextResolutionWaitsForFreshAuthorityAndHonorsRemoval(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	peerIdentity, _ := NewIdentity(testPeerBare, testPeerFull)
	install := func(includePeer bool) {
		t.Helper()
		peers := []PeerAuthorityResult(nil)
		if includePeer {
			peers = []PeerAuthorityResult{{Identity: peerIdentity, Authorized: true}}
		}
		if installErr := topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
			MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true, Peers: peers,
		}); installErr != nil {
			t.Fatal(installErr)
		}
		if publishErr := topology.PublishPeerAuthority(); publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	install(true)
	topology.BlockAdmission()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resolved := make(chan error, 1)
	go func() {
		_, resolveErr := topology.ResolveAgentContext(ctx, testMesh, testLocalFull, testPeerBare)
		resolved <- resolveErr
	}()
	select {
	case err = <-resolved:
		t.Fatalf("resolution crossed authority fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	now = now.Add(time.Millisecond)
	install(false)
	if err = <-resolved; !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("resolution after removal=%v", err)
	}
}

func TestTopologyAdmissionWaitsForCurrentAuthorityOrCallerDeadline(t *testing.T) {
	topology, err := NewTopology(1, testMesh, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = topology.acquireAdmissionContext(ctx, testMesh, testLocalFull); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked admission=%v", err)
	}
}

func TestClassifyMeshErrorPreservesAuthorityWaitDeadline(t *testing.T) {
	if failure := classifyMeshError(context.DeadlineExceeded); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("deadline classification=%#v", failure)
	}
	if failure := classifyMeshError(context.Canceled); failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("cancellation classification=%#v", failure)
	}
}

func TestTopologySeparatesMalformedIdentityFromUnknownCurrentMember(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local, _ := NewIdentity(testLocalBare, testLocalFull)
	if err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}

	if _, err = topology.ResolveAgentContext(context.Background(), testMesh, testLocalFull, "bad\x00identity"); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("malformed destination=%v, want invalid identity", err)
	}
	if failure := classifyMeshError(err); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("malformed destination classification=%#v", failure)
	}
	if _, err = topology.ResolveAgentContext(context.Background(), testMesh, testLocalFull, "absent@example.test"); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("unknown destination=%v, want identity unknown", err)
	}
	if failure := classifyMeshError(err); failure == nil || failure.Code != FailureAuthorization {
		t.Fatalf("unknown destination classification=%#v", failure)
	}
}

func TestAuthoritativeTopologyCurrentReplacementRules(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(4, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(500 * time.Millisecond)
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: append([]Identity(nil), members...)}); err != nil {
		t.Fatalf("identical current renewal = %v", err)
	}
	changed := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: changed}); err != nil {
		t.Fatalf("current membership change = %v", err)
	}
	if _, err := topology.Lookup(testPeerBare); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("removed peer lookup = %v", err)
	}
	now = now.Add(2 * time.Millisecond)
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: nil}); err != nil {
		t.Fatalf("authoritative removal = %v", err)
	}
	if _, err := topology.Lookup(testLocalBare); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("removed member lookup = %v", err)
	}
	now = now.Add(time.Millisecond)
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatalf("current re-add = %v", err)
	}
}

func TestPeerAuthorityCurrentSnapshotRevokesAndReauthorizes(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(3, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local := Identity{AgentID: testLocalBare, Internal: testLocalFull}
	peer := Identity{AgentID: testPeerBare, Internal: testPeerFull}
	initial := PeerAuthoritySnapshot{MeshID: testMesh, ObservedAt: now, Local: local, LocalAuthorized: true, Peers: []PeerAuthorityResult{{Identity: peer, Authorized: true}}}
	if err := topology.ApplyPeerAuthority(initial); err != nil {
		t.Fatal(err)
	}
	if err := topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}
	if _, err := topology.Lookup(testPeerBare); err != nil {
		t.Fatalf("authorized peer missing: %v", err)
	}
	now = now.Add(time.Millisecond)
	revoked := initial
	revoked.ObservedAt = now
	revoked.Peers = []PeerAuthorityResult{{Identity: peer, Authorized: false}}
	if err := topology.ApplyPeerAuthority(revoked); err != nil {
		t.Fatalf("current revoke: %v", err)
	}
	if err := topology.PublishPeerAuthority(); err != nil {
		t.Fatalf("publish revoke: %v", err)
	}
	if _, err := topology.Lookup(testPeerBare); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("revoked peer lookup=%v", err)
	}
	now = now.Add(time.Millisecond)
	reauthorized := revoked
	reauthorized.ObservedAt = now
	reauthorized.Peers = []PeerAuthorityResult{{Identity: peer, Authorized: true}}
	if err := topology.ApplyPeerAuthority(reauthorized); err != nil {
		t.Fatalf("current reauthorization: %v", err)
	}
	if err := topology.PublishPeerAuthority(); err != nil {
		t.Fatalf("publish reauthorization: %v", err)
	}
	if _, err := topology.Lookup(testPeerBare); err != nil {
		t.Fatalf("reauthorized peer lookup=%v", err)
	}
	invalid := revoked
	invalid.ObservedAt = now.Add(time.Hour)
	invalid.Peers = []PeerAuthorityResult{{Identity: peer, Authorized: true}, {Identity: peer, Authorized: false}}
	if err := topology.ApplyPeerAuthority(invalid); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("invalid partial snapshot=%v", err)
	}
	if _, err := topology.Lookup(testPeerBare); err != nil {
		t.Fatal("invalid peer snapshot partially mutated current membership")
	}
	topology.BlockAdmission()
	if _, err := topology.Lookup(testLocalBare); !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("blocked authority lookup=%v", err)
	}
}

func TestPeerAuthorityDiagnosticCountTracksSynchronizedWorkingSet(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(3, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	local := Identity{AgentID: testLocalBare, Internal: testLocalFull}
	peer := Identity{AgentID: testPeerBare, Internal: testPeerFull}
	localOnly := PeerAuthoritySnapshot{
		MeshID: testMesh, ObservedAt: now,
		Local: local, LocalAuthorized: true,
	}
	if err := topology.ApplyPeerAuthority(localOnly); err != nil {
		t.Fatal(err)
	}
	if got := topology.memberCountExcluding(testLocalFull); got != 0 {
		t.Fatalf("local-only synchronized working set count=%d, want 0", got)
	}
	now = now.Add(time.Millisecond)
	withPeer := localOnly
	withPeer.ObservedAt = now
	withPeer.Peers = []PeerAuthorityResult{{Identity: peer, Authorized: true}}
	if err := topology.ApplyPeerAuthority(withPeer); err != nil {
		t.Fatal(err)
	}
	if got := topology.memberCountExcluding(testLocalFull); got != 1 {
		t.Fatalf("peer-synchronized working set count=%d, want 1", got)
	}
}

func TestTopologyRefreshCancellationRemovesWaitingWriterWithoutCommit(t *testing.T) {
	now := time.Date(2026, 8, 14, 18, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatal(err)
	}
	admission, err := topology.acquireAdmission(testMesh, testLocalFull, testPeerFull)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- topology.Refresh(ctx, testTopologySource{snapshot: AuthoritativeGroupSnapshot{
			MeshID: testMesh, ObservedAt: now, Members: []Identity{members[0]},
		}})
	}()
	waitTopologyGateQueue(t, &topology.gate, 1, 0)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled refresh=%v", err)
	}
	if _, err := topology.Lookup(testPeerBare); err != nil {
		t.Fatalf("canceled writer changed membership: %v", err)
	}
	admission.release()
}

func TestTopologyAdmissionCopiesMemberWhileSnapshotWriterIsQueued(t *testing.T) {
	now := time.Date(2026, 8, 14, 18, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if err = topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatal(err)
	}
	admission, err := topology.acquireAdmission(testMesh, testLocalFull, testPeerFull)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{members[0]}})
	}()
	waitTopologyGateQueue(t, &topology.gate, 1, 0)

	peer, err := admission.identity(testPeerFull)
	if err != nil || peer != members[1] {
		admission.release()
		t.Fatalf("admitted peer=%#v err=%v", peer, err)
	}
	admission.release()
	if err = <-writerDone; err != nil {
		t.Fatalf("queued replacement=%v", err)
	}
	if _, err := topology.Lookup(testPeerBare); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("queued replacement did not remove peer: %v", err)
	}
}

func TestTopologyGateWaitingWriterPrecedesLaterReaders(t *testing.T) {
	var gate topologyGate
	gate.readLock()
	writerAcquired := make(chan struct{})
	writerRelease := make(chan struct{})
	go func() {
		if err := gate.lock(context.Background()); err != nil {
			return
		}
		close(writerAcquired)
		<-writerRelease
		gate.unlock()
	}()
	waitTopologyGateQueue(t, &gate, 1, 0)
	readerAcquired := make(chan struct{})
	go func() {
		gate.readLock()
		close(readerAcquired)
		gate.readUnlock()
	}()
	waitTopologyGateQueue(t, &gate, 1, 1)
	gate.readUnlock()
	<-writerAcquired
	select {
	case <-readerAcquired:
		t.Fatal("later reader crossed queued writer")
	default:
	}
	close(writerRelease)
	<-readerAcquired
}

func TestTopologyGateConcurrentReleaseCancelHasNoLostWakeup(t *testing.T) {
	for range 1_000 {
		var gate topologyGate
		gate.readLock()
		ctx, cancel := context.WithCancel(context.Background())
		writerDone := make(chan error, 1)
		go func() { writerDone <- gate.lock(ctx) }()
		waitTopologyGateQueue(t, &gate, 1, 0)
		start := make(chan struct{})
		var concurrent sync.WaitGroup
		concurrent.Add(2)
		go func() {
			defer concurrent.Done()
			<-start
			cancel()
		}()
		go func() {
			defer concurrent.Done()
			<-start
			gate.readUnlock()
		}()
		close(start)
		concurrent.Wait()
		if err := <-writerDone; err == nil {
			gate.unlock()
		} else if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer result=%v", err)
		}
		probe, probeCancel := context.WithTimeout(context.Background(), time.Second)
		if err := gate.lock(probe); err != nil {
			probeCancel()
			t.Fatalf("gate wedged after release/cancel: %v", err)
		}
		probeCancel()
		gate.unlock()
	}
}

func TestTopologyWriterRechecksCancellationImmediatelyBeforeCommit(t *testing.T) {
	now := time.Date(2026, 8, 14, 18, 0, 0, 0, time.UTC)
	entered := make(chan struct{})
	release := make(chan struct{})
	var block sync.Mutex
	blocked := false
	topology, err := NewTopology(2, testMesh, func() time.Time {
		block.Lock()
		shouldBlock := blocked
		block.Unlock()
		if shouldBlock {
			close(entered)
			<-release
		}
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatal(err)
	}
	block.Lock()
	blocked = true
	block.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- topology.Refresh(ctx, testTopologySource{snapshot: AuthoritativeGroupSnapshot{
			MeshID: testMesh, ObservedAt: now, Members: []Identity{members[0]},
		}})
	}()
	<-entered
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("commit-boundary cancellation=%v", err)
	}
	block.Lock()
	blocked = false
	block.Unlock()
	if _, err := topology.Lookup(testPeerBare); err != nil {
		t.Fatalf("canceled writer changed membership: %v", err)
	}
}

func waitTopologyGateQueue(t *testing.T, gate *topologyGate, writers, readers int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		queuedWriters, queuedReaders := 0, 0
		for _, waiter := range gate.waiters {
			if waiter.writer {
				queuedWriters++
			} else {
				queuedReaders++
			}
		}
		gate.mu.Unlock()
		if queuedWriters == writers && queuedReaders == readers {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gate queue writers=%d readers=%d, want writers=%d readers=%d", queuedWriters, queuedReaders, writers, readers)
		}
		runtime.Gosched()
	}
}

func TestAuthoritativeTopologyTreatsResourceAsOpaqueAndRejectsMalformedOrUnorderedMembers(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: "peer@example", Internal: "peer@example/other"}}}); err != nil {
		t.Fatalf("authenticated opaque resource rejected: %v", err)
	}
	invalid := []Identity{{AgentID: "peer@example/extra", Internal: "peer@example/extra/mesh"}}
	for _, member := range invalid {
		if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{member}}); !errors.Is(err, ErrSnapshotInvalid) {
			t.Fatalf("accepted %#v: %v", member, err)
		}
	}
	unordered := []Identity{{AgentID: testPeerBare, Internal: testPeerFull}, {AgentID: testLocalBare, Internal: testLocalFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: unordered}); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("accepted unordered members: %v", err)
	}
}

func TestTopologyAdmissionReadLeaseExcludesSnapshotReplace(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(2, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members}); err != nil {
		t.Fatal(err)
	}
	admission, err := topology.acquireAdmission(testMesh, testLocalFull, testPeerFull)
	if err != nil {
		t.Fatal(err)
	}
	var started sync.WaitGroup
	started.Add(1)
	done := make(chan error, 1)
	go func() {
		started.Done()
		done <- topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: members[:1]})
	}()
	started.Wait()
	select {
	case err := <-done:
		t.Fatalf("Replace crossed read admission: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	admission.release()
	admission.release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := topology.acquireAdmission(testMesh, testLocalFull, testPeerFull); !errors.Is(err, ErrMembershipMissing) {
		t.Fatalf("removed member admission = %v", err)
	}
}
