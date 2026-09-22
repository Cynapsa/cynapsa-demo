package cynapsagocore

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type authorityObservationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *authorityObservationClock) Snapshot() mesh.CalibratedTime {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return mesh.CalibratedTime{UTC: clock.now, Uncertainty: 250 * time.Millisecond}
}

func (clock *authorityObservationClock) set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func TestRank2TopologyObservationDoesNotExpireDuringLocalReconciliation(t *testing.T) {
	observedAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	formerLease := 10 * time.Second
	clock := &authorityObservationClock{now: observedAt}
	source := &rank2GroupTopologySource{
		clock: clock, meshID: authorityRetryMesh,
		localBare: authorityRetryLocalBare, localFull: authorityRetryLocalFull,
		synchronizeMembership: func(context.Context) ([]mesh.Identity, error) {
			// Model reconciliation that runs beyond the former lease boundary.
			clock.set(observedAt.Add(formerLease))
			return []mesh.Identity{
				{AgentID: authorityRetryLocalBare, Internal: authorityRetryLocalFull},
				{AgentID: authorityRetryPeerBare, Internal: authorityRetryPeerFull},
			}, nil
		},
	}
	snapshot, disposition := source.SynchronizeCurrentMembership(context.Background(), authorityRetryMesh)
	if disposition != mesh.TopologyReady {
		t.Fatalf("synchronization disposition=%v", disposition)
	}
	if !snapshot.ObservedAt.Equal(observedAt) {
		t.Fatalf("ObservedAt=%s want fetch-start observation %s", snapshot.ObservedAt, observedAt)
	}
	topology, err := mesh.NewTopology(2, authorityRetryMesh, func() time.Time { return clock.Snapshot().UTC })
	if err != nil {
		t.Fatal(err)
	}
	if err = topology.ApplyPeerAuthority(snapshot); err != nil {
		t.Fatalf("session authority expired during reconciliation: %v", err)
	}
	if err = topology.PublishPeerAuthority(); err != nil {
		t.Fatal(err)
	}
	clock.set(observedAt.Add(24 * time.Hour))
	if _, err = topology.Lookup(authorityRetryPeerBare); err != nil {
		t.Fatalf("connected session authority expired with wall clock: %v", err)
	}
}

func TestRank2TopologyExcludesOnlyExactLocalEndpoint(t *testing.T) {
	observedAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	localBare := "agent@example.test"
	local := localBare + "/r2.00000000-0000-4000-8000-000000000001.AAAAAAAAAAAAAAAA"
	sibling := localBare + "/r2.00000000-0000-4000-8000-000000000002.BBBBBBBBBBBBBBBB"
	peer := "peer@example.test/r2.00000000-0000-4000-8000-000000000003.CCCCCCCCCCCCCCCC"
	source := &rank2GroupTopologySource{
		clock: &authorityObservationClock{now: observedAt}, meshID: authorityRetryMesh,
		localBare: localBare, localFull: local,
		synchronizeMembership: func(context.Context) ([]mesh.Identity, error) {
			return []mesh.Identity{
				{AgentID: localBare, Internal: local},
				{AgentID: localBare, Internal: sibling},
				{AgentID: "peer@example.test", Internal: peer},
			}, nil
		},
	}
	snapshot, disposition := source.SynchronizeCurrentMembership(context.Background(), authorityRetryMesh)
	if disposition != mesh.TopologyReady || len(snapshot.Peers) != 2 {
		t.Fatalf("snapshot=%#v disposition=%v", snapshot, disposition)
	}
	if snapshot.Local.Internal != local || snapshot.Peers[0].Identity.Internal != sibling || snapshot.Peers[1].Identity.Internal != peer {
		t.Fatalf("exact endpoint filtering=%#v", snapshot)
	}
}

func TestRank1MembershipFenceAdvancesPublicationTokenAndBlocksManagerFirst(t *testing.T) {
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	peer := "peer@example.test/mesh"
	if err = live.InstallLive(context.Background(), peer, &rootLiveCarrier{}); err != nil {
		t.Fatal(err)
	}
	authority := &rank1MembershipAuthority{live: live, meshID: "mesh", localFull: "local@example.test/mesh", ready: true}
	authority.Fence()
	firstEpoch := authority.managerEpoch
	if !authority.blocked || authority.ready || firstEpoch == 0 {
		t.Fatalf("fence state blocked=%t ready=%t epoch=%d", authority.blocked, authority.ready, firstEpoch)
	}
	if err = live.Send(context.Background(), transport.KindLive, rootRank2Envelope(t, 1, "fenced")); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("manager admission remained open: %v", err)
	}
	authority.Fence()
	if authority.managerEpoch == 0 || authority.managerEpoch == firstEpoch {
		t.Fatalf("repeated fence did not supersede exact owner: got=%d previous=%d", authority.managerEpoch, firstEpoch)
	}
}

func TestRank1MembershipFenceGenerationExhaustionFailsClosedWithoutWrap(t *testing.T) {
	live, err := transport.NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	authority := &rank1MembershipAuthority{
		live: live, ready: true, generation: math.MaxUint64,
		membershipSession: &mesh.MembershipAuthoritySession{},
		pendingSnapshot:   rank2xmpp.AuthoritySnapshot{Members: []string{"local@example.test/mesh"}},
	}

	authority.Fence()
	authority.Fence()
	authority.mu.Lock()
	generation, exhausted := authority.generation, authority.generationExhausted
	blocked, ready := authority.blocked, authority.ready
	session, pending := authority.membershipSession, authority.pendingSnapshot
	managerEpoch := authority.managerEpoch
	authority.mu.Unlock()
	if generation != math.MaxUint64 || !exhausted || !blocked || ready || session != nil || len(pending.Members) != 0 {
		t.Fatalf("exhausted fence state generation=%d exhausted=%t blocked=%t ready=%t session=%p pending=%#v", generation, exhausted, blocked, ready, session, pending)
	}
	if managerEpoch == 0 || live.LiveAuthorityPublicationCurrent(managerEpoch) {
		t.Fatalf("manager gate not fail closed: epoch=%d current=%t", managerEpoch, live.LiveAuthorityPublicationCurrent(managerEpoch))
	}
	if authority.MembershipReady() {
		t.Fatal("exhausted authority reported ready")
	}
	if _, err = authority.synchronize(context.Background()); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("exhausted synchronization=%v", err)
	}
}

func TestRank1MembershipExhaustionRejectsStalePublication(t *testing.T) {
	live, err := transport.NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	staleEpoch := live.BlockLiveAuthority()
	if err = live.ReconcileLiveAuthority(context.Background(), staleEpoch, nil); err != nil {
		t.Fatal(err)
	}
	authority := &rank1MembershipAuthority{
		live: live, messaging: &mesh.MessagingService{},
		blocked: true, managerEpoch: staleEpoch, generation: math.MaxUint64,
		membershipSession: &mesh.MembershipAuthoritySession{},
		pendingSnapshot:   rank2xmpp.AuthoritySnapshot{Members: []string{"local@example.test/mesh"}},
	}

	authority.Fence()
	if err = authority.publishCurrentMembership(context.Background()); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("stale publication after exhaustion=%v", err)
	}
	if err = live.PublishLiveAuthority(staleEpoch); err == nil {
		t.Fatal("stale manager epoch reopened after Rank1 generation exhaustion")
	}
	authority.mu.Lock()
	generation, exhausted, blocked, ready := authority.generation, authority.generationExhausted, authority.blocked, authority.ready
	authority.mu.Unlock()
	if generation != math.MaxUint64 || !exhausted || !blocked || ready {
		t.Fatalf("stale publication changed exhausted state generation=%d exhausted=%t blocked=%t ready=%t", generation, exhausted, blocked, ready)
	}
}

func TestRank1PublicationPausedBeforeRank2AckCannotSurviveExplicitFence(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	clientOutbox, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	client, err := rank2xmpp.NewClient(rank2xmpp.Config{
		Endpoint:        "mesh.example.test:5222",
		Auth:            rank2xmpp.Authentication{Username: authorityRetryLocalBare, Password: []byte("secret"), MeshID: authorityRetryMesh},
		ReceiveCapacity: 8, TransferWorkers: 1, TransferQueue: 8, TransferByteCapacity: 1 << 20,
		UnresolvedTransferCapacity: 8, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second,
		MailboxLimit: 8, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond,
		ReconnectOperationTimeout: time.Second,
	}, builtInTestDialer{session: newBuiltInTestSession()}, clientOutbox, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	carrier := &rootLiveCarrier{}
	if err = live.InstallLive(context.Background(), authorityRetryPeerFull, carrier); err != nil {
		t.Fatal(err)
	}

	bootstrapAuthority := &rank1MembershipAuthority{blocked: true}
	source := &authorityRetrySource{authority: bootstrapAuthority, now: now}
	identity, err := mesh.NewSessionIdentity(authorityRetryMesh, authorityRetryLocalBare, authorityRetryLocalFull, authorityRetryVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	policies, err := mesh.NewPolicyController(nil)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := mesh.NewHandlerRegistry(8)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	messaging, err := mesh.NewMessagingService(mesh.MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20,
		RPCTimeout: authorityRetryRPCTimeout(time.Second), OperationTimeout: 30 * time.Second, PeerIdleTimeout: 30 * time.Second, OutboxPollInterval: 30 * time.Second,
		Clock: authorityRetryClock{now: now},
	}, mesh.MessagingDependencies{
		Identity: identity, PeerAuthority: source, Carrier: &authorityRetryCarrier{delivered: make(chan protocol.Envelope, 1)},
		Payloads: authorityRetryPipelineFactory{}, Deliveries: authorityRetryDeliverySink{},
		Policies: policies, Handlers: handlers, Outbox: queued,
	})
	if err != nil {
		t.Fatal(err)
	}
	source.service = messaging
	if failure := messaging.Start(context.Background()); failure != nil {
		t.Fatalf("start messaging: %#v", failure)
	}
	t.Cleanup(func() { _ = messaging.Shutdown(context.Background()) })

	authority := &rank1MembershipAuthority{
		client: client, live: live, timeout: time.Second,
		meshID: authorityRetryMesh, localBare: authorityRetryLocalBare, localFull: authorityRetryLocalFull,
		messaging: messaging,
	}
	source.mu.Lock()
	source.authority = authority
	source.mu.Unlock()
	members := []mesh.Identity{
		{AgentID: authorityRetryLocalBare, Internal: authorityRetryLocalFull},
		{AgentID: authorityRetryPeerBare, Internal: authorityRetryPeerFull},
	}

	for iteration := 0; iteration < 100; iteration++ {
		snapshot, syncErr := client.SyncAuthority(context.Background())
		if syncErr != nil {
			t.Fatalf("iteration %d sync: %v", iteration, syncErr)
		}
		epoch := live.BlockLiveAuthority()
		if reconcileErr := live.ReconcileLiveAuthority(context.Background(), epoch, []string{authorityRetryPeerFull}); reconcileErr != nil {
			t.Fatalf("iteration %d reconcile: %v", iteration, reconcileErr)
		}
		session, failure := messaging.BeginMembershipSynchronization()
		if failure != nil {
			t.Fatalf("iteration %d begin: %#v", iteration, failure)
		}
		if failure = messaging.InstallCurrentMembership(session, members); failure != nil {
			t.Fatalf("iteration %d install: %#v", iteration, failure)
		}
		entered := make(chan struct{})
		release := make(chan struct{})
		authority.mu.Lock()
		authority.blocked = true
		authority.ready = false
		authority.managerEpoch = epoch
		authority.membershipSession = session
		authority.pendingSnapshot = snapshot
		authority.beforeAcknowledge = func() {
			close(entered)
			<-release
		}
		authority.mu.Unlock()

		published := make(chan error, 1)
		go func() { published <- authority.publishCurrentMembership(context.Background()) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d publication did not reach pre-ack hook", iteration)
		}
		authority.Fence()
		close(release)
		select {
		case publishErr := <-published:
			if !errors.Is(publishErr, rank2xmpp.ErrUnavailable) {
				t.Fatalf("iteration %d stale publication = %v", iteration, publishErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d stale publication did not return", iteration)
		}
		authority.mu.Lock()
		authority.beforeAcknowledge = nil
		blocked, ready, currentEpoch := authority.blocked, authority.ready, authority.managerEpoch
		authority.mu.Unlock()
		if !blocked || ready || currentEpoch == epoch || client.MembershipReady() {
			t.Fatalf("iteration %d stale gates reopened: blocked=%t ready=%t epoch=%d stale=%d rank2=%t", iteration, blocked, ready, currentEpoch, epoch, client.MembershipReady())
		}
		envelope := rootRank2Envelope(t, uint64(iteration+1), "fenced")
		envelope.Sender, envelope.Recipient, envelope.MeshID = authorityRetryLocalFull, authorityRetryPeerFull, authorityRetryMesh
		if sendErr := live.Send(context.Background(), transport.KindLive, envelope); !errors.Is(sendErr, transport.ErrUnavailable) {
			t.Fatalf("iteration %d live admission = %v", iteration, sendErr)
		}
		if failure = messaging.AuthorizeCurrentPeer(authorityRetryPeerFull); failure == nil || failure.Code != mesh.FailureUnavailable {
			t.Fatalf("iteration %d peer admission = %#v", iteration, failure)
		}
		if failure = messaging.PublishCurrentMembership(session); failure == nil {
			t.Fatalf("iteration %d stale peer session published", iteration)
		}
	}
}

func TestRank1MembershipAuthorizationFailsClosedUntilReady(t *testing.T) {
	authority := &rank1MembershipAuthority{client: &rank2xmpp.Client{}, live: &transport.Manager{}, meshID: "mesh", localFull: "local@example.test/mesh", blocked: true}
	if err := authority.AuthorizeRank1(context.Background(), "peer@example.test/mesh"); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("blocked authorization=%v", err)
	}
	authority.blocked = false
	authority.ready = true
	if err := authority.AuthorizeRank1(context.Background(), "peer@example.test"); !errors.Is(err, rank2xmpp.ErrProtocol) {
		t.Fatalf("malformed endpoint authorization=%v", err)
	}
	if err := authority.AuthorizeRank1(context.Background(), authority.localFull); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("self authorization=%v", err)
	}
}

func TestRank1MembershipSnapshotRequiresExactLocalMemberAndTreatsResourceAsOpaque(t *testing.T) {
	authority := &rank1MembershipAuthority{meshID: "mesh", localFull: "local@example.test/mesh"}
	members, err := authority.snapshotMembers([]string{"local@example.test/mesh", "peer@example.test/mesh"})
	if err != nil || len(members) != 2 || members[1].Internal != "peer@example.test/mesh" {
		t.Fatalf("members=%#v err=%v", members, err)
	}
	for _, invalid := range [][]string{
		{"peer@example.test/mesh"},
		{"local@example.test/mesh", "bad/resource/mesh"},
		{"local@example.test/mesh", "local@example.test/mesh"},
		{"peer@example.test/mesh", "local@example.test/mesh"},
	} {
		if _, err = authority.snapshotMembers(invalid); err == nil {
			t.Fatalf("invalid snapshot accepted: %q", invalid)
		}
	}
	if members, err = authority.snapshotMembers([]string{"local@example.test/mesh", "peer@example.test/other"}); err != nil || len(members) != 2 {
		t.Fatalf("authenticated endpoint resource was treated as mesh authority: members=%#v err=%v", members, err)
	}
}
