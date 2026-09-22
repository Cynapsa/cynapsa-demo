package mesh

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestAcceptanceAuthoritativeTopologySessionLifetimeRemovalReaddAndRaces(t *testing.T) {
	base := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	var unixNanos atomic.Int64
	unixNanos.Store(base.UnixNano())
	now := func() time.Time { return time.Unix(0, unixNanos.Load()).UTC() }
	topology, err := NewTopology(4, "mesh", now)
	if err != nil {
		t.Fatal(err)
	}
	members := []Identity{{AgentID: "agent-a", Internal: "agent-a/mesh"}, {AgentID: "agent-b", Internal: "agent-b/mesh"}}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: base, Members: members}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	done := make(chan struct{})
	var wait sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := topology.RequireAll("mesh", "agent-a/mesh", "agent-b/mesh"); err != nil {
					t.Errorf("RequireAll: %v", err)
					return
				}
			}
		}()
	}
	close(start)
	for replacement := 0; replacement < 255; replacement++ {
		if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: base, Members: members}); err != nil {
			t.Fatalf("replacement %d: %v", replacement, err)
		}
	}
	close(done)
	wait.Wait()
	invalid := []AuthoritativeGroupSnapshot{
		{MeshID: "mesh", ObservedAt: base.Add(time.Second), Members: members},
		{MeshID: "mesh", ObservedAt: base, Members: []Identity{{AgentID: "attacker/extra", Internal: "attacker/extra/mesh"}}},
	}
	for _, snapshot := range invalid {
		if err := topology.Replace(snapshot); !errors.Is(err, ErrSnapshotInvalid) {
			t.Fatalf("invalid snapshot accepted: %#v err=%v", snapshot, err)
		}
		if err := topology.RequireAll("mesh", "agent-a/mesh", "agent-b/mesh"); err != nil {
			t.Fatalf("invalid replacement altered state: err=%v", err)
		}
	}

	unixNanos.Store(base.Add(24 * time.Hour).UnixNano())
	if err := topology.RequireAll("mesh", "agent-a/mesh"); err != nil {
		t.Fatalf("connected authority expired with wall clock: %v", err)
	}
	renewedAt := now()
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: renewedAt}); err != nil {
		t.Fatalf("authoritative removal rejected: %v", err)
	}
	if _, err := topology.Lookup("agent-a"); !errors.Is(err, ErrIdentityUnknown) {
		t.Fatalf("removed member lookup = %v", err)
	}
	unixNanos.Add(int64(time.Millisecond))
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: now(), Members: members}); err != nil {
		t.Fatalf("current re-add rejected: %v", err)
	}
	if identity, err := topology.Lookup("agent-a"); err != nil || identity.Internal != "agent-a/mesh" {
		t.Fatalf("re-added member lookup = %#v %v", identity, err)
	}
}

func TestAcceptanceAuthenticationSecretCanaryNeverEscapesOwnedCallbacksOrFormatting(t *testing.T) {
	canary := []byte("PRIVATE-CREDENTIAL-CANARY-2d91")
	input := CredentialInput{MeshID: "mesh", Username: "agent", Password: canary}
	store := NewCredentialStore()
	if err := store.Replace(input); err != nil {
		t.Fatal(err)
	}

	var callbackCopy []byte
	privateCallbackErr := errors.New(string(canary))
	if err := store.WithPassword("mesh", func(_ string, password []byte) error {
		callbackCopy = password
		return privateCallbackErr
	}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("callback error = %v", err)
	}
	for index, value := range callbackCopy {
		if value != 0 {
			t.Fatalf("temporary callback password byte %d was not cleared", index)
		}
	}
	formatted := []string{
		fmt.Sprintf("%v", input), fmt.Sprintf("%+v", input), fmt.Sprintf("%#v", input),
		fmt.Sprintf("%v", store), fmt.Sprintf("%+v", store), fmt.Sprintf("%#v", store),
		fmt.Sprintf("%v", ErrInvalidCredential), fmt.Sprintf("%#v", ErrInvalidCredential),
	}
	for _, text := range formatted {
		if strings.Contains(text, string(canary)) {
			t.Fatalf("credential canary leaked: %s", text)
		}
	}

	old := store.active
	if err := store.Remove("mesh"); err != nil {
		t.Fatal(err)
	}
	if strings.Trim(string(old.password), "\x00") != "" {
		t.Fatal("removed credential retained private bytes")
	}
}

func TestAcceptanceAuthoritativeTopologyMemoryStaysWithinConfiguredCapacity(t *testing.T) {
	const capacity = 2_048
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	identities := make([]Identity, capacity)
	for index := 0; index < capacity; index++ {
		agentID := fmt.Sprintf("agent-%04d@example", index)
		identities[index] = Identity{AgentID: agentID, Internal: agentID + "/mesh"}
	}
	topology, err := NewTopology(capacity-1, "mesh", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: now, Members: identities}); err != nil {
		t.Fatal(err)
	}
	if len(topology.byAgent) != capacity || len(topology.byInternal) != capacity {
		t.Fatalf("topology allocation = %d/%d", len(topology.byAgent), len(topology.byInternal))
	}
	tooMany := append(append([]Identity(nil), identities...), Identity{AgentID: "overflow@example", Internal: "overflow@example/mesh"})
	if err := topology.Replace(AuthoritativeGroupSnapshot{MeshID: "mesh", ObservedAt: now, Members: tooMany}); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("oversized topology = %v", err)
	}
	if len(topology.byAgent) != capacity || len(topology.byInternal) != capacity {
		t.Fatal("oversized topology mutated bounded state")
	}
}

func TestAcceptanceMeshListAlwaysFreshAndNeverRematerializesStaleActive(t *testing.T) {
	service, now := newTestService(t, &testCarrier{}, &testDeliverySink{}, nil)
	defer service.Shutdown(context.Background())
	calls := 0
	service.topologySrc = topologySourceFunc(func(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
		calls++
		if ctx.Err() != nil || meshID != testMesh {
			return AuthoritativeGroupSnapshot{}, TopologyUnavailable
		}
		return AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: testPeerBare, Internal: testPeerFull}}}, TopologyReady
	})

	active, failure := service.MeshList(context.Background())
	if failure != nil || len(active.Meshes) != 1 || !active.Meshes[0].Active {
		t.Fatalf("fresh active projection = %#v %#v", active, failure)
	}
	if calls != 0 {
		t.Fatalf("mesh.list unexpectedly queried authority: calls=%d", calls)
	}
	if failure = service.MeshRefresh(context.Background()); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("self-removal refresh = %#v", failure)
	}
	inactive, failure := service.MeshList(context.Background())
	if failure != nil || len(inactive.Meshes) != 1 || inactive.Meshes[0].Active || calls != 1 {
		t.Fatalf("fenced self-removal projection = %#v %#v calls=%d", inactive, failure, calls)
	}
}

func TestAcceptanceMaterializationDoesNotRefreshSessionAuthority(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	lease := 20 * time.Millisecond
	clock := &testCalibratedClock{now: now}
	source := &clockTopologySource{clock: clock}
	materializations := 0
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/refresh", AgentID: testPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(4)
	if err != nil {
		t.Fatal(err)
	}
	sink := &testDeliverySink{}
	never := make(chan time.Time)
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 4, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: lease, PeerIdleTimeout: lease, OutboxPollInterval: lease, Clock: clock, After: func(time.Duration) <-chan time.Time { return never },
	}, MessagingDependencies{
		Identity: identity, Topology: source, Carrier: &testCarrier{},
		Payloads:   advancingPipelineFactory{clock: clock, advance: now.Add(lease), calls: &materializations},
		Deliveries: sink, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start = %#v", failure)
	}
	defer service.Shutdown(context.Background())
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/refresh", "one materialization"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
		t.Fatalf("receive across final membership refresh = %#v", failure)
	}
	if materializations != 1 || source.count() != 1 || len(sink.deliveries()) != 1 {
		t.Fatalf("materializations=%d snapshots=%d deliveries=%d", materializations, source.count(), len(sink.deliveries()))
	}
}
