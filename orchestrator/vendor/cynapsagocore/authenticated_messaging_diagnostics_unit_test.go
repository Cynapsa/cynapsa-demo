package cynapsagocore

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestAggregatePeerStatusUsesExactLegacyAndRuntimeV2Endpoints(t *testing.T) {
	live, err := transport.NewLiveManager(3)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	legacy := "peer@example.test/mesh"
	runtimeV2 := "peer@example.test/r2.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAA"
	if err = live.InstallLive(context.Background(), legacy, &rootLiveCarrier{state: transport.HealthConnecting}); err != nil {
		t.Fatal(err)
	}
	if err = live.InstallLive(context.Background(), runtimeV2, &rootLiveCarrier{state: transport.HealthHealthy}); err != nil {
		t.Fatal(err)
	}
	for name, endpoints := range map[string][]string{
		"runtime-v2": {runtimeV2},
	} {
		t.Run(name, func(t *testing.T) {
			status := aggregatePeerStatus("peer@example.test", endpoints, live, nil)
			if status.Peer != "peer@example.test" || status.Connectivity != "available" || !status.Reachable || status.RecoveryInProgress {
				t.Fatalf("status=%#v", status)
			}
		})
	}
	mixed := aggregatePeerStatus("peer@example.test", []string{legacy, runtimeV2}, live, nil)
	if mixed.Connectivity != "available" || !mixed.Reachable || !mixed.RecoveryInProgress {
		t.Fatalf("mixed=%#v", mixed)
	}
	connecting := aggregatePeerStatus("peer@example.test", []string{legacy}, live, nil)
	if connecting.Connectivity != "degraded" || connecting.Reachable || !connecting.RecoveryInProgress {
		t.Fatalf("connecting=%#v", connecting)
	}
	absent := aggregatePeerStatus("peer@example.test", nil, live, nil)
	if absent.Connectivity != "unavailable" || absent.Reachable || absent.RecoveryInProgress {
		t.Fatalf("absent=%#v", absent)
	}
}

func TestDiagnosticsPeerPropagatesDeadlineWhileAuthorityIsFenced(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	authority := &rank1MembershipAuthority{live: live, blocked: true}
	source := &authorityRetrySource{authority: authority, now: now}
	identity, err := mesh.NewSessionIdentity(authorityRetryMesh, authorityRetryLocalBare, authorityRetryLocalFull, authorityRetryVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	policies, _ := mesh.NewPolicyController(nil)
	handlers, _ := mesh.NewHandlerRegistry(8)
	messaging, err := mesh.NewMessagingService(mesh.MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20,
		RPCTimeout: authorityRetryRPCTimeout(time.Second), OperationTimeout: time.Second,
		PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second, Clock: authorityRetryClock{now: now},
	}, mesh.MessagingDependencies{
		Identity: identity, PeerAuthority: source,
		Carrier:  &authorityRetryCarrier{delivered: make(chan protocol.Envelope, 1)},
		Payloads: authorityRetryPipelineFactory{}, Deliveries: authorityRetryDeliverySink{},
		Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	source.service = messaging
	authority.messaging = messaging
	if failure := messaging.Start(context.Background()); failure != nil {
		t.Fatalf("start=%#v", failure)
	}
	t.Cleanup(func() { _ = messaging.Shutdown(context.Background()) })
	messaging.BlockAuthorityAdmission()
	service := &authenticatedMessagingService{
		messaging: messaging,
		rank1:     &rank1MessagingRuntime{live: live},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result, failure := service.diagnosticsPeer(ctx, nil, model.DiagnosticsPeerArgs{Peer: authorityRetryPeerBare})
	if failure == nil || failure.Code != sessionkernel.ProviderDeadline || result != (model.PeerStatus{}) {
		t.Fatalf("result=%#v failure=%#v", result, failure)
	}
}
