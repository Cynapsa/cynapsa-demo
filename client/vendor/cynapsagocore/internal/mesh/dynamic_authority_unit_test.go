package mesh

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

type dynamicResolver struct {
	mu               sync.Mutex
	result           ResolvedPeer
	disposition      TopologyDisposition
	calls            int
	exactResult      ResolvedPeer
	exactDisposition TopologyDisposition
	exactCalls       int
}

func (resolver *dynamicResolver) ResolveExactPeer(ctx context.Context, full string) (ResolvedPeer, TopologyDisposition) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.exactCalls++
	return resolver.exactResult, resolver.exactDisposition
}

func (resolver *dynamicResolver) ResolvePeer(ctx context.Context, bare string) (ResolvedPeer, TopologyDisposition) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.calls++
	return resolver.result, resolver.disposition
}

func (resolver *dynamicResolver) set(result ResolvedPeer, disposition TopologyDisposition) {
	resolver.mu.Lock()
	resolver.result, resolver.disposition = result, disposition
	resolver.mu.Unlock()
}

func (resolver *dynamicResolver) setExact(result ResolvedPeer, disposition TopologyDisposition) {
	resolver.mu.Lock()
	resolver.exactResult, resolver.exactDisposition = result, disposition
	resolver.mu.Unlock()
}

func newDynamicService(t *testing.T, resolver PeerResolver) *MessagingService {
	t.Helper()
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/epoch", AgentID: testPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(8)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second,
		Clock: &testCalibratedClock{now: time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)},
	}, MessagingDependencies{
		Identity: identity, PeerResolver: resolver, Carrier: &testCarrier{}, Payloads: testPipelineFactory{},
		Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	// The fixture stands in for the root's completed Manager publication.
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("publish local authority: %v", err)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	return service
}

func TestDynamicAuthorityLocalOnlyAndUnavailablePeer(t *testing.T) {
	resolver := &dynamicResolver{disposition: TopologyUnavailable}
	service := newDynamicService(t, resolver)
	if count := service.Diagnostics().PeerCount; count != 0 {
		t.Fatalf("local-only login has %d remote peers", count)
	}
	if _, failure := service.CurrentPeerEndpoints(context.Background(), testPeerBare); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("absent active server target: %#v", failure)
	}
	if count := service.Diagnostics().PeerCount; count != 0 {
		t.Fatalf("unavailable lookup installed %d peers", count)
	}
}

func TestDynamicPeerTrafficWaitsForPostManagerPublication(t *testing.T) {
	resolved := ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}
	resolver := &dynamicResolver{}
	resolver.set(resolved, TopologyReady)
	resolver.setExact(resolved, TopologyReady)
	service := newDynamicService(t, resolver)
	if err := service.InitializeLocalAuthority(context.Background()); err != nil {
		t.Fatalf("prepare reconnect: %v", err)
	}
	if result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "before publication")}); result.Accepted || failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("pre-publication send: result=%#v failure=%#v", result, failure)
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "before publication"))
	if accepted, failure := service.ReceiveRank2(context.Background(), testInboundProvenance(t, envelope), envelope); accepted || failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("pre-publication inbound: accepted=%t failure=%#v", accepted, failure)
	}
	resolver.mu.Lock()
	bare, exact := resolver.calls, resolver.exactCalls
	resolver.mu.Unlock()
	if bare != 0 || exact != 0 {
		t.Fatalf("pre-publication server handshake: bare=%d exact=%d", bare, exact)
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("post-publication open: %v", err)
	}
	if result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "after publication")}); !result.Accepted || failure != nil {
		t.Fatalf("post-publication send: result=%#v failure=%#v", result, failure)
	}
	service.BlockAuthorityAdmission()
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != ErrSnapshotStale {
		t.Fatalf("stale publication reopened fenced epoch: %v", err)
	}
	if result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "after fence")}); result.Accepted || failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("fenced send: result=%#v failure=%#v", result, failure)
	}
}

func TestDynamicAuthorityExactGenerationRevocationAndReconnect(t *testing.T) {
	resolver := &dynamicResolver{}
	resolver.set(ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}, TopologyReady)
	service := newDynamicService(t, resolver)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("authorize: %#v", failure)
	}
	if endpoints, failure := service.CurrentPeerEndpoints(context.Background(), testPeerBare); failure != nil || len(endpoints) != 1 || endpoints[0] != testPeerFull {
		t.Fatalf("endpoints=%v failure=%#v", endpoints, failure)
	}
	if failure := service.RevokeInstallation(context.Background(), testPeerBare, "install-1", "stale-session"); failure != nil {
		t.Fatalf("stale revoke: %#v", failure)
	}
	if failure := service.AuthorizeCurrentPeer(testPeerFull); failure != nil {
		t.Fatalf("stale generation revoked live peer: %#v", failure)
	}
	resolver.set(ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-2"}, TopologyReady)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("fresh handshake: %#v", failure)
	}
	if failure := service.RevokeInstallation(context.Background(), testPeerBare, "install-1", "session-1"); failure != nil {
		t.Fatalf("old revoke after replacement: %#v", failure)
	}
	if failure := service.AuthorizeCurrentPeer(testPeerFull); failure != nil {
		t.Fatalf("old revoke closed replacement: %#v", failure)
	}
	if failure := service.RevokeInstallation(context.Background(), testPeerBare, "install-1", "session-2"); failure != nil {
		t.Fatalf("current revoke: %#v", failure)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("current revoke retained peer state")
	}
	if _, ok := service.peerLanes.PeerEpoch(testPeerFull); ok {
		t.Fatal("current revoke retained peer authority epoch")
	}
	if failure := service.InitializeLocalAuthority(context.Background()); failure != nil {
		t.Fatalf("reconnect init: %v", failure)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("reconnect populated peers without handshakes")
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("reconnect publish: %v", err)
	}
}

func TestDynamicAuthorityLogicalRevocation(t *testing.T) {
	resolver := &dynamicResolver{}
	resolver.set(ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}, TopologyReady)
	service := newDynamicService(t, resolver)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("authorize: %#v", failure)
	}
	if failure := service.RevokeLogical(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("revoke logical: %#v", failure)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("logical revoke retained peer")
	}
	if failure := service.AuthorizeCurrentPeer(testPeerFull); failure == nil || failure.Code != FailureAuthorization {
		t.Fatalf("revoked peer authorization: %#v", failure)
	}
}

func TestDynamicAuthorityDropDoesNotReopenBeforeNewLogin(t *testing.T) {
	resolver := &dynamicResolver{}
	resolver.set(ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}, TopologyReady)
	service := newDynamicService(t, resolver)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("authorize: %#v", failure)
	}
	if err := service.DropDynamicAuthority(context.Background()); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("drop unexpectedly reopened handshake: %#v", failure)
	}
	if _, err := service.topology.LookupInternal(testPeerFull); err != ErrSnapshotStale {
		t.Fatalf("peer topology still available: %v", err)
	}
	if _, ok := service.peerLanes.PeerEpoch(testPeerFull); ok {
		t.Fatal("drop retained peer epoch")
	}
	if err := service.InitializeLocalAuthority(context.Background()); err != nil {
		t.Fatalf("new authenticated login: %v", err)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("new login retained old peer")
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("new login publication: %v", err)
	}
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("fresh handshake after login: %#v", failure)
	}
}

func TestDynamicSuspendedPeerRetriesOnlyAfterConfirmedRebind(t *testing.T) {
	resolved := ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}
	resolver := &dynamicResolver{}
	resolver.set(resolved, TopologyReady)
	resolver.setExact(ResolvedPeer{}, TopologyUnavailable)
	service := newDynamicService(t, resolver)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("authorize: %#v", failure)
	}
	if err := service.DropDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.InitializeLocalAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	service.startSuspendedPeerRetry(context.Background())
	resolver.mu.Lock()
	prePublicationCalls := resolver.exactCalls
	resolver.mu.Unlock()
	if prePublicationCalls != 0 {
		t.Fatalf("suspended peer queried before Manager publication: %d", prePublicationCalls)
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), "wrong@example.test/resource"); err != ErrIdentityBinding {
		t.Fatalf("wrong rebind accepted: %v", err)
	}
	resolver.mu.Lock()
	before := resolver.exactCalls
	resolver.mu.Unlock()
	if before != 0 {
		t.Fatalf("queried exact peer without bound-identity proof: %d", before)
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("confirmed rebind: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		resolver.mu.Lock()
		calls := resolver.exactCalls
		resolver.mu.Unlock()
		if calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox poll did not retry suspended exact peer")
		}
		time.Sleep(time.Millisecond)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("temporarily unavailable peer was re-admitted")
	}
	resolver.setExact(resolved, TopologyReady)
	service.wakeDrain()
	for service.Diagnostics().PeerCount == 0 {
		if time.Now().After(deadline) {
			t.Fatal("available exact peer was not reauthorized")
		}
		time.Sleep(time.Millisecond)
	}
	if len(service.suspendedPeers) != 0 {
		t.Fatal("reauthorized peer remained parked")
	}
}

func TestDynamicDisconnectRetainsQueuedRequestUntilExpiry(t *testing.T) {
	resolved := ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}
	resolver := &dynamicResolver{}
	resolver.set(resolved, TopologyReady)
	resolver.setExact(ResolvedPeer{}, TopologyUnavailable)
	service := newDynamicService(t, resolver)
	if failure := service.AuthorizePeer(context.Background(), testPeerBare); failure != nil {
		t.Fatalf("authorize: %#v", failure)
	}
	now := service.config.Clock.(*testCalibratedClock).Snapshot().UTC
	base := signedInbound(t, now, protocol.ModeRequest, "", "", nativePayload("/epoch", "queued request"))
	request, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: base.ConversationID, Sender: testLocalFull, Recipient: testPeerFull, MeshID: testMesh,
		Mode: protocol.ModeRequest, CreatedAt: now, ExpiresAt: now.Add(time.Second), ClockUncertainty: time.Millisecond, Payload: base.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.outboundRPC.Register(request, "pending-command", request.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := service.outbox.Enqueue(request); err != nil {
		t.Fatal(err)
	}
	if err := service.DropDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.InitializeLocalAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.outbox.Get(request.MessageID); err != nil || !service.outboundRPC.ActiveConversation(request.ConversationID) {
		t.Fatalf("transient disconnect lost queued request or waiter: outbox=%v", err)
	}
	service.config.Clock.(*testCalibratedClock).Set(now.Add(2 * time.Second))
	service.wakeDrain()
	deadline := time.Now().Add(time.Second)
	for {
		_, err := service.outbox.Get(request.MessageID)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired request remained parked behind unavailable peer")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := service.outboundRPC.Wait(context.Background(), request.CorrelationID); err == nil {
		t.Fatal("expired RPC waiter completed successfully")
	}
	if err := service.ReauthorizeSuspendedPeers(context.Background(), testLocalFull); err != nil {
		t.Fatalf("publication after expiry: %v", err)
	}
}

func TestDynamicAuthenticationRejectionWakesPendingRPC(t *testing.T) {
	service := newDynamicService(t, &dynamicResolver{})
	now := service.config.Clock.(*testCalibratedClock).Snapshot().UTC
	base := signedInbound(t, now, protocol.ModeRequest, "", "", nativePayload("/epoch", "pending request"))
	request, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: base.ConversationID, Sender: testLocalFull, Recipient: testPeerFull, MeshID: testMesh,
		Mode: protocol.ModeRequest, CreatedAt: now, ExpiresAt: now.Add(time.Minute), ClockUncertainty: time.Millisecond, Payload: base.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.outboundRPC.Register(request, "pending-command", request.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, waitErr := service.outboundRPC.Wait(context.Background(), request.CorrelationID)
		result <- waitErr
	}()
	service.FailPendingRequestsOnAuthenticationRejection()
	select {
	case err := <-result:
		if !errors.Is(err, rpc.ErrCancelled) {
			t.Fatalf("wait failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending RPC still waiting after terminal rejection")
	}
}

func TestDynamicOutboundHandshakesOnceAndUnavailableTargetIsNotAccepted(t *testing.T) {
	resolver := &dynamicResolver{disposition: TopologyUnavailable}
	service := newDynamicService(t, resolver)
	if result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "first")}); failure == nil || failure.Code != FailureUnavailable || result.Accepted {
		t.Fatalf("unavailable send accepted: result=%#v failure=%#v", result, failure)
	}
	resolver.set(ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "session-1"}, TopologyReady)
	for range 2 {
		if result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "message")}); failure != nil || !result.Accepted {
			t.Fatalf("authorized send result=%#v failure=%#v", result, failure)
		}
	}
	resolver.mu.Lock()
	calls := resolver.calls
	resolver.mu.Unlock()
	if calls != 2 { // one unavailable lookup, then one cached handshake
		t.Fatalf("resolver calls=%d; repeated per-message handshake", calls)
	}
}

func TestDynamicFirstInboundAuthorizesExactAuthenticatedSender(t *testing.T) {
	resolver := &dynamicResolver{
		result: ResolvedPeer{Bare: testPeerBare, Full: testPeerBare + "/different-installation", InstallationID: "other", SessionGeneration: "2"}, disposition: TopologyReady,
		exactResult: ResolvedPeer{Bare: testPeerBare, Full: testPeerFull, InstallationID: "install-1", SessionGeneration: "1"}, exactDisposition: TopologyReady,
	}
	service := newDynamicService(t, resolver)
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "first inbound"))
	if accepted, failure := service.ReceiveRank2(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil || !accepted {
		t.Fatalf("first inbound accepted=%t failure=%#v", accepted, failure)
	}
	deliveries := service.deliveries.(*testDeliverySink).deliveries()
	if len(deliveries) != 1 {
		t.Fatalf("first inbound deliveries=%d", len(deliveries))
	}
	resolver.mu.Lock()
	bareCalls, exactCalls := resolver.calls, resolver.exactCalls
	resolver.mu.Unlock()
	if bareCalls != 0 || exactCalls != 1 {
		t.Fatalf("inbound used bare selector: bare=%d exact=%d", bareCalls, exactCalls)
	}
	if _, err := service.topology.LookupInternal(testPeerBare + "/different-installation"); err == nil {
		t.Fatal("bare-selected installation was incorrectly authorized")
	}
}

func TestDynamicInboundRejectsNonmatchingExactServerResult(t *testing.T) {
	resolver := &dynamicResolver{exactResult: ResolvedPeer{Bare: testPeerBare, Full: testPeerBare + "/other", InstallationID: "other", SessionGeneration: "2"}, exactDisposition: TopologyReady}
	service := newDynamicService(t, resolver)
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "spoof"))
	if accepted, failure := service.ReceiveRank2(context.Background(), testInboundProvenance(t, envelope), envelope); accepted || failure == nil || failure.Code != FailureRejected {
		t.Fatalf("mismatched exact result accepted=%t failure=%#v", accepted, failure)
	}
	if service.Diagnostics().PeerCount != 0 {
		t.Fatal("mismatched result installed peer")
	}
}
