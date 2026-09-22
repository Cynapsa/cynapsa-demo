package mesh

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	replicaLocalBare = "agent-a@example.test"
	replicaPeerBare  = "agent-b@example.test"
	replicaLocal     = replicaLocalBare + "/r2.00000000-0000-4000-8000-000000000001.AAAAAAAAAAAAAAAA"
	replicaPeerOne   = replicaPeerBare + "/r2.00000000-0000-4000-8000-000000000002.BBBBBBBBBBBBBBBB"
	replicaPeerTwo   = replicaPeerBare + "/r2.00000000-0000-4000-8000-000000000003.CCCCCCCCCCCCCCCC"
)

func newReplicaRoutingService(t *testing.T, source *mutableGroupSource, carrier EnvelopeCarrier, sink *testDeliverySink) (*MessagingService, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, replicaLocalBare, replicaLocal, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/replicas", AgentID: replicaPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(8)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: time.Second, PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second,
		Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity, Topology: source, Carrier: carrier, Payloads: testPipelineFactory{},
		Deliveries: sink, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start=%#v", failure)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	return service, now
}

type replicaSelectiveCarrier struct {
	mu           sync.Mutex
	dispositions map[string]CarrierDisposition
	sent         []protocol.Envelope
}

func (carrier *replicaSelectiveCarrier) Send(ctx context.Context, envelope protocol.Envelope) CarrierDisposition {
	if ctx.Err() != nil {
		return CarrierUnavailable
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	carrier.sent = append(carrier.sent, envelope.Clone())
	if disposition := carrier.dispositions[envelope.Recipient]; disposition != 0 {
		return disposition
	}
	return CarrierAccepted
}

func (carrier *replicaSelectiveCarrier) envelopes() []protocol.Envelope {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	result := make([]protocol.Envelope, len(carrier.sent))
	for index := range carrier.sent {
		result[index] = carrier.sent[index].Clone()
	}
	return result
}

func TestLogicalAgentEventFansOutToEveryExactEndpoint(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 2)}
	service, _ := newReplicaRoutingService(t, source, carrier, &testDeliverySink{})

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/replicas", "event"),
	})
	if failure != nil || !result.Accepted {
		t.Fatalf("send=%#v failure=%#v", result, failure)
	}
	got := make([]protocol.Envelope, 0, 2)
	for len(got) < 2 {
		select {
		case envelope := <-carrier.notify:
			got = append(got, envelope)
		case <-time.After(time.Second):
			t.Fatalf("fanout envelopes=%#v", got)
		}
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Recipient < got[j].Recipient })
	if got[0].Recipient != replicaPeerOne || got[1].Recipient != replicaPeerTwo {
		t.Fatalf("fanout recipients=%q, %q", got[0].Recipient, got[1].Recipient)
	}
	for _, envelope := range got {
		if envelope.Sender != replicaLocal || envelope.MeshID != testMesh || envelope.Mode != protocol.ModeMessage {
			t.Fatalf("fanout envelope=%#v", envelope)
		}
	}
	if got[0].MessageID == got[1].MessageID || got[0].ConversationID == got[1].ConversationID {
		t.Fatal("exact endpoint deliveries reused endpoint-scoped identifiers")
	}
}

func TestLogicalAgentEventAttemptsHealthyReplicaAfterEarlierFailure(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	carrier := &replicaSelectiveCarrier{dispositions: map[string]CarrierDisposition{
		replicaPeerOne: CarrierRejected,
		replicaPeerTwo: CarrierAccepted,
	}}
	service, _ := newReplicaRoutingService(t, source, carrier, &testDeliverySink{})

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/replicas", "partial-event"),
	})
	if failure == nil || failure.Code != FailureRejected || result.Accepted {
		t.Fatalf("partial fanout result=%#v failure=%#v", result, failure)
	}
	got := carrier.envelopes()
	if len(got) != 2 || got[0].Recipient != replicaPeerOne || got[1].Recipient != replicaPeerTwo {
		t.Fatalf("partial fanout did not attempt every endpoint: %#v", got)
	}
	result, failure = service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/replicas", "later-event"),
	})
	if failure == nil || failure.Code != FailureRejected || result.Accepted {
		t.Fatalf("later degraded fanout result=%#v failure=%#v", result, failure)
	}
	got = carrier.envelopes()
	if len(got) != 3 || got[2].Recipient != replicaPeerTwo {
		t.Fatalf("healthy replica did not remain usable after sibling failure: %#v", got)
	}
}

func TestLogicalAgentReplicaFanoutReturnsAuthorizationForPolicyAndMembership(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	carrier := &replicaSelectiveCarrier{}
	service, _ := newReplicaRoutingService(t, source, carrier, &testDeliverySink{})

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/denied", "policy"),
	})
	if failure == nil || failure.Code != FailureAuthorization || result.Accepted || len(carrier.envelopes()) != 0 {
		t.Fatalf("replica policy denial result=%#v failure=%#v sent=%d", result, failure, len(carrier.envelopes()))
	}

	source.replace(now, Identity{AgentID: replicaLocalBare, Internal: replicaLocal})
	if refreshFailure := service.MeshRefresh(context.Background()); refreshFailure != nil {
		t.Fatalf("refresh=%#v", refreshFailure)
	}
	result, failure = service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/replicas", "removed"),
	})
	if failure == nil || failure.Code != FailureAuthorization || result.Accepted || len(carrier.envelopes()) != 0 {
		t.Fatalf("removed replica result=%#v failure=%#v sent=%d", result, failure, len(carrier.envelopes()))
	}
}

func TestLogicalAgentBatchCapacityRaceLeavesConversationsUsable(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 2)}
	service, _ := newReplicaRoutingService(t, source, carrier, &testDeliverySink{})
	prepared, targets, failure := service.prepareOutboundEndpoints(
		context.Background(), replicaPeerBare,
		nativePayload("/replicas", "capacity-race"),
	)
	if failure != nil {
		t.Fatalf("prepare batch=%#v", failure)
	}
	defer zeroBytes(prepared.Canonical)
	service.mu.Lock()
	service.activeOperations = service.config.QueueCapacity - 1
	service.mu.Unlock()
	requests, allocationFailure := service.allocateMessageSendBatch(targets, prepared)
	if allocationFailure == nil || allocationFailure.Code != FailureCapacity || len(requests) != 0 {
		t.Fatalf("capacity allocation=%#v requests=%d", allocationFailure, len(requests))
	}
	service.mu.Lock()
	service.activeOperations = 0
	for _, target := range targets {
		if target.state.active != 0 || target.state.outboundFail {
			service.mu.Unlock()
			t.Fatalf("failed batch poisoned conversation %#v", target.state)
		}
	}
	service.mu.Unlock()
	result, retryFailure := service.MessageSend(context.Background(), model.MessageSendArgs{
		To: replicaPeerBare, Payload: nativePayload("/replicas", "after-capacity"),
	})
	if retryFailure != nil || !result.Accepted || len(carrier.envelopes()) != 2 {
		t.Fatalf("post-capacity send=%#v failure=%#v envelopes=%d", result, retryFailure, len(carrier.envelopes()))
	}
}

func TestReplicaAndRequestAllocationReportPeerEpochRevocationAsAuthorization(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	service, _ := newReplicaRoutingService(t, source, &testCarrier{}, &testDeliverySink{})

	batchPayload, targets, failure := service.prepareOutboundEndpoints(context.Background(), replicaPeerBare, nativePayload("/replicas", "batch"))
	if failure != nil {
		t.Fatalf("prepare batch=%#v", failure)
	}
	defer zeroBytes(batchPayload.Canonical)
	singlePayload, peerIdentity, state, failure := service.prepareOutbound(context.Background(), replicaPeerBare, nativePayload("/replicas", "request"))
	if failure != nil {
		t.Fatalf("prepare request=%#v", failure)
	}
	defer zeroBytes(singlePayload.Canonical)

	source.replace(now, Identity{AgentID: replicaLocalBare, Internal: replicaLocal})
	if refreshFailure := service.MeshRefresh(context.Background()); refreshFailure != nil {
		t.Fatalf("refresh=%#v", refreshFailure)
	}
	if requests, allocationFailure := service.allocateMessageSendBatch(targets, batchPayload); allocationFailure == nil || allocationFailure.Code != FailureAuthorization || len(requests) != 0 {
		t.Fatalf("revoked replica batch requests=%d failure=%#v", len(requests), allocationFailure)
	}
	if request, allocationFailure := service.allocateSend(state, peerIdentity, protocol.ModeMessage, "", "", time.Time{}, time.Time{}, 0, singlePayload); allocationFailure == nil || allocationFailure.Code != FailureAuthorization || request.MessageID != "" {
		t.Fatalf("revoked request allocation=%#v failure=%#v", request, allocationFailure)
	}
}

func TestLogicalAgentRequestSelectionIsDeterministicAndFailsOverAfterRemoval(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	service, _ := newReplicaRoutingService(t, source, &testCarrier{}, &testDeliverySink{})

	prepared, peer, _, failure := service.prepareOutbound(context.Background(), replicaPeerBare, nativePayload("/replicas", "request"))
	zeroBytes(prepared.Canonical)
	if failure != nil || peer.Internal != replicaPeerOne {
		t.Fatalf("first selection=%#v failure=%#v", peer, failure)
	}
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	if refreshFailure := service.MeshRefresh(context.Background()); refreshFailure != nil {
		t.Fatalf("refresh=%#v", refreshFailure)
	}
	prepared, peer, _, failure = service.prepareOutbound(context.Background(), replicaPeerBare, nativePayload("/replicas", "request"))
	zeroBytes(prepared.Canonical)
	if failure != nil || peer.Internal != replicaPeerTwo {
		t.Fatalf("failover selection=%#v failure=%#v", peer, failure)
	}
	if authorization := service.AuthorizeCurrentPeer(replicaPeerOne); authorization == nil || authorization.Code != FailureAuthorization {
		t.Fatalf("removed endpoint authorization=%#v", authorization)
	}
}

func TestReplyTargetsExactRequestEndpoint(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	service, _ := newReplicaRoutingService(t, source, carrier, &testDeliverySink{})

	conversationID, err := conversation.DeriveID(testMesh, replicaLocal, replicaPeerTwo)
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	prepared, disposition := (testPipeline{}).Prepare(nativePayload("/replicas", "request"))
	if disposition != PayloadAccepted {
		t.Fatal("prepare request")
	}
	descriptor, err := protocol.NewInlinePayload(prepared.Profile, prepared.Canonical)
	zeroBytes(prepared.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: conversationID, Sender: replicaPeerTwo, Recipient: replicaLocal, MeshID: testMesh,
		Mode: protocol.ModeRequest, CorrelationID: correlationID, CreatedAt: now, ExpiresAt: now.Add(time.Second),
		ClockUncertainty: 250 * time.Millisecond, Payload: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := service.inboundRPC.CreateHandle(request, request.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
		RequestHandle: handle, Payload: nativePayload("/replicas", "reply"),
	})
	if failure != nil || !result.Accepted {
		t.Fatalf("reply=%#v failure=%#v", result, failure)
	}
	select {
	case reply := <-carrier.notify:
		if reply.Recipient != replicaPeerTwo || reply.Sender != replicaLocal || reply.ReplyTo != request.MessageID || reply.Mode != protocol.ModeResponse {
			t.Fatalf("reply envelope=%#v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("reply was not sent")
	}
}

func TestTopologyRejectsDifferentMeshSnapshotWithOpaqueReplicaResources(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	topology, err := NewTopology(4, testMesh, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	err = topology.ApplyPeerAuthority(PeerAuthoritySnapshot{
		MeshID: "different-mesh", ObservedAt: now, LocalAuthorized: true,
		Local: Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Peers: []PeerAuthorityResult{{Identity: Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne}, Authorized: true}},
	})
	if err == nil {
		t.Fatal("different mesh authority snapshot was accepted")
	}
}

func TestReplicaDiagnosticsCountExactEndpointsExceptLocalSession(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	localSibling := replicaLocalBare + "/r2.00000000-0000-4000-8000-000000000004.DDDDDDDDDDDDDDDD"
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaLocalBare, Internal: localSibling},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	service, _ := newReplicaRoutingService(t, source, &testCarrier{}, &testDeliverySink{})
	if diagnostics := service.Diagnostics(); diagnostics.PeerCount != 3 {
		t.Fatalf("exact endpoint peer count=%d, want 3", diagnostics.PeerCount)
	}
}

func TestCurrentPeerEndpointsUsesCurrentExactAuthority(t *testing.T) {
	source := &mutableGroupSource{}
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	source.replace(now,
		Identity{AgentID: replicaLocalBare, Internal: replicaLocal},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerOne},
		Identity{AgentID: replicaPeerBare, Internal: replicaPeerTwo},
	)
	service, _ := newReplicaRoutingService(t, source, &testCarrier{}, &testDeliverySink{})
	endpoints, failure := service.CurrentPeerEndpoints(context.Background(), replicaPeerBare)
	if failure != nil || len(endpoints) != 2 || endpoints[0] != replicaPeerOne || endpoints[1] != replicaPeerTwo {
		t.Fatalf("current endpoints=%q failure=%#v", endpoints, failure)
	}
	source.replace(now, Identity{AgentID: replicaLocalBare, Internal: replicaLocal})
	if refreshFailure := service.MeshRefresh(context.Background()); refreshFailure != nil {
		t.Fatalf("refresh=%#v", refreshFailure)
	}
	if endpoints, failure = service.CurrentPeerEndpoints(context.Background(), replicaPeerBare); failure == nil || failure.Code != FailureAuthorization || len(endpoints) != 0 {
		t.Fatalf("removed endpoints=%q failure=%#v", endpoints, failure)
	}
}
