package mesh

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

type closingLaneCarrier struct {
	testCarrier
	closed chan string
}

type nonCooperativeCarrier struct {
	mu        sync.Mutex
	sent      []protocol.Envelope
	notify    chan protocol.Envelope
	entered   chan struct{}
	release   chan struct{}
	blockOnce sync.Once
}

type replyOutcome struct {
	result  model.SendResult
	failure *Failure
}

type envelopeRecorder interface {
	envelopes() []protocol.Envelope
}

type replyAuthoritySource struct {
	mutableGroupSource
	available atomic.Bool
}

type replyPrepareBarrierPipeline struct {
	testPipeline
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type carrierBoundaryBlocker struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

type replyPrepareBarrierFactory struct{ pipeline *replyPrepareBarrierPipeline }

func (source *replyAuthoritySource) SnapshotGroup(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
	if !source.available.Load() {
		return AuthoritativeGroupSnapshot{}, TopologyUnavailable
	}
	return source.mutableGroupSource.SnapshotGroup(ctx, meshID)
}

func (pipeline *replyPrepareBarrierPipeline) Prepare(value model.Payload) (PreparedPayload, PayloadDisposition) {
	pipeline.once.Do(func() {
		close(pipeline.entered)
		<-pipeline.release
	})
	return pipeline.testPipeline.Prepare(value)
}

func (factory replyPrepareBarrierFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	factory.pipeline.testPipeline.publisher = publisher
	return factory.pipeline, PayloadAccepted
}

func (carrier *nonCooperativeCarrier) Send(_ context.Context, envelope protocol.Envelope) CarrierDisposition {
	carrier.mu.Lock()
	carrier.sent = append(carrier.sent, envelope.Clone())
	notify := carrier.notify
	entered := carrier.entered
	release := carrier.release
	carrier.mu.Unlock()
	if entered != nil && release != nil {
		carrier.blockOnce.Do(func() {
			close(entered)
			<-release
		})
	}
	if notify != nil {
		notify <- envelope.Clone()
	}
	return CarrierAccepted
}

func (carrier *nonCooperativeCarrier) envelopes() []protocol.Envelope {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	result := make([]protocol.Envelope, len(carrier.sent))
	for index := range carrier.sent {
		result[index] = carrier.sent[index].Clone()
	}
	return result
}

func (carrier *closingLaneCarrier) ClosePeer(_ context.Context, peerID string) error {
	carrier.closed <- peerID
	return nil
}

func (blocker *carrierBoundaryBlocker) hook(protocol.Envelope) {
	blocker.once.Do(func() {
		blocker.calls.Add(1)
		close(blocker.entered)
		<-blocker.release
	})
}

func newLaneIntegrationService(t *testing.T, carrier EnvelopeCarrier) (*MessagingService, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, _ := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	handlers, _ := NewHandlerRegistry(8)
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: time.Second, PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second, Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{
			{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull},
		}}},
		Carrier: carrier, Payloads: testPipelineFactory{}, Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	service.automaticDrain = false
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start=%#v", failure)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	return service, now
}

func TestMembershipLaneRemovalRevokesRPCStateOutboxAndClosesLivePeerOnce(t *testing.T) {
	carrier := &closingLaneCarrier{closed: make(chan string, 2)}
	service, now := newLaneIntegrationService(t, carrier)
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("begin=%#v", failure)
	}
	full := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if failure := service.InstallCurrentMembership(session, full); failure != nil {
		t.Fatalf("install=%#v", failure)
	}
	if failure := service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	var ran, cleared atomic.Int32
	result, failure := service.AdmitOutboundPeerWork(context.Background(), testPeerFull, peer.Work{
		Kind: peer.WorkOutbound, OwnedBytes: 4,
		Run: func(_ context.Context, commit peer.Commit) error {
			return commit(func() error {
				ran.Add(1)
				return nil
			})
		}, Clear: func() { cleared.Add(1) },
	})
	if failure != nil {
		t.Fatalf("admit=%#v", failure)
	}
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	conversationID, err := conversation.DeriveID(testMesh, testLocalFull, testPeerFull)
	if err != nil {
		t.Fatal(err)
	}
	outbound := conversationRPCRequest(t, now, testPeerFull, conversationID)
	if err := service.outboundRPC.Register(outbound, outbound.MessageID, outbound.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := service.outbox.Enqueue(outbound); err != nil {
		t.Fatal(err)
	}
	inbound := conversationInboundRequest(t, now, testPeerFull)
	handle, err := service.inboundRPC.CreateHandle(inbound, inbound.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	canary := []byte("removed peer payload")
	service.mu.Lock()
	service.inboundValues[inbound.MessageID] = retainedPayload{peerID: testPeerFull, value: model.Payload{Value: model.NativePayload{Body: canary}}}
	service.mu.Unlock()

	removedSession, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("remove begin=%#v", failure)
	}
	if failure := service.InstallCurrentMembership(removedSession, []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}); failure != nil {
		t.Fatalf("remove=%#v", failure)
	}
	if failure := service.PublishCurrentMembership(removedSession); failure != nil {
		t.Fatalf("remove publish=%#v", failure)
	}
	if _, err := service.outboundRPC.Wait(context.Background(), outbound.CorrelationID); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("outbound=%v", err)
	}
	if _, _, err := service.inboundRPC.BeginReply(handle); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("inbound=%v", err)
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("outbox=(%d,%d)", messages, bytes)
	}
	for index, value := range canary {
		if value != 0 {
			t.Fatalf("payload byte %d not scrubbed", index)
		}
	}
	select {
	case peerID := <-carrier.closed:
		if peerID != testPeerFull {
			t.Fatalf("closed=%q", peerID)
		}
	case <-time.After(time.Second):
		t.Fatal("live peer was not closed")
	}
	duplicateSession, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("duplicate begin=%#v", failure)
	}
	if failure := service.InstallCurrentMembership(duplicateSession, []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}); failure != nil {
		t.Fatalf("duplicate snapshot=%#v", failure)
	}
	if failure := service.PublishCurrentMembership(duplicateSession); failure != nil {
		t.Fatalf("duplicate publish=%#v", failure)
	}
	select {
	case peerID := <-carrier.closed:
		t.Fatalf("peer closed twice: %q", peerID)
	default:
	}
	if ran.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
}

func TestMembershipSnapshotExcludesOnlyExactLocalEndpointAndKeepsSiblingReplica(t *testing.T) {
	service, _ := newLaneIntegrationService(t, &testCarrier{})
	local := testLocalBare + "/r2.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAA"
	sibling := testLocalBare + "/r2.01234567-89ab-4def-8123-456789abcdee.BBBBBBBBBBBBBBBB"
	service.identity.boundFull = local
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("begin=%#v", failure)
	}
	if failure = service.InstallCurrentMembership(session, []Identity{
		{AgentID: testLocalBare, Internal: local},
		{AgentID: testLocalBare, Internal: sibling},
	}); failure != nil {
		t.Fatalf("install sibling replica=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish sibling replica=%#v", failure)
	}
	if failure = service.AuthorizeCurrentPeer(sibling); failure != nil {
		t.Fatalf("sibling replica authorization=%#v", failure)
	}
	if failure = service.AuthorizeCurrentPeer(local); failure == nil || failure.Code != FailureAuthorization {
		t.Fatalf("exact local endpoint authorization=%#v", failure)
	}
}

func TestPausedMembershipDoesNotExtendCallerConfiguredRPCTimeout(t *testing.T) {
	service, _ := newLaneIntegrationService(t, &testCarrier{})
	if _, failure := service.BeginMembershipSynchronization(); failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	started := time.Now()
	_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{
		To: testPeerBare, TTL: 20 * time.Millisecond, Payload: nativePayload("/rpc", "question"),
	})
	if failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("paused request=%#v", failure)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("paused request exceeded bounded timeout: %v", elapsed)
	}
}

func TestAuthenticatedReplyWaitsAcrossAuthorityPauseAndPublishesOnceForRetainedPeer(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &replyAuthoritySource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	source.available.Store(true)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 2)}
	sink := &testDeliverySink{}
	service, _ := newCurrentMembershipService(t, source, carrier, sink)

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	request := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "automatic-native-request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, request), request); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("automatic native dispatch=%#v", deliveries)
	}

	source.available.Store(false)
	service.BlockAuthorityAdmission()
	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: deliveries[0].RequestHandle,
			Payload:       nativePayload("/epoch", "automatic-native-response"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	assertReplyPendingWithoutPublication(t, done, carrier)

	source.available.Store(true)
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("fresh retained membership=%v", err)
	}
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure != nil || !outcome.result.Accepted {
		t.Fatalf("retained reply=%#v %#v", outcome.result, outcome.failure)
	}
	published := awaitCarrierEnvelope(t, carrier.notify)
	if published.Mode != protocol.ModeResponse || published.Recipient != request.Sender ||
		published.ConversationID != request.ConversationID || published.CorrelationID != request.CorrelationID ||
		published.ReplyTo != request.MessageID || published.MessageID != outcome.result.MessageID {
		t.Fatalf("retained reply envelope=%#v", published)
	}
	select {
	case duplicate := <-carrier.notify:
		t.Fatalf("reply published twice: %s", duplicate.MessageID)
	case <-time.After(20 * time.Millisecond):
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("retained reply outbox=(%d,%d)", messages, bytes)
	}
	if _, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
		RequestHandle: deliveries[0].RequestHandle,
		Payload:       nativePayload("/epoch", "forged-reuse"),
	}); failure == nil || failure.Code != FailureInvalidHandle {
		t.Fatalf("consumed reply handle=%#v", failure)
	}
}

func TestAuthenticatedReplyAuthorityPauseRejectsAndScrubsRemovedPeer(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &replyAuthoritySource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	source.available.Store(true)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	sink := &testDeliverySink{}
	service, _ := newCurrentMembershipService(t, source, carrier, sink)

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	request := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "removed-request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, request), request); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("native dispatch=%#v", deliveries)
	}

	source.available.Store(false)
	service.BlockAuthorityAdmission()
	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: deliveries[0].RequestHandle,
			Payload:       nativePayload("/epoch", "must-be-scrubbed"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	assertReplyPendingWithoutPublication(t, done, carrier)

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	source.available.Store(true)
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("fresh removed membership=%v", err)
	}
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("removed reply=%#v %#v", outcome.result, outcome.failure)
	}
	select {
	case envelope := <-carrier.notify:
		t.Fatalf("removed reply reached carrier: %s", envelope.MessageID)
	case <-time.After(20 * time.Millisecond):
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("removed reply outbox=(%d,%d)", messages, bytes)
	}
	// The table intentionally retains the fixed handle identity until service
	// destruction to prevent process-lifetime handle reuse. Request metadata and
	// the active reply borrow must both be gone.
	if service.inboundRPC.Len() != 0 || service.inboundRPC.OwnedBytes() != uint64(len(deliveries[0].RequestHandle)) {
		t.Fatalf("removed reply retained inbound state=(%d,%d)", service.inboundRPC.Len(), service.inboundRPC.OwnedBytes())
	}
	if count, bytes := service.peerLanes.Usage(); count != 0 || bytes != 0 {
		t.Fatalf("removed reply retained lane ownership=(%d,%d)", count, bytes)
	}
}

func TestAuthenticatedReplyRemovalBeforePeerAdmissionDoesNotLeakOrPoisonReAdd(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	pipeline := &replyPrepareBarrierPipeline{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, carrier, &testDeliverySink{}, replyPrepareBarrierFactory{pipeline: pipeline})

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	inbound := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "request"))
	requestHandle, err := service.inboundRPC.CreateHandle(inbound, inbound.ExpiresAt)
	if err != nil {
		t.Fatalf("create handle=%v", err)
	}
	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: requestHandle,
			Payload:       nativePayload("/epoch", "denied-response"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	select {
	case <-pipeline.entered:
	case <-time.After(time.Second):
		t.Fatal("reply did not pass BeginReply and enter preparation")
	}
	service.mu.Lock()
	if state := service.conversations[inbound.ConversationID]; state != nil {
		service.mu.Unlock()
		t.Fatalf("reply created conversation before barrier released: %#v", state)
	}
	service.mu.Unlock()

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	close(pipeline.release)
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("removed pre-admission reply=%#v %#v", outcome.result, outcome.failure)
	}
	if _, _, err := service.inboundRPC.BeginReply(requestHandle); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("removed handle=%v", err)
	}
	if again, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{RequestHandle: requestHandle, Payload: nativePayload("/epoch", "again")}); failure == nil || failure.Code != FailureAuthorization || again.Accepted {
		t.Fatalf("retired handle reuse=%#v %#v", again, failure)
	}
	assertReplyRemovalClean(t, service, carrier, inbound.ConversationID, requestHandle)
	assertPeerReAddStartsClean(t, service, source, carrier, now, inbound.ConversationID)
}

func TestAuthenticatedReplyRemovalReAddBeforeStaleAdmissionRejectsWithoutPoison(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	pipeline := &replyPrepareBarrierPipeline{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, carrier, &testDeliverySink{}, replyPrepareBarrierFactory{pipeline: pipeline})

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	inbound := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "request"))
	requestHandle, err := service.inboundRPC.CreateHandle(inbound, inbound.ExpiresAt)
	if err != nil {
		t.Fatalf("create handle=%v", err)
	}
	initialEpoch, ok := service.capturePeerEpoch(testPeerFull)
	if !ok {
		t.Fatal("initial peer epoch missing")
	}

	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: requestHandle,
			Payload:       nativePayload("/epoch", "stale-response"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	select {
	case <-pipeline.entered:
	case <-time.After(time.Second):
		t.Fatal("reply did not pass BeginReply and enter preparation")
	}
	service.mu.Lock()
	if state := service.conversations[inbound.ConversationID]; state != nil {
		service.mu.Unlock()
		t.Fatalf("reply created conversation before stale barrier released: %#v", state)
	}
	service.mu.Unlock()

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	if _, ok := service.capturePeerEpoch(testPeerFull); ok {
		t.Fatal("removed peer retained current epoch")
	}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("re-add peer before stale release=%v", err)
	}
	readdedEpoch, ok := service.capturePeerEpoch(testPeerFull)
	if !ok || readdedEpoch == initialEpoch {
		t.Fatalf("re-add epoch=(%d,%t) initial=%d", readdedEpoch, ok, initialEpoch)
	}

	close(pipeline.release)
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("ABA stale reply=%#v %#v", outcome.result, outcome.failure)
	}
	if _, _, err := service.inboundRPC.BeginReply(requestHandle); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("revoked handle=%v", err)
	}
	if again, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{RequestHandle: requestHandle, Payload: nativePayload("/epoch", "again")}); failure == nil || failure.Code != FailureAuthorization || again.Accepted {
		t.Fatalf("revoked handle reuse=%#v %#v", again, failure)
	}
	assertReplyRemovalClean(t, service, carrier, inbound.ConversationID, requestHandle)

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "fresh-after-aba")})
	if failure != nil || !result.Accepted || result.ConversationID != inbound.ConversationID {
		t.Fatalf("fresh re-add send=%#v %#v", result, failure)
	}
	status, failure := service.ConversationStatus(context.Background(), result.ConversationID)
	if failure != nil || status.Blocked || status.DeliveryState == "blocked" {
		t.Fatalf("fresh re-add status=%#v %#v", status, failure)
	}
	sent := carrier.envelopes()
	if len(sent) != 1 || sent[0].MessageID != result.MessageID || sent[0].Mode != protocol.ModeMessage {
		t.Fatalf("fresh re-add carrier=%#v", sent)
	}
	currentEpoch, ok := service.capturePeerEpoch(testPeerFull)
	if !ok || currentEpoch != readdedEpoch {
		t.Fatalf("fresh operation changed peer epoch=(%d,%t) want=%d", currentEpoch, ok, readdedEpoch)
	}
}

func TestAuthenticatedReplyRemovalBetweenAdmissionAndPublishDoesNotLeakOrPoisonReAdd(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	sink := &testDeliverySink{}
	pipeline := &midCarrierBarrierPipeline{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, carrier, sink, midCarrierBarrierFactory{pipeline: pipeline})

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	inbound := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, inbound), inbound); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("native dispatch=%#v", deliveries)
	}

	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: deliveries[0].RequestHandle,
			Payload:       nativePayload("/epoch", "denied-response"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	select {
	case <-pipeline.entered:
	case <-time.After(time.Second):
		t.Fatal("reply did not enter lane-owned publication pipeline")
	}
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("pre-removal reply reached carrier: %#v", sent)
	}
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	close(pipeline.release)
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("removed mid-publication reply=%#v %#v", outcome.result, outcome.failure)
	}
	if _, _, err := service.inboundRPC.BeginReply(deliveries[0].RequestHandle); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("removed handle=%v", err)
	}
	assertReplyRemovalClean(t, service, carrier, inbound.ConversationID, deliveries[0].RequestHandle)
	if sends, first, second := pipeline.sends.Load(), pipeline.first.Load(), pipeline.second.Load(); sends != 1 || first != 1 || second != 0 {
		t.Fatalf("removed reply replay/effects=%d/%d/%d want 1/1/0", sends, first, second)
	}
	assertPeerReAddStartsClean(t, service, source, carrier, now, inbound.ConversationID)
}

func TestAuthenticatedReplyRemovalAfterOutboxAdmissionBeforeCarrierDoesNotSendOrPoisonReAdd(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &nonCooperativeCarrier{notify: make(chan protocol.Envelope, 1)}
	sink := &testDeliverySink{}
	service, _ := newCurrentMembershipService(t, source, carrier, sink)
	blocker := &carrierBoundaryBlocker{entered: make(chan struct{}), release: make(chan struct{})}
	service.carrierBoundaryHook = blocker.hook

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	inbound := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, inbound), inbound); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("native dispatch=%#v", deliveries)
	}

	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: deliveries[0].RequestHandle,
			Payload:       nativePayload("/epoch", "blocked-before-carrier"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	select {
	case <-blocker.entered:
	case <-time.After(time.Second):
		t.Fatal("reply did not commit outbox reservation before carrier boundary")
	}
	if messages, bytes := service.outbox.Usage(); messages == 0 || bytes == 0 {
		t.Fatalf("reply did not own committed outbox reservation: outbox=(%d,%d)", messages, bytes)
	}
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("reply reached carrier before removal: %#v", sent)
	}

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	close(blocker.release)
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("removed pre-carrier reply=%#v %#v", outcome.result, outcome.failure)
	}
	if calls := blocker.calls.Load(); calls != 1 {
		t.Fatalf("carrier boundary calls=%d want 1", calls)
	}
	if _, _, err := service.inboundRPC.BeginReply(deliveries[0].RequestHandle); !errors.Is(err, rpc.ErrAuthorizationRejected) {
		t.Fatalf("removed handle=%v", err)
	}
	assertReplyRemovalClean(t, service, carrier, inbound.ConversationID, deliveries[0].RequestHandle)
	assertPeerReAddStartsClean(t, service, source, carrier, now, inbound.ConversationID)
}

func TestAuthenticatedReplyCarrierAdmissionBeforeRemovalSendsOnceAndCleansReAdd(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &nonCooperativeCarrier{entered: make(chan struct{}), release: make(chan struct{})}
	sink := &testDeliverySink{}
	service, _ := newCurrentMembershipService(t, source, carrier, sink)

	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	inbound := signedInbound(t, now, protocol.ModeRequest, correlationID, "", nativePayload("/epoch", "request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, inbound), inbound); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("native dispatch=%#v", deliveries)
	}

	done := make(chan replyOutcome, 1)
	go func() {
		result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{
			RequestHandle: deliveries[0].RequestHandle,
			Payload:       nativePayload("/epoch", "admitted-response"),
		})
		done <- replyOutcome{result: result, failure: failure}
	}()
	select {
	case <-carrier.entered:
	case <-time.After(time.Second):
		t.Fatal("reply carrier effect was not admitted")
	}
	sent := carrier.envelopes()
	if len(sent) != 1 || sent[0].Mode != protocol.ModeResponse || sent[0].ReplyTo != inbound.MessageID {
		t.Fatalf("admitted carrier effect=%#v", sent)
	}

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	close(carrier.release)
	outcome := awaitReplyOutcome(t, done)
	if outcome.failure == nil || outcome.failure.Code != FailureAuthorization || outcome.result.Accepted {
		t.Fatalf("removed post-carrier reply=%#v %#v", outcome.result, outcome.failure)
	}
	if sent = carrier.envelopes(); len(sent) != 1 {
		t.Fatalf("admitted carrier effect replayed: %#v", sent)
	}
	assertReplyRemovalStateClean(t, service, inbound.ConversationID, deliveries[0].RequestHandle)

	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("re-add peer=%v", err)
	}
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "after-readd")})
	if failure != nil || !result.Accepted || result.ConversationID != inbound.ConversationID {
		t.Fatalf("re-add send=%#v %#v", result, failure)
	}
	status, failure := service.ConversationStatus(context.Background(), result.ConversationID)
	if failure != nil || status.Blocked || status.DeliveryState == "blocked" {
		t.Fatalf("re-add status=%#v %#v", status, failure)
	}
	sent = carrier.envelopes()
	if len(sent) != 2 || sent[0].Mode != protocol.ModeResponse || sent[1].MessageID != result.MessageID || sent[1].Mode != protocol.ModeMessage {
		t.Fatalf("re-add carrier=%#v", sent)
	}
}

func TestInboundRemovalWhileDecodeDrainsClosedStateAndAllowsReAdd(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	sink := &testDeliverySink{}
	entered := make(chan struct{})
	release := make(chan struct{})
	service, _ := newCurrentMembershipServiceWithFactory(t, source, carrier, sink, decodeBarrierFactory{entered: entered, release: release})

	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "blocked-inbound"))
	acked := make(chan string, 1)
	receipt := inboundReceiptFor(envelope, acked)
	done := make(chan *Failure, 1)
	go func() {
		done <- service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, envelope), envelope, receipt)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("inbound did not reach blocked decode")
	}
	if got := len(sink.deliveries()); got != 0 {
		t.Fatalf("blocked inbound reached SDK before release: deliveries=%d", got)
	}

	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull})
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("remove peer=%v", err)
	}
	select {
	case <-receipt.closed:
	case <-time.After(time.Second):
		t.Fatal("removed inbound receipt route was not closed")
	}
	select {
	case messageID := <-acked:
		t.Fatalf("removed inbound was acknowledged: %s", messageID)
	default:
	}
	close(release)
	select {
	case failure := <-done:
		if failure == nil || failure.Code != FailureAuthorization {
			t.Fatalf("removed inbound failure=%#v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("removed inbound did not drain")
	}
	assertInboundRemovalClean(t, service, envelope.ConversationID)
	assertPeerReAddStartsClean(t, service, source, carrier, now, envelope.ConversationID)
}

func assertReplyRemovalClean(t *testing.T, service *MessagingService, carrier envelopeRecorder, conversationID, requestHandle string) {
	t.Helper()
	assertReplyRemovalStateClean(t, service, conversationID, requestHandle)
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("removed reply reached carrier: %#v", sent)
	}
}

func assertReplyRemovalStateClean(t *testing.T, service *MessagingService, conversationID, requestHandle string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.mu.Lock()
		state := service.conversations[conversationID]
		activeOperations := service.activeOperations
		publications := len(service.publications)
		service.mu.Unlock()
		laneCount, laneBytes := service.peerLanes.Usage()
		activeLanes := service.peerLanes.ActiveLaneCount()
		queued, queuedBytes := service.outbox.Usage()
		if state == nil && activeOperations == 0 && publications == 0 && activeLanes == 0 && laneCount == 0 && laneBytes == 0 && queued == 0 && queuedBytes == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reply removal retained state conversation=%#v active=%d publications=%d activeLanes=%d lane=(%d,%d) outbox=(%d,%d)",
				state, activeOperations, publications, activeLanes, laneCount, laneBytes, queued, queuedBytes)
		}
		time.Sleep(time.Millisecond)
	}
	if service.inboundRPC.Len() != 0 || service.inboundRPC.OwnedBytes() != uint64(len(requestHandle)) {
		t.Fatalf("removed reply retained inbound state=(%d,%d)", service.inboundRPC.Len(), service.inboundRPC.OwnedBytes())
	}
}

func assertInboundRemovalClean(t *testing.T, service *MessagingService, conversationID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.mu.Lock()
		state := service.conversations[conversationID]
		activeOperations := service.activeOperations
		inboundValues := len(service.inboundValues)
		inboundReceipts := len(service.inboundReceipts)
		publications := len(service.publications)
		receiptSlots := len(service.receiptSlots)
		service.mu.Unlock()
		laneCount, laneBytes := service.peerLanes.Usage()
		activeLanes := service.peerLanes.ActiveLaneCount()
		queued, queuedBytes := service.outbox.Usage()
		if state == nil && activeOperations == 0 && inboundValues == 0 && inboundReceipts == 0 &&
			publications == 0 && receiptSlots == 0 && activeLanes == 0 && laneCount == 0 &&
			laneBytes == 0 && queued == 0 && queuedBytes == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("inbound removal retained state conversation=%#v active=%d inboundValues=%d inboundReceipts=%d receiptSlots=%d publications=%d activeLanes=%d lane=(%d,%d) outbox=(%d,%d)",
				state, activeOperations, inboundValues, inboundReceipts, receiptSlots, publications, activeLanes, laneCount, laneBytes, queued, queuedBytes)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertPeerReAddStartsClean(t *testing.T, service *MessagingService, source *mutableGroupSource, carrier envelopeRecorder, now time.Time, removedConversationID string) {
	t.Helper()
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	if err := service.refreshTopology(context.Background()); err != nil {
		t.Fatalf("re-add peer=%v", err)
	}
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "after-readd")})
	if failure != nil || !result.Accepted || result.ConversationID != removedConversationID {
		t.Fatalf("re-add send=%#v %#v", result, failure)
	}
	status, failure := service.ConversationStatus(context.Background(), result.ConversationID)
	if failure != nil || status.Blocked || status.DeliveryState == "blocked" {
		t.Fatalf("re-add status=%#v %#v", status, failure)
	}
	sent := carrier.envelopes()
	if len(sent) != 1 || sent[0].MessageID != result.MessageID || sent[0].Mode != protocol.ModeMessage {
		t.Fatalf("re-add carrier=%#v", sent)
	}
}

func assertReplyPendingWithoutPublication(t *testing.T, done <-chan replyOutcome, carrier *testCarrier) {
	t.Helper()
	select {
	case outcome := <-done:
		t.Fatalf("paused reply terminalized before fresh authority: %#v %#v", outcome.result, outcome.failure)
	case <-time.After(20 * time.Millisecond):
	}
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("paused reply reached carrier: %#v", sent)
	}
}

func awaitReplyOutcome(t *testing.T, done <-chan replyOutcome) replyOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatal("reply did not reach a bounded terminal outcome")
		return replyOutcome{}
	}
}

func awaitCarrierEnvelope(t *testing.T, sent <-chan protocol.Envelope) protocol.Envelope {
	t.Helper()
	select {
	case envelope := <-sent:
		return envelope
	case <-time.After(2 * time.Second):
		t.Fatal("reply did not reach the carrier")
		return protocol.Envelope{}
	}
}

func TestMembershipPublicationWakesOutboxSkippedBeforeLaneAdmission(t *testing.T) {
	carrier := &testCarrier{notify: make(chan protocol.Envelope, 1)}
	service, now := newTestServiceAtWithDrain(t, carrier, &testDeliverySink{}, nil, time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC), nil, true)
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })

	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("begin=%#v", failure)
	}
	conversationID, err := conversation.DeriveID(testMesh, testLocalFull, testPeerFull)
	if err != nil {
		t.Fatal(err)
	}
	envelope := conversationRPCRequest(t, now, testPeerFull, conversationID)
	if _, err = service.outbox.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	service.wakeDrain()

	deadline := time.Now().Add(time.Second)
	for {
		service.drainMu.Lock()
		_, active := service.inflight[envelope.MessageID]
		service.drainMu.Unlock()
		if !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fenced pre-admission drain attempt did not release")
		}
		time.Sleep(time.Millisecond)
	}
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("fenced authority reached carrier: %#v", sent)
	}

	full := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if failure = service.InstallCurrentMembership(session, full); failure != nil {
		t.Fatalf("install=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	select {
	case delivered := <-carrier.notify:
		if delivered.MessageID != envelope.MessageID {
			t.Fatalf("delivered=%q want=%q", delivered.MessageID, envelope.MessageID)
		}
	case <-time.After(time.Second):
		t.Fatal("membership publication did not wake the skipped outbox entry")
	}
}

func TestPausedRank1InputMovesIntoPeerLaneUntilCurrentMembershipPublishes(t *testing.T) {
	service, now := newLaneIntegrationService(t, &testCarrier{})
	sink := service.deliveries.(*testDeliverySink)
	full := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}

	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("begin=%#v", failure)
	}
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/rpc", "paused-retained"))
	acked := make(chan string, 1)
	receipt := inboundReceiptFor(envelope, acked)
	started := time.Now()
	if failure = service.QuarantineRank1WithReceipt(context.Background(), testInboundProvenance(t, envelope), envelope, receipt); failure != nil {
		t.Fatalf("quarantine=%#v", failure)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("quarantine waited for publication: %v", elapsed)
	}
	if got := len(sink.deliveries()); got != 0 {
		t.Fatalf("paused input reached SDK: deliveries=%d", got)
	}
	select {
	case messageID := <-acked:
		t.Fatalf("paused input acknowledged before publication: %s", messageID)
	case <-receipt.closed:
		t.Fatal("retained paused input receipt was closed")
	default:
	}
	if failure = service.InstallCurrentMembership(session, full); failure != nil {
		t.Fatalf("install=%#v", failure)
	}
	if got := len(sink.deliveries()); got != 0 {
		t.Fatalf("installed-but-unpublished input reached SDK: deliveries=%d", got)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	waitRank1Condition(t, func() bool { return len(sink.deliveries()) == 1 })
	select {
	case messageID := <-acked:
		if messageID != envelope.MessageID {
			t.Fatalf("acknowledged=%q", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("published current member was not acknowledged")
	}
}

func TestPausedRank1InputForAbsentPeerIsClearedWithoutDeliveryOrReceipt(t *testing.T) {
	service, now := newLaneIntegrationService(t, &testCarrier{})
	sink := service.deliveries.(*testDeliverySink)
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("begin=%#v", failure)
	}
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/rpc", "paused-absent"))
	acked := make(chan string, 1)
	receipt := inboundReceiptFor(envelope, acked)
	if failure = service.QuarantineRank1WithReceipt(context.Background(), testInboundProvenance(t, envelope), envelope, receipt); failure != nil {
		t.Fatalf("quarantine=%#v", failure)
	}
	if failure = service.InstallCurrentMembership(session, []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}); failure != nil {
		t.Fatalf("install absent=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish absent=%#v", failure)
	}
	select {
	case <-receipt.closed:
	case <-time.After(time.Second):
		t.Fatal("absent peer lane did not clear its exact receipt route")
	}
	if got := len(sink.deliveries()); got != 0 {
		t.Fatalf("absent peer input reached SDK: deliveries=%d", got)
	}
	select {
	case messageID := <-acked:
		t.Fatalf("absent peer input acknowledged: %s", messageID)
	default:
	}
}

func TestSmallApplicationQueueStillInstallsCompleteMembership(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController(nil)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 1, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: time.Second, PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second, Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{
			{AgentID: testLocalBare, Internal: testLocalFull},
			{AgentID: testPeerBare, Internal: testPeerFull},
			{AgentID: "third@example.test", Internal: "third@example.test/mesh"},
		}}},
		Carrier: &testCarrier{}, Payloads: testPipelineFactory{}, Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start=%#v", failure)
	}
	if got := service.Diagnostics().PeerCount; got != 2 {
		t.Fatalf("peer count=%d want 2", got)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("shutdown=%#v", failure)
	}
}
