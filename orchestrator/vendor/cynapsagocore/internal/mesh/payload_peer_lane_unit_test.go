package mesh

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type finalizationBarrierPipeline struct {
	testPipeline
	completed chan struct{}
	release   chan struct{}
	once      sync.Once
	sends     atomic.Int32
}

func (pipeline *finalizationBarrierPipeline) Send(ctx context.Context, request PreparedSend) PayloadDisposition {
	pipeline.sends.Add(1)
	descriptor, err := protocol.NewInlinePayload(request.Payload.Profile, request.Payload.Canonical)
	if err != nil {
		return PayloadRejected
	}
	failure := pipeline.publisher.PublishEnvelope(ctx, EnvelopePublication{
		PeerID: request.PeerID, MessageID: request.MessageID, MeshID: request.MeshID,
		SenderID: request.SenderID, RecipientID: request.RecipientID,
		ConversationID: request.ConversationID, Mode: request.Mode,
		CorrelationID: request.CorrelationID, ReplyTo: request.ReplyTo,
		CreatedAt: request.CreatedAt, ExpiresAt: request.ExpiresAt,
		ClockUncertainty: request.ClockUncertainty, Descriptor: descriptor,
	})
	if failure != nil {
		return PayloadRejected
	}
	pipeline.once.Do(func() { close(pipeline.completed) })
	<-pipeline.release
	return PayloadAccepted
}

type finalizationBarrierFactory struct{ pipeline *finalizationBarrierPipeline }

func (factory finalizationBarrierFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	factory.pipeline.publisher = publisher
	return factory.pipeline, PayloadAccepted
}

type midCarrierBarrierPipeline struct {
	testPipeline
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	sends   atomic.Int32
	first   atomic.Int32
	second  atomic.Int32
}

func (pipeline *midCarrierBarrierPipeline) Send(ctx context.Context, request PreparedSend) PayloadDisposition {
	pipeline.sends.Add(1)
	checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx)
	if !ok {
		return PayloadRejected
	}
	if err := checkpoint(ctx, func() error {
		pipeline.first.Add(1)
		pipeline.once.Do(func() { close(pipeline.entered) })
		<-pipeline.release
		return nil
	}); err != nil {
		return PayloadUnavailable
	}
	if err := checkpoint(ctx, func() error {
		pipeline.second.Add(1)
		return nil
	}); err != nil {
		return PayloadUnavailable
	}
	descriptor, err := protocol.NewInlinePayload(request.Payload.Profile, request.Payload.Canonical)
	if err != nil {
		return PayloadRejected
	}
	if failure := pipeline.publisher.PublishEnvelope(ctx, EnvelopePublication{
		PeerID: request.PeerID, MessageID: request.MessageID, MeshID: request.MeshID,
		SenderID: request.SenderID, RecipientID: request.RecipientID,
		ConversationID: request.ConversationID, Mode: request.Mode,
		CorrelationID: request.CorrelationID, ReplyTo: request.ReplyTo,
		CreatedAt: request.CreatedAt, ExpiresAt: request.ExpiresAt,
		ClockUncertainty: request.ClockUncertainty, Descriptor: descriptor,
	}); failure != nil {
		return PayloadRejected
	}
	return PayloadAccepted
}

type midCarrierBarrierFactory struct{ pipeline *midCarrierBarrierPipeline }

func (factory midCarrierBarrierFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	factory.pipeline.publisher = publisher
	return factory.pipeline, PayloadAccepted
}

type cancelledRequestPipeline struct {
	testPipeline
	entered chan struct{}
	once    sync.Once
}

func (pipeline *cancelledRequestPipeline) Send(ctx context.Context, request PreparedSend) PayloadDisposition {
	if request.Mode != protocol.ModeRequest {
		return pipeline.testPipeline.Send(ctx, request)
	}
	pipeline.once.Do(func() { close(pipeline.entered) })
	<-ctx.Done()
	return PayloadUnavailable
}

type cancelledRequestFactory struct{ pipeline *cancelledRequestPipeline }

func (factory cancelledRequestFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	factory.pipeline.publisher = publisher
	return factory.pipeline, PayloadAccepted
}

func TestOutboundPayloadFinalizationResumesWithoutRepeatingCarrierWork(t *testing.T) {
	service, pipeline := newPayloadFinalizationService(t)
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
		done <- failure
	}()
	<-pipeline.completed
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	close(pipeline.release)
	select {
	case got := <-done:
		t.Fatalf("paused payload terminalized early: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if failure = service.InstallCurrentMembership(session, members); failure != nil {
		t.Fatalf("install=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("retained payload=%#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("retained payload did not finalize")
	}
	if sends := pipeline.sends.Load(); sends != 1 {
		t.Fatalf("pipeline sends=%d want 1", sends)
	}
}

func TestOutboundPayloadFinalizationRemovalRejectsWithoutRepeatingCarrierWork(t *testing.T) {
	service, pipeline := newPayloadFinalizationService(t)
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
		done <- failure
	}()
	<-pipeline.completed
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	close(pipeline.release)
	if failure = service.InstallCurrentMembership(session, []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}); failure != nil {
		t.Fatalf("remove=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	select {
	case got := <-done:
		if got == nil || got.Code != FailureAuthorization {
			t.Fatalf("removed payload=%#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("removed payload did not terminalize")
	}
	if sends := pipeline.sends.Load(); sends != 1 {
		t.Fatalf("pipeline sends=%d want 1", sends)
	}
}

func TestOutboundPayloadMidCarrierPauseRetainsWithoutReplay(t *testing.T) {
	service, pipeline := newPayloadMidCarrierService(t)
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
		done <- failure
	}()
	<-pipeline.entered
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	close(pipeline.release)
	select {
	case got := <-done:
		t.Fatalf("mid-carrier paused payload terminalized early: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if got := pipeline.second.Load(); got != 0 {
		t.Fatalf("later carrier effects during pause=%d want 0", got)
	}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}
	if failure = service.InstallCurrentMembership(session, members); failure != nil {
		t.Fatalf("install=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("retained mid-carrier payload=%#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("retained mid-carrier payload did not finish")
	}
	if sends, first, second := pipeline.sends.Load(), pipeline.first.Load(), pipeline.second.Load(); sends != 1 || first != 1 || second != 1 {
		t.Fatalf("pipeline replay/effects=%d/%d/%d want 1/1/1", sends, first, second)
	}
}

func TestOutboundPayloadMidCarrierRemovalRejectsWithoutReplay(t *testing.T) {
	service, pipeline := newPayloadMidCarrierService(t)
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
		done <- failure
	}()
	<-pipeline.entered
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	close(pipeline.release)
	if failure = service.InstallCurrentMembership(session, []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}); failure != nil {
		t.Fatalf("remove=%#v", failure)
	}
	if failure = service.PublishCurrentMembership(session); failure != nil {
		t.Fatalf("publish=%#v", failure)
	}
	select {
	case got := <-done:
		if got == nil || got.Code != FailureAuthorization {
			t.Fatalf("removed mid-carrier payload=%#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("removed mid-carrier payload did not terminalize")
	}
	if sends, first, second := pipeline.sends.Load(), pipeline.first.Load(), pipeline.second.Load(); sends != 1 || first != 1 || second != 0 {
		t.Fatalf("removed pipeline replay/effects=%d/%d/%d want 1/1/0", sends, first, second)
	}
}

func TestOutboundPayloadMidCarrierPauseUnblocksForShutdownWithoutReplay(t *testing.T) {
	service, pipeline := newPayloadMidCarrierService(t)
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
		done <- failure
	}()
	<-pipeline.entered
	if _, failure := service.BeginMembershipSynchronization(); failure != nil {
		t.Fatalf("pause=%#v", failure)
	}
	close(pipeline.release)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if failure := service.Shutdown(shutdownCtx); failure != nil {
		t.Fatalf("shutdown=%#v", failure)
	}
	select {
	case failure := <-done:
		if failure == nil {
			t.Fatal("paused payload reported success during shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("paused carrier checkpoint did not observe service shutdown")
	}
	if sends, first, second := pipeline.sends.Load(), pipeline.first.Load(), pipeline.second.Load(); sends != 1 || first != 1 || second != 0 {
		t.Fatalf("shutdown pipeline replay/effects=%d/%d/%d want 1/1/0", sends, first, second)
	}
}

func TestCancelledLaneOwnedRequestConsumesOutboundRegistration(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	pipeline := &cancelledRequestPipeline{entered: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, &testCarrier{}, &testDeliverySink{}, cancelledRequestFactory{pipeline: pipeline})
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "idle")})
	if failure != nil {
		t.Fatalf("idle send=%#v", failure)
	}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	done := make(chan *Failure, 1)
	go func() {
		_, requestFailure := service.MessageRequest(requestCtx, model.MessageRequestArgs{To: testPeerBare, TTL: time.Second, Payload: nativePayload("/epoch", "request")})
		done <- requestFailure
	}()
	<-pipeline.entered
	cancelRequest()
	select {
	case requestFailure := <-done:
		if requestFailure == nil || requestFailure.Code != FailureCancelled {
			t.Fatalf("cancelled request=%#v", requestFailure)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled lane-owned request did not return")
	}
	if closeFailure := service.ConversationClose(context.Background(), idle.ConversationID); closeFailure != nil {
		t.Fatalf("terminal request registration still owns conversation: %#v", closeFailure)
	}
}

func newPayloadFinalizationService(t *testing.T) (*MessagingService, *finalizationBarrierPipeline) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	pipeline := &finalizationBarrierPipeline{completed: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, &testCarrier{}, &testDeliverySink{}, finalizationBarrierFactory{pipeline: pipeline})
	return service, pipeline
}

func newPayloadMidCarrierService(t *testing.T) (*MessagingService, *midCarrierBarrierPipeline) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	pipeline := &midCarrierBarrierPipeline{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipServiceWithFactory(t, source, &testCarrier{}, &testDeliverySink{}, midCarrierBarrierFactory{pipeline: pipeline})
	return service, pipeline
}
