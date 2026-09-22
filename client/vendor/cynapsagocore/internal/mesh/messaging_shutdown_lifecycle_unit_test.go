package mesh

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type nonCooperativeShutdownCarrier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu    sync.Mutex
	calls int
}

type nonCooperativeSendPipeline struct {
	testPipeline
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (pipeline *nonCooperativeSendPipeline) Send(context.Context, PreparedSend) PayloadDisposition {
	pipeline.once.Do(func() { close(pipeline.entered) })
	<-pipeline.release
	return PayloadUnavailable
}

type nonCooperativeSendFactory struct {
	entered chan struct{}
	release chan struct{}
}

func (factory nonCooperativeSendFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	return &nonCooperativeSendPipeline{testPipeline: testPipeline{publisher: publisher}, entered: factory.entered, release: factory.release}, PayloadAccepted
}

type nonCooperativeDeliverySink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (sink *nonCooperativeDeliverySink) Deliver(context.Context, InboundDelivery) DeliveryDisposition {
	sink.once.Do(func() { close(sink.entered) })
	<-sink.release
	return DeliveryAccepted
}

func (carrier *nonCooperativeShutdownCarrier) Send(context.Context, protocol.Envelope) CarrierDisposition {
	carrier.mu.Lock()
	carrier.calls++
	carrier.mu.Unlock()
	carrier.once.Do(func() { close(carrier.entered) })
	<-carrier.release
	return CarrierUnavailable
}

func (carrier *nonCooperativeShutdownCarrier) callCount() int {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.calls
}

func TestMessagingShutdownDeadlineJoinsOneLifecycleCleanup(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	carrier := &nonCooperativeShutdownCarrier{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	released := false
	defer func() {
		if !released {
			close(carrier.release)
		}
	}()

	sendResult := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{
			To: testPeerBare, Payload: nativePayload("/epoch", "shutdown"),
		})
		sendResult <- failure
	}()
	select {
	case <-carrier.entered:
	case <-time.After(time.Second):
		t.Fatal("carrier attempt was not entered")
	}

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	if failure := service.Shutdown(first); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("first Shutdown = %#v", failure)
	}
	second, cancelSecond := context.WithCancel(context.Background())
	cancelSecond()
	if failure := service.Shutdown(second); failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("second Shutdown = %#v", failure)
	}
	if calls := carrier.callCount(); calls != 1 {
		t.Fatalf("carrier calls while cleanup blocked = %d", calls)
	}
	if _, failure := service.MessageSend(context.Background(), model.MessageSendArgs{
		To: testPeerBare, Payload: nativePayload("/epoch", "late"),
	}); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("post-fence MessageSend = %#v", failure)
	}

	close(carrier.release)
	released = true
	select {
	case <-sendResult:
	case <-time.After(time.Second):
		t.Fatal("blocked send did not finish")
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("joined Shutdown = %#v", failure)
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("owned outbox after cleanup: messages=%d bytes=%d", messages, bytes)
	}
}

func TestMessagingShutdownRetainsOutboundStateUntilOperationJoins(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	entered, release := make(chan struct{}), make(chan struct{})
	service, _ := newCurrentMembershipServiceWithFactory(t, source, &testCarrier{}, &testDeliverySink{}, nonCooperativeSendFactory{entered: entered, release: release})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	sent := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "retained")})
		sent <- failure
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pipeline Send was not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if failure := service.Shutdown(ctx); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("Shutdown = %#v", failure)
	}
	service.mu.Lock()
	retained := len(service.publications)
	nonzero := false
	for _, publication := range service.publications {
		for _, value := range publication.request.Payload.Canonical {
			nonzero = nonzero || value != 0
		}
	}
	service.mu.Unlock()
	if retained != 1 || !nonzero {
		t.Fatalf("publication cleared before operation join: retained=%d nonzero=%v", retained, nonzero)
	}
	close(release)
	released = true
	if failure := <-sent; failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("MessageSend after shutdown = %#v", failure)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("joined Shutdown = %#v", failure)
	}
	service.mu.Lock()
	remaining := len(service.publications)
	service.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("publications after cleanup = %d", remaining)
	}
}

func TestMessagingShutdownRetainsInboundStateUntilDeliveryJoins(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now,
		Identity{AgentID: testLocalBare, Internal: testLocalFull},
		Identity{AgentID: testPeerBare, Internal: testPeerFull},
	)
	sink := &nonCooperativeDeliverySink{entered: make(chan struct{}), release: make(chan struct{})}
	service, _ := newCurrentMembershipService(t, source, &testCarrier{}, sink)
	released := false
	defer func() {
		if !released {
			close(sink.release)
		}
	}()
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "inbound"))
	provenance := testInboundProvenance(t, envelope)
	received := make(chan *Failure, 1)
	go func() { received <- service.Receive(context.Background(), provenance, envelope) }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("delivery sink was not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if failure := service.Shutdown(ctx); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("Shutdown = %#v", failure)
	}
	service.mu.Lock()
	state := service.conversations[envelope.ConversationID]
	active := 0
	closed := true
	if state != nil {
		active = state.active
		closed = state.closed
	}
	service.mu.Unlock()
	if state == nil || active != 1 || closed {
		t.Fatalf("inbound state changed before delivery join: state=%p active=%d closed=%v", state, active, closed)
	}
	close(sink.release)
	released = true
	if failure := <-received; failure != nil {
		t.Fatalf("Receive = %#v", failure)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("joined Shutdown = %#v", failure)
	}
	service.mu.Lock()
	remaining := len(service.inboundValues)
	service.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("inbound values after cleanup = %d", remaining)
	}
}
