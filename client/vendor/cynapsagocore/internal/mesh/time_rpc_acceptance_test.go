package mesh

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type qaCountingClock struct {
	mu      sync.Mutex
	reading CalibratedTime
	reads   int
}

func (clock *qaCountingClock) Snapshot() CalibratedTime {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.reads++
	return clock.reading
}

func (clock *qaCountingClock) reset() {
	clock.mu.Lock()
	clock.reads = 0
	clock.mu.Unlock()
}

func (clock *qaCountingClock) count() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.reads
}

func TestQAProvenanceRejectionPrecedesCalibratedTimeRead(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	clock := &qaCountingClock{reading: CalibratedTime{UTC: now, Uncertainty: 250 * time.Millisecond}}
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/proof", AgentID: testPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(4)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{QueueCapacity: 4, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second), OperationTimeout: time.Second, PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second, Clock: clock}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}}},
		Carrier:  &testCarrier{}, Payloads: testPipelineFactory{}, Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	clock.reset()
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/proof", "body"))
	if failure := service.Receive(context.Background(), AuthenticatedProvenance{}, envelope); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("provenance rejection = %#v", failure)
	}
	if clock.count() != 0 {
		t.Fatalf("provenance rejection read calibrated clock %d times", clock.count())
	}
	envelope.CredentialProof = []byte("reserved-key-12-must-not-enter-messaging")
	provenance := testInboundProvenance(t, envelope)
	if failure := service.Receive(context.Background(), provenance, envelope); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("compatibility proof rejection = %#v", failure)
	}
	if clock.count() != 0 {
		t.Fatalf("compatibility proof rejection read calibrated clock %d times", clock.count())
	}
}

func TestQAExpiredRequestDoesNotAffectIndependentLiveMessage(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{
		{Action: "allow", Path: "/expired", AgentID: testPeerBare},
		{Action: "allow", Path: "/live", AgentID: testPeerBare},
	})
	conversationID, err := conversationIDForTests()
	if err != nil {
		t.Fatal(err)
	}
	live := signedInboundWithIDs(t, now, protocol.ModeMessage, "", "", conversationID, nativePayload("/live", "two"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, live), live); failure != nil {
		t.Fatalf("receive independent live message: %#v", failure)
	}
	if got := len(sink.deliveries()); got != 1 {
		t.Fatalf("independent live message deliveries: %d", got)
	}
	correlation, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	expired := signedInboundWithIDs(t, now, protocol.ModeRequest, correlation, "", conversationID, nativePayload("/expired", "one"))
	expired.ExpiresAt = now.Add(expired.ClockUncertainty + 250*time.Millisecond)
	if failure := service.Receive(context.Background(), testInboundProvenance(t, expired), expired); failure != nil {
		t.Fatalf("receive expired request: %#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].MessageID != live.MessageID {
		t.Fatalf("independent deliveries = %#v", deliveries)
	}
}

func TestQAFutureCreationUsesFixedOneSecondBoundIndependentOfUncertainty(t *testing.T) {
	for _, test := range []struct {
		name       string
		offset     time.Duration
		senderU    time.Duration
		receiverU  time.Duration
		wantReject bool
	}{
		{name: "one millisecond below with minimum bounds", offset: maximumFutureCreationSkew - time.Millisecond, senderU: time.Microsecond, receiverU: time.Microsecond},
		{name: "exact bound with maximum sender", offset: maximumFutureCreationSkew, senderU: protocol.MaxClockUncertainty, receiverU: time.Microsecond},
		{name: "one millisecond beyond with maximum bounds", offset: maximumFutureCreationSkew + time.Millisecond, senderU: protocol.MaxClockUncertainty, receiverU: protocol.MaxClockUncertainty, wantReject: true},
		{name: "arbitrary future", offset: 24 * time.Hour, senderU: time.Microsecond, receiverU: protocol.MaxClockUncertainty, wantReject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sink := &testDeliverySink{}
			service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/future", AgentID: testPeerBare}})
			service.config.Clock.(*testCalibratedClock).SetUncertainty(test.receiverU)
			envelope := signedInbound(t, now.Add(test.offset), protocol.ModeMessage, "", "", nativePayload("/future", "body"))
			envelope.ClockUncertainty = test.senderU
			failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope)
			if test.wantReject {
				if failure == nil || failure.Code != FailureRejected || len(sink.deliveries()) != 0 {
					t.Fatalf("future envelope accepted: failure=%#v deliveries=%d", failure, len(sink.deliveries()))
				}
				return
			}
			if failure != nil || len(sink.deliveries()) != 1 {
				t.Fatalf("boundary envelope rejected: failure=%#v deliveries=%d", failure, len(sink.deliveries()))
			}
		})
	}
}

func TestQAFutureCreationUsesCalibratedServerTimeAndFailsClosedWithoutIt(t *testing.T) {
	serverNow := time.Date(2040, 2, 3, 4, 5, 6, 0, time.UTC)
	sink := &testDeliverySink{}
	service, _ := newTestServiceAt(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/future", AgentID: testPeerBare}}, serverNow)
	envelope := signedInbound(t, serverNow.Add(maximumFutureCreationSkew), protocol.ModeMessage, "", "", nativePayload("/future", "server-time"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil || len(sink.deliveries()) != 1 {
		t.Fatalf("server-calibrated boundary failure=%#v deliveries=%d", failure, len(sink.deliveries()))
	}

	unavailableSink := &testDeliverySink{}
	unavailable, now := newTestService(t, &testCarrier{}, unavailableSink, []model.PolicyRule{{Action: "allow", Path: "/future", AgentID: testPeerBare}})
	unavailable.config.Clock = staticCalibratedClock{}
	envelope = signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/future", "no-clock"))
	if failure := unavailable.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure == nil || failure.Code != FailureUnavailable || len(unavailableSink.deliveries()) != 0 {
		t.Fatalf("uncalibrated receive failure=%#v deliveries=%d", failure, len(unavailableSink.deliveries()))
	}
}

func TestQAFixedFutureBoundDoesNotChangeRequestExpiry(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	reading := CalibratedTime{UTC: now, Uncertainty: 250 * time.Millisecond}
	request := protocol.Envelope{Mode: protocol.ModeRequest, ClockUncertainty: 250 * time.Millisecond}
	request.ExpiresAt = now.Add(request.ClockUncertainty + reading.Uncertainty + time.Millisecond)
	if requestExpiredAt(request, reading) {
		t.Fatal("request expired before original uncertainty boundary")
	}
	request.ExpiresAt = now.Add(request.ClockUncertainty + reading.Uncertainty)
	if !requestExpiredAt(request, reading) {
		t.Fatal("request remained live at original ambiguity boundary")
	}
}

func TestQAFutureResponseIsRejectedWithoutCompletingRequest(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(ctx, model.MessageRequestArgs{To: testPeerBare, TTL: 10 * time.Second, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := <-notify
	response := signedInboundWithIDs(t, now.Add(maximumFutureCreationSkew+time.Millisecond), protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, nativePayload("/rpc", "answer"))
	response.ClockUncertainty = protocol.MaxClockUncertainty
	service.config.Clock.(*testCalibratedClock).SetUncertainty(protocol.MaxClockUncertainty)
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("future response = %#v", failure)
	}
	select {
	case failure := <-done:
		t.Fatalf("future response completed request: %#v", failure)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	if failure := <-done; failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("cancelled request = %#v", failure)
	}
}

func TestQATTLZeroUsesOneTimeoutSnapshotAndExactLateResponseIsConsumed(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	timeouts := &countingRPCTimeout{duration: 15 * time.Millisecond}
	service.config.RPCTimeout = timeouts
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, TTL: 0, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := <-notify
	if got := request.ExpiresAt.Sub(request.CreatedAt); got != 15*time.Millisecond {
		t.Fatalf("wire TTL = %v", got)
	}
	if got := timeouts.count(); got != 1 {
		t.Fatalf("timeout snapshots = %d", got)
	}
	if failure := <-done; failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("sender timeout = %#v", failure)
	}
	late := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, nativePayload("/rpc", "late"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, late), late); failure != nil {
		t.Fatalf("exact late response was not consumed: %#v", failure)
	}
	if got := timeouts.count(); got != 1 {
		t.Fatalf("late response resampled timeout: %d", got)
	}
}

func TestQAHandlerPathIdempotencyIsRaceSafeAndBounded(t *testing.T) {
	registry, err := NewHandlerRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if index%2 == 0 {
					if err := registry.Register("/orders/create"); err != nil {
						t.Errorf("register: %v", err)
					}
				} else if err := registry.Unregister("/orders/create"); err != nil {
					t.Errorf("unregister: %v", err)
				}
			}
		}(index)
	}
	wait.Wait()
	if registry.Len() > 1 {
		t.Fatalf("registry exceeded bound: %d", registry.Len())
	}
	if err := registry.Register("/orders/create"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("/orders/create"); err != nil || registry.Len() != 1 {
		t.Fatalf("duplicate register: %v len=%d", err, registry.Len())
	}
	if err := registry.Register("/other"); !errors.Is(err, ErrHandlerCapacity) {
		t.Fatalf("capacity = %v", err)
	}
	if err := registry.Unregister("/missing"); err != nil {
		t.Fatalf("missing unregister = %v", err)
	}
}
