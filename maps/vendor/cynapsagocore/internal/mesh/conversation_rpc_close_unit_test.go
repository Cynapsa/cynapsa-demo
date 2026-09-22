package mesh

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

const (
	conversationPeerBBare = "peer-b@example"
	conversationPeerBFull = "peer-b@example/mesh"
)

func newConversationRPCService(t *testing.T, carrier *testCarrier, sink *testDeliverySink) (*MessagingService, time.Time) {
	return newConversationRPCServiceWithDrain(t, carrier, sink, true)
}

func newConversationRPCServiceWithDrain(t *testing.T, carrier *testCarrier, sink *testDeliverySink, automaticDrain bool) (*MessagingService, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{
		{Action: "allow", Path: "/idle", AgentID: testPeerBare},
		{Action: "allow", Path: "/rpc", AgentID: testPeerBare},
		{Action: "allow", Path: "/rpc", AgentID: conversationPeerBBare},
	})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(16)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{QueueCapacity: 16, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second), OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second, Clock: &testCalibratedClock{now: now}}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{
			{AgentID: testLocalBare, Internal: testLocalFull},
			{AgentID: conversationPeerBBare, Internal: conversationPeerBFull},
			{AgentID: testPeerBare, Internal: testPeerFull},
		}}},
		Carrier: carrier, Payloads: testPipelineFactory{}, Deliveries: sink, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	service.automaticDrain = automaticDrain
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	return service, now
}

func TestConversationCloseIgnoresOutboundRPCsOwnedByOtherConversations(t *testing.T) {
	carrier := &testCarrier{}
	service, _ := newConversationRPCService(t, carrier, &testDeliverySink{})
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
	if failure != nil {
		t.Fatalf("idle send: %#v", failure)
	}
	notify := make(chan protocol.Envelope, 8)
	carrier.mu.Lock()
	carrier.notify = notify
	carrier.mu.Unlock()

	const pending = 8
	cancels := make([]context.CancelFunc, 0, pending)
	results := make(chan *Failure, pending)
	for index := 0; index < pending; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func(index int) {
			_, failure := service.MessageRequest(ctx, model.MessageRequestArgs{To: conversationPeerBBare, Payload: nativePayload("/rpc", string(rune('a'+index)))})
			results <- failure
		}(index)
	}
	var busyConversation string
	for index := 0; index < pending; index++ {
		request := <-notify
		if busyConversation == "" {
			busyConversation = request.ConversationID
		} else if request.ConversationID != busyConversation {
			t.Fatalf("busy conversations differ: %q %q", busyConversation, request.ConversationID)
		}
	}
	if failure := service.ConversationClose(context.Background(), idle.ConversationID); failure != nil {
		t.Fatalf("unrelated outbound RPCs blocked idle close: %#v", failure)
	}
	if failure := service.ConversationClose(context.Background(), busyConversation); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("active outbound conversation close=%#v", failure)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for index := 0; index < pending; index++ {
		if failure := <-results; failure == nil || failure.Code != FailureCancelled {
			t.Fatalf("cancelled outbound request=%#v", failure)
		}
	}
	if failure := service.ConversationClose(context.Background(), busyConversation); failure != nil {
		t.Fatalf("completed outbound conversation close=%#v", failure)
	}
}

func TestConversationCloseIgnoresInboundRPCsOwnedByOtherConversations(t *testing.T) {
	carrier := &testCarrier{}
	sink := &testDeliverySink{}
	service, now := newConversationRPCService(t, carrier, sink)
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
	if failure != nil {
		t.Fatalf("idle send: %#v", failure)
	}
	request := conversationInboundRequest(t, now, conversationPeerBFull)
	provenance, err := NewGroupRank2Provenance(request.Sender, request.Recipient, request.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Receive(context.Background(), provenance, request); failure != nil {
		t.Fatalf("inbound request: %#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("inbound delivery=%#v", deliveries)
	}
	if failure := service.ConversationClose(context.Background(), idle.ConversationID); failure != nil {
		t.Fatalf("unrelated inbound RPC blocked idle close: %#v", failure)
	}
	if failure := service.ConversationClose(context.Background(), request.ConversationID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("active inbound conversation close=%#v", failure)
	}
	if _, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{RequestHandle: deliveries[0].RequestHandle, Payload: nativePayload("/rpc", "reply")}); failure != nil {
		t.Fatalf("inbound reply: %#v", failure)
	}
	if failure := service.ConversationClose(context.Background(), request.ConversationID); failure != nil {
		t.Fatalf("completed inbound conversation close=%#v", failure)
	}
}

func TestConversationCloseWaitsForTerminalOutboundResultConsumption(t *testing.T) {
	for _, terminal := range []struct {
		name string
		run  func(*rpc.OutboundTable, protocol.Envelope) error
	}{
		{name: "response completion", run: func(table *rpc.OutboundTable, request protocol.Envelope) error {
			return table.Complete(conversationRPCResponse(t, request))
		}},
		{name: "terminal error", run: func(table *rpc.OutboundTable, request protocol.Envelope) error {
			return table.Fail(request.CorrelationID, rpc.ErrCancelled)
		}},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			service, now := newConversationRPCService(t, &testCarrier{}, &testDeliverySink{})
			idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
			if failure != nil {
				t.Fatalf("idle send: %#v", failure)
			}
			request := conversationRPCRequest(t, now, testPeerFull, idle.ConversationID)
			if err := service.outboundRPC.Register(request, request.MessageID, request.ExpiresAt); err != nil {
				t.Fatal(err)
			}
			if err := terminal.run(service.outboundRPC, request); err != nil {
				t.Fatal(err)
			}
			if failure := service.ConversationClose(context.Background(), idle.ConversationID); failure == nil || failure.Code != FailureRejected {
				t.Fatalf("terminal unconsumed close=%#v", failure)
			}
			_, waitErr := service.outboundRPC.Wait(context.Background(), request.CorrelationID)
			if terminal.name == "response completion" && waitErr != nil || terminal.name == "terminal error" && !errors.Is(waitErr, rpc.ErrCancelled) {
				t.Fatalf("terminal wait=%v", waitErr)
			}
			if failure := service.ConversationClose(context.Background(), idle.ConversationID); failure != nil {
				t.Fatalf("consumed terminal close=%#v", failure)
			}
		})
	}
}

func TestConversationCloseAndOutboundRegistrationHaveOneOwner(t *testing.T) {
	carrier := &testCarrier{}
	service, _ := newConversationRPCService(t, carrier, &testDeliverySink{})
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
	if failure != nil {
		t.Fatalf("idle send: %#v", failure)
	}
	notify := make(chan protocol.Envelope, 1)
	carrier.mu.Lock()
	carrier.notify = notify
	carrier.mu.Unlock()
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := make(chan struct{})
	requestDone := make(chan *Failure, 1)
	closeDone := make(chan *Failure, 1)
	go func() {
		<-start
		_, failure := service.MessageRequest(requestContext, model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		requestDone <- failure
	}()
	go func() {
		<-start
		closeDone <- service.ConversationClose(context.Background(), idle.ConversationID)
	}()
	close(start)
	closeFailure := <-closeDone
	if closeFailure == nil {
		if requestFailure := <-requestDone; requestFailure == nil || requestFailure.Code != FailureRejected {
			t.Fatalf("close won but request outcome=%#v", requestFailure)
		}
		select {
		case envelope := <-notify:
			t.Fatalf("closed conversation published request %s", envelope.MessageID)
		default:
		}
		return
	}
	if closeFailure.Code != FailureRejected {
		t.Fatalf("concurrent close=%#v", closeFailure)
	}
	select {
	case request := <-notify:
		if request.ConversationID != idle.ConversationID {
			t.Fatalf("concurrent request conversation=%q", request.ConversationID)
		}
	case <-time.After(time.Second):
		t.Fatal("registration winner did not publish request")
	}
	cancel()
	if requestFailure := <-requestDone; requestFailure == nil || requestFailure.Code != FailureCancelled {
		t.Fatalf("registration winner cancellation=%#v", requestFailure)
	}
	deadline := time.Now().Add(time.Second)
	for {
		failure := service.ConversationClose(context.Background(), idle.ConversationID)
		if failure == nil {
			break
		}
		if failure.Code != FailureRejected || time.Now().After(deadline) {
			t.Fatalf("post-cancel close=%#v", failure)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConversationCloseFinalRecheckSeesOutboundRegistrationAfterEarlyScans(t *testing.T) {
	service, now := newConversationRPCService(t, &testCarrier{}, &testDeliverySink{})
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
	if failure != nil {
		t.Fatalf("idle send: %#v", failure)
	}
	// This is the state observed by ConversationClose before its final lock.
	if service.outboundRPC.ActiveConversation(idle.ConversationID) || service.inboundRPC.ActiveConversation(idle.ConversationID) {
		t.Fatal("unexpected RPC ownership before barrier")
	}
	request := conversationRPCRequest(t, now, testPeerFull, idle.ConversationID)
	service.mu.Lock()
	state := service.conversations[idle.ConversationID]
	state.active++
	service.activeOperations++
	service.mu.Unlock()
	if err := service.outboundRPC.Register(request, request.MessageID, request.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	// The publishing operation drops active only after the table owns the RPC.
	// The final close recheck must see that exact retained ownership.
	service.finishOutbound(state, false)
	if failure := service.closeConversationIfIdle(idle.ConversationID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("close after outbound registration=%#v", failure)
	}
	if err := service.outboundRPC.Fail(request.CorrelationID, rpc.ErrCancelled); err != nil {
		t.Fatal(err)
	}
	if _, err := service.outboundRPC.Wait(context.Background(), request.CorrelationID); !errors.Is(err, rpc.ErrCancelled) {
		t.Fatalf("outbound cleanup=%v", err)
	}
	if failure := service.closeConversationIfIdle(idle.ConversationID); failure != nil {
		t.Fatalf("close after outbound consumption=%#v", failure)
	}
}

func TestConversationCloseFinalRecheckSeesInboundRegistrationAfterEarlyScans(t *testing.T) {
	service, now := newConversationRPCService(t, &testCarrier{}, &testDeliverySink{})
	idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
	if failure != nil {
		t.Fatalf("idle send: %#v", failure)
	}
	// This is the state observed by ConversationClose before its final lock.
	if service.outboundRPC.ActiveConversation(idle.ConversationID) || service.inboundRPC.ActiveConversation(idle.ConversationID) {
		t.Fatal("unexpected RPC ownership before barrier")
	}
	request := conversationInboundRequest(t, now, testPeerFull)
	service.mu.Lock()
	state := service.conversations[idle.ConversationID]
	state.active++
	service.activeOperations++
	service.mu.Unlock()
	handle, err := service.inboundRPC.CreateHandle(request, request.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	// Inbound publication likewise releases active only after handle ownership
	// has transferred to the table.
	service.finishInbound(state)
	if failure := service.closeConversationIfIdle(idle.ConversationID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("close after inbound registration=%#v", failure)
	}
	if err := service.inboundRPC.Cancel(handle); err != nil {
		t.Fatal(err)
	}
	if failure := service.closeConversationIfIdle(idle.ConversationID); failure != nil {
		t.Fatalf("close after inbound cancellation=%#v", failure)
	}
}

func TestConversationCloseFinalRecheckSeesQueuedWorkAfterEarlyStatus(t *testing.T) {
	t.Run("outbox", func(t *testing.T) {
		service, now := newConversationRPCServiceWithDrain(t, &testCarrier{}, &testDeliverySink{}, false)
		idle, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/idle", "idle")})
		if failure != nil {
			t.Fatalf("idle send: %#v", failure)
		}
		status, failure := service.ConversationStatus(context.Background(), idle.ConversationID)
		if failure != nil || status.QueuedMessageCount != 0 {
			t.Fatalf("early status=%#v failure=%#v", status, failure)
		}
		queued := conversationRPCRequest(t, now, testPeerFull, idle.ConversationID)
		if _, err := service.outbox.Enqueue(queued); err != nil {
			t.Fatal(err)
		}
		if failure := service.closeConversationIfIdle(idle.ConversationID); failure == nil || failure.Code != FailureRejected {
			t.Fatalf("close after outbox enqueue=%#v", failure)
		}
		if err := service.outbox.MarkTerminal(queued.MessageID); err != nil {
			t.Fatal(err)
		}
		if failure := service.closeConversationIfIdle(idle.ConversationID); failure != nil {
			t.Fatalf("close after outbox cleanup=%#v", failure)
		}
	})
}

func conversationInboundRequest(t *testing.T, now time.Time, sender string) protocol.Envelope {
	t.Helper()
	conversationID, err := conversation.DeriveID(testMesh, testLocalFull, sender)
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	prepared, disposition := (testPipeline{}).Prepare(nativePayload("/rpc", "request"))
	if disposition != PayloadAccepted {
		t.Fatal("prepare inbound request")
	}
	descriptor, err := protocol.NewInlinePayload(prepared.Profile, prepared.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: conversationID, Sender: sender, Recipient: testLocalFull, MeshID: testMesh, Mode: protocol.ModeRequest, CorrelationID: correlationID, CreatedAt: now, ExpiresAt: now.Add(time.Second), ClockUncertainty: 250 * time.Millisecond, Payload: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func conversationRPCRequest(t *testing.T, now time.Time, recipient, conversationID string) protocol.Envelope {
	t.Helper()
	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	prepared, disposition := (testPipeline{}).Prepare(nativePayload("/rpc", "question"))
	if disposition != PayloadAccepted {
		t.Fatal("prepare rpc request")
	}
	descriptor, err := protocol.NewInlinePayload(prepared.Profile, prepared.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: conversationID, Sender: testLocalFull, Recipient: recipient, MeshID: testMesh, Mode: protocol.ModeRequest, CorrelationID: correlationID, CreatedAt: now, ExpiresAt: now.Add(time.Second), ClockUncertainty: 250 * time.Millisecond, Payload: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func conversationRPCResponse(t *testing.T, request protocol.Envelope) protocol.Envelope {
	t.Helper()
	prepared, disposition := (testPipeline{}).Prepare(nativePayload("/rpc", "answer"))
	if disposition != PayloadAccepted {
		t.Fatal("prepare rpc response")
	}
	descriptor, err := protocol.NewInlinePayload(prepared.Profile, prepared.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: request.ConversationID, Sender: request.Recipient, Recipient: request.Sender, MeshID: request.MeshID, Mode: protocol.ModeResponse, CorrelationID: request.CorrelationID, ReplyTo: request.MessageID, CreatedAt: request.CreatedAt, ClockUncertainty: request.ClockUncertainty, Payload: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}
