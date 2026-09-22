package mesh

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type testRank1Receipt struct {
	binding Rank1ReceiptBinding
	done    chan bool
	closed  chan struct{}
	once    sync.Once
}

func (receipt *testRank1Receipt) Binding() Rank1ReceiptBinding { return receipt.binding }
func (receipt *testRank1Receipt) Done() <-chan bool            { return receipt.done }
func (receipt *testRank1Receipt) Close() {
	receipt.once.Do(func() { close(receipt.closed) })
}

type testRank1ReceiptCarrier struct {
	queue          *outbox.Outbox
	acknowledge    bool
	mutateBinding  func(*Rank1ReceiptBinding)
	blockSend      bool
	blockReceipts  bool
	mu             sync.Mutex
	live, fallback []protocol.Envelope
	ordinal        uint64
}

type testInboundRank1Receipt struct {
	binding Rank1ReceiptBinding
	acked   chan string
	closed  chan struct{}
	once    sync.Once
}

type panicInboundRank1Receipt struct{ *testInboundRank1Receipt }

func (*panicInboundRank1Receipt) Acknowledge(context.Context) error { panic("receipt write panic") }

type blockingInboundRank1Receipt struct{ *testInboundRank1Receipt }

func (*blockingInboundRank1Receipt) Acknowledge(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (receipt *testInboundRank1Receipt) Binding() Rank1ReceiptBinding { return receipt.binding }
func (receipt *testInboundRank1Receipt) Acknowledge(context.Context) error {
	receipt.acked <- receipt.binding.MessageID
	return nil
}
func (receipt *testInboundRank1Receipt) Close() {
	receipt.once.Do(func() { close(receipt.closed) })
}

func inboundReceiptFor(envelope protocol.Envelope, acked chan string) *testInboundRank1Receipt {
	return &testInboundRank1Receipt{binding: Rank1ReceiptBinding{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: sha256.Sum256([]byte("exact-inbound-rank1-session")),
	}, acked: acked, closed: make(chan struct{})}
}

func (carrier *testRank1ReceiptCarrier) Send(context.Context, protocol.Envelope) CarrierDisposition {
	return CarrierUnavailable
}

func (carrier *testRank1ReceiptCarrier) SendWithReceipt(ctx context.Context, envelope protocol.Envelope) (PendingRank1Receipt, CarrierDisposition) {
	if carrier.blockSend {
		<-ctx.Done()
		return nil, CarrierUnavailable
	}
	binding := Rank1ReceiptBinding{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: sha256.Sum256([]byte("exact-rank1-session")),
	}
	if carrier.mutateBinding != nil {
		carrier.mutateBinding(&binding)
	}
	carrier.mu.Lock()
	carrier.live = append(carrier.live, envelope.Clone())
	carrier.mu.Unlock()
	done := make(chan bool, 1)
	if !carrier.blockReceipts {
		done <- carrier.acknowledge
		close(done)
	}
	return &testRank1Receipt{binding: binding, done: done, closed: make(chan struct{})}, CarrierRank1PendingACK
}

func (carrier *testRank1ReceiptCarrier) Fallback(_ context.Context, envelope protocol.Envelope) CarrierDisposition {
	carrier.mu.Lock()
	carrier.fallback = append(carrier.fallback, envelope.Clone())
	carrier.ordinal++
	ordinal := carrier.ordinal
	carrier.mu.Unlock()
	if carrier.queue == nil || carrier.queue.ClaimRank2(envelope.MessageID, ordinal) != nil {
		return CarrierUnavailable
	}
	return CarrierAmbiguous
}

func TestRank1ReceiptTerminalizesOnlyAfterExactAuthenticatedReceipt(t *testing.T) {
	source := &mutableGroupSource{}
	now := testMessagingNow()
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	carrier := &testRank1ReceiptCarrier{acknowledge: true}
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	carrier.queue = service.outbox
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "receipt")})
	if failure != nil || !result.Accepted {
		t.Fatalf("send=%#v failure=%#v", result, failure)
	}
	waitRank1Condition(t, func() bool { messages, _ := service.outbox.Usage(); return messages == 0 })
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("ACK did not terminalize exact outbox entry: %d/%d", messages, bytes)
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	if len(carrier.live) != 1 || len(carrier.fallback) != 0 {
		t.Fatalf("live=%d fallback=%d", len(carrier.live), len(carrier.fallback))
	}
}

func TestAcceptedRPCResponseRetiresRequestWhenRank1ReceiptIsLost(t *testing.T) {
	source := &mutableGroupSource{}
	now := testMessagingNow()
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	carrier := &testRank1ReceiptCarrier{blockReceipts: true}
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	carrier.queue = service.outbox

	type outcome struct {
		result  model.ResponseResult
		failure *Failure
	}
	done := make(chan outcome, 1)
	go func() {
		result, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{
			To: testPeerBare, Payload: nativePayload("/epoch", "request"),
		})
		done <- outcome{result: result, failure: failure}
	}()
	waitRank1Condition(t, func() bool {
		carrier.mu.Lock()
		defer carrier.mu.Unlock()
		return len(carrier.live) == 1
	})
	carrier.mu.Lock()
	request := carrier.live[0].Clone()
	carrier.mu.Unlock()
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, nativePayload("/epoch", "response"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("receive response=%#v", failure)
	}
	completed := <-done
	if completed.failure != nil || string(completed.result.Payload.Value.(model.NativePayload).Body) != "response" {
		t.Fatalf("request completion=%#v failure=%#v", completed.result, completed.failure)
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("accepted response retained lost-receipt request: %d/%d", messages, bytes)
	}
}

func TestRank1DisconnectAutomaticallyTransfersSameEnvelopeToRank2(t *testing.T) {
	source := &mutableGroupSource{}
	now := testMessagingNow()
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	carrier := &testRank1ReceiptCarrier{}
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	carrier.queue = service.outbox
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "fallback")})
	if failure != nil || !result.Accepted {
		t.Fatalf("send=%#v failure=%#v", result, failure)
	}
	waitRank1Condition(t, func() bool { return service.outbox.OwnsRank2(result.MessageID) })
	carrier.mu.Lock()
	if len(carrier.live) != 1 || len(carrier.fallback) != 1 {
		carrier.mu.Unlock()
		t.Fatalf("live/fallback=%d/%d", len(carrier.live), len(carrier.fallback))
	}
	live, fallback := carrier.live[0], carrier.fallback[0]
	carrier.mu.Unlock()
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	liveWire, liveErr := codec.Encode(live)
	fallbackWire, fallbackErr := codec.Encode(fallback)
	if liveErr != nil || fallbackErr != nil || !bytes.Equal(liveWire, fallbackWire) {
		t.Fatalf("fallback changed identity: live=%#v fallback=%#v", live, fallback)
	}
	if !service.outbox.OwnsRank2(result.MessageID) {
		t.Fatal("fallback did not become Rank2-owned")
	}
	if failure = service.DeliveryRetry(context.Background(), result.MessageID); failure != nil {
		t.Fatalf("Rank2-owned retry hint=%#v", failure)
	}
	carrier.mu.Lock()
	liveCount := len(carrier.live)
	carrier.mu.Unlock()
	if liveCount != 1 || !service.outbox.OwnsRank2(result.MessageID) {
		t.Fatal("Rank2-owned retry migrated back to Rank1")
	}
}

func TestForgedRank1ReceiptDoesNotTerminalizeOrFallback(t *testing.T) {
	source := &mutableGroupSource{}
	now := testMessagingNow()
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	carrier := &testRank1ReceiptCarrier{acknowledge: true, mutateBinding: func(binding *Rank1ReceiptBinding) { binding.MessageID = "msg_AAAAAAAAAAAAAAAAAAAAAA" }}
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	carrier.queue = service.outbox
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "forged")})
	if failure != nil || !result.Accepted {
		t.Fatalf("send=%#v failure=%#v", result, failure)
	}
	if _, err := service.outbox.Get(result.MessageID); err != nil {
		t.Fatalf("forged receipt retired queue entry: %v", err)
	}
	if service.outbox.OwnsRank2(result.MessageID) {
		t.Fatal("invalid receipt caused fallback ownership")
	}
}

func TestRank1DuplicateQueuesBehindOriginalPeerLaneAdmission(t *testing.T) {
	now := testMessagingNow()
	source := &mutableGroupSource{}
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	entered, release := make(chan struct{}), make(chan struct{})
	service, _ := newCurrentMembershipServiceWithFactory(t, source, &testCarrier{}, &testDeliverySink{}, decodeBarrierFactory{entered: entered, release: release})
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/epoch", "dedupe"))
	first := make(chan *Failure, 1)
	go func() { first <- service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope) }()
	<-entered
	type duplicateResult struct {
		eligible bool
		failure  *Failure
	}
	duplicate := make(chan duplicateResult, 1)
	go func() {
		eligible, failure := service.ReceiveRank1(context.Background(), testInboundProvenance(t, envelope), envelope)
		duplicate <- duplicateResult{eligible: eligible, failure: failure}
	}()
	select {
	case result := <-duplicate:
		t.Fatalf("duplicate bypassed peer lane: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if failure := <-first; failure != nil {
		t.Fatalf("original=%#v", failure)
	}
	select {
	case result := <-duplicate:
		if result.failure != nil || !result.eligible {
			t.Fatalf("terminal duplicate=%#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("queued duplicate did not complete")
	}
}

func TestRank1ReceiptsFollowEachIndependentTerminalAdmission(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	second := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "two"))
	first := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "one"))
	acked := make(chan string, 2)
	secondReceipt := inboundReceiptFor(second, acked)
	if failure := service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, second), second, secondReceipt); failure != nil {
		t.Fatalf("receive second=%#v", failure)
	}
	select {
	case messageID := <-acked:
		if messageID != second.MessageID {
			t.Fatalf("wrong second receipt: %s", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("independent second admission was not acknowledged")
	}
	firstReceipt := inboundReceiptFor(first, acked)
	if failure := service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, first), first, firstReceipt); failure != nil {
		t.Fatalf("receive first=%#v", failure)
	}
	select {
	case messageID := <-acked:
		if messageID != first.MessageID {
			t.Fatalf("wrong first receipt: %s", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("first admission was not acknowledged")
	}
	if len(sink.deliveries()) != 2 {
		t.Fatalf("deliveries=%d", len(sink.deliveries()))
	}
}

func TestInboundRank1ReceiptSlotsReleaseOnPanicAndShutdown(t *testing.T) {
	service, now := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	first := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "one"))
	acked := make(chan string, 1)
	panicReceipt := &panicInboundRank1Receipt{testInboundRank1Receipt: inboundReceiptFor(first, acked)}
	if failure := service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, first), first, panicReceipt); failure != nil {
		t.Fatalf("receive first=%#v", failure)
	}
	deadline := time.Now().Add(time.Second)
	for len(service.receiptSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if slots := len(service.receiptSlots); slots != 0 {
		t.Fatalf("receipt panic retained %d admission slots", slots)
	}
	second := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "three"))
	pending := &blockingInboundRank1Receipt{testInboundRank1Receipt: inboundReceiptFor(second, acked)}
	if failure := service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, second), second, pending); failure != nil {
		t.Fatalf("receive pending=%#v", failure)
	}
	if slots := len(service.receiptSlots); slots != 1 {
		t.Fatalf("pending receipt slots=%d, want 1", slots)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("shutdown=%#v", failure)
	}
	if slots := len(service.receiptSlots); slots != 0 {
		t.Fatalf("shutdown retained %d receipt slots", slots)
	}
	select {
	case <-pending.closed:
	default:
		t.Fatal("shutdown did not close pending exact-session route")
	}
}

func TestReceiptRouteCapacityDropsOnlyACKAndRank2FallbackDedupes(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	for len(service.receiptSlots) < cap(service.receiptSlots) {
		service.receiptSlots <- struct{}{}
	}
	defer func() {
		for len(service.receiptSlots) != 0 {
			<-service.receiptSlots
		}
	}()
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "one"))
	acked := make(chan string, 1)
	receipt := inboundReceiptFor(envelope, acked)
	if failure := service.ReceiveRank1WithReceipt(context.Background(), testInboundProvenance(t, envelope), envelope, receipt); failure != nil {
		t.Fatalf("Rank1 delivery with unavailable receipt route=%#v", failure)
	}
	select {
	case <-receipt.closed:
	default:
		t.Fatal("capacity-rejected receipt route was not closed")
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
		t.Fatalf("same-envelope Rank2 fallback=%#v", failure)
	}
	if got := len(sink.deliveries()); got != 1 {
		t.Fatalf("deliveries=%d, want exactly one", got)
	}
	select {
	case messageID := <-acked:
		t.Fatalf("capacity-rejected route acknowledged %s", messageID)
	default:
	}
}

func TestMoreThanDrainSlotsRank1PendingACKDoesNotStarveNextConversation(t *testing.T) {
	now := testMessagingNow()
	source := &mutableGroupSource{}
	members := []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}
	for index := range 9 {
		peer := fmt.Sprintf("peer-%d@example.test/%s", index, testMesh)
		members = append(members, Identity{AgentID: fmt.Sprintf("peer-%d@example.test", index), Internal: peer})
	}
	source.replace(now, members...)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController(nil)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(16)
	if err != nil {
		t.Fatal(err)
	}
	carrier := &testRank1ReceiptCarrier{blockReceipts: true}
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: 16, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second),
		OperationTimeout: 2 * time.Second, PeerIdleTimeout: 2 * time.Second, OutboxPollInterval: 2 * time.Second, Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity, Topology: source, Carrier: carrier, Payloads: testPipelineFactory{},
		Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier.queue = service.outbox
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start=%#v", failure)
	}
	defer func() { _ = service.Shutdown(context.Background()) }()
	for index := range 9 {
		peer := fmt.Sprintf("peer-%d@example.test/%s", index, testMesh)
		conversationID, deriveErr := conversation.DeriveID(testMesh, testLocalFull, peer)
		if deriveErr != nil {
			t.Fatal(deriveErr)
		}
		payload, payloadErr := protocol.NewInlinePayload("native", []byte{byte(index)})
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		envelope, envelopeErr := protocol.NewEnvelope(protocol.EnvelopeInput{
			ConversationID: conversationID, Sender: testLocalFull, Recipient: peer, MeshID: testMesh,
			Mode: protocol.ModeMessage, CreatedAt: now,
			ClockUncertainty: time.Millisecond, Payload: payload,
		})
		if envelopeErr != nil {
			t.Fatal(envelopeErr)
		}
		if _, enqueueErr := service.outbox.Enqueue(envelope); enqueueErr != nil {
			t.Fatal(enqueueErr)
		}
	}
	service.WakeDelivery()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		carrier.mu.Lock()
		count := len(carrier.live)
		carrier.mu.Unlock()
		if count == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending ACKs held drain slots: sends=%d", count)
		}
		time.Sleep(time.Millisecond)
	}
	if messages, bytes := service.outbox.Usage(); messages != 9 || bytes <= 0 {
		t.Fatalf("pending ACK accounting=%d/%d", messages, bytes)
	}
}

func TestRank1PendingACKDeterministicTimeoutMakesEntryFallbackOnly(t *testing.T) {
	queue, err := outbox.New(outbox.Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := signedInbound(t, testMessagingNow(), protocol.ModeMessage, "", "", nativePayload("/epoch", "timeout"))
	// Rebind as an outbound envelope; the outbox validates wire shape and the
	// receipt evidence below validates every exact identity field.
	envelope.Sender, envelope.Recipient = envelope.Recipient, envelope.Sender
	reservation, err := queue.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	binding := Rank1ReceiptBinding{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: sha256.Sum256([]byte("timeout-session")),
	}
	evidence := outbox.Rank1ReceiptEvidence{
		MessageID: binding.MessageID, ConversationID: binding.ConversationID,
		Sender: binding.Sender, Recipient: binding.Recipient, MeshID: binding.MeshID,
		ChannelBinding: binding.ChannelBinding,
	}
	pending, err := queue.ClaimRank1PendingACK(reservation, evidence)
	if err != nil {
		t.Fatal(err)
	}
	fallbackDeadline := make(chan struct{})
	service := &MessagingService{
		config: MessagingConfig{OperationTimeout: time.Second},
		ctx:    context.Background(), outbox: queue, drainWake: make(chan struct{}, 1),
	}
	receipt := &testRank1Receipt{binding: binding, done: make(chan bool), closed: make(chan struct{})}
	service.startRank1ReceiptWait(pending, evidence, receipt, fallbackDeadline, func() {})
	close(fallbackDeadline)
	service.workers.Wait()
	fallback, err := queue.ReserveEligible(envelope.MessageID)
	if err != nil || fallback.MessageID == "" || !fallback.FallbackOnly {
		t.Fatalf("fallback=%#v err=%v", fallback, err)
	}
	if !queue.Release(fallback) {
		t.Fatal("fallback reservation release failed")
	}
}

func TestRank1BlockedWriteUsesBoundedWindowThenHandsSameEntryToRank2(t *testing.T) {
	queue, err := outbox.New(outbox.Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := signedInbound(t, testMessagingNow(), protocol.ModeMessage, "", "", nativePayload("/epoch", "blocked-write"))
	envelope.Sender, envelope.Recipient = envelope.Recipient, envelope.Sender
	reservation, err := queue.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	carrier := &testRank1ReceiptCarrier{queue: queue, blockSend: true}
	service := &MessagingService{
		config: MessagingConfig{OperationTimeout: 40 * time.Millisecond},
		ctx:    context.Background(), outbox: queue, carrier: carrier,
	}
	started := time.Now()
	resolution := service.resolveCarrier(context.Background(), reservation)
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("stale Rank1 write delayed fallback for %s", elapsed)
	}
	if resolution.disposition != CarrierAmbiguous || resolution.finalized {
		t.Fatalf("resolution=%#v", resolution)
	}
	carrier.mu.Lock()
	fallback := append([]protocol.Envelope(nil), carrier.fallback...)
	carrier.mu.Unlock()
	if len(fallback) != 1 || fallback[0].MessageID != envelope.MessageID || !queue.OwnsRank2(envelope.MessageID) {
		t.Fatalf("fallback=%#v owns-rank2=%t", fallback, queue.OwnsRank2(envelope.MessageID))
	}
	if !queue.Release(reservation) {
		t.Fatal("reservation release failed")
	}
}

func TestRank1ReceiptWaitTimeoutAlwaysLeavesFallbackWindow(t *testing.T) {
	now := testMessagingNow()
	service := &MessagingService{config: MessagingConfig{
		OperationTimeout: 30 * time.Second, PeerIdleTimeout: 30 * time.Second, OutboxPollInterval: 30 * time.Second,
		Clock: staticCalibratedClock{reading: CalibratedTime{
			UTC: now, Uncertainty: time.Millisecond,
		}},
	}}

	if timeout := service.rank1ReceiptWaitTimeout(protocol.Envelope{Mode: protocol.ModeMessage}); timeout != 7500*time.Millisecond {
		t.Fatalf("message timeout=%s want=7.5s", timeout)
	}
	if timeout := service.rank1ReceiptWaitTimeout(protocol.Envelope{Mode: protocol.ModeRequest, ExpiresAt: now.Add(8 * time.Second)}); timeout != 2*time.Second {
		t.Fatalf("short request timeout=%s want=2s", timeout)
	}
	if timeout := service.rank1ReceiptWaitTimeout(protocol.Envelope{Mode: protocol.ModeRequest, ExpiresAt: now.Add(time.Nanosecond)}); timeout != time.Nanosecond {
		t.Fatalf("minimum request timeout=%s want=1ns", timeout)
	}
}

func testMessagingNow() time.Time {
	return time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
}

func waitRank1Condition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
