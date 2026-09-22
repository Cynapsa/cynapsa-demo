package mesh

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	deliveryoutbox "github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/policy"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

const (
	testMesh      = "mesh"
	testLocalBare = "local@example"
	testLocalFull = "local@example/mesh"
	testPeerBare  = "peer@example"
	testPeerFull  = "peer@example/mesh"
)

type testTopologySource struct{ snapshot AuthoritativeGroupSnapshot }

type topologySourceFunc func(context.Context, string) (AuthoritativeGroupSnapshot, TopologyDisposition)

func (function topologySourceFunc) SnapshotGroup(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
	return function(ctx, meshID)
}

func (source testTopologySource) SnapshotGroup(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
	if ctx.Err() != nil {
		return AuthoritativeGroupSnapshot{}, TopologyUnavailable
	}
	copy := source.snapshot
	if meshID != copy.MeshID {
		return AuthoritativeGroupSnapshot{}, TopologyRejected
	}
	copy.Members = append([]Identity(nil), source.snapshot.Members...)
	return copy, TopologyReady
}

type testCarrier struct {
	mu          sync.Mutex
	disposition CarrierDisposition
	sent        []protocol.Envelope
	notify      chan protocol.Envelope
}

func (carrier *testCarrier) Send(ctx context.Context, envelope protocol.Envelope) CarrierDisposition {
	if ctx.Err() != nil {
		return CarrierUnavailable
	}
	carrier.mu.Lock()
	carrier.sent = append(carrier.sent, envelope.Clone())
	disposition := carrier.disposition
	if disposition == 0 {
		disposition = CarrierAccepted
	}
	notify := carrier.notify
	carrier.mu.Unlock()
	if notify != nil {
		notify <- envelope.Clone()
	}
	return disposition
}

func (carrier *testCarrier) envelopes() []protocol.Envelope {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	result := make([]protocol.Envelope, len(carrier.sent))
	for index := range carrier.sent {
		result[index] = carrier.sent[index].Clone()
	}
	return result
}

type fixedRPCTimeout time.Duration

func (timeout fixedRPCTimeout) SnapshotRPCTimeout() time.Duration { return time.Duration(timeout) }

type testCalibratedClock struct {
	mu          sync.Mutex
	now         time.Time
	uncertainty time.Duration
}

type staticCalibratedClock struct{ reading CalibratedTime }

func (clock staticCalibratedClock) Snapshot() CalibratedTime { return clock.reading }

type countingCalibratedClock struct {
	mu    sync.Mutex
	inner CalibratedUTCClock
	reads int
}

func (clock *countingCalibratedClock) Snapshot() CalibratedTime {
	clock.mu.Lock()
	clock.reads++
	clock.mu.Unlock()
	return clock.inner.Snapshot()
}

func (clock *countingCalibratedClock) count() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.reads
}

func (clock *testCalibratedClock) Snapshot() CalibratedTime {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	uncertainty := clock.uncertainty
	if uncertainty == 0 {
		uncertainty = 250 * time.Millisecond
	}
	return CalibratedTime{UTC: clock.now, Uncertainty: uncertainty}
}

func (clock *testCalibratedClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (clock *testCalibratedClock) SetUncertainty(uncertainty time.Duration) {
	clock.mu.Lock()
	clock.uncertainty = uncertainty
	clock.mu.Unlock()
}

type countingRPCTimeout struct {
	mu       sync.Mutex
	duration time.Duration
	reads    int
}

func (timeout *countingRPCTimeout) SnapshotRPCTimeout() time.Duration {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	timeout.reads++
	return timeout.duration
}

func (timeout *countingRPCTimeout) count() int {
	timeout.mu.Lock()
	defer timeout.mu.Unlock()
	return timeout.reads
}

type testPipelineFactory struct{}
type testPipeline struct{ publisher EnvelopePublisher }

type advancingPipelineFactory struct {
	clock   *testCalibratedClock
	advance time.Time
	calls   *int
}

type advancingPipeline struct {
	testPipeline
	clock   *testCalibratedClock
	advance time.Time
	calls   *int
}

func (factory advancingPipelineFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	if publisher == nil {
		return nil, PayloadRejected
	}
	return &advancingPipeline{testPipeline: testPipeline{publisher: publisher}, clock: factory.clock, advance: factory.advance, calls: factory.calls}, PayloadAccepted
}

func (pipeline *advancingPipeline) Materialize(ctx context.Context, envelope protocol.Envelope) ([]byte, PayloadDisposition) {
	*pipeline.calls++
	pipeline.clock.Set(pipeline.advance)
	return pipeline.testPipeline.Materialize(ctx, envelope)
}

type clockTopologySource struct {
	mu    sync.Mutex
	clock *testCalibratedClock
	calls int
}

func (source *clockTopologySource) SnapshotGroup(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
	if ctx.Err() != nil || meshID != testMesh {
		return AuthoritativeGroupSnapshot{}, TopologyUnavailable
	}
	source.mu.Lock()
	source.calls++
	source.mu.Unlock()
	return AuthoritativeGroupSnapshot{
		MeshID: meshID, ObservedAt: source.clock.Snapshot().UTC,
		Members: []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}},
	}, TopologyReady
}

func (source *clockTopologySource) count() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

func (testPipelineFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	if publisher == nil {
		return nil, PayloadRejected
	}
	return testPipeline{publisher: publisher}, PayloadAccepted
}

func (pipeline testPipeline) Prepare(value model.Payload) (PreparedPayload, PayloadDisposition) {
	encoded, profile, path, ok := encodeTestPayload(value)
	if !ok {
		return PreparedPayload{}, PayloadRejected
	}
	return PreparedPayload{Profile: profile, Canonical: encoded, ApplicationPath: path}, PayloadAccepted
}

func (pipeline testPipeline) Send(ctx context.Context, request PreparedSend) PayloadDisposition {
	descriptor, err := protocol.NewInlinePayload(request.Payload.Profile, request.Payload.Canonical)
	if err != nil {
		return PayloadRejected
	}
	failure := pipeline.publisher.PublishEnvelope(ctx, EnvelopePublication{
		PeerID: request.PeerID, MessageID: request.MessageID,
		MeshID: request.MeshID, SenderID: request.SenderID, RecipientID: request.RecipientID,
		ConversationID: request.ConversationID, Mode: request.Mode,
		CorrelationID: request.CorrelationID, ReplyTo: request.ReplyTo, CreatedAt: request.CreatedAt,
		ExpiresAt: request.ExpiresAt, ClockUncertainty: request.ClockUncertainty, Descriptor: descriptor,
	})
	if failure != nil {
		return PayloadRejected
	}
	return PayloadAccepted
}

func (testPipeline) Materialize(_ context.Context, envelope protocol.Envelope) ([]byte, PayloadDisposition) {
	canonical, err := envelope.Payload.Materialize()
	if err != nil {
		return nil, PayloadTransferFailed
	}
	return canonical, PayloadAccepted
}

func (testPipeline) Decode(canonical []byte) (model.Payload, PayloadDisposition) {
	value, _, _, ok := decodeTestPayload(canonical)
	if !ok {
		return model.Payload{}, PayloadRejected
	}
	return value, PayloadAccepted
}

func (testPipeline) ApplicationPath(_ string, canonical []byte) (string, PayloadDisposition) {
	_, _, path, ok := decodeTestPayload(canonical)
	if !ok {
		return "", PayloadRejected
	}
	return path, PayloadAccepted
}

func encodeTestPayload(value model.Payload) ([]byte, string, string, bool) {
	var kind byte
	var profile, path, metadata string
	var body []byte
	switch payload := value.Value.(type) {
	case model.NativePayload:
		kind, profile, path, metadata, body = 'n', "aztm.native", payload.Path, payload.ContentType, payload.Body
	case model.HTTPRequestPayload:
		kind, profile, path, metadata, body = 'q', "http.request", payload.Path, payload.Method+"\x00"+payload.Query, payload.Body
	case model.HTTPResponsePayload:
		kind, profile, path, metadata, body = 's', "http.response", "/", payload.Reason, payload.Body
	default:
		return nil, "", "", false
	}
	if path == "" || len(path) > 2048 || len(metadata) > 1<<16 {
		return nil, "", "", false
	}
	encoded := make([]byte, 1+2+len(path)+2+len(metadata)+len(body))
	encoded[0] = kind
	binary.BigEndian.PutUint16(encoded[1:3], uint16(len(path)))
	copy(encoded[3:], path)
	offset := 3 + len(path)
	binary.BigEndian.PutUint16(encoded[offset:offset+2], uint16(len(metadata)))
	copy(encoded[offset+2:], metadata)
	copy(encoded[offset+2+len(metadata):], body)
	return encoded, profile, path, true
}

func decodeTestPayload(encoded []byte) (model.Payload, string, string, bool) {
	if len(encoded) < 5 {
		return model.Payload{}, "", "", false
	}
	pathSize := int(binary.BigEndian.Uint16(encoded[1:3]))
	if 3+pathSize+2 > len(encoded) {
		return model.Payload{}, "", "", false
	}
	path := string(encoded[3 : 3+pathSize])
	offset := 3 + pathSize
	metadataSize := int(binary.BigEndian.Uint16(encoded[offset : offset+2]))
	if offset+2+metadataSize > len(encoded) {
		return model.Payload{}, "", "", false
	}
	metadata := string(encoded[offset+2 : offset+2+metadataSize])
	body := append([]byte(nil), encoded[offset+2+metadataSize:]...)
	switch encoded[0] {
	case 'n':
		return model.Payload{Value: model.NativePayload{Path: path, ContentType: metadata, Body: body}}, "aztm.native", path, true
	case 'q':
		return model.Payload{Value: model.HTTPRequestPayload{Path: path, Method: metadata, Body: body}}, "http.request", path, true
	case 's':
		return model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: metadata, Body: body}}, "http.response", path, true
	default:
		return model.Payload{}, "", "", false
	}
}

type testDeliverySink struct {
	mu          sync.Mutex
	disposition DeliveryDisposition
	items       []InboundDelivery
}

func (sink *testDeliverySink) Deliver(ctx context.Context, delivery InboundDelivery) DeliveryDisposition {
	if ctx.Err() != nil {
		return DeliveryUnavailable
	}
	sink.mu.Lock()
	sink.items = append(sink.items, delivery)
	disposition := sink.disposition
	if disposition == 0 {
		disposition = DeliveryAccepted
	}
	sink.mu.Unlock()
	return disposition
}

func (sink *testDeliverySink) deliveries() []InboundDelivery {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]InboundDelivery(nil), sink.items...)
}

func (sink *testDeliverySink) setDisposition(disposition DeliveryDisposition) {
	sink.mu.Lock()
	sink.disposition = disposition
	sink.mu.Unlock()
}

func newTestService(t *testing.T, carrier *testCarrier, sink *testDeliverySink, rules []model.PolicyRule) (*MessagingService, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	return newTestServiceAt(t, carrier, sink, rules, now)
}

func newTestServiceAt(t *testing.T, carrier *testCarrier, sink *testDeliverySink, rules []model.PolicyRule, now time.Time) (*MessagingService, time.Time) {
	return newTestServiceAtWithDrain(t, carrier, sink, rules, now, nil, true)
}

func newTestServiceAtWithOutbox(t *testing.T, carrier *testCarrier, sink *testDeliverySink, rules []model.PolicyRule, now time.Time, shared *deliveryoutbox.Outbox) (*MessagingService, time.Time) {
	return newTestServiceAtWithDrain(t, carrier, sink, rules, now, shared, true)
}

func newTestServiceAtWithDrain(t *testing.T, carrier *testCarrier, sink *testDeliverySink, rules []model.PolicyRule, now time.Time, shared *deliveryoutbox.Outbox, automaticDrain bool) (*MessagingService, time.Time) {
	t.Helper()
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(bare, meshID, full string) error {
		if bare != testLocalBare || meshID != testMesh || full != testLocalFull {
			t.Fatal("identity binding changed")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController(rules)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(16)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMessagingService(MessagingConfig{QueueCapacity: 16, OutboxByteLimit: 1 << 20, RPCTimeout: fixedRPCTimeout(time.Second), OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second, Clock: &testCalibratedClock{now: now}}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}}},
		Carrier:  carrier, Payloads: testPipelineFactory{}, Deliveries: sink, Policies: policies, Handlers: handlers, Outbox: shared,
	})
	if err != nil {
		t.Fatal(err)
	}
	service.automaticDrain = automaticDrain
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	return service, now
}

func nativePayload(path, body string) model.Payload {
	return model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: path, Body: []byte(body)}}
}

func TestMessagingServiceRequiresCalibratedUTCClock(t *testing.T) {
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, _ := NewPolicyController(nil)
	handlers, _ := NewHandlerRegistry(1)
	dependencies := MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: base, Members: []Identity{{AgentID: testLocalBare, Internal: testLocalFull}}}},
		Carrier:  &testCarrier{}, Payloads: testPipelineFactory{}, Deliveries: &testDeliverySink{}, Policies: policies, Handlers: handlers,
	}
	for name, clock := range map[string]CalibratedUTCClock{
		"nil":              nil,
		"zero time":        staticCalibratedClock{reading: CalibratedTime{Uncertainty: time.Millisecond}},
		"non utc":          staticCalibratedClock{reading: CalibratedTime{UTC: time.Date(2026, 8, 13, 12, 0, 0, 0, time.FixedZone("offset-zero", 0)), Uncertainty: time.Millisecond}},
		"zero uncertainty": staticCalibratedClock{reading: CalibratedTime{UTC: base}},
		"over uncertainty": staticCalibratedClock{reading: CalibratedTime{UTC: base, Uncertainty: protocol.MaxClockUncertainty + time.Nanosecond}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewMessagingService(MessagingConfig{QueueCapacity: 1, OutboxByteLimit: 1, RPCTimeout: fixedRPCTimeout(time.Second), OperationTimeout: time.Second, PeerIdleTimeout: time.Second, OutboxPollInterval: time.Second, Clock: clock}, dependencies); err != ErrInvalidConfig {
				t.Fatalf("clock error = %v", err)
			}
		})
	}
}

func TestMessagingServiceFailsClosedWhenClockUncertaintyGrowsPastCap(t *testing.T) {
	service, _ := newTestService(t, &testCarrier{}, &testDeliverySink{}, nil)
	clock := service.config.Clock.(*testCalibratedClock)
	clock.SetUncertainty(protocol.MaxClockUncertainty + time.Nanosecond)
	if _, failure := service.MeshList(context.Background()); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("operation with excessive uncertainty = %#v", failure)
	}
}

func TestDiagnosticsPeerCountUsesCompleteCurrentMembershipExcludingSelfBeforePeerDemand(t *testing.T) {
	carrier := &testCarrier{}
	service, _ := newTestService(t, carrier, &testDeliverySink{}, nil)
	defer service.Shutdown(context.Background())

	snapshot := service.Diagnostics()
	if snapshot.PeerCount != 1 {
		t.Fatalf("self-plus-peer snapshot count = %d, want one remote member", snapshot.PeerCount)
	}
	if sent := carrier.envelopes(); len(sent) != 0 {
		t.Fatalf("diagnostics required peer demand or carrier I/O: %d envelopes", len(sent))
	}
}

func TestMessagingServiceDiagnosticsReflectBoundedAuthoritativeState(t *testing.T) {
	carrier := &testCarrier{disposition: CarrierUnavailable}
	service, now := newTestService(t, carrier, &testDeliverySink{}, []model.PolicyRule{
		{Action: "allow", Path: "/queued", AgentID: testPeerBare},
		{Action: "allow", Path: "/pending", AgentID: testPeerBare},
	})
	defer service.Shutdown(context.Background())
	if snapshot := service.Diagnostics(); snapshot.PeerCount != 1 || snapshot.QueuedMessages != 0 || snapshot.PendingRPCRequests != 0 {
		t.Fatalf("initial diagnostics = %#v", snapshot)
	}
	if err := service.topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: testPeerBare, Internal: testPeerFull}}}); err != nil {
		t.Fatalf("removed-local snapshot: %v", err)
	}
	if snapshot := service.Diagnostics(); snapshot.PeerCount != 1 {
		t.Fatalf("removed local hid remaining peer: %#v", snapshot)
	}
	if err := service.topology.Replace(AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{{AgentID: testLocalBare, Internal: testLocalFull}, {AgentID: testPeerBare, Internal: testPeerFull}}}); err != nil {
		t.Fatalf("restored-local snapshot: %v", err)
	}
	if _, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/queued", "body")}); failure != nil {
		t.Fatalf("queued send = %#v", failure)
	}
	if snapshot := service.Diagnostics(); snapshot.PeerCount != 1 || snapshot.QueuedMessages != 1 || snapshot.PendingRPCRequests != 0 {
		t.Fatalf("queued diagnostics = %#v", snapshot)
	}
	requestContext, cancel := context.WithCancel(context.Background())
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(requestContext, model.MessageRequestArgs{To: testPeerBare, TTL: time.Second, Payload: nativePayload("/pending", "body")})
		done <- failure
	}()
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := service.Diagnostics()
		if snapshot.PendingRPCRequests == 1 && snapshot.QueuedMessages == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending diagnostics never published: %#v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if failure := <-done; failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("cancelled request = %#v", failure)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("shutdown = %#v", failure)
	}
	if snapshot := service.Diagnostics(); snapshot != (ServiceDiagnostics{}) {
		t.Fatalf("closed diagnostics = %#v", snapshot)
	}
}

func TestClosedConversationRemainsPrivateTombstone(t *testing.T) {
	service, _ := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/close", AgentID: testPeerBare}})
	defer service.Shutdown(context.Background())
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/close", "body")})
	if failure != nil {
		t.Fatalf("send = %#v", failure)
	}
	if failure = service.ConversationClose(context.Background(), result.ConversationID); failure != nil {
		t.Fatalf("close = %#v", failure)
	}
	if _, failure = service.ConversationStatus(context.Background(), result.ConversationID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("closed status = %#v", failure)
	}
	listed, failure := service.ConversationList(context.Background())
	if failure != nil || len(listed.Conversations) != 0 {
		t.Fatalf("closed list = %#v, %#v", listed, failure)
	}
	if _, failure = service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/close", "again")}); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("closed conversation reuse = %#v", failure)
	}
}

func TestMeshListUsesInstalledAuthorityAndExplicitRefreshFailsClosed(t *testing.T) {
	service, _ := newTestService(t, &testCarrier{}, &testDeliverySink{}, nil)
	defer service.Shutdown(context.Background())
	service.topologySrc = topologySourceFunc(func(context.Context, string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
		return AuthoritativeGroupSnapshot{}, TopologyUnavailable
	})
	result, failure := service.MeshList(context.Background())
	if failure != nil || len(result.Meshes) != 1 || !result.Meshes[0].Active {
		t.Fatalf("installed mesh.list = %#v %#v", result, failure)
	}
	if failure := service.MeshRefresh(context.Background()); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("failed explicit mesh.refresh = %#v", failure)
	}
	result, failure = service.MeshList(context.Background())
	if failure != nil || len(result.Meshes) != 1 || result.Meshes[0].Active {
		t.Fatalf("fenced mesh.list = %#v %#v", result, failure)
	}
}

func TestMessagingServiceOutboundAdmissionRetryAndPolicy(t *testing.T) {
	carrier := &testCarrier{disposition: CarrierUnavailable}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	service, _ := newTestServiceAtWithDrain(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/allowed", AgentID: testPeerBare}}, now, nil, false)

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/allowed", "hello")})
	if failure != nil || !result.Accepted {
		t.Fatalf("queued send = %#v %#v", result, failure)
	}
	status, failure := service.QueueStatus(context.Background())
	if failure != nil || status.Queued != 1 {
		t.Fatalf("queue status = %#v %#v", status, failure)
	}
	sent := carrier.envelopes()
	if len(sent) != 1 || sent[0].Sender != testLocalFull || sent[0].Recipient != testPeerFull || sent[0].MessageID != result.MessageID {
		t.Fatalf("sent envelope = %#v", sent)
	}
	if !sent[0].ExpiresAt.IsZero() {
		t.Fatalf("fire-and-forget carried expiry %v", sent[0].ExpiresAt)
	}
	carrier.mu.Lock()
	carrier.disposition = CarrierAccepted
	carrier.mu.Unlock()
	if failure := service.DeliveryRetry(context.Background(), result.MessageID); failure != nil {
		t.Fatalf("retry: %#v", failure)
	}
	deadline := time.Now().Add(time.Second)
	for status, _ = service.QueueStatus(context.Background()); status.Queued != 0 && time.Now().Before(deadline); status, _ = service.QueueStatus(context.Background()) {
		time.Sleep(time.Millisecond)
	}
	if status.Queued != 0 {
		t.Fatalf("queued after retry = %d", status.Queued)
	}
	if failure := service.DeliveryRetry(context.Background(), result.MessageID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("missing retry = %#v", failure)
	}
	if failure := service.DeliveryDrop(context.Background(), result.MessageID); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("missing drop = %#v", failure)
	}
	carrier.mu.Lock()
	carrier.disposition = CarrierUnavailable
	carrier.mu.Unlock()
	dropped, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/allowed", "drop")})
	if failure != nil {
		t.Fatalf("queued drop candidate = %#v", failure)
	}
	if failure = service.DeliveryDrop(context.Background(), dropped.MessageID); failure != nil {
		t.Fatalf("drop queued message = %#v", failure)
	}
	status, _ = service.QueueStatus(context.Background())
	if status.Queued != 0 {
		t.Fatalf("queued after drop = %d", status.Queued)
	}
	if _, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/denied", "x")}); failure == nil || failure.Code != FailureAuthorization {
		t.Fatalf("policy denied send = %#v", failure)
	}
}

func TestMessagingServiceSenderOfflineAdmissionUsesSharedOutbox(t *testing.T) {
	carrier := &testCarrier{disposition: CarrierUnavailable}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	shared, err := deliveryoutbox.New(deliveryoutbox.Config{MessageCapacity: 4, ByteCapacity: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	service, _ := newTestServiceAtWithDrain(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/offline", AgentID: testPeerBare}}, now, shared, false)
	defer service.Shutdown(context.Background())

	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/offline", "body")})
	if failure != nil || !result.Accepted || result.MessageID == "" {
		t.Fatalf("offline send = %#v %#v", result, failure)
	}
	if messages, _ := shared.Usage(); messages != 1 {
		t.Fatalf("shared outbox messages = %d", messages)
	}
	status, failure := service.QueueStatus(context.Background())
	if failure != nil || status.Queued != 1 {
		t.Fatalf("queue status = %#v %#v", status, failure)
	}
	pending, err := shared.Get(result.MessageID)
	if err != nil || pending.MessageID != result.MessageID || pending.Recipient != testPeerFull {
		t.Fatalf("shared pending = %#v err=%v", pending, err)
	}
}

func TestMessagingServiceConversationCloseIdempotency(t *testing.T) {
	service, _ := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/close", AgentID: testPeerBare}})
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/close", "done")})
	if failure != nil {
		t.Fatalf("send: %#v", failure)
	}
	if failure := service.ConversationClose(context.Background(), result.ConversationID); failure != nil {
		t.Fatalf("first close: %#v", failure)
	}
	if failure := service.ConversationClose(context.Background(), result.ConversationID); failure != nil {
		t.Fatalf("idempotent close: %#v", failure)
	}
	unknown, _ := conversation.DeriveID(testMesh, testLocalFull, "unknown@example/mesh")
	if failure := service.ConversationClose(context.Background(), unknown); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("unknown close: %#v", failure)
	}
}

func TestMessagingServiceOutboundOmitsPerEnvelopeCredential(t *testing.T) {
	carrier := &testCarrier{}
	service, _ := newTestService(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/allowed", AgentID: testPeerBare}})
	if _, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/allowed", "secret")}); failure != nil {
		t.Fatalf("send = %#v", failure)
	}
	envelopes := carrier.envelopes()
	if len(envelopes) != 1 || len(envelopes[0].CredentialProof) != 0 {
		t.Fatalf("outbound credential field = %#v", envelopes)
	}
}

func TestMessagingServiceInboundDedupeAndMandatoryAcceptance(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	first := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "one"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, first), first); failure != nil {
		t.Fatalf("receive first: %#v", failure)
	}
	delivered := sink.deliveries()
	if len(delivered) != 1 || string(delivered[0].Payload.Value.(model.NativePayload).Body) != "one" {
		t.Fatalf("deliveries = %#v", delivered)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, first), first); failure != nil || len(sink.deliveries()) != 1 {
		t.Fatalf("terminal replay = %#v count=%d", failure, len(sink.deliveries()))
	}

	rejecting := &testDeliverySink{disposition: DeliveryCapacity}
	blocked, later := newTestService(t, &testCarrier{}, rejecting, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	envelope := signedInbound(t, later, protocol.ModeMessage, "", "", nativePayload("/independent", "blocked"))
	if failure := blocked.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure == nil || failure.Code != FailureCapacity {
		t.Fatalf("mandatory acceptance failure = %#v", failure)
	}
	status, failure := blocked.ConversationStatus(context.Background(), envelope.ConversationID)
	if failure != nil || status.Blocked {
		t.Fatalf("independent-message status = %#v %#v", status, failure)
	}
}

func TestMessagingServiceDedupeRetentionIsIndependentOfQueueCapacity(t *testing.T) {
	const queueCapacity = 1024
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/throughput", AgentID: testPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewHandlerRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	sink := &testDeliverySink{}
	service, err := NewMessagingService(MessagingConfig{
		QueueCapacity: queueCapacity, OutboxByteLimit: 1 << 20,
		RPCTimeout: fixedRPCTimeout(time.Second), OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second,
		Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity,
		Topology: testTopologySource{AuthoritativeGroupSnapshot{MeshID: testMesh, ObservedAt: now, Members: []Identity{
			{AgentID: testLocalBare, Internal: testLocalFull},
			{AgentID: testPeerBare, Internal: testPeerFull},
		}}},
		Carrier: &testCarrier{}, Payloads: testPipelineFactory{}, Deliveries: sink,
		Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	defer service.Shutdown(context.Background())

	var first protocol.Envelope
	for index := 0; index <= queueCapacity; index++ {
		envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/throughput", "body"))
		if index == 0 {
			first = envelope.Clone()
		}
		if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
			t.Fatalf("receive %d of %d: %#v", index+1, queueCapacity+1, failure)
		}
	}
	if got := len(sink.deliveries()); got != queueCapacity+1 {
		t.Fatalf("deliveries = %d, want %d", got, queueCapacity+1)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, first), first); failure != nil {
		t.Fatalf("receive duplicate after %d distinct messages: %#v", queueCapacity+1, failure)
	}
	if got := len(sink.deliveries()); got != queueCapacity+1 {
		t.Fatalf("deliveries after duplicate = %d, want %d", got, queueCapacity+1)
	}
}

func TestRank2AcceptanceRequiresTerminalLocalOwnership(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "accepted"))
	accepted, failure := service.ReceiveRank2(context.Background(), testInboundProvenance(t, envelope), envelope)
	if failure != nil || !accepted || len(sink.deliveries()) != 1 {
		t.Fatalf("terminal Rank2 acceptance accepted=%v failure=%#v deliveries=%d", accepted, failure, len(sink.deliveries()))
	}
	accepted, failure = service.ReceiveRank2(context.Background(), testInboundProvenance(t, envelope), envelope)
	if failure != nil || !accepted || len(sink.deliveries()) != 1 {
		t.Fatalf("terminal duplicate acceptance accepted=%v failure=%#v deliveries=%d", accepted, failure, len(sink.deliveries()))
	}

	rejecting := &testDeliverySink{disposition: DeliveryCapacity}
	blocked, later := newTestService(t, &testCarrier{}, rejecting, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	rejected := signedInbound(t, later, protocol.ModeMessage, "", "", nativePayload("/independent", "rejected"))
	accepted, failure = blocked.ReceiveRank2(context.Background(), testInboundProvenance(t, rejected), rejected)
	if failure == nil || failure.Code != FailureCapacity || !accepted {
		t.Fatalf("capacity-drop Rank2 acceptance accepted=%v failure=%#v", accepted, failure)
	}
	rejecting.setDisposition(DeliveryAccepted)
	accepted, failure = blocked.ReceiveRank2(context.Background(), testInboundProvenance(t, rejected), rejected)
	if failure != nil || !accepted {
		t.Fatalf("capacity-drop replay accepted=%v failure=%#v", accepted, failure)
	}
	if got := len(rejecting.deliveries()); got != 1 {
		t.Fatalf("capacity-drop handler attempts=%d, want 1", got)
	}
}

func TestRank2UnknownRPCResponseIsAcknowledgedAndDropped(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	correlation, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	replyTo, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	response := signedInbound(t, now, protocol.ModeResponse, correlation, replyTo, nativePayload("/rpc", "orphan"))
	accepted, failure := service.ReceiveRank2(context.Background(), testInboundProvenance(t, response), response)
	if failure != nil || !accepted {
		t.Fatalf("orphaned Rank2 response accepted=%v failure=%#v", accepted, failure)
	}
	if len(sink.deliveries()) != 0 {
		t.Fatalf("orphaned Rank2 response deliveries=%d, want 0", len(sink.deliveries()))
	}
	accepted, failure = service.ReceiveRank2(context.Background(), testInboundProvenance(t, response), response)
	if failure != nil || !accepted {
		t.Fatalf("orphaned Rank2 response replay accepted=%v failure=%#v", accepted, failure)
	}
	if got := service.deduper.Len(); got != 1 {
		t.Fatalf("orphaned response dedupe entries=%d, want 1 terminal entry", got)
	}
}

func TestMessagingServiceExpiredRequestIsDroppedWithoutBlockingIndependentMessage(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/independent", AgentID: testPeerBare}})
	second := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/independent", "two"))
	correlation, _ := protocol.NewCorrelationID()
	expired := signedInbound(t, now.Add(-time.Second), protocol.ModeRequest, correlation, "", nativePayload("/independent", "expired"))
	if !expired.ExpiresAt.Equal(now) {
		t.Fatalf("expiry boundary = %v want %v", expired.ExpiresAt, now)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, expired), expired); failure != nil {
		t.Fatalf("drop expired request: %#v", failure)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, second), second); failure != nil {
		t.Fatalf("receive independent message: %#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || string(deliveries[0].Payload.Value.(model.NativePayload).Body) != "two" || deliveries[0].RequestHandle != "" {
		t.Fatalf("deliveries after expiry = %#v", deliveries)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, expired), expired); failure != nil || len(sink.deliveries()) != 1 {
		t.Fatalf("expired replay = %#v count=%d", failure, len(sink.deliveries()))
	}
}

func TestMessagingServiceInboundTrustAndIDOnlyReplayDedupes(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/trusted", AgentID: testPeerBare}})
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/trusted", "original"))
	tampered := envelope.Clone()
	tampered.CredentialProof = []byte("forbidden-legacy-proof")
	if failure := service.Receive(context.Background(), testInboundProvenance(t, tampered), tampered); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("tampered proof = %#v", failure)
	}
	attacker, _ := NewGroupRank2Provenance("attacker@example/mesh", envelope.Recipient, envelope.MeshID)
	if failure := service.Receive(context.Background(), attacker, envelope); failure == nil || failure.Code != FailureAuthorization {
		t.Fatalf("untrusted sender = %#v", failure)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
		t.Fatalf("valid after pre-state rejection = %#v", failure)
	}
	conflict := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/trusted", "conflict"))
	conflict.MessageID = envelope.MessageID
	if failure := service.Receive(context.Background(), testInboundProvenance(t, conflict), conflict); failure != nil {
		t.Fatalf("scoped ID replay = %#v", failure)
	}
	if len(sink.deliveries()) != 1 {
		t.Fatalf("delivery count = %d", len(sink.deliveries()))
	}
}

func TestMessagingServiceRejectedInboundProofPrecedesCalibratedClockRead(t *testing.T) {
	service, now := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/trusted", AgentID: testPeerBare}})
	clock := &countingCalibratedClock{inner: service.config.Clock}
	service.config.Clock = clock
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/trusted", "secret"))
	envelope.CredentialProof = []byte("forbidden-legacy-proof")
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("rejected proof = %#v", failure)
	}
	if reads := clock.count(); reads != 0 {
		t.Fatalf("calibrated clock reads before proof rejection = %d", reads)
	}
}

func TestMessagingServiceRejectsFutureCreationBeforeLaneState(t *testing.T) {
	service, now := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/future", AgentID: testPeerBare}})
	envelope := signedInbound(t, now.Add(maximumFutureCreationSkew+time.Millisecond), protocol.ModeMessage, "", "", nativePayload("/future", "body"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("future creation = %#v", failure)
	}
	conversations, failure := service.ConversationList(context.Background())
	if failure != nil || len(conversations.Conversations) != 0 {
		t.Fatalf("future creation allocated lane = %#v %#v", conversations, failure)
	}
}

func TestMessagingServiceAcceptsFutureCreationAtFixedBound(t *testing.T) {
	sink := &testDeliverySink{}
	service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/future", AgentID: testPeerBare}})
	envelope := signedInbound(t, now.Add(maximumFutureCreationSkew), protocol.ModeMessage, "", "", nativePayload("/future", "body"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
		t.Fatalf("creation at fixed future bound = %#v", failure)
	}
	if len(sink.deliveries()) != 1 {
		t.Fatalf("delivery count = %d", len(sink.deliveries()))
	}
}

func TestMessagingServiceDoesNotRefreshAuthorityDuringMaterialization(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	lease := 20 * time.Millisecond
	clock := &testCalibratedClock{now: now}
	source := &clockTopologySource{clock: clock}
	materializations := 0
	identity, err := NewSessionIdentity(testMesh, testLocalBare, testLocalFull, bindingVerifier(func(string, string, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicyController([]model.PolicyRule{{Action: "allow", Path: "/large", AgentID: testPeerBare}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, _ := NewHandlerRegistry(4)
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
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/large", "large-body"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, envelope), envelope); failure != nil {
		t.Fatalf("receive across lease boundary = %#v", failure)
	}
	if materializations != 1 || source.count() != 1 || len(sink.deliveries()) != 1 {
		t.Fatalf("materializations=%d snapshots=%d deliveries=%d", materializations, source.count(), len(sink.deliveries()))
	}
}

func TestMessagingServiceRequestExpiryRejectsAmbiguityWithoutExtendingTTL(t *testing.T) {
	for name, expiryOffset := range map[string]time.Duration{
		"ambiguous at bound": 500 * time.Millisecond,
		"certainly live":     501 * time.Millisecond,
	} {
		t.Run(name, func(t *testing.T) {
			sink := &testDeliverySink{}
			service, now := newTestService(t, &testCarrier{}, sink, []model.PolicyRule{{Action: "allow", Path: "/expiry", AgentID: testPeerBare}})
			correlation, _ := protocol.NewCorrelationID()
			request := signedInbound(t, now, protocol.ModeRequest, correlation, "", nativePayload("/expiry", "body"))
			request.ExpiresAt = now.Add(expiryOffset)
			if failure := service.Receive(context.Background(), testInboundProvenance(t, request), request); failure != nil {
				t.Fatalf("receive = %#v", failure)
			}
			wantDeliveries := 1
			if name == "ambiguous at bound" {
				wantDeliveries = 0
			}
			if len(sink.deliveries()) != wantDeliveries {
				t.Fatalf("delivery count = %d want %d", len(sink.deliveries()), wantDeliveries)
			}
		})
	}
}

func TestMessagingServiceInboundRequestReplySingleUse(t *testing.T) {
	carrier := &testCarrier{}
	sink := &testDeliverySink{}
	service, now := newTestService(t, carrier, sink, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	correlation, _ := protocol.NewCorrelationID()
	request := signedInbound(t, now, protocol.ModeRequest, correlation, "", nativePayload("/rpc", "request"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, request), request); failure != nil {
		t.Fatalf("receive request: %#v", failure)
	}
	deliveries := sink.deliveries()
	if len(deliveries) != 1 || deliveries[0].RequestHandle == "" {
		t.Fatalf("request delivery = %#v", deliveries)
	}
	result, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{RequestHandle: deliveries[0].RequestHandle, Payload: nativePayload("/rpc", "response")})
	if failure != nil || !result.Accepted {
		t.Fatalf("reply = %#v %#v", result, failure)
	}
	sent := carrier.envelopes()
	if len(sent) != 1 || sent[0].Mode != protocol.ModeResponse || sent[0].CorrelationID != correlation || sent[0].ReplyTo != request.MessageID {
		t.Fatalf("reply envelope = %#v", sent)
	}
	if !sent[0].ExpiresAt.IsZero() {
		t.Fatalf("response carried request expiry %v", sent[0].ExpiresAt)
	}
	if _, failure := service.MessageReply(context.Background(), model.MessageReplyArgs{RequestHandle: deliveries[0].RequestHandle, Payload: nativePayload("/rpc", "again")}); failure == nil || failure.Code != FailureInvalidHandle {
		t.Fatalf("reused handle = %#v", failure)
	}
}

func TestMessagingServiceOutboundRequestCompletesOnlyOnOrderedResponse(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	carrier := &testCarrier{notify: notify}
	service, now := newTestService(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	type outcome struct {
		result  model.ResponseResult
		failure *Failure
	}
	done := make(chan outcome, 1)
	go func() {
		result, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- outcome{result, failure}
	}()
	request := <-notify
	if !request.CreatedAt.Equal(now) || !request.ExpiresAt.Equal(now.Add(time.Second)) {
		t.Fatalf("calibrated request times = created %v expiry %v", request.CreatedAt, request.ExpiresAt)
	}
	if request.ClockUncertainty != 250*time.Millisecond {
		t.Fatalf("request uncertainty = %v", request.ClockUncertainty)
	}
	responsePayload := model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK", Body: []byte("answer")}}
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, responsePayload)
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("receive response: %#v", failure)
	}
	completed := <-done
	if completed.failure != nil || string(completed.result.Payload.Value.(model.HTTPResponsePayload).Body) != "answer" || completed.result.MessageID != response.MessageID || completed.result.FromAgentID != testPeerBare {
		t.Fatalf("request completion = %#v %#v", completed.result, completed.failure)
	}
}

func TestMessagingServiceShutdownCancelsPendingRequest(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, _ := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	<-notify
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("shutdown: %#v", failure)
	}
	if failure := <-done; failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("pending request outcome = %#v", failure)
	}
}

func TestMessagingServiceShutdownDestroysOnlyItsOwnedOutbox(t *testing.T) {
	standalone, now := newTestService(t, &testCarrier{disposition: CarrierUnavailable}, &testDeliverySink{}, nil)
	envelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/event", "standalone secret"))
	if _, err := standalone.outbox.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	if failure := standalone.Shutdown(context.Background()); failure != nil {
		t.Fatalf("standalone shutdown: %#v", failure)
	}
	if messages, bytes := standalone.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("standalone retained outbox after shutdown: %d,%d", messages, bytes)
	}
	if _, err := standalone.outbox.Enqueue(envelope); err == nil {
		t.Fatal("standalone-owned outbox remained usable after shutdown")
	}

	shared, err := deliveryoutbox.New(deliveryoutbox.Config{MessageCapacity: 16, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	production, _ := newTestServiceAtWithOutbox(t, &testCarrier{disposition: CarrierUnavailable}, &testDeliverySink{}, nil, now, shared)
	sharedEnvelope := signedInbound(t, now, protocol.ModeMessage, "", "", nativePayload("/event", "shared secret"))
	if _, err = shared.Enqueue(sharedEnvelope); err != nil {
		t.Fatal(err)
	}
	if failure := production.Shutdown(context.Background()); failure != nil {
		t.Fatalf("production service shutdown: %#v", failure)
	}
	if messages, _ := shared.Usage(); messages != 1 {
		t.Fatalf("messaging destroyed connectivity-owned shared outbox: %d", messages)
	}
	shared.Destroy()
	if messages, bytes := shared.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("terminal shared owner retained outbox: %d,%d", messages, bytes)
	}
}

func TestMessagingServiceConsumesExactLateResponseAfterTerminalCarrierCustody(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	cleanupTestService(t, service)
	timeouts := &countingRPCTimeout{duration: time.Second}
	service.config.RPCTimeout = timeouts
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := waitTestEnvelope(t, notify)
	if !request.ExpiresAt.Equal(now.Add(time.Second)) || timeouts.count() != 1 {
		t.Fatalf("expiry=%v timeout snapshots=%d", request.ExpiresAt, timeouts.count())
	}
	if err := service.outboundRPC.Fail(request.CorrelationID, rpc.ErrExpired); err != nil {
		t.Fatalf("expire request: %v", err)
	}
	if failure := waitTestFailure(t, done); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("request timeout = %#v", failure)
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("terminal carrier retained request outbox=%d/%d", messages, bytes)
	}
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, nativePayload("/rpc", "late"))
	forged := response.Clone()
	forgedReplyTo, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	forged.ReplyTo = forgedReplyTo
	if failure := service.Receive(context.Background(), testInboundProvenance(t, forged), forged); failure != nil {
		t.Fatalf("unmatched late response drop = %#v", failure)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("exact late response after custody = %#v", failure)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("duplicate exact late response after custody = %#v", failure)
	}
	if timeouts.count() != 1 {
		t.Fatalf("timeout snapshots after completion = %d", timeouts.count())
	}
}

func TestMessagingServiceConsumesExactLateResponseForDurablyQueuedRequest(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	carrier := &testCarrier{notify: notify, disposition: CarrierAmbiguous}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	service, _ := newTestServiceAtWithDrain(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}}, now, nil, false)
	cleanupTestService(t, service)
	service.config.RPCTimeout = &countingRPCTimeout{duration: time.Second}
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := waitTestEnvelope(t, notify)
	if err := service.outboundRPC.Fail(request.CorrelationID, rpc.ErrExpired); err != nil {
		t.Fatalf("expire durable request: %v", err)
	}
	if failure := waitTestFailure(t, done); failure == nil || failure.Code != FailureDeadline {
		t.Fatalf("request timeout = %#v", failure)
	}
	if messages, _ := service.outbox.Usage(); messages != 1 {
		t.Fatalf("durable request usage=%d", messages)
	}
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, nativePayload("/rpc", "late"))
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("exact late response = %#v", failure)
	}
	if messages, bytes := service.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("retired durable request usage=%d/%d", messages, bytes)
	}
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("duplicate late response = %#v", failure)
	}
}

func waitTestEnvelope(t *testing.T, envelopes <-chan protocol.Envelope) protocol.Envelope {
	t.Helper()
	select {
	case envelope := <-envelopes:
		return envelope
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for carrier envelope")
		return protocol.Envelope{}
	}
}

func waitTestFailure(t *testing.T, failures <-chan *Failure) *Failure {
	t.Helper()
	select {
	case failure := <-failures:
		return failure
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request completion")
		return nil
	}
}

func cleanupTestService(t *testing.T, service *MessagingService) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if failure := service.Shutdown(ctx); failure != nil {
			t.Errorf("service shutdown: %#v", failure)
		}
	})
}

func TestPolicyAndHandlerUpdatesAreAtomicAndBounded(t *testing.T) {
	service, _ := newTestService(t, &testCarrier{}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/one", AgentID: testPeerBare}})
	before, failure := service.PolicyGet(context.Background())
	if failure != nil || len(before.Rules) != 1 {
		t.Fatalf("policy get = %#v %#v", before, failure)
	}
	if _, failure := service.PolicySet(context.Background(), model.PolicySetArgs{Rules: []model.PolicyRule{{Action: "allow", Path: "/same", AgentID: testPeerBare}, {Action: "deny", Path: "/same", AgentID: testPeerBare}}}); failure == nil || failure.Code != FailureRejected {
		t.Fatalf("ambiguous set = %#v", failure)
	}
	after, _ := service.PolicyGet(context.Background())
	if len(after.Rules) != 1 || after.Rules[0].Path != "/one" {
		t.Fatalf("failed update changed policy: %#v", after)
	}
	if failure := service.HandlerRegister(context.Background(), "/one"); failure != nil {
		t.Fatal(failure)
	}
	if failure := service.HandlerRegister(context.Background(), "/one"); failure != nil || service.handlers.Len() != 1 {
		t.Fatalf("idempotent register = %#v len=%d", failure, service.handlers.Len())
	}
	if failure := service.HandlerUnregister(context.Background(), "/missing"); failure != nil {
		t.Fatalf("idempotent unregister = %#v", failure)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatalf("shutdown: %#v", failure)
	}
	if _, failure := service.MeshList(context.Background()); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("operation after shutdown = %#v", failure)
	}
}

func signedInbound(t *testing.T, now time.Time, mode protocol.Mode, correlation, replyTo string, payload model.Payload) protocol.Envelope {
	t.Helper()
	conversationID, err := conversationIDForTests()
	if err != nil {
		t.Fatal(err)
	}
	return signedInboundWithIDs(t, now, mode, correlation, replyTo, conversationID, payload)
}

func signedInboundWithIDs(t *testing.T, now time.Time, mode protocol.Mode, correlation, replyTo, conversationID string, payload model.Payload) protocol.Envelope {
	t.Helper()
	prepared, disposition := (testPipeline{}).Prepare(payload)
	if disposition != PayloadAccepted {
		t.Fatal("prepare inbound")
	}
	descriptor, err := protocol.NewInlinePayload(prepared.Profile, prepared.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Time{}
	if mode == protocol.ModeRequest {
		expiresAt = now.Add(time.Second)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: conversationID, Sender: testPeerFull, Recipient: testLocalFull, MeshID: testMesh, Mode: mode, CorrelationID: correlation, ReplyTo: replyTo, CreatedAt: now, ExpiresAt: expiresAt, ClockUncertainty: 250 * time.Millisecond, Payload: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func testInboundProvenance(t *testing.T, envelope protocol.Envelope) AuthenticatedProvenance {
	t.Helper()
	provenance, err := NewGroupRank2Provenance(envelope.Sender, envelope.Recipient, envelope.MeshID)
	if err != nil {
		t.Fatal(err)
	}
	return provenance
}

func conversationIDForTests() (string, error) {
	return conversation.DeriveID(testMesh, testLocalFull, testPeerFull)
}

func TestPayloadFailurePreservesInvalidHandleCancellationAndDeadline(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		disposition PayloadDisposition
		want        FailureCode
	}{
		{PayloadInvalidHandle, FailureInvalidHandle},
		{PayloadCancelled, FailureCancelled},
		{PayloadDeadline, FailureDeadline},
	} {
		failure := payloadFailure(test.disposition)
		if failure == nil || failure.Code != test.want {
			t.Fatalf("disposition=%v failure=%#v want=%v", test.disposition, failure, test.want)
		}
	}
}

func TestInboundFailureTaxonomySeparatesAuthorizationFromMalformedAndIntegrity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want FailureCode
	}{
		{policy.ErrMembership, FailureAuthorization},
		{policy.ErrApplicationDenied, FailureAuthorization},
		{policy.ErrProvenance, FailureRejected},
		{policy.ErrIdentity, FailureRejected},
		{policy.ErrMesh, FailureRejected},
		{policy.ErrIntegrity, FailureRejected},
		{policy.ErrMaterializationRejected, FailureRejected},
		{policy.ErrInvalidPermit, FailureRejected},
	} {
		failure := classifyInboundError(test.err)
		if failure == nil || failure.Code != test.want {
			t.Fatalf("error=%v failure=%#v want=%v", test.err, failure, test.want)
		}
	}
}
