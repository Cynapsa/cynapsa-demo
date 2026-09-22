package mesh

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

type mutableGroupSource struct {
	mu       sync.Mutex
	snapshot AuthoritativeGroupSnapshot
}

func (source *mutableGroupSource) SnapshotGroup(ctx context.Context, meshID string) (AuthoritativeGroupSnapshot, TopologyDisposition) {
	if ctx.Err() != nil {
		return AuthoritativeGroupSnapshot{}, TopologyUnavailable
	}
	source.mu.Lock()
	snapshot := source.snapshot
	snapshot.Members = append([]Identity(nil), snapshot.Members...)
	source.mu.Unlock()
	if snapshot.MeshID != meshID {
		return AuthoritativeGroupSnapshot{}, TopologyRejected
	}
	return snapshot, TopologyReady
}

func (source *mutableGroupSource) replace(observedAt time.Time, members ...Identity) {
	source.mu.Lock()
	source.snapshot = AuthoritativeGroupSnapshot{
		MeshID: testMesh, ObservedAt: observedAt,
		Members: append([]Identity(nil), members...),
	}
	source.mu.Unlock()
}

type decodeBarrierPipeline struct {
	testPipeline
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (pipeline *decodeBarrierPipeline) Decode(canonical []byte) (model.Payload, PayloadDisposition) {
	pipeline.once.Do(func() { close(pipeline.entered) })
	<-pipeline.release
	return pipeline.testPipeline.Decode(canonical)
}

type decodeBarrierFactory struct {
	entered chan struct{}
	release chan struct{}
}

func (factory decodeBarrierFactory) Create(publisher EnvelopePublisher) (PayloadPipeline, PayloadDisposition) {
	return &decodeBarrierPipeline{testPipeline: testPipeline{publisher: publisher}, entered: factory.entered, release: factory.release}, PayloadAccepted
}

func newCurrentMembershipService(t *testing.T, source AuthoritativeGroupSource, carrier EnvelopeCarrier, sink DeliverySink) (*MessagingService, time.Time) {
	return newCurrentMembershipServiceWithFactory(t, source, carrier, sink, testPipelineFactory{})
}

func newCurrentMembershipServiceWithFactory(t *testing.T, source AuthoritativeGroupSource, carrier EnvelopeCarrier, sink DeliverySink, factory PayloadPipelineFactory) (*MessagingService, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
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
		OperationTimeout: 10 * time.Second, PeerIdleTimeout: 10 * time.Second, OutboxPollInterval: 10 * time.Second, Clock: &testCalibratedClock{now: now},
	}, MessagingDependencies{
		Identity: identity, Topology: source, Carrier: carrier, Payloads: factory,
		Deliveries: sink, Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if failure := service.Start(context.Background()); failure != nil {
		t.Fatalf("start: %#v", failure)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	return service, now
}
