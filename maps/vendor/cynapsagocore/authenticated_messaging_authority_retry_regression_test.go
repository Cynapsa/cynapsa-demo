package cynapsagocore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

const (
	authorityRetryMesh      = "mesh-one"
	authorityRetryLocalBare = "agent@example.test"
	authorityRetryLocalFull = "agent@example.test/mesh-one"
	authorityRetryPeerBare  = "peer@example.test"
	authorityRetryPeerFull  = "peer@example.test/mesh-one"
)

type authorityRetryVerifier struct{}

func (authorityRetryVerifier) VerifyBoundIdentity(bare, meshID, full string) error {
	if bare != authorityRetryLocalBare || meshID != authorityRetryMesh || full != authorityRetryLocalFull {
		return errors.New("unexpected bound identity")
	}
	return nil
}

type authorityRetryClock struct{ now time.Time }

func (clock authorityRetryClock) Snapshot() mesh.CalibratedTime {
	return mesh.CalibratedTime{UTC: clock.now, Uncertainty: time.Millisecond}
}

type authorityRetryRPCTimeout time.Duration

func (timeout authorityRetryRPCTimeout) SnapshotRPCTimeout() time.Duration {
	return time.Duration(timeout)
}

type authorityRetryCarrier struct {
	delivered chan protocol.Envelope
}

func (carrier *authorityRetryCarrier) Send(ctx context.Context, envelope protocol.Envelope) mesh.CarrierDisposition {
	if ctx.Err() != nil {
		return mesh.CarrierUnavailable
	}
	select {
	case carrier.delivered <- envelope.Clone():
	default:
	}
	return mesh.CarrierAccepted
}

type authorityRetryPipelineFactory struct{}

func (authorityRetryPipelineFactory) Create(publisher mesh.EnvelopePublisher) (mesh.PayloadPipeline, mesh.PayloadDisposition) {
	if publisher == nil {
		return nil, mesh.PayloadRejected
	}
	return authorityRetryPipeline{}, mesh.PayloadAccepted
}

type authorityRetryPipeline struct{}

func (authorityRetryPipeline) Prepare(model.Payload) (mesh.PreparedPayload, mesh.PayloadDisposition) {
	return mesh.PreparedPayload{}, mesh.PayloadRejected
}

func (authorityRetryPipeline) Send(context.Context, mesh.PreparedSend) mesh.PayloadDisposition {
	return mesh.PayloadRejected
}

func (authorityRetryPipeline) Materialize(context.Context, protocol.Envelope) ([]byte, mesh.PayloadDisposition) {
	return nil, mesh.PayloadRejected
}

func (authorityRetryPipeline) Decode([]byte) (model.Payload, mesh.PayloadDisposition) {
	return model.Payload{}, mesh.PayloadRejected
}

func (authorityRetryPipeline) ApplicationPath(string, []byte) (string, mesh.PayloadDisposition) {
	return "", mesh.PayloadRejected
}

type authorityRetryDeliverySink struct{}

func (authorityRetryDeliverySink) Deliver(context.Context, mesh.InboundDelivery) mesh.DeliveryDisposition {
	return mesh.DeliveryRejected
}

type authorityRetrySource struct {
	mu        sync.Mutex
	service   *mesh.MessagingService
	authority *rank1MembershipAuthority
	now       time.Time
	pending   *mesh.MembershipAuthoritySession

	synchronizations int
	publications     int
	blocks           int
	failNext         bool
	failureEntered   chan struct{}
	failureRelease   chan struct{}
}

func (source *authorityRetrySource) armFailure() (<-chan struct{}, chan<- struct{}) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.failNext = true
	source.failureEntered = make(chan struct{})
	source.failureRelease = make(chan struct{})
	return source.failureEntered, source.failureRelease
}

func (source *authorityRetrySource) SynchronizeCurrentMembership(ctx context.Context, meshID string) (mesh.PeerAuthoritySnapshot, mesh.TopologyDisposition) {
	source.mu.Lock()
	source.synchronizations++
	fail := source.failNext
	entered, release := source.failureEntered, source.failureRelease
	if fail {
		source.failNext = false
	}
	service := source.service
	now := source.now
	source.mu.Unlock()

	if meshID != authorityRetryMesh || service == nil {
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyRejected
	}
	if fail {
		close(entered)
		select {
		case <-release:
			return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
		case <-ctx.Done():
			return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
		}
	}

	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
	}
	members := []mesh.Identity{
		{AgentID: authorityRetryLocalBare, Internal: authorityRetryLocalFull},
		{AgentID: authorityRetryPeerBare, Internal: authorityRetryPeerFull},
	}
	if failure = service.InstallCurrentMembership(session, members); failure != nil {
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
	}
	source.mu.Lock()
	source.pending = session
	source.mu.Unlock()
	return mesh.PeerAuthoritySnapshot{
		MeshID:          authorityRetryMesh,
		ObservedAt:      now,
		Local:           members[0],
		LocalAuthorized: true,
		Peers:           []mesh.PeerAuthorityResult{{Identity: members[1], Authorized: true}},
	}, mesh.TopologyReady
}

func (source *authorityRetrySource) PublishPeerAuthority(context.Context) error {
	source.mu.Lock()
	service, session, authority := source.service, source.pending, source.authority
	source.mu.Unlock()
	if service == nil || session == nil || authority == nil {
		return errors.New("missing authority publication state")
	}
	if failure := service.PublishCurrentMembership(session); failure != nil {
		return errors.New("peer lane publication failed")
	}
	authority.mu.Lock()
	authority.blocked = false
	authority.ready = true
	authority.membershipSession = session
	authority.currentPeers = []string{authorityRetryPeerFull}
	authority.signalStateChangedLocked()
	authority.mu.Unlock()
	source.mu.Lock()
	source.publications++
	source.pending = nil
	source.mu.Unlock()
	return nil
}

func TestRank1AuthoritySuspendResumePreservesCurrentMembershipCapability(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close(context.Background()) })
	authority := &rank1MembershipAuthority{
		live: live, timeout: time.Second, meshID: authorityRetryMesh,
		localBare: authorityRetryLocalBare, localFull: authorityRetryLocalFull,
		blocked: true,
	}
	source := &authorityRetrySource{authority: authority, now: now}
	identity, err := mesh.NewSessionIdentity(authorityRetryMesh, authorityRetryLocalBare, authorityRetryLocalFull, authorityRetryVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	policies, _ := mesh.NewPolicyController(nil)
	handlers, _ := mesh.NewHandlerRegistry(8)
	messaging, err := mesh.NewMessagingService(mesh.MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20,
		RPCTimeout: authorityRetryRPCTimeout(time.Second), OperationTimeout: 30 * time.Second, PeerIdleTimeout: 30 * time.Second, OutboxPollInterval: 30 * time.Second,
		Clock: authorityRetryClock{now: now},
	}, mesh.MessagingDependencies{
		Identity: identity, PeerAuthority: source, Carrier: &authorityRetryCarrier{delivered: make(chan protocol.Envelope, 1)},
		Payloads: authorityRetryPipelineFactory{}, Deliveries: authorityRetryDeliverySink{},
		Policies: policies, Handlers: handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	source.service = messaging
	authority.messaging = messaging
	if failure := messaging.Start(context.Background()); failure != nil {
		t.Fatalf("start messaging: %#v", failure)
	}
	t.Cleanup(func() { _ = messaging.Shutdown(context.Background()) })

	epoch := live.BlockLiveAuthority()
	if err = live.ReconcileLiveAuthority(context.Background(), epoch, []string{authorityRetryPeerFull}); err != nil {
		t.Fatal(err)
	}
	if err = live.PublishLiveAuthority(epoch); err != nil {
		t.Fatal(err)
	}
	if err = live.InstallLive(context.Background(), authorityRetryPeerFull, &rootLiveCarrier{}); err != nil {
		t.Fatal(err)
	}
	authority.mu.Lock()
	authority.managerEpoch = epoch
	authority.mu.Unlock()

	if !authority.Suspend() {
		t.Fatal("current authority did not suspend")
	}
	if authority.MembershipReady() || !authority.DataReady() {
		t.Fatal("suspended control did not preserve only retained data authority")
	}
	if err = authority.AuthorizeRank1(context.Background(), authorityRetryPeerFull); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("new Rank1 setup while suspended=%v", err)
	}
	if !authority.AdmitExistingRank1(authorityRetryPeerFull) {
		t.Fatal("existing exact Rank1 link was rejected during control outage")
	}
	if err = live.InstallLive(context.Background(), authorityRetryPeerFull, &rootLiveCarrier{}); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("live install while suspended=%v", err)
	}
	if failure := messaging.AuthorizeCurrentPeer(authorityRetryPeerFull); failure != nil {
		t.Fatalf("retained peer authority while suspended=%#v", failure)
	}
	retained := rootRank2Envelope(t, 7, "retained-outage")
	retained.Sender = authorityRetryLocalFull
	retained.Recipient = authorityRetryPeerFull
	retained.MeshID = authorityRetryMesh
	if err = live.Send(context.Background(), transport.KindLive, retained); err != nil {
		t.Fatalf("existing live send while control suspended=%v", err)
	}

	restored, err := authority.Resume(context.Background())
	if err != nil || !restored {
		t.Fatalf("resume=(%t,%v)", restored, err)
	}
	if !authority.MembershipReady() {
		t.Fatal("resumed authority did not report ready")
	}
	if failure := messaging.AuthorizeCurrentPeer(authorityRetryPeerFull); failure != nil {
		t.Fatalf("peer authority after resume=%#v", failure)
	}
	if err = live.InstallLive(context.Background(), authorityRetryPeerFull, &rootLiveCarrier{}); err != nil {
		t.Fatalf("live install after resume=%v", err)
	}
}

func (source *authorityRetrySource) BlockPeerAuthority() {
	source.mu.Lock()
	service, authority := source.service, source.authority
	source.blocks++
	source.pending = nil
	source.mu.Unlock()
	if service != nil {
		service.BlockAuthorityAdmission()
	}
	if authority != nil {
		authority.mu.Lock()
		authority.blocked = true
		authority.ready = false
		authority.signalStateChangedLocked()
		authority.mu.Unlock()
	}
}

func (source *authorityRetrySource) CurrentMembershipReady() bool {
	source.mu.Lock()
	authority := source.authority
	source.mu.Unlock()
	return authority != nil && authority.MembershipReady()
}

func (source *authorityRetrySource) counts() (synchronizations, publications, blocks int) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.synchronizations, source.publications, source.blocks
}

func TestRank2AuthorityMonitorRetriesFailedLivePublicationAndWakesQueuedDelivery(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	clientOutbox, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	client, err := rank2xmpp.NewClient(rank2xmpp.Config{
		Endpoint:                       "mesh.example.test:5222",
		Auth:                           rank2xmpp.Authentication{Username: authorityRetryLocalBare, Password: []byte("secret"), MeshID: authorityRetryMesh},
		ReceiveCapacity:                8,
		TransferWorkers:                1,
		TransferQueue:                  8,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     8,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   8,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, builtInTestDialer{session: newBuiltInTestSession()}, clientOutbox, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	snapshot, err := client.SyncAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = client.AcknowledgeAuthoritySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if !client.MembershipReady() {
		t.Fatal("Rank2 client did not enter the narrow ready state")
	}

	authority := &rank1MembershipAuthority{blocked: true}
	source := &authorityRetrySource{authority: authority, now: now}
	identity, err := mesh.NewSessionIdentity(authorityRetryMesh, authorityRetryLocalBare, authorityRetryLocalFull, authorityRetryVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	policies, err := mesh.NewPolicyController(nil)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := mesh.NewHandlerRegistry(8)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	carrier := &authorityRetryCarrier{delivered: make(chan protocol.Envelope, 1)}
	messaging, err := mesh.NewMessagingService(mesh.MessagingConfig{
		QueueCapacity: 8, OutboxByteLimit: 1 << 20,
		RPCTimeout: authorityRetryRPCTimeout(time.Second), OperationTimeout: 30 * time.Second, PeerIdleTimeout: 30 * time.Second, OutboxPollInterval: 30 * time.Second,
		Clock: authorityRetryClock{now: now},
	}, mesh.MessagingDependencies{
		Identity: identity, PeerAuthority: source, Carrier: carrier,
		Payloads: authorityRetryPipelineFactory{}, Deliveries: authorityRetryDeliverySink{},
		Policies: policies, Handlers: handlers, Outbox: queued,
	})
	if err != nil {
		t.Fatal(err)
	}
	source.service = messaging
	if failure := messaging.Start(context.Background()); failure != nil {
		t.Fatalf("start messaging: %#v", failure)
	}
	t.Cleanup(func() { _ = messaging.Shutdown(context.Background()) })

	if !client.MembershipReady() || !authority.MembershipReady() {
		t.Fatal("test did not establish the initial fully-ready authority state")
	}

	monitorContext, cancelMonitor := context.WithCancel(context.Background())
	monitorDone := make(chan error, 1)
	service := &authenticatedMessagingService{
		messaging: messaging,
		rank1: &rank1MessagingRuntime{
			client: client, authority: authority,
			timeout: 100 * time.Millisecond, retryDelay: 5 * time.Millisecond,
		},
	}
	go func() { monitorDone <- service.monitorRank2ForRank1(monitorContext) }()
	defer func() {
		cancelMonitor()
		<-monitorDone
	}()

	// The monitor is now allowed to settle into its Live+ready wait. A periodic
	// topology refresh then fences authority and fails without changing the
	// XMPP durable state. The local authority edge must wake the sleeping state
	// monitor so it can retry publication.
	time.Sleep(20 * time.Millisecond)
	failureEntered, failureRelease := source.armFailure()
	refreshDone := make(chan *mesh.Failure, 1)
	go func() { refreshDone <- messaging.RefreshCurrentMembership(context.Background()) }()
	select {
	case <-failureEntered:
	case <-time.After(time.Second):
		t.Fatal("periodic-style refresh did not enter the injected failure")
	}
	envelope := authorityRetryEnvelope(t, now)
	if _, err = queued.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	close(failureRelease)
	select {
	case failure := <-refreshDone:
		if failure == nil {
			t.Fatal("injected periodic-style refresh unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("injected periodic-style refresh did not return")
	}

	select {
	case delivered := <-carrier.delivered:
		if delivered.MessageID != envelope.MessageID {
			t.Fatalf("delivered message = %q, want %q", delivered.MessageID, envelope.MessageID)
		}
	case <-time.After(time.Second):
		t.Fatal("failed live refresh was not retried and woken after full authority publication")
	}
	if !authority.MembershipReady() {
		t.Fatal("retry delivered before composite authority became ready")
	}
	synchronizations, publications, blocks := source.counts()
	if synchronizations != 3 || publications != 2 || blocks != 1 {
		t.Fatalf("authority lifecycle sync=%d publish=%d block=%d, want 3/2/1", synchronizations, publications, blocks)
	}
}

func authorityRetryEnvelope(t *testing.T, now time.Time) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("native", []byte("queued-after-reconnect"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("authority-retry-conversation"))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   "conv_" + base64.RawURLEncoding.EncodeToString(digest[:]),
		Sender:           authorityRetryLocalFull,
		Recipient:        authorityRetryPeerFull,
		MeshID:           authorityRetryMesh,
		Mode:             protocol.ModeMessage,
		CreatedAt:        now,
		ClockUncertainty: time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}
