package rank2xmpp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

type admissionRouteSource struct {
	mu      sync.Mutex
	routes  map[string]payload.CarrierRoute
	changed chan struct{}
	waiting chan struct{}
	once    sync.Once
}

type completionPublicationReceiver struct {
	entered chan struct{}
	release chan struct{}
}

func (receiver *completionPublicationReceiver) HandleStanza(ctx context.Context, route payload.CarrierRoute, stanza Stanza) (*payload.CompletionEvidence, error) {
	close(receiver.entered)
	select {
	case <-receiver.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	evidence := payload.CompletionEvidence{TransferID: stanza.TransferID, MessageID: route.MessageID, Digest: payload.Digest([]byte("completion"))}
	return &evidence, nil
}

func newAdmissionRouteSource() *admissionRouteSource {
	return &admissionRouteSource{routes: make(map[string]payload.CarrierRoute), changed: make(chan struct{}), waiting: make(chan struct{})}
}

func (source *admissionRouteSource) ResolveTransferRoute(id string) (payload.CarrierRoute, bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	route, ok := source.routes[id]
	return route, ok
}

func (source *admissionRouteSource) WaitTransferRoute(ctx context.Context, id string) (payload.CarrierRoute, bool) {
	source.once.Do(func() { close(source.waiting) })
	for {
		source.mu.Lock()
		route, ok := source.routes[id]
		changed := source.changed
		source.mu.Unlock()
		if ok {
			return route, true
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return payload.CarrierRoute{}, false
		}
	}
}

func (source *admissionRouteSource) install(id string, route payload.CarrierRoute) {
	source.mu.Lock()
	source.routes[id] = route
	close(source.changed)
	source.changed = make(chan struct{})
	source.mu.Unlock()
}

func (source *admissionRouteSource) installSilent(id string, route payload.CarrierRoute) {
	source.mu.Lock()
	source.routes[id] = route
	source.mu.Unlock()
}

func (source *admissionRouteSource) notify() {
	source.mu.Lock()
	close(source.changed)
	source.changed = make(chan struct{})
	source.mu.Unlock()
}

func newAdmissionClient(t *testing.T, source RouteResolver) (*Client, context.CancelFunc) {
	t.Helper()
	lifetime, cancel := context.WithCancel(context.Background())
	box, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{
		config: Config{
			Auth: Authentication{MeshID: "mesh"}, TransferQueue: 4, TransferByteCapacity: 1 << 20,
			UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20,
			UnresolvedTransferLifetime: time.Second,
		},
		outbox: box, routes: source, ctx: lifetime, cancel: cancel, started: true,
		membershipReady: true, jobs: []chan transferWork{make(chan transferWork, 4)},
		transferAdmission: newTransferAdmission(1), complete: make(map[string]completionWaiter),
		readiness: make(map[string]objectReadinessWaiter), readinessSeen: make(map[string]objectReadinessInbound),
	}
	return client, cancel
}

func admissionRoute(id string) payload.CarrierRoute {
	return payload.CarrierRoute{
		PeerID: "peer@example.test/mesh", MeshID: "mesh", SenderID: "peer@example.test/mesh",
		RecipientID: "local@example.test/mesh", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA",
	}
}

func admissionStanza(kind StanzaKind, id string, marker byte) Stanza {
	return Stanza{
		Kind: kind, From: "peer@example.test/mesh", To: "local@example.test/mesh", MeshID: "mesh",
		TransferID: id, Data: []byte{marker},
	}
}

func TestRank2AdmissionResolvesBeforeNormalQueueAndPreservesLaneOrder(t *testing.T) {
	source := newAdmissionRouteSource()
	client, cancel := newAdmissionClient(t, source)
	defer cancel()
	id := "xfer_AAAAAAAAAAAAAAAAAAAAAA"
	client.admitTransfer(admissionStanza(StanzaTransferManifest, id, 1), client.ctx)
	<-source.waiting
	client.admitTransfer(admissionStanza(StanzaTransferChunk, id, 2), client.ctx)
	_, _, unresolvedCount, unresolvedBytes := client.transferAdmissionUsage()
	if unresolvedCount != 2 || unresolvedBytes == 0 {
		t.Fatalf("unresolved count=%d bytes=%d", unresolvedCount, unresolvedBytes)
	}
	source.install(id, admissionRoute(id))
	first := <-client.jobs[0]
	second := <-client.jobs[0]
	client.wg.Wait()
	if first.stanza.Data[0] != 1 || second.stanza.Data[0] != 2 {
		t.Fatalf("order/generation first=%#v second=%#v", first, second)
	}
	normalCount, normalBytes, unresolvedCount, unresolvedBytes := client.transferAdmissionUsage()
	if normalCount != 2 || normalBytes == 0 || unresolvedCount != 0 || unresolvedBytes != 0 {
		t.Fatalf("normal=%d/%d unresolved=%d/%d", normalCount, normalBytes, unresolvedCount, unresolvedBytes)
	}
	client.releaseTransferWork(first)
	client.releaseTransferWork(second)
	if count, bytes, _, _ := client.transferAdmissionUsage(); count != 0 || bytes != 0 {
		t.Fatalf("released normal=%d/%d", count, bytes)
	}
}

func TestRank2AdmissionRouteBecomesVisibleBeforeResolverMigrationPreservesPrefix(t *testing.T) {
	source := newAdmissionRouteSource()
	client, cancel := newAdmissionClient(t, source)
	defer cancel()
	id := "xfer_AAAAAAAAAAAAAAAAAAAAAA"
	client.admitTransfer(admissionStanza(StanzaTransferManifest, id, 1), client.ctx)
	<-source.waiting
	// Make ResolveTransferRoute succeed without waking the one lane resolver.
	// A later frame must still join the unresolved prefix, not bypass it.
	source.installSilent(id, admissionRoute(id))
	client.admitTransfer(admissionStanza(StanzaTransferChunk, id, 2), client.ctx)
	select {
	case work := <-client.jobs[0]:
		client.releaseTransferWork(work)
		t.Fatal("later frame bypassed unresolved prefix")
	default:
	}
	source.notify()
	first := <-client.jobs[0]
	second := <-client.jobs[0]
	client.wg.Wait()
	if first.stanza.Data[0] != 1 || second.stanza.Data[0] != 2 {
		t.Fatalf("migration order first=%d second=%d", first.stanza.Data[0], second.stanza.Data[0])
	}
	client.releaseTransferWork(first)
	client.releaseTransferWork(second)
}

func TestRank2AdmissionLateOldOwnerCannotDropReplacementLane(t *testing.T) {
	client, cancel := newAdmissionClient(t, newAdmissionRouteSource())
	defer cancel()
	id := "xfer_AAAAAAAAAAAAAAAAAAAAAA"
	old := &unresolvedTransferLane{entries: []unresolvedTransferEntry{{stanza: admissionStanza(StanzaTransferChunk, id, 1), bytes: 1}}}
	replacement := &unresolvedTransferLane{entries: []unresolvedTransferEntry{{stanza: admissionStanza(StanzaTransferChunk, id, 2), bytes: 1}}}
	client.transferAdmission.unresolved[id] = replacement
	client.transferAdmission.unresolvedCount = 1
	client.transferAdmission.unresolvedBytes = 1
	client.dropUnresolvedTransfer(id, old)
	if client.transferAdmission.unresolved[id] != replacement {
		t.Fatal("late old owner removed replacement generation lane")
	}
}

func TestRank2AdmissionRejectsFullSelectedShardBeforeMigration(t *testing.T) {
	client, cancel := newAdmissionClient(t, newAdmissionRouteSource())
	defer cancel()
	client.config.TransferQueue = 4
	client.jobs = []chan transferWork{make(chan transferWork, 2), make(chan transferWork, 2)}
	client.transferAdmission = newTransferAdmission(len(client.jobs))
	id := "xfer_AAAAAAAAAAAAAAAAAAAAAA"
	laneIndex := transferLane(id, len(client.jobs))
	for marker := byte(1); marker <= 2; marker++ {
		stanza := admissionStanza(StanzaTransferChunk, id, marker)
		charge, _ := retainedStanzaBytes(stanza)
		client.jobs[laneIndex] <- transferWork{stanza: stanza, route: admissionRoute(id), bytes: charge, lane: laneIndex}
		client.transferAdmission.normalCount++
		client.transferAdmission.normalBytes += charge
		client.transferAdmission.normalLaneCount[laneIndex]++
	}
	third := admissionStanza(StanzaTransferFinish, id, 3)
	charge, _ := retainedStanzaBytes(third)
	owner := &unresolvedTransferLane{entries: []unresolvedTransferEntry{{stanza: third, bytes: charge}}}
	client.transferAdmission.unresolved[id] = owner
	client.transferAdmission.unresolvedCount = 1
	client.transferAdmission.unresolvedBytes = charge
	client.migrateUnresolvedTransfer(id, owner, admissionRoute(id))
	if len(client.jobs[laneIndex]) != 2 || !bytesAllZero(third.Data) {
		t.Fatalf("selected shard admitted partial work len=%d cleared=%t", len(client.jobs[laneIndex]), bytesAllZero(third.Data))
	}
	if normal, _, unresolved, unresolvedBytes := client.transferAdmissionUsage(); normal != 2 || unresolved != 0 || unresolvedBytes != 0 {
		t.Fatalf("usage normal=%d unresolved=%d/%d", normal, unresolved, unresolvedBytes)
	}
	for len(client.jobs[laneIndex]) > 0 {
		client.releaseTransferWork(<-client.jobs[laneIndex])
	}
}

func TestRank2AdmissionFailsClosedAtUnresolvedByteBound(t *testing.T) {
	source := newAdmissionRouteSource()
	id := "xfer_AAAAAAAAAAAAAAAAAAAAAA"
	client, cancel := newAdmissionClient(t, source)
	defer cancel()
	client.config.UnresolvedTransferByteCapacity = 1
	client.admitTransfer(admissionStanza(StanzaTransferManifest, id, 2), client.ctx)
	if _, _, unresolved, unresolvedBytes := client.transferAdmissionUsage(); unresolved != 0 || unresolvedBytes != 0 {
		t.Fatalf("oversize unresolved admitted=%d/%d", unresolved, unresolvedBytes)
	}
}

func bytesAllZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

func TestRank2CompletionPublicationFinishesAdmission(t *testing.T) {
	release := make(chan struct{})
	close(release)
	receiver := &completionPublicationReceiver{entered: make(chan struct{}), release: release}
	client, session, source := newCompletionPublicationClient(t, receiver)
	id := typed("xfer_", 0x62)
	route := admissionRoute(id)
	source.install(id, route)
	client.admitTransfer(Stanza{Kind: StanzaTransferFinish, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: id, MessageID: route.MessageID}, client.ctx)
	<-receiver.entered
	waitForTransferAdmissionIdle(t, client)
	if !completionSent(session, 7) {
		t.Fatal("completion-first publication was lost")
	}
}

func newCompletionPublicationClient(t *testing.T, receiver TransferReceiver) (*Client, *fakeSession, *admissionRouteSource) {
	t.Helper()
	session := &fakeSession{events: make(chan Event)}
	box, err := outbox.New(outbox.Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	source := newAdmissionRouteSource()
	client, err := NewClient(Config{
		Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20,
		UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second,
		MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second,
	}, fakeDialer{session}, box, receiver, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client, session, source
}

func waitForTransferAdmissionIdle(t *testing.T, client *Client) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		normal, _, unresolved, _ := client.transferAdmissionUsage()
		if normal == 0 && unresolved == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("transfer admission did not become idle")
		}
		time.Sleep(time.Millisecond)
	}
}

func completionSent(session *fakeSession, _ uint64) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, stanza := range session.sent {
		if stanza.Kind == StanzaTransferCompletion {
			return true
		}
	}
	return false
}
