package rank2xmpp

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

type readinessRoutes struct {
	route payload.CarrierRoute
}

func (routes readinessRoutes) ResolveTransferRoute(string) (payload.CarrierRoute, bool) {
	return routes.route, routes.route.MessageID != ""
}

type readinessReceiver struct{}

func (readinessReceiver) HandleStanza(context.Context, payload.CarrierRoute, Stanza) (*payload.CompletionEvidence, error) {
	return nil, nil
}

type readinessLaneReceiver struct {
	readinessReceiver
	entered chan struct{}
	release chan struct{}
}

func (receiver *readinessLaneReceiver) AdmitAuthenticatedPayloadWork(ctx context.Context, peerID string, _ uint64, run func(context.Context) error) error {
	if peerID != "b@example.test/mesh" {
		return ErrAuthentication
	}
	close(receiver.entered)
	<-receiver.release
	return run(ctx)
}

type readinessProbe struct {
	mu         sync.Mutex
	references []string
	err        error
}

func (probe *readinessProbe) ProbeDownloadReference(ctx context.Context, reference string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	probe.mu.Lock()
	probe.references = append(probe.references, reference)
	err := probe.err
	probe.mu.Unlock()
	return err
}

func (probe *readinessProbe) count() int {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return len(probe.references)
}

func readinessManifest(t *testing.T, sender, recipient string, expires time.Time) payload.TransferManifest {
	t.Helper()
	canonical := []byte("canonical-payload")
	digest := sha256.Sum256(canonical)
	manifest, err := payload.BuildManifest(payload.TransferBinding{
		TransferID: typed("xfer_", 0x61), MessageID: typed("msg_", 0x62), MeshID: "mesh",
		SenderID: sender, RecipientID: recipient, CanonicalSize: int64(len(canonical)), CanonicalDigest: digest,
	}, canonical, canonical, 8, "", expires.UTC().Truncate(time.Millisecond), nil)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func sentReadiness(t *testing.T, session *fakeSession, kind StanzaKind) Stanza {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		session.mu.Lock()
		for _, stanza := range session.sent {
			if stanza.Kind == kind {
				result := stanza.clone()
				session.mu.Unlock()
				return result
			}
		}
		session.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("readiness stanza %d was not sent", kind)
	return Stanza{}
}

func sentReadinessCount(session *fakeSession, kind StanzaKind) int {
	session.mu.Lock()
	defer session.mu.Unlock()
	count := 0
	for _, stanza := range session.sent {
		if stanza.Kind == kind {
			count++
		}
	}
	return count
}

func TestObjectReadinessCanonicalFrameRoundTrip(t *testing.T) {
	manifest := readinessManifest(t, "a@example.test/mesh", "b@example.test/mesh", time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	manifestBytes, err := payload.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := objectReadinessRequest{
		AttemptID: typed("rdy_", 0x63), MessageID: manifest.MessageID, TransferID: manifest.TransferID,
		ExpiresAt: manifest.ExpiresAt, Reference: "https://objects.example.test:8443/blob/token",
		ReferenceDigest: sha256.Sum256([]byte("https://objects.example.test:8443/blob/token")), ManifestDigest: sha256.Sum256(manifestBytes),
	}
	encoded, err := encodeObjectReadinessRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeObjectReadinessRequest(encoded)
	if err != nil || !sameObjectReadinessRequest(decoded, request) || decoded.Reference != request.Reference {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
	stanza := Stanza{Kind: StanzaObjectReadinessRequest, From: manifest.SenderID, To: manifest.RecipientID, MeshID: manifest.MeshID, TransferID: manifest.TransferID, Data: encoded}
	wire, err := EncodeStanzaFrame(stanza, 64<<10, 96<<10)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := DecodeStanzaFrame(wire, stanza.From, stanza.To, stanza.MeshID, 64<<10, 96<<10)
	if err != nil || roundTrip.AttemptID != request.AttemptID || roundTrip.MessageID != request.MessageID || roundTrip.TransferID != request.TransferID {
		t.Fatalf("round trip=%#v err=%v", roundTrip, err)
	}
	if _, err = decodeObjectReadinessRequest(append(encoded, 0)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing data accepted: %v", err)
	}
	clear(manifestBytes)
}

func TestConfirmObjectReadinessRequiresExactAuthenticatedResult(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	pendingClient := newReadinessClient(t, session)
	defer pendingClient.Close(context.Background())
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh", MessageID: typed("msg_", 0x62)}
	manifest := readinessManifest(t, route.SenderID, route.RecipientID, time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	done := make(chan error, 1)
	go func() {
		done <- pendingClient.ConfirmObjectReadiness(context.Background(), route, manifest, "https://objects.example.test:8443/blob/token")
	}()
	requestStanza := sentReadiness(t, session, StanzaObjectReadinessRequest)
	request, err := decodeObjectReadinessRequest(requestStanza.Data)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeObjectReadinessResult(objectReadinessResult{request: request, ready: true})
	if err != nil {
		t.Fatal(err)
	}
	// A result for the right attempt but the wrong authenticated sender must
	// not consume the single-use correlation.
	pendingClient.handleObjectReadinessResult(Stanza{Kind: StanzaObjectReadinessResult, From: "mallory@example.test/mesh", To: route.SenderID, MeshID: route.MeshID, TransferID: manifest.TransferID, AttemptID: request.AttemptID, MessageID: request.MessageID, Data: encoded})
	select {
	case err = <-done:
		t.Fatalf("spoofed result completed exchange: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	pendingClient.handleObjectReadinessResult(Stanza{Kind: StanzaObjectReadinessResult, From: route.PeerID, To: route.SenderID, MeshID: route.MeshID, TransferID: manifest.TransferID, AttemptID: request.AttemptID, MessageID: request.MessageID, Data: encoded})
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("exact readiness result did not complete")
	}
}

func TestObjectReadinessReceiverProbesOnceAndReplaysDuplicate(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	client := newReadinessClient(t, session)
	defer client.Close(context.Background())
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "b@example.test/mesh", RecipientID: "a@example.test/mesh", MessageID: typed("msg_", 0x62)}
	probe := &readinessProbe{}
	if err := client.BindPayloadRuntime(readinessReceiver{}, readinessRoutes{route: route}, probe); err != nil {
		t.Fatal(err)
	}
	manifest := readinessManifest(t, route.SenderID, route.RecipientID, time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	manifestBytes, err := payload.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	reference := "https://objects.example.test:8443/blob/token"
	request := objectReadinessRequest{
		AttemptID: typed("rdy_", 0x63), MessageID: route.MessageID, TransferID: manifest.TransferID,
		ExpiresAt: manifest.ExpiresAt, Reference: reference,
		ReferenceDigest: sha256.Sum256([]byte(reference)), ManifestDigest: sha256.Sum256(manifestBytes),
	}
	encoded, err := encodeObjectReadinessRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	stanza := Stanza{Kind: StanzaObjectReadinessRequest, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: manifest.TransferID, AttemptID: request.AttemptID, MessageID: request.MessageID, Data: encoded}
	session.events <- Event{Kind: EventStanza, Stanza: stanza}
	resultStanza := sentReadiness(t, session, StanzaObjectReadinessResult)
	result, err := decodeObjectReadinessResult(resultStanza.Data)
	if err != nil || !result.ready || probe.count() != 1 {
		t.Fatalf("result=%#v probes=%d err=%v", result, probe.count(), err)
	}
	session.events <- Event{Kind: EventStanza, Stanza: stanza}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && sentReadinessCount(session, StanzaObjectReadinessResult) < 2 {
		time.Sleep(time.Millisecond)
	}
	if probe.count() != 1 || sentReadinessCount(session, StanzaObjectReadinessResult) != 2 {
		t.Fatalf("duplicate handling probes=%d responses=%d", probe.count(), sentReadinessCount(session, StanzaObjectReadinessResult))
	}
	client.mu.Lock()
	seen := len(client.readinessSeen)
	client.mu.Unlock()
	if seen != 1 || !replayMeshAllowed(stanza, "mesh") {
		t.Fatalf("current-mesh object rejected: seen=%d replay=%v", seen, replayMeshAllowed(stanza, "mesh"))
	}
	clear(manifestBytes)
}

func TestObjectReadinessEntersPeerLaneBeforeProbeOrRetention(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	client := newReadinessClient(t, session)
	defer client.Close(context.Background())
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "b@example.test/mesh", RecipientID: "a@example.test/mesh", MessageID: typed("msg_", 0x72)}
	admitter := &readinessLaneReceiver{entered: make(chan struct{}), release: make(chan struct{})}
	probe := &readinessProbe{}
	if err := client.BindPayloadRuntime(admitter, readinessRoutes{route: route}, probe); err != nil {
		t.Fatal(err)
	}
	manifest := readinessManifest(t, route.SenderID, route.RecipientID, time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	manifest.MessageID = route.MessageID
	manifestBytes, err := payload.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(manifestBytes)
	reference := "https://objects.example.test:8443/blob/lane"
	request := objectReadinessRequest{
		AttemptID: typed("rdy_", 0x73), MessageID: route.MessageID, TransferID: manifest.TransferID,
		ExpiresAt: manifest.ExpiresAt, Reference: reference,
		ReferenceDigest: sha256.Sum256([]byte(reference)), ManifestDigest: sha256.Sum256(manifestBytes),
	}
	encoded, err := encodeObjectReadinessRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	stanza := Stanza{Kind: StanzaObjectReadinessRequest, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: manifest.TransferID, AttemptID: request.AttemptID, MessageID: request.MessageID, Data: encoded}
	session.events <- Event{Kind: EventStanza, Stanza: stanza}
	<-admitter.entered
	client.mu.Lock()
	seen := len(client.readinessSeen)
	client.mu.Unlock()
	if probe.count() != 0 || seen != 0 {
		t.Fatalf("before lane release probes=%d seen=%d", probe.count(), seen)
	}
	close(admitter.release)
	resultStanza := sentReadiness(t, session, StanzaObjectReadinessResult)
	result, err := decodeObjectReadinessResult(resultStanza.Data)
	if err != nil || !result.ready || probe.count() != 1 {
		t.Fatalf("result=%#v probes=%d err=%v", result, probe.count(), err)
	}
}

func TestObjectReadinessWaitIsBoundedByManifestExpiry(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	client := newReadinessClient(t, session)
	defer client.Close(context.Background())
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh", MessageID: typed("msg_", 0x62)}
	client.mu.Lock()
	snapshot, ok := client.clock.Snapshot()
	client.mu.Unlock()
	if !ok {
		t.Fatal("calibrated clock unavailable")
	}
	manifest := readinessManifest(t, route.SenderID, route.RecipientID, snapshot.UTC.Add(25*time.Millisecond))
	started := time.Now()
	err := client.ConfirmObjectReadiness(context.Background(), route, manifest, "https://objects.example.test:8443/blob/token")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("expiry error=%v elapsed=%v", err, time.Since(started))
	}
}

func newReadinessClient(t *testing.T, session *fakeSession) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity: 8, TransferWorkers: 1, TransferQueue: 8, MailboxLimit: 8,
		TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 8,
		UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second,
		ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second,
	}, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	return client
}
