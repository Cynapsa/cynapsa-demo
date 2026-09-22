package rank1webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

type blockingTransferReceiver struct{ entered chan struct{} }

func (r *blockingTransferReceiver) HandleFrame(ctx context.Context, _ payload.CarrierRoute, _ Frame) (*payload.CompletionEvidence, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingPionNegotiator struct{ entered chan struct{} }

func (n *blockingPionNegotiator) Negotiate(ctx context.Context, _ *webrtc.PeerConnection, _ bool) error {
	select {
	case n.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}
func (*blockingPionNegotiator) EffectiveMaximumFrameBytes() int { return 1024 }
func (*blockingPionNegotiator) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("owned-open")), true
}

type controlledRouteResolver struct {
	mu      sync.Mutex
	routes  map[string]payload.CarrierRoute
	release map[string]chan struct{}
	entered chan string
	cancel  chan string
}

func (r *controlledRouteResolver) ResolveTransferRoute(id string) (payload.CarrierRoute, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	route, ok := r.routes[id]
	return route, ok
}

func (r *controlledRouteResolver) WaitTransferRoute(ctx context.Context, id string) (payload.CarrierRoute, bool) {
	r.mu.Lock()
	release := r.release[id]
	if release == nil {
		release = make(chan struct{})
		r.release[id] = release
	}
	r.mu.Unlock()
	select {
	case r.entered <- id:
	default:
	}
	select {
	case <-release:
		return r.ResolveTransferRoute(id)
	case <-ctx.Done():
		select {
		case r.cancel <- id:
		default:
		}
		return payload.CarrierRoute{}, false
	}
}

func (r *controlledRouteResolver) authorize(id string, route payload.CarrierRoute) {
	r.mu.Lock()
	r.routes[id] = route
	release := r.release[id]
	if release == nil {
		release = make(chan struct{})
		r.release[id] = release
	}
	select {
	case <-release:
	default:
		close(release)
	}
	r.mu.Unlock()
}

type recordingTransferReceiver struct{ frames chan Frame }

func (r recordingTransferReceiver) HandleFrame(_ context.Context, _ payload.CarrierRoute, frame Frame) (*payload.CompletionEvidence, error) {
	r.frames <- frame.Clone()
	return nil, nil
}

func groupBTransferID(seed byte) string {
	return "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{seed}, 16))
}

func groupBInboundRoute() payload.CarrierRoute {
	return payload.CarrierRoute{PeerID: "b", MeshID: "mesh", SenderID: "b", RecipientID: "a", MessageID: "msg_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16))}
}

func groupBLink(t *testing.T, queue int, timeout time.Duration, receiver TransferReceiver, resolver RouteResolver) (*Link, *fakeChannel) {
	t.Helper()
	channel := &fakeChannel{maximum: 4096, inbound: make(chan Frame, 64), state: transport.HealthHealthy, closed: make(chan struct{}), sentNote: make(chan Frame, 64)}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 4096, ReceiveCapacity: 8, TransferWorkers: 1, TransferQueue: queue, TransferQueueBytes: queue * 4096, TransferRouteTimeout: timeout, Clock: transport.ClockFunc(func() time.Time { return time.Now().UTC() })}, &fakePC{channel: channel}, receiver, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = link.Close(ctx)
	})
	return link, channel
}

func TestLinkUnresolvedTransferDoesNotBlockEnvelope(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	link, channel := groupBLink(t, 4, time.Second, recordingTransferReceiver{frames: make(chan Frame, 4)}, resolver)
	channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: groupBTransferID(1), Data: []byte("manifest")}
	<-resolver.entered
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(liveEnvelope(t, "b", "a"))
	if err != nil {
		t.Fatal(err)
	}
	channel.inbound <- Frame{Kind: FrameEnvelope, Data: encoded}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err = link.ReceiveAuthenticated(ctx); err != nil {
		t.Fatalf("envelope blocked behind unresolved transfer: %v", err)
	}
}

func TestLinkManifestBeforeEnvelopeResolvesAndPreservesOrder(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	received := make(chan Frame, 4)
	_, channel := groupBLink(t, 4, time.Second, recordingTransferReceiver{frames: received}, resolver)
	id := groupBTransferID(2)
	channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: id, Data: []byte("manifest")}
	channel.inbound <- Frame{Kind: FrameTransferChunk, TransferID: id, Data: []byte("chunk")}
	<-resolver.entered
	resolver.authorize(id, groupBInboundRoute())
	for index, want := range []FrameKind{FrameTransferManifest, FrameTransferChunk} {
		select {
		case frame := <-received:
			if frame.Kind != want {
				t.Fatalf("frame %d kind=%v want=%v", index, frame.Kind, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("frame %d not delivered", index)
		}
	}
}

func TestLinkUnknownTransferDoesNotBlockHealthReply(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	_, channel := groupBLink(t, 2, time.Second, recordingTransferReceiver{frames: make(chan Frame, 2)}, resolver)
	channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: groupBTransferID(3), Data: []byte("unknown")}
	<-resolver.entered
	nonce := bytes.Repeat([]byte{7}, 16)
	channel.inbound <- Frame{Kind: FrameHealthProbe, ProbeNonce: nonce}
	select {
	case frame := <-channel.sentNote:
		if frame.Kind != FrameHealthReply || !bytes.Equal(frame.ProbeNonce, nonce) {
			t.Fatalf("reply=%#v", frame)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("health reply blocked behind unknown transfer")
	}
}

func TestLinkUnresolvedTransferAdmissionIsBounded(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 16), cancel: make(chan string, 16)}
	link, channel := groupBLink(t, 2, time.Second, recordingTransferReceiver{frames: make(chan Frame, 2)}, resolver)
	for i := 0; i < 32; i++ {
		channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: groupBTransferID(byte(i + 1)), Data: bytes.Repeat([]byte{byte(i)}, 128)}
	}
	deadline := time.Now().Add(time.Second)
	for {
		link.transferAdmission.mu.Lock()
		count, retained := link.transferAdmission.count, link.transferAdmission.bytes
		link.transferAdmission.mu.Unlock()
		if count == 2 {
			if retained > link.config.TransferQueueBytes {
				t.Fatalf("retained bytes=%d", retained)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission count=%d", count)
		}
		time.Sleep(time.Millisecond)
	}
	nonce := bytes.Repeat([]byte{9}, 16)
	channel.inbound <- Frame{Kind: FrameHealthProbe, ProbeNonce: nonce}
	select {
	case <-channel.sentNote:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("saturated admission blocked receive pump")
	}
}

func TestLinkCloseCancelsRouteWaiters(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	link, channel := groupBLink(t, 2, time.Second, recordingTransferReceiver{frames: make(chan Frame, 2)}, resolver)
	id := groupBTransferID(4)
	channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: id, Data: []byte("wait")}
	<-resolver.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := link.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-resolver.cancel:
		if got != id {
			t.Fatalf("cancelled=%q", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("route waiter was not cancelled")
	}
	link.transferAdmission.mu.Lock()
	count, retained := link.transferAdmission.count, link.transferAdmission.bytes
	link.transferAdmission.mu.Unlock()
	if count != 0 || retained != 0 {
		t.Fatalf("retained count=%d bytes=%d", count, retained)
	}
}

func TestLinkCloseClearsAllTransferOwnership(t *testing.T) {
	resolver := &controlledRouteResolver{routes: make(map[string]payload.CarrierRoute), release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	link, channel := groupBLink(t, 4, time.Second, recordingTransferReceiver{frames: make(chan Frame, 4)}, resolver)
	for _, id := range []string{groupBTransferID(91), groupBTransferID(92)} {
		channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: id, Data: []byte("retained")}
	}
	deadline := time.Now().Add(time.Second)
	for {
		link.transferAdmission.mu.Lock()
		count := link.transferAdmission.count
		link.transferAdmission.mu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admitted=%d", count)
		}
		time.Sleep(time.Millisecond)
	}
	if err := link.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	link.transferAdmission.mu.Lock()
	defer link.transferAdmission.mu.Unlock()
	if link.transferAdmission.count != 0 || link.transferAdmission.bytes != 0 || len(link.transferAdmission.pending) != 0 || len(link.transferAdmission.usage) != 0 || len(link.transferAdmission.queue) != 0 {
		t.Fatalf("ownership remains: count=%d bytes=%d pending=%d usage=%d queue=%d", link.transferAdmission.count, link.transferAdmission.bytes, len(link.transferAdmission.pending), len(link.transferAdmission.usage), len(link.transferAdmission.queue))
	}
}

func TestLinkPerTransferQuotaSpansReceiverLifecycle(t *testing.T) {
	id := groupBTransferID(93)
	resolver := &controlledRouteResolver{routes: map[string]payload.CarrierRoute{id: groupBInboundRoute()}, release: make(map[string]chan struct{}), entered: make(chan string, 8), cancel: make(chan string, 8)}
	receiver := &blockingTransferReceiver{entered: make(chan struct{}, 1)}
	link, channel := groupBLink(t, 4, time.Second, receiver, resolver)
	channel.inbound <- Frame{Kind: FrameTransferManifest, TransferID: id, Data: bytes.Repeat([]byte{1}, 64)}
	select {
	case <-receiver.entered:
	case <-time.After(time.Second):
		t.Fatal("receiver did not block")
	}
	for index := 0; index < 16; index++ {
		channel.inbound <- Frame{Kind: FrameTransferChunk, TransferID: id, Data: bytes.Repeat([]byte{byte(index + 2)}, 64)}
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		link.transferAdmission.mu.Lock()
		usage := link.transferAdmission.usage[id]
		perID := link.transferAdmission.perIDCount
		link.transferAdmission.mu.Unlock()
		if usage.count > perID {
			t.Fatalf("usage=%d per-id=%d", usage.count, perID)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandshakeRestartTokenExhaustionIsPermanent(t *testing.T) {
	assembly, _ := groupBAssembly(t, 1)
	assembly.mu.Lock()
	assembly.nextToken = math.MaxUint64
	assembly.mu.Unlock()
	for _, peer := range []string{"first/mesh", "second/mesh"} {
		if reservation, err := assembly.reserveRestart(peer); err == nil || reservation.token != 0 {
			t.Fatalf("peer=%q reservation=%#v err=%v", peer, reservation, err)
		}
	}
	assembly.mu.Lock()
	defer assembly.mu.Unlock()
	if assembly.nextToken != math.MaxUint64 || !assembly.tokenExhausted || len(assembly.reserved) != 0 {
		t.Fatalf("token=%d exhausted=%v reserved=%d", assembly.nextToken, assembly.tokenExhausted, len(assembly.reserved))
	}
}

func TestPionCloseJoinsOpenAndOwnsICEProfile(t *testing.T) {
	servers := []webrtc.ICEServer{{URLs: []string{"turn:example.test"}, Username: "private", Credential: "secret"}}
	negotiator := &blockingPionNegotiator{entered: make(chan struct{}, 1)}
	connection, err := NewPionPeerConnection(PionConfig{Initiator: true, ICEServers: servers, ICETransportPolicy: webrtc.ICETransportPolicyAll, ReceiveCapacity: 1, Clock: transport.ClockFunc(func() time.Time { return time.Now().UTC() })}, negotiator)
	if err != nil {
		t.Fatal(err)
	}
	servers[0].URLs[0] = "turn:caller-mutated.test"
	if connection.config.ICEServers[0].URLs[0] != "turn:example.test" {
		t.Fatal("constructor retained caller URL storage")
	}
	result := make(chan error, 1)
	go func() {
		_, openErr := connection.OpenDataChannel(context.Background(), DataChannelConfig{Label: "aztm", MaximumFrameBytes: 1024})
		result <- openErr
	}()
	select {
	case <-negotiator.entered:
	case <-time.After(time.Second):
		t.Fatal("open did not enter")
	}
	if err = connection.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("late open err=%v", err)
	}
	if servers[0].Username != "private" || servers[0].Credential != "secret" || servers[0].URLs[0] != "turn:caller-mutated.test" {
		t.Fatalf("caller profile mutated: %#v", servers[0])
	}
}

type controlledJingleSource struct {
	items chan sourceJingle
}

type sourceJingle struct {
	from   string
	signal rank2xmpp.Jingle
	read   chan struct{}
}

func (s *controlledJingleSource) ReceiveJingle(ctx context.Context) (string, rank2xmpp.Jingle, error) {
	select {
	case item := <-s.items:
		close(item.read)
		return item.from, item.signal, nil
	case <-ctx.Done():
		return "", rank2xmpp.Jingle{}, ctx.Err()
	}
}

func (s *controlledJingleSource) send(t *testing.T, from string, signal rank2xmpp.Jingle) {
	t.Helper()
	read := make(chan struct{})
	s.items <- sourceJingle{from: from, signal: signal, read: read}
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("signal source was not drained")
	}
}

type blockingSignalNegotiator struct {
	mu      sync.Mutex
	active  map[string]int
	maximum int
	entered chan string
	release <-chan struct{}
}

type signalPumpMutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *signalPumpMutableClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *signalPumpMutableClock) set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

type cleanupPastDeadlineNegotiator struct {
	mu           sync.Mutex
	calls        int
	active       int
	maximum      int
	entered      chan int
	releaseFirst chan struct{}
}

func (*cleanupPastDeadlineNegotiator) Establish(context.Context, handshake.Attempt) error {
	return nil
}

func (negotiator *cleanupPastDeadlineNegotiator) Accept(_ context.Context, _ handshake.Attempt, _ handshake.Signal) error {
	negotiator.mu.Lock()
	negotiator.calls++
	call := negotiator.calls
	negotiator.active++
	if negotiator.active > negotiator.maximum {
		negotiator.maximum = negotiator.active
	}
	negotiator.mu.Unlock()
	negotiator.entered <- call
	defer func() {
		negotiator.mu.Lock()
		negotiator.active--
		negotiator.mu.Unlock()
	}()
	if call == 1 {
		<-negotiator.releaseFirst
		return handshake.ErrFailed
	}
	return nil
}

func (*cleanupPastDeadlineNegotiator) Apply(context.Context, handshake.Attempt, handshake.Signal) error {
	return handshake.ErrStale
}

func (*blockingSignalNegotiator) Establish(context.Context, handshake.Attempt) error { return nil }
func (n *blockingSignalNegotiator) Accept(ctx context.Context, attempt handshake.Attempt, _ handshake.Signal) error {
	n.mu.Lock()
	n.active[attempt.InitiatorID]++
	if total := signalActive(n.active); total > n.maximum {
		n.maximum = total
	}
	n.mu.Unlock()
	select {
	case n.entered <- attempt.InitiatorID:
	default:
	}
	defer func() {
		n.mu.Lock()
		n.active[attempt.InitiatorID]--
		n.mu.Unlock()
	}()
	if n.release == nil {
		return nil
	}
	select {
	case <-n.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*blockingSignalNegotiator) Apply(context.Context, handshake.Attempt, handshake.Signal) error {
	return handshake.ErrStale
}

func signalActive(active map[string]int) int {
	total := 0
	for _, count := range active {
		total += count
	}
	return total
}

type recordingRestartHandler struct {
	entered chan string
	sids    chan string
}

func (h recordingRestartHandler) HandleTransportReplace(ctx context.Context, _ string, signal rank2xmpp.Jingle) error {
	select {
	case h.entered <- signal.SID:
	case <-ctx.Done():
		return ctx.Err()
	}
	if h.sids != nil {
		h.sids <- signal.SID
	}
	return nil
}

func groupBJingle(action, from, to, sid string) rank2xmpp.Jingle {
	return rank2xmpp.Jingle{XMLName: xml.Name{Space: rank2xmpp.JingleNamespace, Local: "jingle"}, Action: action, Initiator: from, Responder: to, SID: sid, Content: rank2xmpp.JingleContent{Creator: "initiator", Name: "data", Description: rank2xmpp.DataChannelDescription{XMLName: xml.Name{Space: rank2xmpp.AZTMDataChannelNamespace, Local: "description"}, Media: "application", MeshID: "mesh", MaximumMessageSize: 1024}, Transport: rank2xmpp.ICETransport{XMLName: xml.Name{Space: rank2xmpp.ICEUDPNamespace, Local: "transport"}, Ufrag: "ufrag", Password: "password", Candidates: []rank2xmpp.ICECandidate{{Component: 1, Foundation: "1", ID: "candidate", IP: "127.0.0.1", Port: 9999, Priority: 1, Protocol: "udp", Type: "host"}}, Fingerprint: rank2xmpp.DTLSFingerprint{XMLName: xml.Name{Space: rank2xmpp.DTLSNamespace, Local: "fingerprint"}, Hash: "sha-256", Setup: "actpass", Value: "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"}, SCTP: rank2xmpp.SCTPMap{XMLName: xml.Name{Space: rank2xmpp.SCTPNamespace, Local: "sctpmap"}, Number: 5000, Protocol: "webrtc-datachannel", Streams: 16}}}}
}

func groupBHandshakeID(seed byte) string {
	return "hsk_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{seed}, 16))
}

func groupBSignalPump(t *testing.T, workers, queue int, negotiator handshake.Negotiator, restarts RestartSignalHandler) (*SignalPump, *controlledJingleSource) {
	t.Helper()
	clock := transport.ClockFunc(func() time.Time { return time.Now().UTC() })
	manager, err := handshake.NewManager(handshake.Config{LocalIdentity: "local/mesh", MaximumPeers: 64, MaximumAttempts: 64, AttemptTimeout: time.Second, CooldownInitial: time.Millisecond, CooldownMaximum: time.Millisecond, Clock: clock, Random: bytes.NewReader(bytes.Repeat([]byte{1}, 4096))}, negotiator)
	if err != nil {
		t.Fatal(err)
	}
	source := &controlledJingleSource{items: make(chan sourceJingle)}
	pump, err := NewSignalPump(source, manager, restarts, time.Second, clock, SignalExecutorConfig{Workers: workers, QueueCapacity: queue, QueueBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return pump, source
}

func TestSignalPumpBlockedHandshakeDoesNotStopDrain(t *testing.T) {
	release := make(chan struct{})
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: release}
	pump, source := groupBSignalPump(t, 2, 4, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	probe, ok := pump.makeJob("a/mesh", groupBJingle("session-initiate", "a/mesh", "local/mesh", groupBHandshakeID(1)))
	if !ok {
		t.Fatal("valid session signal did not produce a job")
	}
	clearSignalJob(probe)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	source.send(t, "a/mesh", groupBJingle("session-initiate", "a/mesh", "local/mesh", groupBHandshakeID(1)))
	select {
	case <-negotiator.entered:
	case <-time.After(time.Second):
		pump.mu.Lock()
		t.Fatalf("first signal not executed: count=%d sessions=%d active=%d", pump.count, len(pump.pendingSession), len(pump.active))
	}
	source.send(t, "b/mesh", groupBJingle("session-initiate", "b/mesh", "local/mesh", groupBHandshakeID(2)))
	select {
	case peer := <-negotiator.entered:
		if peer != "b/mesh" {
			t.Fatalf("peer=%q", peer)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second peer blocked behind handshake")
	}
	cancel()
	close(release)
	<-result
}

func TestSignalPumpSaturationDoesNotBackpressureRank2Ingress(t *testing.T) {
	release := make(chan struct{})
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: release}
	pump, source := groupBSignalPump(t, 1, 1, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	for i := 0; i < 16; i++ {
		peer := fmt.Sprintf("peer-%d/mesh", i)
		source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", groupBHandshakeID(byte(i+1))))
	}
	pump.mu.Lock()
	count := pump.count
	pump.mu.Unlock()
	if count > 2 {
		t.Fatalf("running+queued=%d", count)
	}
	cancel()
	close(release)
	<-result
}

func TestSignalPumpExecutorConcurrencyIsBounded(t *testing.T) {
	release := make(chan struct{})
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 16), release: release}
	pump, source := groupBSignalPump(t, 2, 8, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	for i := 0; i < 8; i++ {
		peer := fmt.Sprintf("worker-%d/mesh", i)
		source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", groupBHandshakeID(byte(i+1))))
	}
	for i := 0; i < 2; i++ {
		<-negotiator.entered
	}
	negotiator.mu.Lock()
	maximum := negotiator.maximum
	negotiator.mu.Unlock()
	if maximum != 2 {
		t.Fatalf("maximum concurrency=%d", maximum)
	}
	cancel()
	close(release)
	<-result
}

func TestSignalPumpPreservesPerPeerSingleFlightAndNewestRestart(t *testing.T) {
	release := make(chan struct{})
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: release}
	restarts := recordingRestartHandler{entered: make(chan string, 8), sids: make(chan string, 8)}
	pump, source := groupBSignalPump(t, 2, 8, negotiator, restarts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	peer := "peer/mesh"
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", groupBHandshakeID(1)))
	<-negotiator.entered
	oldSID, newSID := groupBHandshakeID(2), groupBHandshakeID(3)
	source.send(t, peer, groupBJingle("transport-replace", peer, "local/mesh", oldSID))
	source.send(t, peer, groupBJingle("transport-replace", peer, "local/mesh", newSID))
	deadline := time.Now().Add(time.Second)
	for {
		pump.mu.Lock()
		queued := pump.pendingRestart[peer]
		queuedPayload := []byte(nil)
		if queued != nil {
			queuedPayload = append(queuedPayload, queued.payload...)
		}
		pump.mu.Unlock()
		queuedSignal, err := rank2xmpp.DecodeJingle(queuedPayload)
		clear(queuedPayload)
		if err == nil && queuedSignal.SID == newSID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("newest restart was not coalesced")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	select {
	case sid := <-restarts.sids:
		if sid != newSID {
			t.Fatalf("restart=%q", sid)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced restart did not run after session")
	}
	cancel()
	<-result
}

func TestSignalPumpBoundsRestartInsideSessionBudget(t *testing.T) {
	pump, _ := groupBSignalPump(t, 1, 4, &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 1), release: make(chan struct{})}, recordingRestartHandler{entered: make(chan string, 1)})
	pump.timeout = 10 * time.Second
	started := pump.clock.Now().UTC()
	restart, ok := pump.makeJob("a/mesh", groupBJingle("transport-replace", "a/mesh", "local/mesh", groupBHandshakeID(1)))
	if !ok {
		t.Fatal("restart job rejected")
	}
	defer clearSignalJob(restart)
	session, ok := pump.makeJob("a/mesh", groupBJingle("session-initiate", "a/mesh", "local/mesh", groupBHandshakeID(2)))
	if !ok {
		t.Fatal("session job rejected")
	}
	defer clearSignalJob(session)
	if got := restart.deadline.Sub(restart.started); got != 5*time.Second {
		t.Fatalf("restart budget=%s", got)
	}
	if got := session.deadline.Sub(session.started); got != 10*time.Second {
		t.Fatalf("session budget=%s", got)
	}
	if restart.started.Before(started) || session.started.Before(started) {
		t.Fatal("job started before authenticated clock sample")
	}
}

func TestSignalPumpQueuesOneSamePeerSessionBehindActive(t *testing.T) {
	release := make(chan struct{})
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: release}
	pump, source := groupBSignalPump(t, 2, 8, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	peer := "retry/mesh"
	firstSID, retrySID := groupBHandshakeID(1), groupBHandshakeID(2)
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", firstSID))
	select {
	case got := <-negotiator.entered:
		if got != peer {
			t.Fatalf("first peer=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first session did not execute")
	}
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", retrySID))

	deadline := time.Now().Add(time.Second)
	for {
		pump.mu.Lock()
		queued := pump.pendingSession[peer]
		active := pump.active[peer]
		pump.mu.Unlock()
		if queued != nil && active != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry session was not retained behind active session")
		}
		time.Sleep(time.Millisecond)
	}
	negotiator.mu.Lock()
	activeForPeer := negotiator.active[peer]
	negotiator.mu.Unlock()
	if activeForPeer != 1 {
		t.Fatalf("same-peer active handshakes=%d", activeForPeer)
	}

	close(release)
	select {
	case got := <-negotiator.entered:
		if got != peer {
			t.Fatalf("retry peer=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("retry session did not execute after active session finished")
	}
	negotiator.mu.Lock()
	maximum := negotiator.maximum
	negotiator.mu.Unlock()
	if maximum != 1 {
		t.Fatalf("same-peer concurrency=%d", maximum)
	}
	cancel()
	<-result
}

func TestSignalPumpQueuedReplacementSurvivesCleanupPastFirstDeadline(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &signalPumpMutableClock{now: base}
	negotiator := &cleanupPastDeadlineNegotiator{entered: make(chan int, 2), releaseFirst: make(chan struct{})}
	manager, err := handshake.NewManager(handshake.Config{
		LocalIdentity: "local/mesh", MaximumPeers: 4, MaximumAttempts: 8,
		AttemptTimeout: time.Second, CooldownInitial: 100 * time.Millisecond, CooldownMaximum: 100 * time.Millisecond,
		Clock: clock, Random: bytes.NewReader(bytes.Repeat([]byte{1}, 512)),
	}, negotiator)
	if err != nil {
		t.Fatal(err)
	}
	source := &controlledJingleSource{items: make(chan sourceJingle)}
	pump, err := NewSignalPump(source, manager, recordingRestartHandler{entered: make(chan string, 1)}, time.Second, clock, SignalExecutorConfig{Workers: 2, QueueCapacity: 8, QueueBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	peer := "retry/mesh"
	firstSID, replacementSID := groupBHandshakeID(21), groupBHandshakeID(22)
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", firstSID))
	select {
	case call := <-negotiator.entered:
		if call != 1 {
			t.Fatalf("first Accept call=%d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("first responder attempt did not enter cleanup")
	}

	// The replacement receives its own original SignalPump timestamps while
	// the first attempt still owns responder cleanup.
	clock.set(base.Add(500 * time.Millisecond))
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", replacementSID))
	deadline := time.Now().Add(time.Second)
	for {
		pump.mu.Lock()
		queued, active := pump.pendingSession[peer], pump.active[peer]
		pump.mu.Unlock()
		if queued != nil && active != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement was not retained by SignalPump")
		}
		time.Sleep(time.Millisecond)
	}

	// Cleanup completes after the first deadline and its cooldown boundary,
	// but before the queued replacement's independently preserved deadline.
	clock.set(base.Add(time.Second + 100*time.Millisecond))
	close(negotiator.releaseFirst)
	select {
	case call := <-negotiator.entered:
		if call != 2 {
			t.Fatalf("replacement Accept call=%d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("queued replacement was rejected before Negotiator.Accept")
	}
	negotiator.mu.Lock()
	calls, maximum := negotiator.calls, negotiator.maximum
	negotiator.mu.Unlock()
	if calls != 2 || maximum != 1 {
		t.Fatalf("Accept calls=%d same-peer concurrency=%d", calls, maximum)
	}
	cancel()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("SignalPump did not join on cancellation")
	}
}

func TestSignalPumpSamePeerPendingSessionPreservesCapacityAndBytes(t *testing.T) {
	pump := &SignalPump{
		config: SignalExecutorConfig{Workers: 1, QueueCapacity: 1, QueueBytes: 1024}, pendingSession: make(map[string]*signalJob),
		pendingRestart: make(map[string]*signalJob), active: make(map[string]uint64), notify: make(chan struct{}, 1),
	}
	peer := "peer/mesh"
	first := &signalJob{class: signalSession, peer: peer, payload: []byte("first"), size: 14}
	pump.admit(first)
	active := pump.next(context.Background())
	retry := &signalJob{class: signalSession, peer: peer, payload: []byte("retry"), size: 14}
	pump.admit(retry)
	third := &signalJob{class: signalSession, peer: peer, payload: []byte("third"), size: 14}
	pump.admit(third)
	other := &signalJob{class: signalSession, peer: "other/mesh", payload: []byte("other"), size: 15}
	pump.admit(other)

	pump.mu.Lock()
	if pump.pendingSession[peer] != retry || pump.count != 2 || pump.bytes != first.size+retry.size || len(pump.sessionOrder) != 1 {
		t.Fatalf("pending=%p want=%p count=%d bytes=%d order=%d", pump.pendingSession[peer], retry, pump.count, pump.bytes, len(pump.sessionOrder))
	}
	pump.mu.Unlock()
	if third.payload != nil || third.token != 0 {
		t.Fatalf("additional same-peer session retained ownership: %#v", third)
	}
	if other.payload != nil || other.token != 0 {
		t.Fatalf("over-capacity session retained ownership: %#v", other)
	}

	blockedCtx, blockedCancel := context.WithCancel(context.Background())
	blockedCancel()
	if got := pump.next(blockedCtx); got != nil {
		t.Fatalf("same-peer session ran concurrently: %p", got)
	}
	pump.finish(active)
	queued := pump.next(context.Background())
	if queued != retry {
		t.Fatalf("next=%p want retry=%p", queued, retry)
	}
	pump.finish(queued)
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if pump.count != 0 || pump.bytes != 0 || len(pump.active) != 0 || len(pump.pendingSession) != 0 || len(pump.sessionOrder) != 0 {
		t.Fatalf("ownership remains: count=%d bytes=%d active=%d pending=%d order=%d", pump.count, pump.bytes, len(pump.active), len(pump.pendingSession), len(pump.sessionOrder))
	}
}

func TestSignalPumpShutdownClearsQueuedSamePeerSession(t *testing.T) {
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: make(chan struct{})}
	pump, source := groupBSignalPump(t, 1, 2, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	peer := "shutdown/mesh"
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", groupBHandshakeID(1)))
	<-negotiator.entered
	source.send(t, peer, groupBJingle("session-initiate", peer, "local/mesh", groupBHandshakeID(2)))

	var queued *signalJob
	deadline := time.Now().Add(time.Second)
	for queued == nil {
		pump.mu.Lock()
		queued = pump.pendingSession[peer]
		pump.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("retry session was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers did not join")
	}
	if queued.payload != nil || queued.token != 0 || queued.size != 0 {
		t.Fatalf("queued ownership not cleared: %#v", queued)
	}
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if pump.count != 0 || pump.bytes != 0 || len(pump.active) != 0 || len(pump.pendingSession) != 0 || len(pump.pendingRestart) != 0 || len(pump.sessionOrder) != 0 || len(pump.restartOrder) != 0 {
		t.Fatalf("shutdown ownership remains: count=%d bytes=%d active=%d sessions=%d restarts=%d session-order=%d restart-order=%d", pump.count, pump.bytes, len(pump.active), len(pump.pendingSession), len(pump.pendingRestart), len(pump.sessionOrder), len(pump.restartOrder))
	}
}

func TestSignalPumpCancellationStopsWorkers(t *testing.T) {
	negotiator := &blockingSignalNegotiator{active: make(map[string]int), entered: make(chan string, 8), release: make(chan struct{})}
	pump, source := groupBSignalPump(t, 1, 2, negotiator, recordingRestartHandler{entered: make(chan string, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pump.Run(ctx) }()
	source.send(t, "peer/mesh", groupBJingle("session-initiate", "peer/mesh", "local/mesh", groupBHandshakeID(1)))
	<-negotiator.entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers did not join")
	}
	pump.mu.Lock()
	count, retained := pump.count, pump.bytes
	pump.mu.Unlock()
	if count != 0 || retained != 0 {
		t.Fatalf("retained count=%d bytes=%d", count, retained)
	}
}

func TestSignalPumpTokenExhaustionPreservesOwnershipAndSingleFlight(t *testing.T) {
	pump := &SignalPump{
		config: SignalExecutorConfig{Workers: 2, QueueCapacity: 4, QueueBytes: 1024}, pendingSession: make(map[string]*signalJob),
		pendingRestart: make(map[string]*signalJob), active: make(map[string]uint64), notify: make(chan struct{}, 1), nextToken: math.MaxUint64 - 1,
	}
	peer := "peer/mesh"
	first := &signalJob{class: signalSession, peer: peer, payload: []byte("first"), size: 14}
	pump.admit(first)
	if first.token != math.MaxUint64 {
		t.Fatalf("last token=%d", first.token)
	}
	active := pump.next(context.Background())
	if active != first || pump.active[peer] != math.MaxUint64 {
		t.Fatalf("active=%p token=%d", active, pump.active[peer])
	}
	trigger := &signalJob{class: signalSession, peer: "other/mesh", payload: []byte("trigger"), size: 18}
	pump.admit(trigger)
	if trigger.payload != nil || trigger.token != 0 {
		t.Fatalf("overflow trigger retained ownership: %#v", trigger)
	}
	rejected := &signalJob{class: signalSession, peer: peer, payload: []byte("rejected"), size: 17}
	pump.admit(rejected)
	if rejected.payload != nil || rejected.token != 0 {
		t.Fatalf("rejected job retained ownership: %#v", rejected)
	}
	pump.mu.Lock()
	if !pump.tokenExhausted || pump.nextToken != math.MaxUint64 || pump.count != 1 || pump.bytes != first.size || pump.pendingSession[peer] != nil {
		t.Fatalf("exhausted=%v token=%d count=%d bytes=%d pending=%v", pump.tokenExhausted, pump.nextToken, pump.count, pump.bytes, pump.pendingSession[peer] != nil)
	}
	pump.mu.Unlock()
	pump.finish(active)
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if pump.count != 0 || pump.bytes != 0 || pump.active[peer] != 0 || !pump.tokenExhausted {
		t.Fatalf("finish count=%d bytes=%d active=%d exhausted=%v", pump.count, pump.bytes, pump.active[peer], pump.tokenExhausted)
	}
}

func TestSignalPumpExhaustedRestartReplacementPreservesQueuedJob(t *testing.T) {
	pump := &SignalPump{
		config: SignalExecutorConfig{Workers: 1, QueueCapacity: 2, QueueBytes: 1024}, pendingSession: make(map[string]*signalJob),
		pendingRestart: make(map[string]*signalJob), active: make(map[string]uint64), notify: make(chan struct{}, 1), nextToken: math.MaxUint64 - 1,
	}
	peer := "peer/mesh"
	queued := &signalJob{class: signalRestart, peer: peer, payload: []byte("queued"), size: 15}
	pump.admit(queued)
	count, retained := pump.count, pump.bytes
	replacement := &signalJob{class: signalRestart, peer: peer, payload: []byte("newest"), size: 15}
	pump.admit(replacement)
	if replacement.payload != nil || replacement.token != 0 {
		t.Fatalf("replacement ownership retained: %#v", replacement)
	}
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if !pump.tokenExhausted || pump.pendingRestart[peer] != queued || queued.token != math.MaxUint64 || pump.count != count || pump.bytes != retained || len(pump.restartOrder) != 1 {
		t.Fatalf("queued=%p want=%p token=%d count=%d/%d bytes=%d/%d order=%d exhausted=%v", pump.pendingRestart[peer], queued, queued.token, pump.count, count, pump.bytes, retained, len(pump.restartOrder), pump.tokenExhausted)
	}
}

type lifecycleConnection struct {
	*fakePC
}

func (*lifecycleConnection) Restart(context.Context) error                         { return nil }
func (*lifecycleConnection) AcceptRestart(context.Context, rank2xmpp.Jingle) error { return nil }

type tokenBoundLifecycleConnection struct {
	*fakePC
	mu       sync.Mutex
	restarts int
	entered  chan struct{}
	release  chan struct{}
}

func (connection *tokenBoundLifecycleConnection) Restart(ctx context.Context) error {
	connection.mu.Lock()
	connection.restarts++
	entered, release := connection.entered, connection.release
	connection.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release == nil {
		return nil
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (connection *tokenBoundLifecycleConnection) AcceptRestart(ctx context.Context, _ rank2xmpp.Jingle) error {
	return connection.Restart(ctx)
}

func groupBAttempt(t *testing.T, peer string, seed byte) handshake.Attempt {
	t.Helper()
	attempt, err := handshake.NewAttempt(peer, "local/mesh", time.Now().UTC(), time.Second, bytes.NewReader(bytes.Repeat([]byte{seed}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = handshake.AttemptRunning
	return attempt
}

func groupBAssembly(t *testing.T, capacity int, connections ...RestartablePeerConnection) (*HandshakeNegotiator, *transport.Manager) {
	t.Helper()
	manager, err := transport.NewLiveManager(max(2, capacity))
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "local/mesh", Manager: manager, Authority: staticRank1Authority(1), Exchange: fakeExchange{}, ICEServers: []ICEServer{{URLs: []string{"turn:example.test"}, Username: "private", Credential: []byte("secret")}}, MaximumMessageBytes: 1024, ReceiveCapacity: capacity, TransferWorkers: 1, TransferQueue: max(1, capacity), SCTPStreams: 8, Clock: transport.ClockFunc(func() time.Time { return time.Now().UTC() })})
	if err != nil {
		t.Fatal(err)
	}
	index := 0
	assembly.newPeer = func(PionConfig, PionNegotiator) (PeerConnection, error) {
		if index >= len(connections) {
			return nil, transport.ErrUnavailable
		}
		connection := connections[index]
		index++
		return connection, nil
	}
	t.Cleanup(func() {
		_ = assembly.Close(context.Background())
		_ = manager.Close(context.Background())
	})
	return assembly, manager
}

func groupBLifecycleConnection() *lifecycleConnection {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 4), state: transport.HealthHealthy, closed: make(chan struct{})}
	return &lifecycleConnection{fakePC: &fakePC{channel: channel, binding: sha256.Sum256([]byte("group-b-lifecycle"))}}
}

func groupBTokenBoundConnection(release chan struct{}) *tokenBoundLifecycleConnection {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 4), state: transport.HealthHealthy, closed: make(chan struct{})}
	return &tokenBoundLifecycleConnection{
		fakePC:  &fakePC{channel: channel, binding: sha256.Sum256([]byte("group-b-token-bound"))},
		entered: make(chan struct{}, 1), release: release,
	}
}

func TestTokenBoundRecoveryRejectsReplacementBeforeRefresh(t *testing.T) {
	first, second := groupBTokenBoundConnection(nil), groupBTokenBoundConnection(nil)
	assembly, manager := groupBAssembly(t, 2, first, second)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	observed, ok := manager.LivePeer("peer/mesh")
	if !ok {
		t.Fatal("initial live peer missing")
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 2)); err != nil {
		t.Fatal(err)
	}
	if err := assembly.RecoverInPlace(context.Background(), "peer/mesh", observed); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("stale token recovery=%v", err)
	}
	second.mu.Lock()
	restarts := second.restarts
	second.mu.Unlock()
	if restarts != 0 {
		t.Fatalf("stale progress restarted replacement %d times", restarts)
	}
}

func TestTokenBoundRecoveryRejectsReplacementDuringRefresh(t *testing.T) {
	release := make(chan struct{})
	first, second := groupBTokenBoundConnection(release), groupBTokenBoundConnection(nil)
	assembly, manager := groupBAssembly(t, 2, first, second)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	observed, ok := manager.LivePeer("peer/mesh")
	if !ok {
		t.Fatal("initial live peer missing")
	}
	result := make(chan error, 1)
	go func() { result <- assembly.RecoverInPlace(context.Background(), "peer/mesh", observed) }()
	select {
	case <-first.entered:
	case <-time.After(time.Second):
		t.Fatal("token-bound restart did not begin")
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 2)); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-result:
		if !errors.Is(err, transport.ErrUnavailable) {
			t.Fatalf("replaced refresh=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replaced refresh did not finish")
	}
	second.mu.Lock()
	restarts := second.restarts
	second.mu.Unlock()
	if restarts != 0 {
		t.Fatalf("old refresh mutated replacement %d times", restarts)
	}
}

func TestTokenBoundRecoveryCancellationCannotPublishSuccess(t *testing.T) {
	release := make(chan struct{})
	connection := groupBTokenBoundConnection(release)
	assembly, manager := groupBAssembly(t, 2, connection)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	observed, ok := manager.LivePeer("peer/mesh")
	if !ok {
		t.Fatal("initial live peer missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- assembly.RecoverInPlace(ctx, "peer/mesh", observed) }()
	select {
	case <-connection.entered:
	case <-time.After(time.Second):
		t.Fatal("token-bound restart did not begin")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled refresh=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled refresh did not finish")
	}
	if !manager.IsLivePeer("peer/mesh", observed) {
		t.Fatal("cancellation replaced the exact live token")
	}
}

func TestHandshakeRestartSessionRetiresWithExactLinkToken(t *testing.T) {
	first, second := groupBLifecycleConnection(), groupBLifecycleConnection()
	assembly, manager := groupBAssembly(t, 2, first, second)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	assembly.mu.Lock()
	oldToken := assembly.sessions["peer/mesh"].token
	assembly.mu.Unlock()
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "peer/mesh", 2)); err != nil {
		t.Fatal(err)
	}
	assembly.mu.Lock()
	newToken := assembly.sessions["peer/mesh"].token
	assembly.mu.Unlock()
	if oldToken == 0 || newToken == oldToken {
		t.Fatalf("tokens old=%d new=%d", oldToken, newToken)
	}
	assembly.retireRestart("peer/mesh", oldToken)
	assembly.mu.Lock()
	current := assembly.sessions["peer/mesh"]
	assembly.mu.Unlock()
	if current == nil || current.token != newToken {
		t.Fatal("stale retirement removed replacement")
	}
	if err := manager.RemoveLive(context.Background(), "peer/mesh"); err != nil {
		t.Fatal(err)
	}
	if err := assembly.recoverInPlace(context.Background(), "peer/mesh", nil); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("retired recovery=%v", err)
	}
}

func TestHandshakeRestartCapacityFailsThenRecovers(t *testing.T) {
	first, rejected, recovered := groupBLifecycleConnection(), groupBLifecycleConnection(), groupBLifecycleConnection()
	assembly, manager := groupBAssembly(t, 1, first, rejected, recovered)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "first/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "second/mesh", 2)); !errors.Is(err, handshake.ErrFailed) {
		t.Fatalf("capacity=%v", err)
	}
	if _, ok := manager.LivePeer("second/mesh"); ok {
		t.Fatal("capacity failure installed non-restartable link")
	}
	if err := manager.RemoveLive(context.Background(), "first/mesh"); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "second/mesh", 3)); err != nil {
		t.Fatalf("capacity did not recover: %v", err)
	}
}

func TestHandshakeRestartSessionsRetireAcrossAuthorityFence(t *testing.T) {
	connections := []RestartablePeerConnection{groupBLifecycleConnection(), groupBLifecycleConnection(), groupBLifecycleConnection(), groupBLifecycleConnection()}
	assembly, manager := groupBAssembly(t, 2, connections...)
	for index, peer := range []string{"first/mesh", "second/mesh"} {
		if err := assembly.Establish(context.Background(), groupBAttempt(t, peer, byte(index+1))); err != nil {
			t.Fatal(err)
		}
	}
	epoch := manager.BlockLiveAuthority()
	manager.QuarantineLiveAuthority()
	if err := manager.RetireAllLive(context.Background()); err != nil {
		t.Fatal(err)
	}
	assembly.mu.Lock()
	remaining := len(assembly.sessions)
	assembly.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("authority fence left %d sessions", remaining)
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "third/mesh", 3)); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("blocked authority establish=%v", err)
	}
	if err := manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "third/mesh", 3)); err != nil {
		t.Fatalf("current membership establish=%v", err)
	}
	if err := manager.RetireAllLive(context.Background()); err != nil {
		t.Fatal(err)
	}
	assembly.mu.Lock()
	remaining = len(assembly.sessions)
	assembly.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("retire-all left %d sessions", remaining)
	}
}

func TestHandshakeCloseWakesWaitersAndClearsPrivateProfile(t *testing.T) {
	connection := groupBLifecycleConnection()
	assembly, _ := groupBAssembly(t, 1, connection)
	if err := assembly.Establish(context.Background(), groupBAttempt(t, "a/mesh", 1)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- assembly.recoverInPlace(context.Background(), "a/mesh", nil) }()
	deadline := time.Now().Add(time.Second)
	for {
		assembly.mu.Lock()
		waiters := assembly.sessions["a/mesh"].waiters
		assembly.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, transport.ErrUnavailable) {
			t.Fatalf("waiter=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retirement did not wake waiter")
	}
	assembly.mu.Lock()
	sessions, servers := len(assembly.sessions), len(assembly.config.ICEServers)
	assembly.mu.Unlock()
	if sessions != 0 || servers != 0 {
		t.Fatalf("sessions=%d servers=%d", sessions, servers)
	}
}
