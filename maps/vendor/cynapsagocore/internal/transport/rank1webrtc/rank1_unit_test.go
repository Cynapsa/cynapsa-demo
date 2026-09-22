package rank1webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/loopback"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

var rank1TestClock = transport.ClockFunc(func() time.Time {
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
})

type assemblyDurable struct{ done chan struct{} }

type staticRank1Authority uint64

func (authority staticRank1Authority) AuthorizeRank1(context.Context, string) error {
	if authority == 0 {
		return transport.ErrUnavailable
	}
	return nil
}

type sequenceRank1Authority struct {
	mu     sync.Mutex
	values []uint64
}

func (authority *sequenceRank1Authority) AuthorizeRank1(context.Context, string) error {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if len(authority.values) == 0 {
		return transport.ErrUnavailable
	}
	value := authority.values[0]
	authority.values = authority.values[1:]
	if value == 0 {
		return transport.ErrUnavailable
	}
	return nil
}

func (d *assemblyDurable) Kind() transport.Kind                          { return transport.KindDurable }
func (d *assemblyDurable) Start(context.Context) error                   { return nil }
func (d *assemblyDurable) Send(context.Context, protocol.Envelope) error { return nil }
func (d *assemblyDurable) Receive(ctx context.Context) (protocol.Envelope, error) {
	select {
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	case <-d.done:
		return protocol.Envelope{}, transport.ErrClosed
	}
}
func (d *assemblyDurable) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (d *assemblyDurable) Close(context.Context) error {
	select {
	case <-d.done:
	default:
		close(d.done)
	}
	return nil
}

type fakeChannel struct {
	maximum   int
	inbound   chan Frame
	mu        sync.Mutex
	sent      []Frame
	state     transport.HealthState
	progress  time.Time
	closed    chan struct{}
	sentNote  chan Frame
	restartOK bool
	once      sync.Once
}

func (c *fakeChannel) MaximumFrameBytes() int { return c.maximum }
func (c *fakeChannel) Send(ctx context.Context, f Frame) error {
	if !validFrame(f, c.maximum) {
		return transport.ErrProtocol
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	c.sent = append(c.sent, f.Clone())
	c.mu.Unlock()
	if c.sentNote != nil {
		select {
		case c.sentNote <- f.Clone():
		default:
		}
	}
	return nil
}
func (c *fakeChannel) Receive(ctx context.Context) (Frame, error) {
	select {
	case f := <-c.inbound:
		return f.Clone(), nil
	case <-c.closed:
		return Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}
func (c *fakeChannel) Observe() transport.Observation {
	return transport.Observation{State: c.state, LastProgress: c.progress}
}
func (c *fakeChannel) restartable() bool { return c.restartOK }

func TestLiveHealthRequiresPeerConfirmedProgress(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: transport.ClockFunc(func() time.Time { return now })}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer link.Close(context.Background())
	if got := link.Observe().State; got != transport.HealthConnecting {
		t.Fatalf("unconfirmed state=%v", got)
	}
	channel.inbound <- Frame{Kind: FrameHealthProbe, ProbeNonce: make([]byte, 16)}
	deadline := time.Now().Add(time.Second)
	for link.Observe().State != transport.HealthHealthy && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := link.Observe().State; got != transport.HealthHealthy {
		t.Fatalf("confirmed state=%v", got)
	}
}

func TestLiveHealthNoProgressBoundary(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, progress: now, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: transport.ClockFunc(func() time.Time { return now })}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer link.Close(context.Background())
	now = now.Add(2 * HealthProbeInterval)
	if got := link.Observe().State; got != transport.HealthHealthy {
		t.Fatalf("two-interval boundary=%v", got)
	}
	now = now.Add(time.Nanosecond)
	if got := link.Observe().State; got != transport.HealthDisconnected {
		t.Fatalf("boundary=%v", got)
	}
}
func (c *fakeChannel) Close(context.Context) error { c.once.Do(func() { close(c.closed) }); return nil }

type fakePC struct {
	channel *fakeChannel
	config  DataChannelConfig
	binding [sha256.Size]byte
}

func (p *fakePC) OpenDataChannel(_ context.Context, c DataChannelConfig) (DataChannel, error) {
	p.config = c
	return p.channel, nil
}
func (p *fakePC) ChannelBinding() ([sha256.Size]byte, bool) {
	if p.binding != [sha256.Size]byte{} {
		return p.binding, true
	}
	return sha256.Sum256([]byte("rank1-test-binding")), true
}
func (p *fakePC) Close(context.Context) error { return nil }

type fakeRestartablePC struct{ *fakePC }

func (*fakeRestartablePC) Restart(context.Context) error                         { return nil }
func (*fakeRestartablePC) AcceptRestart(context.Context, rank2xmpp.Jingle) error { return nil }

func TestLinkRestartEligibilityRequiresRestartCapableNonterminalPath(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	for _, test := range []struct {
		name              string
		state             transport.HealthState
		connectionCapable bool
		channelCapable    bool
		want              bool
	}{
		{name: "healthy restartable", state: transport.HealthHealthy, connectionCapable: true, want: true},
		{name: "disconnected restartable", state: transport.HealthDisconnected, connectionCapable: true, want: true},
		{name: "ICE failed but nonterminal", state: transport.HealthFailed, connectionCapable: true, channelCapable: true, want: true},
		{name: "hard channel failure", state: transport.HealthFailed, connectionCapable: true},
		{name: "closed", state: transport.HealthClosed, connectionCapable: true, channelCapable: true},
		{name: "connection cannot restart", state: transport.HealthDisconnected},
	} {
		t.Run(test.name, func(t *testing.T) {
			channel := &fakeChannel{
				maximum: 1024, inbound: make(chan Frame, 1), state: test.state,
				progress: now, closed: make(chan struct{}), restartOK: test.channelCapable,
			}
			base := &fakePC{channel: channel}
			var connection PeerConnection = base
			if test.connectionCapable {
				connection = &fakeRestartablePC{fakePC: base}
			}
			link, err := NewLink(Config{
				MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024,
				ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1,
				Clock: transport.ClockFunc(func() time.Time { return now }),
			}, connection, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = link.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := link.RestartEligible(); got != test.want {
				t.Fatalf("RestartEligible()=%v want %v", got, test.want)
			}
			if err = link.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type blockingPC struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (p *blockingPC) OpenDataChannel(ctx context.Context, _ DataChannelConfig) (DataChannel, error) {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingPC) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("rank1-test-binding")), true
}

func (p *blockingPC) Close(context.Context) error {
	select {
	case <-p.closed:
	default:
		close(p.closed)
	}
	return nil
}

func liveEnvelope(t *testing.T, sender, recipient string) protocol.Envelope {
	t.Helper()
	payload, _ := protocol.NewInlinePayload("native", []byte("x"))
	h := sha256.Sum256([]byte("conv"))
	e, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(h[:]), Sender: sender, Recipient: recipient, MeshID: "mesh", Mode: protocol.ModeMessage, CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestLinkUsesExplicitReliableUnorderedChannelAndEffectiveCap(t *testing.T) {
	channel := &fakeChannel{maximum: 512, inbound: make(chan Frame, 2), state: transport.HealthHealthy, closed: make(chan struct{})}
	pc := &fakePC{channel: channel}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, Clock: rank1TestClock}, pc, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pc.config.Label != "aztm" || pc.config.Ordered || pc.config.MaximumRetransmits != nil || pc.config.MaximumLifetimeMS != nil {
		t.Fatalf("channel config=%#v", pc.config)
	}
	if link.config.MaximumFrameBytes != 512 {
		t.Fatalf("effective cap=%d", link.config.MaximumFrameBytes)
	}
	e := liveEnvelope(t, "a", "b")
	if err = link.Send(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	sent := append([]Frame(nil), channel.sent...)
	channel.mu.Unlock()
	if len(sent) != 1 || sent[0].Kind != FrameEnvelope {
		t.Fatalf("sent=%#v", sent)
	}
	staleOutbound := e.Clone()
	staleOutbound.Recipient = "other"
	if err = link.Send(context.Background(), staleOutbound); !errors.Is(err, transport.ErrInvalidEnvelope) {
		t.Fatalf("channel-binding recipient mismatch accepted: %v", err)
	}
	codec, _ := protocol.NewCodec()
	incoming := liveEnvelope(t, "b", "a")
	staleIncoming := incoming.Clone()
	staleIncoming.Sender = "other"
	staleEncoded, _ := codec.Encode(staleIncoming)
	channel.inbound <- Frame{Kind: FrameEnvelope, Data: staleEncoded}
	encoded, _ := codec.Encode(incoming)
	channel.inbound <- Frame{Kind: FrameEnvelope, Data: encoded}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := link.ReceiveAuthenticated(ctx)
	wantBinding := sha256.Sum256([]byte("rank1-test-binding"))
	if err != nil || got.Envelope.MessageID != incoming.MessageID || got.Authentication.Peer != "b" || got.Authentication.MeshID != "mesh" || got.Authentication.ChannelBinding != wantBinding {
		t.Fatalf("receive=%#v %v", got, err)
	}
	if err = link.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRank1TrackedReceiptIsExactSessionBoundAndDuplicateSafe(t *testing.T) {
	channel := &fakeChannel{maximum: 2048, inbound: make(chan Frame, 4), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 2048, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	envelope := liveEnvelope(t, "a", "b")
	pending, err := link.SendTracked(context.Background(), envelope)
	if err != nil || pending == nil {
		t.Fatalf("tracked=%#v err=%v", pending, err)
	}
	binding := pending.Binding()
	wrong := binding
	wrong.MessageID = "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 16))
	wrong.ChannelBinding = [sha256.Size]byte{}
	channel.inbound <- Frame{Kind: FrameEnvelopeReceipt, Receipt: wrong}
	select {
	case <-pending.Done():
		t.Fatal("wrong receipt completed pending ownership")
	case <-time.After(10 * time.Millisecond):
	}
	exact := binding
	exact.ChannelBinding = [sha256.Size]byte{}
	channel.inbound <- Frame{Kind: FrameEnvelopeReceipt, Receipt: exact}
	select {
	case accepted := <-pending.Done():
		if !accepted {
			t.Fatal("exact receipt was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("exact receipt did not complete")
	}
	// Duplicate and late receipts find no registration and remain inert.
	channel.inbound <- Frame{Kind: FrameEnvelopeReceipt, Receipt: exact}
	pending.Close()
}

func TestRank1TrackedReceiptFailsOnAuthorityLoss(t *testing.T) {
	channel := &fakeChannel{maximum: 2048, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 2048, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err := link.SendTracked(context.Background(), liveEnvelope(t, "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	link.BlockLiveAuthority()
	select {
	case accepted := <-pending.Done():
		if accepted {
			t.Fatal("authority loss authenticated a receipt")
		}
	case <-time.After(time.Second):
		t.Fatal("authority loss did not release pending receipt")
	}
	_ = link.Close(context.Background())
}

func TestRank1ReceiptSendRequiresExactInboundSessionBinding(t *testing.T) {
	channel := &fakeChannel{maximum: 2048, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "b", PeerID: "a", MaximumFrameBytes: 2048, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = link.Close(context.Background()) }()
	binding := sha256.Sum256([]byte("rank1-test-binding"))
	receipt := receiptForEnvelope(liveEnvelope(t, "a", "b"), binding)
	wrong := receipt
	wrong.ChannelBinding = sha256.Sum256([]byte("other-session"))
	if err = link.SendReceipt(context.Background(), wrong); !errors.Is(err, transport.ErrAuthentication) {
		t.Fatalf("wrong session=%v", err)
	}
	if err = link.SendReceipt(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	sent := append([]Frame(nil), channel.sent...)
	channel.mu.Unlock()
	if len(sent) != 1 || sent[0].Kind != FrameEnvelopeReceipt || sent[0].Receipt.ChannelBinding != [sha256.Size]byte{} || sent[0].Receipt.MessageID != receipt.MessageID {
		t.Fatalf("sent=%#v", sent)
	}
}

func TestLinkCloseCancelsStartWithoutWaitingForLifecycleLock(t *testing.T) {
	connection := &blockingPC{entered: make(chan struct{}), closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: rank1TestClock}, connection, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- link.Start(context.Background()) }()
	<-connection.entered
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := link.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := <-startResult; !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Start() = %v", err)
	}
	select {
	case <-connection.closed:
	default:
		t.Fatal("peer connection was not closed")
	}
}

type fakeExchange struct{}

func (fakeExchange) ExchangeJingle(context.Context, string, rank2xmpp.Jingle) (rank2xmpp.Jingle, error) {
	return rank2xmpp.Jingle{}, errors.New("unused")
}
func (fakeExchange) SendJingle(context.Context, string, rank2xmpp.Jingle) error {
	return errors.New("unused")
}

func TestJingleMaximumNegotiationFailsClosed(t *testing.T) {
	n, err := NewJingleNegotiator(JingleNegotiatorConfig{LocalIdentity: "a/mesh", PeerIdentity: "b/mesh", MeshID: "mesh", SID: "sid", SCTPStreams: 8, MaximumMessageBytes: 1024}, fakeExchange{})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setRemoteMaximum(512); err != nil {
		t.Fatal(err)
	}
	if got := n.EffectiveMaximumFrameBytes(); got != 512 {
		t.Fatalf("effective=%d", got)
	}
	if err = n.setRemoteMaximum(0); !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("missing=%v", err)
	}
	if _, err = NewJingleNegotiator(JingleNegotiatorConfig{LocalIdentity: "a/mesh", PeerIdentity: "b/mesh", MeshID: "mesh", SID: "sid", SCTPStreams: 0, MaximumMessageBytes: 1024}, fakeExchange{}); !errors.Is(err, transport.ErrInvalidConfig) {
		t.Fatalf("streams=%v", err)
	}
}

func TestHandshakeAssemblyInstallsIndependentPeerLiveLinks(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(8, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "local/mesh", Manager: manager, Authority: staticRank1Authority(1), Exchange: fakeExchange{}, MaximumMessageBytes: 1024, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	channels := make(map[string]*fakeChannel)
	peers := []string{"agent1/mesh", "agent2/mesh", "agent3/mesh"}
	index := 0
	assembly.newPeer = func(PionConfig, PionNegotiator) (PeerConnection, error) {
		peer := peers[index]
		index++
		channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 4), state: transport.HealthHealthy, closed: make(chan struct{})}
		channels[peer] = channel
		return &fakePC{channel: channel}, nil
	}
	for i, peer := range peers {
		attempt, makeErr := handshake.NewAttempt(peer, "local/mesh", time.Unix(int64(100+i), 0).UTC(), time.Second, bytes.NewReader(bytes.Repeat([]byte{byte(i + 1)}, 16)))
		if makeErr != nil {
			t.Fatal(makeErr)
		}
		attempt.State = handshake.AttemptRunning
		if err = assembly.Establish(context.Background(), attempt); err != nil {
			t.Fatalf("%s: %v", peer, err)
		}
	}
	for _, peer := range peers {
		envelope := liveEnvelope(t, "local/mesh", peer)
		if err = manager.Send(context.Background(), transport.KindLive, envelope); err != nil {
			t.Fatalf("%s: %v", peer, err)
		}
	}
	for _, peer := range peers {
		channels[peer].mu.Lock()
		count := len(channels[peer].sent)
		channels[peer].mu.Unlock()
		if count != 1 {
			t.Fatalf("%s frames=%d", peer, count)
		}
	}
}

func TestHandshakeAssemblyReplacesPeerLinkBeforeRetiringOldConnection(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(2, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "a/mesh", Manager: manager, Authority: staticRank1Authority(1), Exchange: fakeExchange{}, MaximumMessageBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	channels := []*fakeChannel{
		{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthDisconnected, closed: make(chan struct{})},
		{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})},
	}
	connections := []*fakePC{
		{channel: channels[0], binding: sha256.Sum256([]byte("first-link"))},
		{channel: channels[1], binding: sha256.Sum256([]byte("replacement-link"))},
	}
	index := 0
	assembly.newPeer = func(PionConfig, PionNegotiator) (PeerConnection, error) {
		connection := connections[index]
		index++
		return connection, nil
	}
	install := func(seed byte) {
		t.Helper()
		attempt, makeErr := handshake.NewAttempt("b/mesh", "a/mesh", rank1TestClock.Now(), time.Second, bytes.NewReader(bytes.Repeat([]byte{seed}, 16)))
		if makeErr != nil {
			t.Fatal(makeErr)
		}
		attempt.State = handshake.AttemptRunning
		if installErr := assembly.Establish(context.Background(), attempt); installErr != nil {
			t.Fatal(installErr)
		}
	}
	install(1)
	install(2)
	select {
	case <-channels[0].closed:
	case <-time.After(time.Second):
		t.Fatal("superseded connection was not retired")
	}
	select {
	case <-channels[1].closed:
		t.Fatal("replacement was closed with superseded connection")
	default:
	}
	envelope := liveEnvelope(t, "a/mesh", "b/mesh")
	if err = manager.Send(context.Background(), transport.KindLive, envelope); err != nil {
		t.Fatal(err)
	}
	channels[0].mu.Lock()
	oldSends := len(channels[0].sent)
	channels[0].mu.Unlock()
	channels[1].mu.Lock()
	newSends := len(channels[1].sent)
	channels[1].mu.Unlock()
	if oldSends != 0 || newSends != 1 {
		t.Fatalf("old sends=%d replacement sends=%d", oldSends, newSends)
	}
}

func TestHandshakeAssemblyCopiesInjectedICEConfiguration(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(2, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	servers := []ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: []byte("secret")}}
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "a/mesh", Manager: manager, Authority: staticRank1Authority(7), Exchange: fakeExchange{}, ICEServers: servers, ICETransportPolicy: webrtc.ICETransportPolicyRelay, MaximumMessageBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	servers[0].URLs[0] = "mutated"
	var captured PionConfig
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	assembly.newPeer = func(config PionConfig, _ PionNegotiator) (PeerConnection, error) {
		captured = config
		captured.ICEServers = clonePionICEServers(config.ICEServers)
		return &fakePC{channel: channel}, nil
	}
	attempt, err := handshake.NewAttempt("b/mesh", "a/mesh", rank1TestClock.Now(), time.Second, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = handshake.AttemptRunning
	if err = assembly.Establish(context.Background(), attempt); err != nil {
		t.Fatal(err)
	}
	if captured.ICETransportPolicy != webrtc.ICETransportPolicyRelay || len(captured.ICEServers) != 1 || captured.ICEServers[0].URLs[0] != "turn:turn.test:3478?transport=udp" {
		t.Fatalf("captured=%#v", captured)
	}
}

func TestHandshakeAssemblyRejectsMembershipLossBeforeLiveInstall(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(2, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	authority := &sequenceRank1Authority{values: []uint64{1, 0}}
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "a/mesh", Manager: manager, Authority: authority, Exchange: fakeExchange{}, MaximumMessageBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	assembly.newPeer = func(PionConfig, PionNegotiator) (PeerConnection, error) { return &fakePC{channel: channel}, nil }
	attempt, err := handshake.NewAttempt("b/mesh", "a/mesh", rank1TestClock.Now(), time.Second, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = handshake.AttemptRunning
	if err = assembly.Establish(context.Background(), attempt); !errors.Is(err, handshake.ErrFailed) {
		t.Fatalf("membership loss accepted: %v", err)
	}
	if state := manager.ObservePeer(transport.KindLive, "b/mesh").State; state != transport.HealthUnknown {
		t.Fatalf("retired link installed: %v", state)
	}
}

func TestDataOnlySDPConvertsToStructuredJingle(t *testing.T) {
	fingerprint := "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"
	raw := "v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=ice-ufrag:u\r\na=ice-pwd:password\r\na=fingerprint:sha-256 " + fingerprint + "\r\na=setup:actpass\r\na=mid:0\r\na=sctp-port:5000\r\na=candidate:1 1 udp 1 127.0.0.1 9999 typ host generation 0 ufrag u\r\na=end-of-candidates\r\n"
	config := JingleNegotiatorConfig{LocalIdentity: "a/mesh", PeerIdentity: "b/mesh", MeshID: "mesh", SID: "sid", SCTPStreams: 16, MaximumMessageBytes: 65536}
	signal, err := descriptionToJingle(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: raw}, "session-initiate", config)
	if err != nil {
		t.Fatal(err)
	}
	if signal.Content.Description.MaximumMessageSize != 65536 || signal.Content.Transport.SCTP.Streams != 16 {
		t.Fatalf("signal=%#v", signal)
	}
	converted, err := jingleToDescription(signal, webrtc.SDPTypeOffer)
	if err != nil {
		t.Fatal(err)
	}
	if converted.SDP == "" {
		t.Fatal("empty local SDP")
	}
	if !strings.Contains(converted.SDP, " ufrag u") {
		t.Fatalf("candidate ufrag missing from SDP: %q", converted.SDP)
	}
	if _, err := parseCandidate("1 1 udp 1 127.0.0.1 9999 typ host generation 0 ufrag wrong", "u"); !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("mismatched candidate ufrag = %v", err)
	}
}

func TestJingleChannelBindingCoversIdentitiesMeshAndFingerprints(t *testing.T) {
	fingerprintA := "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"
	fingerprintB := "11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11"
	base := func(action, fingerprint, setup string) rank2xmpp.Jingle {
		initiator, responder := "a/mesh", "b/mesh"
		if action == "session-accept" {
			setup = "active"
		}
		return rank2xmpp.Jingle{XMLName: xmlName(rank2xmpp.JingleNamespace, "jingle"), Action: action, Initiator: initiator, Responder: responder, SID: "hsk_0123456789012345678901", Content: rank2xmpp.JingleContent{Creator: "initiator", Name: "data", Description: rank2xmpp.DataChannelDescription{XMLName: xmlName(rank2xmpp.AZTMDataChannelNamespace, "description"), Media: "application", MeshID: "mesh", MaximumMessageSize: 1024}, Transport: rank2xmpp.ICETransport{XMLName: xmlName(rank2xmpp.ICEUDPNamespace, "transport"), Ufrag: "ufrag", Password: "password", Candidates: []rank2xmpp.ICECandidate{{Component: 1, Foundation: "1", ID: "candidate", IP: "127.0.0.1", Port: 9999, Priority: 1, Protocol: "udp", Type: "host"}}, Fingerprint: rank2xmpp.DTLSFingerprint{XMLName: xmlName(rank2xmpp.DTLSNamespace, "fingerprint"), Hash: "sha-256", Setup: setup, Value: fingerprint}, SCTP: rank2xmpp.SCTPMap{XMLName: xmlName(rank2xmpp.SCTPNamespace, "sctpmap"), Number: 5000, Protocol: "webrtc-datachannel", Streams: 16}}}}
	}
	offer, answer := base("session-initiate", fingerprintA, "actpass"), base("session-accept", fingerprintB, "active")
	binding, err := deriveJingleChannelBinding(offer, answer)
	if err != nil || binding == [sha256.Size]byte{} {
		t.Fatalf("binding=%x err=%v", binding, err)
	}
	changed := answer
	changed.Content.Transport.Fingerprint.Value = fingerprintA
	other, err := deriveJingleChannelBinding(offer, changed)
	if err != nil || other == binding {
		t.Fatalf("fingerprint not bound: %x %x err=%v", binding, other, err)
	}
	changed = answer
	changed.Content.Description.MeshID = "other"
	if _, err = deriveJingleChannelBinding(offer, changed); !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("mesh mismatch accepted: %v", err)
	}
}

func TestRealPionGatheredOfferConvertsToStructuredJingle(t *testing.T) {
	settings := webrtc.SettingEngine{}
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})
	peer, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	ordered := false
	if _, err = peer.CreateDataChannel("aztm", &webrtc.DataChannelInit{Ordered: &ordered}); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(5 * time.Second):
		t.Fatal("ICE gathering timeout")
	}
	local := peer.LocalDescription()
	if local == nil {
		t.Fatal("no local description")
	}
	const installationA = "01234567-89ab-4def-8123-456789abcdef"
	const installationB = "11234567-89ab-4def-8123-456789abcdef"
	signal, err := descriptionToJingle(*local, "session-initiate", JingleNegotiatorConfig{
		LocalIdentity: "a@example.test/r2." + installationA + ".AAAAAAAAAAAAAAAA",
		PeerIdentity:  "b@example.test/r2." + installationB + ".BBBBBBBBBBBBBBBB",
		MeshID:        "mesh", SID: "sid", SCTPStreams: 8, MaximumMessageBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(signal.Content.Transport.Candidates) == 0 {
		t.Fatal("no gathered candidates")
	}
	converted, err := jingleToDescription(signal, webrtc.SDPTypeOffer)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(converted.SDP, " ufrag "+signal.Content.Transport.Ufrag) {
		t.Fatalf("candidate ufrag missing: %q", converted.SDP)
	}
}

func TestDirectPayloadCarrierWaitsForAuthenticatedCompletion(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 8), state: transport.HealthHealthy, closed: make(chan struct{})}
	pc := &fakePC{channel: channel}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 4, Clock: rank1TestClock}, pc, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer link.Close(context.Background())
	carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 512, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	messageID := "msg_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	route := payload.CarrierRoute{PeerID: "b", MeshID: "mesh", SenderID: "a", RecipientID: "b", MessageID: messageID}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: make([]byte, 513)}); !errors.Is(err, payload.ErrFrameTooLarge) {
		t.Fatalf("encoded boundary=%v", err)
	}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
		t.Fatal(err)
	}
	if err = carrier.SendChunk(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("chunk")}); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("payload"))
	result := make(chan payload.CompletionEvidence, 1)
	failure := make(chan error, 1)
	go func() { e, err := carrier.Finish(context.Background(), route, transferID); result <- e; failure <- err }()
	channel.inbound <- Frame{Kind: FrameTransferCompletion, TransferID: transferID, Evidence: payload.CompletionEvidence{TransferID: transferID, MessageID: messageID, Digest: digest}}
	select {
	case evidence := <-result:
		if err := <-failure; err != nil || evidence.Digest != digest {
			t.Fatalf("completion=%#v %v", evidence, err)
		}
	case <-time.After(time.Second):
		t.Fatal("finish did not complete")
	}
}

func TestDirectPayloadFinishCancellationAndAbortReleaseWaiter(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(context.CancelFunc, *PayloadTransfer, payload.CarrierRoute, string) error
		want error
	}{
		{
			name: "caller cancellation",
			stop: func(cancel context.CancelFunc, _ *PayloadTransfer, _ payload.CarrierRoute, _ string) error {
				cancel()
				return nil
			},
			want: context.Canceled,
		},
		{
			name: "explicit abort",
			stop: func(_ context.CancelFunc, carrier *PayloadTransfer, route payload.CarrierRoute, transferID string) error {
				return carrier.Abort(context.Background(), route, transferID)
			},
			want: payload.ErrCarrierRejected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 8), sentNote: make(chan Frame, 16), state: transport.HealthHealthy, closed: make(chan struct{})}
			link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 4, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := link.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer link.Close(context.Background())
			carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 512, InFlightChunks: 1, MaximumTransfers: 1})
			if err != nil {
				t.Fatal(err)
			}
			transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{3}, 16))
			messageID := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{4}, 16))
			route := payload.CarrierRoute{PeerID: "b", MeshID: "mesh", SenderID: "a", RecipientID: "b", MessageID: messageID}
			if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
				t.Fatal(err)
			}
			finishCtx, cancel := context.WithCancel(context.Background())
			finishResult := make(chan error, 1)
			go func() {
				_, finishErr := carrier.Finish(finishCtx, route, transferID)
				finishResult <- finishErr
			}()
			for {
				select {
				case sent := <-channel.sentNote:
					if sent.Kind == FrameTransferFinish {
						goto finishSent
					}
				case <-time.After(time.Second):
					t.Fatal("finish frame was not sent")
				}
			}
		finishSent:
			if err := test.stop(cancel, carrier, route, transferID); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finishResult:
				if !errors.Is(err, test.want) {
					t.Fatalf("Finish() = %v, want %v", err, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("Finish waiter was not released")
			}
			cancel()
			if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("retry")}); err != nil {
				t.Fatalf("transfer state was not released: %v", err)
			}
		})
	}
}

func TestDataChannelConfigurationRejectsReliabilityDefaults(t *testing.T) {
	valid := DataChannelConfig{Label: "aztm", Ordered: false, MaximumFrameBytes: 1024}
	if !valid.valid() {
		t.Fatal("explicit reliable unordered config rejected")
	}
	retries := uint16(1)
	valid.MaximumRetransmits = &retries
	if valid.valid() {
		t.Fatal("partial reliability accepted")
	}
}

func TestIncomingDataChannelMustMatchExactAZTMProfile(t *testing.T) {
	peer, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	ordered := false
	negotiated := false
	protocolName := ""
	valid, err := peer.CreateDataChannel("aztm", &webrtc.DataChannelInit{Ordered: &ordered, Negotiated: &negotiated, Protocol: &protocolName})
	if err != nil {
		t.Fatal(err)
	}
	config := DataChannelConfig{Label: "aztm", Ordered: false, MaximumFrameBytes: 1024}
	if !exactDataChannelProfile(valid, config) {
		t.Fatal("exact reliable-unordered channel was rejected")
	}
	wrong, err := peer.CreateDataChannel("aztm", nil)
	if err != nil {
		t.Fatal(err)
	}
	if exactDataChannelProfile(wrong, config) {
		t.Fatal("ordered dependency-default channel was accepted")
	}
}

func TestHandshakeAssemblyInstallsIsolatedPeerLinks(t *testing.T) {
	durable, _, err := loopback.NewPair(8, transport.KindDurable)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := transport.NewManager(8, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: "a/mesh", Manager: manager, Authority: staticRank1Authority(1), Exchange: fakeExchange{}, MaximumMessageBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	channels := make(map[string]*fakeChannel)
	peers := []string{"b/mesh", "c/mesh"}
	created := 0
	assembly.newPeer = func(_ PionConfig, negotiator PionNegotiator) (PeerConnection, error) {
		peer := peers[created]
		created++
		if negotiator == nil {
			t.Fatal("nil Jingle negotiator")
		}
		channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 2), state: transport.HealthHealthy, closed: make(chan struct{})}
		channels[peer] = channel
		return &fakePC{channel: channel}, nil
	}
	now := time.Unix(500, 0).UTC()
	for i, peer := range peers {
		attempt := handshake.Attempt{ID: fmt.Sprintf("hsk_%022d", i), PeerID: peer, InitiatorID: "a/mesh", StartedAt: now, Deadline: now.Add(time.Second), State: handshake.AttemptRunning}
		if err := assembly.Establish(context.Background(), attempt); err != nil {
			t.Fatalf("Establish(%s) = %v", peer, err)
		}
	}
	for _, peer := range peers {
		if err := manager.Send(context.Background(), transport.KindLive, liveEnvelope(t, "a/mesh", peer)); err != nil {
			t.Fatalf("Send(%s) = %v", peer, err)
		}
	}
	for peer, channel := range channels {
		channel.mu.Lock()
		sent := append([]Frame(nil), channel.sent...)
		channel.mu.Unlock()
		if len(sent) != 1 || sent[0].Kind != FrameEnvelope {
			t.Fatalf("peer %s received %#v", peer, sent)
		}
	}
}

type deadlineChannel struct {
	writeStarted chan struct{}
	unblock      chan struct{}
	startOnce    sync.Once
	unblockOnce  sync.Once
}

func (d *deadlineChannel) Read([]byte) (int, error)  { return 0, errors.New("unused") }
func (d *deadlineChannel) Write([]byte) (int, error) { return 0, errors.New("unused") }
func (d *deadlineChannel) ReadDataChannel([]byte) (int, bool, error) {
	return 0, false, errors.New("unused")
}
func (d *deadlineChannel) WriteDataChannel([]byte, bool) (int, error) {
	d.startOnce.Do(func() { close(d.writeStarted) })
	<-d.unblock
	return 0, errors.New("write interrupted")
}
func (d *deadlineChannel) Close() error {
	d.unblockOnce.Do(func() { close(d.unblock) })
	return nil
}
func (d *deadlineChannel) SetReadDeadline(time.Time) error { return nil }
func (d *deadlineChannel) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		d.unblockOnce.Do(func() { close(d.unblock) })
	}
	return nil
}

func TestPionSendCancellationInterruptsDetachedWrite(t *testing.T) {
	detached := &deadlineChannel{writeStarted: make(chan struct{}), unblock: make(chan struct{})}
	channel := &pionDataChannel{maximum: 1024, detached: detached, done: make(chan struct{}), receive: make(chan Frame, 1), state: transport.HealthHealthy}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- channel.Send(ctx, Frame{Kind: FrameHealthProbe, ProbeNonce: make([]byte, 16)}) }()
	<-detached.writeStarted
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not honor cancellation")
	}
}

func TestPionTerminalStateCannotBeRevivedByLateConnectedCallback(t *testing.T) {
	channel := &pionDataChannel{maximum: 1024, detached: &deadlineChannel{writeStarted: make(chan struct{}), unblock: make(chan struct{})}, done: make(chan struct{}), receive: make(chan Frame, 1), state: transport.HealthHealthy}
	channel.fail()
	if channel.restartable() {
		t.Fatal("terminal data channel remained restartable")
	}
	channel.updatePeerState(webrtc.PeerConnectionStateConnected)
	if got := channel.Observe().State; got != transport.HealthFailed {
		t.Fatalf("late connected revived terminal state: %v", got)
	}
	if err := channel.Send(context.Background(), Frame{Kind: FrameHealthProbe, ProbeNonce: make([]byte, 16)}); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("terminal Send()=%v", err)
	}
}

func TestPionDetachedHealthyStateCannotBeDowngradedByLatePreConnectedCallback(t *testing.T) {
	for _, state := range []webrtc.PeerConnectionState{
		webrtc.PeerConnectionStateNew,
		webrtc.PeerConnectionStateConnecting,
	} {
		t.Run(state.String(), func(t *testing.T) {
			current := webrtc.PeerConnectionStateConnected
			channel := &pionDataChannel{
				maximum: 1024,
				detached: &deadlineChannel{
					writeStarted: make(chan struct{}),
					unblock:      make(chan struct{}),
				},
				done:    make(chan struct{}),
				receive: make(chan Frame, 1),
				state:   transport.HealthHealthy,
				peerState: func() webrtc.PeerConnectionState {
					return current
				},
			}

			channel.updatePeerState(state)
			if got := channel.Observe().State; got != transport.HealthHealthy {
				t.Fatalf("late %s downgraded detached healthy channel: %v", state, got)
			}
		})
	}
}

func TestPionDetachedChannelPreservesRestartStateTransitions(t *testing.T) {
	current := webrtc.PeerConnectionStateConnected
	channel := &pionDataChannel{
		maximum: 1024,
		detached: &deadlineChannel{
			writeStarted: make(chan struct{}),
			unblock:      make(chan struct{}),
		},
		done:    make(chan struct{}),
		receive: make(chan Frame, 1),
		state:   transport.HealthHealthy,
		peerState: func() webrtc.PeerConnectionState {
			return current
		},
	}

	current = webrtc.PeerConnectionStateConnecting
	channel.updatePeerState(webrtc.PeerConnectionStateConnecting)
	if got := channel.Observe().State; got != transport.HealthConnecting {
		t.Fatalf("direct restart connecting state=%v", got)
	}
	current = webrtc.PeerConnectionStateConnected
	channel.updatePeerState(webrtc.PeerConnectionStateConnected)
	if got := channel.Observe().State; got != transport.HealthHealthy {
		t.Fatalf("successful direct restart state=%v", got)
	}

	current = webrtc.PeerConnectionStateDisconnected
	channel.updatePeerState(webrtc.PeerConnectionStateDisconnected)
	if got := channel.Observe().State; got != transport.HealthDisconnected {
		t.Fatalf("disconnected state=%v", got)
	}
	current = webrtc.PeerConnectionStateConnecting
	channel.updatePeerState(webrtc.PeerConnectionStateConnecting)
	if got := channel.Observe().State; got != transport.HealthConnecting {
		t.Fatalf("restart connecting state=%v", got)
	}
	current = webrtc.PeerConnectionStateConnected
	channel.updatePeerState(webrtc.PeerConnectionStateConnected)
	if got := channel.Observe().State; got != transport.HealthHealthy {
		t.Fatalf("successful restart state=%v", got)
	}
	current = webrtc.PeerConnectionStateFailed
	channel.updatePeerState(webrtc.PeerConnectionStateFailed)
	if got := channel.Observe().State; got != transport.HealthFailed {
		t.Fatalf("failed state=%v", got)
	}
}

func TestPionStalePreConnectedCallbackCannotHideCurrentOutage(t *testing.T) {
	current := webrtc.PeerConnectionStateDisconnected
	channel := &pionDataChannel{
		maximum: 1024,
		detached: &deadlineChannel{
			writeStarted: make(chan struct{}),
			unblock:      make(chan struct{}),
		},
		done: make(chan struct{}), receive: make(chan Frame, 1),
		state: transport.HealthHealthy,
		peerState: func() webrtc.PeerConnectionState {
			return current
		},
	}

	channel.updatePeerState(webrtc.PeerConnectionStateConnecting)
	if got := channel.Observe().State; got != transport.HealthDisconnected {
		t.Fatalf("stale connecting callback hid current outage: %v", got)
	}
}

func TestPionICEFailureRemainsRestartableUntilDataChannelFails(t *testing.T) {
	channel := &pionDataChannel{maximum: 1024, detached: &deadlineChannel{writeStarted: make(chan struct{}), unblock: make(chan struct{})}, done: make(chan struct{}), receive: make(chan Frame, 1), state: transport.HealthHealthy}
	channel.updatePeerState(webrtc.PeerConnectionStateFailed)
	if !channel.restartable() {
		t.Fatal("ICE-only failure incorrectly retired data channel")
	}
	if got := channel.Observe().State; got != transport.HealthFailed {
		t.Fatalf("failed state=%v", got)
	}
	channel.updatePeerState(webrtc.PeerConnectionStateConnected)
	if got := channel.Observe().State; got != transport.HealthHealthy {
		t.Fatalf("restart did not revive nonterminal ICE failure: %v", got)
	}
}
