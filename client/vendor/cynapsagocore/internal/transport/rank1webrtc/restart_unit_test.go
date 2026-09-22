package rank1webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

type restartTestConnection struct {
	mu       sync.Mutex
	restarts int
	accepts  int
	release  chan struct{}
}

type restartConfigurationSource struct {
	mu      sync.Mutex
	configs []ICEConfiguration
	err     error
	calls   int
}

func (source *restartConfigurationSource) ResolveICEConfiguration(context.Context) (ICEConfiguration, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	if source.err != nil || len(source.configs) == 0 {
		return ICEConfiguration{}, source.err
	}
	config := source.configs[0]
	source.configs = source.configs[1:]
	config.Servers = cloneICEServers(config.Servers)
	return config, nil
}

type reconfigurableRestartTestConnection struct {
	restartTestConnection
	mu      sync.Mutex
	configs []ICEConfiguration
	entered chan struct{}
	release chan struct{}
}

func (connection *reconfigurableRestartTestConnection) RestartWithConfiguration(ctx context.Context, config ICEConfiguration) error {
	connection.mu.Lock()
	connection.configs = append(connection.configs, ICEConfiguration{Servers: cloneICEServers(config.Servers), Policy: config.Policy, ExpiresAt: config.ExpiresAt})
	entered, release := connection.entered, connection.release
	connection.mu.Unlock()
	if entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (connection *reconfigurableRestartTestConnection) AcceptRestartWithConfiguration(_ context.Context, _ rank2xmpp.Jingle, config ICEConfiguration) error {
	return connection.RestartWithConfiguration(context.Background(), config)
}

func (*restartTestConnection) OpenDataChannel(context.Context, DataChannelConfig) (DataChannel, error) {
	return nil, transport.ErrUnavailable
}
func (*restartTestConnection) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("restart-test")), true
}
func (*restartTestConnection) Close(context.Context) error { return nil }
func (connection *restartTestConnection) Restart(ctx context.Context) error {
	connection.mu.Lock()
	connection.restarts++
	connection.mu.Unlock()
	if connection.release == nil {
		return nil
	}
	select {
	case <-connection.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (connection *restartTestConnection) AcceptRestart(ctx context.Context, _ rank2xmpp.Jingle) error {
	connection.mu.Lock()
	connection.accepts++
	connection.mu.Unlock()
	if connection.release == nil {
		return nil
	}
	select {
	case <-connection.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newRestartAssembly(t *testing.T, local string, authority Rank1GroupAuthority, connection RestartablePeerConnection) *HandshakeNegotiator {
	t.Helper()
	manager, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{MeshID: "mesh", LocalIdentity: local, Manager: manager, Authority: authority, Exchange: fakeExchange{}, MaximumMessageBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, SCTPStreams: 8, Clock: rank1TestClock})
	if err != nil {
		t.Fatal(err)
	}
	peer := "b/mesh"
	if local == peer {
		peer = "a/mesh"
	}
	assembly.sessions[peer] = &restartSession{sid: "sid", connection: connection, changed: make(chan struct{})}
	return assembly
}

func TestRestartOwnerRefreshesExistingSessionAndReauthorizes(t *testing.T) {
	connection := &restartTestConnection{}
	authority := &sequenceRank1Authority{values: []uint64{7, 7}}
	assembly := newRestartAssembly(t, "a/mesh", authority, connection)
	if err := assembly.recoverInPlace(context.Background(), "b/mesh", nil); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	restarts := connection.restarts
	connection.mu.Unlock()
	if restarts != 1 {
		t.Fatalf("restarts=%d", restarts)
	}
}

func TestRestartResponderWakesPassivePeerOnlyAfterReauthorization(t *testing.T) {
	connection := &restartTestConnection{}
	authority := &sequenceRank1Authority{values: []uint64{7, 7, 7}}
	assembly := newRestartAssembly(t, "b/mesh", authority, connection)
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
			t.Fatal("passive peer did not begin waiting")
		}
		time.Sleep(time.Millisecond)
	}
	signal := rank2xmpp.Jingle{Action: "transport-replace", Initiator: "a/mesh", Responder: "b/mesh", SID: "sid", Content: rank2xmpp.JingleContent{Description: rank2xmpp.DataChannelDescription{MeshID: "mesh"}}}
	if err := assembly.HandleTransportReplace(context.Background(), "a/mesh", signal); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("passive peer did not observe accepted restart")
	}
	connection.mu.Lock()
	accepts := connection.accepts
	connection.mu.Unlock()
	if accepts != 1 {
		t.Fatalf("accepts=%d", accepts)
	}
}

func TestHandshakeCloseCancelsAndJoinsAdmittedRestartOperations(t *testing.T) {
	for _, operation := range []string{"recover", "transport-replace"} {
		t.Run(operation, func(t *testing.T) {
			connection := &restartTestConnection{release: make(chan struct{})}
			local, peer := "a/mesh", "b/mesh"
			if operation == "transport-replace" {
				local, peer = "b/mesh", "a/mesh"
			}
			assembly := newRestartAssembly(t, local, staticRank1Authority(7), connection)
			result := make(chan error, 1)
			go func() {
				if operation == "recover" {
					result <- assembly.recoverInPlace(context.Background(), peer, nil)
					return
				}
				signal := rank2xmpp.Jingle{Action: "transport-replace", Initiator: peer, Responder: local, SID: "sid", Content: rank2xmpp.JingleContent{Description: rank2xmpp.DataChannelDescription{MeshID: "mesh"}}}
				result <- assembly.HandleTransportReplace(context.Background(), peer, signal)
			}()
			deadline := time.Now().Add(time.Second)
			for {
				connection.mu.Lock()
				entered := connection.restarts == 1 || connection.accepts == 1
				connection.mu.Unlock()
				if entered {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("restart operation was not entered")
				}
				time.Sleep(time.Millisecond)
			}
			closed := make(chan error, 1)
			go func() { closed <- assembly.Close(context.Background()) }()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("restart after Close = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close lifetime did not cancel restart")
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not join canceled restart")
			}
		})
	}
}

func TestRestartAuthorityLossAndCancellationFailClosed(t *testing.T) {
	connection := &restartTestConnection{release: make(chan struct{})}
	assembly := newRestartAssembly(t, "a/mesh", staticRank1Authority(7), connection)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := assembly.recoverInPlace(ctx, "b/mesh", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	changed := newRestartAssembly(t, "a/mesh", &sequenceRank1Authority{values: []uint64{0}}, &restartTestConnection{})
	if err := changed.recoverInPlace(context.Background(), "b/mesh", nil); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("authority loss=%v", err)
	}
}

func TestRestartFetchesFreshAuthenticatedICEForCurrentSession(t *testing.T) {
	now := rank1TestClock.Now()
	source := &restartConfigurationSource{configs: []ICEConfiguration{{Servers: []ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: []byte("fresh")}}, Policy: webrtc.ICETransportPolicyAll, ExpiresAt: now.Add(time.Minute)}}}
	connection := &reconfigurableRestartTestConnection{}
	assembly := newRestartAssembly(t, "a/mesh", staticRank1Authority(7), connection)
	assembly.config.ICEConfiguration = source
	assembly.config.ExpiryCleanupTimeout = time.Second
	session := assembly.sessions["b/mesh"]
	session.expiryChanged = make(chan struct{})
	session.retired = make(chan struct{})
	if err := assembly.restartWithCurrentICE(context.Background(), "b/mesh", session, nil); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	calls := source.calls
	source.mu.Unlock()
	connection.mu.Lock()
	configs := append([]ICEConfiguration(nil), connection.configs...)
	connection.mu.Unlock()
	if calls != 1 || len(configs) != 1 || !bytes.Equal(configs[0].Servers[0].Credential, []byte("fresh")) || !session.expiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("calls=%d configs=%#v expiry=%v", calls, configs, session.expiresAt)
	}
}

func TestCredentialExpiryWinsNoncooperativeRestart(t *testing.T) {
	now := rank1TestClock.Now()
	source := &restartConfigurationSource{configs: []ICEConfiguration{{Servers: []ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: []byte("fresh")}}, Policy: webrtc.ICETransportPolicyAll, ExpiresAt: now.Add(time.Minute)}}}
	connection := &reconfigurableRestartTestConnection{entered: make(chan struct{}), release: make(chan struct{})}
	assembly := newRestartAssembly(t, "a/mesh", staticRank1Authority(7), connection)
	assembly.config.ICEConfiguration = source
	assembly.config.ExpiryCleanupTimeout = time.Second
	session := assembly.sessions["b/mesh"]
	session.expiryChanged = make(chan struct{})
	session.retired = make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- assembly.restartWithCurrentICE(context.Background(), "b/mesh", session, nil) }()
	<-connection.entered
	if !assembly.retireRestart("b/mesh", session.token) {
		t.Fatal("expiry did not retire exact restart session")
	}
	close(connection.release)
	if err := <-result; !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("late restart crossed expiry: %v", err)
	}
}

func TestRestartRejectsOfferFromNonOwnerAndWrongSession(t *testing.T) {
	connection := &restartTestConnection{}
	assembly := newRestartAssembly(t, "a/mesh", staticRank1Authority(7), connection)
	nonOwner := rank2xmpp.Jingle{Action: "transport-replace", Initiator: "a/mesh", Responder: "b/mesh", SID: "sid", Content: rank2xmpp.JingleContent{Description: rank2xmpp.DataChannelDescription{MeshID: "mesh"}}}
	if err := assembly.HandleTransportReplace(context.Background(), "b/mesh", nonOwner); !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("non-owner=%v", err)
	}
	responder := newRestartAssembly(t, "b/mesh", staticRank1Authority(7), connection)
	stale := rank2xmpp.Jingle{Action: "transport-replace", Initiator: "a/mesh", Responder: "b/mesh", SID: "other", Content: rank2xmpp.JingleContent{Description: rank2xmpp.DataChannelDescription{MeshID: "mesh"}}}
	if err := responder.HandleTransportReplace(context.Background(), "a/mesh", stale); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("stale=%v", err)
	}
	connection.mu.Lock()
	accepts := connection.accepts
	connection.mu.Unlock()
	if accepts != 0 {
		t.Fatalf("invalid offers accepted=%d", accepts)
	}
}

func TestRestartJingleActionsPreserveChannelBinding(t *testing.T) {
	fingerprintA := "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"
	fingerprintB := "11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11:11"
	makeSignal := func(action, fingerprint, setup string) rank2xmpp.Jingle {
		return rank2xmpp.Jingle{XMLName: xmlName(rank2xmpp.JingleNamespace, "jingle"), Action: action, Initiator: "a/mesh", Responder: "b/mesh", SID: "sid", Content: rank2xmpp.JingleContent{Creator: "initiator", Name: "data", Description: rank2xmpp.DataChannelDescription{XMLName: xmlName(rank2xmpp.AZTMDataChannelNamespace, "description"), Media: "application", MeshID: "mesh", MaximumMessageSize: 1024}, Transport: rank2xmpp.ICETransport{XMLName: xmlName(rank2xmpp.ICEUDPNamespace, "transport"), Ufrag: "ufrag", Password: "password", Candidates: []rank2xmpp.ICECandidate{{Component: 1, Foundation: "1", ID: "candidate", IP: "127.0.0.1", Port: 9999, Priority: 1, Protocol: "udp", Type: "host"}}, Fingerprint: rank2xmpp.DTLSFingerprint{XMLName: xmlName(rank2xmpp.DTLSNamespace, "fingerprint"), Hash: "sha-256", Setup: setup, Value: fingerprint}, SCTP: rank2xmpp.SCTPMap{XMLName: xmlName(rank2xmpp.SCTPNamespace, "sctpmap"), Number: 5000, Protocol: "webrtc-datachannel", Streams: 16}}}}
	}
	initialOffer := makeSignal("session-initiate", fingerprintA, "actpass")
	initialAnswer := makeSignal("session-accept", fingerprintB, "active")
	restartOffer := makeSignal("transport-replace", fingerprintA, "actpass")
	restartAnswer := makeSignal("transport-accept", fingerprintB, "active")
	initial, err := deriveJingleChannelBinding(initialOffer, initialAnswer)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := deriveJingleChannelBinding(restartOffer, restartAnswer)
	if err != nil || restarted != initial {
		t.Fatalf("restart binding=%x initial=%x err=%v", restarted, initial, err)
	}
	encoded, err := rank2xmpp.EncodeJingle(restartAnswer)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := rank2xmpp.DecodeJingle(encoded)
	if err != nil || decoded.Action != "transport-accept" {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
}

type pairedJingleExchange struct {
	requests  chan rank2xmpp.Jingle
	responses chan rank2xmpp.Jingle
}

func (exchange *pairedJingleExchange) ExchangeJingle(ctx context.Context, _ string, signal rank2xmpp.Jingle) (rank2xmpp.Jingle, error) {
	select {
	case exchange.requests <- signal:
	case <-ctx.Done():
		return rank2xmpp.Jingle{}, ctx.Err()
	}
	select {
	case response := <-exchange.responses:
		return response, nil
	case <-ctx.Done():
		return rank2xmpp.Jingle{}, ctx.Err()
	}
}

func (exchange *pairedJingleExchange) SendJingle(ctx context.Context, _ string, signal rank2xmpp.Jingle) error {
	select {
	case exchange.responses <- signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPionRestartsExistingDataChannelWithStructuredJingle(t *testing.T) {
	// This live path performs two initial ICE/DTLS/SCTP handshakes and an ICE
	// restart. Keep one bounded end-to-end budget, with enough headroom for race
	// instrumentation and package-level parallelism.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	exchange := &pairedJingleExchange{requests: make(chan rank2xmpp.Jingle, 2), responses: make(chan rank2xmpp.Jingle, 2)}
	initiatorNegotiator, err := NewJingleNegotiator(JingleNegotiatorConfig{LocalIdentity: "a/mesh", PeerIdentity: "b/mesh", MeshID: "mesh", SID: "sid", SCTPStreams: 16, MaximumMessageBytes: 1024}, exchange)
	if err != nil {
		t.Fatal(err)
	}
	clock := transport.ClockFunc(func() time.Time { return time.Now().UTC() })
	initiator, err := NewPionPeerConnection(PionConfig{Initiator: true, ICETransportPolicy: webrtc.ICETransportPolicyAll, ReceiveCapacity: 4, Clock: clock}, initiatorNegotiator)
	if err != nil {
		t.Fatal(err)
	}
	channelConfig := DataChannelConfig{Label: "aztm", Ordered: false, MaximumFrameBytes: 1024}
	type openResult struct {
		channel DataChannel
		err     error
	}
	initiatorOpen := make(chan openResult, 1)
	go func() {
		channel, openErr := initiator.OpenDataChannel(ctx, channelConfig)
		initiatorOpen <- openResult{channel: channel, err: openErr}
	}()
	var offer rank2xmpp.Jingle
	select {
	case offer = <-exchange.requests:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	responderNegotiator, err := NewJingleNegotiator(JingleNegotiatorConfig{LocalIdentity: "b/mesh", PeerIdentity: "a/mesh", MeshID: "mesh", SID: "sid", SCTPStreams: 16, MaximumMessageBytes: 1024, Remote: &offer}, exchange)
	if err != nil {
		t.Fatal(err)
	}
	responder, err := NewPionPeerConnection(PionConfig{ICETransportPolicy: webrtc.ICETransportPolicyAll, ReceiveCapacity: 4, Clock: clock}, responderNegotiator)
	if err != nil {
		t.Fatal(err)
	}
	responderOpen := make(chan openResult, 1)
	go func() {
		channel, openErr := responder.OpenDataChannel(ctx, channelConfig)
		responderOpen <- openResult{channel: channel, err: openErr}
	}()
	left, right := <-initiatorOpen, <-responderOpen
	if left.err != nil || right.err != nil {
		t.Fatalf("open initiator=%v responder=%v", left.err, right.err)
	}
	defer initiator.Close(context.Background())
	defer responder.Close(context.Background())
	before := Frame{Kind: FrameHealthProbe, ProbeNonce: bytes.Repeat([]byte{1}, 16)}
	if err = left.channel.Send(ctx, before); err != nil {
		t.Fatal(err)
	}
	if received, receiveErr := right.channel.Receive(ctx); receiveErr != nil || string(received.ProbeNonce) != string(before.ProbeNonce) {
		t.Fatalf("before=%#v err=%v", received, receiveErr)
	}
	initiated := make(chan error, 1)
	go func() { initiated <- initiator.Restart(ctx) }()
	var replacement rank2xmpp.Jingle
	select {
	case replacement = <-exchange.requests:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	accepted := make(chan error, 1)
	go func() { accepted <- responder.AcceptRestart(ctx, replacement) }()
	if err = <-initiated; err != nil {
		t.Fatalf("initiator restart=%v", err)
	}
	if err = <-accepted; err != nil {
		t.Fatalf("responder restart=%v", err)
	}
	after := Frame{Kind: FrameHealthProbe, ProbeNonce: bytes.Repeat([]byte{2}, 16)}
	if err = left.channel.Send(ctx, after); err != nil {
		t.Fatal(err)
	}
	if received, receiveErr := right.channel.Receive(ctx); receiveErr != nil || string(received.ProbeNonce) != string(after.ProbeNonce) {
		t.Fatalf("after=%#v err=%v", received, receiveErr)
	}
}
