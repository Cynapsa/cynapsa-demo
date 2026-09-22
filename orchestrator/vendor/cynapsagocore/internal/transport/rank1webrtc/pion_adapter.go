package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/datachannel"
	"github.com/pion/sctp"
	"github.com/pion/webrtc/v4"
)

const pionCloseReaderJoinTimeout = time.Second

// PionNegotiator maps Pion's data-only descriptions to the approved bounded
// Jingle XEP-0166/0176/0320/0343 profile. Raw SDP must never be sent.
type PionNegotiator interface {
	Negotiate(context.Context, *webrtc.PeerConnection, bool) error
	EffectiveMaximumFrameBytes() int
	ChannelBinding() ([sha256.Size]byte, bool)
}

type pionRestartNegotiator interface {
	Restart(context.Context, *webrtc.PeerConnection) error
	AcceptRestart(context.Context, *webrtc.PeerConnection, rank2xmpp.Jingle) error
}

type PionConfig struct {
	Initiator          bool
	ICEServers         []webrtc.ICEServer
	ICETransportPolicy webrtc.ICETransportPolicy
	ReceiveCapacity    int
	Clock              transport.Clock
}

// PionPeerConnection is the approved pure-Go WebRTC dependency adapter.
type PionPeerConnection struct {
	config     PionConfig
	negotiator PionNegotiator
	mu         sync.Mutex
	peer       *webrtc.PeerConnection
	channel    *pionDataChannel
	closed     bool
	opening    bool
	openDone   chan struct{}
	lifetime   context.Context
	cancel     context.CancelFunc
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
	operation  chan struct{}
	iceChanged chan webrtc.ICEConnectionState
	turnDialer *pionTURNDialer
}

func NewPionPeerConnection(config PionConfig, negotiator PionNegotiator) (*PionPeerConnection, error) {
	if config.ReceiveCapacity <= 0 || config.ReceiveCapacity > transport.MaximumReceiveQueue || (config.ICETransportPolicy != webrtc.ICETransportPolicyAll && config.ICETransportPolicy != webrtc.ICETransportPolicyRelay) || config.Clock == nil || config.Clock.Now().IsZero() || negotiator == nil {
		return nil, transport.ErrInvalidConfig
	}
	operation := make(chan struct{}, 1)
	operation <- struct{}{}
	config.ICEServers = clonePionICEServers(config.ICEServers)
	lifetime, cancel := context.WithCancel(context.Background())
	return &PionPeerConnection{config: config, negotiator: negotiator, lifetime: lifetime, cancel: cancel, closeDone: make(chan struct{}), operation: operation, iceChanged: make(chan webrtc.ICEConnectionState, 16), turnDialer: newPionTURNDialer()}, nil
}

func (p *PionPeerConnection) OpenDataChannel(ctx context.Context, config DataChannelConfig) (DataChannel, error) {
	if p == nil || ctx == nil || !config.valid() {
		return nil, transport.ErrInvalidConfig
	}
	p.mu.Lock()
	if p.closed || p.peer != nil || p.opening {
		p.mu.Unlock()
		return nil, transport.ErrUnavailable
	}
	p.opening = true
	p.openDone = make(chan struct{})
	openDone := p.openDone
	servers := clonePionICEServers(p.config.ICEServers)
	policy := p.config.ICETransportPolicy
	initiator := p.config.Initiator
	receiveCapacity := p.config.ReceiveCapacity
	clock := p.config.Clock
	lifetime := p.lifetime
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.opening = false
		close(openDone)
		p.mu.Unlock()
	}()
	defer clearPionICEServers(servers)
	operation, cancelOperation := context.WithCancel(ctx)
	stopLifetime := context.AfterFunc(lifetime, cancelOperation)
	defer func() {
		stopLifetime()
		cancelOperation()
	}()
	routes, err := pionTURNRoutes(servers)
	if err != nil {
		return nil, transport.ErrUnavailable
	}
	finishTURNSetup := p.turnDialer.begin(operation, routes)
	defer finishTURNSetup()
	settings := webrtc.SettingEngine{}
	settings.SetICEProxyDialer(p.turnDialer)
	settings.SetNetworkTypes(pionNetworkTypes(servers))
	settings.SetSCTPMaxMessageSize(uint32(config.MaximumFrameBytes))
	settings.DetachDataChannels()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	peer, err := newPionPeerWithConfiguration(api, servers, policy)
	if err != nil {
		return nil, transport.ErrUnavailable
	}
	peer.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		select {
		case p.iceChanged <- state:
		default:
		}
	})
	type channelResult struct {
		channel *pionDataChannel
		err     error
	}
	ready := make(chan channelResult, 1)
	wrap := func(raw *webrtc.DataChannel) {
		if !exactDataChannelProfile(raw, config) {
			_ = raw.Close()
			select {
			case ready <- channelResult{err: transport.ErrProtocol}:
			default:
			}
			return
		}
		channel := newPionDataChannel(raw, peer, config.MaximumFrameBytes, receiveCapacity, clock)
		select {
		case ready <- channelResult{channel: channel}:
		default:
			_ = raw.Close()
		}
	}
	if initiator {
		ordered := false
		negotiated := false
		protocol := ""
		channel, createErr := peer.CreateDataChannel(config.Label, &webrtc.DataChannelInit{Ordered: &ordered, Protocol: &protocol, Negotiated: &negotiated})
		if createErr != nil {
			_ = peer.Close()
			return nil, transport.ErrUnavailable
		}
		wrap(channel)
	} else {
		peer.OnDataChannel(wrap)
	}
	if err := callPionNegotiator(p.negotiator, operation, peer, initiator); err != nil {
		_ = peer.Close()
		return nil, p.openError(err, ctx)
	}
	effectiveMaximum := pionMaximum(p.negotiator)
	if effectiveMaximum < transport.MinimumRank1MessageBytes || effectiveMaximum > config.MaximumFrameBytes {
		_ = peer.Close()
		return nil, transport.ErrProtocol
	}
	var channel *pionDataChannel
	select {
	case result := <-ready:
		if result.err != nil || result.channel == nil {
			_ = peer.Close()
			return nil, transport.ErrProtocol
		}
		channel = result.channel
	case <-operation.Done():
		_ = peer.Close()
		return nil, p.openError(operation.Err(), ctx)
	}
	channel.maximum = effectiveMaximum
	if err := channel.waitOpen(operation); err != nil {
		_ = peer.Close()
		return nil, p.openError(err, ctx)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = peer.Close()
		return nil, transport.ErrClosed
	}
	p.peer, p.channel = peer, channel
	// The live Pion peer now owns the unavoidable immutable credential string.
	// Core no longer needs its retained configuration copy.
	clearPionICEServers(p.config.ICEServers)
	p.config.ICEServers = nil
	p.mu.Unlock()
	return channel, nil
}

func (p *PionPeerConnection) openError(err error, caller context.Context) error {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return transport.ErrClosed
	}
	if errors.Is(err, transport.ErrProtocol) {
		return transport.ErrProtocol
	}
	return normalize(err, caller, transport.ErrUnavailable)
}

func pionNetworkTypes(servers []webrtc.ICEServer) []webrtc.NetworkType {
	types := []webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6}
	for _, server := range servers {
		for _, raw := range server.URLs {
			lower := strings.ToLower(raw)
			if strings.HasPrefix(lower, "turns:") || strings.HasSuffix(lower, "?transport=tcp") {
				return append(types, webrtc.NetworkTypeTCP4, webrtc.NetworkTypeTCP6)
			}
		}
	}
	return types
}

func exactDataChannelProfile(channel *webrtc.DataChannel, config DataChannelConfig) bool {
	return channel != nil && channel.Label() == config.Label && channel.Ordered() == config.Ordered && channel.MaxRetransmits() == nil && channel.MaxPacketLifeTime() == nil && channel.Protocol() == "" && !channel.Negotiated()
}

func (p *PionPeerConnection) ChannelBinding() ([sha256.Size]byte, bool) {
	if p == nil || p.negotiator == nil {
		return [sha256.Size]byte{}, false
	}
	return pionChannelBinding(p.negotiator)
}

func (p *PionPeerConnection) Restart(ctx context.Context) error {
	return p.restart(ctx, nil, nil)
}

func (p *PionPeerConnection) AcceptRestart(ctx context.Context, remote rank2xmpp.Jingle) error {
	return p.restart(ctx, &remote, nil)
}

func (p *PionPeerConnection) RestartWithConfiguration(ctx context.Context, config ICEConfiguration) error {
	return p.restart(ctx, nil, &config)
}

func (p *PionPeerConnection) AcceptRestartWithConfiguration(ctx context.Context, remote rank2xmpp.Jingle, config ICEConfiguration) error {
	return p.restart(ctx, &remote, &config)
}

func (p *PionPeerConnection) restart(ctx context.Context, remote *rank2xmpp.Jingle, refreshed *ICEConfiguration) error {
	if p == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	select {
	case <-p.operation:
		defer func() { p.operation <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	peer, channel, closed := p.peer, p.channel, p.closed
	p.mu.Unlock()
	restarter, ok := p.negotiator.(pionRestartNegotiator)
	if closed {
		return transport.ErrClosed
	}
	if peer == nil || channel == nil || !channel.restartable() || !ok {
		return transport.ErrUnavailable
	}
	var refreshedRoutes map[string]pionTURNRoute
	if refreshed != nil {
		if !validICEConfiguration(*refreshed, p.config.Clock.Now().UTC()) {
			return transport.ErrUnavailable
		}
		servers := pionICEServers(refreshed.Servers)
		defer clearPionICEServers(servers)
		var err error
		refreshedRoutes, err = pionTURNRoutes(servers)
		if err != nil {
			return transport.ErrUnavailable
		}
		err = setPionPeerConfiguration(peer, servers, refreshed.Policy)
		if err != nil {
			return transport.ErrUnavailable
		}
		p.mu.Lock()
		if p.closed || p.peer != peer || p.channel != channel {
			p.mu.Unlock()
			return transport.ErrClosed
		}
		clearPionICEServers(p.config.ICEServers)
		p.config.ICEServers = nil
		p.config.ICETransportPolicy = refreshed.Policy
		p.mu.Unlock()
	}
	operation, cancelOperation := context.WithCancel(ctx)
	stopLifetime := context.AfterFunc(p.lifetime, cancelOperation)
	defer func() {
		stopLifetime()
		cancelOperation()
	}()
	finishTURNSetup := p.turnDialer.begin(operation, refreshedRoutes)
	defer finishTURNSetup()
	for {
		select {
		case <-p.iceChanged:
			continue
		default:
			goto drained
		}
	}
drained:
	var err error
	if remote == nil {
		err = restarter.Restart(operation, peer)
	} else {
		err = restarter.AcceptRestart(operation, peer, *remote)
	}
	if err != nil {
		return normalize(err, ctx, transport.ErrUnavailable)
	}
	if err = p.waitRestartConnected(operation, peer); err != nil {
		return err
	}
	if !channel.restartable() {
		return transport.ErrUnavailable
	}
	return nil
}

func validICEConfiguration(config ICEConfiguration, now time.Time) bool {
	if len(config.Servers) > maximumPrivateServers || (config.Policy != webrtc.ICETransportPolicyAll && config.Policy != webrtc.ICETransportPolicyRelay) || len(config.Servers) == 0 && config.Policy != webrtc.ICETransportPolicyAll || len(config.Servers) == 0 && !config.ExpiresAt.IsZero() || now.IsZero() || now.Location() != time.UTC || !config.ExpiresAt.IsZero() && !now.Before(config.ExpiresAt) {
		return false
	}
	hasTURN := false
	for _, server := range config.Servers {
		if len(server.URLs) == 0 || len(server.URLs) > maximumPrivateURLs || len(server.Username) > maximumPrivateFieldBytes {
			return false
		}
		if !validPrivateCredential(server.Credential) {
			return false
		}
		for _, raw := range server.URLs {
			if !validPrivateServerURL(raw) {
				return false
			}
			if strings.HasPrefix(raw, "turn:") || strings.HasPrefix(raw, "turns:") {
				hasTURN = true
			}
		}
	}
	return config.Policy != webrtc.ICETransportPolicyRelay || hasTURN
}

func validPrivateCredential(credential []byte) bool {
	if len(credential) > maximumPrivateFieldBytes || !utf8.Valid(credential) {
		return false
	}
	for _, char := range credential {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

// pionICEServers is the single Core-to-Pion credential boundary. Pion's API
// requires an immutable string, so that dependency-owned backing cannot be
// overwritten by Core. All Core-owned inputs remain []byte and are cleared by
// their owners after this call.
func pionICEServers(servers []ICEServer) []webrtc.ICEServer {
	converted := make([]webrtc.ICEServer, len(servers))
	for i := range servers {
		converted[i] = webrtc.ICEServer{
			URLs:           append([]string(nil), servers[i].URLs...),
			Username:       servers[i].Username,
			Credential:     string(servers[i].Credential),
			CredentialType: webrtc.ICECredentialTypePassword,
		}
	}
	return converted
}

// clonePionICEServers deliberately shares immutable credential backing. Pion
// may require configuration copies, but duplicating strings would only create
// additional unzeroable secret allocations. References are dropped on every
// Core error, replacement, success, and close path; Pion retains its own copy
// only for the exact lifetime of the peer/configuration that needs it.
func clonePionICEServers(servers []webrtc.ICEServer) []webrtc.ICEServer {
	cloned := make([]webrtc.ICEServer, len(servers))
	for i := range servers {
		cloned[i] = servers[i]
		cloned[i].URLs = append([]string(nil), servers[i].URLs...)
		// Credential is assigned, never strings.Clone'd, to share its backing.
		cloned[i].Credential = servers[i].Credential
	}
	return cloned
}

func clearPionICEServers(servers []webrtc.ICEServer) {
	for i := range servers {
		clear(servers[i].URLs)
		servers[i].URLs = nil
		servers[i].Username = ""
		servers[i].Credential = nil
	}
	clear(servers)
}

func newPionPeerWithConfiguration(api *webrtc.API, servers []webrtc.ICEServer, policy webrtc.ICETransportPolicy) (*webrtc.PeerConnection, error) {
	configuration := webrtc.Configuration{ICEServers: clonePionICEServers(servers), ICETransportPolicy: policy}
	defer clearPionICEServers(configuration.ICEServers)
	return api.NewPeerConnection(configuration)
}

func setPionPeerConfiguration(peer *webrtc.PeerConnection, servers []webrtc.ICEServer, policy webrtc.ICETransportPolicy) error {
	configuration := webrtc.Configuration{ICEServers: clonePionICEServers(servers), ICETransportPolicy: policy}
	transferred := false
	defer func() {
		if !transferred {
			clearPionICEServers(configuration.ICEServers)
		}
	}()
	if err := peer.SetConfiguration(configuration); err != nil {
		return err
	}
	// Pion shallow-retains the handed ICEServers slice on success. Relinquish
	// this exact slice to the live peer; clearing it here would corrupt Pion's
	// installed configuration. Credential string backing remains shared with
	// the caller's short-lived Pion view, avoiding another immutable copy.
	transferred = true
	configuration.ICEServers = nil
	return nil
}

var _ ReconfigurableRestartablePeerConnection = (*PionPeerConnection)(nil)

func (p *PionPeerConnection) waitRestartConnected(ctx context.Context, peer *webrtc.PeerConnection) error {
	leftConnected := false
	for {
		state := peer.ICEConnectionState()
		switch state {
		case webrtc.ICEConnectionStateChecking, webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateNew:
			leftConnected = true
		case webrtc.ICEConnectionStateConnected, webrtc.ICEConnectionStateCompleted:
			if leftConnected {
				return nil
			}
		case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			return transport.ErrUnavailable
		}
		select {
		case observed := <-p.iceChanged:
			switch observed {
			case webrtc.ICEConnectionStateChecking, webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateNew:
				leftConnected = true
			case webrtc.ICEConnectionStateConnected, webrtc.ICEConnectionStateCompleted:
				if leftConnected {
					return nil
				}
			case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
				return transport.ErrUnavailable
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func callPionNegotiator(negotiator PionNegotiator, ctx context.Context, peer *webrtc.PeerConnection, initiator bool) (err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrUnavailable
		}
	}()
	return negotiator.Negotiate(ctx, peer, initiator)
}
func pionMaximum(negotiator PionNegotiator) (maximum int) {
	defer func() {
		if recover() != nil {
			maximum = 0
		}
	}()
	return negotiator.EffectiveMaximumFrameBytes()
}

func pionChannelBinding(negotiator PionNegotiator) (binding [sha256.Size]byte, ok bool) {
	defer func() {
		if recover() != nil {
			binding, ok = [sha256.Size]byte{}, false
		}
	}()
	return negotiator.ChannelBinding()
}

func (p *PionPeerConnection) Close(ctx context.Context) error {
	if p == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cancel()
		openDone := p.openDone
		p.mu.Unlock()
		go p.finishClose(openDone)
	})
	select {
	case <-p.closeDone:
		p.mu.Lock()
		err := p.closeErr
		p.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PionPeerConnection) finishClose(openDone <-chan struct{}) {
	if openDone != nil {
		<-openDone
	}
	p.mu.Lock()
	peer := p.peer
	channel := p.channel
	p.peer = nil
	p.channel = nil
	clearPionICEServers(p.config.ICEServers)
	p.config.ICEServers = nil
	p.mu.Unlock()
	var result error
	if peer != nil {
		if channel != nil {
			// Fence the read side before owned peer shutdown aborts SCTP. A
			// resulting detached-read error is local teardown, not an
			// independent channel failure that should race channel cleanup.
			channel.beginPeerClose()
		}
		if err := peer.Close(); err != nil && !errors.Is(err, webrtc.ErrConnectionClosed) {
			result = transport.ErrClosed
		}
	}
	if result == nil && channel != nil {
		// The peer is the dependency that can unblock a failed detached read.
		// Join that sole producer before publishing terminal connection close.
		_ = channel.waitForReader(context.Background())
	}
	p.mu.Lock()
	p.closeErr = result
	p.mu.Unlock()
	close(p.closeDone)
}

type pionDataChannel struct {
	raw            *webrtc.DataChannel
	peer           *webrtc.PeerConnection // observed only; PionPeerConnection owns Close
	maximum        int
	receiveMaximum int
	receive        chan Frame
	leasedReceive  chan inboundFrame
	opened         chan struct{}
	done           chan struct{}
	readDone       chan struct{}
	readStarted    bool
	terminalFrame  Frame
	terminalLease  *transport.InboundLease
	terminalOwned  bool
	detached       datachannel.ReadWriteCloserDeadliner
	once           sync.Once
	openOnce       sync.Once
	closeOnce      sync.Once
	mu             sync.Mutex
	closeDone      chan struct{}
	closeErr       error
	closeJoinWait  time.Duration
	writeMu        sync.Mutex
	state          transport.HealthState
	terminal       bool
	peerClosing    bool
	progress       time.Time
	clock          transport.Clock
	peerState      func() webrtc.PeerConnectionState
	inboundBudget  *transport.InboundBudget
	inboundCtx     context.Context
	inboundCancel  context.CancelFunc
}

func newPionDataChannel(raw *webrtc.DataChannel, peer *webrtc.PeerConnection, maximum, capacity int, clock transport.Clock) *pionDataChannel {
	inboundBudget, _ := transport.NewInboundBudget(capacity, maximum)
	inboundCtx, inboundCancel := context.WithCancel(context.Background())
	channel := &pionDataChannel{raw: raw, peer: peer, maximum: maximum, receiveMaximum: maximum, leasedReceive: make(chan inboundFrame, capacity), opened: make(chan struct{}), done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}), state: transport.HealthConnecting, clock: clock, peerState: peer.ConnectionState, inboundBudget: inboundBudget, inboundCtx: inboundCtx, inboundCancel: inboundCancel}
	raw.OnOpen(func() {
		detached, err := raw.DetachWithDeadline()
		if err != nil {
			channel.fail()
			return
		}
		channel.mu.Lock()
		if channel.terminal {
			channel.mu.Unlock()
			_ = detached.Close()
			return
		}
		channel.detached = detached
		channel.state = transport.HealthHealthy
		channel.readStarted = true
		channel.mu.Unlock()
		channel.openOnce.Do(func() { close(channel.opened) })
		go channel.readLoop(detached)
	})
	raw.OnError(func(error) { channel.fail() })
	raw.OnClose(channel.markClosed)
	peer.OnConnectionStateChange(channel.updatePeerState)
	return channel
}

// updatePeerState projects Pion's authoritative current state while preserving
// terminal monotonicity. Pion stores its state before dispatching callbacks in
// separate goroutines, so callback arguments can arrive out of order. Reading
// the current state prevents a stale pre-connected callback from hiding an
// open channel without masking a real ICE restart or outage.
func (c *pionDataChannel) updatePeerState(state webrtc.PeerConnectionState) {
	if c == nil {
		return
	}
	if c.peerState != nil {
		state = c.peerState()
	}
	c.mu.Lock()
	if c.terminal {
		c.mu.Unlock()
		return
	}
	switch state {
	case webrtc.PeerConnectionStateNew, webrtc.PeerConnectionStateConnecting:
		c.state = transport.HealthConnecting
	case webrtc.PeerConnectionStateConnected:
		if c.detached == nil {
			c.state = transport.HealthConnecting
		} else {
			c.state = transport.HealthHealthy
		}
	case webrtc.PeerConnectionStateDisconnected:
		c.state = transport.HealthDisconnected
	case webrtc.PeerConnectionStateFailed:
		// ICE failure remains eligible for an authenticated ICE restart. It is
		// not a terminal DataChannel read-side failure by itself.
		c.state = transport.HealthFailed
	case webrtc.PeerConnectionStateClosed:
		c.state, c.terminal = transport.HealthClosed, true
	}
	terminal := c.terminal
	c.mu.Unlock()
	if terminal {
		c.once.Do(func() { close(c.done) })
	}
}

func (c *pionDataChannel) readLoop(detached datachannel.ReadWriteCloserDeadliner) {
	defer close(c.readDone)
	buffer := make([]byte, c.receiveMaximum+1)
	defer clear(buffer)
	for {
		n, isString, err := detached.ReadDataChannel(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.markClosed()
			} else {
				c.fail()
			}
			return
		}
		if isString || n <= 0 || n > c.receiveMaximum {
			c.fail()
			return
		}
		frame, err := transport.DecodeControlFrame(buffer[:n], c.receiveMaximum)
		if err != nil {
			c.fail()
			return
		}
		c.mu.Lock()
		c.progress = c.clock.Now().UTC()
		c.mu.Unlock()
		if c.inboundBudget == nil {
			if !enqueueRawFrameBeforeClose(c.receive, frame, c.done) {
				owned := inboundFrame{frame: frame}
				frame = Frame{}
				if !c.retainTerminalFrame(&owned) {
					clearInboundFrame(&owned)
				}
				return
			}
			continue
		}
		charge := retainedFrameBytes(frame)
		lease, budgetErr := c.inboundBudget.Acquire(c.inboundCtx, charge)
		if budgetErr != nil {
			clearOwnedFrame(&frame)
			if c.inboundCtx.Err() == nil {
				c.fail()
			}
			return
		}
		owned := inboundFrame{frame: frame, lease: lease, validated: true}
		frame = Frame{}
		if !enqueueFrameBeforeClose(c.leasedReceive, owned, c.done) {
			if !c.retainTerminalFrame(&owned) {
				clearInboundFrame(&owned)
			}
			return
		}
	}
}
func (c *pionDataChannel) waitOpen(ctx context.Context) error {
	select {
	case <-c.opened:
		return nil
	case <-c.done:
		return transport.ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *pionDataChannel) MaximumFrameBytes() int {
	if c == nil {
		return 0
	}
	return c.maximum
}

func (c *pionDataChannel) restartable() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.terminal && c.detached != nil
}

func (c *pionDataChannel) Send(ctx context.Context, frame Frame) error {
	if c == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := transport.EncodeControlFrame(frame, c.maximum)
	if err != nil {
		return transport.ErrProtocol
	}
	defer clear(encoded)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	detached, state, terminal := c.detached, c.state, c.terminal
	c.mu.Unlock()
	if detached == nil || terminal || state == transport.HealthFailed || state == transport.HealthClosed {
		return transport.ErrUnavailable
	}
	select {
	case <-c.done:
		return transport.ErrUnavailable
	default:
	}
	deadline, _ := ctx.Deadline()
	if err := detached.SetWriteDeadline(deadline); err != nil {
		c.fail()
		return transport.ErrSendAmbiguous
	}
	interrupted := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() {
		_ = detached.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	_, err = detached.WriteDataChannel(encoded, false)
	if !stopInterrupt() {
		<-interrupted
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		c.fail()
		return ctxErr
	}
	if resetErr := detached.SetWriteDeadline(time.Time{}); resetErr != nil {
		c.fail()
		return transport.ErrSendAmbiguous
	}
	if err != nil {
		return transport.ErrSendAmbiguous
	}
	return nil
}
func (c *pionDataChannel) Receive(ctx context.Context) (Frame, error) {
	if c == nil || ctx == nil {
		return Frame{}, transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	return c.receiveOwned(ctx)
}

func (c *pionDataChannel) receiveOwned(ctx context.Context) (Frame, error) {
	if c == nil || ctx == nil {
		return Frame{}, transport.ErrInvalidConfig
	}
	owned, err := c.receiveFrame(ctx, true)
	if err != nil {
		clearInboundFrame(&owned)
		return Frame{}, err
	}
	frame := owned.frame.Clone()
	clearInboundFrame(&owned)
	return frame, nil
}

func (c *pionDataChannel) receiveLeased(ctx context.Context) (inboundFrame, error) {
	if c == nil || ctx == nil {
		return inboundFrame{}, transport.ErrInvalidConfig
	}
	return c.receiveFrame(ctx, true)
}

func (c *pionDataChannel) receiveFrame(ctx context.Context, owned bool) (inboundFrame, error) {
	if owned {
		if frame, ok := c.takeQueuedFrame(); ok {
			return frame, nil
		}
	}
	select {
	case frame := <-c.leasedReceive:
		return frame, nil
	case frame := <-c.receive:
		return inboundFrame{frame: frame}, nil
	case <-ctx.Done():
		if frame, ok := c.takeQueuedFrame(); ok {
			return frame, nil
		}
		if owned {
			if frame, ok := c.takeTerminalFrame(); ok {
				return frame, nil
			}
		}
		return inboundFrame{}, ctx.Err()
	case <-c.done:
		if err := c.waitForReader(ctx); err != nil {
			if frame, ok := c.takeQueuedFrame(); ok {
				return frame, nil
			}
			return inboundFrame{}, err
		}
		if frame, ok := c.takeQueuedFrame(); ok {
			return frame, nil
		}
		if frame, ok := c.takeTerminalFrame(); ok {
			return frame, nil
		}
		return inboundFrame{}, transport.ErrClosed
	}
}

func (c *pionDataChannel) takeQueuedFrame() (inboundFrame, bool) {
	select {
	case frame := <-c.leasedReceive:
		return frame, true
	default:
	}
	select {
	case frame := <-c.receive:
		return inboundFrame{frame: frame}, true
	default:
		return inboundFrame{}, false
	}
}

func (c *pionDataChannel) retainTerminalFrame(frame *inboundFrame) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminalOwned {
		return false
	}
	c.terminalFrame = frame.frame
	c.terminalLease = frame.lease
	*frame = inboundFrame{}
	c.terminalOwned = true
	return true
}

func (c *pionDataChannel) takeTerminalFrame() (inboundFrame, bool) {
	c.mu.Lock()
	if !c.terminalOwned {
		c.mu.Unlock()
		return inboundFrame{}, false
	}
	frame := inboundFrame{frame: c.terminalFrame, lease: c.terminalLease}
	c.terminalFrame = Frame{}
	c.terminalLease = nil
	c.terminalOwned = false
	c.mu.Unlock()
	return frame, true
}

func (c *pionDataChannel) waitForReader(ctx context.Context) error {
	c.mu.Lock()
	started, done := c.readStarted, c.readDone
	c.mu.Unlock()
	if !started || done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func enqueueFrameBeforeClose(queue chan<- inboundFrame, frame inboundFrame, done <-chan struct{}) bool {
	select {
	case queue <- frame:
		return true
	default:
	}
	select {
	case queue <- frame:
		return true
	case <-done:
		select {
		case queue <- frame:
			return true
		default:
			return false
		}
	}
}

func enqueueRawFrameBeforeClose(queue chan<- Frame, frame Frame, done <-chan struct{}) bool {
	select {
	case queue <- frame:
		return true
	default:
	}
	select {
	case queue <- frame:
		return true
	case <-done:
		select {
		case queue <- frame:
			return true
		default:
			return false
		}
	}
}

func (c *pionDataChannel) Observe() transport.Observation {
	if c == nil {
		return transport.Observation{State: transport.HealthClosed}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return transport.Observation{State: c.state, LastProgress: c.progress}
}
func (c *pionDataChannel) Close(ctx context.Context) error {
	if c == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	done := c.startClose()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-done:
		c.mu.Lock()
		result := c.closeErr
		c.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *pionDataChannel) startClose() <-chan struct{} {
	c.mu.Lock()
	if c.closeDone == nil {
		c.closeDone = make(chan struct{})
	}
	done := c.closeDone
	c.mu.Unlock()
	c.closeOnce.Do(func() { go c.finishClose(done) })
	return done
}

func (c *pionDataChannel) finishClose(done chan struct{}) {
	var result error
	defer func() {
		if recover() != nil {
			result = transport.ErrClosed
		}
		c.markClosed()
		c.mu.Lock()
		c.closeErr = result
		c.mu.Unlock()
		close(done)
	}()
	c.mu.Lock()
	detached, raw := c.detached, c.raw
	terminal, readStarted, readDone := c.terminal, c.readStarted, c.readDone
	closeJoinWait := c.closeJoinWait
	c.mu.Unlock()
	if detached != nil {
		if err := detached.Close(); err != nil {
			if !pionDependencyAlreadyClosed(err, terminal, readStarted, readDone, closeJoinWait) {
				result = transport.ErrClosed
			}
		}
	} else if raw != nil {
		if err := raw.Close(); err != nil {
			if !pionDependencyAlreadyClosed(err, terminal, readStarted, readDone, closeJoinWait) {
				result = transport.ErrClosed
			}
		}
	}
	c.markClosed()
	if result == nil {
		// A successful dependency close (including a dependency already closed
		// by the remote) must join its sole read producer before terminal success.
		_ = c.waitForReader(context.Background())
	}
}

func pionDependencyAlreadyClosed(err error, terminal, readStarted bool, readDone <-chan struct{}, joinWait time.Duration) bool {
	// Pion reports its SCTP reset sentinel when the remote association has
	// already left the established state. Accept it only for a channel that was
	// already terminal before dependency shutdown and after its sole reader has
	// actually joined. detached.Close may be what releases that reader, so the
	// join evidence must be collected after Close returns. Missing reader state
	// and arbitrary close errors remain failures.
	if !terminal || !errors.Is(err, sctp.ErrResetPacketInStateNotExist) {
		return false
	}
	if !readStarted {
		return true
	}
	if readDone == nil {
		return false
	}
	if channelIsClosed(readDone) {
		return true
	}
	if joinWait <= 0 {
		joinWait = pionCloseReaderJoinTimeout
	}
	timer := time.NewTimer(joinWait)
	defer timer.Stop()
	select {
	case <-readDone:
		return channelIsClosed(readDone)
	case <-timer.C:
		return false
	}
}

// DiscardShutdownOwned clears bounded lower-layer ownership after the read
// producer has terminally joined. It is not used by live-link replacement.
func (c *pionDataChannel) DiscardShutdownOwned() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if !c.terminal || c.readStarted && !channelIsClosed(c.readDone) {
		c.mu.Unlock()
		return
	}
	terminal := inboundFrame{frame: c.terminalFrame, lease: c.terminalLease}
	c.terminalFrame = Frame{}
	c.terminalLease = nil
	c.terminalOwned = false
	c.mu.Unlock()
	clearInboundFrame(&terminal)
	for {
		select {
		case frame := <-c.leasedReceive:
			clearInboundFrame(&frame)
		default:
			for {
				select {
				case frame := <-c.receive:
					clearOwnedFrame(&frame)
				default:
					return
				}
			}
		}
	}
}
func (c *pionDataChannel) fail() {
	c.mu.Lock()
	if c.terminal {
		c.mu.Unlock()
		return
	}
	if c.peerClosing {
		c.mu.Unlock()
		c.markClosed()
		return
	}
	c.state, c.terminal = transport.HealthFailed, true
	c.mu.Unlock()
	c.once.Do(func() { close(c.done) })
	if c.inboundCancel != nil {
		c.inboundCancel()
	}
	// Failure cannot leave the detached reader waiting for unrelated service
	// shutdown. Start the same once-only channel cleanup that Close joins; raw
	// peer ownership remains exclusively with PionPeerConnection.
	c.startClose()
}

func (c *pionDataChannel) beginPeerClose() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.peerClosing = true
	c.mu.Unlock()
}

func (c *pionDataChannel) markClosed() {
	c.mu.Lock()
	// Detached-channel cleanup must not erase an already-recorded terminal
	// failure. A healthy remote or local close still becomes HealthClosed.
	if c.state != transport.HealthFailed {
		c.state = transport.HealthClosed
	}
	c.terminal = true
	c.mu.Unlock()
	c.once.Do(func() { close(c.done) })
	if c.inboundCancel != nil {
		c.inboundCancel()
	}
}

var _ PeerConnection = (*PionPeerConnection)(nil)
var _ RestartablePeerConnection = (*PionPeerConnection)(nil)
var _ DataChannel = (*pionDataChannel)(nil)
