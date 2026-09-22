package rank1webrtc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

// HandshakeAssemblyConfig contains private production wiring for one local
// identity. Every attempt creates a distinct peer-bound Pion connection.
type HandshakeAssemblyConfig struct {
	MeshID                string
	LocalIdentity         string
	Manager               *transport.Manager
	Authority             Rank1GroupAuthority
	Exchange              JingleExchange
	ICEServers            []ICEServer
	ICETransportPolicy    webrtc.ICETransportPolicy
	ICEConfiguration      AuthenticatedICEConfigurationSource
	ExpiryCleanupTimeout  time.Duration
	MaximumMessageBytes   int
	ReceiveCapacity       int
	TransferWorkers       int
	TransferQueue         int
	SCTPStreams           uint16
	TransferReceiver      TransferReceiver
	TransferRouteResolver RouteResolver
	Clock                 transport.Clock
}

// Rank1GroupAuthority authorizes only when the local and peer full identities
// are members of the complete current server snapshot.
type Rank1GroupAuthority interface {
	AuthorizeRank1(context.Context, string) error
}

type peerConnectionFactory func(PionConfig, PionNegotiator) (PeerConnection, error)

// HandshakeNegotiator bridges establishment coordination to structured
// Jingle, Pion, and the peer-scoped live transport registry.
type HandshakeNegotiator struct {
	config         HandshakeAssemblyConfig
	newPeer        peerConnectionFactory
	mu             sync.Mutex
	sessions       map[string]*restartSession
	reserved       map[string]restartReservation
	nextToken      uint64
	tokenExhausted bool
	closed         bool
	closeDone      chan struct{}
	lifetime       context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

type restartSession struct {
	token         uint64
	sid           string
	connection    RestartablePeerConnection
	link          *Link
	changed       chan struct{}
	retired       chan struct{}
	terminal      bool
	waiters       int
	expiresAt     time.Time
	expiryChanged chan struct{}
}

type restartReservation struct {
	token    uint64
	consumes bool
}

func NewHandshakeNegotiator(config HandshakeAssemblyConfig) (*HandshakeNegotiator, error) {
	if protocol.ValidateMeshID(config.MeshID) != nil || protocol.ValidateAgentIdentity(config.LocalIdentity) != nil || !boundToMesh(config.LocalIdentity, config.MeshID) || config.Manager == nil || config.Authority == nil || config.Exchange == nil || (config.ICETransportPolicy != webrtc.ICETransportPolicyAll && config.ICETransportPolicy != webrtc.ICETransportPolicyRelay) || config.MaximumMessageBytes < transport.MinimumRank1MessageBytes || config.MaximumMessageBytes > transport.MaximumControlFrameBytes || config.ReceiveCapacity <= 0 || config.ReceiveCapacity > transport.MaximumReceiveQueue || config.TransferWorkers <= 0 || config.TransferWorkers > 1024 || config.TransferQueue < config.TransferWorkers || config.TransferQueue > 65536 || config.SCTPStreams == 0 || config.Clock == nil || config.Clock.Now().IsZero() || (config.TransferReceiver != nil && config.TransferRouteResolver == nil) || config.ICEConfiguration != nil && (len(config.ICEServers) != 0 || config.ExpiryCleanupTimeout <= 0 || config.ExpiryCleanupTimeout > 30*time.Second) {
		return nil, transport.ErrInvalidConfig
	}
	config.ICEServers = cloneICEServers(config.ICEServers)
	lifetime, cancel := context.WithCancel(context.Background())
	return &HandshakeNegotiator{config: config, sessions: make(map[string]*restartSession), reserved: make(map[string]restartReservation), lifetime: lifetime, cancel: cancel, newPeer: func(pionConfig PionConfig, negotiator PionNegotiator) (PeerConnection, error) {
		return NewPionPeerConnection(pionConfig, negotiator)
	}}, nil
}

func (n *HandshakeNegotiator) Establish(ctx context.Context, attempt handshake.Attempt) error {
	if n == nil || ctx == nil || !n.validAttempt(attempt, true) {
		return handshake.ErrFailed
	}
	if !n.beginOperation() {
		return handshake.ErrClosed
	}
	defer n.wg.Done()
	return n.install(ctx, attempt.PeerID, attempt.ID, true, nil)
}

func (n *HandshakeNegotiator) Accept(ctx context.Context, attempt handshake.Attempt, incoming handshake.Signal) error {
	if n == nil || ctx == nil || !n.validAttempt(attempt, false) || !sameAttemptBinding(attempt, incoming.Attempt) {
		return handshake.ErrStale
	}
	remote, err := rank2xmpp.DecodeJingle(incoming.Payload)
	if err != nil || remote.Action != "session-initiate" || remote.SID != attempt.ID || remote.Initiator != attempt.InitiatorID || remote.Responder != n.config.LocalIdentity {
		return handshake.ErrFailed
	}
	if !n.beginOperation() {
		return handshake.ErrClosed
	}
	defer n.wg.Done()
	return n.install(ctx, attempt.InitiatorID, attempt.ID, false, &remote)
}

func (n *HandshakeNegotiator) beginOperation() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return false
	}
	n.wg.Add(1)
	return true
}

func (n *HandshakeNegotiator) operationContext(parent context.Context) (context.Context, func()) {
	operation, cancel := context.WithCancel(parent)
	stopLifetime := context.AfterFunc(n.lifetime, cancel)
	return operation, func() {
		stopLifetime()
		cancel()
	}
}

// V1 uses complete gathering, so the initiator consumes session-accept inside
// ExchangeJingle and there are no independent Apply messages.
func (n *HandshakeNegotiator) Apply(context.Context, handshake.Attempt, handshake.Signal) error {
	return handshake.ErrStale
}

func (n *HandshakeNegotiator) install(ctx context.Context, peerID, sid string, initiator bool, remote *rank2xmpp.Jingle) error {
	operation, releaseOperation := n.operationContext(ctx)
	defer releaseOperation()
	if err := n.config.Authority.AuthorizeRank1(operation, peerID); err != nil {
		return handshake.ErrFailed
	}
	ice, err := n.resolveICEConfiguration(operation)
	if err != nil {
		return handshake.ErrFailed
	}
	defer clearICEServers(ice.Servers)
	n.mu.Lock()
	closed := n.closed
	n.mu.Unlock()
	if closed {
		return handshake.ErrClosed
	}
	jingle, err := NewJingleNegotiator(JingleNegotiatorConfig{LocalIdentity: n.config.LocalIdentity, PeerIdentity: peerID, MeshID: n.config.MeshID, SID: sid, SCTPStreams: n.config.SCTPStreams, MaximumMessageBytes: n.config.MaximumMessageBytes, Remote: remote}, n.config.Exchange)
	if err != nil {
		return handshake.ErrFailed
	}
	pionServers := pionICEServers(ice.Servers)
	defer clearPionICEServers(pionServers)
	connection, err := n.newPeer(PionConfig{Initiator: initiator, ICEServers: pionServers, ICETransportPolicy: ice.Policy, ReceiveCapacity: n.config.ReceiveCapacity, Clock: n.config.Clock}, jingle)
	if err != nil {
		return handshake.ErrFailed
	}
	restartable, restartCapable := connection.(RestartablePeerConnection)
	if n.config.ICEConfiguration != nil {
		configured, ok := connection.(ReconfigurableRestartablePeerConnection)
		if !ok {
			_ = closeConnection(connection, operation)
			return handshake.ErrFailed
		}
		restartable, restartCapable = configured, true
	}
	var reservation restartReservation
	if restartCapable {
		reservation, err = n.reserveRestart(peerID)
		if err != nil {
			_ = closeConnection(connection, operation)
			return handshake.ErrFailed
		}
		defer n.cancelRestartReservation(peerID, reservation.token)
	}
	link, err := NewLink(Config{MeshID: n.config.MeshID, LocalIdentity: n.config.LocalIdentity, PeerID: peerID, MaximumFrameBytes: n.config.MaximumMessageBytes, ReceiveCapacity: n.config.ReceiveCapacity, TransferWorkers: n.config.TransferWorkers, TransferQueue: n.config.TransferQueue, Clock: n.config.Clock}, connection, n.config.TransferReceiver, n.config.TransferRouteResolver)
	if err != nil {
		_ = closeConnection(connection, operation)
		return handshake.ErrFailed
	}
	if restartCapable && !link.setRetirement(func() { n.retireRestart(peerID, reservation.token) }) {
		_ = link.Close(operation)
		return handshake.ErrFailed
	}
	if confirmErr := n.config.Authority.AuthorizeRank1(operation, peerID); confirmErr != nil {
		_ = link.Close(operation)
		return handshake.ErrFailed
	}
	if err := n.config.Manager.InstallLive(operation, peerID, link); err != nil {
		_ = link.Close(operation)
		if errors.Is(err, transport.ErrUnavailable) && link.authorityBound() && link.authorityAdmitted() {
			return handshake.ErrFailed
		}
		return err
	}
	installed := &restartSession{token: reservation.token, sid: sid, connection: restartable, link: link, changed: make(chan struct{}), retired: make(chan struct{}), expiresAt: ice.ExpiresAt, expiryChanged: make(chan struct{})}
	if restartCapable && !n.publishRestart(peerID, reservation, installed) {
		_ = n.config.Manager.RemoveLiveIf(operation, peerID, link)
		_ = link.Close(operation)
		return handshake.ErrFailed
	}
	if restartCapable {
		n.monitorICEExpiry(peerID, installed)
	}
	return nil
}

func (n *HandshakeNegotiator) resolveICEConfiguration(ctx context.Context) (ICEConfiguration, error) {
	if n.config.ICEConfiguration == nil {
		return ICEConfiguration{Servers: cloneICEServers(n.config.ICEServers), Policy: n.config.ICETransportPolicy}, nil
	}
	config, err := n.config.ICEConfiguration.ResolveICEConfiguration(ctx)
	if err != nil || !validICEConfiguration(config, n.config.Clock.Now().UTC()) {
		clearICEServers(config.Servers)
		return ICEConfiguration{}, transport.ErrUnavailable
	}
	servers := cloneICEServers(config.Servers)
	clearICEServers(config.Servers)
	config.Servers = servers
	return config, nil
}

func (n *HandshakeNegotiator) monitorICEExpiry(peerID string, session *restartSession) {
	if session == nil || n.config.ICEConfiguration == nil {
		return
	}
	n.mu.Lock()
	if n.closed || n.sessions[peerID] != session || session.terminal {
		n.mu.Unlock()
		return
	}
	n.wg.Add(1)
	n.mu.Unlock()
	go func() {
		defer n.wg.Done()
		for {
			n.mu.Lock()
			current := n.sessions[peerID] == session && !session.terminal
			expires, changed := session.expiresAt, session.expiryChanged
			n.mu.Unlock()
			if !current {
				return
			}
			if expires.IsZero() {
				select {
				case <-changed:
					continue
				case <-session.retired:
					return
				case <-n.lifetime.Done():
					return
				}
			}
			delay := expires.Sub(n.config.Clock.Now().UTC())
			if delay < 0 {
				delay = 0
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
				if n.retireRestart(peerID, session.token) {
					operation, cancel := context.WithTimeout(context.Background(), n.config.ExpiryCleanupTimeout)
					stop := context.AfterFunc(n.lifetime, cancel)
					_ = n.config.Manager.RemoveLiveIf(operation, peerID, session.link)
					stop()
					cancel()
				}
				return
			case <-changed:
				timer.Stop()
				continue
			case <-session.retired:
				timer.Stop()
				return
			case <-n.lifetime.Done():
				timer.Stop()
				return
			}
		}
	}()
}

func (n *HandshakeNegotiator) reserveRestart(peerID string) (restartReservation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.reserved[peerID].token != 0 {
		return restartReservation{}, transport.ErrUnavailable
	}
	if n.tokenExhausted || n.nextToken == ^uint64(0) {
		n.tokenExhausted = true
		return restartReservation{}, transport.ErrQueueFull
	}
	consumes := n.sessions[peerID] == nil
	consumed := len(n.sessions)
	for _, reservation := range n.reserved {
		if reservation.consumes {
			consumed++
		}
	}
	if consumes && consumed >= n.config.ReceiveCapacity {
		return restartReservation{}, transport.ErrQueueFull
	}
	n.nextToken++
	reservation := restartReservation{token: n.nextToken, consumes: consumes}
	n.reserved[peerID] = reservation
	return reservation, nil
}

func (n *HandshakeNegotiator) cancelRestartReservation(peerID string, token uint64) {
	n.mu.Lock()
	if n.reserved[peerID].token == token {
		delete(n.reserved, peerID)
	}
	n.mu.Unlock()
}

func (n *HandshakeNegotiator) publishRestart(peerID string, reservation restartReservation, session *restartSession) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || session == nil || reservation.token == 0 || n.reserved[peerID].token != reservation.token {
		return false
	}
	delete(n.reserved, peerID)
	previous := n.sessions[peerID]
	n.sessions[peerID] = session
	retireRestartSessionLocked(previous)
	return true
}

func (n *HandshakeNegotiator) retireRestart(peerID string, token uint64) bool {
	n.mu.Lock()
	retired := false
	session := n.sessions[peerID]
	if session != nil && session.token == token {
		delete(n.sessions, peerID)
		retireRestartSessionLocked(session)
		retired = true
	}
	n.mu.Unlock()
	return retired
}

func retireRestartSessionLocked(session *restartSession) {
	if session == nil || session.terminal {
		return
	}
	session.terminal = true
	if session.retired != nil {
		close(session.retired)
	}
	if session.changed != nil {
		close(session.changed)
		session.changed = nil
	}
	if session.expiryChanged != nil {
		close(session.expiryChanged)
		session.expiryChanged = nil
	}
}

// RecoverInPlace refreshes only the exact live adapter observed by the
// recovery policy. The adapter is an identity token: it prevents progress on
// an old link from authorizing mutation of, or success for, a concurrently
// installed replacement. The lexicographically smaller authenticated full
// identity owns initiation; the other endpoint waits for that exact session's
// accepted refresh.
func (n *HandshakeNegotiator) RecoverInPlace(ctx context.Context, peerID string, expected transport.Transport) error {
	expectedLink, ok := expected.(*Link)
	if !ok || expectedLink == nil || n == nil || n.config.Manager == nil || !n.config.Manager.IsLivePeer(peerID, expectedLink) {
		return transport.ErrUnavailable
	}
	return n.recoverInPlace(ctx, peerID, expectedLink)
}

// recoverInPlace contains the restart mechanics. A nil expected link is used
// only by package-local unit tests of the restart protocol; production enters
// through RecoverInPlace and is always bound to an exact Manager-owned link.
func (n *HandshakeNegotiator) recoverInPlace(ctx context.Context, peerID string, expectedLink *Link) error {
	if n == nil || ctx == nil || protocol.ValidateAgentIdentity(peerID) != nil || peerID == n.config.LocalIdentity {
		return transport.ErrInvalidConfig
	}
	if !n.beginOperation() {
		return transport.ErrUnavailable
	}
	defer n.wg.Done()
	operation, releaseOperation := n.operationContext(ctx)
	defer releaseOperation()
	ctx = operation
	n.mu.Lock()
	session := n.sessions[peerID]
	if session == nil || session.terminal || expectedLink != nil && session.link != expectedLink {
		n.mu.Unlock()
		return transport.ErrUnavailable
	}
	changed := session.changed
	retired := session.retired
	if n.config.LocalIdentity > peerID {
		session.waiters++
	}
	n.mu.Unlock()
	if n.config.LocalIdentity > peerID {
		defer func() {
			n.mu.Lock()
			session.waiters--
			n.mu.Unlock()
		}()
		select {
		case <-retired:
			return transport.ErrUnavailable
		case <-changed:
			n.mu.Lock()
			current := n.sessions[peerID] == session && !session.terminal && (expectedLink == nil || session.link == expectedLink)
			n.mu.Unlock()
			if !current {
				return transport.ErrUnavailable
			}
			if err := n.config.Authority.AuthorizeRank1(ctx, peerID); err != nil {
				return transport.ErrUnavailable
			}
			if expectedLink != nil && !n.config.Manager.IsLivePeer(peerID, expectedLink) {
				return transport.ErrUnavailable
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := n.config.Authority.AuthorizeRank1(ctx, peerID); err != nil {
		return transport.ErrUnavailable
	}
	if expectedLink != nil && !n.config.Manager.IsLivePeer(peerID, expectedLink) {
		return transport.ErrUnavailable
	}
	if err := n.restartWithCurrentICE(ctx, peerID, session, nil); err != nil {
		return err
	}
	if err := n.config.Authority.AuthorizeRank1(ctx, peerID); err != nil {
		return transport.ErrUnavailable
	}
	if expectedLink != nil && !n.config.Manager.IsLivePeer(peerID, expectedLink) {
		return transport.ErrUnavailable
	}
	if !n.completeRestart(peerID, session) {
		return transport.ErrUnavailable
	}
	if expectedLink != nil && !n.config.Manager.IsLivePeer(peerID, expectedLink) {
		return transport.ErrUnavailable
	}
	return nil
}

// HandleTransportReplace accepts only the deterministic peer owner's offer on
// the currently installed authenticated session.
func (n *HandshakeNegotiator) HandleTransportReplace(ctx context.Context, from string, signal rank2xmpp.Jingle) error {
	if n == nil || ctx == nil || from == "" || from >= n.config.LocalIdentity || signal.Action != "transport-replace" {
		return transport.ErrProtocol
	}
	if !n.beginOperation() {
		return transport.ErrUnavailable
	}
	defer n.wg.Done()
	operation, releaseOperation := n.operationContext(ctx)
	defer releaseOperation()
	ctx = operation
	n.mu.Lock()
	session := n.sessions[from]
	current := session != nil && !session.terminal
	if current {
		current = signal.SID == session.sid
	}
	n.mu.Unlock()
	boundEndpoints := signal.Initiator == from && signal.Responder == n.config.LocalIdentity || signal.Responder == from && signal.Initiator == n.config.LocalIdentity
	if !current || !boundEndpoints || signal.Content.Description.MeshID != n.config.MeshID {
		return transport.ErrUnavailable
	}
	if err := n.config.Authority.AuthorizeRank1(ctx, from); err != nil {
		return transport.ErrUnavailable
	}
	if err := n.restartWithCurrentICE(ctx, from, session, &signal); err != nil {
		return err
	}
	if err := n.config.Authority.AuthorizeRank1(ctx, from); err != nil {
		return transport.ErrUnavailable
	}
	if !n.completeRestart(from, session) {
		return transport.ErrUnavailable
	}
	return nil
}

func (n *HandshakeNegotiator) restartWithCurrentICE(ctx context.Context, peerID string, session *restartSession, remote *rank2xmpp.Jingle) error {
	if n.config.ICEConfiguration == nil {
		if remote == nil {
			return session.connection.Restart(ctx)
		}
		return session.connection.AcceptRestart(ctx, *remote)
	}
	ice, err := n.resolveICEConfiguration(ctx)
	if err != nil {
		return err
	}
	defer clearICEServers(ice.Servers)
	connection, ok := session.connection.(ReconfigurableRestartablePeerConnection)
	if !ok {
		return transport.ErrUnavailable
	}
	if remote == nil {
		err = connection.RestartWithConfiguration(ctx, ice)
	} else {
		err = connection.AcceptRestartWithConfiguration(ctx, *remote, ice)
	}
	if err == nil {
		n.mu.Lock()
		current := n.sessions[peerID] == session && !session.terminal
		if current {
			session.expiresAt = ice.ExpiresAt
			if session.expiryChanged != nil {
				close(session.expiryChanged)
			}
			session.expiryChanged = make(chan struct{})
		}
		n.mu.Unlock()
		if !current {
			return transport.ErrUnavailable
		}
	}
	return err
}

func (n *HandshakeNegotiator) completeRestart(peerID string, session *restartSession) bool {
	n.mu.Lock()
	current := n.sessions[peerID] == session && !session.terminal
	if current {
		close(session.changed)
		session.changed = make(chan struct{})
	}
	n.mu.Unlock()
	return current
}

func (n *HandshakeNegotiator) validAttempt(attempt handshake.Attempt, initiatedLocally bool) bool {
	if attempt.ID == "" || len(attempt.ID) > 128 || attempt.StartedAt.IsZero() || !attempt.Deadline.After(attempt.StartedAt) || attempt.State != handshake.AttemptRunning {
		return false
	}
	if initiatedLocally {
		return attempt.InitiatorID == n.config.LocalIdentity && attempt.PeerID != n.config.LocalIdentity && protocol.ValidateAgentIdentity(attempt.PeerID) == nil
	}
	return attempt.PeerID == n.config.LocalIdentity && attempt.InitiatorID != n.config.LocalIdentity && protocol.ValidateAgentIdentity(attempt.InitiatorID) == nil
}

func sameAttemptBinding(left, right handshake.Attempt) bool {
	return left.ID == right.ID && left.PeerID == right.PeerID && left.InitiatorID == right.InitiatorID && left.StartedAt.Equal(right.StartedAt) && left.Deadline.Equal(right.Deadline)
}

// Close terminally retires every restart token and releases copied ICE
// configuration. Link/manager closure is idempotent.
func (n *HandshakeNegotiator) Close(ctx context.Context) error {
	if n == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	n.mu.Lock()
	if n.closeDone != nil {
		done := n.closeDone
		n.mu.Unlock()
		return waitHandshakeClose(ctx, done)
	}
	n.closed = true
	done := make(chan struct{})
	n.closeDone = done
	// Retire restart sessions while holding the same lifecycle lock that fences
	// admission. This preserves the public unavailable result for passive
	// waiters even though lifetime cancellation simultaneously unblocks active,
	// cooperative restart dependencies.
	for peerID, session := range n.sessions {
		delete(n.sessions, peerID)
		retireRestartSessionLocked(session)
	}
	n.reserved = make(map[string]restartReservation)
	if n.cancel != nil {
		n.cancel()
	}
	n.mu.Unlock()
	go n.finishClose(done)
	return waitHandshakeClose(ctx, done)
}

func (n *HandshakeNegotiator) finishClose(done chan struct{}) {
	n.wg.Wait()
	// Admitted operations may still be cloning the shared static profile. The
	// lifecycle fence prevents new readers; clear it only after every existing
	// operation and expiry worker has joined.
	n.mu.Lock()
	clearICEServers(n.config.ICEServers)
	n.config.ICEServers = nil
	n.mu.Unlock()
	close(done)
}

func waitHandshakeClose(ctx context.Context, done <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneICEServers(servers []ICEServer) []ICEServer {
	cloned := make([]ICEServer, len(servers))
	for i := range servers {
		cloned[i].URLs = make([]string, len(servers[i].URLs))
		copy(cloned[i].URLs, servers[i].URLs)
		cloned[i].Username = servers[i].Username
		cloned[i].Credential = append([]byte(nil), servers[i].Credential...)
	}
	return cloned
}

func clearICEServers(servers []ICEServer) {
	for i := range servers {
		for j := range servers[i].URLs {
			servers[i].URLs[j] = ""
		}
		clear(servers[i].URLs)
		servers[i].URLs = nil
		servers[i].Username = ""
		clear(servers[i].Credential)
		servers[i].Credential = nil
	}
	clear(servers)
}

var _ handshake.Negotiator = (*HandshakeNegotiator)(nil)
