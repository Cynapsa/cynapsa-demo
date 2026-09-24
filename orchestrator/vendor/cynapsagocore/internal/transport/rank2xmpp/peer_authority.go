package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	peerAuthorityNamespace = "urn:cynapsa:peer-authority:1"
	peerAuthorityTimeout   = 10 * time.Second
	peerRevocationTimeout  = 25 * time.Second
	maximumRevokeTargets   = 256
)

// AuthorizedPeer is an exact, server-authorized session destination. The
// generation is an opaque public fencing token, never an XEP-0198 resume ID.
type AuthorizedPeer struct {
	FullJID           string
	InstallationID    string
	SessionGeneration string
}

type RevocationType string

const (
	RevokeLogical      RevocationType = "logical"
	RevokeInstallation RevocationType = "installation"
)

// Revocation names a logical peer or one particular installation session.
// Installation revokes include the bare peer name and must match both
// installation ID and session generation against current peer state.
type Revocation struct {
	Type              RevocationType
	Peer              string // canonical bare JID
	InstallationID    string
	SessionGeneration string
}

type peerAuthorizationSession interface {
	ResolveAuthorizedPeer(context.Context, string) (AuthorizedPeer, error)
}

type peerExactAuthorizationSession interface {
	ResolveAuthorizedExactPeer(context.Context, string) (AuthorizedPeer, error)
}

type peerRevocationAckSession interface {
	AcknowledgePeerRevocation(context.Context, string, uint64) error
}

// CurrentBoundIdentity returns the actual exact JID published by the current
// authenticated transport session. A configured or previously bound resource
// is not evidence for reconnect outbox revalidation.
func (c *Client) CurrentBoundIdentity() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started || c.state != DurableLive || c.session == nil {
		return ""
	}
	return c.identity.BoundIdentity
}

type revocationWork struct {
	ingress *ingressGeneration
	event   Event
}

func (c *Client) enqueuePeerRevocation(ingress *ingressGeneration, event Event) {
	if c == nil || ingress == nil || !c.ingressCurrent(ingress) || c.revocationJobs == nil {
		return
	}
	// Event's ingress-budget lease belongs to the control loop. Keep only
	// copied small metadata in this separately bounded queue.
	copyEvent := Event{Kind: EventPeerRevoked, Revocations: append([]Revocation(nil), event.Revocations...), ControlID: event.ControlID, sessionGeneration: event.sessionGeneration}
	select {
	case c.revocationJobs <- revocationWork{ingress: ingress, event: copyEvent}:
	default:
		_ = c.transitionIngressFailure(ingress.session, ingress, DurablePending, true)
	}
}

func (c *Client) revocationLoop() {
	defer c.wg.Done()
	for {
		select {
		case work := <-c.revocationJobs:
			if !c.handlePeerRevocation(work.ingress, work.event) && c.ingressCurrent(work.ingress) {
				_ = c.transitionIngressFailure(work.ingress.session, work.ingress, DurablePending, true)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// ResolveAuthorizedPeer asks the connected server for one exact live peer.
// No local membership snapshot is consulted. Every new handshake should make
// a fresh query; the result is scoped to this published XMPP session.
func (c *Client) ResolveAuthorizedPeer(ctx context.Context, bare string) (AuthorizedPeer, error) {
	if !canonicalBarePeer(bare) {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	return c.resolveAuthorizedPeer(ctx, bare, false)
}

// ResolveAuthorizedExactPeer validates an inbound full-JID sender with the
// server before admitting its first application stanza to a peer lane.
func (c *Client) ResolveAuthorizedExactPeer(ctx context.Context, full string) (AuthorizedPeer, error) {
	if !canonicalFullPeer(full) {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	return c.resolveAuthorizedPeer(ctx, full, true)
}

func (c *Client) resolveAuthorizedPeer(ctx context.Context, requested string, exact bool) (AuthorizedPeer, error) {
	if c == nil || ctx == nil {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return AuthorizedPeer{}, err
	}
	c.mu.Lock()
	session, lifetime, generation, epoch := c.session, c.ctx, c.generation, c.sessionEpoch
	ready := c.started && !c.closed && c.state == DurableLive && c.membershipReady && session != nil
	local := c.identity.BoundIdentity
	c.mu.Unlock()
	localJID, err := jid.Parse(local)
	requestedJID, requestErr := jid.Parse(requested)
	if err != nil || requestErr != nil || localJID.Domainpart() == "" || requestedJID.Domainpart() != localJID.Domainpart() {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	if !ready || lifetime == nil {
		return AuthorizedPeer{}, ErrUnavailable
	}
	operation, cancel := context.WithTimeout(ctx, peerAuthorityTimeout)
	stop := context.AfterFunc(lifetime, cancel)
	defer func() { stop(); cancel() }()
	var peer AuthorizedPeer
	if exact {
		source, ok := session.(peerExactAuthorizationSession)
		if !ok {
			return AuthorizedPeer{}, ErrUnavailable
		}
		peer, err = source.ResolveAuthorizedExactPeer(operation, requested)
	} else {
		source, ok := session.(peerAuthorizationSession)
		if !ok {
			return AuthorizedPeer{}, ErrUnavailable
		}
		peer, err = source.ResolveAuthorizedPeer(operation, requested)
	}
	c.mu.Lock()
	current := !c.closed && c.started && c.state == DurableLive && c.membershipReady && c.session == session && c.generation == generation && c.sessionEpoch == epoch
	c.mu.Unlock()
	if !current {
		return AuthorizedPeer{}, ErrUnavailable
	}
	if err != nil {
		if ctx.Err() != nil {
			return AuthorizedPeer{}, ctx.Err()
		}
		return AuthorizedPeer{}, err
	}
	if !validAuthorizedPeer(peer, requested) {
		return AuthorizedPeer{}, ErrProtocol
	}
	return peer, nil
}

// SetRevocationHandler installs the owner's synchronous peer cleanup. A
// successful return is the sole permission to send the action acknowledgement.
// Installing it twice is forbidden because a stale callback could ACK without
// cleaning the current owner.
func (c *Client) SetRevocationHandler(handler func(context.Context, Revocation) error) error {
	if c == nil || handler == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.revocationHandler != nil {
		return ErrUnavailable
	}
	c.revocationHandler = handler
	return nil
}

// SetRoutingFailureHandler reports a validated server-originated routing
// error for one outbound application message, without changing XMPP liveness.
func (c *Client) SetRoutingFailureHandler(handler func(string, string)) error {
	if c == nil || handler == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.routingFailureHandler != nil {
		return ErrUnavailable
	}
	c.routingFailureHandler = handler
	return nil
}

func (c *Client) handlePeerRevocation(ingress *ingressGeneration, event Event) bool {
	emitRank2Evidence(c.evidenceRecord("peer_revocation_started", "control", "action", ingress), nil)
	if c == nil || ingress == nil || event.ControlID == "" || len(event.Revocations) == 0 {
		return false
	}
	c.mu.Lock()
	handler := c.revocationHandler
	c.mu.Unlock()
	if handler == nil || !c.ingressCurrent(ingress) {
		emitRank2Evidence(c.evidenceRecord("peer_revocation_failed", "control", "handler_missing_or_stale", ingress), nil)
		return false // no cleanup proof: do not ACK
	}
	operation, cancel := context.WithTimeout(ingress.ctx, peerRevocationTimeout)
	defer cancel()
	for _, revoke := range event.Revocations {
		if err := callRevocationHandler(handler, operation, revoke); err != nil {
			emitRank2Evidence(c.evidenceRecord("peer_revocation_failed", "control", "handler", ingress), err)
			return false
		}
		if err := operation.Err(); err != nil || !c.ingressCurrent(ingress) {
			emitRank2Evidence(c.evidenceRecord("peer_revocation_failed", "control", "handler_fence", ingress), err)
			return false
		}
	}
	ack, ok := ingress.session.(peerRevocationAckSession)
	if !ok {
		emitRank2Evidence(c.evidenceRecord("peer_revocation_failed", "control", "ack_session_missing", ingress), nil)
		return false
	}
	if err := ack.AcknowledgePeerRevocation(operation, event.ControlID, event.sessionGeneration); err != nil {
		emitRank2Evidence(c.evidenceRecord("peer_revocation_failed", "control", "action_ack", ingress), err)
		// Failure is not success. The server's action-ACK deadline will fence
		// this session, even if its XEP-0198 packet was acknowledged.
		return false
	}
	emitRank2Evidence(c.evidenceRecord("peer_revocation_completed", "control", "action_ack", ingress), nil)
	return true
}

func callRevocationHandler(handler func(context.Context, Revocation) error, ctx context.Context, revoke Revocation) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return handler(ctx, revoke)
}

func canonicalBarePeer(value string) bool {
	if protocol.ValidateAgentIdentity(value) != nil {
		return false
	}
	parsed, err := jid.Parse(value)
	return err == nil && parsed.Localpart() != "" && parsed.Domainpart() != "" && parsed.Resourcepart() == "" && parsed.String() == value
}

func canonicalFullPeer(value string) bool {
	if protocol.ValidateAgentIdentity(value) != nil {
		return false
	}
	parsed, err := jid.Parse(value)
	return err == nil && parsed.Localpart() != "" && parsed.Domainpart() != "" && parsed.Resourcepart() != "" && parsed.String() == value
}

func canonicalPeerRequest(value string) bool {
	return canonicalBarePeer(value) || canonicalFullPeer(value)
}

func validAuthorityToken(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			if char < 'a' || char > 'z' {
				if char < 'A' || char > 'Z' {
					if char != '-' && char != '_' && char != '.' {
						return false
					}
				}
			}
		}
	}
	return true
}

func validAuthorizedPeer(peer AuthorizedPeer, requested string) bool {
	if !canonicalPeerRequest(requested) || !validAuthorityToken(peer.InstallationID) || !validAuthorityToken(peer.SessionGeneration) {
		return false
	}
	parsed, err := jid.Parse(peer.FullJID)
	requestJID, requestErr := jid.Parse(requested)
	if err != nil || requestErr != nil || parsed.Resourcepart() == "" || parsed.String() != peer.FullJID || parsed.Bare().String() != requestJID.Bare().String() || !strings.HasPrefix(parsed.Resourcepart(), "r2."+peer.InstallationID+".") {
		return false
	}
	if requestJID.Resourcepart() != "" && peer.FullJID != requested {
		return false
	}
	return true
}

func validRevocation(revoke Revocation) bool {
	switch revoke.Type {
	case RevokeLogical:
		return canonicalBarePeer(revoke.Peer) && revoke.InstallationID == "" && revoke.SessionGeneration == ""
	case RevokeInstallation:
		return canonicalBarePeer(revoke.Peer) && validAuthorityToken(revoke.InstallationID) && validAuthorityToken(revoke.SessionGeneration)
	default:
		return false
	}
}

func (s *melliumSession) ResolveAuthorizedPeer(ctx context.Context, bare string) (AuthorizedPeer, error) {
	if !canonicalBarePeer(bare) {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	return s.resolveAuthorizedPeer(ctx, bare)
}

func (s *melliumSession) ResolveAuthorizedExactPeer(ctx context.Context, full string) (AuthorizedPeer, error) {
	if !canonicalFullPeer(full) {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	return s.resolveAuthorizedPeer(ctx, full)
}

// PreparedReplayPeers returns metadata only. The resumed wire session has
// already been published by PrepareResume, but no application replay may
// begin until each exact recipient passes a new server authorization IQ.
func (s *melliumSession) PreparedReplayPeers(ctx context.Context) ([]string, error) {
	if s == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.suspended || s.resumeGeneration == 0 || s.resumeGeneration != s.generation {
		return nil, ErrUnavailable
	}
	peers := make(map[string]struct{})
	for _, record := range s.resumeReplay {
		if isSessionControlStanza(record.Kind) {
			continue
		}
		if !canonicalFullPeer(record.To) {
			return nil, ErrProtocol
		}
		peers[record.To] = struct{}{}
	}
	result := make([]string, 0, len(peers))
	for peer := range peers {
		result = append(result, peer)
	}
	return result, nil
}

func (s *melliumSession) resolveAuthorizedPeer(ctx context.Context, requested string) (AuthorizedPeer, error) {
	if s == nil || ctx == nil {
		return AuthorizedPeer{}, ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return AuthorizedPeer{}, err
	}
	defer s.releaseCorrelated()
	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return AuthorizedPeer{}, ErrUnavailable
	}
	local, server := session.LocalAddr(), session.LocalAddr().Domain()
	peerJID, parseErr := jid.Parse(requested)
	if local.Localpart() == "" || local.Resourcepart() != s.boundResource() || server.Domainpart() == "" || parseErr != nil || peerJID.Domainpart() != server.Domainpart() {
		return AuthorizedPeer{}, ErrIdentityBinding
	}
	random := rand.Text()
	if len(random) < privateIQRandomLength {
		return AuthorizedPeer{}, ErrUnavailable
	}
	id := "cynapsa-peer-authorize-" + random[:privateIQRandomLength]
	record := Stanza{Kind: StanzaPeerAuthorization, From: local.String(), To: server.String(), MeshID: s.meshID, MessageID: id}
	request := xml.StartElement{Name: xml.Name{Space: peerAuthorityNamespace, Local: "authorize"}, Attr: []xml.Attr{{Name: xml.Name{Local: "peer"}, Value: requested}}}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: server, Type: stanza.GetIQ}
	if err := s.rememberIssuedPeerAuthorization(session, management, requested, record); err != nil {
		return AuthorizedPeer{}, err
	}
	emitRank2Evidence(rank2EvidenceRecord{Event: "peer_authorization_request", Source: "control", Stage: "send_start", StanzaID: id, To: requested}, nil)
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xmlstream.Wrap(nil, request), iq)
	emitRank2Evidence(rank2EvidenceRecord{Event: "peer_authorization_request", Source: "control", Stage: "send_return", StanzaID: id, To: requested, Admitted: response != nil}, err)
	if err != nil {
		return AuthorizedPeer{}, err
	}
	if response == nil {
		return AuthorizedPeer{}, ErrProtocol
	}
	defer response.Close()
	peer, correlated, handled, err := decodeAuthorizedPeer(response, id, server, local, requested, s.config.StanzaBudgetBytes, true)
	emitRank2Evidence(rank2EvidenceRecord{Event: "peer_authorization_request", Source: "control", Stage: "decode_return", StanzaID: id, To: requested, Correlated: correlated, Handled: handled}, err)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return AuthorizedPeer{}, confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return AuthorizedPeer{}, emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	return peer, err
}

func decodeAuthorizedPeer(source xml.TokenReader, expectedID string, server, local jid.JID, requested string, budget int, mellium bool) (AuthorizedPeer, bool, bool, error) {
	if source == nil || expectedID == "" || !canonicalPeerRequest(requested) || budget <= 0 {
		return AuthorizedPeer{}, false, false, ErrProtocol
	}
	token, err := source.Token()
	outer, ok := token.(xml.StartElement)
	if err != nil || !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return AuthorizedPeer{}, false, false, ErrProtocol
	}
	iqType, ok := validCorrelatedIQAttrs(outer.Attr, expectedID, server, local)
	if !ok {
		return AuthorizedPeer{}, false, false, ErrProtocol
	}
	bounded, err := newStanzaBudget(source, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return AuthorizedPeer{}, true, false, ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	if iqType == stanza.ErrorIQ {
		failure, valid := decodePeerAuthorizationIQError(reader, outer, requested, mellium)
		if valid && errors.Is(failure, ErrProtocol) {
			switch {
			case strings.Contains(failure.Error(), "iq-error-condition-forbidden"):
				failure = ErrAuthentication
			case strings.Contains(failure.Error(), "iq-error-condition-item-not-found"):
				failure = ErrUnavailable
			}
		}
		return AuthorizedPeer{}, true, valid, failure
	}
	token, err = nextPrivateToken(reader)
	start, ok := token.(xml.StartElement)
	if err != nil || !ok || start.Name != (xml.Name{Space: peerAuthorityNamespace, Local: "authorized"}) {
		return AuthorizedPeer{}, true, false, ErrProtocol
	}
	attrs, ok := exactPrivateAttrs(start.Attr, []string{"peer", "installation-id", "session-generation"}, peerAuthorityNamespace)
	if !ok {
		return AuthorizedPeer{}, true, false, ErrProtocol
	}
	peer := AuthorizedPeer{FullJID: attrs["peer"], InstallationID: attrs["installation-id"], SessionGeneration: attrs["session-generation"]}
	if !validAuthorizedPeer(peer, requested) {
		return AuthorizedPeer{}, true, false, ErrProtocol
	}
	end, err := nextPrivateToken(reader)
	if err != nil || end != start.End() || consumePrivateIQEnd(reader, outer, mellium) != nil {
		return AuthorizedPeer{}, true, false, ErrProtocol
	}
	return peer, true, true, nil
}

// XMPP stanza errors echo the original IQ child before <error>. Older peers
// may omit that echo. Admit only the exact authorize request we sent, then
// delegate the error and outer-IQ boundary checks to the shared decoder.
func decodePeerAuthorizationIQError(reader xml.TokenReader, outer xml.StartElement, requested string, mellium bool) (error, bool) {
	first, err := nextPrivateToken(reader)
	if err != nil {
		return ErrProtocol, false
	}
	if echo, ok := first.(xml.StartElement); ok && echo.Name == (xml.Name{Space: peerAuthorityNamespace, Local: "authorize"}) {
		attrs, valid := exactPrivateAttrs(echo.Attr, []string{"peer"}, peerAuthorityNamespace)
		if !valid || attrs["peer"] != requested {
			return ErrProtocol, false
		}
		end, err := nextPrivateToken(reader)
		if err != nil || end != echo.End() {
			return ErrProtocol, false
		}
		return decodePrivateIQError(reader, outer, mellium)
	}
	return decodePrivateIQError(&prependXMLTokenReader{first: first, source: reader}, outer, mellium)
}

// decodePeerRevocation consumes one server-issued batch control payload.
func decodePeerRevocation(source xml.TokenReader, outer xml.StartElement, budget int) ([]Revocation, error) {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return nil, ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	token, err := nextPrivateToken(reader)
	start, ok := token.(xml.StartElement)
	if err != nil || !ok || start.Name != (xml.Name{Space: peerAuthorityNamespace, Local: "revoke"}) {
		return nil, ErrProtocol
	}
	revokes, _, err := decodePeerRevocationFromStart(reader, children, start)
	return revokes, err
}

func decodePeerRevocationFromStart(reader xml.TokenReader, children *outerBoundaryTokenReader, start xml.StartElement) ([]Revocation, string, error) {
	attrs, ok := exactPrivateAttrs(start.Attr, []string{"type", "action-id"}, peerAuthorityNamespace)
	if !ok || !validAuthorityToken(attrs["action-id"]) || attrs["type"] != string(RevokeLogical) && attrs["type"] != string(RevokeInstallation) {
		return nil, "", ErrProtocol
	}
	kind := RevocationType(attrs["type"])
	revokes := make([]Revocation, 0, 1)
	seen := make(map[string]struct{})
	for {
		token, err := nextPrivateToken(reader)
		if err != nil {
			return nil, "", ErrProtocol
		}
		switch value := token.(type) {
		case xml.StartElement:
			if value.Name != (xml.Name{Space: peerAuthorityNamespace, Local: "target"}) || len(revokes) >= maximumRevokeTargets {
				return nil, "", ErrProtocol
			}
			revoke := Revocation{Type: kind}
			if kind == RevokeLogical {
				target, valid := exactPrivateAttrs(value.Attr, []string{"agent"}, peerAuthorityNamespace)
				if !valid {
					return nil, "", ErrProtocol
				}
				revoke.Peer = target["agent"]
			} else {
				target, valid := exactPrivateAttrs(value.Attr, []string{"agent", "installation-id", "session-generation"}, peerAuthorityNamespace)
				if !valid {
					return nil, "", ErrProtocol
				}
				revoke.Peer, revoke.InstallationID, revoke.SessionGeneration = target["agent"], target["installation-id"], target["session-generation"]
			}
			if !validRevocation(revoke) {
				return nil, "", ErrProtocol
			}
			end, err := nextPrivateToken(reader)
			if err != nil || end != value.End() {
				return nil, "", ErrProtocol
			}
			key := string(kind) + ":" + revoke.Peer + ":" + revoke.InstallationID + ":" + revoke.SessionGeneration
			if _, duplicate := seen[key]; duplicate {
				return nil, "", ErrProtocol
			}
			seen[key] = struct{}{}
			revokes = append(revokes, revoke)
		case xml.EndElement:
			if value != start.End() || len(revokes) == 0 {
				return nil, "", ErrProtocol
			}
			if token, err := nextPrivateToken(reader); err != io.EOF || token != nil || !children.done {
				return nil, "", ErrProtocol
			}
			return revokes, attrs["action-id"], nil
		default:
			return nil, "", ErrProtocol
		}
	}
}

// decodeServerAuthoritySet preserves the legacy membership-change packet
// during rollout, while admitting the new revoke packet on the same trusted
// server IQ-set route. It reads at most one bounded stanza before dispatch.
func decodeServerAuthoritySet(source xml.TokenReader, outer xml.StartElement, budget int) (EventKind, []Revocation, string, error) {
	if source == nil || budget <= 0 {
		return 0, nil, "", ErrProtocol
	}
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return 0, nil, "", ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	first, err := nextPrivateToken(reader)
	start, ok := first.(xml.StartElement)
	if err != nil || !ok {
		return 0, nil, "", ErrProtocol
	}
	switch start.Name {
	case xml.Name{Space: authorityNamespace, Local: "membership-changed"}:
		if err := decodeMembershipChangedBody(&prependXMLTokenReader{first: start, source: reader}, children); err != nil {
			return 0, nil, "", err
		}
		return EventMembershipChanged, nil, "", nil
	case xml.Name{Space: peerAuthorityNamespace, Local: "revoke"}:
		revokes, actionID, err := decodePeerRevocationFromStart(reader, children, start)
		if err != nil {
			return 0, nil, "", err
		}
		return EventPeerRevoked, revokes, actionID, nil
	default:
		return 0, nil, "", ErrProtocol
	}
}

type prependXMLTokenReader struct {
	first  xml.Token
	source xml.TokenReader
}

func (r *prependXMLTokenReader) Token() (xml.Token, error) {
	if r.first != nil {
		first := r.first
		r.first = nil
		return first, nil
	}
	return r.source.Token()
}

func (s *melliumSession) AcknowledgePeerRevocation(ctx context.Context, id string, generation uint64) error {
	if s == nil || ctx == nil || id == "" || generation == 0 {
		return ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return err
	}
	defer s.releaseCorrelated()
	s.mu.Lock()
	session, management := s.session, s.management
	current := !s.closed && !s.suspended && s.generation == generation && session != nil && management != nil
	s.mu.Unlock()
	if !current {
		return ErrUnavailable
	}
	local, server := session.LocalAddr(), session.LocalAddr().Domain()
	if local.Localpart() == "" || local.Resourcepart() != s.boundResource() {
		return ErrIdentityBinding
	}
	random := rand.Text()
	if len(random) < privateIQRandomLength {
		return ErrUnavailable
	}
	requestID := "cynapsa-revoke-applied-" + random[:privateIQRandomLength]
	record := Stanza{Kind: StanzaPeerRevocationAck, From: local.String(), To: server.String(), MeshID: s.meshID, MessageID: requestID}
	request := xml.StartElement{Name: xml.Name{Space: peerAuthorityNamespace, Local: "applied"}, Attr: []xml.Attr{{Name: xml.Name{Local: "control-id"}, Value: id}}}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: requestID, To: server, Type: stanza.SetIQ}
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xmlstream.Wrap(nil, request), iq)
	if err != nil {
		emitRank2Evidence(rank2EvidenceRecord{Event: "peer_revocation_action_failed", Source: "control", Stage: "send"}, err)
		return err
	}
	if response == nil {
		emitRank2Evidence(rank2EvidenceRecord{Event: "peer_revocation_action_failed", Source: "control", Stage: "missing_result"}, ErrProtocol)
		return ErrProtocol
	}
	defer response.Close()
	if err := decodePeerRevocationAppliedResult(response, requestID, server, local); err != nil {
		emitRank2Evidence(rank2EvidenceRecord{Event: "peer_revocation_action_failed", Source: "control", Stage: "decode_result"}, err)
		return err
	}
	ordinal, count, err := management.ConfirmCorrelatedHandled(sequence)
	if err != nil {
		emitRank2Evidence(rank2EvidenceRecord{Event: "peer_revocation_action_failed", Source: "control", Stage: "confirm_handled"}, err)
		return err
	}
	if count > 0 {
		if err := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); err != nil {
			return err
		}
	}
	management.MarkHandledInbound()
	return nil
}

func decodePeerRevocationAppliedResult(source xml.TokenReader, expectedID string, server, local jid.JID) error {
	if source == nil || expectedID == "" {
		return ErrProtocol
	}
	token, err := source.Token()
	outer, ok := token.(xml.StartElement)
	if err != nil || !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return ErrProtocol
	}
	kind, ok := validCorrelatedIQAttrs(outer.Attr, expectedID, server, local)
	if !ok || kind != stanza.ResultIQ {
		emitRank2Evidence(rank2EvidenceRecord{Event: "peer_revocation_action_result_rejected", Source: "control", Stage: string(kind), Admitted: ok}, ErrProtocol)
		return ErrProtocol
	}
	reader := &privateIQTokenReader{source: source, remaining: maximumPrivateIQTokens}
	return consumePrivateIQEnd(reader, outer, true)
}
