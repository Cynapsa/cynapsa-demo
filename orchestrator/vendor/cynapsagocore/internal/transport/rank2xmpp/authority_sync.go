package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"sort"
	"strconv"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	authorityNamespace    = "urn:cynapsa:mesh-authority:1"
	maximumAuthorityPeers = 65_536
	maximumAuthorityPage  = 256
)

// AuthoritySnapshot is one complete current-member vector read from one
// exact authenticated mesh session. The private fields are a
// process-local stale-install capability; they are not wire metadata or a
// membership revision.
type AuthoritySnapshot struct {
	Members []string

	session             Session
	sessionEpoch        uint64
	authorityGeneration uint64
	nonce               string
}

func (snapshot AuthoritySnapshot) clone() AuthoritySnapshot {
	snapshot.Members = append([]string(nil), snapshot.Members...)
	return snapshot
}

type authoritySyncSession interface {
	SyncAuthority(context.Context) (AuthoritySnapshot, error)
}

// refreshAuthorityForSession obtains a complete membership snapshot from a
// recovery-owned authenticated session before application traffic may resume.
func (c *Client) refreshAuthorityForSession(ctx context.Context, session Session) error {
	if c == nil || ctx == nil || session == nil {
		return ErrInvalidConfig
	}
	snapshot, err := synchronizeAuthoritySession(ctx, session, c.config.Auth.MeshID, c.config.Auth.Username+"/"+c.boundResource())
	if err != nil {
		return err
	}
	if snapshot.nonce == "" {
		return ErrUnavailable
	}
	c.mu.Lock()
	exhausted := c.authorityExhausted
	if c.session == session && c.sessionEpoch != 0 {
		snapshot.session = session
		snapshot.sessionEpoch = c.sessionEpoch
		if c.pauseMembershipLocked(snapshot.nonce) {
			snapshot.authorityGeneration = c.authorityGeneration
			c.authoritySnapshot = snapshot.clone()
		}
	}
	exhausted = c.authorityExhausted
	c.mu.Unlock()
	if exhausted {
		return ErrUnavailable
	}
	return nil
}

// authoritativeQuerySessionError fences a multi-page result to the exact
// authenticated session publication on which synchronization began.
func (c *Client) authoritativeQuerySessionError(lifetimeGeneration, sessionEpoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation != lifetimeGeneration {
		return ErrClosed
	}
	if !c.started || c.state == DurablePending || c.session == nil || c.sessionEpoch != sessionEpoch {
		return ErrUnavailable
	}
	return nil
}

// SyncAuthority downloads the complete current membership from the exact
// authenticated session. Callers must install Members into the membership
// owner and then call AcknowledgeAuthoritySnapshot before application ingress
// can be published.
func (c *Client) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	if c == nil || ctx == nil {
		return AuthoritySnapshot{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, err
	}
	c.mu.Lock()
	session, lifetime, clientGeneration, sessionEpoch, authorityGeneration := c.session, c.ctx, c.generation, c.sessionEpoch, c.authorityGeneration
	authorityExhausted := c.authorityExhausted
	started, closed, state := c.started, c.closed, c.state
	cached := c.authoritySnapshot.clone()
	c.mu.Unlock()
	if closed {
		return AuthoritySnapshot{}, ErrClosed
	}
	if authorityExhausted {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	if !started || lifetime == nil || state == DurablePending || session == nil {
		if lifetime != nil && lifetime.Err() == nil {
			c.requestReconnect()
		}
		return AuthoritySnapshot{}, ErrUnavailable
	}
	if cached.session == session && cached.sessionEpoch == sessionEpoch && cached.authorityGeneration == authorityGeneration && cached.nonce != "" {
		return cached, nil
	}
	source, ok := session.(authoritySyncSession)
	if !ok {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	operation, cancel := context.WithTimeout(ctx, privateIQTimeout)
	stop := context.AfterFunc(lifetime, cancel)
	defer func() { stop(); cancel() }()
	snapshot, err := source.SyncAuthority(operation)
	if err != nil {
		if stateErr := c.authoritativeQuerySessionError(clientGeneration, sessionEpoch); stateErr != nil {
			return AuthoritySnapshot{}, stateErr
		}
		if ctx.Err() != nil {
			return AuthoritySnapshot{}, ctx.Err()
		}
		if operation.Err() != nil {
			return AuthoritySnapshot{}, ErrUnavailable
		}
		return AuthoritySnapshot{}, err
	}
	localFull := c.config.Auth.Username + "/" + c.boundResource()
	local, parseErr := jid.Parse(localFull)
	if parseErr != nil || validateAuthoritySnapshot(snapshot, c.config.Auth.MeshID, localFull, local.Domainpart()) != nil {
		return AuthoritySnapshot{}, ErrProtocol
	}
	snapshot.nonce = newAuthorityCapability()
	if snapshot.nonce == "" {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	c.mu.Lock()
	current := !c.closed && !c.authorityExhausted && c.started && c.session == session && c.sessionEpoch == sessionEpoch && c.authorityGeneration == authorityGeneration
	if current {
		snapshot.session = session
		snapshot.sessionEpoch = sessionEpoch
		current = c.pauseMembershipLocked(snapshot.nonce)
		if current {
			snapshot.authorityGeneration = c.authorityGeneration
			c.authoritySnapshot = snapshot.clone()
		}
	}
	c.mu.Unlock()
	if !current {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	return snapshot.clone(), nil
}

// AcknowledgeAuthoritySnapshot opens application delivery only after the
// membership owner has atomically installed this exact complete snapshot.
// A snapshot from a replaced session, or one superseded by a live wakeup,
// fails closed.
func (c *Client) AcknowledgeAuthoritySnapshot(snapshot AuthoritySnapshot) error {
	return c.acknowledgeAuthoritySnapshot(snapshot, nil)
}

// AcknowledgeAuthoritySnapshotIf opens Rank2 admission only while current
// still validates the caller's exact local publication capability. The
// callback runs under the client publication lock, so a fence that has already
// superseded that capability cannot race between validation and the Rank2
// ready-state linearization point.
func (c *Client) AcknowledgeAuthoritySnapshotIf(snapshot AuthoritySnapshot, current func() bool) error {
	if current == nil {
		return ErrInvalidConfig
	}
	return c.acknowledgeAuthoritySnapshot(snapshot, current)
}

func (c *Client) acknowledgeAuthoritySnapshot(snapshot AuthoritySnapshot, current func() bool) error {
	if c == nil || snapshot.session == nil || snapshot.sessionEpoch == 0 || snapshot.authorityGeneration == 0 || snapshot.nonce == "" {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.authorityExhausted || !c.started || c.state == DurablePending || c.session != snapshot.session || c.sessionEpoch != snapshot.sessionEpoch || c.authorityGeneration != snapshot.authorityGeneration || c.authorityPendingNonce != snapshot.nonce {
		c.mu.Unlock()
		return ErrUnavailable
	}
	if current != nil && !current() {
		c.mu.Unlock()
		return ErrUnavailable
	}
	hasReplay := len(c.replay) != 0
	lifetime := c.ctx
	c.mu.Unlock()
	if hasReplay {
		if lifetime == nil {
			return ErrUnavailable
		}
		timeout := c.config.ReconnectOperationTimeout
		if timeout <= 0 {
			timeout = time.Nanosecond
		}
		operation, cancel := context.WithTimeout(lifetime, timeout)
		err := c.replayInstalledAuthority(operation, snapshot, current)
		cancel()
		if err != nil {
			return err
		}
	}
	c.mu.Lock()
	if c.closed || !c.started || c.state == DurablePending || c.session != snapshot.session || c.sessionEpoch != snapshot.sessionEpoch || c.authorityGeneration != snapshot.authorityGeneration || c.authorityPendingNonce != snapshot.nonce {
		c.mu.Unlock()
		return ErrUnavailable
	}
	if current != nil && !current() {
		c.mu.Unlock()
		return ErrUnavailable
	}
	c.authorityPendingNonce = ""
	c.authoritySnapshot = AuthoritySnapshot{}
	c.membershipReady = true
	c.retainedDataAuthority = false
	if c.membershipReadySignal != nil {
		close(c.membershipReadySignal)
		c.membershipReadySignal = nil
	}
	handled := c.authorityHandledPending
	c.authorityHandledPending = 0
	c.mu.Unlock()
	if handled != 0 {
		return c.markRank2Handled(handled)
	}
	return nil
}

// replayInstalledAuthority publishes clean-session replay only after the
// composed authority owner has installed the exact complete snapshot. Failed
// sends retain the complete replay set: stable message IDs make a later retry
// safe, while sending before installation could escape a peer-removal fence.
func (c *Client) replayInstalledAuthority(ctx context.Context, snapshot AuthoritySnapshot, current func() bool) (err error) {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err = c.sendMu.LockContext(ctx); err != nil {
		return err
	}
	defer c.sendMu.Unlock()

	c.mu.Lock()
	valid := !c.closed && c.started && c.state != DurablePending && c.session == snapshot.session &&
		c.sessionEpoch == snapshot.sessionEpoch && c.authorityGeneration == snapshot.authorityGeneration &&
		c.authorityPendingNonce == snapshot.nonce
	if valid && current != nil {
		valid = current()
	}
	if !valid {
		c.mu.Unlock()
		return ErrUnavailable
	}
	retained := c.replay
	c.replay = nil
	local, session, ingress := c.identity.BoundIdentity, c.session, c.ingress
	owned, handled := c.outbox.Rank2Metadata(), c.rank2Handled
	c.mu.Unlock()

	defer func() {
		if len(retained) == 0 {
			return
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			clearStanzas(retained)
			return
		}
		if len(c.replay) == 0 {
			c.replay = retained
			retained = nil
			c.mu.Unlock()
			return
		}
		existing := c.replay
		merged := make([]Stanza, 0, len(existing)+len(retained))
		merged = append(merged, existing...)
		merged = append(merged, retained...)
		for index := range existing {
			existing[index] = Stanza{}
		}
		for index := range retained {
			retained[index] = Stanza{}
		}
		c.replay = merged
		retained = nil
		c.mu.Unlock()
	}()

	retained, err = filterReplayAuthority(retained, snapshot.Members, local, c.config.Auth.MeshID)
	if err != nil {
		return err
	}
	owned = filterOwnedReplayAuthority(owned, snapshot.Members)
	retained, err = reconcileRank2Replay(retained, owned, local, c.config.Auth.MeshID, handled)
	if err != nil {
		return err
	}
	for _, record := range retained {
		if err = ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		valid = !c.closed && c.started && c.state != DurablePending && c.session == session && c.ingress == ingress &&
			c.sessionEpoch == snapshot.sessionEpoch && c.authorityGeneration == snapshot.authorityGeneration &&
			c.authorityPendingNonce == snapshot.nonce
		if valid && current != nil {
			valid = current()
		}
		c.mu.Unlock()
		if !valid {
			return ErrUnavailable
		}
		if err = c.sendReplayStanza(session, ctx, local, record); err != nil {
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			return normalize(err, ctx, ErrUnavailable)
		}
	}
	clearStanzas(retained)
	retained = nil
	return nil
}

func synchronizeAuthoritySession(ctx context.Context, session Session, meshID, localFull string) (AuthoritySnapshot, error) {
	source, ok := session.(authoritySyncSession)
	if !ok {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	snapshot, err := source.SyncAuthority(ctx)
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	local, parseErr := jid.Parse(localFull)
	if parseErr != nil || validateAuthoritySnapshot(snapshot, meshID, localFull, local.Domainpart()) != nil {
		return AuthoritySnapshot{}, ErrProtocol
	}
	snapshot.nonce = newAuthorityCapability()
	if snapshot.nonce == "" {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	return snapshot, nil
}

func validateAuthoritySnapshot(snapshot AuthoritySnapshot, meshID, localFull, authorityDomain string) error {
	if len(snapshot.Members) == 0 || len(snapshot.Members) > maximumAuthorityPeers {
		return ErrProtocol
	}
	localFound := false
	for index, member := range snapshot.Members {
		parsed, err := jid.Parse(member)
		if err != nil || parsed.Localpart() == "" || parsed.Domainpart() != authorityDomain || parsed.Resourcepart() == "" || parsed.String() != member || index > 0 && snapshot.Members[index-1] >= member {
			return ErrProtocol
		}
		localFound = localFound || member == localFull
	}
	if !localFound {
		return ErrProtocol
	}
	return nil
}

// newAuthorityCapability mints process-local stale-publication identity after
// a complete snapshot has been validated. Session implementations return only
// public member data and cannot forge this capability.
func newAuthorityCapability() string {
	value := rand.Text()
	if len(value) < privateIQRandomLength {
		return ""
	}
	return value[:privateIQRandomLength]
}

func (s *melliumSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	if s == nil || ctx == nil {
		return AuthoritySnapshot{}, ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return AuthoritySnapshot{}, err
	}
	defer s.releaseCorrelated()
	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return AuthoritySnapshot{}, ErrUnavailable
	}
	local, server := session.LocalAddr(), session.LocalAddr().Domain()
	if local.Localpart() == "" || local.Resourcepart() == "" || local.Resourcepart() != s.resource || server.Domainpart() == "" {
		return AuthoritySnapshot{}, ErrIdentityBinding
	}
	nonce := rand.Text()
	if len(nonce) < privateIQRandomLength {
		return AuthoritySnapshot{}, ErrProtocol
	}
	nonce = nonce[:privateIQRandomLength]
	result := AuthoritySnapshot{nonce: nonce}
	var total uint64
	for cursor := uint64(0); ; {
		page, err := s.queryAuthorityPage(ctx, session, management, local, server, nonce, cursor)
		if err != nil {
			return AuthoritySnapshot{}, err
		}
		if cursor == 0 {
			total = page.Total
		}
		if page.Cursor != cursor || page.Total != total || total == 0 || total > maximumAuthorityPeers || uint64(len(page.Members)) > maximumAuthorityPage || uint64(len(result.Members))+uint64(len(page.Members)) > total {
			return AuthoritySnapshot{}, ErrProtocol
		}
		result.Members = append(result.Members, page.Members...)
		end := uint64(len(result.Members))
		if page.Next == 0 {
			if end != total {
				return AuthoritySnapshot{}, ErrProtocol
			}
			break
		}
		if page.Next != end || page.Next <= cursor {
			return AuthoritySnapshot{}, ErrProtocol
		}
		cursor = page.Next
	}
	if err := validateAuthoritySnapshot(result, s.meshID, local.String(), server.Domainpart()); err != nil {
		return AuthoritySnapshot{}, err
	}
	return result, nil
}

type authorityPage struct {
	Total, Cursor, Next uint64
	Members             []string
}

func (s *melliumSession) queryAuthorityPage(ctx context.Context, session *xmpp.Session, management *StreamManagement, local, server jid.JID, nonce string, cursor uint64) (authorityPage, error) {
	id := "cynapsa-authority-" + nonce + "-" + strconv.FormatUint(cursor, 10)
	record := Stanza{Kind: StanzaAuthoritySync, From: local.String(), To: server.String(), MeshID: s.meshID, MessageID: id}
	var err error
	var encoded bytes.Buffer
	encoder := xml.NewEncoder(&encoded)
	start := xml.StartElement{Name: xml.Name{Space: authorityNamespace, Local: "sync"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "nonce"}, Value: nonce},
		{Name: xml.Name{Local: "cursor"}, Value: strconv.FormatUint(cursor, 10)},
	}}
	if err := encoder.EncodeToken(start); err != nil {
		return authorityPage{}, ErrProtocol
	}
	if err := encoder.EncodeToken(start.End()); err != nil {
		return authorityPage{}, ErrProtocol
	}
	if err := encoder.Flush(); err != nil {
		return authorityPage{}, ErrProtocol
	}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: server, Type: stanza.GetIQ}
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xml.NewDecoder(bytes.NewReader(encoded.Bytes())), iq)
	if err != nil {
		return authorityPage{}, err
	}
	if response == nil {
		return authorityPage{}, ErrProtocol
	}
	defer response.Close()
	page, correlated, handled, err := decodeAuthorityPage(response, id, server, local, nonce, cursor, s.config.StanzaBudgetBytes, true)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return authorityPage{}, confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return authorityPage{}, emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	return page, err
}

func decodeAuthorityPage(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, nonce string, cursor uint64, budget int, mellium bool) (authorityPage, bool, bool, error) {
	if source == nil || expectedID == "" || expectedFrom.String() == "" || expectedTo.String() == "" || nonce == "" || budget <= 0 {
		return authorityPage{}, false, false, ErrProtocol
	}
	token, err := source.Token()
	if err != nil {
		return authorityPage{}, false, false, ErrProtocol
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return authorityPage{}, false, false, ErrProtocol
	}
	iqType, ok := validCorrelatedIQAttrs(outer.Attr, expectedID, expectedFrom, expectedTo)
	if !ok {
		return authorityPage{}, false, false, ErrProtocol
	}
	bounded, err := newStanzaBudget(source, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return authorityPage{}, true, false, ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	if iqType == stanza.ErrorIQ {
		decoded, valid := decodePrivateIQError(reader, outer, mellium)
		return authorityPage{}, true, valid, decoded
	}
	startToken, err := nextPrivateToken(reader)
	if err != nil {
		return authorityPage{}, true, false, ErrProtocol
	}
	start, ok := startToken.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: authorityNamespace, Local: "synchronized"}) {
		return authorityPage{}, true, false, ErrProtocol
	}
	attrs, ok := exactPrivateAttrs(start.Attr, []string{"nonce", "total", "cursor", "next"}, authorityNamespace)
	if !ok || attrs["nonce"] != nonce {
		return authorityPage{}, true, false, ErrProtocol
	}
	total, okt := parseCanonicalUint(attrs["total"], maximumAuthorityPeers, false)
	decodedCursor, okc := parseCanonicalUint(attrs["cursor"], maximumAuthorityPeers, true)
	next := uint64(0)
	okn := true
	if attrs["next"] != "" {
		next, okn = parseCanonicalUint(attrs["next"], maximumAuthorityPeers, false)
	}
	if !okt || !okc || !okn || decodedCursor != cursor || total == 0 || cursor >= total || next > total {
		return authorityPage{}, true, false, ErrProtocol
	}
	page := authorityPage{Total: total, Cursor: decodedCursor, Next: next}
	for {
		token, err := nextPrivateToken(reader)
		if err != nil {
			return authorityPage{}, true, false, ErrProtocol
		}
		switch value := token.(type) {
		case xml.StartElement:
			if value.Name != (xml.Name{Space: authorityNamespace, Local: "member"}) || len(page.Members) >= maximumAuthorityPage {
				return authorityPage{}, true, false, ErrProtocol
			}
			attrs, ok := exactPrivateAttrs(value.Attr, []string{"jid"}, authorityNamespace)
			if !ok {
				return authorityPage{}, true, false, ErrProtocol
			}
			end, err := nextPrivateToken(reader)
			if err != nil {
				return authorityPage{}, true, false, ErrProtocol
			}
			if decoded, ok := end.(xml.EndElement); !ok || decoded.Name != value.Name {
				return authorityPage{}, true, false, ErrProtocol
			}
			page.Members = append(page.Members, attrs["jid"])
		case xml.EndElement:
			if value.Name != start.Name || uint64(len(page.Members)) == 0 || cursor+uint64(len(page.Members)) > total || next == 0 && cursor+uint64(len(page.Members)) != total || next != 0 && next != cursor+uint64(len(page.Members)) {
				return authorityPage{}, true, false, ErrProtocol
			}
			if err := consumePrivateIQEnd(reader, outer, mellium); err != nil {
				return authorityPage{}, true, false, err
			}
			return page, true, true, nil
		default:
			return authorityPage{}, true, false, ErrProtocol
		}
	}
}

func canonicalAuthorityMembers(members []string) []string {
	result := append([]string(nil), members...)
	sort.Strings(result)
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
