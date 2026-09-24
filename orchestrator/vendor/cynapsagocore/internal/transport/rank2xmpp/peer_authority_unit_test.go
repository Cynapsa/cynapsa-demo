package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func TestAuthorizedPeerRequiresExactCorrelatedServerResult(t *testing.T) {
	server := jid.MustParse("connect.example.test")
	local := jid.MustParse("a@connect.example.test/r2.local.nonce")
	bare := "b@connect.example.test"
	valid := `<iq xmlns="jabber:client" id="auth-1" from="connect.example.test" to="a@connect.example.test/r2.local.nonce" type="result"><authorized xmlns="urn:cynapsa:peer-authority:1" peer="b@connect.example.test/r2.install-1.nonce" installation-id="install-1" session-generation="123"></authorized></iq>`
	peer, correlated, handled, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(valid)), "auth-1", server, local, bare, maximumPrivateIQBytes, false)
	if err != nil || !correlated || !handled || peer.FullJID != "b@connect.example.test/r2.install-1.nonce" || peer.InstallationID != "install-1" || peer.SessionGeneration != "123" {
		t.Fatalf("result=%#v correlated=%v handled=%v err=%v", peer, correlated, handled, err)
	}
	if _, correlated, handled, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(valid)), "auth-1", server, local, peer.FullJID, maximumPrivateIQBytes, false); err != nil || !correlated || !handled {
		t.Fatalf("exact resolution correlated=%v handled=%v err=%v", correlated, handled, err)
	}
	if _, correlated, handled, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(valid)), "auth-1", server, local, "b@connect.example.test/r2.install-2.nonce", maximumPrivateIQBytes, false); !correlated || handled || !errors.Is(err, ErrProtocol) {
		t.Fatalf("switched exact resource correlated=%v handled=%v err=%v", correlated, handled, err)
	}
	for _, document := range []string{
		strings.Replace(valid, `id="auth-1"`, `id="other"`, 1),
		strings.Replace(valid, `from="connect.example.test"`, `from="evil.example.test"`, 1),
		strings.Replace(valid, `peer="b@connect.example.test/r2.install-1.nonce"`, `peer="c@connect.example.test/r2.install-1.nonce"`, 1),
		strings.Replace(valid, `session-generation="123"`, `session-generation=""`, 1),
		strings.Replace(valid, `installation-id="install-1"`, `installation-id="install-1" extra="x"`, 1),
		strings.Replace(valid, `</authorized>`, `<child/></authorized>`, 1),
	} {
		if _, _, _, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(document)), "auth-1", server, local, bare, maximumPrivateIQBytes, false); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted malformed authority result %q: %v", document, err)
		}
	}
}

func TestPeerAuthorizationErrorsSeparateForbiddenAndUnavailable(t *testing.T) {
	server := jid.MustParse("connect.example.test")
	local := jid.MustParse("a@connect.example.test/r2.local.nonce")
	outer := `<iq xmlns="jabber:client" id="auth-1" from="connect.example.test" to="a@connect.example.test/r2.local.nonce" type="error">`
	requested := "b@connect.example.test"
	echo := `<authorize xmlns="urn:cynapsa:peer-authority:1" peer="` + requested + `"/>`
	for _, test := range []struct {
		condition string
		want      error
	}{
		{"forbidden", ErrAuthentication},
		{"service-unavailable", ErrUnavailable},
		{"item-not-found", ErrUnavailable},
	} {
		failure := `<error type="cancel"><` + test.condition + ` xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error>`
		for _, echoed := range []bool{false, true} {
			for _, mellium := range []bool{false, true} {
				body := failure
				if echoed {
					body = echo + body
				}
				_, correlated, handled, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(outer+body+`</iq>`)), "auth-1", server, local, requested, maximumPrivateIQBytes, mellium)
				if !correlated || !handled || !errors.Is(err, test.want) {
					t.Fatalf("condition=%s echoed=%v mellium=%v correlated=%v handled=%v err=%v", test.condition, echoed, mellium, correlated, handled, err)
				}
			}
		}
	}
	failure := `<error type="cancel"><forbidden xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error>`
	for _, body := range []string{
		`<authorize xmlns="urn:cynapsa:peer-authority:1" peer="c@connect.example.test"/>` + failure,
		`<authorize xmlns="urn:cynapsa:peer-authority:1"/>` + failure,
		`<authorize xmlns="urn:cynapsa:peer-authority:1" peer="b@connect.example.test" extra="x"/>` + failure,
		`<authorize xmlns="urn:cynapsa:peer-authority:1" peer="b@connect.example.test"><child/></authorize>` + failure,
		`<authorize xmlns="urn:cynapsa:peer-authority:1" peer="b@connect.example.test">text</authorize>` + failure,
		`<authorize xmlns="urn:wrong" peer="b@connect.example.test"/>` + failure,
		echo + echo + failure,
		echo + failure + echo,
	} {
		_, correlated, handled, err := decodeAuthorizedPeer(xml.NewDecoder(strings.NewReader(outer+body+`</iq>`)), "auth-1", server, local, requested, maximumPrivateIQBytes, false)
		if !correlated || handled || !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted malformed echo %q: correlated=%v handled=%v err=%v", body, correlated, handled, err)
		}
	}
}

func TestDynamicPeerAuthorityStartsAndResumesWithoutSnapshot(t *testing.T) {
	session := &authorityDiscoveryGateSession{fakeSession: &fakeSession{events: make(chan Event, 4), resume: true}, discoveryErr: ErrProtocol}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.DynamicPeerAuthority = true
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("dynamic Start: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if !client.membershipReady || session.syncCalls.Load() != 0 || session.discoveryCalls.Load() != 0 {
		t.Fatalf("startup readiness=%v sync=%d discovery=%d", client.membershipReady, session.syncCalls.Load(), session.discoveryCalls.Load())
	}
	resumed, err := client.Resume(context.Background())
	if err != nil || !resumed || !client.membershipReady || session.syncCalls.Load() != 0 || session.discoveryCalls.Load() != 0 {
		t.Fatalf("resume=%v err=%v readiness=%v sync=%d discovery=%d", resumed, err, client.membershipReady, session.syncCalls.Load(), session.discoveryCalls.Load())
	}
}

func TestDynamicMelliumResumeSkipsLegacySnapshotMarker(t *testing.T) {
	session := &melliumSession{generation: 3, resumeBarrierGeneration: 3, resumeBarrier: &resumeAuthorityBarrier{done: make(chan struct{})}}
	session.setDynamicPeerAuthority(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitResumeAuthorityResult(ctx); err != nil {
		t.Fatalf("dynamic resume waited for obsolete marker: %v", err)
	}
}

func TestPeerRevocationScopeAndGenerationAreStrict(t *testing.T) {
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}}
	for _, test := range []struct {
		payload string
		kind    RevocationType
	}{
		{`<revoke xmlns="urn:cynapsa:peer-authority:1" type="logical" action-id="action-1"><target agent="b@connect.example.test"/></revoke>`, RevokeLogical},
		{`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation" action-id="action-1"><target agent="b@connect.example.test" installation-id="install-1" session-generation="123"/></revoke>`, RevokeInstallation},
	} {
		revokes, err := decodePeerRevocation(melliumIQChildDecoder(t, []byte(test.payload), outer), outer, maximumPrivateIQBytes)
		if err != nil || len(revokes) != 1 || revokes[0].Type != test.kind {
			t.Fatalf("revokes=%#v err=%v", revokes, err)
		}
	}
	for _, payload := range []string{
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation"><target installation-id="install-1"/></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation"><target installation-id="install-1" session-generation="123"/></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation"><target agent="" installation-id="install-1" session-generation="123"/></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="logical"><target agent="b@connect.example.test" session-generation="123"/></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation"><target agent="b@connect.example.test" installation-id="install-1" session-generation="123"><extra/></target></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="installation"><target agent="b@connect.example.test" installation-id="install-1" session-generation="123" session-generation="124"/></revoke>`,
		`<revoke xmlns="urn:cynapsa:peer-authority:1" type="logical"></revoke>`,
	} {
		if _, err := decodePeerRevocation(melliumIQChildDecoder(t, []byte(payload), outer), outer, maximumPrivateIQBytes); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted invalid revoke %q: %v", payload, err)
		}
	}
	if validRevocation(Revocation{Type: RevokeInstallation, InstallationID: "install-1", SessionGeneration: "123"}) {
		t.Fatal("accepted installation revocation without an agent")
	}
}

func TestServerControlDispatchAcceptsRevokeAndLegacyWake(t *testing.T) {
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}}
	for _, test := range []struct {
		payload string
		kind    EventKind
	}{
		{`<revoke xmlns="urn:cynapsa:peer-authority:1" type="logical" action-id="action-1"><target agent="b@connect.example.test"/></revoke>`, EventPeerRevoked},
		{`<membership-changed xmlns="urn:cynapsa:mesh-authority:1" snapshot-required="true"><removed jid="b@connect.example.test/mesh"/></membership-changed>`, EventMembershipChanged},
	} {
		kind, revokes, actionID, err := decodeServerAuthoritySet(melliumIQChildDecoder(t, []byte(test.payload), outer), outer, maximumPrivateIQBytes)
		if err != nil || kind != test.kind || kind == EventPeerRevoked && (len(revokes) != 1 || revokes[0].Peer != "b@connect.example.test") {
			t.Fatalf("payload=%q kind=%v revokes=%#v action=%q err=%v", test.payload, kind, revokes, actionID, err)
		}
	}
}

func TestServerRevocationIQQueuesControlAndOnlyPacketAcks(t *testing.T) {
	session, management := newXEP0199TestSession(t)
	wire := `<iq xmlns="jabber:client" from="example.test" to="a@example.test/mesh" type="set" id="revoke-1"><revoke xmlns="urn:cynapsa:peer-authority:1" type="installation" action-id="action-1"><target agent="b@example.test" installation-id="install-1" session-generation="123"/></revoke></iq>`
	output, err := handleEncodedXEP0199(t, session, wire)
	if err != nil || !strings.Contains(output, `type="result"`) || !strings.Contains(output, `id="revoke-1"`) || strings.Contains(output, "applied") {
		t.Fatalf("expected packet-only response %q err=%v", output, err)
	}
	select {
	case event := <-session.events:
		if event.Kind != EventPeerRevoked || event.ControlID != "action-1" || len(event.Revocations) != 1 || event.Revocations[0].InstallationID != "install-1" || event.Revocations[0].SessionGeneration != "123" {
			t.Fatalf("revoke event=%#v", event)
		}
		clearEventOwned(&event)
	default:
		t.Fatal("revoke was not queued")
	}
	if handled := management.HandledInbound(); handled != 1 {
		t.Fatalf("packet handled=%d", handled)
	}
	if pending := management.PendingSnapshot(); len(pending) != 1 || pending[0].Kind != StanzaPeerRevocationPacketAck {
		t.Fatalf("packet ACK ledger=%#v", pending)
	}
}

func TestPeerRevocationActionResultRequiresExactServerCorrelation(t *testing.T) {
	server := jid.MustParse("connect.example.test")
	local := jid.MustParse("a@connect.example.test/r2.local.nonce")
	valid := `<iq xmlns="jabber:client" id="applied-1" from="connect.example.test" to="a@connect.example.test/r2.local.nonce" type="result"></iq>`
	if err := decodePeerRevocationAppliedResult(xml.NewDecoder(strings.NewReader(valid)), "applied-1", server, local); err != nil {
		t.Fatalf("valid action result: %v", err)
	}
	for _, document := range []string{
		strings.Replace(valid, `id="applied-1"`, `id="other"`, 1),
		strings.Replace(valid, `from="connect.example.test"`, `from="evil.example.test"`, 1),
		strings.Replace(valid, `to="a@connect.example.test/r2.local.nonce"`, `to="b@connect.example.test/r2.local.nonce"`, 1),
		strings.Replace(valid, `type="result"`, `type="error"`, 1),
		strings.Replace(valid, `></iq>`, `><extra/></iq>`, 1),
	} {
		if err := decodePeerRevocationAppliedResult(xml.NewDecoder(strings.NewReader(document)), "applied-1", server, local); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted invalid action result %q: %v", document, err)
		}
	}
}

type peerAuthorityTestSession struct {
	*fakeSession
	peer       AuthorizedPeer
	resolveErr error
	ackIDs     []string
	ackGen     []uint64
}

func (s *peerAuthorityTestSession) ResolveAuthorizedPeer(_ context.Context, _ string) (AuthorizedPeer, error) {
	return s.peer, s.resolveErr
}
func (s *peerAuthorityTestSession) ResolveAuthorizedExactPeer(_ context.Context, _ string) (AuthorizedPeer, error) {
	return s.peer, s.resolveErr
}
func (s *peerAuthorityTestSession) AcknowledgePeerRevocation(_ context.Context, id string, generation uint64) error {
	s.ackIDs = append(s.ackIDs, id)
	s.ackGen = append(s.ackGen, generation)
	return nil
}

func TestClientRevocationActionAckFollowsOwnerCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &peerAuthorityTestSession{fakeSession: &fakeSession{}}
	ingress := newIngressGeneration(3, session, "a@connect.example.test/r2.local.nonce", nil, ctx)
	c := &Client{started: true, session: session, ingress: ingress, sessionEpoch: 3}
	revoke := Revocation{Type: RevokeInstallation, Peer: "b@connect.example.test", InstallationID: "install-1", SessionGeneration: "123"}
	called := false
	if err := c.SetRevocationHandler(func(_ context.Context, got Revocation) error {
		if got != revoke || len(session.ackIDs) != 0 {
			t.Fatalf("ack before owner cleanup: %#v ids=%v", got, session.ackIDs)
		}
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c.handlePeerRevocation(ingress, Event{Kind: EventPeerRevoked, Revocations: []Revocation{revoke}, ControlID: "revoke-1", sessionGeneration: 7})
	if !called || len(session.ackIDs) != 1 || session.ackIDs[0] != "revoke-1" || session.ackGen[0] != 7 {
		t.Fatalf("cleanup=%v ack=%v gen=%v", called, session.ackIDs, session.ackGen)
	}
	if err := c.SetRevocationHandler(func(context.Context, Revocation) error { return nil }); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("replaced handler: %v", err)
	}
}

func TestBatchRevocationActionAckRequiresEveryTargetCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &peerAuthorityTestSession{fakeSession: &fakeSession{}}
	ingress := newIngressGeneration(3, session, "a@connect.example.test/r2.local.nonce", nil, ctx)
	c := &Client{started: true, session: session, ingress: ingress, sessionEpoch: 3}
	calls := 0
	_ = c.SetRevocationHandler(func(_ context.Context, _ Revocation) error {
		calls++
		if len(session.ackIDs) != 0 {
			t.Fatal("batch ACK preceded complete cleanup")
		}
		return nil
	})
	batch := []Revocation{{Type: RevokeLogical, Peer: "b@connect.example.test"}, {Type: RevokeLogical, Peer: "c@connect.example.test"}}
	if !c.handlePeerRevocation(ingress, Event{Revocations: batch, ControlID: "batch-1", sessionGeneration: 7}) || calls != 2 || len(session.ackIDs) != 1 {
		t.Fatalf("calls=%d ACKs=%v", calls, session.ackIDs)
	}
}

func TestExactInboundPeerResolutionCannotSwitchInstallation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requested := "b@connect.example.test/r2.install-1.nonce"
	session := &peerAuthorityTestSession{fakeSession: &fakeSession{}, peer: AuthorizedPeer{FullJID: requested, InstallationID: "install-1", SessionGeneration: "123"}}
	c := &Client{config: Config{DynamicPeerAuthority: true}, started: true, state: DurableLive, membershipReady: true, session: session, ctx: ctx, generation: 1, sessionEpoch: 3, identity: Authenticated{BoundIdentity: "a@connect.example.test/r2.local.nonce"}}
	if peer, err := c.ResolveAuthorizedExactPeer(ctx, requested); err != nil || peer.FullJID != requested {
		t.Fatalf("exact peer=%#v err=%v", peer, err)
	}
	session.peer.FullJID = "b@connect.example.test/r2.install-2.nonce"
	if _, err := c.ResolveAuthorizedExactPeer(ctx, requested); !errors.Is(err, ErrProtocol) {
		t.Fatalf("accepted switched installation: %v", err)
	}
}

func TestClientRevocationDoesNotAckOnFailureOrStaleIngress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &peerAuthorityTestSession{fakeSession: &fakeSession{}}
	ingress := newIngressGeneration(3, session, "a@connect.example.test/r2.local.nonce", nil, ctx)
	c := &Client{started: true, session: session, ingress: ingress, sessionEpoch: 3}
	revoke := Revocation{Type: RevokeLogical, Peer: "b@connect.example.test"}
	_ = c.SetRevocationHandler(func(context.Context, Revocation) error { return ErrUnavailable })
	c.handlePeerRevocation(ingress, Event{Revocations: []Revocation{revoke}, ControlID: "revoke-1", sessionGeneration: 7})
	if len(session.ackIDs) != 0 {
		t.Fatalf("acked failed cleanup: %v", session.ackIDs)
	}
	c.sessionEpoch = 4
	c.handlePeerRevocation(ingress, Event{Revocations: []Revocation{revoke}, ControlID: "revoke-2", sessionGeneration: 7})
	if len(session.ackIDs) != 0 {
		t.Fatalf("acked stale event: %v", session.ackIDs)
	}
}
