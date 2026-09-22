package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"mellium.im/xmpp/jid"
)

type authorityDiscoveryGateSession struct {
	*fakeSession
	discoveryErr   error
	discoveryCalls atomic.Int32
	syncCalls      atomic.Int32
}

func (s *authorityDiscoveryGateSession) DiscoverAuthority(context.Context) error {
	s.discoveryCalls.Add(1)
	return s.discoveryErr
}

func (s *authorityDiscoveryGateSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	s.syncCalls.Add(1)
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func TestFreshAuthorityDiscoveryGatesSnapshotSynchronization(t *testing.T) {
	for _, test := range []struct {
		name      string
		discovery error
		wantStart bool
		wantSync  int32
	}{
		{name: "success", wantStart: true, wantSync: 1},
		{name: "rejected", discovery: ErrProtocol, wantSync: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := &authorityDiscoveryGateSession{
				fakeSession:  &fakeSession{events: make(chan Event)},
				discoveryErr: test.discovery,
			}
			client := qaUnstartedClient(t, fakeDialer{session})
			err := client.Start(context.Background())
			if test.wantStart && err != nil {
				t.Fatal(err)
			}
			if !test.wantStart && !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Start()=%v, want unavailable", err)
			}
			if calls := session.discoveryCalls.Load(); calls != 1 {
				t.Fatalf("discovery calls=%d, want 1", calls)
			}
			if calls := session.syncCalls.Load(); calls != test.wantSync {
				t.Fatalf("snapshot calls=%d, want %d", calls, test.wantSync)
			}
			if test.wantStart {
				if err := client.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestAuthorityDiscoveryRequiresExactlyOneCurrentFeature(t *testing.T) {
	server := jid.MustParse("example.test")
	local := jid.MustParse("a@example.test/mesh")
	const id = "authority-disco-id"
	valid := `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><identity category='server' type='im'/><feature var='urn:xmpp:ping'/><feature var='urn:cynapsa:mesh-authority:1'/></query></iq>`
	correlated, handled, err := decodeAuthorityDiscovery(xml.NewDecoder(strings.NewReader(valid)), id, server, local, maximumPrivateIQBytes, false)
	if err != nil || !correlated || !handled {
		t.Fatalf("valid discovery = correlated %v handled %v err %v", correlated, handled, err)
	}

	tests := map[string]string{
		"absent":            `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:xmpp:ping'/></query></iq>`,
		"duplicate":         `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:cynapsa:mesh-authority:1'/><feature var='urn:cynapsa:mesh-authority:1'/></query></iq>`,
		"wrong version":     `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:cynapsa:mesh-authority:2'/></query></iq>`,
		"malformed query":   `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info' node='unexpected'><feature var='urn:cynapsa:mesh-authority:1'/></query></iq>`,
		"malformed feature": `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:cynapsa:mesh-authority:1'>text</feature></query></iq>`,
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			correlated, _, err := decodeAuthorityDiscovery(xml.NewDecoder(strings.NewReader(document)), id, server, local, maximumPrivateIQBytes, false)
			if !correlated || !errors.Is(err, ErrProtocol) {
				t.Fatalf("decode = correlated %v err %v, want correlated protocol rejection", correlated, err)
			}
		})
	}
}

func TestAuthorityDiscoveryRejectsCorrelationAndIQErrors(t *testing.T) {
	server := jid.MustParse("example.test")
	local := jid.MustParse("a@example.test/mesh")
	const id = "authority-disco-id"
	base := `<iq xmlns='jabber:client' id='%s' type='result' from='%s' to='%s'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:cynapsa:mesh-authority:1'/></query></iq>`
	tests := map[string]string{
		"wrong id":   formatAuthorityDiscoveryTest(base, "other", server.String(), local.String()),
		"wrong from": formatAuthorityDiscoveryTest(base, id, "other.test", local.String()),
		"wrong to":   formatAuthorityDiscoveryTest(base, id, server.String(), "b@example.test/mesh"),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			correlated, handled, err := decodeAuthorityDiscovery(xml.NewDecoder(strings.NewReader(document)), id, server, local, maximumPrivateIQBytes, false)
			if correlated || handled || !errors.Is(err, ErrProtocol) {
				t.Fatalf("decode = correlated %v handled %v err %v", correlated, handled, err)
			}
		})
	}

	iqError := `<iq xmlns='jabber:client' id='authority-disco-id' type='error' from='example.test' to='a@example.test/mesh'><error type='cancel'><service-unavailable xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error></iq>`
	correlated, handled, err := decodeAuthorityDiscovery(xml.NewDecoder(strings.NewReader(iqError)), id, server, local, maximumPrivateIQBytes, false)
	if !correlated || !handled || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("IQ error = correlated %v handled %v err %v", correlated, handled, err)
	}
}

func TestAuthorityDiscoveryRejectsOverBudgetResponse(t *testing.T) {
	server := jid.MustParse("example.test")
	local := jid.MustParse("a@example.test/mesh")
	document := `<iq xmlns='jabber:client' id='authority-disco-id' type='result' from='example.test' to='a@example.test/mesh'><query xmlns='http://jabber.org/protocol/disco#info'><feature var='urn:cynapsa:mesh-authority:1'/></query></iq>`
	correlated, handled, err := decodeAuthorityDiscovery(xml.NewDecoder(strings.NewReader(document)), "authority-disco-id", server, local, 32, false)
	if !correlated || handled || !errors.Is(err, ErrProtocol) {
		t.Fatalf("over-budget = correlated %v handled %v err %v", correlated, handled, err)
	}
}

func formatAuthorityDiscoveryTest(format, id, from, to string) string {
	result := strings.Replace(format, "%s", id, 1)
	result = strings.Replace(result, "%s", from, 1)
	return strings.Replace(result, "%s", to, 1)
}
