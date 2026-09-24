package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"testing"

	"mellium.im/xmpp"
	"mellium.im/xmpp/stanza"
)

func latePeerAuthorizationTestSession(t *testing.T, id string) (*melliumSession, context.Context, *StreamManagement) {
	t.Helper()
	management, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 8, StanzaBudgetBytes: 4096}, Endpoint{})
	session.username = "agent@example.test"
	session.meshID = "mesh"
	session.management = management
	session.generation = 7
	record := Stanza{Kind: StanzaPeerAuthorization, From: "agent@example.test/mesh", To: "example.test", MeshID: "mesh", MessageID: id}
	if _, err = management.RecordSentTracked(record); err != nil {
		t.Fatal(err)
	}
	session.issuedPeerAuthorizations = map[string]issuedPeerAuthorization{id: {requested: "b@example.test", record: record}}
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	return session, ctx, management
}

func deliverLatePeerAuthorization(t *testing.T, session *melliumSession, ctx context.Context, id, peer string) error {
	t.Helper()
	outer := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "example.test"},
		{Name: xml.Name{Local: "to"}, Value: "agent@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "result"},
		{Name: xml.Name{Local: "id"}, Value: id},
	}}
	payload := []byte(`<authorized xmlns="urn:cynapsa:peer-authority:1" peer="` + peer + `" installation-id="install-1" session-generation="123"></authorized>`)
	wire := &qaTokenReadEncoder{decoder: melliumIQChildDecoder(t, payload, outer), encoder: xml.NewEncoder(io.Discard)}
	return session.handleElement(ctx, wire, &outer)
}

func TestLatePeerAuthorizationExactIssuedResultDoesNotKillStream(t *testing.T) {
	const id = "cynapsa-peer-authorize-ABCDEFGHIJKLMNOPQRSTUVWXY1"
	session, ctx, management := latePeerAuthorizationTestSession(t, id)
	defer func() { _ = session.Close(context.Background()) }()
	for attempt := 0; attempt < 2; attempt++ {
		if err := deliverLatePeerAuthorization(t, session, ctx, id, "b@example.test/r2.install-1.nonce"); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if got := management.HandledInbound(); got != 2 {
		t.Fatalf("inbound h=%d; want 2", got)
	}
	if err := deliverLatePeerAuthorization(t, session, ctx, "cynapsa-peer-authorize-UNISSUED", "b@example.test/r2.install-1.nonce"); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unissued ID error=%v; want authentication rejection", err)
	}
	if err := deliverLatePeerAuthorization(t, session, ctx, id, "c@example.test/r2.install-1.nonce"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("wrong peer error=%v; want protocol rejection", err)
	}
	if got := management.HandledInbound(); got != 2 {
		t.Fatalf("invalid result advanced inbound h to %d", got)
	}
}

func TestIssuedPeerAuthorizationLedgerIsBoundedAndCleared(t *testing.T) {
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 8}, Endpoint{})
	wire := &xmpp.Session{}
	management, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	session.session = wire
	session.management = management
	for index := 0; index <= issuedPeerAuthorizationCapacity; index++ {
		id := fmt.Sprintf("cynapsa-peer-authorize-%026d", index)
		record := Stanza{Kind: StanzaPeerAuthorization, MessageID: id}
		if err = session.rememberIssuedPeerAuthorization(wire, management, "b@example.test", record); err != nil {
			t.Fatal(err)
		}
	}
	if len(session.issuedPeerAuthorizations) != issuedPeerAuthorizationCapacity ||
		len(session.issuedPeerAuthorizationIDs) != issuedPeerAuthorizationCapacity {
		t.Fatalf("ledger grew beyond %d entries", issuedPeerAuthorizationCapacity)
	}
	if _, _, found := session.lookupIssuedPeerAuthorization("cynapsa-peer-authorize-00000000000000000000000000"); found {
		t.Fatal("evicted ID remained authorized")
	}
	if _, _, found := session.lookupIssuedPeerAuthorization(fmt.Sprintf("cynapsa-peer-authorize-%026d", issuedPeerAuthorizationCapacity)); !found {
		t.Fatal("latest issued ID was lost")
	}
	session.mu.Lock()
	session.clearIssuedPeerAuthorizationsLocked()
	session.mu.Unlock()
	if len(session.issuedPeerAuthorizations) != 0 || len(session.issuedPeerAuthorizationIDs) != 0 {
		t.Fatal("ledger survived clear bind")
	}
}
