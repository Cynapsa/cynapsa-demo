package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/fxamacker/cbor/v2"
)

type testObjectReferenceWire struct {
	Version    uint16 `cbor:"0,keyasint"`
	TransferID string `cbor:"1,keyasint"`
	URL        string `cbor:"2,keyasint"`
}

func TestObjectPublicationCanonicalRoundTrip(t *testing.T) {
	manifest := readinessManifest(t, "a@example.test/mesh", "b@example.test/mesh", time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	reference := testObjectReference(t, manifest.TransferID, "https://objects.example.test/blob/token")
	encoded, err := EncodeObjectPublication(ObjectPublication{Manifest: manifest, PrivateReference: reference})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeObjectPublication(encoded)
	if err != nil || decoded.PrivateReference != reference || !objectManifestsEqual(decoded.Manifest, manifest) {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
	if _, err = DecodeObjectPublication(append(encoded, 0)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing data accepted: %v", err)
	}
	stanza := Stanza{Kind: StanzaObjectTransfer, From: manifest.SenderID, To: manifest.RecipientID, MeshID: manifest.MeshID, TransferID: manifest.TransferID, MessageID: manifest.MessageID, Data: encoded}
	wire, err := EncodeStanzaFrame(stanza, 64<<10, 96<<10)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := DecodeStanzaFrame(wire, stanza.From, stanza.To, stanza.MeshID, 64<<10, 96<<10)
	if err != nil || roundTrip.Kind != stanza.Kind || roundTrip.MessageID != stanza.MessageID || !bytes.Equal(roundTrip.Data, stanza.Data) {
		t.Fatalf("frame round trip=%#v err=%v", roundTrip, err)
	}
}

func TestObjectTransferRequiresReadinessAndExactCompletion(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	client := newReadinessClient(t, session)
	defer client.Close(context.Background())
	transfer, err := NewObjectTransfer(client, 2)
	if err != nil {
		t.Fatal(err)
	}
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh", MessageID: typed("msg_", 0x62)}
	manifest := readinessManifest(t, route.SenderID, route.RecipientID, time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	rawReference := "https://objects.example.test/blob/token"
	privateReference := testObjectReference(t, manifest.TransferID, rawReference)
	if err = transfer.PublishObject(context.Background(), route, manifest, privateReference); !errors.Is(err, payload.ErrAuthentication) {
		t.Fatalf("publication bypassed readiness: %v", err)
	}

	ready := make(chan error, 1)
	go func() {
		ready <- transfer.ConfirmObjectReadiness(context.Background(), route, manifest, rawReference)
	}()
	requestStanza := sentReadiness(t, session, StanzaObjectReadinessRequest)
	request, err := decodeObjectReadinessRequest(requestStanza.Data)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err := encodeObjectReadinessResult(objectReadinessResult{request: request, ready: true})
	if err != nil {
		t.Fatal(err)
	}
	client.handleObjectReadinessResult(Stanza{Kind: StanzaObjectReadinessResult, From: route.PeerID, To: route.SenderID, MeshID: route.MeshID, TransferID: manifest.TransferID, AttemptID: request.AttemptID, MessageID: request.MessageID, Data: resultBytes})
	if err = <-ready; err != nil {
		t.Fatal(err)
	}
	if err = transfer.PublishObject(context.Background(), route, manifest, privateReference); err != nil {
		t.Fatal(err)
	}
	publicationStanza := sentReadiness(t, session, StanzaObjectTransfer)
	publication, err := DecodeObjectPublication(publicationStanza.Data)
	client.mu.Lock()
	client.readinessSeen[manifest.TransferID] = objectReadinessInbound{request: request, response: append([]byte(nil), resultBytes...)}
	client.mu.Unlock()
	if err != nil || !client.authorizeObjectPublication(publicationStanza, publication) {
		t.Fatalf("publication authorization=%v err=%v", client.authorizeObjectPublication(publicationStanza, publication), err)
	}

	done := make(chan error, 1)
	evidence := payload.CompletionEvidence{TransferID: manifest.TransferID, MessageID: route.MessageID, Digest: payload.Digest([]byte("canonical-payload"))}
	go func() {
		got, awaitErr := transfer.AwaitMaterialization(context.Background(), route, manifest.TransferID)
		if awaitErr == nil && got != evidence {
			awaitErr = errors.New("completion evidence mismatch")
		}
		done <- awaitErr
	}()
	client.deliverCompletion(Stanza{Kind: StanzaTransferCompletion, From: route.PeerID, To: route.SenderID, MeshID: route.MeshID, TransferID: manifest.TransferID, MessageID: route.MessageID, Evidence: evidence})
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("object completion did not release waiter")
	}
}

func testObjectReference(t *testing.T, transferID, rawURL string) string {
	t.Helper()
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := mode.Marshal(testObjectReferenceWire{Version: payload.TransferVersion1, TransferID: transferID, URL: rawURL})
	if err != nil {
		t.Fatal(err)
	}
	return "obj1_" + base64.RawURLEncoding.EncodeToString(encoded)
}
