package rank2xmpp

import (
	"bytes"
	"testing"
	"time"
)

func TestObjectPublicationCleanupZeroizesOwnedDigestsOnly(t *testing.T) {
	manifest := readinessManifest(t, "a@example.test/mesh", "b@example.test/mesh", time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	reference := testObjectReference(t, manifest.TransferID, "https://objects.example.test/blob/private")
	encoded, err := EncodeObjectPublication(ObjectPublication{Manifest: manifest, PrivateReference: reference})
	if err != nil {
		t.Fatal(err)
	}
	wantEncoded := append([]byte(nil), encoded...)
	publication, err := decodeObjectPublication(encoded)
	if err != nil {
		t.Fatal(err)
	}
	canonical := publication.Manifest.CanonicalDigest
	transferred := publication.Manifest.TransferredDigest
	clearObjectPublicationOwned(&publication)
	assertRank2Zero(t, canonical)
	assertRank2Zero(t, transferred)
	if !bytes.Equal(encoded, wantEncoded) {
		t.Fatal("publication cleanup changed caller-owned stanza data")
	}
}

func TestPublicationConsumersPreserveCallerStanzaData(t *testing.T) {
	manifest := readinessManifest(t, "a@example.test/mesh", "b@example.test/mesh", time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC))
	reference := testObjectReference(t, manifest.TransferID, "https://objects.example.test/blob/private")
	encoded, err := EncodeObjectPublication(ObjectPublication{Manifest: manifest, PrivateReference: reference})
	if err != nil {
		t.Fatal(err)
	}
	wantEncoded := append([]byte(nil), encoded...)
	stanza := Stanza{
		Kind: StanzaObjectTransferFailure, From: manifest.SenderID, To: manifest.RecipientID,
		MeshID: manifest.MeshID, TransferID: manifest.TransferID, MessageID: manifest.MessageID,
		Data: encoded,
	}
	client := &Client{complete: make(map[string]completionWaiter), membershipReady: true}
	client.failObjectCompletion(stanza)
	if !bytes.Equal(encoded, wantEncoded) {
		t.Fatal("completion failure cleanup changed caller-owned stanza data")
	}
	if !replayMeshAllowed(stanza, manifest.MeshID) {
		t.Fatal("valid object publication was rejected for replay")
	}
	if !bytes.Equal(encoded, wantEncoded) {
		t.Fatal("replay validation cleanup changed caller-owned stanza data")
	}
}
