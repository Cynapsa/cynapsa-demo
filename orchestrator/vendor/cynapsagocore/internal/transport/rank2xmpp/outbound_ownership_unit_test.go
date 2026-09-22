package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

func TestReviewRank2SendClearsDependencyTemporary(t *testing.T) {
	session := newOwnershipSession()
	var seen []byte
	var retained Stanza
	session.send = func(_ context.Context, stanza Stanza) error {
		seen = stanza.Data
		retained = stanza.clone()
		return nil
	}
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer func() {
		clearStanzaOwned(&retained)
		closeOwnershipClient(t, client)
	}()

	caller := []byte("rank-two-private-signal")
	want := append([]byte(nil), caller...)
	if err := client.SendSignal(context.Background(), "b@example.test/mesh", "attempt", caller); err != nil {
		t.Fatal(err)
	}
	assertRank2Zero(t, seen)
	if !bytes.Equal(caller, want) {
		t.Fatalf("caller bytes changed: %q", caller)
	}
	if !bytes.Equal(retained.Data, want) {
		t.Fatalf("dependency's explicit retained clone changed: %q", retained.Data)
	}
}

func TestSendSessionClearsDependencySnapshotOnErrorAndPanic(t *testing.T) {
	privateErr := errors.New("private dependency error")
	for _, test := range []struct {
		name  string
		panic bool
		want  error
	}{
		{name: "error", want: privateErr},
		{name: "panic", panic: true, want: ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newOwnershipSession()
			var seen []byte
			var retained Stanza
			session.send = func(_ context.Context, stanza Stanza) error {
				seen = stanza.Data
				retained = stanza.clone()
				if test.panic {
					panic("dependency panic")
				}
				return privateErr
			}
			caller := []byte("private stanza")
			wantCaller := append([]byte(nil), caller...)
			err := sendSession(session, context.Background(), Stanza{Data: caller})
			if !errors.Is(err, test.want) {
				t.Fatalf("sendSession error = %v, want %v", err, test.want)
			}
			assertRank2Zero(t, seen)
			if !bytes.Equal(caller, wantCaller) {
				t.Fatalf("caller bytes changed: %q", caller)
			}
			if !bytes.Equal(retained.Data, wantCaller) {
				t.Fatalf("retained clone changed: %q", retained.Data)
			}
			clearStanzaOwned(&retained)
		})
	}
}

func TestSendJingleClearsEncodedTemporary(t *testing.T) {
	session := newOwnershipSession()
	var seen []byte
	var retained Stanza
	session.send = func(_ context.Context, stanza Stanza) error {
		seen = stanza.Data
		retained = stanza.clone()
		return nil
	}
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer func() {
		clearStanzaOwned(&retained)
		closeOwnershipClient(t, client)
	}()

	signal := sampleJingle()
	signal.Initiator = "a@example.test/mesh"
	signal.Responder = "b@example.test/mesh"
	signal.Content.Description.MeshID = "mesh"
	if err := client.SendJingle(context.Background(), signal.Responder, signal); err != nil {
		t.Fatal(err)
	}
	assertRank2Zero(t, seen)
	decoded, err := DecodeJingle(retained.Data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SID != signal.SID || decoded.Content.Transport.Password != signal.Content.Transport.Password {
		t.Fatalf("retained signal changed: %#v", decoded)
	}
}

func TestSendOwnedClearsWireAttemptButPreservesOutboxOwnership(t *testing.T) {
	session := newOwnershipSession()
	var seen []byte
	var retained Stanza
	session.send = func(_ context.Context, stanza Stanza) error {
		seen = stanza.Data
		retained = stanza.clone()
		return nil
	}
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer func() {
		clearStanzaOwned(&retained)
		closeOwnershipClient(t, client)
	}()

	envelope := durableEnvelope(t)
	wantInline := append([]byte(nil), envelope.Payload.Inline...)
	if got := client.SendOwned(context.Background(), envelope); got != AcceptedOwned {
		t.Fatalf("ownership = %v", got)
	}
	assertRank2Zero(t, seen)
	if len(retained.Data) == 0 {
		t.Fatal("dependency's explicit replay clone was cleared")
	}
	if !bytes.Equal(envelope.Payload.Inline, wantInline) {
		t.Fatalf("caller envelope changed: %q", envelope.Payload.Inline)
	}
	assertOwnershipOutbox(t, client, 1)
}

func TestSendSessionAllocationIsBoundedToOwnershipCopies(t *testing.T) {
	session := newOwnershipSession()
	session.send = func(context.Context, Stanza) error { return nil }
	caller := make([]byte, 256)
	allocations := testing.AllocsPerRun(1000, func() {
		if err := sendSession(session, context.Background(), Stanza{Data: caller}); err != nil {
			panic(err)
		}
	})
	if allocations > 1 {
		t.Fatalf("allocations per send = %.2f, want one dependency snapshot", allocations)
	}
}

func TestPayloadChunksPreservesCallerBytesAndClearsWireSnapshots(t *testing.T) {
	session := newOwnershipSession()
	var seen []byte
	var retained Stanza
	session.send = func(_ context.Context, stanza Stanza) error {
		seen = stanza.Data
		clearStanzaOwned(&retained)
		retained = stanza.clone()
		return nil
	}
	client := newOwnershipClient(t, 4, session)
	startOwnershipClient(t, client)
	defer func() {
		clearStanzaOwned(&retained)
		closeOwnershipClient(t, client)
	}()
	envelope := durableEnvelope(t)
	if got := client.SendOwned(context.Background(), envelope); got != AcceptedOwned {
		t.Fatalf("ownership = %v", got)
	}
	clearStanzaOwned(&retained)

	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{
		MaximumFrameBytes: 1024, MaximumChunkBytes: 256, StanzaBudgetBytes: 900,
		XMLOverheadBytes: 128, InFlightChunks: 1, MaximumTransfers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := payload.CarrierRoute{
		PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh",
		RecipientID: "b@example.test/mesh", MessageID: envelope.MessageID,
	}
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))

	manifest := []byte(base64.RawStdEncoding.EncodeToString([]byte("private manifest")))
	wantManifest := append([]byte(nil), manifest...)
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: manifest}); err != nil {
		t.Fatal(err)
	}
	assertRank2Zero(t, seen)
	if !bytes.Equal(manifest, wantManifest) || !bytes.Equal(retained.Data, wantManifest) {
		t.Fatal("manifest ownership was not preserved")
	}
	clearStanzaOwned(&retained)

	chunk := []byte(base64.RawStdEncoding.EncodeToString([]byte("private chunk")))
	wantChunk := append([]byte(nil), chunk...)
	if err := carrier.SendChunk(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: chunk}); err != nil {
		t.Fatal(err)
	}
	assertRank2Zero(t, seen)
	if !bytes.Equal(chunk, wantChunk) || !bytes.Equal(retained.Data, wantChunk) {
		t.Fatal("chunk ownership was not preserved")
	}
	clearStanzaOwned(&retained)
	_ = carrier.Abort(context.Background(), route, transferID)
}

func assertRank2Zero(t *testing.T, data []byte) {
	t.Helper()
	if len(data) == 0 {
		t.Fatal("dependency did not receive payload bytes")
	}
	for index, value := range data {
		if value != 0 {
			t.Fatalf("dependency temporary remained non-zero at byte %d", index)
		}
	}
}
