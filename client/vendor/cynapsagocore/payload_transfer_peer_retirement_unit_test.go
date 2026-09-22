package cynapsagocore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestAuthenticatedPayloadRuntimeRetiresPeerRoutesAndTransferAuthority(t *testing.T) {
	limits := runtimePayloadLimits()
	transfers, err := payload.NewReassembler(limits, runtimePassCipher{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newAuthenticatedPayloadRuntime(transfers, 4, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bindDirectPayloadAdmissionForTest(runtime)
	defer runtime.Close()

	canonical := runtimeCanonical(t)
	defer clear(canonical)
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{31}, 16))
	messageID := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{32}, 16))
	sender := "a@example.test/mesh"
	recipient := "b@example.test/mesh"
	descriptor, err := protocol.NewReferencedPayload(protocol.PayloadTransferReference, payload.ProfileNative, transferID, int64(len(canonical)), payload.Digest(canonical), testEncryptionReference())
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{MessageID: messageID, Sender: sender, Recipient: recipient, MeshID: "mesh", Payload: descriptor}
	release, err := runtime.registerEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	route, ok := runtime.ResolveTransferRoute(transferID)
	if !ok {
		t.Fatal("route not registered")
	}
	binding := payload.TransferBinding{TransferID: transferID, MessageID: messageID, MeshID: "mesh", SenderID: sender, RecipientID: recipient, Profile: payload.ProfileNative, CanonicalSize: int64(len(canonical)), CanonicalDigest: payload.Digest(canonical)}
	transferred, _, err := (runtimePassCipher{}).Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(transferred)
	manifest, err := payload.BuildManifest(binding, canonical, transferred, limits.ChunkBytes, testEncryptionReference(), time.Now().UTC().Truncate(time.Millisecond).Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := payload.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	if _, err := runtime.handle(context.Background(), payload.CarrierDirectChunks, route, transport.FrameTransferManifest, transferID, encoded); err != nil {
		t.Fatal(err)
	}

	runtime.retirePeer("mesh", sender)
	transfers.RetirePeer("mesh", sender)
	if _, ok := runtime.ResolveTransferRoute(transferID); ok {
		t.Fatal("removed peer route remains visible")
	}
	if transfers.CurrentBytes() != 0 {
		t.Fatalf("retained bytes=%d", transfers.CurrentBytes())
	}
	if err := transfers.Begin(manifest, binding, payload.CarrierDirectChunks); !errors.Is(err, payload.ErrAuthentication) {
		t.Fatalf("revoked transfer Begin=%v", err)
	}

	transfers.AllowPeer("mesh", sender)
	newRelease, err := runtime.registerEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	defer newRelease()
	if _, ok := runtime.ResolveTransferRoute(transferID); !ok {
		t.Fatal("fresh current snapshot did not permit a new route")
	}
}
