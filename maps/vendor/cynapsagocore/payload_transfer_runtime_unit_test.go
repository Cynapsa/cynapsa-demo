package cynapsagocore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank1webrtc"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

func TestAuthenticatedPayloadRuntimePreservesPeerRevocationAsAuthorization(t *testing.T) {
	transfers, err := payload.NewReassembler(runtimePayloadLimits(), runtimePassCipher{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newAuthenticatedPayloadRuntime(transfers, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.admit = func(context.Context, string, uint64, func(context.Context) error) error {
		return peer.ErrUnauthorized
	}
	if err := runtime.AdmitAuthenticatedPayloadWork(context.Background(), "peer@example.test/mesh", 1, func(context.Context) error { return nil }); !errors.Is(err, payload.ErrAuthorization) {
		t.Fatalf("peer revocation mapped to %v", err)
	}
}

type runtimePassCipher struct{}

func (runtimePassCipher) Encrypt(_ context.Context, _ payload.TransferBinding, value []byte) ([]byte, string, error) {
	framed := make([]byte, len(value)+29)
	copy(framed[29:], value)
	return framed, testEncryptionReference(), nil
}

func (runtimePassCipher) Decrypt(_ context.Context, _ payload.TransferBinding, value []byte, _ string) ([]byte, error) {
	if len(value) < 29 {
		return nil, payload.ErrEncryption
	}
	return append([]byte(nil), value[29:]...), nil
}

func TestAuthenticatedPayloadRuntimeCompletesDirectAndMessageChunks(t *testing.T) {
	for _, carrier := range []payload.CarrierKind{payload.CarrierDirectChunks, payload.CarrierMessageChunks} {
		t.Run(map[payload.CarrierKind]string{payload.CarrierDirectChunks: "direct", payload.CarrierMessageChunks: "message"}[carrier], func(t *testing.T) {
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
			transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
			messageID := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 16))
			descriptor, err := protocol.NewReferencedPayload(protocol.PayloadTransferReference, payload.ProfileNative, transferID, int64(len(canonical)), payload.Digest(canonical), testEncryptionReference())
			if err != nil {
				t.Fatal(err)
			}
			envelope := protocol.Envelope{MessageID: messageID, Sender: "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh", Payload: descriptor}
			release, err := runtime.registerEnvelope(envelope)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			route, ok := runtime.ResolveTransferRoute(transferID)
			if !ok || route.PeerID != envelope.Sender || route.MessageID != messageID {
				t.Fatalf("route = %#v, %v", route, ok)
			}
			binding := payload.TransferBinding{TransferID: transferID, MessageID: messageID, MeshID: "mesh", SenderID: envelope.Sender, RecipientID: envelope.Recipient, Profile: payload.ProfileNative, CanonicalSize: int64(len(canonical)), CanonicalDigest: payload.Digest(canonical)}
			transferred, _, err := (runtimePassCipher{}).Encrypt(context.Background(), binding, canonical)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(transferred)
			manifest, err := payload.BuildManifest(binding, canonical, transferred, limits.ChunkBytes, testEncryptionReference(), time.Now().UTC().Truncate(time.Millisecond).Add(time.Minute), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = runtimeBegin(t, runtime, carrier, route, manifest); err != nil {
				t.Fatal(err)
			}
			if err = payload.ForEachChunk(manifest, transferred, func(chunk payload.TransferChunk) error {
				return runtimeChunk(t, runtime, carrier, route, chunk)
			}); err != nil {
				t.Fatal(err)
			}
			evidence, err := runtimeFinish(runtime, carrier, route, transferID)
			if err != nil || evidence == nil || evidence.TransferID != transferID || evidence.MessageID != messageID || evidence.Digest != payload.Digest(canonical) {
				t.Fatalf("finish = %#v, %v", evidence, err)
			}
			consumed, err := transfers.WaitConsume(context.Background(), transferID, binding, time.Time{})
			if err != nil || !bytes.Equal(consumed, canonical) {
				t.Fatalf("consume = %x, %v", consumed, err)
			}
			clear(consumed)
			clear(canonical)
		})
	}
}

func TestAuthenticatedPayloadRuntimeWaitsForLogicalRoute(t *testing.T) {
	transfers, _ := payload.NewReassembler(runtimePayloadLimits(), runtimePassCipher{}, time.Now)
	runtime, _ := newAuthenticatedPayloadRuntime(transfers, 1, time.Second)
	bindDirectPayloadAdmissionForTest(runtime)
	defer runtime.Close()
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 16))
	ready := make(chan payload.CarrierRoute, 1)
	go func() {
		route, _ := runtime.WaitTransferRoute(context.Background(), transferID)
		ready <- route
	}()
	canonical := runtimeCanonical(t)
	descriptor, _ := protocol.NewReferencedPayload(protocol.PayloadTransferReference, payload.ProfileNative, transferID, int64(len(canonical)), payload.Digest(canonical), testEncryptionReference())
	release, err := runtime.registerEnvelope(protocol.Envelope{MessageID: "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{10}, 16)), Sender: "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh", Payload: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	select {
	case route := <-ready:
		if route.PeerID != "a@example.test/mesh" {
			t.Fatalf("route = %#v", route)
		}
	case <-time.After(time.Second):
		t.Fatal("route waiter did not wake")
	}
}

func TestAuthenticatedPayloadRuntimePeerAdmissionPrecedesReassemblyMutation(t *testing.T) {
	for index, carrier := range []payload.CarrierKind{payload.CarrierDirectChunks, payload.CarrierMessageChunks} {
		t.Run(map[payload.CarrierKind]string{payload.CarrierDirectChunks: "rank1", payload.CarrierMessageChunks: "rank2"}[carrier], func(t *testing.T) {
			transfers, _ := payload.NewReassembler(runtimePayloadLimits(), runtimePassCipher{}, time.Now)
			runtime, _ := newAuthenticatedPayloadRuntime(transfers, 4, time.Second)
			defer runtime.Close()
			route, manifest := runtimeManifestFixture(t, runtime, byte(70+index))
			entered, release := make(chan struct{}), make(chan struct{})
			runtime.mu.Lock()
			runtime.admit = func(ctx context.Context, peerID string, _ uint64, run func(context.Context) error) error {
				if peerID != route.PeerID {
					return payload.ErrAuthentication
				}
				close(entered)
				<-release
				return run(ctx)
			}
			runtime.mu.Unlock()
			done := make(chan error, 1)
			go func() { done <- runtimeBegin(t, runtime, carrier, route, manifest) }()
			<-entered
			if bytes := transfers.CurrentBytes(); bytes != 0 {
				t.Fatalf("pre-admission reassembly bytes=%d", bytes)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if bytes := transfers.CurrentBytes(); bytes == 0 {
				t.Fatal("admitted manifest did not reserve reassembly")
			}
		})
	}
}

func runtimeManifestFixture(t *testing.T, runtime *authenticatedPayloadRuntime, marker byte) (payload.CarrierRoute, payload.TransferManifest) {
	t.Helper()
	canonical := runtimeCanonical(t)
	defer clear(canonical)
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{marker}, 16))
	messageID := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{marker + 1}, 16))
	descriptor, err := protocol.NewReferencedPayload(protocol.PayloadTransferReference, payload.ProfileNative, transferID, int64(len(canonical)), payload.Digest(canonical), testEncryptionReference())
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{MessageID: messageID, Sender: "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh", Payload: descriptor}
	if _, err = runtime.registerEnvelope(envelope); err != nil {
		t.Fatal(err)
	}
	route, ok := runtime.ResolveTransferRoute(transferID)
	if !ok {
		t.Fatal("route unavailable")
	}
	binding := payload.TransferBinding{TransferID: transferID, MessageID: messageID, MeshID: envelope.MeshID, SenderID: envelope.Sender, RecipientID: envelope.Recipient, Profile: payload.ProfileNative, CanonicalSize: int64(len(canonical)), CanonicalDigest: payload.Digest(canonical)}
	transferred, _, err := (runtimePassCipher{}).Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(transferred)
	manifest, err := payload.BuildManifest(binding, canonical, transferred, runtimePayloadLimits().ChunkBytes, testEncryptionReference(), time.Now().UTC().Truncate(time.Millisecond).Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	return route, manifest
}

func bindDirectPayloadAdmissionForTest(runtime *authenticatedPayloadRuntime) {
	runtime.mu.Lock()
	runtime.admit = func(ctx context.Context, _ string, _ uint64, run func(context.Context) error) error { return run(ctx) }
	runtime.mu.Unlock()
}

func runtimeBegin(t *testing.T, runtime *authenticatedPayloadRuntime, carrier payload.CarrierKind, route payload.CarrierRoute, manifest payload.TransferManifest) error {
	t.Helper()
	if carrier == payload.CarrierDirectChunks {
		encoded, err := payload.EncodeManifest(manifest)
		if err != nil {
			return err
		}
		defer clear(encoded)
		_, err = runtime.HandleFrame(context.Background(), route, rank1webrtc.Frame{Kind: transport.FrameTransferManifest, TransferID: manifest.TransferID, Data: encoded})
		return err
	}
	encoded, err := payload.EncodeTextManifest(manifest)
	if err != nil {
		return err
	}
	_, err = runtime.HandleStanza(context.Background(), route, rank2xmpp.Stanza{Kind: rank2xmpp.StanzaTransferManifest, TransferID: manifest.TransferID, Data: []byte(encoded)})
	return err
}

func runtimeChunk(t *testing.T, runtime *authenticatedPayloadRuntime, carrier payload.CarrierKind, route payload.CarrierRoute, chunk payload.TransferChunk) error {
	t.Helper()
	if carrier == payload.CarrierDirectChunks {
		encoded, err := payload.EncodeChunk(chunk)
		if err != nil {
			return err
		}
		defer clear(encoded)
		_, err = runtime.HandleFrame(context.Background(), route, rank1webrtc.Frame{Kind: transport.FrameTransferChunk, TransferID: chunk.TransferID, Data: encoded})
		return err
	}
	encoded, err := payload.EncodeTextChunk(chunk)
	if err != nil {
		return err
	}
	_, err = runtime.HandleStanza(context.Background(), route, rank2xmpp.Stanza{Kind: rank2xmpp.StanzaTransferChunk, TransferID: chunk.TransferID, Data: []byte(encoded)})
	return err
}

func runtimeFinish(runtime *authenticatedPayloadRuntime, carrier payload.CarrierKind, route payload.CarrierRoute, transferID string) (*payload.CompletionEvidence, error) {
	if carrier == payload.CarrierDirectChunks {
		return runtime.HandleFrame(context.Background(), route, rank1webrtc.Frame{Kind: transport.FrameTransferFinish, TransferID: transferID})
	}
	return runtime.HandleStanza(context.Background(), route, rank2xmpp.Stanza{Kind: rank2xmpp.StanzaTransferFinish, TransferID: transferID})
}

func runtimePayloadLimits() payload.Limits {
	return payload.Limits{InlineBytes: 8, MaximumPayloadBytes: 4096, ChunkBytes: 32, MaximumFrameBytes: 4096, MaximumChunks: 256, InFlightChunks: 2, TransfersPerPeer: 2, MaximumTransfers: 4, ReassemblyBytesPerPeer: 4096, ReassemblyBytes: 4096, TransferLifetime: time.Minute, CleanupTimeout: time.Second, WorkerCount: 1, WorkerQueue: 4}
}

func runtimeCanonical(t *testing.T) []byte {
	t.Helper()
	serializer, err := payload.NewSerializer(4096)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := serializer.Serialize(model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/file", Body: bytes.Repeat([]byte("x"), 96)}})
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func testEncryptionReference() string {
	return "enc1_" + base64.RawURLEncoding.EncodeToString([]byte("wrapped-test-key"))
}
