package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func typedID(prefix, seed string, size int) string {
	digest := sha256.Sum256([]byte(seed))
	return prefix + base64.RawURLEncoding.EncodeToString(digest[:size])
}

func integrationEnvelope(t interface{ Fatal(...any) }, marker uint64, mode protocol.Mode) protocol.Envelope {
	sender, recipient, mesh := "agent-a@mesh.test/readiness", "agent-b@mesh.test/readiness", "readiness"
	conversationID, err := conversation.DeriveID(mesh, sender, recipient)
	if err != nil {
		t.Fatal(err)
	}
	p, err := protocol.NewInlinePayload("application/octet-stream", []byte{byte(marker), 0xa5})
	if err != nil {
		t.Fatal(err)
	}
	in := protocol.EnvelopeInput{
		MessageID: typedID("msg_", "integration-message-"+string(rune(marker)), 16), ConversationID: conversationID,
		Sender: sender, Recipient: recipient, MeshID: mesh, Mode: mode,
		CreatedAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), ClockUncertainty: time.Millisecond,
		Payload: p, CredentialProof: []byte("proof"),
	}
	if mode == protocol.ModeRequest {
		in.CorrelationID = typedID("cor_", "integration-correlation-"+string(rune(marker)), 16)
		in.ExpiresAt = in.CreatedAt.Add(time.Minute)
	}
	e, err := protocol.NewEnvelope(in)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func responseFor(t interface{ Fatal(...any) }, request protocol.Envelope) protocol.Envelope {
	p, err := protocol.NewInlinePayload(request.Payload.Profile, []byte("response"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: typedID("msg_", "integration-response", 16), ConversationID: request.ConversationID,
		Sender: request.Recipient, Recipient: request.Sender, MeshID: request.MeshID,
		Mode: protocol.ModeResponse, CorrelationID: request.CorrelationID, ReplyTo: request.MessageID,
		CreatedAt: request.CreatedAt, ClockUncertainty: request.ClockUncertainty,
		Payload: p, CredentialProof: []byte("proof"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func integrationLimits() payload.Limits {
	return payload.Limits{
		InlineBytes: 8, MaximumPayloadBytes: 1 << 20, ChunkBytes: 64, MaximumFrameBytes: 4096,
		MaximumChunks: 1024, InFlightChunks: 4, TransfersPerPeer: 4, MaximumTransfers: 8,
		ReassemblyBytesPerPeer: 2 << 20, ReassemblyBytes: 4 << 20,
		TransferLifetime: time.Second, CleanupTimeout: 100 * time.Millisecond, WorkerCount: 1, WorkerQueue: 4,
	}
}

type fixtureCipher struct{}

func (fixtureCipher) Encrypt(ctx context.Context, _ payload.TransferBinding, plaintext []byte) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return append(make([]byte, 29), plaintext...), "enc1_AQ", nil
}
func (fixtureCipher) Decrypt(ctx context.Context, _ payload.TransferBinding, ciphertext []byte, _ string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ciphertext) < 29 {
		return nil, payload.ErrAuthentication
	}
	return append([]byte(nil), ciphertext[29:]...), nil
}

type fixturePublisher struct {
	mu           sync.Mutex
	publications []payload.EnvelopePublication
}

func (p *fixturePublisher) PublishPayloadEnvelope(_ context.Context, publication payload.EnvelopePublication) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publications = append(p.publications, publication)
	return nil
}

type fixtureCarrier struct {
	available bool
	finishErr error
	manifest  payload.TransferManifest
	chunks    int
	aborts    int
}

func (c *fixtureCarrier) Available(context.Context, payload.CarrierRoute) (bool, error) {
	return c.available, nil
}
func (c *fixtureCarrier) Begin(_ context.Context, _ payload.CarrierRoute, frame payload.CarrierFrame) error {
	var err error
	if frame.Encoding == payload.FrameText {
		c.manifest, err = payload.DecodeTextManifest(string(frame.Data))
	} else {
		c.manifest, err = payload.DecodeManifest(frame.Data)
	}
	return err
}
func (c *fixtureCarrier) SendChunk(_ context.Context, _ payload.CarrierRoute, frame payload.CarrierFrame) error {
	if frame.TransferID != c.manifest.TransferID {
		return payload.ErrAuthentication
	}
	c.chunks++
	return nil
}
func (c *fixtureCarrier) Finish(_ context.Context, route payload.CarrierRoute, _ string) (payload.CompletionEvidence, error) {
	if c.finishErr != nil {
		return payload.CompletionEvidence{}, c.finishErr
	}
	var digest [32]byte
	copy(digest[:], c.manifest.CanonicalDigest)
	return payload.CompletionEvidence{TransferID: c.manifest.TransferID, MessageID: c.manifest.MessageID, Digest: digest}, nil
}
func (c *fixtureCarrier) Abort(context.Context, payload.CarrierRoute, string) error {
	c.aborts++
	return nil
}

type failingObjectStore struct{ available bool }

type failingPreparedUpload struct{}

func (failingPreparedUpload) DownloadReference() string {
	return "https://objects.example/blob"
}
func (failingPreparedUpload) Commit(context.Context, []byte) error {
	return payload.ErrCarrierUpload
}
func (failingPreparedUpload) Abort() {}

func (o failingObjectStore) Available(context.Context) (bool, error) { return o.available, nil }
func (failingObjectStore) PrepareUpload(context.Context, int64) (payload.PreparedObjectUpload, error) {
	return failingPreparedUpload{}, nil
}
func (failingObjectStore) Upload(context.Context, []byte) (string, error) {
	return "", payload.ErrCarrierUpload
}
func (failingObjectStore) Download(context.Context, string) ([]byte, error) {
	return nil, payload.ErrCarrierMaterialization
}

type unusedEvidence struct{ aborts int }

func (*unusedEvidence) ConfirmObjectReadiness(context.Context, payload.CarrierRoute, payload.TransferManifest, string) error {
	return nil
}

func (*unusedEvidence) PublishObject(context.Context, payload.CarrierRoute, payload.TransferManifest, string) error {
	return errors.New("unexpected object publication")
}
func (*unusedEvidence) AwaitMaterialization(context.Context, payload.CarrierRoute, string) (payload.CompletionEvidence, error) {
	return payload.CompletionEvidence{}, errors.New("unexpected object wait")
}
func (e *unusedEvidence) AbortMaterialization(context.Context, payload.CarrierRoute, string) error {
	e.aborts++
	return nil
}

func transferRequest() payload.TransferRequest {
	conversationID, _ := conversation.DeriveID("mesh-one", "agent-a@mesh.test/mesh-one", "agent-b@mesh.test/mesh-one")
	body := make([]byte, 300)
	for i := range body {
		body[i] = byte((i * 31) % 251)
	}
	serializer, _ := payload.NewSerializer(1 << 20)
	canonical, _ := serializer.Serialize(model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/integration", Body: body}})
	return payload.TransferRequest{
		PeerID: "agent-b@mesh.test/mesh-one", MessageID: typedID("msg_", "payload-message", 16), MeshID: "mesh-one",
		SenderID: "agent-a@mesh.test/mesh-one", RecipientID: "agent-b@mesh.test/mesh-one", ConversationID: conversationID,
		Mode: protocol.ModeMessage, CreatedAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		ClockUncertainty: time.Millisecond,
		Profile:          payload.ProfileNative, Canonical: canonical,
	}
}
