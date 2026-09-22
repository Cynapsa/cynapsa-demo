package cynapsagocore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type rootPipelinePublisher struct {
	publication mesh.EnvelopePublication
	calls       int
	failure     *mesh.Failure
}

func (publisher *rootPipelinePublisher) PublishEnvelope(_ context.Context, publication mesh.EnvelopePublication) *mesh.Failure {
	publisher.calls++
	publication.Descriptor.Inline = append([]byte(nil), publication.Descriptor.Inline...)
	publisher.publication = publication
	return publisher.failure
}

func rootPipelineFactory(t testing.TB) mesh.PayloadPipelineFactory {
	t.Helper()
	handles, err := payload.NewHandleStore(payload.HandleLimits{
		MaximumHandles: 4, MaximumBytes: 4096, MaximumPerHandle: 4096,
		MaximumWriteBytes: 4096, MaximumReadBytes: 4096,
	}, bytes.NewReader(make([]byte, 256)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handles.Close)
	limits := payload.Limits{
		InlineBytes: 256, MaximumPayloadBytes: 4096, ChunkBytes: 64,
		MaximumFrameBytes: 4096, MaximumChunks: 64, InFlightChunks: 2,
		TransfersPerPeer: 2, MaximumTransfers: 4,
		ReassemblyBytesPerPeer: 4096, ReassemblyBytes: 4096,
		TransferLifetime: time.Minute, CleanupTimeout: time.Second,
		WorkerCount: 1, WorkerQueue: 4,
	}
	neutral, err := payload.NewPipelineFactory(limits, payload.PipelineDependencies{Handles: handles})
	if err != nil {
		t.Fatal(err)
	}
	created, err := newMeshPayloadPipelineFactory(neutral)
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestMeshPayloadPipelineAdapterPreservesRequestEnvelopeMetadata(t *testing.T) {
	t.Parallel()
	factory := rootPipelineFactory(t)
	publisher := &rootPipelinePublisher{}
	pipeline, disposition := factory.Create(publisher)
	if disposition != mesh.PayloadAccepted || pipeline == nil {
		t.Fatalf("create disposition=%v", disposition)
	}
	prepared, disposition := pipeline.Prepare(model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/agent", Body: []byte("body")}})
	if disposition != mesh.PayloadAccepted || prepared.Profile != payload.ProfileHTTPRequest || prepared.ApplicationPath != "/agent" {
		t.Fatalf("prepared=%#v disposition=%v", prepared, disposition)
	}
	created := time.UnixMilli(1_700_000_000_000).UTC()
	expires := created.Add(30 * time.Second)
	request := mesh.PreparedSend{
		PeerID: "peer", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh",
		SenderID: "sender", RecipientID: "recipient",
		ConversationID: "conv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Mode:           protocol.ModeRequest,
		CorrelationID:  "cor_AAAAAAAAAAAAAAAAAAAAAA", CreatedAt: created,
		ExpiresAt: expires, ClockUncertainty: 375 * time.Microsecond,
		Payload: prepared,
	}
	if disposition = pipeline.Send(context.Background(), request); disposition != mesh.PayloadAccepted {
		t.Fatalf("send disposition=%v", disposition)
	}
	got := publisher.publication
	if publisher.calls != 1 || !got.CreatedAt.Equal(created) || !got.ExpiresAt.Equal(expires) || got.ClockUncertainty != request.ClockUncertainty || got.CorrelationID != request.CorrelationID {
		t.Fatalf("publication changed: %#v calls=%d", got, publisher.calls)
	}
	envelope := protocol.Envelope{
		MessageID: got.MessageID, MeshID: got.MeshID, Sender: got.SenderID,
		Recipient: got.RecipientID, Payload: got.Descriptor,
	}
	canonical, disposition := pipeline.Materialize(context.Background(), envelope)
	if disposition != mesh.PayloadAccepted || !bytes.Equal(canonical, prepared.Canonical) {
		t.Fatalf("materialize disposition=%v equal=%v", disposition, bytes.Equal(canonical, prepared.Canonical))
	}
	decoded, disposition := pipeline.Decode(canonical)
	if disposition != mesh.PayloadAccepted || decoded.Value.(model.HTTPRequestPayload).Path != "/agent" {
		t.Fatalf("decode=%#v disposition=%v", decoded, disposition)
	}
}

func TestMeshPayloadPipelineAdapterReportsDeferredOffloadAsTransferFailure(t *testing.T) {
	t.Parallel()
	factory := rootPipelineFactory(t)
	publisher := &rootPipelinePublisher{}
	pipeline, _ := factory.Create(publisher)
	prepared, disposition := pipeline.Prepare(model.Payload{Value: model.NativePayload{Path: "/large", Body: bytes.Repeat([]byte{'x'}, 512)}})
	if disposition != mesh.PayloadAccepted {
		t.Fatalf("prepare disposition=%v", disposition)
	}
	request := mesh.PreparedSend{
		PeerID: "peer", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh",
		SenderID: "sender", RecipientID: "recipient",
		ConversationID: "conv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Mode:           protocol.ModeMessage,
		CreatedAt:      time.UnixMilli(1_700_000_000_000).UTC(), ClockUncertainty: time.Millisecond,
		Payload: prepared,
	}
	if disposition = pipeline.Send(context.Background(), request); disposition != mesh.PayloadTransferFailed {
		t.Fatalf("offload disposition=%v", disposition)
	}
	if publisher.calls != 0 {
		t.Fatal("unavailable offload published an envelope")
	}
}

func TestMeshPayloadPipelineAdapterPreservesClosedFailureClasses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want mesh.PayloadDisposition
	}{
		{name: "invalid handle", err: payload.ErrInvalidHandle, want: mesh.PayloadInvalidHandle},
		{name: "invalid handle state", err: payload.ErrInvalidHandleState, want: mesh.PayloadInvalidHandle},
		{name: "authorization", err: payload.ErrAuthorization, want: mesh.PayloadAuthorization},
		{name: "caller cancellation", err: context.Canceled, want: mesh.PayloadCancelled},
		{name: "caller deadline", err: context.DeadlineExceeded, want: mesh.PayloadDeadline},
		{name: "wrapped deadline", err: errors.Join(errors.New("private"), context.DeadlineExceeded), want: mesh.PayloadDeadline},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyPayloadDisposition(test.err); got != test.want {
				t.Fatalf("disposition=%v want=%v", got, test.want)
			}
		})
	}
}

func TestMeshPayloadPipelinePublicationPreservesClosedFailureClasses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		code mesh.FailureCode
		want mesh.PayloadDisposition
	}{
		{name: "cancelled", code: mesh.FailureCancelled, want: mesh.PayloadCancelled},
		{name: "deadline", code: mesh.FailureDeadline, want: mesh.PayloadDeadline},
		{name: "unavailable", code: mesh.FailureUnavailable, want: mesh.PayloadUnavailable},
		{name: "rejected", code: mesh.FailureRejected, want: mesh.PayloadRejected},
		{name: "authorization", code: mesh.FailureAuthorization, want: mesh.PayloadAuthorization},
		{name: "capacity", code: mesh.FailureCapacity, want: mesh.PayloadCapacity},
		{name: "invalid handle", code: mesh.FailureInvalidHandle, want: mesh.PayloadInvalidHandle},
		{name: "too large", code: mesh.FailurePayloadTooLarge, want: mesh.PayloadTooLarge},
		{name: "integrity", code: mesh.FailurePayloadIntegrity, want: mesh.PayloadIntegrity},
		{name: "transfer", code: mesh.FailurePayloadTransfer, want: mesh.PayloadTransferFailed},
		{name: "internal", code: mesh.FailureInternal, want: mesh.PayloadInternal},
		{name: "unknown", code: mesh.FailureCode(255), want: mesh.PayloadInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := rootPipelineFactory(t)
			publisher := &rootPipelinePublisher{failure: &mesh.Failure{Code: test.code}}
			pipeline, disposition := factory.Create(publisher)
			if disposition != mesh.PayloadAccepted || pipeline == nil {
				t.Fatalf("create disposition=%v", disposition)
			}
			prepared, disposition := pipeline.Prepare(model.Payload{Value: model.NativePayload{Path: "/", Body: []byte("body")}})
			if disposition != mesh.PayloadAccepted {
				t.Fatalf("prepare disposition=%v", disposition)
			}
			request := mesh.PreparedSend{
				PeerID: "peer", MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", MeshID: "mesh",
				SenderID: "sender", RecipientID: "recipient",
				ConversationID: "conv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				Mode:           protocol.ModeMessage,
				CreatedAt:      time.UnixMilli(1_700_000_000_000).UTC(), ClockUncertainty: time.Millisecond,
				Payload: prepared,
			}
			if got := pipeline.Send(context.Background(), request); got != test.want {
				t.Fatalf("send disposition=%v want=%v", got, test.want)
			}
		})
	}
}

func TestClassifyAllCarriersFailedAsPayloadTransfer(t *testing.T) {
	if got := classifyPayloadDisposition(payload.ErrAllCarriersFailed); got != mesh.PayloadTransferFailed {
		t.Fatalf("all carriers failed disposition = %v, want %v", got, mesh.PayloadTransferFailed)
	}
}
