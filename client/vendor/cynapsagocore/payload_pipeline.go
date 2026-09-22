package cynapsagocore

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// meshPayloadPipelineFactory is the only root-level translation between the
// Pod 3 messaging contract and the Pod 5-neutral pipeline. It owns no payload
// bytes and introduces no alternate object, key, or transport implementation.
type meshPayloadPipelineFactory struct {
	source  *payload.PipelineFactory
	runtime *authenticatedPayloadRuntime
}

var _ mesh.PayloadPipelineFactory = (*meshPayloadPipelineFactory)(nil)

func newMeshPayloadPipelineFactory(source *payload.PipelineFactory, runtimes ...*authenticatedPayloadRuntime) (mesh.PayloadPipelineFactory, error) {
	if source == nil {
		return nil, payload.ErrInvalidLimits
	}
	if len(runtimes) > 1 {
		return nil, payload.ErrInvalidLimits
	}
	var runtime *authenticatedPayloadRuntime
	if len(runtimes) == 1 {
		runtime = runtimes[0]
	}
	return &meshPayloadPipelineFactory{source: source, runtime: runtime}, nil
}

func (factory *meshPayloadPipelineFactory) Create(publisher mesh.EnvelopePublisher) (mesh.PayloadPipeline, mesh.PayloadDisposition) {
	if factory == nil || factory.source == nil || publisher == nil {
		return nil, mesh.PayloadRejected
	}
	created, err := factory.source.Create(meshEnvelopePublisher{target: publisher})
	if err != nil {
		return nil, classifyPayloadDisposition(err)
	}
	if factory.runtime != nil {
		if err = factory.runtime.bindPipeline(created); err != nil {
			return nil, classifyPayloadDisposition(err)
		}
	}
	return &meshPayloadPipeline{source: created, runtime: factory.runtime}, mesh.PayloadAccepted
}

type meshEnvelopePublisher struct {
	target mesh.EnvelopePublisher
}

var _ payload.EnvelopePublisher = meshEnvelopePublisher{}

func (publisher meshEnvelopePublisher) PublishPayloadEnvelope(ctx context.Context, publication payload.EnvelopePublication) error {
	if publisher.target == nil {
		return payload.ErrEnvelopePublication
	}
	failure := publisher.target.PublishEnvelope(ctx, mesh.EnvelopePublication{
		PeerID: publication.Route.PeerID, MessageID: publication.Route.MessageID,
		MeshID: publication.Route.MeshID, SenderID: publication.Route.SenderID,
		RecipientID: publication.Route.RecipientID, ConversationID: publication.ConversationID,
		Mode:          publication.Mode,
		CorrelationID: publication.CorrelationID, ReplyTo: publication.ReplyTo,
		CreatedAt: publication.CreatedAt, ExpiresAt: publication.ExpiresAt,
		ClockUncertainty: publication.ClockUncertainty, Descriptor: publication.Descriptor,
	})
	if failure == nil {
		return nil
	}
	switch failure.Code {
	case mesh.FailureCancelled:
		return context.Canceled
	case mesh.FailureDeadline:
		return context.DeadlineExceeded
	case mesh.FailureUnavailable:
		return payload.ErrCarrierUnavailable
	case mesh.FailureRejected:
		return payload.ErrPublicationRejected
	case mesh.FailureAuthorization:
		return payload.ErrAuthorization
	case mesh.FailureCapacity:
		return payload.ErrQueueFull
	case mesh.FailureInvalidHandle:
		return payload.ErrInvalidHandle
	case mesh.FailurePayloadTooLarge:
		return payload.ErrPayloadTooLarge
	case mesh.FailurePayloadIntegrity:
		return payload.ErrIntegrity
	case mesh.FailurePayloadTransfer:
		return payload.ErrEnvelopePublication
	case mesh.FailureInternal:
		return payload.ErrPublicationInternal
	default:
		return payload.ErrPublicationInternal
	}
}

type meshPayloadPipeline struct {
	source  *payload.Pipeline
	runtime *authenticatedPayloadRuntime
}

var _ mesh.PayloadPipeline = (*meshPayloadPipeline)(nil)
var _ mesh.PeerPayloadAuthority = (*meshPayloadPipeline)(nil)

func (pipeline *meshPayloadPipeline) RetirePeer(meshID, peerID string) {
	if pipeline == nil || pipeline.source == nil {
		return
	}
	if pipeline.runtime != nil {
		pipeline.runtime.retirePeer(meshID, peerID)
	}
	pipeline.source.RetirePeer(meshID, peerID)
}

func (pipeline *meshPayloadPipeline) AllowPeer(meshID, peerID string) {
	if pipeline == nil || pipeline.source == nil {
		return
	}
	pipeline.source.AllowPeer(meshID, peerID)
}

func (pipeline *meshPayloadPipeline) Prepare(value model.Payload) (mesh.PreparedPayload, mesh.PayloadDisposition) {
	if pipeline == nil || pipeline.source == nil {
		return mesh.PreparedPayload{}, mesh.PayloadRejected
	}
	prepared, err := pipeline.source.Prepare(value)
	if err != nil {
		clearPipelineBytes(prepared.Canonical)
		return mesh.PreparedPayload{}, classifyPayloadDisposition(err)
	}
	return mesh.PreparedPayload{Profile: prepared.Profile, Canonical: prepared.Canonical, ApplicationPath: prepared.ApplicationPath}, mesh.PayloadAccepted
}

func (pipeline *meshPayloadPipeline) Send(ctx context.Context, request mesh.PreparedSend) mesh.PayloadDisposition {
	if pipeline == nil || pipeline.source == nil || ctx == nil {
		return mesh.PayloadRejected
	}
	if checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx); ok {
		ctx = payload.WithAuthorityCheckpoint(ctx, func(operation context.Context, effect func() error) error {
			err := checkpoint(operation, effect)
			switch {
			case err == nil:
				return nil
			case errors.Is(err, peer.ErrUnauthorized):
				return payload.ErrAuthorization
			case errors.Is(err, peer.ErrUnavailable), errors.Is(err, peer.ErrClosed):
				return payload.ErrCarrierUnavailable
			default:
				return err
			}
		})
	}
	err := pipeline.source.Send(ctx, payload.TransferRequest{
		PeerID: request.PeerID, MessageID: request.MessageID, MeshID: request.MeshID,
		SenderID: request.SenderID, RecipientID: request.RecipientID,
		ConversationID: request.ConversationID, Mode: request.Mode,
		CorrelationID: request.CorrelationID, ReplyTo: request.ReplyTo,
		CreatedAt: request.CreatedAt, ExpiresAt: request.ExpiresAt,
		ClockUncertainty: request.ClockUncertainty, Profile: request.Payload.Profile,
		Canonical: request.Payload.Canonical,
	})
	if err != nil {
		return classifyPayloadDisposition(err)
	}
	return mesh.PayloadAccepted
}

func (pipeline *meshPayloadPipeline) Materialize(ctx context.Context, envelope protocol.Envelope) ([]byte, mesh.PayloadDisposition) {
	if pipeline == nil || pipeline.source == nil || ctx == nil {
		return nil, mesh.PayloadRejected
	}
	cleanup := func() {}
	if pipeline.runtime != nil {
		var err error
		cleanup, err = pipeline.runtime.registerEnvelope(envelope)
		if err != nil {
			return nil, classifyPayloadDisposition(err)
		}
	}
	defer cleanup()
	canonical, err := pipeline.source.Materialize(ctx, payload.MaterializationRequest{
		MessageID: envelope.MessageID, MeshID: envelope.MeshID,
		SenderID: envelope.Sender, RecipientID: envelope.Recipient,
		ExpiresAt:  envelope.ExpiresAt,
		Descriptor: envelope.Payload,
	})
	if err != nil {
		clearPipelineBytes(canonical)
		return nil, classifyPayloadDisposition(err)
	}
	return canonical, mesh.PayloadAccepted
}

func (pipeline *meshPayloadPipeline) Decode(canonical []byte) (model.Payload, mesh.PayloadDisposition) {
	if pipeline == nil || pipeline.source == nil {
		return model.Payload{}, mesh.PayloadRejected
	}
	value, err := pipeline.source.Decode(canonical)
	if err != nil {
		return model.Payload{}, classifyPayloadDisposition(err)
	}
	return value, mesh.PayloadAccepted
}

func (pipeline *meshPayloadPipeline) ApplicationPath(profile string, canonical []byte) (string, mesh.PayloadDisposition) {
	if pipeline == nil || pipeline.source == nil {
		return "", mesh.PayloadRejected
	}
	path, err := pipeline.source.ApplicationPath(profile, canonical)
	if err != nil {
		return "", classifyPayloadDisposition(err)
	}
	return path, mesh.PayloadAccepted
}

func classifyPayloadDisposition(err error) mesh.PayloadDisposition {
	switch {
	case err == nil:
		return mesh.PayloadAccepted
	case errors.Is(err, context.Canceled):
		return mesh.PayloadCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return mesh.PayloadDeadline
	case errors.Is(err, payload.ErrInvalidHandle), errors.Is(err, payload.ErrInvalidHandleState):
		return mesh.PayloadInvalidHandle
	case errors.Is(err, payload.ErrPublicationRejected):
		return mesh.PayloadRejected
	case errors.Is(err, payload.ErrAuthorization):
		return mesh.PayloadAuthorization
	case errors.Is(err, payload.ErrPublicationInternal):
		return mesh.PayloadInternal
	case errors.Is(err, payload.ErrPayloadTooLarge), errors.Is(err, payload.ErrInlineLimitExceeded):
		return mesh.PayloadTooLarge
	case errors.Is(err, payload.ErrHandleCapacity), errors.Is(err, payload.ErrReassemblyQuota), errors.Is(err, payload.ErrQueueFull):
		return mesh.PayloadCapacity
	case errors.Is(err, payload.ErrIntegrity), errors.Is(err, payload.ErrChunkConflict):
		return mesh.PayloadIntegrity
	case errors.Is(err, payload.ErrCarrierUnavailable), errors.Is(err, payload.ErrCarrierUnsupported), errors.Is(err, payload.ErrWorkerClosed):
		return mesh.PayloadUnavailable
	case errors.Is(err, payload.ErrCarrierRejected), errors.Is(err, payload.ErrCarrierTimeout), errors.Is(err, payload.ErrCarrierUpload), errors.Is(err, payload.ErrCarrierMaterialization), errors.Is(err, payload.ErrEnvelopePublication), errors.Is(err, payload.ErrAmbiguousCompletion), errors.Is(err, payload.ErrAllCarriersFailed), errors.Is(err, payload.ErrTransferIncomplete), errors.Is(err, payload.ErrTransferExpired):
		return mesh.PayloadTransferFailed
	default:
		return mesh.PayloadRejected
	}
}

func clearPipelineBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
