package payload

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// PreparedPayload is one owned canonical application snapshot. The empty
// application path is valid only for an HTTP response; request authorization
// obtains that response path from the authenticated request correlation.
type PreparedPayload struct {
	Profile         string
	Canonical       []byte
	ApplicationPath string
}

// MaterializationRequest contains only authenticated envelope context. A
// private reference can select bytes, but can never establish this binding.
type MaterializationRequest struct {
	MessageID   string
	MeshID      string
	SenderID    string
	RecipientID string
	ExpiresAt   time.Time
	Descriptor  protocol.PayloadDescriptor
}

// PipelineDependencies are the complete closed Pod 5 construction seams.
// Offload fields may all be nil for an honest inline-only pipeline. Any
// configured offload capability requires a Reassembler; Cipher is optional in
// V1 because the carriers already provide transport encryption. Object storage
// and authenticated evidence are an inseparable pair.
type PipelineDependencies struct {
	Handles        *HandleStore
	Direct         ChunkCarrier
	Objects        ObjectStore
	ObjectEvidence MaterializationAcknowledger
	Messages       ChunkCarrier
	Cipher         PayloadCipher
	Transfers      *Reassembler
}

// PipelineFactory owns immutable construction dependencies and creates the
// publisher-bound coordinator only after the mesh layer exists.
type PipelineFactory struct {
	limits       Limits
	dependencies PipelineDependencies
}

func NewPipelineFactory(limits Limits, dependencies PipelineDependencies) (*PipelineFactory, error) {
	if err := limits.Validate(); err != nil || dependencies.Handles == nil || (dependencies.Objects == nil) != (dependencies.ObjectEvidence == nil) {
		return nil, ErrInvalidLimits
	}
	offloadEnabled := dependencies.Direct != nil || dependencies.Objects != nil || dependencies.Messages != nil || dependencies.Cipher != nil || dependencies.Transfers != nil
	if offloadEnabled && dependencies.Transfers == nil {
		return nil, ErrInvalidLimits
	}
	return &PipelineFactory{limits: limits, dependencies: dependencies}, nil
}

func (factory *PipelineFactory) Create(publisher EnvelopePublisher) (*Pipeline, error) {
	if factory == nil || publisher == nil {
		return nil, ErrInvalidLimits
	}
	serializer, err := NewSerializer(factory.limits.MaximumPayloadBytes)
	if err != nil {
		return nil, err
	}
	coordinator, err := NewCoordinator(
		factory.limits,
		factory.dependencies.Direct,
		factory.dependencies.Objects,
		factory.dependencies.ObjectEvidence,
		factory.dependencies.Messages,
		factory.dependencies.Cipher,
		publisher,
	)
	if err != nil {
		return nil, err
	}
	materializer, err := NewMaterializer(factory.limits.MaximumPayloadBytes, factory.dependencies.Objects, factory.dependencies.Cipher, factory.dependencies.Transfers)
	if err != nil {
		return nil, err
	}
	return &Pipeline{serializer: serializer, handles: factory.dependencies.Handles, coordinator: coordinator, materializer: materializer}, nil
}

// Pipeline composes canonical serialization, handle snapshots, the transfer
// coordinator, strict materialization, and canonical reconstruction without
// depending on mesh or concrete transports.
type Pipeline struct {
	serializer   *Serializer
	handles      *HandleStore
	coordinator  *Coordinator
	materializer *Materializer
}

// PrivateTransferID returns the authenticated private transfer identifier
// selected by a referenced descriptor without exposing its object URL. Inline
// descriptors intentionally have no transfer identifier.
func PrivateTransferID(descriptor protocol.PayloadDescriptor) (string, error) {
	if err := protocol.ValidatePayloadDescriptor(descriptor); err != nil {
		return "", ErrInvalidManifest
	}
	switch descriptor.Kind {
	case protocol.PayloadInline:
		return "", nil
	case protocol.PayloadTransferReference:
		if !validTransferID(descriptor.Reference) {
			return "", ErrInvalidManifest
		}
		return descriptor.Reference, nil
	case protocol.PayloadObjectReference:
		transferID, _, err := decodeObjectReference(descriptor.Reference)
		return transferID, err
	default:
		return "", ErrInvalidManifest
	}
}

// IngestObject authorizes one private object materialization against the same
// pipeline and retained-transfer registry used by logical envelope delivery.
func (pipeline *Pipeline) IngestObject(ctx context.Context, manifest TransferManifest, binding TransferBinding, privateReference string) (CompletionEvidence, error) {
	if pipeline == nil || pipeline.materializer == nil {
		return CompletionEvidence{}, ErrInvalidLimits
	}
	return pipeline.materializer.IngestObject(ctx, manifest, binding, privateReference)
}

// Prepare returns a newly owned canonical snapshot. Payload-handle content is
// copied under the HandleStore lock and its retained digest is rechecked.
func (pipeline *Pipeline) Prepare(value model.Payload) (PreparedPayload, error) {
	if pipeline == nil || pipeline.serializer == nil || pipeline.handles == nil {
		return PreparedPayload{}, ErrInvalidLimits
	}
	var canonical []byte
	var retainedDigest [sha256.Size]byte
	var err error
	switch input := value.Value.(type) {
	case model.PayloadHandle:
		canonical, retainedDigest, err = pipeline.handles.Snapshot(input.Handle)
		if err == nil && sha256.Sum256(canonical) != retainedDigest {
			err = ErrIntegrity
		}
	default:
		canonical, err = pipeline.serializer.Serialize(value)
	}
	if err != nil {
		zero(canonical)
		return PreparedPayload{}, err
	}
	profile, applicationPath, err := pipeline.inspect(canonical, true)
	if err != nil {
		zero(canonical)
		return PreparedPayload{}, err
	}
	return PreparedPayload{Profile: profile, Canonical: canonical, ApplicationPath: applicationPath}, nil
}

func (pipeline *Pipeline) Send(ctx context.Context, request TransferRequest) error {
	if pipeline == nil || pipeline.coordinator == nil || ctx == nil {
		return ErrInvalidLimits
	}
	_, err := pipeline.coordinator.Send(ctx, request)
	return err
}

// ActiveTransfers reports only large outbound transfers that currently own a
// carrier attempt. It is a private bounded diagnostic and exposes no carrier,
// object, encryption, or peer metadata.
func (pipeline *Pipeline) ActiveTransfers() uint64 {
	if pipeline == nil || pipeline.coordinator == nil {
		return 0
	}
	return pipeline.coordinator.ActiveTransfers()
}

// RetirePeer cancels and zeroizes every retained inbound transfer whose
// authenticated binding names a peer absent from the current membership
// snapshot. Outbound coordinator attempts are owned by their peer-lane
// contexts and are canceled by that lane's terminal fence.
func (pipeline *Pipeline) RetirePeer(meshID, peerID string) {
	if pipeline == nil || pipeline.materializer == nil || pipeline.materializer.transfers == nil {
		return
	}
	pipeline.materializer.transfers.RetirePeer(meshID, peerID)
}

func (pipeline *Pipeline) AllowPeer(meshID, peerID string) {
	if pipeline == nil || pipeline.materializer == nil || pipeline.materializer.transfers == nil {
		return
	}
	pipeline.materializer.transfers.AllowPeer(meshID, peerID)
}

func (pipeline *Pipeline) Materialize(ctx context.Context, request MaterializationRequest) ([]byte, error) {
	if pipeline == nil || pipeline.materializer == nil || ctx == nil {
		return nil, ErrInvalidLimits
	}
	transferID := ""
	switch request.Descriptor.Kind {
	case protocol.PayloadInline:
	case protocol.PayloadTransferReference:
		transferID = request.Descriptor.Reference
	case protocol.PayloadObjectReference:
		var err error
		transferID, _, err = decodeObjectReference(request.Descriptor.Reference)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidManifest
	}
	binding := TransferBinding{
		TransferID: transferID, MessageID: request.MessageID, MeshID: request.MeshID,
		SenderID: request.SenderID, RecipientID: request.RecipientID,
		Profile: request.Descriptor.Profile, CanonicalSize: request.Descriptor.Size,
		CanonicalDigest: request.Descriptor.Digest,
	}
	return pipeline.materializer.MaterializeUntil(ctx, request.Descriptor, binding, request.ExpiresAt)
}

func (pipeline *Pipeline) Decode(canonical []byte) (model.Payload, error) {
	if pipeline == nil || pipeline.serializer == nil {
		return model.Payload{}, ErrInvalidLimits
	}
	return pipeline.serializer.Deserialize(canonical)
}

// ApplicationPath accepts only canonical request-bearing variants. HTTP
// responses intentionally have no self-asserted path.
func (pipeline *Pipeline) ApplicationPath(profile string, canonical []byte) (string, error) {
	if pipeline == nil || pipeline.serializer == nil || !validCanonicalProfile(profile) {
		return "", ErrMalformedCanonical
	}
	actualProfile, applicationPath, err := pipeline.inspect(canonical, false)
	if err != nil || actualProfile != profile {
		return "", ErrMalformedCanonical
	}
	return applicationPath, nil
}

func (pipeline *Pipeline) inspect(canonical []byte, allowResponse bool) (string, string, error) {
	value, err := pipeline.serializer.Deserialize(canonical)
	if err != nil {
		return "", "", err
	}
	defer zeroModelPayload(value)
	profile, err := Profile(value)
	if err != nil {
		return "", "", err
	}
	switch payload := value.Value.(type) {
	case model.NativePayload:
		return profile, payload.Path, nil
	case model.HTTPRequestPayload:
		return profile, payload.Path, nil
	case model.HTTPResponsePayload:
		if allowResponse {
			return profile, "", nil
		}
		return "", "", ErrUnsupportedVariant
	default:
		return "", "", ErrUnsupportedVariant
	}
}
