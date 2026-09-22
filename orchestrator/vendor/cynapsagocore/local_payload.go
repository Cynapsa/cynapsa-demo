package cynapsagocore

import (
	"context"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// newInlinePayloadPipeline creates the one process-local canonicalization
// pipeline shared by local policy checks and the later authenticated graph.
// No carrier, object store, cipher, or reassembler is installed: payloads that
// exceed the inline limit therefore fail closed until real offload providers
// are composed.
func newInlinePayloadPipeline(config model.RuntimeConfig, handles *payload.HandleStore) (*payload.PipelineFactory, *payload.Pipeline, error) {
	maximum := int64(config.PayloadLimit)
	inline := min(maximum, int64(protocol.MaxInlinePayloadBytes))
	limits := payload.Limits{
		InlineBytes: inline, MaximumPayloadBytes: maximum,
		// These dimensions are inactive for an inline-only pipeline. Their
		// minimum valid values keep construction closed without claiming an
		// offload capability or allocating transfer workers.
		ChunkBytes: 1, MaximumFrameBytes: 1, MaximumChunks: 1, InFlightChunks: 1,
		TransfersPerPeer: 1, MaximumTransfers: 1,
		ReassemblyBytesPerPeer: 1, ReassemblyBytes: 1,
		TransferLifetime: time.Second, CleanupTimeout: time.Second,
		WorkerCount: 1, WorkerQueue: 1,
	}
	factory, err := payload.NewPipelineFactory(limits, payload.PipelineDependencies{Handles: handles})
	if err != nil {
		return nil, nil, err
	}
	pipeline, err := factory.Create(rejectingPayloadPublisher{})
	if err != nil {
		return nil, nil, err
	}
	return factory, pipeline, nil
}

type rejectingPayloadPublisher struct{}

func (rejectingPayloadPublisher) PublishPayloadEnvelope(context.Context, payload.EnvelopePublication) error {
	return payload.ErrEnvelopePublication
}
