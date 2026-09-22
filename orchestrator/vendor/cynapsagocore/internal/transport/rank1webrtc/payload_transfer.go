package rank1webrtc

import (
	"context"
	"errors"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type PayloadTransferConfig struct {
	MaximumChunkBytes int
	InFlightChunks    int
	MaximumTransfers  int
}

type directState struct {
	route     payload.CarrierRoute
	done      <-chan completion
	finishing bool
}

// PayloadTransfer implements payload.ChunkCarrier over a healthy live link.
type PayloadTransfer struct {
	link   *Link
	config PayloadTransferConfig
	window chan struct{}
	mu     sync.Mutex
	active map[string]directState
}

func NewPayloadTransfer(link *Link, config PayloadTransferConfig) (*PayloadTransfer, error) {
	if link == nil || config.MaximumChunkBytes <= 0 || config.MaximumChunkBytes > link.config.MaximumFrameBytes || !validFrame(Frame{Kind: FrameTransferChunk, TransferID: "xfer_AAAAAAAAAAAAAAAAAAAAAA", Data: make([]byte, config.MaximumChunkBytes)}, link.config.MaximumFrameBytes) || config.InFlightChunks <= 0 || config.InFlightChunks > 65536 || config.MaximumTransfers <= 0 || config.MaximumTransfers > 65536 {
		return nil, payload.ErrInvalidLimits
	}
	return &PayloadTransfer{link: link, config: config, window: make(chan struct{}, config.InFlightChunks), active: make(map[string]directState)}, nil
}

func (t *PayloadTransfer) Available(ctx context.Context, route payload.CarrierRoute) (bool, error) {
	if t == nil || ctx == nil || !t.routeValid(route) {
		return false, payload.ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return t.link.Observe().State == transport.HealthHealthy, nil
}

func (t *PayloadTransfer) Begin(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	if t == nil || ctx == nil || !t.routeValid(route) || frame.Encoding != payload.FrameBinary || frame.TransferID == "" || len(frame.Data) == 0 || len(frame.Data) > t.config.MaximumChunkBytes || !validFrame(Frame{Kind: FrameTransferManifest, TransferID: frame.TransferID, Data: frame.Data}, t.link.config.MaximumFrameBytes) {
		return payload.ErrFrameTooLarge
	}
	done, err := t.link.registerCompletion(frame.TransferID)
	if err != nil {
		return payload.ErrQueueFull
	}
	t.mu.Lock()
	if len(t.active) >= t.config.MaximumTransfers || t.active[frame.TransferID].done != nil {
		t.mu.Unlock()
		t.link.removeCompletion(frame.TransferID)
		return payload.ErrQueueFull
	}
	t.active[frame.TransferID] = directState{route: route, done: done}
	t.mu.Unlock()
	err = t.link.sendFrame(ctx, Frame{Kind: FrameTransferManifest, Route: route, TransferID: frame.TransferID, Data: frame.Data})
	if err != nil {
		t.remove(frame.TransferID)
		return classifyPayload(err)
	}
	return nil
}

func (t *PayloadTransfer) SendChunk(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	if t == nil || ctx == nil || !t.routeValid(route) || frame.Encoding != payload.FrameBinary || len(frame.Data) == 0 || len(frame.Data) > t.config.MaximumChunkBytes || !validFrame(Frame{Kind: FrameTransferChunk, TransferID: frame.TransferID, Data: frame.Data}, t.link.config.MaximumFrameBytes) || !t.matches(frame.TransferID, route) {
		return payload.ErrFrameTooLarge
	}
	select {
	case t.window <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-t.window }()
	return classifyPayload(t.link.sendFrame(ctx, Frame{Kind: FrameTransferChunk, Route: route, TransferID: frame.TransferID, Data: frame.Data}))
}

func (t *PayloadTransfer) Finish(ctx context.Context, route payload.CarrierRoute, transferID string) (payload.CompletionEvidence, error) {
	if t == nil || ctx == nil {
		return payload.CompletionEvidence{}, payload.ErrCarrierRejected
	}
	state, ok := t.beginFinish(transferID, route)
	if !ok {
		return payload.CompletionEvidence{}, payload.ErrCarrierRejected
	}
	if err := t.link.sendFrame(ctx, Frame{Kind: FrameTransferFinish, Route: route, TransferID: transferID}); err != nil {
		t.cancel(transferID, classifyPayload(err))
		return payload.CompletionEvidence{}, classifyPayload(err)
	}
	select {
	case result := <-state.done:
		t.remove(transferID)
		if !t.link.authorityAdmitted() {
			return payload.CompletionEvidence{}, payload.ErrCarrierUnavailable
		}
		if result.err != nil {
			return payload.CompletionEvidence{}, classifyPayload(result.err)
		}
		if result.evidence.TransferID != transferID || result.evidence.MessageID != route.MessageID {
			return payload.CompletionEvidence{}, payload.ErrAuthentication
		}
		return result.evidence, nil
	case <-t.link.authorityLost():
		t.cancel(transferID, transport.ErrUnavailable)
		return payload.CompletionEvidence{}, payload.ErrCarrierUnavailable
	case <-ctx.Done():
		t.cancel(transferID, ctx.Err())
		return payload.CompletionEvidence{}, ctx.Err()
	}
}

func (t *PayloadTransfer) Abort(ctx context.Context, route payload.CarrierRoute, transferID string) error {
	if t == nil || ctx == nil || !t.routeValid(route) {
		return payload.ErrAuthentication
	}
	if t.matches(transferID, route) {
		if t.cancel(transferID, payload.ErrCarrierRejected) {
			_ = t.link.sendFrame(ctx, Frame{Kind: FrameTransferAbort, Route: route, TransferID: transferID})
		}
	}
	return nil
}

func (t *PayloadTransfer) routeValid(route payload.CarrierRoute) bool {
	return route.PeerID == t.link.config.PeerID && route.MeshID == t.link.config.MeshID && route.SenderID == t.link.config.LocalIdentity && route.RecipientID == t.link.config.PeerID && route.MessageID != ""
}

func (t *PayloadTransfer) matches(id string, route payload.CarrierRoute) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.active[id]
	return ok && state.route == route
}

func (t *PayloadTransfer) beginFinish(id string, route payload.CarrierRoute) (directState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.active[id]
	if !ok || state.route != route || state.finishing {
		return directState{}, false
	}
	state.finishing = true
	t.active[id] = state
	return state, true
}

func (t *PayloadTransfer) remove(id string) {
	t.mu.Lock()
	delete(t.active, id)
	t.mu.Unlock()
	t.link.removeCompletion(id)
}

func (t *PayloadTransfer) cancel(id string, err error) bool {
	t.mu.Lock()
	if _, ok := t.active[id]; !ok {
		t.mu.Unlock()
		return false
	}
	delete(t.active, id)
	t.mu.Unlock()
	t.link.failCompletion(id, err)
	return true
}

func classifyPayload(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, transport.ErrSendAmbiguous) {
		return payload.ErrAmbiguousCompletion
	}
	if errors.Is(err, transport.ErrQueueFull) {
		return payload.ErrQueueFull
	}
	for _, known := range []error{payload.ErrCarrierRejected, payload.ErrCarrierUnavailable, payload.ErrAmbiguousCompletion, payload.ErrAuthentication, payload.ErrFrameTooLarge} {
		if errors.Is(err, known) {
			return known
		}
	}
	return payload.ErrCarrierUnavailable
}

var _ payload.ChunkCarrier = (*PayloadTransfer)(nil)
