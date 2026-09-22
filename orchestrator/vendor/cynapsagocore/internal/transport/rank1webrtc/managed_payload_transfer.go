package rank1webrtc

import (
	"context"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type managedPayloadState struct {
	route    payload.CarrierRoute
	transfer *PayloadTransfer
	link     *Link
}

// ManagedPayloadTransfer resolves the current authenticated peer link only at
// Begin, then pins that exact authenticated link for the complete transfer.
// Link replacement therefore cannot splice one transfer across two DTLS
// sessions; failure advances the carrier-neutral fallback ladder instead.
type ManagedPayloadTransfer struct {
	manager *transport.Manager
	config  PayloadTransferConfig

	mu     sync.Mutex
	active map[string]managedPayloadState
}

func NewManagedPayloadTransfer(manager *transport.Manager, config PayloadTransferConfig) (*ManagedPayloadTransfer, error) {
	if manager == nil || config.MaximumChunkBytes <= 0 || config.InFlightChunks <= 0 || config.MaximumTransfers <= 0 {
		return nil, payload.ErrInvalidLimits
	}
	return &ManagedPayloadTransfer{manager: manager, config: config, active: make(map[string]managedPayloadState)}, nil
}

func (carrier *ManagedPayloadTransfer) Available(ctx context.Context, route payload.CarrierRoute) (bool, error) {
	if carrier == nil || ctx == nil {
		return false, payload.ErrCarrierUnavailable
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	link, ok := carrier.current(route.PeerID)
	if !ok {
		return false, nil
	}
	transfer, err := NewPayloadTransfer(link, carrier.config)
	if err != nil {
		return false, payload.ErrCarrierUnavailable
	}
	return transfer.Available(ctx, route)
}

func (carrier *ManagedPayloadTransfer) Begin(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	if carrier == nil || ctx == nil || frame.TransferID == "" {
		return payload.ErrCarrierRejected
	}
	link, ok := carrier.current(route.PeerID)
	if !ok {
		return payload.ErrCarrierUnavailable
	}
	transfer, err := NewPayloadTransfer(link, carrier.config)
	if err != nil {
		return payload.ErrCarrierUnavailable
	}
	carrier.mu.Lock()
	if len(carrier.active) >= carrier.config.MaximumTransfers || carrier.active[frame.TransferID].transfer != nil {
		carrier.mu.Unlock()
		return payload.ErrQueueFull
	}
	carrier.active[frame.TransferID] = managedPayloadState{route: route, transfer: transfer, link: link}
	carrier.mu.Unlock()
	if !carrier.currentExact(route.PeerID, link) {
		carrier.remove(frame.TransferID)
		return payload.ErrCarrierUnavailable
	}
	if err = transfer.Begin(ctx, route, frame); err != nil {
		carrier.remove(frame.TransferID)
		return err
	}
	return nil
}

func (carrier *ManagedPayloadTransfer) SendChunk(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	state, ok := carrier.state(frame.TransferID, route)
	if !ok {
		return payload.ErrCarrierRejected
	}
	if !carrier.currentExact(route.PeerID, state.link) {
		carrier.remove(frame.TransferID)
		return payload.ErrCarrierUnavailable
	}
	return state.transfer.SendChunk(ctx, route, frame)
}

func (carrier *ManagedPayloadTransfer) Finish(ctx context.Context, route payload.CarrierRoute, transferID string) (payload.CompletionEvidence, error) {
	state, ok := carrier.state(transferID, route)
	if !ok {
		return payload.CompletionEvidence{}, payload.ErrCarrierRejected
	}
	if !carrier.currentExact(route.PeerID, state.link) {
		carrier.remove(transferID)
		return payload.CompletionEvidence{}, payload.ErrCarrierUnavailable
	}
	evidence, err := state.transfer.Finish(ctx, route, transferID)
	carrier.remove(transferID)
	return evidence, err
}

func (carrier *ManagedPayloadTransfer) Abort(ctx context.Context, route payload.CarrierRoute, transferID string) error {
	state, ok := carrier.state(transferID, route)
	if !ok {
		return nil
	}
	if !carrier.currentExact(route.PeerID, state.link) {
		carrier.remove(transferID)
		return payload.ErrCarrierUnavailable
	}
	err := state.transfer.Abort(ctx, route, transferID)
	carrier.remove(transferID)
	return err
}

func (carrier *ManagedPayloadTransfer) currentExact(peer string, expected *Link) bool {
	current, ok := carrier.current(peer)
	return ok && current == expected
}

func (carrier *ManagedPayloadTransfer) current(peer string) (*Link, bool) {
	adapter, ok := carrier.manager.LivePeer(peer)
	link, typed := adapter.(*Link)
	return link, ok && typed
}

func (carrier *ManagedPayloadTransfer) state(id string, route payload.CarrierRoute) (managedPayloadState, bool) {
	if carrier == nil || id == "" {
		return managedPayloadState{}, false
	}
	carrier.mu.Lock()
	state, ok := carrier.active[id]
	carrier.mu.Unlock()
	return state, ok && state.route == route && state.transfer != nil
}

func (carrier *ManagedPayloadTransfer) remove(id string) {
	carrier.mu.Lock()
	delete(carrier.active, id)
	carrier.mu.Unlock()
}

var _ payload.ChunkCarrier = (*ManagedPayloadTransfer)(nil)
