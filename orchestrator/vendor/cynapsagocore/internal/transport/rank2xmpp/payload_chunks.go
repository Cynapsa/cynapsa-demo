package rank2xmpp

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

type PayloadChunksConfig struct {
	MaximumFrameBytes int
	MaximumChunkBytes int
	StanzaBudgetBytes int
	XMLOverheadBytes  int
	InFlightChunks    int
	MaximumTransfers  int
}

type chunkState struct {
	route payload.CarrierRoute
	done  <-chan completion
}

// PayloadChunks implements payload.ChunkCarrier using Pod5-provided canonical
// unpadded Base64 text. It never re-encodes or derives routing from that text.
type PayloadChunks struct {
	client *Client
	config PayloadChunksConfig
	window chan struct{}
	mu     sync.Mutex
	active map[string]chunkState
}

func NewPayloadChunks(client *Client, config PayloadChunksConfig) (*PayloadChunks, error) {
	if client == nil || config.MaximumFrameBytes <= 0 || config.MaximumFrameBytes > 2<<20 || config.MaximumChunkBytes <= 0 || config.MaximumChunkBytes > config.MaximumFrameBytes || config.StanzaBudgetBytes <= 0 || config.StanzaBudgetBytes > 4<<20 || config.XMLOverheadBytes < 0 || config.XMLOverheadBytes >= config.StanzaBudgetBytes || config.MaximumChunkBytes > config.StanzaBudgetBytes-config.XMLOverheadBytes || config.InFlightChunks <= 0 || config.InFlightChunks > 65536 || config.MaximumTransfers <= 0 || config.MaximumTransfers > 65536 {
		return nil, payload.ErrInvalidLimits
	}
	worst, err := EncodeStanzaFrame(Stanza{Kind: StanzaTransferChunk, TransferID: "xfer_AAAAAAAAAAAAAAAAAAAAAA", Data: make([]byte, config.MaximumChunkBytes)}, config.MaximumFrameBytes, config.StanzaBudgetBytes)
	clear(worst)
	if err != nil {
		return nil, payload.ErrInvalidLimits
	}
	return &PayloadChunks{client: client, config: config, window: make(chan struct{}, config.InFlightChunks), active: make(map[string]chunkState)}, nil
}

func (t *PayloadChunks) Available(ctx context.Context, route payload.CarrierRoute) (bool, error) {
	if t == nil || ctx == nil || !t.routeValid(route) {
		return false, payload.ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	t.client.mu.Lock()
	ready := t.client.started && !t.client.closed && t.client.state != DurableUnknown
	t.client.mu.Unlock()
	return ready, nil
}

func (t *PayloadChunks) Begin(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	if t == nil || ctx == nil || !t.routeValid(route) || !t.frameValid(StanzaTransferManifest, frame) {
		return payload.ErrFrameTooLarge
	}
	done, err := t.client.registerCompletion(frame.TransferID, route.PeerID, route.MessageID)
	if err != nil {
		return payload.ErrQueueFull
	}
	t.mu.Lock()
	if len(t.active) >= t.config.MaximumTransfers || t.active[frame.TransferID].done != nil {
		t.mu.Unlock()
		t.client.removeCompletion(frame.TransferID)
		return payload.ErrQueueFull
	}
	t.active[frame.TransferID] = chunkState{route: route, done: done}
	t.mu.Unlock()
	err = t.client.sendStanza(ctx, t.stanza(StanzaTransferManifest, route, frame.TransferID, frame.Data))
	if err != nil {
		t.remove(frame.TransferID)
		return classify(err)
	}
	return nil
}

func (t *PayloadChunks) SendChunk(ctx context.Context, route payload.CarrierRoute, frame payload.CarrierFrame) error {
	if t == nil || ctx == nil || !t.matches(frame.TransferID, route) || !t.frameValid(StanzaTransferChunk, frame) {
		return payload.ErrFrameTooLarge
	}
	select {
	case t.window <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-t.window }()
	return classify(t.client.sendStanza(ctx, t.stanza(StanzaTransferChunk, route, frame.TransferID, frame.Data)))
}

func (t *PayloadChunks) Finish(ctx context.Context, route payload.CarrierRoute, transferID string) (payload.CompletionEvidence, error) {
	if t == nil || ctx == nil || !t.matches(transferID, route) {
		return payload.CompletionEvidence{}, payload.ErrCarrierRejected
	}
	t.mu.Lock()
	state := t.active[transferID]
	t.mu.Unlock()
	if err := t.client.sendStanza(ctx, t.stanza(StanzaTransferFinish, route, transferID, nil)); err != nil {
		t.terminalize(transferID, completion{err: err})
		return payload.CompletionEvidence{}, classify(err)
	}
	select {
	case result := <-state.done:
		t.remove(transferID)
		if result.err != nil {
			return payload.CompletionEvidence{}, classify(result.err)
		}
		if result.evidence.TransferID != transferID || result.evidence.MessageID != route.MessageID {
			return payload.CompletionEvidence{}, payload.ErrAuthentication
		}
		return result.evidence, nil
	case <-ctx.Done():
		t.terminalize(transferID, completion{err: ctx.Err()})
		return payload.CompletionEvidence{}, ctx.Err()
	}
}

func (t *PayloadChunks) Abort(ctx context.Context, route payload.CarrierRoute, transferID string) error {
	if t == nil || ctx == nil || !t.routeValid(route) {
		return payload.ErrAuthentication
	}
	if t.matches(transferID, route) {
		_ = t.client.sendStanza(ctx, t.stanza(StanzaTransferAbort, route, transferID, nil))
		t.terminalize(transferID, completion{err: ErrCompletionAmbiguous})
	}
	return nil
}

func (t *PayloadChunks) frameValid(kind StanzaKind, frame payload.CarrierFrame) bool {
	if frame.Encoding != payload.FrameText || frame.TransferID == "" || len(frame.Data) == 0 || len(frame.Data) > t.config.MaximumChunkBytes || len(frame.Data)+t.config.XMLOverheadBytes > t.config.StanzaBudgetBytes {
		return false
	}
	decoded, err := base64.RawStdEncoding.Strict().DecodeString(string(frame.Data))
	if err != nil || base64.RawStdEncoding.EncodeToString(decoded) != string(frame.Data) {
		clear(decoded)
		return false
	}
	clear(decoded)
	encoded, err := EncodeStanzaFrame(Stanza{Kind: kind, TransferID: frame.TransferID, Data: frame.Data}, t.config.MaximumFrameBytes, t.config.StanzaBudgetBytes)
	clear(encoded)
	return err == nil
}
func (t *PayloadChunks) routeValid(route payload.CarrierRoute) bool {
	t.client.mu.Lock()
	local := t.client.identity.BoundIdentity
	t.client.mu.Unlock()
	return local != "" && route.PeerID == route.RecipientID && route.MeshID == t.client.config.Auth.MeshID && route.SenderID == local && route.MessageID != ""
}
func (t *PayloadChunks) stanza(kind StanzaKind, route payload.CarrierRoute, id string, data []byte) Stanza {
	return Stanza{Kind: kind, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: id, MessageID: route.MessageID, Data: data}
}
func (t *PayloadChunks) matches(id string, route payload.CarrierRoute) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.active[id]
	return ok && state.route == route
}
func (t *PayloadChunks) remove(id string) {
	t.mu.Lock()
	delete(t.active, id)
	t.mu.Unlock()
	t.client.removeCompletion(id)
}

func (t *PayloadChunks) terminalize(id string, result completion) {
	t.mu.Lock()
	_, ok := t.active[id]
	if ok {
		delete(t.active, id)
	}
	t.mu.Unlock()
	if ok {
		t.client.terminalizeCompletion(id, result)
	}
}
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrCompletionAmbiguous) {
		return payload.ErrAmbiguousCompletion
	}
	if errors.Is(err, ErrQueueFull) {
		return payload.ErrQueueFull
	}
	return payload.ErrCarrierUnavailable
}

func (c *Client) registerCompletion(id, peer, messageID string) (<-chan completion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started || len(c.complete) >= c.config.TransferQueue {
		return nil, ErrQueueFull
	}
	if _, exists := c.complete[id]; exists {
		return nil, ErrQueueFull
	}
	result := make(chan completion, 1)
	c.complete[id] = completionWaiter{peer: peer, messageID: messageID, result: result}
	return result, nil
}
func (c *Client) removeCompletion(id string) { c.mu.Lock(); delete(c.complete, id); c.mu.Unlock() }
func (c *Client) terminalizeCompletion(id string, result completion) {
	c.mu.Lock()
	waiter, ok := c.complete[id]
	if ok {
		delete(c.complete, id)
	}
	c.mu.Unlock()
	if ok {
		select {
		case waiter.result <- result:
		default:
		}
	}
}
func (c *Client) deliverCompletion(stanza Stanza) {
	c.mu.Lock()
	waiter, ok := c.complete[stanza.TransferID]
	if !ok || waiter.result == nil || stanza.From != waiter.peer || stanza.MessageID != waiter.messageID || stanza.Evidence.TransferID != stanza.TransferID || stanza.Evidence.MessageID != stanza.MessageID {
		c.mu.Unlock()
		return
	}
	delete(c.complete, stanza.TransferID)
	select {
	case waiter.result <- completion{evidence: stanza.Evidence}:
	default:
	}
	c.mu.Unlock()
}

var _ payload.ChunkCarrier = (*PayloadChunks)(nil)
