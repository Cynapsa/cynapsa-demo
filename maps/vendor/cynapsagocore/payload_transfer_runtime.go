package cynapsagocore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank1webrtc"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type payloadRouteEntry struct {
	route   payload.CarrierRoute
	binding payload.TransferBinding
	refs    int
}

// authenticatedPayloadRuntime is the one session-owned bridge between the
// carrier-neutral payload state machine and authenticated private frame
// delivery. Routes exist only while a validated logical envelope is waiting
// for its private transfer; carrier bytes can never create routing authority.
type authenticatedPayloadRuntime struct {
	transfers *payload.Reassembler
	wait      time.Duration
	capacity  int

	mu       sync.Mutex
	routes   map[string]*payloadRouteEntry
	changed  chan struct{}
	pipeline *payload.Pipeline
	admit    func(context.Context, string, uint64, func(context.Context) error) error
	closed   bool
}

func newAuthenticatedPayloadRuntime(transfers *payload.Reassembler, capacity int, wait time.Duration) (*authenticatedPayloadRuntime, error) {
	if transfers == nil || capacity <= 0 || wait <= 0 {
		return nil, payload.ErrInvalidLimits
	}
	return &authenticatedPayloadRuntime{transfers: transfers, wait: wait, capacity: capacity, routes: make(map[string]*payloadRouteEntry), changed: make(chan struct{})}, nil
}

func (runtime *authenticatedPayloadRuntime) registerEnvelope(envelope protocol.Envelope) (func(), error) {
	if runtime == nil {
		return nil, payload.ErrCarrierUnavailable
	}
	transferID, err := payload.PrivateTransferID(envelope.Payload)
	if err != nil {
		return nil, err
	}
	if transferID == "" {
		return func() {}, nil
	}
	route := payload.CarrierRoute{PeerID: envelope.Sender, MeshID: envelope.MeshID, SenderID: envelope.Sender, RecipientID: envelope.Recipient, MessageID: envelope.MessageID}
	binding := payload.TransferBinding{
		TransferID: transferID, MessageID: envelope.MessageID, MeshID: envelope.MeshID,
		SenderID: envelope.Sender, RecipientID: envelope.Recipient, Profile: envelope.Payload.Profile,
		CanonicalSize: envelope.Payload.Size, CanonicalDigest: envelope.Payload.Digest,
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil, payload.ErrCarrierUnavailable
	}
	entry := runtime.routes[transferID]
	if entry != nil {
		if entry.route != route || entry.binding != binding {
			runtime.mu.Unlock()
			return nil, payload.ErrAuthentication
		}
		entry.refs++
	} else {
		if len(runtime.routes) >= runtime.capacity {
			runtime.mu.Unlock()
			return nil, payload.ErrQueueFull
		}
		runtime.routes[transferID] = &payloadRouteEntry{route: route, binding: binding, refs: 1}
		runtime.notifyLocked()
	}
	runtime.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { runtime.release(transferID, route, binding) }) }, nil
}

func (runtime *authenticatedPayloadRuntime) ResolveTransferRoute(transferID string) (payload.CarrierRoute, bool) {
	if runtime == nil || transferID == "" {
		return payload.CarrierRoute{}, false
	}
	runtime.mu.Lock()
	entry := runtime.routes[transferID]
	closed := runtime.closed
	var route payload.CarrierRoute
	if entry != nil {
		route = entry.route
	}
	runtime.mu.Unlock()
	return route, !closed && entry != nil
}

func (runtime *authenticatedPayloadRuntime) WaitTransferRoute(ctx context.Context, transferID string) (payload.CarrierRoute, bool) {
	if runtime == nil || ctx == nil || transferID == "" {
		return payload.CarrierRoute{}, false
	}
	operation, cancel := context.WithTimeout(ctx, runtime.wait)
	defer cancel()
	for {
		runtime.mu.Lock()
		entry := runtime.routes[transferID]
		if entry != nil && !runtime.closed {
			route := entry.route
			runtime.mu.Unlock()
			return route, true
		}
		if runtime.closed {
			runtime.mu.Unlock()
			return payload.CarrierRoute{}, false
		}
		changed := runtime.changed
		runtime.mu.Unlock()
		select {
		case <-changed:
		case <-operation.Done():
			return payload.CarrierRoute{}, false
		}
	}
}

func (runtime *authenticatedPayloadRuntime) HandleFrame(ctx context.Context, route payload.CarrierRoute, frame rank1webrtc.Frame) (*payload.CompletionEvidence, error) {
	var evidence *payload.CompletionEvidence
	err := runtime.AdmitAuthenticatedPayloadWork(ctx, route.PeerID, payloadFrameOwnedBytes(route, frame.TransferID, frame.Data), func(runCtx context.Context) (err error) {
		evidence, err = runtime.handle(runCtx, payload.CarrierDirectChunks, route, frame.Kind, frame.TransferID, frame.Data)
		return err
	})
	return evidence, err
}

func (runtime *authenticatedPayloadRuntime) HandleStanza(ctx context.Context, route payload.CarrierRoute, stanza rank2xmpp.Stanza) (*payload.CompletionEvidence, error) {
	var evidence *payload.CompletionEvidence
	err := runtime.AdmitAuthenticatedPayloadWork(ctx, route.PeerID, payloadFrameOwnedBytes(route, stanza.TransferID, stanza.Data), func(runCtx context.Context) (err error) {
		evidence, err = runtime.handleStanza(runCtx, route, stanza)
		return err
	})
	return evidence, err
}

func (runtime *authenticatedPayloadRuntime) handleStanza(ctx context.Context, route payload.CarrierRoute, stanza rank2xmpp.Stanza) (*payload.CompletionEvidence, error) {
	if stanza.Kind == rank2xmpp.StanzaObjectTransfer {
		publication, err := rank2xmpp.DecodeObjectPublication(stanza.Data)
		if err != nil || publication.Manifest.TransferID != stanza.TransferID {
			return nil, payload.ErrAuthentication
		}
		binding, ok := runtime.binding(stanza.TransferID, route)
		if !ok || !manifestMatchesBinding(publication.Manifest, binding) {
			return nil, payload.ErrAuthentication
		}
		runtime.mu.Lock()
		pipeline := runtime.pipeline
		runtime.mu.Unlock()
		if pipeline == nil {
			return nil, payload.ErrCarrierUnavailable
		}
		evidence, err := pipeline.IngestObject(ctx, publication.Manifest, binding, publication.PrivateReference)
		if err != nil {
			return nil, err
		}
		return &evidence, nil
	}
	if stanza.Kind == rank2xmpp.StanzaObjectTransferAbort {
		publication, err := rank2xmpp.DecodeObjectPublication(stanza.Data)
		if err != nil || publication.Manifest.TransferID != stanza.TransferID {
			return nil, payload.ErrAuthentication
		}
		binding, ok := runtime.binding(stanza.TransferID, route)
		if !ok || !manifestMatchesBinding(publication.Manifest, binding) {
			return nil, payload.ErrAuthentication
		}
		return nil, runtime.transfers.Abort(stanza.TransferID, payload.CarrierObjectUpload)
	}
	return runtime.handle(ctx, payload.CarrierMessageChunks, route, stanzaKindToFrameKind(stanza.Kind), stanza.TransferID, stanza.Data)
}

func (runtime *authenticatedPayloadRuntime) bindMessaging(messaging *mesh.MessagingService) error {
	if runtime == nil || messaging == nil {
		return payload.ErrInvalidLimits
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.admit != nil {
		return payload.ErrInvalidHandleState
	}
	runtime.admit = func(ctx context.Context, peerID string, ownedBytes uint64, run func(context.Context) error) error {
		result, failure := messaging.AdmitAuthenticatedPeerWork(ctx, peerID, peer.Work{
			Kind: peer.WorkInbound, OwnedBytes: ownedBytes,
			Run: func(runCtx context.Context, commit peer.Commit) error {
				return commit(func() error { return run(runCtx) })
			},
			Clear: func() {},
		})
		if failure != nil {
			return payloadPeerFailure(failure)
		}
		return result.Wait(ctx)
	}
	return nil
}

// AdmitAuthenticatedPayloadWork is also the durable-carrier object-readiness
// seam. It transfers the exact authenticated peer's control operation into
// the common lane before payload mutation, probe I/O, or completion publication.
func (runtime *authenticatedPayloadRuntime) AdmitAuthenticatedPayloadWork(ctx context.Context, peerID string, ownedBytes uint64, run func(context.Context) error) error {
	if runtime == nil || ctx == nil || peerID == "" || run == nil {
		return payload.ErrAuthentication
	}
	runtime.mu.Lock()
	admit, closed := runtime.admit, runtime.closed
	runtime.mu.Unlock()
	if closed || admit == nil {
		return payload.ErrCarrierUnavailable
	}
	// The carrier worker remains the sole byte owner until this synchronous
	// admission returns; its existing unwind path performs exact scrubbing.
	if err := admit(ctx, peerID, ownedBytes, run); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, peer.ErrUnauthorized) {
			return payload.ErrAuthorization
		}
		if errors.Is(err, peer.ErrQueueFull) {
			return payload.ErrQueueFull
		}
		if errors.Is(err, peer.ErrUnavailable) || errors.Is(err, peer.ErrClosed) {
			return payload.ErrCarrierUnavailable
		}
		return err
	}
	return nil
}

func payloadPeerFailure(failure *mesh.Failure) error {
	if failure == nil {
		return nil
	}
	switch failure.Code {
	case mesh.FailureCancelled:
		return context.Canceled
	case mesh.FailureDeadline:
		return context.DeadlineExceeded
	case mesh.FailureCapacity:
		return payload.ErrQueueFull
	case mesh.FailureRejected:
		return payload.ErrAuthentication
	case mesh.FailureAuthorization:
		return payload.ErrAuthorization
	default:
		return payload.ErrCarrierUnavailable
	}
}

func payloadFrameOwnedBytes(route payload.CarrierRoute, transferID string, data []byte) uint64 {
	return uint64(len(route.PeerID) + len(route.MeshID) + len(route.SenderID) + len(route.RecipientID) + len(route.MessageID) + len(transferID) + len(data))
}

func (runtime *authenticatedPayloadRuntime) bindPipeline(pipeline *payload.Pipeline) error {
	if runtime == nil || pipeline == nil {
		return payload.ErrInvalidLimits
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.pipeline != nil {
		return payload.ErrInvalidHandleState
	}
	runtime.pipeline = pipeline
	return nil
}

func (runtime *authenticatedPayloadRuntime) handle(ctx context.Context, carrier payload.CarrierKind, route payload.CarrierRoute, kind transport.FrameKind, transferID string, data []byte) (*payload.CompletionEvidence, error) {
	if runtime == nil || ctx == nil || transferID == "" {
		return nil, payload.ErrAuthentication
	}
	binding, ok := runtime.binding(transferID, route)
	if !ok {
		return nil, payload.ErrAuthentication
	}
	switch kind {
	case transport.FrameTransferManifest:
		manifest, err := decodeCarrierManifest(carrier, data)
		if err != nil || manifest.TransferID != transferID || !manifestMatchesBinding(manifest, binding) {
			return nil, payload.ErrAuthentication
		}
		return nil, runtime.transfers.Begin(manifest, binding, carrier)
	case transport.FrameTransferChunk:
		chunk, err := decodeCarrierChunk(carrier, data)
		if err != nil || chunk.TransferID != transferID {
			return nil, payload.ErrIntegrity
		}
		defer clearBytes(chunk.Bytes)
		defer clearBytes(chunk.Digest)
		return nil, runtime.transfers.Accept(carrier, chunk)
	case transport.FrameTransferFinish:
		evidence, err := runtime.transfers.Complete(ctx, transferID, carrier)
		if err != nil {
			return nil, err
		}
		return &evidence, nil
	case transport.FrameTransferAbort:
		return nil, runtime.transfers.Abort(transferID, carrier)
	default:
		return nil, payload.ErrCarrierRejected
	}
}

func (runtime *authenticatedPayloadRuntime) binding(transferID string, route payload.CarrierRoute) (payload.TransferBinding, bool) {
	runtime.mu.Lock()
	entry := runtime.routes[transferID]
	closed := runtime.closed
	var binding payload.TransferBinding
	if entry != nil && entry.route == route {
		binding = entry.binding
	}
	runtime.mu.Unlock()
	return binding, !closed && entry != nil && entry.route == route
}

func (runtime *authenticatedPayloadRuntime) release(transferID string, route payload.CarrierRoute, binding payload.TransferBinding) {
	runtime.mu.Lock()
	entry := runtime.routes[transferID]
	if entry != nil && entry.route == route && entry.binding == binding {
		entry.refs--
		if entry.refs == 0 {
			delete(runtime.routes, transferID)
			runtime.notifyLocked()
		}
	}
	runtime.mu.Unlock()
}

func (runtime *authenticatedPayloadRuntime) retirePeer(meshID, peerID string) {
	if runtime == nil || meshID == "" || peerID == "" {
		return
	}
	runtime.mu.Lock()
	for transferID, entry := range runtime.routes {
		if entry.route.MeshID == meshID && (entry.route.SenderID == peerID || entry.route.RecipientID == peerID || entry.route.PeerID == peerID) {
			delete(runtime.routes, transferID)
		}
	}
	runtime.notifyLocked()
	runtime.mu.Unlock()
}

func (runtime *authenticatedPayloadRuntime) Close() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if !runtime.closed {
		runtime.closed = true
		clear(runtime.routes)
		runtime.pipeline = nil
		runtime.admit = nil
		runtime.notifyLocked()
	}
	runtime.mu.Unlock()
	runtime.transfers.Close()
}

// activeTransfers combines outbound carrier ownership with inbound logical
// transfers waiting for authenticated materialization. It saturates at the V1
// diagnostic counter ceiling and never exposes route or transfer identifiers.
func (runtime *authenticatedPayloadRuntime) activeTransfers() uint64 {
	if runtime == nil {
		return 0
	}
	runtime.mu.Lock()
	inbound := uint64(len(runtime.routes))
	pipeline := runtime.pipeline
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return 0
	}
	outbound := uint64(0)
	if pipeline != nil {
		outbound = pipeline.ActiveTransfers()
	}
	const maximum = uint64(65_536)
	if inbound >= maximum || outbound >= maximum-inbound {
		return maximum
	}
	return inbound + outbound
}

func (runtime *authenticatedPayloadRuntime) notifyLocked() {
	close(runtime.changed)
	runtime.changed = make(chan struct{})
}

func stanzaKindToFrameKind(kind rank2xmpp.StanzaKind) transport.FrameKind {
	switch kind {
	case rank2xmpp.StanzaTransferManifest:
		return transport.FrameTransferManifest
	case rank2xmpp.StanzaTransferChunk:
		return transport.FrameTransferChunk
	case rank2xmpp.StanzaTransferFinish:
		return transport.FrameTransferFinish
	case rank2xmpp.StanzaTransferAbort:
		return transport.FrameTransferAbort
	default:
		return 0
	}
}

func decodeCarrierManifest(carrier payload.CarrierKind, data []byte) (payload.TransferManifest, error) {
	if carrier == payload.CarrierMessageChunks {
		return payload.DecodeTextManifest(string(data))
	}
	return payload.DecodeManifest(data)
}

func decodeCarrierChunk(carrier payload.CarrierKind, data []byte) (payload.TransferChunk, error) {
	if carrier == payload.CarrierMessageChunks {
		return payload.DecodeTextChunk(string(data), 1<<20)
	}
	return payload.DecodeChunk(data, 1<<20)
}

func manifestMatchesBinding(manifest payload.TransferManifest, binding payload.TransferBinding) bool {
	return manifest.MessageID == binding.MessageID && manifest.MeshID == binding.MeshID && manifest.SenderID == binding.SenderID && manifest.RecipientID == binding.RecipientID && manifest.CanonicalSize == binding.CanonicalSize && bytes.Equal(manifest.CanonicalDigest, binding.CanonicalDigest[:])
}

func clearBytes(value []byte) { clear(value) }

var _ rank1webrtc.TransferReceiver = (*authenticatedPayloadRuntime)(nil)
var _ rank1webrtc.RouteResolver = (*authenticatedPayloadRuntime)(nil)
var _ rank2xmpp.TransferReceiver = (*authenticatedPayloadRuntime)(nil)
var _ rank2xmpp.RouteResolver = (*authenticatedPayloadRuntime)(nil)
