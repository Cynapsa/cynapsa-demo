package rank1webrtc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

const maximumDefaultTransferQueueBytes = 64 << 20

type pendingTransfer struct {
	frames   []Frame
	bytes    int
	deadline time.Time
}

type transferJob struct {
	id    string
	frame Frame
	size  int
}

type transferUsage struct {
	count int
	bytes int
}

// transferAdmission owns the only retained pre-route frame copies. One queue
// token per transfer preserves unordered DataChannel arrival order without
// ever making Link.pump wait for route creation or receiver capacity.
type transferAdmission struct {
	link       *Link
	workers    int
	queue      chan string
	mu         sync.Mutex
	pending    map[string]*pendingTransfer
	usage      map[string]transferUsage
	count      int
	bytes      int
	maxCount   int
	maxBytes   int
	perIDCount int
	perIDBytes int
}

func newTransferAdmission(link *Link) *transferAdmission {
	workers := min(link.config.TransferWorkers, 4)
	perIDCount := max(1, link.config.TransferQueue/2)
	perIDBytes := max(link.config.MaximumFrameBytes, link.config.TransferQueueBytes/2)
	return &transferAdmission{
		link: link, workers: workers, queue: make(chan string, link.config.TransferQueue),
		pending: make(map[string]*pendingTransfer), usage: make(map[string]transferUsage), maxCount: link.config.TransferQueue,
		maxBytes: link.config.TransferQueueBytes, perIDCount: perIDCount, perIDBytes: perIDBytes,
	}
}

func defaultTransferQueueBytes(count, maximumFrame int) int {
	if count <= 0 || maximumFrame <= 0 {
		return 0
	}
	if count > maximumDefaultTransferQueueBytes/maximumFrame {
		return maximumDefaultTransferQueueBytes
	}
	return count * maximumFrame
}

func (a *transferAdmission) admit(frame Frame) {
	if a == nil || a.link.routes == nil || a.link.ctx.Err() != nil || a.link.authorityBlocked.Load() {
		clearOwnedFrame(&frame)
		return
	}
	owned := cloneOwnedFrame(frame)
	size := retainedFrameBytes(owned)
	a.mu.Lock()
	entry := a.pending[owned.TransferID]
	usage := a.usage[owned.TransferID]
	if size > a.maxBytes || size > a.perIDBytes || a.count >= a.maxCount || a.bytes > a.maxBytes-size || usage.count >= a.perIDCount || usage.bytes > a.perIDBytes-size {
		a.mu.Unlock()
		clearOwnedFrame(&owned)
		return
	}
	if entry != nil {
		entry.frames = append(entry.frames, owned)
		entry.bytes += size
		a.count++
		a.bytes += size
		a.usage[owned.TransferID] = transferUsage{count: usage.count + 1, bytes: usage.bytes + size}
		a.mu.Unlock()
		return
	}
	entry = &pendingTransfer{frames: []Frame{owned}, bytes: size, deadline: time.Now().Add(a.link.config.TransferRouteTimeout)}
	a.pending[owned.TransferID] = entry
	a.count++
	a.bytes += size
	a.usage[owned.TransferID] = transferUsage{count: usage.count + 1, bytes: usage.bytes + size}
	select {
	case a.queue <- owned.TransferID:
		a.mu.Unlock()
	default:
		delete(a.pending, owned.TransferID)
		a.releaseLocked(owned.TransferID, size)
		a.mu.Unlock()
		clearOwnedFrame(&owned)
	}
}

func (a *transferAdmission) run() {
	defer a.link.wg.Done()
	for {
		if a.link.ctx.Err() != nil {
			return
		}
		select {
		case id := <-a.queue:
			a.resolve(id)
		case <-a.link.ctx.Done():
			return
		}
	}
}

func (a *transferAdmission) resolve(id string) {
	if a.link.authorityBlocked.Load() {
		a.reject(id)
		return
	}
	a.mu.Lock()
	entry := a.pending[id]
	if entry == nil {
		a.mu.Unlock()
		return
	}
	deadline := entry.deadline
	a.mu.Unlock()

	operation, cancel := context.WithDeadline(a.link.ctx, deadline)
	route, ok := callResolveTransferRoute(a.link.routes, id)
	if !ok {
		if waiting, supportsWait := a.link.routes.(waitingRouteResolver); supportsWait {
			route, ok = callWaitTransferRoute(waiting, operation, id)
		}
	}
	cancel()
	if !ok || !a.validRoute(route) {
		a.reject(id)
		return
	}
	for {
		a.mu.Lock()
		entry = a.pending[id]
		if entry == nil {
			a.mu.Unlock()
			return
		}
		if len(entry.frames) == 0 {
			delete(a.pending, id)
			a.mu.Unlock()
			return
		}
		frame := entry.frames[0]
		entry.frames[0] = Frame{}
		entry.frames = entry.frames[1:]
		originalSize := retainedFrameBytes(frame)
		entry.bytes -= originalSize
		frame.Route = cloneOwnedRoute(route)
		size := retainedFrameBytes(frame)
		added := size - originalSize
		usage := a.usage[id]
		if added > a.maxBytes-a.bytes || added > a.perIDBytes-usage.bytes {
			a.releaseLocked(id, originalSize)
			a.mu.Unlock()
			clearOwnedFrame(&frame)
			a.reject(id)
			return
		}
		a.bytes += added
		usage.bytes += added
		a.usage[id] = usage
		a.mu.Unlock()
		select {
		case a.link.jobs[transferLane(id, len(a.link.jobs))] <- transferJob{id: id, frame: frame, size: size}:
		case <-a.link.ctx.Done():
			clearOwnedFrame(&frame)
			a.release(id, size)
			a.reject(id)
			return
		}
	}
}

func (a *transferAdmission) release(id string, size int) {
	if a == nil || size < 0 {
		return
	}
	a.mu.Lock()
	a.releaseLocked(id, size)
	a.mu.Unlock()
}

func (a *transferAdmission) releaseLocked(id string, size int) {
	usage, ok := a.usage[id]
	if !ok || usage.count <= 0 || usage.bytes < size || a.count <= 0 || a.bytes < size {
		return
	}
	a.count--
	a.bytes -= size
	usage.count--
	usage.bytes -= size
	if usage.count == 0 {
		delete(a.usage, id)
	} else {
		a.usage[id] = usage
	}
}

func callResolveTransferRoute(resolver RouteResolver, id string) (route payload.CarrierRoute, ok bool) {
	defer func() {
		if recover() != nil {
			route, ok = payload.CarrierRoute{}, false
		}
	}()
	return resolver.ResolveTransferRoute(id)
}

func callWaitTransferRoute(resolver waitingRouteResolver, ctx context.Context, id string) (route payload.CarrierRoute, ok bool) {
	defer func() {
		if recover() != nil {
			route, ok = payload.CarrierRoute{}, false
		}
	}()
	return resolver.WaitTransferRoute(ctx, id)
}

func (a *transferAdmission) validRoute(route payload.CarrierRoute) bool {
	return route.PeerID == a.link.config.PeerID && route.MeshID == a.link.config.MeshID && route.SenderID == a.link.config.PeerID && route.RecipientID == a.link.config.LocalIdentity
}

func (a *transferAdmission) reject(id string) {
	a.mu.Lock()
	entry := a.pending[id]
	if entry != nil {
		delete(a.pending, id)
		for i := range entry.frames {
			a.releaseLocked(id, retainedFrameBytes(entry.frames[i]))
		}
	}
	a.mu.Unlock()
	if entry != nil {
		for i := range entry.frames {
			clearOwnedFrame(&entry.frames[i])
		}
	}
}

// clearAll runs only after every resolver and receiver worker has joined.
// It retires the remaining pre-route ownership and all accounting exactly once.
func (a *transferAdmission) clearAll() {
	if a == nil {
		return
	}
	a.mu.Lock()
	for id, entry := range a.pending {
		for i := range entry.frames {
			clearOwnedFrame(&entry.frames[i])
		}
		delete(a.pending, id)
	}
	a.pending = make(map[string]*pendingTransfer)
	a.usage = make(map[string]transferUsage)
	a.count, a.bytes = 0, 0
	for {
		select {
		case <-a.queue:
		default:
			a.mu.Unlock()
			return
		}
	}
}

func cloneOwnedFrame(frame Frame) Frame {
	owned := frame.Clone()
	owned.TransferID = strings.Clone(frame.TransferID)
	owned.Route = cloneOwnedRoute(frame.Route)
	owned.Evidence.TransferID = strings.Clone(frame.Evidence.TransferID)
	owned.Evidence.MessageID = strings.Clone(frame.Evidence.MessageID)
	return owned
}

func cloneOwnedRoute(route payload.CarrierRoute) payload.CarrierRoute {
	return payload.CarrierRoute{
		PeerID: strings.Clone(route.PeerID), MeshID: strings.Clone(route.MeshID), SenderID: strings.Clone(route.SenderID),
		RecipientID: strings.Clone(route.RecipientID), MessageID: strings.Clone(route.MessageID),
	}
}

func retainedFrameBytes(frame Frame) int {
	return len(frame.TransferID) + len(frame.Data) + len(frame.ProbeNonce) +
		len(frame.Route.PeerID) + len(frame.Route.MeshID) + len(frame.Route.SenderID) + len(frame.Route.RecipientID) + len(frame.Route.MessageID) +
		len(frame.Evidence.TransferID) + len(frame.Evidence.MessageID) +
		len(frame.Receipt.MessageID) + len(frame.Receipt.ConversationID) + len(frame.Receipt.Sender) + len(frame.Receipt.Recipient) + len(frame.Receipt.MeshID)
}

func clearOwnedFrame(frame *Frame) {
	if frame == nil {
		return
	}
	clear(frame.Data)
	clear(frame.ProbeNonce)
	*frame = Frame{}
}
