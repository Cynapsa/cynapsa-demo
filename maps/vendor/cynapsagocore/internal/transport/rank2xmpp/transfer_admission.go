package rank2xmpp

import (
	"context"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

const (
	MaximumTransferWorkBytes         int64 = 64 << 20
	MaximumUnresolvedTransferBytes   int64 = 16 << 20
	MaximumUnresolvedTransferEntries       = 1024
)

type transferWork struct {
	stanza Stanza
	route  payload.CarrierRoute
	bytes  int64
	lane   int
}

type unresolvedTransferEntry struct {
	stanza Stanza
	bytes  int64
}

type unresolvedTransferLane struct {
	entries []unresolvedTransferEntry
	cancel  context.CancelFunc
}

// transferAdmission owns both pre-authorization and authorized Rank 2
// transfer buffers. Callers take Client.mu before this mutex whenever both are
// needed, so exact-session membership pause is the outer linearization.
type transferAdmission struct {
	mu              sync.Mutex
	normalCount     int
	normalBytes     int64
	normalLaneCount []int
	unresolvedCount int
	unresolvedBytes int64
	unresolved      map[string]*unresolvedTransferLane
	closed          bool
}

func newTransferAdmission(lanes int) transferAdmission {
	return transferAdmission{normalLaneCount: make([]int, lanes), unresolved: make(map[string]*unresolvedTransferLane)}
}

func (c *Client) admitTransfer(stanza Stanza, ingress context.Context) {
	if c == nil {
		clear(stanza.Data)
		return
	}
	c.mu.Lock()
	routes := c.routes
	active := c.started && !c.closed && c.ctx != nil && ingress != nil && ingress.Err() == nil
	c.mu.Unlock()
	if !active || routes == nil {
		clear(stanza.Data)
		return
	}
	// Once a transfer has an unresolved lane, every later frame joins that
	// same lane until its single resolver atomically migrates the whole prefix.
	// Re-resolving here could enqueue a later frame ahead of the retained prefix.
	if c.admitExistingUnresolvedTransfer(stanza) {
		return
	}

	if route, ok := routes.ResolveTransferRoute(stanza.TransferID); ok {
		c.admitResolvedTransfer(stanza, route)
		return
	}
	waiting, ok := routes.(waitingRouteResolver)
	if !ok {
		clear(stanza.Data)
		return
	}
	c.admitUnresolvedTransfer(stanza, waiting)
}

func (c *Client) admitExistingUnresolvedTransfer(stanza Stanza) bool {
	charge, ok := retainedStanzaBytes(stanza)
	if !ok {
		clear(stanza.Data)
		return true
	}
	c.mu.Lock()
	if c.closed || !c.started {
		c.mu.Unlock()
		clear(stanza.Data)
		return true
	}
	c.transferAdmission.mu.Lock()
	admission := &c.transferAdmission
	lane := admission.unresolved[stanza.TransferID]
	if lane == nil {
		admission.mu.Unlock()
		c.mu.Unlock()
		return false
	}
	if admission.closed || admission.unresolvedCount >= c.config.UnresolvedTransferCapacity || charge > c.config.UnresolvedTransferByteCapacity-admission.unresolvedBytes {
		admission.mu.Unlock()
		c.mu.Unlock()
		clear(stanza.Data)
		return true
	}
	lane.entries = append(lane.entries, unresolvedTransferEntry{stanza: stanza, bytes: charge})
	admission.unresolvedCount++
	admission.unresolvedBytes += charge
	admission.mu.Unlock()
	c.mu.Unlock()
	return true
}

func (c *Client) admitResolvedTransfer(stanza Stanza, route payload.CarrierRoute) {
	charge, ok := retainedStanzaBytes(stanza)
	if !ok {
		clear(stanza.Data)
		return
	}
	c.mu.Lock()
	if c.closed || !c.started || !transferRouteAuthorizes(route, stanza) {
		c.mu.Unlock()
		clear(stanza.Data)
		return
	}
	c.transferAdmission.mu.Lock()
	laneIndex := transferLane(stanza.TransferID, len(c.jobs))
	if c.transferAdmission.closed || c.transferAdmission.normalCount >= c.config.TransferQueue || c.transferAdmission.normalLaneCount[laneIndex] >= cap(c.jobs[laneIndex]) || charge > c.config.TransferByteCapacity-c.transferAdmission.normalBytes {
		c.transferAdmission.mu.Unlock()
		c.mu.Unlock()
		clear(stanza.Data)
		return
	}
	work := transferWork{stanza: stanza, route: route, bytes: charge, lane: laneIndex}
	lane := c.jobs[laneIndex]
	select {
	case lane <- work:
		c.transferAdmission.normalCount++
		c.transferAdmission.normalBytes += charge
		c.transferAdmission.normalLaneCount[laneIndex]++
	default:
		clear(stanza.Data)
	}
	c.transferAdmission.mu.Unlock()
	c.mu.Unlock()
}

func (c *Client) admitUnresolvedTransfer(stanza Stanza, waiting waitingRouteResolver) {
	charge, ok := retainedStanzaBytes(stanza)
	if !ok {
		clear(stanza.Data)
		return
	}
	c.mu.Lock()
	if c.closed || !c.started || c.ctx == nil {
		c.mu.Unlock()
		clear(stanza.Data)
		return
	}
	lifetime := c.ctx
	c.transferAdmission.mu.Lock()
	admission := &c.transferAdmission
	if admission.closed || admission.unresolvedCount >= c.config.UnresolvedTransferCapacity || charge > c.config.UnresolvedTransferByteCapacity-admission.unresolvedBytes {
		admission.mu.Unlock()
		c.mu.Unlock()
		clear(stanza.Data)
		return
	}
	lane := admission.unresolved[stanza.TransferID]
	newLane := lane == nil
	var laneContext context.Context
	if newLane {
		var cancel context.CancelFunc
		laneContext, cancel = context.WithCancel(lifetime)
		lane = &unresolvedTransferLane{cancel: cancel}
		admission.unresolved[stanza.TransferID] = lane
	}
	lane.entries = append(lane.entries, unresolvedTransferEntry{stanza: stanza, bytes: charge})
	admission.unresolvedCount++
	admission.unresolvedBytes += charge
	if newLane {
		c.wg.Add(1)
	}
	admission.mu.Unlock()
	c.mu.Unlock()
	if newLane {
		go c.resolveUnresolvedTransfer(laneContext, stanza.TransferID, lane, waiting)
	}
}

func (c *Client) resolveUnresolvedTransfer(lifetime context.Context, transferID string, lane *unresolvedTransferLane, waiting waitingRouteResolver) {
	defer c.wg.Done()
	operation, cancel := context.WithTimeout(lifetime, c.config.UnresolvedTransferLifetime)
	route, ok := waiting.WaitTransferRoute(operation, transferID)
	operationErr := operation.Err()
	cancel()
	if !ok || operationErr != nil {
		c.dropUnresolvedTransfer(transferID, lane)
		return
	}
	c.migrateUnresolvedTransfer(transferID, lane, route)
}

func (c *Client) migrateUnresolvedTransfer(transferID string, owner *unresolvedTransferLane, route payload.CarrierRoute) {
	c.mu.Lock()
	c.transferAdmission.mu.Lock()
	admission := &c.transferAdmission
	lane := admission.unresolved[transferID]
	if lane == nil || lane != owner {
		admission.mu.Unlock()
		c.mu.Unlock()
		return
	}
	valid := !c.closed && c.started
	laneIndex := transferLane(transferID, len(c.jobs))
	var bytes int64
	for _, entry := range lane.entries {
		if !transferRouteAuthorizes(route, entry.stanza) || entry.bytes > c.config.TransferByteCapacity-bytes {
			valid = false
			break
		}
		bytes += entry.bytes
	}
	if valid && (admission.normalCount > c.config.TransferQueue-len(lane.entries) || admission.normalLaneCount[laneIndex] > cap(c.jobs[laneIndex])-len(lane.entries) || bytes > c.config.TransferByteCapacity-admission.normalBytes) {
		valid = false
	}
	delete(admission.unresolved, transferID)
	if lane.cancel != nil {
		lane.cancel()
	}
	admission.unresolvedCount -= len(lane.entries)
	admission.unresolvedBytes -= bytesOfUnresolved(lane.entries)
	if valid {
		for _, entry := range lane.entries {
			work := transferWork{stanza: entry.stanza, route: route, bytes: entry.bytes, lane: laneIndex}
			select {
			case c.jobs[laneIndex] <- work:
				admission.normalCount++
				admission.normalBytes += entry.bytes
				admission.normalLaneCount[laneIndex]++
			default:
				// Capacity accounting guarantees this is reachable only when an
				// out-of-contract test bypassed admission. Fail closed.
				clear(entry.stanza.Data)
			}
		}
	} else {
		clearUnresolved(lane.entries)
	}
	admission.mu.Unlock()
	c.mu.Unlock()
}

func (c *Client) dropUnresolvedTransfer(transferID string, owner *unresolvedTransferLane) {
	c.transferAdmission.mu.Lock()
	lane := c.transferAdmission.unresolved[transferID]
	if lane != nil && lane == owner {
		delete(c.transferAdmission.unresolved, transferID)
		if lane.cancel != nil {
			lane.cancel()
		}
		c.transferAdmission.unresolvedCount -= len(lane.entries)
		c.transferAdmission.unresolvedBytes -= bytesOfUnresolved(lane.entries)
		clearUnresolved(lane.entries)
	}
	c.transferAdmission.mu.Unlock()
}

func (c *Client) releaseTransferWork(work transferWork) {
	clear(work.stanza.Data)
	c.transferAdmission.mu.Lock()
	if c.transferAdmission.normalCount > 0 {
		c.transferAdmission.normalCount--
	}
	if work.lane >= 0 && work.lane < len(c.transferAdmission.normalLaneCount) && c.transferAdmission.normalLaneCount[work.lane] > 0 {
		c.transferAdmission.normalLaneCount[work.lane]--
	}
	if work.bytes <= c.transferAdmission.normalBytes {
		c.transferAdmission.normalBytes -= work.bytes
	} else {
		c.transferAdmission.normalBytes = 0
	}
	c.transferAdmission.mu.Unlock()
}

func (c *Client) retireTransferAdmissionLocked() {
	admission := &c.transferAdmission
	admission.mu.Lock()
	for id, lane := range admission.unresolved {
		delete(admission.unresolved, id)
		if lane.cancel != nil {
			lane.cancel()
		}
		admission.unresolvedCount -= len(lane.entries)
		admission.unresolvedBytes -= bytesOfUnresolved(lane.entries)
		clearUnresolved(lane.entries)
	}
	admission.mu.Unlock()
}

func (c *Client) closeTransferAdmissionLocked() {
	admission := &c.transferAdmission
	admission.mu.Lock()
	admission.closed = true
	for id, lane := range admission.unresolved {
		delete(admission.unresolved, id)
		if lane.cancel != nil {
			lane.cancel()
		}
		clearUnresolved(lane.entries)
	}
	admission.unresolvedCount = 0
	admission.unresolvedBytes = 0
	for _, jobs := range c.jobs {
		for {
			select {
			case work := <-jobs:
				clear(work.stanza.Data)
				admission.normalCount--
				admission.normalBytes -= work.bytes
				if work.lane >= 0 && work.lane < len(admission.normalLaneCount) {
					admission.normalLaneCount[work.lane]--
				}
			default:
				goto drained
			}
		}
	drained:
	}
	if admission.normalCount < 0 {
		admission.normalCount = 0
	}
	if admission.normalBytes < 0 {
		admission.normalBytes = 0
	}
	clear(admission.normalLaneCount)
	admission.mu.Unlock()
}

func (c *Client) transferAdmissionUsage() (int, int64, int, int64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.transferAdmission.mu.Lock()
	defer c.transferAdmission.mu.Unlock()
	return c.transferAdmission.normalCount, c.transferAdmission.normalBytes, c.transferAdmission.unresolvedCount, c.transferAdmission.unresolvedBytes
}

func transferRouteAuthorizes(route payload.CarrierRoute, stanza Stanza) bool {
	return route.PeerID == stanza.From && route.MeshID == stanza.MeshID && route.SenderID == stanza.From && route.RecipientID == stanza.To && route.MessageID != "" && (stanza.MessageID == "" || stanza.MessageID == route.MessageID)
}

func bytesOfUnresolved(entries []unresolvedTransferEntry) int64 {
	var total int64
	for _, entry := range entries {
		total += entry.bytes
	}
	return total
}

func clearUnresolved(entries []unresolvedTransferEntry) {
	for i := range entries {
		clear(entries[i].stanza.Data)
		entries[i] = unresolvedTransferEntry{}
	}
}
