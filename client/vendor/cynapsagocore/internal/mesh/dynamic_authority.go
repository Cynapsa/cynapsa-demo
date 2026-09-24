package mesh

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

// ResolvedPeer is authenticated server output from one Cynapsa handshake.
// Bare and Full identify a single target; installation and generation bind
// subsequent installation revokes to the exact server session authorized.
type ResolvedPeer struct {
	Bare              string
	Full              string
	InstallationID    string
	SessionGeneration string
}

// PeerResolver must return only server-authenticated, mesh-authorized active
// targets. TopologyUnavailable includes a target with no active session;
// TopologyRejected means the server rejected the handshake's authorization.
type PeerResolver interface {
	ResolvePeer(context.Context, string) (ResolvedPeer, TopologyDisposition)
}

// ExactPeerResolver is required for first inbound contact and for reconnect
// revalidation of retained exact-addressed outbox entries. It must query the
// authenticated server for this exact full JID, never select by bare name.
type ExactPeerResolver interface {
	ResolveExactPeer(context.Context, string) (ResolvedPeer, TopologyDisposition)
}

// FailPendingRequestsOnAuthenticationRejection wakes RPC callers when the
// local installation is definitively denied. The public composition layer
// projects this terminal transport state to authentication_failed.
func (service *MessagingService) FailPendingRequestsOnAuthenticationRejection() {
	if service != nil && service.outboundRPC != nil {
		service.outboundRPC.FailAll(rpc.ErrCancelled)
	}
}

func (service *MessagingService) ensureDynamicPeer(ctx context.Context, bare string) *Failure {
	if service == nil || ctx == nil {
		return &Failure{Code: FailureRejected}
	}
	service.refreshMu.Lock()
	ready := service.dynamicPublicationReady() && !service.topology.authorityBlocked()
	full := service.outboundBound[bare]
	_, known := service.dynamicPeers[full]
	known = known && service.peerLanes.Authorize(full) == nil
	if !ready {
		service.refreshMu.Unlock()
		return &Failure{Code: FailureUnavailable}
	}
	if known {
		service.refreshMu.Unlock()
		return nil
	}
	if attempt := service.resolvingPeers[bare]; attempt != nil {
		service.refreshMu.Unlock()
		select {
		case <-attempt.done:
			return attempt.failure
		case <-ctx.Done():
			return contextFailure(ctx)
		}
	}
	attempt := &peerResolutionAttempt{done: make(chan struct{})}
	service.resolvingPeers[bare] = attempt
	service.refreshMu.Unlock()
	failure := service.AuthorizePeer(ctx, bare)
	service.refreshMu.Lock()
	attempt.failure = failure
	delete(service.resolvingPeers, bare)
	close(attempt.done)
	service.refreshMu.Unlock()
	return failure
}

func (service *MessagingService) ensureExactDynamicPeer(ctx context.Context, full string) *Failure {
	if service == nil || ctx == nil {
		return &Failure{Code: FailureRejected}
	}
	service.refreshMu.Lock()
	ready := service.dynamicPublicationReady() && !service.topology.authorityBlocked()
	_, known := service.dynamicPeers[full]
	known = known && service.peerLanes.Authorize(full) == nil
	service.refreshMu.Unlock()
	if !ready {
		return &Failure{Code: FailureUnavailable}
	}
	if known {
		return nil
	}
	return service.AuthorizeExactPeer(ctx, full)
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func (service *MessagingService) dynamicPublicationReady() bool {
	return service.peerResolver == nil || service.dynamicPublication.Load()&1 != 0
}

// The generation and open bit share one atomic word. A concurrent fence can
// never be overwritten by a delayed post-publication opener.
func (service *MessagingService) closeDynamicPublication() uint64 {
	for {
		current := service.dynamicPublication.Load()
		if current >= ^uint64(0)-2 {
			if service.dynamicPublication.CompareAndSwap(current, ^uint64(0)-1) {
				return 0 // permanently closed on generation exhaustion
			}
			continue
		}
		next := (current &^ 1) + 2
		if service.dynamicPublication.CompareAndSwap(current, next) {
			return next
		}
	}
}

// InitializeLocalAuthority is the new-login/reconnect boundary. It fences
// all prior peer work, joins its cleanup, and opens authority only for the
// authenticated local identity. The server has already verified this login.
func (service *MessagingService) InitializeLocalAuthority(ctx context.Context) error {
	if service == nil || ctx == nil || service.peerResolver == nil {
		return ErrInvalidConfig
	}
	publication := service.closeDynamicPublication()
	service.advanceAuthorityFenceEpoch()
	service.topology.BlockAdmission()
	service.peerLanes.BlockAdmission()
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	service.replayLocalFull = ""
	service.dynamicPublicationPrepared = 0
	for full, current := range service.dynamicPeers {
		service.suspendedPeers[full] = current
	}
	if err := service.peerLanes.ResetDynamicAuthority(ctx); err != nil {
		return err
	}
	local := Identity{AgentID: service.identity.AgentID(), Internal: service.identity.BoundFull()}
	if err := service.topology.InitializeLocalAuthority(ctx, local); err != nil {
		service.peerLanes.BlockAdmission()
		return err
	}
	clear(service.dynamicPeers)
	clear(service.outboundBound)
	if publication == 0 || service.dynamicPublication.Load() != publication {
		return ErrSnapshotStale
	}
	service.dynamicPublicationPrepared = publication
	service.wakeDrain()
	// The old exact envelopes remain parked until their server session is
	// revalidated. This runs outside refreshMu to keep controls responsive.
	// The caller also gets an explicit ReauthorizeSuspendedPeers seam for a
	// later retry when a target was temporarily unavailable.
	return nil
}

// DropDynamicAuthority synchronously tears down all peer state on a durable
// transport outage. It deliberately does not reopen local admission: the
// caller must complete a new authenticated bind before InitializeLocalAuthority.
func (service *MessagingService) DropDynamicAuthority(ctx context.Context) error {
	if service == nil || ctx == nil || service.peerResolver == nil {
		return ErrInvalidConfig
	}
	service.closeDynamicPublication()
	service.advanceAuthorityFenceEpoch()
	service.topology.BlockAdmission()
	service.peerLanes.BlockAdmission()
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	service.replayLocalFull = ""
	service.dynamicPublicationPrepared = 0
	for full, current := range service.dynamicPeers {
		service.suspendedPeers[full] = current
	}
	if err := service.peerLanes.DropDynamicAuthority(ctx); err != nil {
		return err
	}
	if err := service.topology.ClearDynamicAuthority(ctx); err != nil {
		return err
	}
	clear(service.dynamicPeers)
	clear(service.outboundBound)
	return nil
}

// ReauthorizeSuspendedPeers is the post-Manager-publication edge. It confirms
// the actual full JID of the authenticated bind, opens this exact prepared
// generation, and schedules bounded exact-destination checks. A concurrent
// fence advances the generation and cannot be undone by this call.
func (service *MessagingService) ReauthorizeSuspendedPeers(ctx context.Context, currentLocalFull string) error {
	if service == nil || ctx == nil || service.peerResolver == nil || currentLocalFull != service.identity.BoundFull() {
		return ErrIdentityBinding
	}
	if _, ok := service.peerResolver.(ExactPeerResolver); !ok {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service.refreshMu.Lock()
	if service.topology.authorityBlocked() || service.dynamicPublicationPrepared == 0 {
		service.refreshMu.Unlock()
		return ErrSnapshotStale
	}
	prepared := service.dynamicPublicationPrepared
	current := service.dynamicPublication.Load()
	if current != prepared && current != prepared|1 {
		service.refreshMu.Unlock()
		return ErrSnapshotStale
	}
	if current == prepared && !service.dynamicPublication.CompareAndSwap(prepared, prepared|1) {
		service.refreshMu.Unlock()
		return ErrSnapshotStale
	}
	service.replayLocalFull = currentLocalFull
	service.refreshMu.Unlock()
	service.wakeDrain()
	return nil
}

// startSuspendedPeerRetry never blocks runOutboxDrain. A single server query
// is permitted in flight; each polling pass gives one candidate a turn.
func (service *MessagingService) startSuspendedPeerRetry(ctx context.Context) {
	if service.peerResolver == nil || !service.replayRunning.CompareAndSwap(false, true) {
		return
	}
	service.refreshMu.Lock()
	ready := service.dynamicPublicationReady() && service.replayLocalFull == service.identity.BoundFull() && len(service.suspendedPeers) != 0 && !service.topology.authorityBlocked()
	service.refreshMu.Unlock()
	if !ready || ctx.Err() != nil {
		service.replayRunning.Store(false)
		return
	}
	service.workers.Add(1)
	go func() {
		defer service.workers.Done()
		defer service.replayRunning.Store(false)
		_ = service.reauthorizeOneSuspendedPeer(ctx)
	}()
}

func (service *MessagingService) reauthorizeOneSuspendedPeer(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	exact, ok := service.peerResolver.(ExactPeerResolver)
	if !ok {
		return ErrInvalidConfig
	}
	service.refreshMu.Lock()
	if !service.dynamicPublicationReady() || service.replayLocalFull != service.identity.BoundFull() || service.topology.authorityBlocked() {
		service.refreshMu.Unlock()
		return ErrSnapshotStale
	}
	keys := make([]string, 0, len(service.suspendedPeers))
	for full := range service.suspendedPeers {
		keys = append(keys, full)
	}
	if len(keys) == 0 {
		service.refreshMu.Unlock()
		return nil
	}
	sort.Strings(keys)
	index := service.replayCursor % len(keys)
	service.replayCursor++
	candidate := service.suspendedPeers[keys[index]]
	epoch := service.authorityFenceEpoch.Load()
	service.refreshMu.Unlock()
	resolved, disposition := exact.ResolveExactPeer(ctx, candidate.Full)
	if err := ctx.Err(); err != nil {
		return err
	}
	if service.authorityFenceEpoch.Load() != epoch {
		return ErrSnapshotStale
	}
	if !service.dynamicPublicationReady() {
		return ErrSnapshotStale
	}
	switch disposition {
	case TopologyUnavailable:
		return nil // Keep this exact destination parked for a later poll.
	case TopologyRejected:
		service.refreshMu.Lock()
		if service.authorityFenceEpoch.Load() == epoch && service.suspendedPeers[candidate.Full] == candidate {
			service.retireSuspendedPeerLocked(candidate)
		}
		service.refreshMu.Unlock()
		return nil
	case TopologyReady:
		return meshFailureError(service.installResolvedPeer(ctx, candidate.Bare, candidate.Full, epoch, resolved, disposition, false))
	default:
		return ErrSnapshotInvalid
	}
}

// AuthorizePeer resolves one bare peer through the server before adding any
// exact endpoint. A stale handshake result cannot reopen an old authority
// session after a disconnect, reconnect, or revoke.
func (service *MessagingService) AuthorizePeer(ctx context.Context, bare string) *Failure {
	if service == nil || service.peerResolver == nil || ctx == nil || protocol.ValidateAgentIdentity(bare) != nil || strings.Contains(bare, "/") {
		return &Failure{Code: FailureRejected}
	}
	if !service.dynamicPublicationReady() || service.topology.authorityBlocked() {
		return &Failure{Code: FailureUnavailable}
	}
	epoch := service.authorityFenceEpoch.Load()
	resolved, disposition := service.peerResolver.ResolvePeer(ctx, bare)
	return service.installResolvedPeer(ctx, bare, "", epoch, resolved, disposition, true)
}

// AuthorizeExactPeer admits the transport-authenticated full sender only
// after an exact server query. It never substitutes the server's bare-selected
// installation for the installation that actually sent the envelope.
func (service *MessagingService) AuthorizeExactPeer(ctx context.Context, full string) *Failure {
	if service == nil || service.peerResolver == nil || ctx == nil || protocol.ValidateAgentIdentity(full) != nil {
		return &Failure{Code: FailureRejected}
	}
	slash := strings.LastIndexByte(full, '/')
	if slash <= 0 || slash == len(full)-1 || strings.Contains(full[:slash], "/") || !service.dynamicPublicationReady() || service.topology.authorityBlocked() {
		return &Failure{Code: FailureUnavailable}
	}
	exact, ok := service.peerResolver.(ExactPeerResolver)
	if !ok {
		return &Failure{Code: FailureUnavailable}
	}
	epoch := service.authorityFenceEpoch.Load()
	resolved, disposition := exact.ResolveExactPeer(ctx, full)
	return service.installResolvedPeer(ctx, full[:slash], full, epoch, resolved, disposition, false)
}

func (service *MessagingService) installResolvedPeer(ctx context.Context, bare, exactFull string, epoch uint64, resolved ResolvedPeer, disposition TopologyDisposition, outbound bool) *Failure {
	if err := ctx.Err(); err != nil {
		return contextFailure(ctx)
	}
	if disposition == TopologyUnavailable {
		return &Failure{Code: FailureUnavailable}
	}
	if disposition == TopologyRejected {
		return &Failure{Code: FailureAuthorization}
	}
	if disposition != TopologyReady || resolved.Bare != bare || exactFull != "" && resolved.Full != exactFull || resolved.Full == service.identity.BoundFull() || resolved.InstallationID == "" || resolved.SessionGeneration == "" {
		return &Failure{Code: FailureRejected}
	}
	identity, err := NewIdentity(resolved.Bare, resolved.Full)
	if err != nil || !validAuthorityIdentity(identity) {
		return &Failure{Code: FailureRejected}
	}
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	if service.authorityFenceEpoch.Load() != epoch || !service.dynamicPublicationReady() || service.topology.authorityBlocked() {
		return &Failure{Code: FailureUnavailable}
	}
	if suspended, ok := service.suspendedPeers[resolved.Full]; ok {
		if suspended.Bare != resolved.Bare || suspended.InstallationID != resolved.InstallationID || suspended.SessionGeneration != resolved.SessionGeneration {
			service.retireSuspendedPeerLocked(suspended)
		}
	}
	if existing, ok := service.dynamicPeers[resolved.Full]; ok {
		if existing.InstallationID == resolved.InstallationID && existing.SessionGeneration == resolved.SessionGeneration {
			if service.peerLanes.Authorize(resolved.Full) == nil {
				if outbound {
					service.outboundBound[bare] = resolved.Full
				}
				return nil
			}
		}
		if err := service.removeDynamicPeerLocked(ctx, existing); err != nil {
			return classifyContextOr(err, ctx, FailureUnavailable)
		}
	}
	if err := service.peerLanes.AddDynamicPeer(resolved.Full); err != nil {
		return classifyPeerLaneError(err)
	}
	if err := service.topology.AddDynamicPeer(ctx, identity); err != nil {
		_ = service.peerLanes.RemoveDynamicPeer(context.Background(), resolved.Full)
		return classifyMeshError(err)
	}
	service.dynamicPeers[resolved.Full] = resolved
	delete(service.suspendedPeers, resolved.Full)
	if outbound {
		service.outboundBound[bare] = resolved.Full
	}
	service.wakeDrain()
	return nil
}

// RevokeInstallation closes only the matching exact server session. A late
// revoke for a prior generation is a no-op, never a fresh-session teardown.
func (service *MessagingService) RevokeInstallation(ctx context.Context, bare, installationID, sessionGeneration string) *Failure {
	if service == nil || service.peerResolver == nil || ctx == nil || protocol.ValidateAgentIdentity(bare) != nil || strings.Contains(bare, "/") || installationID == "" || sessionGeneration == "" {
		return &Failure{Code: FailureRejected}
	}
	service.advanceAuthorityFenceEpoch()
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	for _, current := range service.dynamicPeers {
		if current.Bare != bare || current.InstallationID != installationID || current.SessionGeneration != sessionGeneration {
			continue
		}
		if err := service.removeDynamicPeerLocked(ctx, current); err != nil {
			return classifyContextOr(err, ctx, FailureUnavailable)
		}
	}
	for _, current := range service.suspendedPeers {
		if current.Bare == bare && current.InstallationID == installationID && current.SessionGeneration == sessionGeneration {
			service.retireSuspendedPeerLocked(current)
		}
	}
	return nil
}

// RevokeLogical closes every authorized installation of one logical agent.
// Completion is the action ACK boundary: peer-owned data and transport are
// fenced and all matching lanes have joined before nil is returned.
func (service *MessagingService) RevokeLogical(ctx context.Context, bare string) *Failure {
	if service == nil || service.peerResolver == nil || ctx == nil || protocol.ValidateAgentIdentity(bare) != nil || strings.Contains(bare, "/") {
		return &Failure{Code: FailureRejected}
	}
	service.advanceAuthorityFenceEpoch()
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	for _, current := range service.dynamicPeers {
		if current.Bare != bare {
			continue
		}
		if err := service.removeDynamicPeerLocked(ctx, current); err != nil {
			return classifyContextOr(err, ctx, FailureUnavailable)
		}
	}
	for _, current := range service.suspendedPeers {
		if current.Bare == bare {
			service.retireSuspendedPeerLocked(current)
		}
	}
	return nil
}

// retireSuspendedPeerLocked is destructive only for definitive authority
// loss. DropDynamicAuthority never calls this path.
func (service *MessagingService) retireSuspendedPeerLocked(current ResolvedPeer) {
	service.fencePeerState(current.Full)
	delete(service.suspendedPeers, current.Full)
	if service.outboundBound[current.Bare] == current.Full {
		delete(service.outboundBound, current.Bare)
	}
}

func (service *MessagingService) removeDynamicPeerLocked(ctx context.Context, current ResolvedPeer) error {
	if err := service.peerLanes.RemoveDynamicPeer(ctx, current.Full); err != nil {
		return err
	}
	if err := service.topology.RemoveDynamicPeer(ctx, Identity{AgentID: current.Bare, Internal: current.Full}); err != nil {
		return err
	}
	delete(service.dynamicPeers, current.Full)
	delete(service.suspendedPeers, current.Full)
	if service.outboundBound[current.Bare] == current.Full {
		delete(service.outboundBound, current.Bare)
	}
	return nil
}

func meshFailureError(failure *Failure) error {
	if failure == nil {
		return nil
	}
	switch failure.Code {
	case FailureAuthorization:
		return ErrMembershipMissing
	case FailureUnavailable:
		return ErrSnapshotStale
	case FailureCancelled:
		return context.Canceled
	case FailureDeadline:
		return context.DeadlineExceeded
	case FailureRejected:
		return ErrInvalidIdentity
	default:
		return errors.New("mesh: peer resolution failed")
	}
}
