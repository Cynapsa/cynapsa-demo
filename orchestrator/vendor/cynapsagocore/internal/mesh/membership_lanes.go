package mesh

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// MembershipAuthoritySession wraps one exact authenticated server session in
// its separately verified mesh. It is process-local stale-callback protection, not a
// membership revision or generation.
type MembershipAuthoritySession struct{ session *peer.AuthoritySession }

// MembershipAuthoritySuspension retains one exact published member set while
// all topology and peer-lane effects are reversibly fenced.
type MembershipAuthoritySuspension struct {
	session  *MembershipAuthoritySession
	topology *TopologySuspension
}

// BeginMembershipSynchronization pauses every active lane and returns the
// only capability allowed to install the next complete server snapshot.
func (service *MessagingService) BeginMembershipSynchronization() (*MembershipAuthoritySession, *Failure) {
	if service == nil || service.peerLanes == nil {
		return nil, &Failure{Code: FailureInternal}
	}
	session, err := service.peerLanes.BeginSession()
	if err != nil {
		return nil, classifyPeerLaneError(err)
	}
	wrapped := &MembershipAuthoritySession{session: session}
	return wrapped, nil
}

// InstallCurrentMembership reconciles directly against one complete current
// snapshot. It excludes the authenticated local identity from remote lanes.
func (service *MessagingService) InstallCurrentMembership(session *MembershipAuthoritySession, members []Identity) *Failure {
	if service == nil || service.peerLanes == nil || session == nil || session.session == nil || len(members) > MaxMembershipEntries {
		return &Failure{Code: FailureRejected}
	}
	peers := make([]string, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	localFound := false
	for _, member := range members {
		identity, err := NewIdentity(member.AgentID, member.Internal)
		if err != nil || !validAuthorityIdentity(identity) {
			return &Failure{Code: FailureRejected}
		}
		if _, duplicate := seen[identity.Internal]; duplicate {
			return &Failure{Code: FailureRejected}
		}
		seen[identity.Internal] = struct{}{}
		if identity.Internal == service.identity.BoundFull() {
			localFound = identity.AgentID == service.identity.AgentID()
			continue
		}
		peers = append(peers, identity.Internal)
	}
	if !localFound {
		return &Failure{Code: FailureRejected}
	}
	if err := service.peerLanes.InstallSnapshot(session.session, peers); err != nil {
		return classifyPeerLaneError(err)
	}
	return nil
}

// PublishCurrentMembership resumes peer lanes only for the exact session that
// installed the current complete snapshot. A newer pause or synchronization
// retires the capability and fails closed.
func (service *MessagingService) PublishCurrentMembership(session *MembershipAuthoritySession) *Failure {
	if service == nil || service.peerLanes == nil || session == nil || session.session == nil {
		return &Failure{Code: FailureRejected}
	}
	if err := service.peerLanes.Publish(session.session); err != nil {
		return classifyPeerLaneError(err)
	}
	// A drain attempt can observe the authority fence before it reaches lane
	// admission, release its reservation, and return without becoming queued on
	// a lane. Publication therefore has to wake the shared outbox explicitly;
	// resuming lanes alone cannot reach that pre-admission attempt.
	service.wakeDrain()
	return nil
}

func (service *MessagingService) PauseMembershipAuthority(session *MembershipAuthoritySession) *Failure {
	if service == nil || service.peerLanes == nil || session == nil || session.session == nil {
		return &Failure{Code: FailureRejected}
	}
	if err := service.peerLanes.Pause(session.session); err != nil {
		return classifyPeerLaneError(err)
	}
	return nil
}

func (service *MessagingService) SuspendMembershipAuthority(session *MembershipAuthoritySession) (*MembershipAuthoritySuspension, *Failure) {
	if service == nil || service.peerLanes == nil || service.topology == nil || session == nil || session.session == nil {
		return nil, &Failure{Code: FailureRejected}
	}
	service.advanceAuthorityFenceEpoch()
	topology := service.topology.SuspendAdmission()
	if topology == nil {
		return nil, &Failure{Code: FailureUnavailable}
	}
	if err := service.peerLanes.Suspend(session.session); err != nil {
		service.topology.BlockAdmission()
		return nil, classifyPeerLaneError(err)
	}
	return &MembershipAuthoritySuspension{session: session, topology: topology}, nil
}

func (service *MessagingService) ResumeMembershipAuthority(ctx context.Context, suspension *MembershipAuthoritySuspension) *Failure {
	if service == nil || service.peerLanes == nil || service.topology == nil || ctx == nil || suspension == nil || suspension.session == nil || suspension.session.session == nil || suspension.topology == nil {
		return &Failure{Code: FailureRejected}
	}
	if err := service.topology.ResumeAdmission(ctx, suspension.topology); err != nil {
		return &Failure{Code: FailureUnavailable}
	}
	if err := service.peerLanes.Resume(suspension.session.session); err != nil {
		service.topology.BlockAdmission()
		return classifyPeerLaneError(err)
	}
	service.wakeDrain()
	return nil
}

// AuthorizeCurrentPeer performs the immediate current-snapshot check used by
// authenticated transports before establishment and publication. It carries
// no revision, generation, or historical membership evidence.
func (service *MessagingService) AuthorizeCurrentPeer(peerID string) *Failure {
	if service == nil || service.peerLanes == nil {
		return &Failure{Code: FailureInternal}
	}
	if err := service.peerLanes.Authorize(peerID); err != nil {
		return classifyPeerLaneError(err)
	}
	return nil
}

// AdmitAuthenticatedPeerWork and AdmitOutboundPeerWork are the concrete
// carrier/service seams: every carrier submits to the same peer lane. The
// operation callback must use its Commit capability for SDK publication,
// carrier admission, or successful completion.
func (service *MessagingService) AdmitAuthenticatedPeerWork(ctx context.Context, peerID string, work peer.Work) (peer.Result, *Failure) {
	if service == nil || service.peerLanes == nil {
		return peer.Result{}, &Failure{Code: FailureInternal}
	}
	result, err := service.peerLanes.AdmitAuthenticatedInbound(ctx, peerID, work)
	if err != nil {
		return peer.Result{}, classifyPeerLaneError(err)
	}
	return result, nil
}

func (service *MessagingService) AdmitOutboundPeerWork(ctx context.Context, peerID string, work peer.Work) (peer.Result, *Failure) {
	if service == nil || service.peerLanes == nil {
		return peer.Result{}, &Failure{Code: FailureInternal}
	}
	result, err := service.peerLanes.AdmitOutbound(ctx, peerID, work)
	if err != nil {
		return peer.Result{}, classifyPeerLaneError(err)
	}
	return result, nil
}

func (service *MessagingService) AdmitOutboundPeerWorkEpoch(ctx context.Context, peerID string, epoch uint64, work peer.Work) (peer.Result, *Failure) {
	if service == nil || service.peerLanes == nil || epoch == 0 {
		return peer.Result{}, &Failure{Code: FailureInternal}
	}
	result, err := service.peerLanes.AdmitOutboundEpoch(ctx, peerID, epoch, work)
	if err != nil {
		return peer.Result{}, classifyPeerLaneError(err)
	}
	return result, nil
}

func classifyPeerLaneError(err error) *Failure {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, peer.ErrUnauthorized):
		return &Failure{Code: FailureAuthorization}
	case errors.Is(err, peer.ErrQueueFull):
		return &Failure{Code: FailureCapacity}
	case errors.Is(err, peer.ErrEpochExhausted):
		return &Failure{Code: FailureUnavailable}
	case errors.Is(err, context.Canceled):
		return &Failure{Code: FailureCancelled}
	case errors.Is(err, context.DeadlineExceeded):
		return &Failure{Code: FailureDeadline}
	case errors.Is(err, peer.ErrUnavailable), errors.Is(err, peer.ErrClosed):
		return &Failure{Code: FailureUnavailable}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func envelopeOwnedBytes(envelope protocol.Envelope) uint64 {
	return uint64(len(envelope.MessageID) + len(envelope.ConversationID) + len(envelope.CorrelationID) +
		len(envelope.ReplyTo) + len(envelope.Sender) + len(envelope.Recipient) + len(envelope.MeshID) +
		len(envelope.Payload.Profile) + len(envelope.Payload.Reference) + len(envelope.Payload.EncryptionRef) +
		len(envelope.Payload.Inline) + len(envelope.CredentialProof))
}

func clearOwnedEnvelope(envelope *protocol.Envelope) {
	if envelope == nil {
		return
	}
	zeroBytes(envelope.Payload.Inline)
	zeroBytes(envelope.CredentialProof)
	*envelope = protocol.Envelope{}
}

// fencePeerState is invoked after the lane's external authority fence and
// before any present lane resumes. It performs no carrier I/O.
func (service *MessagingService) fencePeerState(peerID string) {
	if service == nil || peerID == "" {
		return
	}
	meshID := service.identity.MeshID()
	failedCorrelations := service.outboundRPC.FailPeerCorrelations(meshID, peerID)
	service.inboundRPC.CancelPeer(meshID, peerID)
	service.outbox.RetirePeerOwned(meshID, peerID)
	service.deduper.RetirePeer(meshID, peerID)
	if payloads, ok := service.pipeline.(PeerPayloadAuthority); ok {
		payloads.RetirePeer(meshID, peerID)
	}

	var payloads []retainedPayload
	var receipts []InboundRank1Receipt
	service.mu.Lock()
	for _, correlation := range failedCorrelations {
		if retained, ok := service.responseValues[correlation]; ok {
			payloads = append(payloads, retained)
			delete(service.responseValues, correlation)
		}
	}
	for messageID, retained := range service.inboundValues {
		if retained.peerID == peerID {
			payloads = append(payloads, retained)
			delete(service.inboundValues, messageID)
		}
	}
	for _, publication := range service.publications {
		if publication.request.PeerID == peerID && !publication.published {
			publication.failure = Failure{Code: FailureAuthorization}
			publication.failed = true
		}
	}
	for conversationID, state := range service.conversations {
		if state.peerInternal == peerID {
			state.closed = true
			if state.active == 0 {
				delete(service.conversations, conversationID)
			}
		}
	}
	for messageID, retained := range service.inboundReceipts {
		if retained.binding.MeshID == meshID && retained.binding.Sender == peerID {
			receipts = append(receipts, retained.receipt)
			delete(service.inboundReceipts, messageID)
		}
	}
	service.mu.Unlock()
	for _, retained := range payloads {
		zeroModelPayload(retained.value)
		service.rpcBudget.ReleaseOwned(retained.bytes)
	}
	for _, receipt := range receipts {
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
	}
}

func (service *MessagingService) allowPeerState(peerID string) {
	if service == nil || peerID == "" {
		return
	}
	if payloads, ok := service.pipeline.(PeerPayloadAuthority); ok {
		payloads.AllowPeer(service.identity.MeshID(), peerID)
	}
}

func (service *MessagingService) capturePeerEpoch(peerID string) (uint64, bool) {
	if service == nil || service.peerLanes == nil || peerID == "" {
		return 0, false
	}
	return service.peerLanes.PeerEpoch(peerID)
}

func (service *MessagingService) peerEpochMatches(peerID string, epoch uint64) bool {
	if service == nil || peerID == "" || epoch == 0 {
		return false
	}
	current, ok := service.capturePeerEpoch(peerID)
	return ok && current == epoch
}

// closePeerTransport is called exactly once by the terminal lane owner after
// its queues are scrubbed. The deadline prevents a dependency from blocking
// lane join forever; transport quarantine remains owned by its manager.
func (service *MessagingService) closePeerTransport(peerID string) {
	closer, ok := service.carrier.(PeerClosingCarrier)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), min(service.config.OperationTimeout, maximumOperationTimeout))
	defer cancel()
	_ = closer.ClosePeer(ctx, peerID)
}
