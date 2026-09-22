package mesh

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"time"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

var errOutboundPeerWorkFailed = errors.New("mesh: outbound peer work failed")

type outboundPayloadLaneContextKey struct{}

type outboundPayloadLaneContext struct {
	service    *MessagingService
	peerID     string
	checkpoint peer.AuthorityCheckpoint
}

func (service *MessagingService) MessageSend(ctx context.Context, args model.MessageSendArgs) (model.SendResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.SendResult{}, operationFailure
	}
	defer release()
	ctx = operation
	prepared, targets, aggregateFailure := service.prepareOutboundEndpoints(ctx, args.To, args.Payload)
	if len(targets) == 0 {
		if aggregateFailure == nil {
			aggregateFailure = &Failure{Code: FailureRejected}
		}
		return model.SendResult{}, aggregateFailure
	}
	if len(prepared.Canonical) == 0 {
		return model.SendResult{}, &Failure{Code: FailureInternal}
	}
	defer zeroBytes(prepared.Canonical)
	requests, allocationFailure := service.allocateMessageSendBatch(targets, prepared)
	if allocationFailure != nil {
		return model.SendResult{}, allocationFailure
	}
	for _, request := range requests {
		if sendFailure := service.sendPrepared(ctx, request); sendFailure != nil {
			if aggregateFailure == nil {
				aggregateFailure = sendFailure
			}
		}
	}
	if aggregateFailure != nil {
		return model.SendResult{}, aggregateFailure
	}
	// The public V1 result remains one logical acceptance receipt. Exact
	// per-endpoint identifiers stay private; the first endpoint is stable because
	// topology sorts the complete authenticated endpoint set.
	first := requests[0]
	return model.SendResult{MessageID: first.MessageID, ConversationID: first.ConversationID, Accepted: true}, nil
}

// allocateMessageSendBatch reserves every endpoint operation atomically. A
// concurrent command can consume either the capacity before this batch or the
// capacity after it, but can never cause a partially reserved replica batch.
func (service *MessagingService) allocateMessageSendBatch(targets []outboundEndpoint, payload PreparedPayload) ([]PreparedSend, *Failure) {
	if service == nil || len(targets) == 0 {
		return nil, &Failure{Code: FailureRejected}
	}
	reading, failure := service.calibratedTime()
	if failure != nil {
		return nil, failure
	}
	messageIDs := make([]string, len(targets))
	for index := range targets {
		messageID, err := protocol.NewMessageID()
		if err != nil {
			return nil, &Failure{Code: FailureInternal}
		}
		messageIDs[index] = messageID
	}
	service.mu.Lock()
	if service.activeOperations+len(targets) > service.config.QueueCapacity {
		service.mu.Unlock()
		return nil, &Failure{Code: FailureCapacity}
	}
	for _, target := range targets {
		if target.state == nil {
			service.mu.Unlock()
			return nil, &Failure{Code: FailureRejected}
		}
		if !service.peerEpochMatches(target.peer.Internal, target.state.peerEpoch) {
			service.mu.Unlock()
			return nil, &Failure{Code: FailureAuthorization}
		}
		if target.state.closed || target.state.outboundFail {
			service.mu.Unlock()
			return nil, &Failure{Code: FailureRejected}
		}
	}
	for _, target := range targets {
		target.state.active++
	}
	service.activeOperations += len(targets)
	service.mu.Unlock()

	createdAt := reading.UTC.Truncate(time.Millisecond)
	requests := make([]PreparedSend, len(targets))
	for index, target := range targets {
		requests[index] = PreparedSend{
			PeerID: target.peer.Internal, MessageID: messageIDs[index], MeshID: service.identity.MeshID(),
			SenderID: service.identity.BoundFull(), RecipientID: target.peer.Internal, ConversationID: target.state.id,
			Mode: protocol.ModeMessage, CreatedAt: createdAt, ClockUncertainty: reading.Uncertainty, Payload: payload.clone(),
			conversation: target.state, peerEpoch: target.state.peerEpoch,
		}
	}
	return requests, nil
}

func (service *MessagingService) MessageRequest(ctx context.Context, args model.MessageRequestArgs) (model.ResponseResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.ResponseResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.ResponseResult{}, failure
	}
	reading, failure := service.calibratedTime()
	if failure != nil {
		return model.ResponseResult{}, failure
	}
	startedAt := reading.UTC.Truncate(time.Millisecond)
	ttl := args.TTL
	if ttl == 0 {
		ttl = service.config.RPCTimeout.SnapshotRPCTimeout()
	}
	expiresAt, validTTL := rpcExpiresAt(startedAt, ttl)
	if !validTTL {
		return model.ResponseResult{}, &Failure{Code: FailureRejected}
	}
	// The caller-configured RPC lifetime starts before peer-lane admission.
	// Membership synchronization may pause the destination lane, but it must
	// not pause or extend the SDK's requested timeout.
	rpcCtx, cancelRPC := context.WithTimeout(ctx, ttl)
	defer cancelRPC()
	prepared, peer, state, failure := service.prepareOutbound(rpcCtx, args.To, args.Payload)
	if failure != nil {
		return model.ResponseResult{}, failure
	}
	defer zeroBytes(prepared.Canonical)
	request, failure := service.allocateSend(state, peer, protocol.ModeRequest, "", "", startedAt, expiresAt, reading.Uncertainty, prepared)
	if failure != nil {
		return model.ResponseResult{}, failure
	}
	if failure := service.sendPrepared(rpcCtx, request); failure != nil {
		service.failAndConsumeOutboundRequest(request.CorrelationID)
		if errors.Is(rpcCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return model.ResponseResult{}, &Failure{Code: FailureDeadline}
		}
		return model.ResponseResult{}, failure
	}
	response, err := service.outboundRPC.Wait(rpcCtx, request.CorrelationID)
	if errors.Is(err, rpc.ErrCancelled) && errors.Is(rpcCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		err = rpc.ErrExpired
	}
	retained, retainedOK := service.takeResponseValue(request.CorrelationID)
	if !retainedOK && response.MessageID != "" {
		// Compatibility cleanup for any value admitted by an older in-process
		// producer before correlation-key ownership was established.
		retained, retainedOK = service.takeResponseValue(response.MessageID)
	}
	defer func() { zeroModelPayload(retained.value) }()
	if err != nil {
		zeroModelPayload(retained.value)
		return model.ResponseResult{}, classifyRPCError(err)
	}
	defer zeroBytes(response.Payload.Inline)
	defer zeroBytes(response.CredentialProof)
	admission, admissionFailure := service.acquireTopologyAdmission(ctx, response)
	if admissionFailure != nil {
		return model.ResponseResult{}, admissionFailure
	}
	admission.release()
	if !retainedOK {
		zeroModelPayload(retained.value)
		return model.ResponseResult{}, &Failure{Code: FailureInternal}
	}
	resultPayload := retained.value
	retained.value = model.Payload{}
	return model.ResponseResult{
		MessageID: response.MessageID, ConversationID: response.ConversationID,
		FromAgentID: peer.AgentID, MeshID: response.MeshID, Payload: resultPayload,
	}, nil
}

func rpcExpiresAt(startedAt time.Time, ttl time.Duration) (time.Time, bool) {
	if ttl <= 0 {
		return time.Time{}, false
	}
	expiresAt := startedAt.Add(ttl).UTC().Truncate(time.Millisecond)
	if !protocol.ValidRequestTimeWindow(startedAt, expiresAt) {
		return time.Time{}, false
	}
	return expiresAt, true
}

// failAndConsumeOutboundRequest retires registration ownership on every send
// failure. Fail makes a live entry immediately readable; ErrCompleted means a
// response won the same terminal race, whose sole owned envelope must still be
// consumed and scrubbed here because the ordinary Wait path will not run.
func (service *MessagingService) failAndConsumeOutboundRequest(correlationID string) {
	if service == nil || correlationID == "" {
		return
	}
	_ = service.outboundRPC.Fail(correlationID, rpc.ErrCancelled)
	response, _ := service.outboundRPC.Wait(context.Background(), correlationID)
	zeroBytes(response.Payload.Inline)
	zeroBytes(response.CredentialProof)
	retained, retainedOK := service.takeResponseValue(correlationID)
	if !retainedOK && response.MessageID != "" {
		retained, _ = service.takeResponseValue(response.MessageID)
	}
	zeroModelPayload(retained.value)
}

func (service *MessagingService) MessageReply(ctx context.Context, args model.MessageReplyArgs) (model.SendResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.SendResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.SendResult{}, failure
	}
	original, lease, err := service.inboundRPC.BeginReply(args.RequestHandle)
	if err != nil {
		return model.SendResult{}, classifyRPCError(err)
	}
	defer func() {
		zeroBytes(original.Payload.Inline)
		zeroBytes(original.CredentialProof)
		original = protocol.Envelope{}
		_ = service.inboundRPC.FinalizeReply(lease)
	}()
	peer, err := service.replyPeerIdentity(original)
	if err != nil {
		_ = service.inboundRPC.ReleaseReply(lease)
		return model.SendResult{}, &Failure{Code: FailureInvalidHandle}
	}
	peerEpoch, ok := service.capturePeerEpoch(peer.Internal)
	if !ok {
		_ = service.inboundRPC.ReleaseReply(lease)
		return model.SendResult{}, &Failure{Code: FailureAuthorization}
	}
	prepared, disposition := service.pipeline.Prepare(args.Payload)
	if disposition != PayloadAccepted {
		zeroBytes(prepared.Canonical)
		_ = service.inboundRPC.ReleaseReply(lease)
		return model.SendResult{}, payloadFailure(disposition)
	}
	defer zeroBytes(prepared.Canonical)
	result, failure := service.sendReplyPrepared(ctx, peer, peerEpoch, original, prepared)
	if failure != nil {
		_ = service.inboundRPC.ReleaseReply(lease)
		return model.SendResult{}, failure
	}
	if err := service.inboundRPC.CommitReply(lease); err != nil {
		return model.SendResult{}, classifyRPCError(err)
	}
	return result, nil
}

// replyPeerIdentity derives the only response destination represented by an
// already-authenticated inbound request handle. The handle's private envelope
// was admitted through the peer lane and is sufficient to construct a reply;
// consulting session-bound topology here would reject valid work during a
// transient authority pause before the work can move into that same lane.
// Current membership is still enforced by lane admission, its final Commit,
// and publishEnvelope's topology admission.
func (service *MessagingService) replyPeerIdentity(original protocol.Envelope) (Identity, error) {
	if service == nil || original.Mode != protocol.ModeRequest || original.MeshID != service.identity.MeshID() || original.Recipient != service.identity.BoundFull() {
		return Identity{}, ErrIdentityUnknown
	}
	slash := strings.LastIndexByte(original.Sender, '/')
	if slash <= 0 || slash == len(original.Sender)-1 {
		return Identity{}, ErrIdentityUnknown
	}
	agentID := original.Sender[:slash]
	peer, err := NewIdentity(agentID, original.Sender)
	if err != nil {
		return Identity{}, err
	}
	conversationID, err := conversation.DeriveID(original.MeshID, service.identity.BoundFull(), peer.Internal)
	if err != nil || conversationID != original.ConversationID {
		return Identity{}, ErrIdentityUnknown
	}
	return peer, nil
}

func (service *MessagingService) prepareOutbound(ctx context.Context, recipient string, payload model.Payload) (PreparedPayload, Identity, *conversationState, *Failure) {
	if failure := service.ensureActive(ctx); failure != nil {
		return PreparedPayload{}, Identity{}, nil, failure
	}
	peer, err := service.resolveOutboundPeer(ctx, recipient)
	if err != nil {
		return PreparedPayload{}, Identity{}, nil, classifyMeshError(err)
	}
	prepared, disposition := service.pipeline.Prepare(payload)
	if disposition != PayloadAccepted {
		zeroBytes(prepared.Canonical)
		return PreparedPayload{}, Identity{}, nil, payloadFailure(disposition)
	}
	defer zeroBytes(prepared.Canonical)
	if failure := service.validatePrepared(prepared); failure != nil {
		return PreparedPayload{}, Identity{}, nil, failure
	}
	if !service.policies.Allows(peer.AgentID, prepared.ApplicationPath) {
		return PreparedPayload{}, Identity{}, nil, &Failure{Code: FailureAuthorization}
	}
	stateAdmission, admissionFailure := service.acquireParticipantAdmission(ctx, service.identity.MeshID(), service.identity.BoundFull(), peer.Internal)
	if admissionFailure != nil {
		return PreparedPayload{}, Identity{}, nil, admissionFailure
	}
	peerEpoch, ok := service.capturePeerEpoch(peer.Internal)
	if !ok {
		stateAdmission.release()
		return PreparedPayload{}, Identity{}, nil, &Failure{Code: FailureAuthorization}
	}
	state, failure := service.conversationForOutbound(peer, peerEpoch)
	stateAdmission.release()
	owned := prepared.clone()
	return owned, peer, state, failure
}

type outboundEndpoint struct {
	peer  Identity
	state *conversationState
}

// prepareOutboundEndpoints resolves one logical recipient into the complete
// bounded set of exact endpoints in the current authenticated mesh snapshot.
// It retains the public bare-agent contract while ensuring resource text is
// never interpreted as mesh authority.
func (service *MessagingService) prepareOutboundEndpoints(ctx context.Context, recipient string, payload model.Payload) (PreparedPayload, []outboundEndpoint, *Failure) {
	if failure := service.ensureActive(ctx); failure != nil {
		return PreparedPayload{}, nil, failure
	}
	peers, err := service.resolveOutboundEndpoints(ctx, recipient)
	if err != nil {
		return PreparedPayload{}, nil, classifyMeshError(err)
	}
	prepared, disposition := service.pipeline.Prepare(payload)
	if disposition != PayloadAccepted {
		zeroBytes(prepared.Canonical)
		return PreparedPayload{}, nil, payloadFailure(disposition)
	}
	defer zeroBytes(prepared.Canonical)
	if failure := service.validatePrepared(prepared); failure != nil {
		return PreparedPayload{}, nil, failure
	}
	if !service.policies.Allows(recipient, prepared.ApplicationPath) {
		return PreparedPayload{}, nil, &Failure{Code: FailureAuthorization}
	}
	targets := make([]outboundEndpoint, 0, len(peers))
	var aggregateFailure *Failure
	for _, peer := range peers {
		peerEpoch, ok := service.capturePeerEpoch(peer.Internal)
		if !ok {
			if aggregateFailure == nil {
				aggregateFailure = &Failure{Code: FailureAuthorization}
			}
			continue
		}
		state, failure := service.conversationForOutbound(peer, peerEpoch)
		if failure != nil {
			if aggregateFailure == nil {
				aggregateFailure = failure
			}
			continue
		}
		targets = append(targets, outboundEndpoint{peer: peer, state: state})
	}
	if len(targets) == 0 {
		if aggregateFailure == nil {
			aggregateFailure = &Failure{Code: FailureRejected}
		}
		return PreparedPayload{}, nil, aggregateFailure
	}
	participants := make([]string, 1, len(targets)+1)
	participants[0] = service.identity.BoundFull()
	for _, target := range targets {
		participants = append(participants, target.peer.Internal)
	}
	admission, admissionFailure := service.acquireParticipantAdmission(ctx, service.identity.MeshID(), participants...)
	if admissionFailure != nil {
		return PreparedPayload{}, nil, admissionFailure
	}
	defer admission.release()
	return prepared.clone(), targets, aggregateFailure
}

func (service *MessagingService) conversationForOutbound(peer Identity, peerEpoch uint64) (*conversationState, *Failure) {
	id, err := conversation.DeriveID(service.identity.MeshID(), service.identity.BoundFull(), peer.Internal)
	if err != nil {
		return nil, &Failure{Code: FailureRejected}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if !service.peerEpochMatches(peer.Internal, peerEpoch) {
		return nil, &Failure{Code: FailureAuthorization}
	}
	if existing := service.conversations[id]; existing != nil {
		if existing.peerEpoch != peerEpoch {
			return nil, &Failure{Code: FailureAuthorization}
		}
		if existing.closed || existing.outboundFail || existing.peerAgent != peer.AgentID || existing.peerInternal != peer.Internal {
			return nil, &Failure{Code: FailureRejected}
		}
		return existing, nil
	}
	if len(service.conversations) >= service.config.QueueCapacity {
		return nil, &Failure{Code: FailureCapacity}
	}
	state := &conversationState{id: id, peerAgent: peer.AgentID, peerInternal: peer.Internal, peerEpoch: peerEpoch}
	service.conversations[id] = state
	return state, nil
}

func (service *MessagingService) allocateSend(state *conversationState, peer Identity, mode protocol.Mode, correlationID, replyTo string, requestStart, expiresAt time.Time, clockUncertainty time.Duration, payload PreparedPayload) (PreparedSend, *Failure) {
	reading, failure := service.calibratedTime()
	if failure != nil {
		return PreparedSend{}, failure
	}
	if clockUncertainty == 0 {
		clockUncertainty = reading.Uncertainty
	}
	service.mu.Lock()
	if state == nil {
		service.mu.Unlock()
		return PreparedSend{}, &Failure{Code: FailureRejected}
	}
	if !service.peerEpochMatches(peer.Internal, state.peerEpoch) {
		service.mu.Unlock()
		return PreparedSend{}, &Failure{Code: FailureAuthorization}
	}
	if state.closed || state.outboundFail {
		service.mu.Unlock()
		return PreparedSend{}, &Failure{Code: FailureRejected}
	}
	if service.activeOperations >= service.config.QueueCapacity {
		service.mu.Unlock()
		return PreparedSend{}, &Failure{Code: FailureCapacity}
	}
	state.active++
	service.activeOperations++
	service.mu.Unlock()
	messageID, err := protocol.NewMessageID()
	if err != nil {
		service.finishOutbound(state, true)
		return PreparedSend{}, &Failure{Code: FailureInternal}
	}
	if mode == protocol.ModeRequest && correlationID == "" {
		correlationID, err = protocol.NewCorrelationID()
		if err != nil {
			service.finishOutbound(state, true)
			return PreparedSend{}, &Failure{Code: FailureInternal}
		}
	}
	createdAt := reading.UTC.Truncate(time.Millisecond)
	if mode == protocol.ModeRequest {
		createdAt = requestStart
		if createdAt.IsZero() || !expiresAt.After(createdAt) {
			service.finishOutbound(state, true)
			return PreparedSend{}, &Failure{Code: FailureRejected}
		}
	} else if !expiresAt.IsZero() || !requestStart.IsZero() {
		service.finishOutbound(state, true)
		return PreparedSend{}, &Failure{Code: FailureRejected}
	}
	request := PreparedSend{
		PeerID: peer.Internal, MessageID: messageID, MeshID: service.identity.MeshID(),
		SenderID: service.identity.BoundFull(), RecipientID: peer.Internal, ConversationID: state.id,
		Mode: mode, CorrelationID: correlationID, ReplyTo: replyTo,
		CreatedAt: createdAt, ExpiresAt: expiresAt, ClockUncertainty: clockUncertainty, Payload: payload.clone(),
		conversation: state, peerEpoch: state.peerEpoch,
	}
	return request, nil
}

func (service *MessagingService) sendPrepared(ctx context.Context, request PreparedSend) *Failure {
	defer zeroBytes(request.Payload.Canonical)
	failed := true
	defer func() { service.finishOutbound(request.conversation, failed) }()
	work := &outboundPayloadLaneWork{service: service, request: request.clone()}
	result, admissionFailure := service.AdmitOutboundPeerWorkEpoch(ctx, work.request.PeerID, work.request.peerEpoch, peer.Work{
		Kind:       peer.WorkOutbound,
		OwnedBytes: preparedSendOwnedBytes(work.request),
		Run:        work.run,
		Clear:      work.clear,
	})
	if admissionFailure != nil {
		work.clear()
		if admissionFailure.Code == FailureRejected || admissionFailure.Code == FailureAuthorization {
			service.retireOutboundState(request.conversation)
		}
		return admissionFailure
	}
	if err := result.Wait(ctx); err != nil {
		if ctx.Err() != nil {
			return contextFailure(ctx)
		}
		if errors.Is(err, peer.ErrUnauthorized) {
			service.retireOutboundState(request.conversation)
		}
		if errors.Is(err, errOutboundPeerWorkFailed) && work.failure != nil {
			return &Failure{Code: work.failure.Code}
		}
		return classifyPeerLaneError(err)
	}
	failed = false
	return nil
}

func (service *MessagingService) sendReplyPrepared(ctx context.Context, peerIdentity Identity, peerEpoch uint64, original protocol.Envelope, prepared PreparedPayload) (model.SendResult, *Failure) {
	work := &outboundReplyLaneWork{
		service:   service,
		peer:      peerIdentity,
		peerEpoch: peerEpoch,
		original:  original,
		prepared:  prepared.clone(),
	}
	result, admissionFailure := service.AdmitOutboundPeerWorkEpoch(ctx, peerIdentity.Internal, peerEpoch, peer.Work{
		Kind:       peer.WorkOutbound,
		OwnedBytes: replyLaneOwnedBytes(peerIdentity, original, prepared),
		Run:        work.run,
		Clear:      work.clear,
	})
	if admissionFailure != nil {
		work.clear()
		return model.SendResult{}, admissionFailure
	}
	if err := result.Wait(ctx); err != nil {
		if ctx.Err() != nil {
			return model.SendResult{}, contextFailure(ctx)
		}
		if errors.Is(err, errOutboundPeerWorkFailed) && work.failure != nil {
			return model.SendResult{}, &Failure{Code: work.failure.Code}
		}
		return model.SendResult{}, classifyPeerLaneError(err)
	}
	return work.result, nil
}

type outboundPayloadLaneWork struct {
	service      *MessagingService
	request      PreparedSend
	publication  *publicationState
	pipelineDone bool
	failure      *Failure
}

type outboundReplyLaneWork struct {
	service      *MessagingService
	peer         Identity
	peerEpoch    uint64
	original     protocol.Envelope
	prepared     PreparedPayload
	request      PreparedSend
	publication  *publicationState
	pipelineDone bool
	success      bool
	failure      *Failure
	result       model.SendResult
}

// run keeps a completed pipeline result lane-owned when a transient Pause
// wins only the final success boundary. Resume retries Commit, not carrier I/O.
func (work *outboundPayloadLaneWork) run(ctx context.Context, commit peer.Commit) error {
	if work == nil || work.service == nil {
		return errOutboundPeerWorkFailed
	}
	if !work.pipelineDone {
		checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx)
		if !ok {
			return errOutboundPeerWorkFailed
		}
		// A registry-wide authority fence can race with mailbox selection while
		// the lane itself is still Active. Wait at the lane boundary before the
		// payload pipeline can consult topology or create publication state.
		// Present peers resume this exact Work; removed peers scrub it without an
		// external effect.
		if err := checkpoint(ctx, func() error { return nil }); err != nil {
			return err
		}
		work.failure = work.runPipeline(ctx)
		work.pipelineDone = true
	}
	if work.failure != nil {
		return errOutboundPeerWorkFailed
	}
	if err := commit(func() error { return nil }); err != nil {
		// Lane.run recognizes this exact transient result and retains the Work.
		// An absent peer instead returns ErrUnauthorized and is destroyed.
		return err
	}
	return nil
}

func (work *outboundPayloadLaneWork) runPipeline(ctx context.Context) *Failure {
	service, request := work.service, work.request
	checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx)
	if !ok {
		return &Failure{Code: FailureInternal}
	}
	service.mu.Lock()
	if !service.peerEpochMatches(request.PeerID, request.peerEpoch) {
		service.mu.Unlock()
		return &Failure{Code: FailureAuthorization}
	}
	if _, exists := service.publications[request.MessageID]; exists {
		service.mu.Unlock()
		return &Failure{Code: FailureInternal}
	}
	state := &publicationState{request: request}
	work.publication = state
	service.publications[request.MessageID] = state
	service.mu.Unlock()

	operation, cancel := service.operationContext(ctx)
	operation = context.WithValue(operation, outboundPayloadLaneContextKey{}, outboundPayloadLaneContext{
		service: service, peerID: request.PeerID, checkpoint: checkpoint,
	})
	disposition := service.pipeline.Send(operation, request)
	cancel()
	service.mu.Lock()
	closed := service.closed
	failed, failure, published := state.failed, state.failure, state.published
	service.mu.Unlock()
	if closed {
		return &Failure{Code: FailureCancelled}
	}
	if failed {
		return &Failure{Code: failure.Code}
	}
	if disposition != PayloadAccepted {
		return payloadFailure(disposition)
	}
	if !published {
		return &Failure{Code: FailureInternal}
	}
	return nil
}

func (work *outboundPayloadLaneWork) clear() {
	if work == nil {
		return
	}
	if work.service != nil && work.publication != nil {
		work.service.mu.Lock()
		if work.service.publications[work.request.MessageID] == work.publication {
			delete(work.service.publications, work.request.MessageID)
		}
		work.service.mu.Unlock()
	}
	zeroBytes(work.request.Payload.Canonical)
	work.request = PreparedSend{}
	work.publication = nil
}

func (work *outboundReplyLaneWork) run(ctx context.Context, commit peer.Commit) error {
	if work == nil || work.service == nil {
		return errOutboundPeerWorkFailed
	}
	if work.request.MessageID == "" {
		checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx)
		if !ok {
			return errOutboundPeerWorkFailed
		}
		if err := checkpoint(ctx, func() error { return nil }); err != nil {
			return err
		}
		state, failure := work.service.conversationForOutbound(work.peer, work.peerEpoch)
		if failure != nil {
			work.failure = failure
			return errOutboundPeerWorkFailed
		}
		request, failure := work.service.allocateSend(state, work.peer, protocol.ModeResponse, work.original.CorrelationID, work.original.MessageID, time.Time{}, time.Time{}, 0, work.prepared)
		if failure != nil {
			work.failure = failure
			return errOutboundPeerWorkFailed
		}
		work.request = request
	}
	if !work.pipelineDone {
		payloadWork := outboundPayloadLaneWork{service: work.service, request: work.request}
		work.failure = payloadWork.runPipeline(ctx)
		work.publication = payloadWork.publication
		work.pipelineDone = true
	}
	if work.failure != nil {
		return errOutboundPeerWorkFailed
	}
	if err := commit(func() error { return nil }); err != nil {
		if errors.Is(err, peer.ErrUnauthorized) {
			work.service.retireOutboundState(work.request.conversation)
		}
		return err
	}
	work.success = true
	work.result = model.SendResult{MessageID: work.request.MessageID, ConversationID: work.request.ConversationID, Accepted: true}
	return nil
}

func (work *outboundReplyLaneWork) clear() {
	if work == nil {
		return
	}
	if work.service != nil && work.publication != nil {
		work.service.mu.Lock()
		if work.service.publications[work.request.MessageID] == work.publication {
			delete(work.service.publications, work.request.MessageID)
		}
		work.service.mu.Unlock()
	}
	if work.service != nil && work.request.conversation != nil {
		work.service.finishOutbound(work.request.conversation, !work.success)
	}
	zeroBytes(work.original.Payload.Inline)
	zeroBytes(work.original.CredentialProof)
	zeroBytes(work.prepared.Canonical)
	zeroBytes(work.request.Payload.Canonical)
	work.publication = nil
	work.original = protocol.Envelope{}
	work.prepared = PreparedPayload{}
	work.request = PreparedSend{}
}

func (service *MessagingService) finishOutbound(state *conversationState, failed bool) {
	if service == nil || state == nil {
		return
	}
	service.mu.Lock()
	if state.active > 0 {
		state.active--
		if service.activeOperations > 0 {
			service.activeOperations--
		}
	}
	if failed {
		state.outboundFail = true
	}
	if state.closed && state.active == 0 && service.conversations[state.id] == state {
		delete(service.conversations, state.id)
	}
	service.mu.Unlock()
}

func (service *MessagingService) retireOutboundState(state *conversationState) {
	if service == nil || state == nil {
		return
	}
	service.mu.Lock()
	if service.conversations[state.id] == state {
		state.closed = true
	}
	service.mu.Unlock()
}

func (service *MessagingService) PublishEnvelope(ctx context.Context, publication EnvelopePublication) (failure *Failure) {
	if service == nil || ctx == nil || publication.PeerID == "" {
		return &Failure{Code: FailureRejected}
	}
	defer func() {
		if failure == nil {
			return
		}
		service.mu.Lock()
		if state := service.publications[publication.MessageID]; state != nil {
			state.failure = *failure
			state.failed = true
		}
		service.mu.Unlock()
	}()
	owned := cloneEnvelopePublication(publication)
	if lane, ok := ctx.Value(outboundPayloadLaneContextKey{}).(outboundPayloadLaneContext); ok && lane.service == service && lane.peerID == owned.PeerID && lane.checkpoint != nil {
		defer clearEnvelopePublication(&owned)
		return service.publishEnvelope(ctx, owned, func(effect func() error) error {
			return lane.checkpoint(ctx, effect)
		})
	}
	var publishFailure *Failure
	result, admissionFailure := service.AdmitOutboundPeerWork(ctx, owned.PeerID, peer.Work{
		Kind:       peer.WorkOutbound,
		OwnedBytes: publicationOwnedBytes(owned),
		Run: func(runCtx context.Context, commit peer.Commit) error {
			publishFailure = service.publishEnvelope(runCtx, owned, commit)
			if publishFailure != nil {
				return errOutboundPeerWorkFailed
			}
			return nil
		},
		Clear: func() { clearEnvelopePublication(&owned) },
	})
	if admissionFailure != nil {
		clearEnvelopePublication(&owned)
		return admissionFailure
	}
	if err := result.Wait(ctx); err != nil {
		if errors.Is(err, errOutboundPeerWorkFailed) && publishFailure != nil {
			return publishFailure
		}
		return classifyPeerLaneError(err)
	}
	return publishFailure
}

func (service *MessagingService) publishEnvelope(ctx context.Context, publication EnvelopePublication, commit peer.Commit) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	markPublished := func() *Failure {
		if err := commit(func() error {
			service.mu.Lock()
			if current := service.publications[publication.MessageID]; current != nil && !current.published && !current.failed {
				current.published = true
			}
			service.mu.Unlock()
			return nil
		}); err != nil {
			return classifyPeerLaneError(err)
		}
		return nil
	}
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	service.mu.Lock()
	state := service.publications[publication.MessageID]
	if state == nil || state.published || !publicationMatches(state.request, publication) {
		service.mu.Unlock()
		return &Failure{Code: FailureRejected}
	}
	peerEpoch := state.request.peerEpoch
	service.mu.Unlock()
	if !service.peerEpochMatches(publication.PeerID, peerEpoch) {
		return &Failure{Code: FailureAuthorization}
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: publication.MessageID, ConversationID: publication.ConversationID,
		Sender: publication.SenderID, Recipient: publication.RecipientID, MeshID: publication.MeshID,
		Mode: publication.Mode, CorrelationID: publication.CorrelationID,
		ReplyTo: publication.ReplyTo, CreatedAt: publication.CreatedAt, ExpiresAt: publication.ExpiresAt,
		ClockUncertainty: publication.ClockUncertainty, Payload: publication.Descriptor,
	})
	if err != nil {
		return &Failure{Code: FailureRejected}
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return &Failure{Code: FailureRejected}
	}
	admission, admissionFailure := service.acquireTopologyAdmission(ctx, envelope)
	if admissionFailure != nil {
		return admissionFailure
	}
	defer admission.release()
	provisional, err := service.outbox.EnqueueProvisional(envelope)
	if err != nil {
		return classifyOutboxError(err)
	}
	cleanupProvisional := func() { _ = service.outbox.AbortProvisional(provisional); service.wakeDrain() }
	if envelope.Mode == protocol.ModeRequest {
		if err := service.outboundRPC.RegisterWithResponsePath(envelope, envelope.MessageID, state.request.Payload.ApplicationPath, envelope.ExpiresAt); err != nil {
			cleanupProvisional()
			return classifyRPCError(err)
		}
	}
	service.mu.Lock()
	if current := service.publications[publication.MessageID]; current != nil {
		current.admitted = true
	}
	service.mu.Unlock()
	var reservation outbox.Reservation
	reservation, err = service.outbox.CommitProvisional(provisional)
	if err != nil {
		_ = service.outbox.MarkUnownedTerminal(envelope.MessageID)
		service.wakeDrain()
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		return classifyOutboxError(err)
	}
	admission.release()
	if reservation.MessageID == "" {
		if failure := markPublished(); failure != nil {
			return failure
		}
		service.wakeDrain()
		return nil
	}
	if !service.beginDrain(envelope.MessageID) {
		service.outbox.Release(reservation)
		service.wakeDrain()
		return nil
	}
	if service.carrierBoundaryHook != nil {
		service.carrierBoundaryHook(envelope)
	}
	var resolution carrierResolution
	effectRan := false
	checkpoint, ok := peer.AuthorityCheckpointFromContext(ctx)
	if !ok {
		if err := service.outbox.MarkReservedTerminal(reservation); err != nil {
			service.outbox.Release(reservation)
		}
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
		return &Failure{Code: FailureInternal}
	}
	if err := checkpoint(ctx, func() error {
		effectRan = true
		resolution = service.resolveCarrier(ctx, reservation)
		return nil
	}); err != nil {
		if terminalErr := service.outbox.MarkReservedTerminal(reservation); terminalErr != nil {
			service.outbox.Release(reservation)
		}
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
		return classifyPeerLaneError(err)
	}
	if !effectRan {
		if err := service.outbox.MarkReservedTerminal(reservation); err != nil {
			service.outbox.Release(reservation)
		}
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
		return &Failure{Code: FailureInternal}
	}
	if resolution.finalized {
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
		if resolution.disposition != CarrierAccepted {
			return &Failure{Code: FailureUnavailable}
		}
		return markPublished()
	}
	switch resolution.disposition {
	case CarrierAccepted:
		if err := service.outbox.MarkReservedTerminal(reservation); err != nil {
			service.endDrain(envelope.MessageID)
			return &Failure{Code: FailureInternal}
		}
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
	case CarrierAmbiguous, CarrierUnavailable, CarrierCapacity:
		// The exact identity stays queued, but local acceptance succeeds.
		service.outbox.Release(reservation)
		service.endDrain(envelope.MessageID)
		service.wakeDrain()
	case CarrierRejected:
		_ = service.outbox.MarkReservedTerminal(reservation)
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		service.endDrain(envelope.MessageID)
		return &Failure{Code: FailureRejected}
	default:
		service.outbox.Release(reservation)
		if envelope.Mode == protocol.ModeRequest {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
		}
		service.endDrain(envelope.MessageID)
		return &Failure{Code: FailureInternal}
	}
	return markPublished()
}

func cloneEnvelopePublication(publication EnvelopePublication) EnvelopePublication {
	publication.PeerID = strings.Clone(publication.PeerID)
	publication.MessageID = strings.Clone(publication.MessageID)
	publication.MeshID = strings.Clone(publication.MeshID)
	publication.SenderID = strings.Clone(publication.SenderID)
	publication.RecipientID = strings.Clone(publication.RecipientID)
	publication.ConversationID = strings.Clone(publication.ConversationID)
	publication.CorrelationID = strings.Clone(publication.CorrelationID)
	publication.ReplyTo = strings.Clone(publication.ReplyTo)
	publication.Descriptor.Profile = strings.Clone(publication.Descriptor.Profile)
	publication.Descriptor.Inline = append([]byte(nil), publication.Descriptor.Inline...)
	publication.Descriptor.Reference = strings.Clone(publication.Descriptor.Reference)
	publication.Descriptor.EncryptionRef = strings.Clone(publication.Descriptor.EncryptionRef)
	return publication
}

func publicationOwnedBytes(publication EnvelopePublication) uint64 {
	return uint64(len(publication.PeerID) + len(publication.MessageID) + len(publication.MeshID) +
		len(publication.SenderID) + len(publication.RecipientID) + len(publication.ConversationID) +
		len(publication.CorrelationID) + len(publication.ReplyTo) + len(publication.Descriptor.Profile) +
		len(publication.Descriptor.Inline) + len(publication.Descriptor.Reference) + len(publication.Descriptor.EncryptionRef))
}

func preparedSendOwnedBytes(request PreparedSend) uint64 {
	return uint64(len(request.PeerID) + len(request.MessageID) + len(request.MeshID) +
		len(request.SenderID) + len(request.RecipientID) + len(request.ConversationID) +
		len(request.CorrelationID) + len(request.ReplyTo) + len(request.Payload.Profile) +
		len(request.Payload.ApplicationPath) + len(request.Payload.Canonical))
}

func replyLaneOwnedBytes(peer Identity, original protocol.Envelope, prepared PreparedPayload) uint64 {
	return uint64(len(peer.AgentID) + len(peer.Internal) + len(original.MessageID) + len(original.ConversationID) +
		len(original.CorrelationID) + len(original.ReplyTo) + len(original.Sender) + len(original.Recipient) +
		len(original.MeshID) + len(original.Payload.Profile) + len(original.Payload.Reference) + len(original.Payload.EncryptionRef) +
		len(original.Payload.Inline) + len(original.CredentialProof) + len(prepared.Profile) + len(prepared.ApplicationPath) + len(prepared.Canonical))
}

func clearEnvelopePublication(publication *EnvelopePublication) {
	if publication == nil {
		return
	}
	zeroBytes(publication.Descriptor.Inline)
	*publication = EnvelopePublication{}
}

func publicationMatches(request PreparedSend, publication EnvelopePublication) bool {
	return request.PeerID == publication.PeerID && request.MessageID == publication.MessageID && request.MeshID == publication.MeshID && request.SenderID == publication.SenderID && request.RecipientID == publication.RecipientID && request.ConversationID == publication.ConversationID && request.Mode == publication.Mode && request.CorrelationID == publication.CorrelationID && request.ReplyTo == publication.ReplyTo && request.CreatedAt.Equal(publication.CreatedAt) && request.ExpiresAt.Equal(publication.ExpiresAt) && request.ClockUncertainty == publication.ClockUncertainty && publication.Descriptor.Profile == request.Payload.Profile && publication.Descriptor.Size == int64(len(request.Payload.Canonical)) && publication.Descriptor.Digest == sha256.Sum256(request.Payload.Canonical) && protocol.ValidatePayloadDescriptor(publication.Descriptor) == nil
}

func (service *MessagingService) DeliveryRetry(ctx context.Context, messageID string) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	// Retry is an expedite hint. If the automatic worker already owns the exact
	// message, coalesce behind that attempt and request one post-attempt wake.
	if !service.beginDrain(messageID) {
		service.requestRetry(messageID)
		return nil
	}
	reservation, err := service.outbox.ReserveEligible(messageID)
	if err != nil {
		service.endDrain(messageID)
		return classifyOutboxError(err)
	}
	if reservation.MessageID == "" {
		service.endDrain(messageID)
		service.requestRetry(messageID)
		return nil
	}
	defer service.endDrain(messageID)
	envelope := reservation.Envelope
	admission, admissionFailure := service.acquireTopologyAdmission(ctx, envelope)
	if admissionFailure != nil {
		_ = service.outbox.MarkReservedTerminal(reservation)
		return admissionFailure
	}
	defer admission.release()
	resolution := service.resolveCarrier(ctx, reservation)
	if resolution.finalized {
		if resolution.disposition == CarrierAccepted {
			return nil
		}
		return &Failure{Code: FailureUnavailable}
	}
	switch resolution.disposition {
	case CarrierAccepted:
		if err := service.outbox.MarkReservedTerminal(reservation); err != nil {
			return &Failure{Code: FailureInternal}
		}
		return nil
	case CarrierAmbiguous:
		service.outbox.Release(reservation)
		service.wakeDrain()
		return nil
	case CarrierUnavailable:
		service.outbox.Release(reservation)
		service.wakeDrain()
		return &Failure{Code: FailureUnavailable}
	case CarrierRejected:
		_ = service.outbox.MarkReservedTerminal(reservation)
		return &Failure{Code: FailureRejected}
	case CarrierCapacity:
		service.outbox.Release(reservation)
		return &Failure{Code: FailureCapacity}
	default:
		service.outbox.Release(reservation)
		return &Failure{Code: FailureInternal}
	}
}

func (service *MessagingService) DeliveryDrop(ctx context.Context, messageID string) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	wait, err := service.outbox.DropUnowned(messageID)
	if err != nil {
		return classifyOutboxError(err)
	}
	if wait != nil {
		select {
		case result := <-wait:
			if result != nil {
				return classifyOutboxError(result)
			}
		case <-ctx.Done():
			if service.outbox.CancelDrop(messageID, wait) {
				return &Failure{Code: FailureCancelled}
			}
			result := <-wait
			if result != nil {
				return classifyOutboxError(result)
			}
		}
	}
	return nil
}

func classifyOutboxError(err error) *Failure {
	switch {
	case errors.Is(err, outbox.ErrCapacity), errors.Is(err, outbox.ErrOrdinalExhausted):
		return &Failure{Code: FailureCapacity}
	case errors.Is(err, outbox.ErrUnknownEntry), errors.Is(err, outbox.ErrOwnedEntry):
		return &Failure{Code: FailureRejected}
	case errors.Is(err, outbox.ErrDuplicateConflict):
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func classifyRPCError(err error) *Failure {
	switch {
	case errors.Is(err, rpc.ErrCapacity), errors.Is(err, rpc.ErrIssuanceExhausted):
		return &Failure{Code: FailureCapacity}
	case errors.Is(err, rpc.ErrInvalidHandle), errors.Is(err, rpc.ErrCompleted), errors.Is(err, rpc.ErrReplyInProgress), errors.Is(err, rpc.ErrInvalidLease):
		return &Failure{Code: FailureInvalidHandle}
	case errors.Is(err, rpc.ErrExpired):
		return &Failure{Code: FailureDeadline}
	case errors.Is(err, rpc.ErrCancelled):
		return &Failure{Code: FailureCancelled}
	case errors.Is(err, rpc.ErrAuthorizationRejected):
		return &Failure{Code: FailureAuthorization}
	case errors.Is(err, rpc.ErrUnknown), errors.Is(err, rpc.ErrDuplicate), errors.Is(err, rpc.ErrInvalidResponse):
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func cloneModelPayload(input model.Payload) model.Payload {
	switch value := input.Value.(type) {
	case model.NativePayload:
		value.ContentType = strings.Clone(value.ContentType)
		value.Path = strings.Clone(value.Path)
		value.Body = cloneBytes(value.Body)
		return model.Payload{Value: value}
	case model.HTTPRequestPayload:
		value.Method = strings.Clone(value.Method)
		value.Path = strings.Clone(value.Path)
		value.Query = strings.Clone(value.Query)
		value.Body = cloneBytes(value.Body)
		value.Headers = cloneHeaders(value.Headers)
		return model.Payload{Value: value}
	case model.HTTPResponsePayload:
		value.Reason = strings.Clone(value.Reason)
		value.Body = cloneBytes(value.Body)
		value.Headers = cloneHeaders(value.Headers)
		value.Error = cloneApplicationError(value.Error)
		return model.Payload{Value: value}
	case model.PayloadHandle:
		value.Handle = strings.Clone(value.Handle)
		return model.Payload{Value: value}
	default:
		return model.Payload{}
	}
}

func cloneBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	output := make([]byte, len(input))
	copy(output, input)
	return output
}

func cloneHeaders(input []model.Header) []model.Header {
	if input == nil {
		return nil
	}
	output := make([]model.Header, len(input))
	for index := range input {
		output[index] = model.Header{Name: strings.Clone(input[index].Name), Value: strings.Clone(input[index].Value)}
	}
	return output
}

func cloneApplicationError(input *model.ApplicationError) *model.ApplicationError {
	if input == nil {
		return nil
	}
	return &model.ApplicationError{Code: strings.Clone(input.Code), Detail: strings.Clone(input.Detail), DetailsJSON: strings.Clone(input.DetailsJSON)}
}

func modelPayloadDynamicBytes(input model.Payload) uint64 {
	total := uint64(0)
	add := func(value uint64) {
		if total > math.MaxUint64-value {
			total = math.MaxUint64
			return
		}
		total += value
	}
	addStrings := func(values ...string) {
		for _, value := range values {
			add(uint64(len(value)))
		}
	}
	addHeaders := func(headers []model.Header) {
		count := uint64(len(headers))
		width := uint64(unsafe.Sizeof(model.Header{}))
		if width != 0 && count > math.MaxUint64/width {
			total = math.MaxUint64
		} else {
			add(count * width)
		}
		for _, header := range headers {
			addStrings(header.Name, header.Value)
		}
	}
	switch value := input.Value.(type) {
	case model.NativePayload:
		addStrings(value.ContentType, value.Path)
		add(uint64(len(value.Body)))
	case model.HTTPRequestPayload:
		addStrings(value.Method, value.Path, value.Query)
		add(uint64(len(value.Body)))
		addHeaders(value.Headers)
	case model.HTTPResponsePayload:
		addStrings(value.Reason)
		add(uint64(len(value.Body)))
		addHeaders(value.Headers)
		if value.Error != nil {
			add(uint64(unsafe.Sizeof(model.ApplicationError{})))
			addStrings(value.Error.Code, value.Error.Detail, value.Error.DetailsJSON)
		}
	case model.PayloadHandle:
		addStrings(value.Handle)
	}
	return total
}

func (service *MessagingService) takeResponseValue(correlationID string) (retainedPayload, bool) {
	service.mu.Lock()
	retained, ok := service.responseValues[correlationID]
	delete(service.responseValues, correlationID)
	service.mu.Unlock()
	if ok {
		service.rpcBudget.ReleaseOwned(retained.bytes)
		retained.bytes = 0
	}
	return retained, ok
}
