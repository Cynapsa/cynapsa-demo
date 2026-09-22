package mesh

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/policy"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

// maximumFutureCreationSkew is the bounded interim V1 allowance for two
// independently server-calibrated agents. It is deliberately independent of
// the signed sender and receiver uncertainty fields; those fields remain
// integrity-bound and continue to govern request expiry.
const maximumFutureCreationSkew = time.Second

var errInboundPeerWorkFailed = errors.New("mesh: inbound peer work failed")

// Receive accepts only an envelope paired with a transport-authenticated
// private sender identity. Envelope sender fields cannot establish trust.
func (service *MessagingService) Receive(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope) *Failure {
	return service.receiveThroughPeerLane(ctx, provenance, envelope, nil, nil)
}

// ReceiveRank2 reports terminal local acceptance separately from transport
// parsing. XEP-0198 may advance only when accepted is true; an in-flight
// duplicate deliberately remains unhandled until its owning path terminates.
// Capacity exhaustion drops the current carrier item: retaining it by tearing
// down the entire stream creates an unbounded replay loop. Capacity failures
// after logical admission retain terminal message-ID evidence so another
// carrier cannot resurrect the discarded message.
func (service *MessagingService) ReceiveRank2(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope) (accepted bool, failure *Failure) {
	var terminal atomic.Bool
	failure = service.receiveThroughPeerLane(ctx, provenance, envelope, &terminal, nil)
	if failure != nil && failure.Code == FailureCapacity {
		return true, failure
	}
	return terminal.Load() && failure == nil, failure
}

// ReceiveRank1 additionally reports whether this exact successful admission
// may generate an authenticated Rank1 receipt. A terminal duplicate and a new
// durably recorded envelope are eligible; a duplicate of work still in-flight
// on another ingress path is not.
func (service *MessagingService) ReceiveRank1(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope) (bool, *Failure) {
	var eligible atomic.Bool
	failure := service.receiveThroughPeerLane(ctx, provenance, envelope, &eligible, nil)
	return eligible.Load() && failure == nil, failure
}

// ReceiveRank1WithReceipt transfers one exact authenticated source route to
// the service. The route is acknowledged only after this exact envelope is
// terminal.
func (service *MessagingService) ReceiveRank1WithReceipt(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope, receipt InboundRank1Receipt) *Failure {
	return service.receiveThroughPeerLane(ctx, provenance, envelope, nil, receipt)
}

// QuarantineRank1WithReceipt transfers paused authenticated input directly
// into its peer lane and returns after bounded admission. The lane owns the
// envelope, receipt capability, accounting, deadline, and membership outcome;
// no Manager- or Link-level paused queue remains as a second owner.
func (service *MessagingService) QuarantineRank1WithReceipt(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope, receipt InboundRank1Receipt) *Failure {
	return service.receiveThroughPeerLaneMode(ctx, provenance, envelope, nil, receipt, false)
}

// receiveThroughPeerLane freezes the carrier-owned envelope before handing it
// to the one actor shared by every transport for this authenticated peer. The
// lane's Commit capability is the membership linearization point for all SDK
// publication, receipt admission, and terminal success performed by receive.
func (service *MessagingService) receiveThroughPeerLane(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope, receiptEligible *atomic.Bool, receipt InboundRank1Receipt) *Failure {
	return service.receiveThroughPeerLaneMode(ctx, provenance, envelope, receiptEligible, receipt, true)
}

func (service *MessagingService) receiveThroughPeerLaneMode(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope, receiptEligible *atomic.Bool, receipt InboundRank1Receipt, wait bool) *Failure {
	if service == nil || ctx == nil || provenance.Sender() == "" {
		closeInboundRank1Receipt(receipt)
		return &Failure{Code: FailureRejected}
	}
	owned := envelope.Clone()
	ownedReceipt := receipt
	var receiveFailure atomic.Pointer[Failure]
	result, admissionFailure := service.AdmitAuthenticatedPeerWork(ctx, provenance.Sender(), peer.Work{
		Kind:       peer.WorkInbound,
		OwnedBytes: envelopeOwnedBytes(owned),
		Run: func(runCtx context.Context, commit peer.Commit) error {
			transferredReceipt := ownedReceipt
			ownedReceipt = nil
			failure := service.receive(runCtx, provenance, owned, receiptEligible, transferredReceipt, commit)
			receiveFailure.Store(failure)
			if failure != nil {
				return errInboundPeerWorkFailed
			}
			return nil
		},
		Clear: func() {
			clearOwnedEnvelope(&owned)
			closeInboundRank1Receipt(ownedReceipt)
			ownedReceipt = nil
		},
	})
	if admissionFailure != nil {
		clearOwnedEnvelope(&owned)
		closeInboundRank1Receipt(ownedReceipt)
		return admissionFailure
	}
	if !wait {
		return nil
	}
	if err := result.Wait(ctx); err != nil {
		if failure := receiveFailure.Load(); errors.Is(err, errInboundPeerWorkFailed) && failure != nil {
			return failure
		}
		return classifyPeerLaneError(err)
	}
	return receiveFailure.Load()
}

func (service *MessagingService) receive(ctx context.Context, provenance AuthenticatedProvenance, envelope protocol.Envelope, receiptEligible *atomic.Bool, receipt InboundRank1Receipt, commit peer.Commit) *Failure {
	receiptRegistered := false
	receiptRetained := false
	defer func() {
		if receipt != nil && receiptRegistered && !receiptRetained {
			service.removeInboundRank1Receipt(envelope.MessageID)
		}
		if receipt != nil && !receiptRegistered && !receiptRetained {
			closeInboundRank1Receipt(receipt)
		}
	}()
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureLifecycleActive(ctx); failure != nil {
		return failure
	}
	if len(envelope.CredentialProof) != 0 {
		return &Failure{Code: FailureRejected}
	}
	permit, err := service.gate.EvaluateInbound(envelope.Clone(), provenance)
	if err != nil {
		return classifyInboundError(err)
	}
	reading, failure := service.calibratedTime()
	if failure != nil {
		return failure
	}
	maximumCreatedAt := reading.UTC.Add(maximumFutureCreationSkew)
	if envelope.CreatedAt.After(maximumCreatedAt) {
		return &Failure{Code: FailureRejected}
	}
	responsePath := ""
	lateResponse := false
	lane, err := conversation.VerifyEnvelopeBinding(envelope, conversation.AuthenticatedBinding{
		MeshID: service.identity.MeshID(), AuthenticatedSender: provenance.Sender(), Recipient: service.identity.BoundFull(),
	})
	if err != nil {
		return &Failure{Code: FailureRejected}
	}
	stateAdmission, failure := service.acquireTopologyAdmission(ctx, envelope)
	if failure != nil {
		return failure
	}
	peer, lookupErr := stateAdmission.identity(lane.Sender())
	if lookupErr != nil {
		stateAdmission.release()
		return classifyMeshError(lookupErr)
	}
	state, failure := service.conversationForInbound(lane, peer)
	stateAdmission.release()
	if failure != nil {
		return failure
	}
	defer service.finishInbound(state)
	classification, err := service.deduper.Check(lane, envelope)
	if err != nil {
		return classifyConversationError(err)
	}
	if classification == conversation.DedupeDuplicateTerminal {
		if err := commit(func() error {
			if receiptEligible != nil {
				receiptEligible.Store(true)
			}
			service.dispatchTerminalDuplicateReceipt(envelope, receipt)
			receiptRetained = true
			return nil
		}); err != nil {
			return classifyPeerLaneError(err)
		}
		return nil
	}
	if classification == conversation.DedupeDuplicateInFlight {
		if err := commit(func() error { return nil }); err != nil {
			return classifyPeerLaneError(err)
		}
		return nil
	}
	if envelope.Mode == protocol.ModeResponse {
		var ok bool
		responsePath, ok = service.outboundRPC.PendingResponsePath(envelope)
		if !ok {
			if !service.outboundRPC.ValidateLateResponse(envelope) {
				// A valid response can outlive process-local RPC state when the
				// previous process received it but exited before its XEP-0198 ACK
				// reached the server. It is permanently irrelevant to this process,
				// so consume it instead of turning it into a replay poison pill.
				// TODO: emit terminal-drop telemetry for the orphaned response.
				var consumeFailure *Failure
				if err := commit(func() error {
					consumeFailure = service.consumeWithoutDelivery(ctx, lane, state, envelope)
					if consumeFailure == nil && receiptEligible != nil {
						receiptEligible.Store(true)
					}
					return nil
				}); err != nil {
					return classifyPeerLaneError(err)
				}
				return consumeFailure
			}
			lateResponse = true
		}
	}
	if receipt != nil {
		if !service.registerInboundRank1Receipt(envelope, receipt) {
			// Receipt routing is transport-control ownership, not logical
			// admission. Losing its bounded route means no ACK; the sender keeps
			// the same envelope and its later Rank2 fallback converges through
			// terminal dedupe.
			receipt = nil
		} else {
			receiptRegistered = true
		}
	}
	if lateResponse {
		var consumeFailure *Failure
		if err := commit(func() error {
			consumeFailure = service.consumeWithoutDelivery(ctx, lane, state, envelope)
			if consumeFailure != nil {
				return nil
			}
			if retireErr := service.outbox.RetireLateRPCResponse(envelope); retireErr != nil {
				consumeFailure = classifyOutboxError(retireErr)
				return nil
			}
			if receiptEligible != nil {
				receiptEligible.Store(true)
			}
			receiptRetained = true
			return nil
		}); err != nil {
			return classifyPeerLaneError(err)
		}
		return consumeFailure
	}
	if requestExpiredAt(envelope, reading) {
		var failure *Failure
		if err := commit(func() error {
			failure = service.consumeWithoutDelivery(ctx, lane, state, envelope)
			return nil
		}); err != nil {
			return classifyPeerLaneError(err)
		}
		if failure == nil && receiptEligible != nil {
			receiptEligible.Store(true)
		}
		if failure == nil {
			receiptRetained = true
		}
		return failure
	}
	canonical, disposition := service.pipeline.Materialize(ctx, envelope.Clone())
	if disposition != PayloadAccepted {
		zeroBytes(canonical)
		failure := payloadFailure(disposition)
		if failure.Code == FailureCapacity {
			service.markInboundCapacityDrop(lane, envelope)
		}
		service.failInboundLane(lane, state, errors.New("materialization failed"))
		return failure
	}
	defer zeroBytes(canonical)
	var materialized policy.MaterializationResult
	if envelope.Mode == protocol.ModeResponse {
		materialized, err = service.gate.VerifyResponseMaterialization(permit, envelope.Clone(), canonical, responsePath)
	} else {
		materialized, err = service.gate.VerifyMaterialization(permit, envelope.Clone(), canonical)
	}
	if err != nil {
		service.failInboundLane(lane, state, errors.New("materialization rejected"))
		service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		return classifyInboundError(err)
	}
	err = service.gate.AuthorizeApplication(permit, envelope.Clone(), materialized)
	if err != nil {
		service.failInboundLane(lane, state, errors.New("authorization rejected"))
		service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		return classifyInboundError(err)
	}
	expired, failure := service.requestExpired(envelope)
	if failure != nil {
		service.failInboundLane(lane, state, errors.New("calibrated clock unavailable"))
		service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		return failure
	}
	if expired {
		if err := commit(func() error {
			failure = service.consumeWithoutDelivery(ctx, lane, state, envelope)
			return nil
		}); err != nil {
			return classifyPeerLaneError(err)
		}
		if failure == nil && receiptEligible != nil {
			receiptEligible.Store(true)
		}
		if failure == nil {
			receiptRetained = true
		}
		return failure
	}
	value, disposition := service.pipeline.Decode(canonical)
	if disposition != PayloadAccepted {
		failure := payloadFailure(disposition)
		if failure.Code == FailureCapacity {
			service.markInboundCapacityDrop(lane, envelope)
		}
		service.failInboundLane(lane, state, errors.New("payload decode failed"))
		service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		return failure
	}
	service.mu.Lock()
	if len(service.inboundValues) >= service.config.QueueCapacity {
		service.mu.Unlock()
		service.markInboundCapacityDrop(lane, envelope)
		service.failInboundLane(lane, state, errors.New("delivery capacity exhausted"))
		service.failAuthenticatedResponse(envelope, rpc.ErrCapacity)
		return &Failure{Code: FailureCapacity}
	}
	if _, exists := service.inboundValues[envelope.MessageID]; exists {
		service.mu.Unlock()
		service.failInboundLane(lane, state, errors.New("duplicate materialized delivery"))
		service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		return &Failure{Code: FailureRejected}
	}
	retainedBytes := uint64(0)
	if envelope.Mode == protocol.ModeResponse {
		retainedBytes = modelPayloadDynamicBytes(value) + uint64(len(envelope.MessageID))
		if !service.rpcBudget.ReserveOwned(retainedBytes) {
			service.mu.Unlock()
			service.markInboundCapacityDrop(lane, envelope)
			service.failInboundLane(lane, state, errors.New("RPC response byte capacity exhausted"))
			service.failAuthenticatedResponse(envelope, rpc.ErrCapacity)
			return &Failure{Code: FailureCapacity}
		}
	}
	// Freeze the charged key only after admission. A valid short identifier may
	// otherwise keep an arbitrarily large caller string backing alive.
	ownedMessageID := strings.Clone(envelope.MessageID)
	service.inboundValues[ownedMessageID] = retainedPayload{lane: lane, peerID: peer.Internal, value: cloneModelPayload(value), bytes: retainedBytes}
	service.mu.Unlock()
	delivery := conversation.Delivery{Envelope: envelope.Clone(), CanonicalPayload: append([]byte(nil), canonical...)}
	if failure := service.deliver(ctx, lane, state, delivery, commit); failure != nil {
		if failure.Code == FailureCapacity {
			service.markInboundCapacityDrop(lane, envelope)
		}
		service.terminateInboundLane(lane, state, errors.New("local delivery failed"))
		if failure.Code == FailureCapacity {
			service.failAuthenticatedResponse(envelope, rpc.ErrCapacity)
		} else {
			service.failAuthenticatedResponse(envelope, rpc.ErrInvalidResponse)
		}
		return failure
	}
	classification, _ = service.deduper.Check(lane, envelope)
	if classification == conversation.DedupeDuplicateTerminal {
		if receiptEligible != nil {
			receiptEligible.Store(true)
		}
	}
	receiptRetained = true
	return nil
}

func (service *MessagingService) requestExpired(envelope protocol.Envelope) (bool, *Failure) {
	reading, failure := service.calibratedTime()
	if failure != nil {
		return false, failure
	}
	return requestExpiredAt(envelope, reading), nil
}

// requestExpiredAt rejects whenever clock uncertainty makes expiry ambiguous.
// It therefore may retire a request early, but can never extend sender TTL.
func requestExpiredAt(envelope protocol.Envelope, reading CalibratedTime) bool {
	if envelope.Mode != protocol.ModeRequest {
		return false
	}
	senderEarliestExpiry := envelope.ExpiresAt.Add(-envelope.ClockUncertainty)
	receiverLatestNow := reading.UTC.Add(reading.Uncertainty)
	return !receiverLatestNow.Before(senderEarliestExpiry)
}

func (service *MessagingService) consumeWithoutDelivery(_ context.Context, lane conversation.LaneID, state *conversationState, envelope protocol.Envelope) *Failure {
	if err := service.deduper.MarkTerminal(lane, envelope); err != nil {
		service.terminateInboundLane(lane, state, errors.New("terminal dedupe failed"))
		return classifyConversationError(err)
	}
	service.dispatchInboundRank1Receipt(envelope.MessageID)
	return nil
}

// markInboundCapacityDrop converts this already-admitted message identity into
// a terminal tombstone before lane cleanup removes in-flight state. The caller
// still returns FailureCapacity for local diagnostics, while Rank2 acknowledges
// and discards the carrier item.
func (service *MessagingService) markInboundCapacityDrop(lane conversation.LaneID, envelope protocol.Envelope) {
	_ = service.deduper.MarkTerminal(lane, envelope)
}

func matchesPendingResponse(request, response protocol.Envelope) bool {
	return request.Mode == protocol.ModeRequest && response.Mode == protocol.ModeResponse && request.CorrelationID == response.CorrelationID && request.MessageID == response.ReplyTo && request.MeshID == response.MeshID && request.ConversationID == response.ConversationID && request.Sender == response.Recipient && request.Recipient == response.Sender
}

func (service *MessagingService) conversationForInbound(lane conversation.LaneID, peer Identity) (*conversationState, *Failure) {
	peerEpoch, ok := service.capturePeerEpoch(peer.Internal)
	if !ok {
		return nil, &Failure{Code: FailureAuthorization}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.activeOperations >= service.config.QueueCapacity {
		return nil, &Failure{Code: FailureCapacity}
	}
	if existing := service.conversations[lane.ConversationID()]; existing != nil {
		if existing.peerEpoch != peerEpoch {
			return nil, &Failure{Code: FailureAuthorization}
		}
		if existing.closed || existing.peerAgent != peer.AgentID || existing.peerInternal != peer.Internal {
			return nil, &Failure{Code: FailureRejected}
		}
		existing.active++
		service.activeOperations++
		return existing, nil
	}
	if len(service.conversations) >= service.config.QueueCapacity {
		return nil, &Failure{Code: FailureCapacity}
	}
	state := &conversationState{id: lane.ConversationID(), peerAgent: peer.AgentID, peerInternal: peer.Internal, peerEpoch: peerEpoch, active: 1}
	service.conversations[state.id] = state
	service.activeOperations++
	return state, nil
}

func (service *MessagingService) finishInbound(state *conversationState) {
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
	if state.closed && state.active == 0 && service.conversations[state.id] == state {
		delete(service.conversations, state.id)
	}
	service.mu.Unlock()
}

func (service *MessagingService) deliver(ctx context.Context, lane conversation.LaneID, state *conversationState, delivery conversation.Delivery, commit peer.Commit) *Failure {
	defer zeroBytes(delivery.CanonicalPayload)
	defer zeroBytes(delivery.Envelope.Payload.Inline)
	defer zeroBytes(delivery.Envelope.CredentialProof)
	admission, admissionFailure := service.acquireTopologyAdmission(ctx, delivery.Envelope)
	if admissionFailure != nil {
		return admissionFailure
	}
	defer admission.release()
	service.mu.Lock()
	retained, ok := service.inboundValues[delivery.Envelope.MessageID]
	delete(service.inboundValues, delivery.Envelope.MessageID)
	service.mu.Unlock()
	if !ok {
		return &Failure{Code: FailureInternal}
	}
	payload := retained.value
	payloadBytes := retained.bytes
	defer func() {
		zeroModelPayload(payload)
		service.rpcBudget.ReleaseOwned(payloadBytes)
	}()
	switch delivery.Envelope.Mode {
	case protocol.ModeResponse:
		var responseFailure *Failure
		if err := commit(func() error {
			correlationID := delivery.Envelope.CorrelationID
			retainedBytes := modelPayloadDynamicBytes(payload) + uint64(len(correlationID))
			service.mu.Lock()
			if len(service.responseValues) >= service.config.QueueCapacity {
				service.mu.Unlock()
				service.failAuthenticatedResponse(delivery.Envelope, rpc.ErrCapacity)
				responseFailure = &Failure{Code: FailureCapacity}
				return nil
			}
			if _, exists := service.responseValues[correlationID]; exists {
				service.mu.Unlock()
				service.failAuthenticatedResponse(delivery.Envelope, rpc.ErrInvalidResponse)
				responseFailure = &Failure{Code: FailureRejected}
				return nil
			}
			if !service.rpcBudget.ReplaceOwned(payloadBytes, retainedBytes) {
				service.mu.Unlock()
				service.failAuthenticatedResponse(delivery.Envelope, rpc.ErrCapacity)
				responseFailure = &Failure{Code: FailureCapacity}
				return nil
			}
			payloadBytes = retainedBytes
			ownedCorrelation := strings.Clone(correlationID)
			service.responseValues[ownedCorrelation] = retainedPayload{peerID: delivery.Envelope.Sender, value: payload, bytes: retainedBytes}
			payload = model.Payload{}
			payloadBytes = 0
			service.mu.Unlock()
			if completeErr := service.outboundRPC.CompleteAccepted(delivery.Envelope, func(request, response protocol.Envelope) error {
				return service.outbox.RetireAcceptedRPCResponse(request, response)
			}); completeErr != nil {
				failed, _ := service.takeResponseValue(correlationID)
				zeroModelPayload(failed.value)
				responseFailure = classifyRPCError(completeErr)
			} else {
				service.wakeDrain()
			}
			return nil
		}); err != nil {
			if errors.Is(err, peer.ErrUnauthorized) {
				service.outboundRPC.FailPeerCorrelations(delivery.Envelope.MeshID, delivery.Envelope.Sender)
				failed, _ := service.takeResponseValue(delivery.Envelope.CorrelationID)
				zeroModelPayload(failed.value)
			}
			return classifyPeerLaneError(err)
		}
		if responseFailure != nil {
			return responseFailure
		}
	case protocol.ModeMessage, protocol.ModeRequest:
		expired, failure := service.requestExpired(delivery.Envelope)
		if failure != nil {
			return failure
		}
		if expired {
			break
		}
		service.mu.Lock()
		lifetime := service.ctx
		service.mu.Unlock()
		select {
		case <-service.deliverySlot:
			defer func() { service.deliverySlot <- struct{}{} }()
		case <-ctx.Done():
			return contextFailure(ctx)
		case <-lifetime.Done():
			return &Failure{Code: FailureUnavailable}
		}
		requestHandle := ""
		var disposition DeliveryDisposition
		var commitFailure *Failure
		if err := commit(func() error {
			if delivery.Envelope.Mode == protocol.ModeRequest {
				var handleErr error
				requestHandle, handleErr = service.inboundRPC.CreateHandle(delivery.Envelope, delivery.Envelope.ExpiresAt)
				if handleErr != nil {
					commitFailure = classifyRPCError(handleErr)
					return nil
				}
			}
			deliveryContext, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(lifetime, cancel)
			disposition = service.deliveries.Deliver(deliveryContext, InboundDelivery{
				MessageID: delivery.Envelope.MessageID, ConversationID: delivery.Envelope.ConversationID,
				FromAgentID: state.peerAgent, MeshID: delivery.Envelope.MeshID, Mode: delivery.Envelope.Mode,
				RequestHandle: requestHandle, Payload: cloneModelPayload(payload),
			})
			stop()
			cancel()
			return nil
		}); err != nil {
			if requestHandle != "" && errors.Is(err, peer.ErrUnauthorized) {
				service.inboundRPC.CancelPeer(delivery.Envelope.MeshID, delivery.Envelope.Sender)
			}
			return classifyPeerLaneError(err)
		}
		if commitFailure != nil {
			return commitFailure
		}
		if disposition != DeliveryAccepted {
			if requestHandle != "" {
				_ = service.inboundRPC.Cancel(requestHandle)
			}
			if disposition == DeliveryCapacity {
				service.markInboundCapacityDrop(lane, delivery.Envelope)
			}
			service.terminateInboundLane(lane, state, errors.New("local delivery failed"))
			switch disposition {
			case DeliveryCapacity:
				return &Failure{Code: FailureCapacity}
			case DeliveryUnavailable:
				return &Failure{Code: FailureUnavailable}
			case DeliveryRejected:
				return &Failure{Code: FailureRejected}
			default:
				return &Failure{Code: FailureInternal}
			}
		}
	default:
		return &Failure{Code: FailureRejected}
	}
	if err := service.deduper.MarkTerminal(lane, delivery.Envelope); err != nil {
		return classifyConversationError(err)
	}
	service.dispatchInboundRank1Receipt(delivery.Envelope.MessageID)
	return nil
}

func (service *MessagingService) failAuthenticatedResponse(envelope protocol.Envelope, cause error) {
	if service == nil || envelope.Mode != protocol.ModeResponse {
		return
	}
	_ = service.outboundRPC.Fail(envelope.CorrelationID, cause)
	retained, ok := service.takeResponseValue(envelope.CorrelationID)
	if ok {
		zeroModelPayload(retained.value)
	}
}

func (service *MessagingService) HandlerRegister(ctx context.Context, path string) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	return classifyHandlerError(service.handlers.Register(path))
}

func (service *MessagingService) HandlerUnregister(ctx context.Context, path string) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	return classifyHandlerError(service.handlers.Unregister(path))
}

func classifyHandlerError(err error) *Failure {
	switch err {
	case nil:
		return nil
	case ErrHandlerCapacity:
		return &Failure{Code: FailureCapacity}
	case ErrHandlerInvalid:
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func (service *MessagingService) ConversationList(ctx context.Context) (model.ConversationListResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.ConversationListResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.ConversationListResult{}, failure
	}
	service.mu.Lock()
	ids := make([]string, 0, len(service.conversations))
	for id, state := range service.conversations {
		if state.closed {
			continue
		}
		ids = append(ids, id)
	}
	service.mu.Unlock()
	sort.Strings(ids)
	result := model.ConversationListResult{Conversations: make([]model.ConversationStatus, 0, len(ids))}
	for _, id := range ids {
		status, failure := service.ConversationStatus(ctx, id)
		if failure != nil {
			return model.ConversationListResult{}, failure
		}
		result.Conversations = append(result.Conversations, status)
	}
	return result, nil
}

func (service *MessagingService) ConversationStatus(ctx context.Context, conversationID string) (model.ConversationStatus, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.ConversationStatus{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.ConversationStatus{}, failure
	}
	service.mu.Lock()
	state := service.conversations[conversationID]
	if state != nil {
		state = &conversationState{id: state.id, peerAgent: state.peerAgent, peerInternal: state.peerInternal, peerEpoch: state.peerEpoch, closed: state.closed, active: state.active, outboundFail: state.outboundFail}
	}
	service.mu.Unlock()
	if state == nil || state.closed {
		return model.ConversationStatus{}, &Failure{Code: FailureRejected}
	}
	queued := uint64(0)
	pending, err := service.outbox.Pending()
	if err != nil {
		return model.ConversationStatus{}, &Failure{Code: FailureInternal}
	}
	for _, envelope := range pending {
		if envelope.ConversationID == conversationID {
			queued++
		}
	}
	blocked := state.outboundFail
	deliveryState := "ready"
	if blocked {
		deliveryState = "blocked"
	} else if queued > 0 {
		deliveryState = "queued"
	}
	return model.ConversationStatus{ConversationID: state.id, MeshID: service.identity.MeshID(), Peer: state.peerAgent, DeliveryState: deliveryState, QueuedMessageCount: queued, Blocked: blocked}, nil
}

func (service *MessagingService) ConversationClose(ctx context.Context, conversationID string) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	service.mu.Lock()
	state := service.conversations[conversationID]
	if state != nil && state.closed {
		service.mu.Unlock()
		return nil
	}
	service.mu.Unlock()
	status, failure := service.ConversationStatus(ctx, conversationID)
	if failure != nil {
		return failure
	}
	// RPC ownership is indexed by exact conversation. A retained terminal
	// outbound result continues to block until its waiter consumes it, matching
	// the prior table-wide contract without coupling unrelated conversations.
	if status.QueuedMessageCount != 0 || service.outboundRPC.ActiveConversation(conversationID) || service.inboundRPC.ActiveConversation(conversationID) {
		return &Failure{Code: FailureRejected}
	}
	return service.closeConversationIfIdle(conversationID)
}

// closeConversationIfIdle is the final close linearization point. The early
// status checks above provide detailed validation and prune expired RPC state,
// but cannot authorize the transition: an operation may register exact queued
// ownership before dropping state.active and before this lock is acquired.
func (service *MessagingService) closeConversationIfIdle(conversationID string) *Failure {
	service.mu.Lock()
	defer service.mu.Unlock()
	state := service.conversations[conversationID]
	if state == nil || state.active != 0 {
		return &Failure{Code: FailureRejected}
	}
	// Lock order is service.mu -> bounded outbox/RPC locks. None of
	// those ownership tables calls back into MessagingService or acquires
	// service.mu. Rechecking all three exact stores here closes the interval
	// between the earlier scans and this final state transition.
	if service.outbox.HasConversation(conversationID) ||
		service.outboundRPC.RetainsConversation(conversationID) ||
		service.inboundRPC.RetainsConversation(conversationID) {
		return &Failure{Code: FailureRejected}
	}
	state.closed = true
	return nil
}

func (service *MessagingService) QueueStatus(ctx context.Context) (model.DeliveryQueueStatus, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.DeliveryQueueStatus{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.DeliveryQueueStatus{}, failure
	}
	messages, _ := service.outbox.Usage()
	return model.DeliveryQueueStatus{Queued: uint64(messages)}, nil
}

func classifyInboundError(err error) *Failure {
	switch {
	case errors.Is(err, policy.ErrMembership), errors.Is(err, policy.ErrApplicationDenied):
		return &Failure{Code: FailureAuthorization}
	case errors.Is(err, policy.ErrCredential), errors.Is(err, policy.ErrClock), errors.Is(err, policy.ErrIdentity), errors.Is(err, policy.ErrMesh), errors.Is(err, policy.ErrIntegrity), errors.Is(err, policy.ErrMaterializationRejected), errors.Is(err, policy.ErrInvalidPermit):
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func classifyConversationError(err error) *Failure {
	switch {
	case errors.Is(err, conversation.ErrDedupeCapacity):
		return &Failure{Code: FailureCapacity}
	case errors.Is(err, conversation.ErrMessageConflict), errors.Is(err, conversation.ErrLaneMismatch):
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}
