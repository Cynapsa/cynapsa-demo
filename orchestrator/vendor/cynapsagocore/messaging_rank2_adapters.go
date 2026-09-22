package cynapsagocore

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

// rank2OwnedSender is the exact Pod 4 ownership boundary consumed by root.
// Attempt errors are intentionally absent: only durable handoff controls the
// Pod 3 carrier disposition.
type rank2OwnedSender interface {
	SendOwned(context.Context, protocol.Envelope) rank2xmpp.SendOwnership
}

type rank2EnvelopeCarrier struct {
	sender rank2OwnedSender
}

func newRank2EnvelopeCarrier(sender rank2OwnedSender) mesh.EnvelopeCarrier {
	return &rank2EnvelopeCarrier{sender: sender}
}

func (carrier *rank2EnvelopeCarrier) Send(ctx context.Context, envelope protocol.Envelope) mesh.CarrierDisposition {
	if carrier == nil || carrier.sender == nil {
		return mesh.CarrierUnavailable
	}
	sender := carrier.sender.SendOwned
	if queued, ok := carrier.sender.(interface {
		SendQueuedOwned(context.Context, protocol.Envelope) rank2xmpp.SendOwnership
	}); ok {
		sender = queued.SendQueuedOwned
	}
	switch sender(ctx, envelope.Clone()) {
	case rank2xmpp.AcceptedOwned:
		return mesh.CarrierAmbiguous
	case rank2xmpp.UnavailableNoHandoff:
		return mesh.CarrierUnavailable
	case rank2xmpp.Rejected:
		return mesh.CarrierRejected
	case rank2xmpp.Capacity:
		return mesh.CarrierCapacity
	default:
		return mesh.CarrierUnavailable
	}
}

func (carrier *rank2EnvelopeCarrier) OwnsEnvelope(messageID string) bool {
	if carrier == nil || carrier.sender == nil {
		return false
	}
	owned, ok := carrier.sender.(interface{ OwnsEnvelope(string) bool })
	return ok && owned.OwnsEnvelope(messageID)
}

type rank2AuthenticatedSource interface {
	ReceiveAuthenticatedForQuarantine(context.Context) (rank2xmpp.AuthenticatedInbound, error)
}

type authenticatedEnvelopeReceiver interface {
	Receive(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) *mesh.Failure
}

type acceptingRank2EnvelopeReceiver interface {
	ReceiveRank2(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) (bool, *mesh.Failure)
}

// rank2InboundPump is deliberately synchronous and owns no background
// goroutine. Composition runs Run under its session context, cancels and joins
// it before closing the source client, and therefore shuts down in reverse
// dependency order. ReceiveAuthenticated itself spans clean reconnects, so the
// pump never swaps sources or creates a competing session consumer.
type rank2InboundPump struct {
	source         rank2AuthenticatedSource
	receiver       authenticatedEnvelopeReceiver
	localRecipient string
	meshID         string
}

// rank2InboundItem keeps the per-delivery terminal decision beside the bytes
// it owns. The small wrapper also lets processInboundItem make cleanup an
// unconditional lexical property of one delivery instead of relying on every
// receiver and provenance branch to remember it.
type rank2InboundItem struct {
	authenticatedSender string
	envelope            protocol.Envelope
	accept              func(context.Context) error
	reject              func()
}

func newRank2InboundItem(inbound rank2xmpp.AuthenticatedInbound) rank2InboundItem {
	return rank2InboundItem{
		authenticatedSender: inbound.AuthenticatedSender,
		envelope:            inbound.Envelope,
		accept:              inbound.Accept,
		reject:              inbound.Reject,
	}
}

func newRank2InboundPump(source rank2AuthenticatedSource, receiver authenticatedEnvelopeReceiver, localRecipient, meshID string) *rank2InboundPump {
	return &rank2InboundPump{source: source, receiver: receiver, localRecipient: localRecipient, meshID: meshID}
}

func (pump *rank2InboundPump) Run(ctx context.Context) *mesh.Failure {
	if pump == nil || pump.source == nil || pump.receiver == nil || ctx == nil {
		return &mesh.Failure{Code: mesh.FailureInternal}
	}
	for {
		inbound, err := pump.source.ReceiveAuthenticatedForQuarantine(ctx)
		if err != nil {
			return classifyRank2PumpError(ctx, err)
		}
		failure := pump.processInboundItem(ctx, newRank2InboundItem(inbound))
		if failure != nil {
			if failure.Code < mesh.FailureCancelled || failure.Code > mesh.FailureInternal {
				return &mesh.Failure{Code: mesh.FailureInternal}
			}
			// Envelope-local rejection, expiry, policy, capacity, and temporary
			// topology outcomes must not retire the sole authenticated ingress
			// consumer. Only a caller-owned stop or an internal invariant failure
			// terminates the pump.
			if ctx.Err() != nil || failure.Code == mesh.FailureInternal {
				return &mesh.Failure{Code: failure.Code}
			}
		}
	}
}

// processInboundItem owns exactly one authenticated delivery until Accept
// completes. Every earlier return and every panic rejects the deferred stream
// position and clears both the transport-owned bytes and the receiver copy.
// Panics deliberately continue unwinding so the messaging worker supervisor
// observes the failure and restarts this pump independently.
func (pump *rank2InboundPump) processInboundItem(ctx context.Context, item rank2InboundItem) *mesh.Failure {
	acceptCompleted := false
	defer func() {
		if !acceptCompleted && item.reject != nil {
			item.reject()
		}
	}()
	defer clearRank2Envelope(item.envelope)

	provenance, provenanceErr := mesh.NewGroupRank2Provenance(item.authenticatedSender, pump.localRecipient, pump.meshID)
	if provenanceErr != nil {
		return &mesh.Failure{Code: mesh.FailureInternal}
	}
	envelope := item.envelope.Clone()
	defer clearRank2Envelope(envelope)

	accepted := false
	var failure *mesh.Failure
	if receiver, ok := pump.receiver.(acceptingRank2EnvelopeReceiver); ok {
		accepted, failure = receiver.ReceiveRank2(ctx, provenance, envelope)
	} else {
		failure = pump.receiver.Receive(ctx, provenance, envelope)
		accepted = failure == nil
	}
	evidence := coreEvidenceRecord{
		Event:         "rank2_inbound_disposition",
		MessageID:     item.envelope.MessageID,
		CorrelationID: item.envelope.CorrelationID,
		ReplyTo:       item.envelope.ReplyTo,
		Mode:          string(item.envelope.Mode),
		Accepted:      accepted,
	}
	if failure != nil {
		evidence.FailureCode = uint8(failure.Code)
	}
	emitCoreEvidence(evidence)
	if !accepted {
		return failure
	}
	if item.accept == nil {
		return &mesh.Failure{Code: mesh.FailureInternal}
	}
	acceptErr := item.accept(ctx)
	acceptCompleted = true
	if acceptErr != nil && ctx.Err() == nil {
		return &mesh.Failure{Code: mesh.FailureUnavailable}
	}
	return failure
}

func classifyRank2PumpError(ctx context.Context, err error) *mesh.Failure {
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return &mesh.Failure{Code: mesh.FailureCancelled}
		case context.DeadlineExceeded:
			return &mesh.Failure{Code: mesh.FailureDeadline}
		}
	}
	switch {
	case errors.Is(err, context.Canceled):
		return &mesh.Failure{Code: mesh.FailureCancelled}
	case errors.Is(err, context.DeadlineExceeded):
		return &mesh.Failure{Code: mesh.FailureDeadline}
	case errors.Is(err, rank2xmpp.ErrClosed), errors.Is(err, rank2xmpp.ErrUnavailable):
		return &mesh.Failure{Code: mesh.FailureUnavailable}
	default:
		return &mesh.Failure{Code: mesh.FailureInternal}
	}
}

func clearRank2Envelope(envelope protocol.Envelope) {
	clear(envelope.Payload.Inline)
	clear(envelope.CredentialProof)
}
