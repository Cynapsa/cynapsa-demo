package mesh

import (
	"context"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func inboundRank1ReceiptMatchesEnvelope(binding Rank1ReceiptBinding, envelope protocol.Envelope) bool {
	return binding.MessageID == envelope.MessageID &&
		binding.ConversationID == envelope.ConversationID &&
		binding.Sender == envelope.Sender && binding.Recipient == envelope.Recipient &&
		binding.MeshID == envelope.MeshID && binding.ChannelBinding != [32]byte{}
}

func (service *MessagingService) registerInboundRank1Receipt(envelope protocol.Envelope, receipt InboundRank1Receipt) bool {
	if receipt == nil {
		return true
	}
	binding, valid := inboundRank1ReceiptBinding(receipt)
	if !valid || !inboundRank1ReceiptMatchesEnvelope(binding, envelope) {
		closeInboundRank1Receipt(receipt)
		return false
	}
	select {
	case service.receiptSlots <- struct{}{}:
	default:
		closeInboundRank1Receipt(receipt)
		return false
	}
	service.mu.Lock()
	if service.closed || len(service.inboundReceipts) >= service.config.QueueCapacity {
		service.mu.Unlock()
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
		return false
	}
	if _, exists := service.inboundReceipts[envelope.MessageID]; exists {
		service.mu.Unlock()
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
		return false
	}
	service.inboundReceipts[envelope.MessageID] = retainedInboundReceipt{binding: binding, receipt: receipt}
	service.mu.Unlock()
	return true
}

func (service *MessagingService) removeInboundRank1Receipt(messageID string) {
	service.mu.Lock()
	retained := service.inboundReceipts[messageID]
	delete(service.inboundReceipts, messageID)
	service.mu.Unlock()
	closeInboundRank1Receipt(retained.receipt)
	if retained.receipt != nil {
		service.releaseInboundReceiptSlot()
	}
}

func (service *MessagingService) failInboundLane(lane conversation.LaneID, _ *conversationState, _ error) {
	service.abandonInboundLane(lane)
}

func (service *MessagingService) terminateInboundLane(lane conversation.LaneID, _ *conversationState, _ error) {
	service.abandonInboundLane(lane)
}

// abandonInboundLane retires all pre-terminal ownership for one authenticated
// lane. Already-terminal dedupe evidence and already-dispatched receipts are
// intentionally outside this ownership set.
func (service *MessagingService) abandonInboundLane(lane conversation.LaneID) {
	var receipts []InboundRank1Receipt
	var payloads []retainedPayload
	service.mu.Lock()
	for messageID, retained := range service.inboundReceipts {
		binding := retained.binding
		if binding.MeshID == lane.MeshID() && binding.ConversationID == lane.ConversationID() &&
			binding.Sender == lane.Sender() && binding.Recipient == lane.Recipient() {
			receipts = append(receipts, retained.receipt)
			delete(service.inboundReceipts, messageID)
		}
	}
	for messageID, retained := range service.inboundValues {
		if retained.lane == lane {
			payloads = append(payloads, retained)
			delete(service.inboundValues, messageID)
		}
	}
	service.mu.Unlock()
	for _, receipt := range receipts {
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
	}
	for _, payload := range payloads {
		zeroModelPayload(payload.value)
		service.rpcBudget.ReleaseOwned(payload.bytes)
	}
	service.deduper.AbandonLane(lane)
}

func (service *MessagingService) dispatchInboundRank1Receipt(messageID string) {
	service.mu.Lock()
	retained := service.inboundReceipts[messageID]
	delete(service.inboundReceipts, messageID)
	ctx := service.ctx
	closed := service.closed
	service.mu.Unlock()
	receipt := retained.receipt
	if receipt == nil {
		return
	}
	if closed || ctx == nil {
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
		return
	}
	select {
	case service.receiptQueue <- receipt:
	default:
		// Receipt loss is safe: the sender retains the exact message and falls
		// back through Rank2. Never block inbound delivery on control traffic.
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
	}
}

func (service *MessagingService) dispatchTerminalDuplicateReceipt(envelope protocol.Envelope, receipt InboundRank1Receipt) {
	if receipt == nil {
		return
	}
	binding, valid := inboundRank1ReceiptBinding(receipt)
	if !valid || !inboundRank1ReceiptMatchesEnvelope(binding, envelope) {
		closeInboundRank1Receipt(receipt)
		return
	}
	select {
	case service.receiptSlots <- struct{}{}:
	default:
		closeInboundRank1Receipt(receipt)
		return
	}
	service.mu.Lock()
	ctx := service.ctx
	closed := service.closed
	service.mu.Unlock()
	if closed || ctx == nil {
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
		return
	}
	select {
	case service.receiptQueue <- receipt:
	default:
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
	}
}

func (service *MessagingService) runInboundReceipts(ctx context.Context) {
	defer service.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case receipt := <-service.receiptQueue:
			receiptCtx, cancel := context.WithTimeout(ctx, min(service.config.OperationTimeout, 30*time.Second))
			_ = acknowledgeInboundRank1Receipt(receipt, receiptCtx)
			cancel()
			closeInboundRank1Receipt(receipt)
			service.releaseInboundReceiptSlot()
		}
	}
}

func (service *MessagingService) releaseInboundReceiptSlot() {
	select {
	case <-service.receiptSlots:
	default:
		panic("mesh: inbound Rank1 receipt slot underflow")
	}
}

func inboundRank1ReceiptBinding(receipt InboundRank1Receipt) (binding Rank1ReceiptBinding, valid bool) {
	if receipt == nil {
		return Rank1ReceiptBinding{}, false
	}
	defer func() {
		if recover() != nil {
			binding, valid = Rank1ReceiptBinding{}, false
		}
	}()
	return receipt.Binding(), true
}

func acknowledgeInboundRank1Receipt(receipt InboundRank1Receipt, ctx context.Context) (err error) {
	if receipt == nil || ctx == nil {
		return context.Canceled
	}
	defer func() {
		if recover() != nil {
			err = context.Canceled
		}
	}()
	return receipt.Acknowledge(ctx)
}

func closeInboundRank1Receipt(receipt InboundRank1Receipt) {
	if receipt == nil {
		return
	}
	defer func() { _ = recover() }()
	receipt.Close()
}
