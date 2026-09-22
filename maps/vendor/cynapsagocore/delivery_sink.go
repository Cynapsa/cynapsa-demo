package cynapsagocore

import (
	"context"
	"errors"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// inboundDeliveryRuntime is the narrow typed root composition seam implemented
// by Runtime. It keeps Pod 3 unaware of Pod 2 while preserving Pod 2's exact
// mandatory-accept ownership transaction.
type inboundDeliveryRuntime interface {
	DeliverInbound(context.Context, model.MessageReceivedEvent) error
}

type runtimeDeliverySink struct {
	target inboundDeliveryRuntime
}

var _ mesh.DeliverySink = (*runtimeDeliverySink)(nil)

func newRuntimeDeliverySink(target inboundDeliveryRuntime) mesh.DeliverySink {
	if target == nil {
		return &runtimeDeliverySink{}
	}
	return &runtimeDeliverySink{target: target}
}

func (sink *runtimeDeliverySink) Deliver(ctx context.Context, inbound mesh.InboundDelivery) mesh.DeliveryDisposition {
	if sink == nil || sink.target == nil || ctx == nil {
		return mesh.DeliveryRejected
	}
	if err := ctx.Err(); err != nil {
		return mesh.DeliveryUnavailable
	}
	value, ok := projectInboundDelivery(inbound)
	if !ok {
		return mesh.DeliveryRejected
	}
	return classifyDeliveryDisposition(sink.target.DeliverInbound(ctx, value))
}

func projectInboundDelivery(inbound mesh.InboundDelivery) (model.MessageReceivedEvent, bool) {
	if inbound.MessageID == "" || inbound.ConversationID == "" || inbound.FromAgentID == "" || inbound.MeshID == "" {
		return model.MessageReceivedEvent{}, false
	}
	switch inbound.Mode {
	case protocol.ModeMessage:
		if inbound.RequestHandle != "" {
			return model.MessageReceivedEvent{}, false
		}
	case protocol.ModeRequest:
		if rpc.ValidateRequestHandle(inbound.RequestHandle) != nil {
			return model.MessageReceivedEvent{}, false
		}
	default:
		return model.MessageReceivedEvent{}, false
	}
	payload, ok := cloneInboundPayload(inbound.Payload)
	if !ok {
		return model.MessageReceivedEvent{}, false
	}
	return model.MessageReceivedEvent{
		MessageID: inbound.MessageID, ConversationID: inbound.ConversationID,
		FromAgentID: inbound.FromAgentID, MeshID: inbound.MeshID,
		Mode: string(inbound.Mode), RequestHandle: inbound.RequestHandle,
		Payload: payload,
	}, true
}

func cloneInboundPayload(input model.Payload) (model.Payload, bool) {
	switch value := input.Value.(type) {
	case model.NativePayload:
		value.Body = append([]byte(nil), value.Body...)
		return model.Payload{Value: value}, true
	case model.HTTPRequestPayload:
		value.Headers = append([]model.Header(nil), value.Headers...)
		value.Body = append([]byte(nil), value.Body...)
		return model.Payload{Value: value}, true
	case model.HTTPResponsePayload:
		value.Headers = append([]model.Header(nil), value.Headers...)
		value.Body = append([]byte(nil), value.Body...)
		value.Error = cloneApplicationError(value.Error)
		return model.Payload{Value: value}, true
	case model.PayloadHandle:
		return model.Payload{Value: value}, true
	default:
		return model.Payload{}, false
	}
}

func cloneApplicationError(value *model.ApplicationError) *model.ApplicationError {
	if value == nil {
		return nil
	}
	return &model.ApplicationError{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}

func classifyDeliveryDisposition(err error) mesh.DeliveryDisposition {
	switch {
	case err == nil:
		return mesh.DeliveryAccepted
	case errors.Is(err, delivery.ErrQueueFull), errors.Is(err, delivery.ErrAdmissionExhausted):
		return mesh.DeliveryCapacity
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, coreruntime.ErrClosing), errors.Is(err, coreruntime.ErrClosed),
		errors.Is(err, delivery.ErrClosing), errors.Is(err, delivery.ErrClosed):
		return mesh.DeliveryUnavailable
	default:
		return mesh.DeliveryRejected
	}
}
