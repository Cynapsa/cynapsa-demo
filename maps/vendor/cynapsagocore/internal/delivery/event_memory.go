package delivery

import (
	"math"
	"strings"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const (
	// MaximumEventOwnedBytes is independent from the native ABI descriptor
	// registry's byte quota. A leased event can temporarily coexist with its
	// encoded descriptor, and each ownership domain accounts its bytes once.
	MaximumEventOwnedBytes      uint64 = 256 << 20
	eventMetadataAllowance      uint64 = 256 << 10
	shutdownEventByteCapacity          = 1 << 10
	shutdownEventMaximumBytes          = shutdownEventByteCapacity / shutdownEventCapacity
	maximumOrdinaryEventBytes          = MaximumEventOwnedBytes - shutdownEventByteCapacity
	headerBackingSlotBytes             = uint64(unsafe.Sizeof(model.Header{}))
	applicationErrorObjectBytes        = uint64(unsafe.Sizeof(model.ApplicationError{}))
)

// DeriveOrdinaryByteCapacity derives the immutable runtime event budget from
// V1 queue and payload configuration. The per-item metadata allowance covers
// bounded identifiers, headers, and private error metadata outside payload
// bytes; the fixed lifecycle reserve is added by Dispatcher separately.
func DeriveOrdinaryByteCapacity(queueLimit uint32, payloadLimit uint64) (uint64, error) {
	if queueLimit == 0 {
		return 0, ErrInvalidCapacity
	}
	perItem, ok := checkedAdd(payloadLimit, eventMetadataAllowance)
	if !ok {
		return 0, ErrInvalidCapacity
	}
	total, ok := checkedMultiply(uint64(queueLimit), perItem)
	if !ok || total > maximumOrdinaryEventBytes {
		total = maximumOrdinaryEventBytes
	}
	if total == 0 {
		return 0, ErrInvalidCapacity
	}
	return total, nil
}

// eventOwnedBytes computes the conservative dynamic-memory charge before any
// caller-owned field is frozen. Preflight charges slice backing capacity to
// prevent a tiny view from retaining a large caller allocation. After the
// exact-length freeze, admission recomputes and retains the exact owned byte
// charge. CoreError.Cause is deliberately excluded: it is never boundary-
// mapped and successful admission replaces it with nil.
func eventOwnedBytes(event model.Event) (uint64, error) {
	if event.ID == "" || event.Name == "" || event.CreatedAt.IsZero() || event.Value == nil {
		return 0, ErrInvalidEvent
	}
	size := uint64(0)
	if !addStringBytes(&size, event.ID) || !addStringBytes(&size, event.Name) {
		return 0, ErrInvalidEvent
	}
	if !addEventValueBytes(&size, event.Value) {
		return 0, ErrInvalidEvent
	}
	return size, nil
}

func addEventValueBytes(size *uint64, value model.EventValue) bool {
	switch typed := value.(type) {
	case model.SessionStateChangedEvent:
		return addStringBytes(size, string(typed.Previous)) && addStringBytes(size, string(typed.Current))
	case *model.SessionStateChangedEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.ConnectivityChangedEvent:
		return addStringBytes(size, typed.Previous) && addStringBytes(size, typed.Current)
	case *model.ConnectivityChangedEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.PeerReachabilityEvent:
		return addStringBytes(size, typed.Peer)
	case *model.PeerReachabilityEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.MessageReceivedEvent:
		return addMessageReceivedBytes(size, typed)
	case *model.MessageReceivedEvent:
		return typed != nil && addMessageReceivedBytes(size, *typed)
	case model.MessageStateEvent:
		return addStringBytes(size, typed.MessageID) && addStringBytes(size, typed.ConversationID) && addStringBytes(size, typed.State)
	case *model.MessageStateEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.PayloadTransferEvent:
		return addStringBytes(size, typed.Handle) && addStringBytes(size, typed.State)
	case *model.PayloadTransferEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.PolicyRejectedEvent:
		return addStringBytes(size, typed.MessageID) && addStringBytes(size, typed.Peer) && addStringBytes(size, typed.Path)
	case *model.PolicyRejectedEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.RPCTimeoutEvent:
		return addStringBytes(size, typed.CommandID)
	case *model.RPCTimeoutEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.QueueCapacityEvent:
		return addStringBytes(size, typed.Queue)
	case *model.QueueCapacityEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	case model.CoreErrorEvent:
		return addModelErrorBytes(size, typed.Err)
	case *model.CoreErrorEvent:
		return typed != nil && addModelErrorBytes(size, typed.Err)
	case model.DiagnosticLogEvent:
		return addStringBytes(size, typed.Level) && addStringBytes(size, typed.Code) && addStringBytes(size, typed.DiagnosticID)
	case *model.DiagnosticLogEvent:
		return typed != nil && addEventValueBytes(size, *typed)
	default:
		return false
	}
}

func addMessageReceivedBytes(size *uint64, value model.MessageReceivedEvent) bool {
	return addStringBytes(size, value.MessageID) && addStringBytes(size, value.ConversationID) &&
		addStringBytes(size, value.FromAgentID) && addStringBytes(size, value.MeshID) &&
		addStringBytes(size, value.Mode) && addStringBytes(size, value.RequestHandle) &&
		addPayloadBytes(size, value.Payload)
}

func addPayloadBytes(size *uint64, payload model.Payload) bool {
	if payload.Value == nil {
		return true
	}
	switch typed := payload.Value.(type) {
	case model.NativePayload:
		return addNativePayloadBytes(size, typed)
	case *model.NativePayload:
		return typed != nil && addNativePayloadBytes(size, *typed)
	case model.HTTPRequestPayload:
		return addHTTPRequestPayloadBytes(size, typed)
	case *model.HTTPRequestPayload:
		return typed != nil && addHTTPRequestPayloadBytes(size, *typed)
	case model.HTTPResponsePayload:
		return addHTTPResponsePayloadBytes(size, typed)
	case *model.HTTPResponsePayload:
		return typed != nil && addHTTPResponsePayloadBytes(size, *typed)
	case model.PayloadHandle:
		return addStringBytes(size, typed.Handle)
	case *model.PayloadHandle:
		return typed != nil && addStringBytes(size, typed.Handle)
	default:
		return false
	}
}

func addNativePayloadBytes(size *uint64, payload model.NativePayload) bool {
	return addStringBytes(size, payload.ContentType) && addStringBytes(size, payload.Path) && addSliceBytes(size, cap(payload.Body), 1)
}

func addHTTPRequestPayloadBytes(size *uint64, payload model.HTTPRequestPayload) bool {
	return addStringBytes(size, payload.Method) && addStringBytes(size, payload.Path) && addStringBytes(size, payload.Query) &&
		addHeaderBytes(size, payload.Headers) && addSliceBytes(size, cap(payload.Body), 1)
}

func addHTTPResponsePayloadBytes(size *uint64, payload model.HTTPResponsePayload) bool {
	if !addStringBytes(size, payload.Reason) || !addHeaderBytes(size, payload.Headers) || !addSliceBytes(size, cap(payload.Body), 1) {
		return false
	}
	return payload.Error == nil || (addSliceBytes(size, 1, applicationErrorObjectBytes) && addStringBytes(size, payload.Error.Code) && addStringBytes(size, payload.Error.Detail) && addStringBytes(size, payload.Error.DetailsJSON))
}

func addHeaderBytes(size *uint64, headers []model.Header) bool {
	if !addSliceBytes(size, cap(headers), headerBackingSlotBytes) {
		return false
	}
	for _, header := range headers {
		if !addStringBytes(size, header.Name) || !addStringBytes(size, header.Value) {
			return false
		}
	}
	return true
}

func addModelErrorBytes(size *uint64, value *model.Error) bool {
	if value == nil {
		return true
	}
	return addStringBytes(size, value.Code) && addStringBytes(size, value.Stage) &&
		addStringBytes(size, value.Location) && addStringBytes(size, value.DiagnosticID)
}

func addStringBytes(size *uint64, value string) bool {
	next, ok := checkedAdd(*size, uint64(len(value)))
	if ok {
		*size = next
	}
	return ok
}

func addSliceBytes(size *uint64, capacity int, width uint64) bool {
	if capacity < 0 {
		return false
	}
	bytes, ok := checkedMultiply(uint64(capacity), width)
	if !ok {
		return false
	}
	next, ok := checkedAdd(*size, bytes)
	if ok {
		*size = next
	}
	return ok
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if left > math.MaxUint64-right {
		return 0, false
	}
	return left + right, true
}

func checkedMultiply(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

// freezeEventDynamicFields runs only after the dispatcher reserves count and
// byte ownership. Every supported pointer or value variant is normalized to
// an independently owned value variant, so an interior pointer cannot retain
// an arbitrarily large caller allocation. Every retained slice has exact
// length capacity and every retained string has an independent backing store.
func freezeEventDynamicFields(event model.Event) model.Event {
	event.ID = strings.Clone(event.ID)
	event.Name = strings.Clone(event.Name)
	// Runtime event timestamps are UTC instants. Canonicalizing here drops any
	// caller-owned custom *time.Location graph instead of retaining hidden
	// location names and transition tables behind time.Time.
	event.CreatedAt = event.CreatedAt.UTC()
	event.Value = freezeEventValue(event.Value)
	return event
}

func freezeEventValue(value model.EventValue) model.EventValue {
	switch typed := value.(type) {
	case model.SessionStateChangedEvent:
		return freezeSessionStateChanged(typed)
	case *model.SessionStateChangedEvent:
		return freezeSessionStateChanged(*typed)
	case model.ConnectivityChangedEvent:
		return freezeConnectivityChanged(typed)
	case *model.ConnectivityChangedEvent:
		return freezeConnectivityChanged(*typed)
	case model.PeerReachabilityEvent:
		return freezePeerReachability(typed)
	case *model.PeerReachabilityEvent:
		return freezePeerReachability(*typed)
	case model.MessageReceivedEvent:
		return freezeMessageReceived(typed)
	case *model.MessageReceivedEvent:
		return freezeMessageReceived(*typed)
	case model.MessageStateEvent:
		return freezeMessageState(typed)
	case *model.MessageStateEvent:
		return freezeMessageState(*typed)
	case model.PayloadTransferEvent:
		return freezePayloadTransfer(typed)
	case *model.PayloadTransferEvent:
		return freezePayloadTransfer(*typed)
	case model.PolicyRejectedEvent:
		return freezePolicyRejected(typed)
	case *model.PolicyRejectedEvent:
		return freezePolicyRejected(*typed)
	case model.RPCTimeoutEvent:
		return model.RPCTimeoutEvent{CommandID: strings.Clone(typed.CommandID)}
	case *model.RPCTimeoutEvent:
		return model.RPCTimeoutEvent{CommandID: strings.Clone(typed.CommandID)}
	case model.QueueCapacityEvent:
		return model.QueueCapacityEvent{Queue: strings.Clone(typed.Queue), Capacity: typed.Capacity}
	case *model.QueueCapacityEvent:
		return model.QueueCapacityEvent{Queue: strings.Clone(typed.Queue), Capacity: typed.Capacity}
	case model.CoreErrorEvent:
		return model.CoreErrorEvent{Err: freezeModelError(typed.Err)}
	case *model.CoreErrorEvent:
		return model.CoreErrorEvent{Err: freezeModelError(typed.Err)}
	case model.DiagnosticLogEvent:
		return freezeDiagnosticLog(typed)
	case *model.DiagnosticLogEvent:
		return freezeDiagnosticLog(*typed)
	default:
		return nil
	}
}

func freezeSessionStateChanged(value model.SessionStateChangedEvent) model.SessionStateChangedEvent {
	return model.SessionStateChangedEvent{Previous: model.LifecycleState(strings.Clone(string(value.Previous))), Current: model.LifecycleState(strings.Clone(string(value.Current)))}
}

func freezeConnectivityChanged(value model.ConnectivityChangedEvent) model.ConnectivityChangedEvent {
	return model.ConnectivityChangedEvent{Previous: strings.Clone(value.Previous), Current: strings.Clone(value.Current)}
}

func freezePeerReachability(value model.PeerReachabilityEvent) model.PeerReachabilityEvent {
	return model.PeerReachabilityEvent{Peer: strings.Clone(value.Peer), Reachable: value.Reachable}
}

func freezeMessageReceived(value model.MessageReceivedEvent) model.MessageReceivedEvent {
	value.MessageID = strings.Clone(value.MessageID)
	value.ConversationID = strings.Clone(value.ConversationID)
	value.FromAgentID = strings.Clone(value.FromAgentID)
	value.MeshID = strings.Clone(value.MeshID)
	value.Mode = strings.Clone(value.Mode)
	value.RequestHandle = strings.Clone(value.RequestHandle)
	value.Payload = freezePayload(value.Payload)
	return value
}

func freezeMessageState(value model.MessageStateEvent) model.MessageStateEvent {
	return model.MessageStateEvent{MessageID: strings.Clone(value.MessageID), ConversationID: strings.Clone(value.ConversationID), State: strings.Clone(value.State)}
}

func freezePayloadTransfer(value model.PayloadTransferEvent) model.PayloadTransferEvent {
	return model.PayloadTransferEvent{Handle: strings.Clone(value.Handle), State: strings.Clone(value.State), Completed: value.Completed, Total: value.Total}
}

func freezePolicyRejected(value model.PolicyRejectedEvent) model.PolicyRejectedEvent {
	return model.PolicyRejectedEvent{MessageID: strings.Clone(value.MessageID), Peer: strings.Clone(value.Peer), Path: strings.Clone(value.Path)}
}

func freezeDiagnosticLog(value model.DiagnosticLogEvent) model.DiagnosticLogEvent {
	return model.DiagnosticLogEvent{Level: strings.Clone(value.Level), Code: strings.Clone(value.Code), DiagnosticID: strings.Clone(value.DiagnosticID)}
}

func freezePayload(payload model.Payload) model.Payload {
	switch typed := payload.Value.(type) {
	case model.NativePayload:
		payload.Value = freezeNativePayload(typed)
	case *model.NativePayload:
		payload.Value = freezeNativePayload(*typed)
	case model.HTTPRequestPayload:
		payload.Value = freezeHTTPRequestPayload(typed)
	case *model.HTTPRequestPayload:
		payload.Value = freezeHTTPRequestPayload(*typed)
	case model.HTTPResponsePayload:
		payload.Value = freezeHTTPResponsePayload(typed)
	case *model.HTTPResponsePayload:
		payload.Value = freezeHTTPResponsePayload(*typed)
	case model.PayloadHandle:
		payload.Value = model.PayloadHandle{Handle: strings.Clone(typed.Handle)}
	case *model.PayloadHandle:
		payload.Value = model.PayloadHandle{Handle: strings.Clone(typed.Handle)}
	}
	return payload
}

func freezeNativePayload(value model.NativePayload) model.NativePayload {
	return model.NativePayload{ContentType: strings.Clone(value.ContentType), Path: strings.Clone(value.Path), Body: cloneBytes(value.Body)}
}

func freezeHTTPRequestPayload(value model.HTTPRequestPayload) model.HTTPRequestPayload {
	return model.HTTPRequestPayload{Method: strings.Clone(value.Method), Path: strings.Clone(value.Path), Query: strings.Clone(value.Query), Headers: cloneHeaders(value.Headers), Body: cloneBytes(value.Body)}
}

func freezeHTTPResponsePayload(value model.HTTPResponsePayload) model.HTTPResponsePayload {
	return model.HTTPResponsePayload{StatusCode: value.StatusCode, Reason: strings.Clone(value.Reason), Headers: cloneHeaders(value.Headers), Body: cloneBytes(value.Body), Error: cloneApplicationError(value.Error)}
}

func cloneApplicationError(value *model.ApplicationError) *model.ApplicationError {
	if value == nil {
		return nil
	}
	return &model.ApplicationError{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}

func freezeModelError(value *model.Error) *model.Error {
	if value == nil {
		return nil
	}
	return &model.Error{Code: strings.Clone(value.Code), Stage: strings.Clone(value.Stage), Retryable: value.Retryable, Location: strings.Clone(value.Location), DiagnosticID: strings.Clone(value.DiagnosticID)}
}

func cloneHeaders(headers []model.Header) []model.Header {
	if len(headers) == 0 {
		return nil
	}
	result := make([]model.Header, len(headers))
	for index, header := range headers {
		result[index] = model.Header{Name: strings.Clone(header.Name), Value: strings.Clone(header.Value)}
	}
	return result
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

// clearOwnedEvent releases and zeroes only dispatcher-frozen ownership. It is
// never called after successful transfer to a consumer.
func clearOwnedEvent(event *model.Event) {
	if event == nil {
		return
	}
	clearEventValue(event.Value)
	*event = model.Event{}
}

func clearEventValue(value model.EventValue) {
	switch typed := value.(type) {
	case model.MessageReceivedEvent:
		clearPayload(&typed.Payload)
	case *model.MessageReceivedEvent:
		if typed != nil {
			clearPayload(&typed.Payload)
			*typed = model.MessageReceivedEvent{}
		}
	case model.CoreErrorEvent:
		clearModelError(typed.Err)
	case *model.CoreErrorEvent:
		if typed != nil {
			clearModelError(typed.Err)
			*typed = model.CoreErrorEvent{}
		}
	case *model.SessionStateChangedEvent:
		if typed != nil {
			*typed = model.SessionStateChangedEvent{}
		}
	case *model.ConnectivityChangedEvent:
		if typed != nil {
			*typed = model.ConnectivityChangedEvent{}
		}
	case *model.PeerReachabilityEvent:
		if typed != nil {
			*typed = model.PeerReachabilityEvent{}
		}
	case *model.MessageStateEvent:
		if typed != nil {
			*typed = model.MessageStateEvent{}
		}
	case *model.PayloadTransferEvent:
		if typed != nil {
			*typed = model.PayloadTransferEvent{}
		}
	case *model.PolicyRejectedEvent:
		if typed != nil {
			*typed = model.PolicyRejectedEvent{}
		}
	case *model.RPCTimeoutEvent:
		if typed != nil {
			*typed = model.RPCTimeoutEvent{}
		}
	case *model.QueueCapacityEvent:
		if typed != nil {
			*typed = model.QueueCapacityEvent{}
		}
	case *model.DiagnosticLogEvent:
		if typed != nil {
			*typed = model.DiagnosticLogEvent{}
		}
	}
}

func clearPayload(payload *model.Payload) {
	if payload == nil {
		return
	}
	switch typed := payload.Value.(type) {
	case model.NativePayload:
		clearBytes(typed.Body)
	case *model.NativePayload:
		if typed != nil {
			clearBytes(typed.Body)
			*typed = model.NativePayload{}
		}
	case model.HTTPRequestPayload:
		clearHeaders(typed.Headers)
		clearBytes(typed.Body)
	case *model.HTTPRequestPayload:
		if typed != nil {
			clearHeaders(typed.Headers)
			clearBytes(typed.Body)
			*typed = model.HTTPRequestPayload{}
		}
	case model.HTTPResponsePayload:
		clearHeaders(typed.Headers)
		clearBytes(typed.Body)
		if typed.Error != nil {
			*typed.Error = model.ApplicationError{}
		}
	case *model.HTTPResponsePayload:
		if typed != nil {
			clearHeaders(typed.Headers)
			clearBytes(typed.Body)
			if typed.Error != nil {
				*typed.Error = model.ApplicationError{}
			}
			*typed = model.HTTPResponsePayload{}
		}
	case *model.PayloadHandle:
		if typed != nil {
			*typed = model.PayloadHandle{}
		}
	}
	*payload = model.Payload{}
}

func clearHeaders(headers []model.Header) {
	for index := range headers {
		headers[index] = model.Header{}
	}
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func clearModelError(value *model.Error) {
	if value != nil {
		*value = model.Error{}
	}
}
