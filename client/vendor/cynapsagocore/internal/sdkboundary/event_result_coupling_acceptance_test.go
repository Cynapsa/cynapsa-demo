package sdkboundary

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestEventNamesAcceptOnlyTheirExactStateAndPayload(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	now := time.Unix(9, 8).UTC()

	messageEvents := []struct {
		name  v1.EventName
		state v1.DeliveryState
	}{
		{name: v1.EventMessageQueued, state: v1.DeliveryStateQueued},
		{name: v1.EventMessageRetried, state: v1.DeliveryStateQueued},
		{name: v1.EventDeliveryReplayed, state: v1.DeliveryStateQueued},
		{name: v1.EventMessageDeduplicated, state: v1.DeliveryStateReady},
		{name: v1.EventDeliveryResumed, state: v1.DeliveryStateReady},
	}
	for _, test := range messageEvents {
		t.Run(string(test.name), func(t *testing.T) {
			mapped, err := adapter.MapEvent(model.Event{ID: "event", Name: string(test.name), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: string(test.state)}})
			if err != nil {
				t.Fatalf("map valid event: %v", err)
			}
			if _, err := adapter.EncodeABIEvent(mapped); err != nil {
				t.Fatalf("encode valid event: %v", err)
			}
			for _, wrong := range []v1.DeliveryState{v1.DeliveryStateReady, v1.DeliveryStateQueued, v1.DeliveryStateBlocked, v1.DeliveryStateFailed} {
				if wrong == test.state {
					continue
				}
				internal := model.Event{ID: "event", Name: string(test.name), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: string(wrong)}}
				if _, err := adapter.MapEvent(internal); !errors.Is(err, ErrMalformedInput) {
					t.Errorf("internal wrong state %q error = %v", wrong, err)
				}
				public := v1.Event{ID: "event", Name: test.name, CreatedAt: now, Payload: v1.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: wrong}}
				if _, err := adapter.EncodeABIEvent(public); !errors.Is(err, ErrMalformedInput) {
					t.Errorf("public wrong state %q error = %v", wrong, err)
				}
			}
		})
	}

	payloadEvents := []struct {
		name  v1.EventName
		state v1.PayloadTransferState
	}{
		{name: v1.EventPayloadTransferStarted, state: v1.PayloadTransferStarted},
		{name: v1.EventPayloadTransferProgress, state: v1.PayloadTransferProgress},
		{name: v1.EventPayloadTransferCompleted, state: v1.PayloadTransferCompleted},
		{name: v1.EventPayloadTransferFailed, state: v1.PayloadTransferFailed},
	}
	for _, test := range payloadEvents {
		t.Run(string(test.name), func(t *testing.T) {
			mapped, err := adapter.MapEvent(model.Event{ID: "event", Name: string(test.name), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(test.state), Completed: 1, Total: 2}})
			if err != nil {
				t.Fatalf("map valid event: %v", err)
			}
			if _, err := adapter.EncodeABIEvent(mapped); err != nil {
				t.Fatalf("encode valid event: %v", err)
			}
			for _, wrong := range []v1.PayloadTransferState{v1.PayloadTransferStarted, v1.PayloadTransferProgress, v1.PayloadTransferCompleted, v1.PayloadTransferFailed} {
				if wrong == test.state {
					continue
				}
				internal := model.Event{ID: "event", Name: string(test.name), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(wrong), Total: 2}}
				if _, err := adapter.MapEvent(internal); !errors.Is(err, ErrMalformedInput) {
					t.Errorf("internal wrong state %q error = %v", wrong, err)
				}
				public := v1.Event{ID: "event", Name: test.name, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: wrong, Total: 2}}
				if _, err := adapter.EncodeABIEvent(public); !errors.Is(err, ErrMalformedInput) {
					t.Errorf("public wrong state %q error = %v", wrong, err)
				}
			}
		})
	}
}

func TestRetiredDeliveryEventsAndUnknownStateFailClosed(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(9, 8).UTC()
	for _, name := range []string{"delivery.ordering_blocked", "delivery.unknown"} {
		internal := model.Event{ID: "event", Name: name, CreatedAt: now, Value: model.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: "unknown"}}
		if _, err := adapter.MapEvent(internal); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("retired internal event %q error = %v", name, err)
		}
		public := v1.Event{ID: "event", Name: v1.EventName(name), CreatedAt: now, Payload: v1.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: v1.DeliveryState("unknown")}}
		if _, err := adapter.EncodeABIEvent(public); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("retired public event %q error = %v", name, err)
		}
	}
	unknown := model.ConversationStatus{ConversationID: "conversation", MeshID: "mesh", Peer: "peer", DeliveryState: "unknown"}
	if _, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: unknown}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("private ambiguous state escaped through conversation status: %v", err)
	}
}

func TestEventEncodingRejectsInvalidStateAssociatedData(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	now := time.Unix(9, 8).UTC()
	tests := []v1.Event{
		{ID: "event", Name: v1.EventPayloadTransferProgress, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: "payload-1", State: v1.PayloadTransferProgress, Completed: 1, Total: 2}},
		{ID: "event", Name: v1.EventPayloadTransferProgress, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: v1.PayloadTransferProgress, Completed: 3, Total: 2}},
		{ID: "event", Name: v1.EventPayloadTransferProgress, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: v1.PayloadTransferProgress, Total: maxCanonicalPayloadSize + 1}},
		{ID: "event", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Mode: v1.MessageModeRequest, Payload: v1.Payload{Value: v1.NativePayload{Path: "/"}}}},
		{ID: "event", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Mode: v1.MessageModeOneWay, RequestHandle: validRequestHandle, Payload: v1.Payload{Value: v1.NativePayload{Path: "/"}}}},
	}
	for index, event := range tests {
		if _, err := adapter.EncodeABIEvent(event); err == nil {
			t.Errorf("invalid associated data case %d was encoded", index)
		}
	}
}

func TestCompletionResultTypesAreDeterministicallyCoupledToClosedVariants(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	tests := []struct {
		want   string
		result v1.Result
	}{
		{want: "empty", result: v1.EmptyResult{}},
		{want: "auth", result: v1.AuthResult{AgentID: "agent", MeshID: "mesh", Personality: v1.SDKPersonalityNative}},
		{want: "send", result: v1.SendResult{MessageID: "message", ConversationID: "conversation", Accepted: true}},
		{want: "payload_handle", result: v1.PayloadHandleResult{Handle: validPayloadHandle, Size: 1, EOF: true, Chunk: []byte{0}}},
		{want: "completion_channel", result: v1.CompletionChannelResult{ChannelID: "channel", MaxInFlight: 1}},
	}
	for _, test := range tests {
		encoded, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: test.result})
		if err != nil {
			t.Fatalf("encode %T: %v", test.result, err)
		}
		var wire struct {
			ResultType string `json:"result_type"`
		}
		if err := json.Unmarshal(encoded, &wire); err != nil {
			t.Fatal(err)
		}
		if wire.ResultType != test.want {
			t.Errorf("%T result_type = %q, want %q", test.result, wire.ResultType, test.want)
		}
	}
}

func TestCompletionEncodingRejectsInvalidResultAssociatedData(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	invalid := []v1.Result{
		v1.Status{Lifecycle: "private_state", Connectivity: v1.ConnectivityAvailable, Personality: v1.SDKPersonalityNative},
		v1.AuthResult{AgentID: "agent", MeshID: "mesh", Personality: v1.SDKPersonalityUnset},
		v1.SendResult{MessageID: "message", ConversationID: "conversation", Accepted: false},
		v1.DeliveryQueueStatus{Queued: maxQueueCapacity + 1},
		v1.PayloadHandleResult{Handle: "payload-1", Size: 1},
		v1.PayloadHandleResult{Handle: validPayloadHandle, Size: maxCanonicalPayloadSize + 1},
		v1.PayloadHandleResult{Handle: validPayloadHandle, Size: 1, Chunk: []byte{0, 1}},
		v1.ConversationStatus{ConversationID: "conversation", MeshID: "mesh", Peer: "peer", DeliveryState: "private_state"},
		v1.PeerStatus{Peer: "peer", Connectivity: "private_state"},
		v1.ConnectivityStatus{State: "private_state"},
		v1.EventResult{Event: v1.Event{Name: v1.EventCommandQueueFull, Payload: v1.QueueCapacityEvent{Queue: v1.QueueNameCommand, Capacity: 1}}},
	}
	for index, result := range invalid {
		if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: result}); err == nil {
			t.Errorf("invalid result case %d (%T) was encoded", index, result)
		}
	}
}

func TestABIEncodersRejectUnnormalizedPublicErrorValues(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	private := &v1.Error{
		Code:         "private_error",
		Message:      "raw dependency failure at /private/path",
		Retryable:    true,
		Stage:        "private_stage",
		Location:     "private_location",
		DiagnosticID: "not-a-diagnostic-id",
	}
	if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: false, Error: private}); err == nil {
		t.Error("completion encoder accepted an unnormalized error")
	}
	if _, err := adapter.EncodeABIAdmission(v1.Admission{CommandID: "command", Accepted: false, Error: private}); err == nil {
		t.Error("admission encoder accepted an unnormalized error")
	}
	if _, err := adapter.EncodeABIEvent(v1.Event{ID: "event", Name: v1.EventCoreError, CreatedAt: time.Unix(1, 0).UTC(), Payload: v1.CoreErrorEvent{Error: *private}}); err == nil {
		t.Error("event encoder accepted an unnormalized error")
	}

	knownCodeRawMessage := &v1.Error{
		Code:     v1.ErrorCodeCore,
		Message:  "raw dependency failure despite known code",
		Stage:    v1.ErrorStageCommand,
		Location: v1.ErrorLocationLocal,
	}
	if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: false, Error: knownCodeRawMessage}); err == nil {
		t.Error("completion encoder accepted a non-template message for a known error code")
	}
	if _, err := adapter.EncodeABIEvent(v1.Event{
		ID: "event", Name: v1.EventDiagnosticLog, CreatedAt: time.Unix(1, 0).UTC(),
		Payload: v1.DiagnosticLogEvent{Level: v1.DiagnosticLogError, Code: v1.DiagnosticLogConnectivity, Message: "raw dependency diagnostic"},
	}); err == nil {
		t.Error("event encoder accepted a non-template diagnostic log message")
	}
}
