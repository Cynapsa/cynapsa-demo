package sdkboundary

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestEveryFrozenResultMapsAndEncodes(t *testing.T) {
	adapter, _ := New()
	payload := model.Payload{Value: model.NativePayload{Path: "/", Body: []byte("value")}}
	status := model.Status{Lifecycle: model.LifecycleReady, ConnectivityDetail: "available", Personality: "native"}
	values := []model.ResultValue{
		model.EmptyResult{}, model.CapabilitiesResult{Commands: []string{string(v1.CommandCoreStatus)}, Features: []string{string(v1.CapabilityRPC)}}, model.StatusResult{Status: status}, model.ConfigResult{Config: model.RuntimeConfig{CommandTimeout: 30 * time.Second, RPCTimeout: 30 * time.Second, QueueLimit: 1, PayloadLimit: 1}},
		model.AuthResult{AgentID: "agent", MeshID: "mesh", Personality: "native"}, model.AgentIDResult{AgentID: "agent"}, model.MeshListResult{Meshes: []model.MeshSummary{{MeshID: "m", Active: true}}},
		model.AddressMappingsResult{Mappings: []model.AddressMapping{{VirtualOrigin: "https://a.test", Recipient: "a"}}}, model.AddressResolution{Recipient: "a", Path: "/"},
		model.SendResult{MessageID: "m", ConversationID: "c", Accepted: true}, model.ResponseResult{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Payload: payload}, model.EventResult{Event: model.Event{ID: "e", Name: string(v1.EventCommandQueueFull), CreatedAt: time.Unix(1, 0), Value: model.QueueCapacityEvent{Queue: string(v1.QueueNameCommand), Capacity: 1}}}, model.DeliveryQueueStatus{}, model.PayloadHandleResult{Handle: validPayloadHandle},
		model.ConversationStatus{ConversationID: "c", MeshID: "mesh", Peer: "a", DeliveryState: string(v1.DeliveryStateReady)}, model.ConversationListResult{}, model.PolicyResult{}, model.PeerStatus{Peer: "a"}, model.ConnectivityStatus{},
		model.DiagnosticSnapshot{Status: status}, model.CoreInitResult{SessionID: "s"}, model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: 1}, model.EventSinkResult{SinkID: "sink"},
	}
	goldens := []string{
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"empty","result":{}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"capabilities","result":{"commands":["core.status"],"features":["rpc"]}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"status","result":{"lifecycle":"ready","connectivity":"available","personality":"native","agent_id":"","mesh_id":"","mesh_endpoint":"","queued_message_count":0}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"config","result":{"command_timeout_ms":30000,"rpc_timeout_ms":30000,"queue_limit":1,"payload_limit":1}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"auth","result":{"agent_id":"agent","mesh_id":"mesh","agent_instance_id":"","personality":"native"}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"agent_id","result":{"agent_id":"agent"}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"mesh_list","result":{"meshes":[{"mesh_id":"m","active":true}]}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"address_mappings","result":{"mappings":[{"virtual_origin":"https://a.test","recipient":"a"}]}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"address_resolution","result":{"recipient":"a","path":"/","query":""}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"send","result":{"message_id":"m","conversation_id":"c","accepted":true}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"response","result":{"message_id":"m","conversation_id":"c","from_agent_id":"a","mesh_id":"mesh","payload":{"native":{"content_type":"","path":"/","body":"dmFsdWU="}}}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"event","result":{"abi_version":1,"event_id":"e","event_name":"command.queue_full","created_at":"1970-01-01T00:00:01Z","payload":{"queue":"command","capacity":1}}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"delivery_queue_status","result":{"queued":0,"paused":false}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"payload_handle","result":{"handle":"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","size":0,"eof":false,"chunk":null}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"conversation_status","result":{"conversation_id":"c","mesh_id":"mesh","peer":"a","delivery_state":"ready","queued_message_count":0,"blocked":false}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"conversation_list","result":{"conversations":[]}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"policy","result":{"rules":[],"allowed":false}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"peer_status","result":{"peer":"a","connectivity":"unknown","reachable":false,"recovery_in_progress":false}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"connectivity_status","result":{"state":"unknown"}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"diagnostic_snapshot","result":{"status":{"lifecycle":"ready","connectivity":"available","personality":"native","agent_id":"","mesh_id":"","mesh_endpoint":"","queued_message_count":0},"command_queue_depth":0,"event_queue_depth":0,"peer_count":0,"queued_message_count":0,"pending_rpc_count":0,"payload_transfer_count":0}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"core_init","result":{"sdk_session_id":"s"}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"completion_channel","result":{"channel_id":"channel","max_in_flight":1}}`,
		`{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"event_sink","result":{"sink_id":"sink"}}`,
	}
	if len(values) != len(goldens) {
		t.Fatalf("result fixtures=%d goldens=%d", len(values), len(goldens))
	}
	catalog := frozenResultCatalog()
	if len(values) != len(catalog) {
		t.Fatalf("mapped result fixtures=%d closed catalog=%d", len(values), len(catalog))
	}
	seenPrivate := make(map[string]bool, len(values))
	vectors := make([]discriminatorVector, 0, len(values))
	for _, value := range values {
		name := reflect.TypeOf(value).Name()
		if _, known := catalog[name]; !known || seenPrivate[name] {
			t.Fatalf("missing or duplicate private result fixture %s", name)
		}
		seenPrivate[name] = true
	}
	for index, value := range values {
		completion, err := adapter.MapCompletion(model.Result{CommandID: "command-1", Value: value})
		if err != nil {
			t.Errorf("map %T: %v", value, err)
			continue
		}
		encoded, err := adapter.EncodeABICompletion(completion)
		if err != nil {
			t.Errorf("encode %T: %v", value, err)
			continue
		}
		if string(encoded) != goldens[index] {
			t.Errorf("golden %T\ngot  %s\nwant %s", value, encoded, goldens[index])
		}
		var discriminator struct {
			ResultType string `json:"result_type"`
		}
		if err := json.Unmarshal(encoded, &discriminator); err != nil {
			t.Fatal(err)
		}
		vectors = append(vectors, discriminatorVector{Name: discriminator.ResultType, JSON: json.RawMessage(append([]byte(nil), encoded...))})
		if forbidden, found := forbiddenPublicVocabulary(string(encoded)); found {
			t.Errorf("unsafe result fixture %T contains %s: %s", value, forbidden, encoded)
		}
	}
	assertConformanceVectors(t, "results.json", vectors)
}

func TestEveryFrozenEventMapsAndEncodes(t *testing.T) {
	adapter, _ := New()
	now := time.Unix(1, 0).UTC()
	payload := model.Payload{Value: model.NativePayload{Path: "/", Body: []byte("value")}}
	tests := []model.Event{
		{ID: "e1", Name: string(v1.EventSessionStateChanged), CreatedAt: now, Value: model.SessionStateChangedEvent{Previous: model.LifecycleCreated, Current: model.LifecycleReady}},
		{ID: "e2", Name: string(v1.EventConnectivityStateChanged), CreatedAt: now, Value: model.ConnectivityChangedEvent{Previous: "unavailable", Current: "available"}},
		{ID: "e3", Name: string(v1.EventPeerReachable), CreatedAt: now, Value: model.PeerReachabilityEvent{Peer: "a", Reachable: true}},
		{ID: "e4", Name: string(v1.EventPeerUnreachable), CreatedAt: now, Value: model.PeerReachabilityEvent{Peer: "a", Reachable: false}},
		{ID: "e5", Name: string(v1.EventMessageReceived), CreatedAt: now, Value: model.MessageReceivedEvent{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: string(v1.MessageModeOneWay), Payload: payload}},
		{ID: "e6", Name: string(v1.EventMessageQueued), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateQueued)}},
		{ID: "e7", Name: string(v1.EventMessageRetried), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateQueued)}},
		{ID: "e8", Name: string(v1.EventMessageDeduplicated), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateReady)}},
		{ID: "e9", Name: string(v1.EventDeliveryResumed), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateReady)}},
		{ID: "e10", Name: string(v1.EventDeliveryReplayed), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateQueued)}},
		{ID: "e12", Name: string(v1.EventPayloadTransferStarted), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(v1.PayloadTransferStarted), Total: 10}},
		{ID: "e13", Name: string(v1.EventPayloadTransferProgress), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(v1.PayloadTransferProgress), Completed: 5, Total: 10}},
		{ID: "e14", Name: string(v1.EventPayloadTransferCompleted), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(v1.PayloadTransferCompleted), Completed: 10, Total: 10}},
		{ID: "e15", Name: string(v1.EventPayloadTransferFailed), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(v1.PayloadTransferFailed), Completed: 5, Total: 10}},
		{ID: "e16", Name: string(v1.EventPolicyRejected), CreatedAt: now, Value: model.PolicyRejectedEvent{MessageID: "m", Peer: "a", Path: "/"}},
		{ID: "e17", Name: string(v1.EventRPCTimeout), CreatedAt: now, Value: model.RPCTimeoutEvent{CommandID: "c"}},
		{ID: "e18", Name: string(v1.EventCommandQueueFull), CreatedAt: now, Value: model.QueueCapacityEvent{Queue: string(v1.QueueNameCommand), Capacity: 1}},
		{ID: "e19", Name: string(v1.EventEventQueueFull), CreatedAt: now, Value: model.QueueCapacityEvent{Queue: string(v1.QueueNameEvent), Capacity: 1}},
		{ID: "e20", Name: string(v1.EventCoreError), CreatedAt: now, Value: model.CoreErrorEvent{Err: &model.Error{Code: "queue_full"}}},
		{ID: "e21", Name: string(v1.EventDiagnosticLog), CreatedAt: now, Value: model.DiagnosticLogEvent{Level: string(v1.DiagnosticLogInfo), Code: "core_state", DiagnosticID: validDiagnosticID}},
	}
	goldens := []string{
		`{"abi_version":1,"event_id":"e1","event_name":"session.state_changed","created_at":"1970-01-01T00:00:01Z","payload":{"previous":"created","current":"ready"}}`,
		`{"abi_version":1,"event_id":"e2","event_name":"connectivity.state_changed","created_at":"1970-01-01T00:00:01Z","payload":{"previous":"unavailable","current":"available"}}`,
		`{"abi_version":1,"event_id":"e3","event_name":"peer.reachable","created_at":"1970-01-01T00:00:01Z","payload":{"peer":"a","reachable":true}}`,
		`{"abi_version":1,"event_id":"e4","event_name":"peer.unreachable","created_at":"1970-01-01T00:00:01Z","payload":{"peer":"a","reachable":false}}`,
		`{"abi_version":1,"event_id":"e5","event_name":"message.received","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","from_agent_id":"a","mesh_id":"mesh","mode":"msg","request_handle":"","payload":{"native":{"content_type":"","path":"/","body":"dmFsdWU="}}}}`,
		`{"abi_version":1,"event_id":"e6","event_name":"message.queued","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","state":"queued"}}`,
		`{"abi_version":1,"event_id":"e7","event_name":"message.retried","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","state":"queued"}}`,
		`{"abi_version":1,"event_id":"e8","event_name":"message.deduplicated","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","state":"ready"}}`,
		`{"abi_version":1,"event_id":"e9","event_name":"delivery.resumed","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","state":"ready"}}`,
		`{"abi_version":1,"event_id":"e10","event_name":"delivery.replayed","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","conversation_id":"c","state":"queued"}}`,
		`{"abi_version":1,"event_id":"e12","event_name":"payload.transfer_started","created_at":"1970-01-01T00:00:01Z","payload":{"handle":"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","state":"started","completed":0,"total":10}}`,
		`{"abi_version":1,"event_id":"e13","event_name":"payload.transfer_progress","created_at":"1970-01-01T00:00:01Z","payload":{"handle":"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","state":"progress","completed":5,"total":10}}`,
		`{"abi_version":1,"event_id":"e14","event_name":"payload.transfer_completed","created_at":"1970-01-01T00:00:01Z","payload":{"handle":"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","state":"completed","completed":10,"total":10}}`,
		`{"abi_version":1,"event_id":"e15","event_name":"payload.transfer_failed","created_at":"1970-01-01T00:00:01Z","payload":{"handle":"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","state":"failed","completed":5,"total":10}}`,
		`{"abi_version":1,"event_id":"e16","event_name":"policy.rejected","created_at":"1970-01-01T00:00:01Z","payload":{"message_id":"m","peer":"a","path":"/"}}`,
		`{"abi_version":1,"event_id":"e17","event_name":"rpc.timeout","created_at":"1970-01-01T00:00:01Z","payload":{"command_id":"c"}}`,
		`{"abi_version":1,"event_id":"e18","event_name":"command.queue_full","created_at":"1970-01-01T00:00:01Z","payload":{"queue":"command","capacity":1}}`,
		`{"abi_version":1,"event_id":"e19","event_name":"event.queue_full","created_at":"1970-01-01T00:00:01Z","payload":{"queue":"event","capacity":1}}`,
		`{"abi_version":1,"event_id":"e20","event_name":"core.error","created_at":"1970-01-01T00:00:01Z","payload":{"error":{"code":"queue_full","message":"The local queue is full","retryable":false,"stage":"command","local_or_remote":"local"}}}`,
		`{"abi_version":1,"event_id":"e21","event_name":"diagnostics.log","created_at":"1970-01-01T00:00:01Z","payload":{"level":"info","code":"core_state","message":"Core state changed","diagnostic_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}`,
	}
	if len(tests) != len(goldens) {
		t.Fatalf("event fixtures=%d goldens=%d", len(tests), len(goldens))
	}
	seen := make(map[v1.EventName]bool, len(tests))
	vectors := make([]discriminatorVector, 0, len(tests))
	for index, input := range tests {
		event, err := adapter.MapEvent(input)
		if err != nil {
			t.Errorf("map %s: %v", input.Name, err)
			continue
		}
		encoded, err := adapter.EncodeABIEvent(event)
		if err != nil {
			t.Errorf("encode %s: %v", input.Name, err)
			continue
		}
		if string(encoded) != goldens[index] {
			t.Errorf("golden %s\ngot  %s\nwant %s", input.Name, encoded, goldens[index])
		}
		if forbidden, found := forbiddenPublicVocabulary(string(encoded)); found {
			t.Errorf("unsafe fixture %s contains %s: %s", input.Name, forbidden, encoded)
		}
		seen[event.Name] = true
		vectors = append(vectors, discriminatorVector{Name: string(event.Name), JSON: json.RawMessage(append([]byte(nil), encoded...))})
	}
	if len(seen) != len(tests) {
		t.Fatalf("encoded discriminators=%d want=%d", len(seen), len(tests))
	}
	assertConformanceVectors(t, "events.json", vectors)
}

func TestDiagnosticLogEventGoldenIsNormalized(t *testing.T) {
	adapter, _ := New()
	event, err := adapter.MapEvent(model.Event{ID: "event-1", Name: string(v1.EventDiagnosticLog), CreatedAt: time.Unix(1, 0).UTC(), Value: model.DiagnosticLogEvent{Level: "info", Code: "core_state", DiagnosticID: validDiagnosticID}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := adapter.EncodeABIEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"abi_version":1,"event_id":"event-1","event_name":"diagnostics.log","created_at":"1970-01-01T00:00:01Z","payload":{"level":"info","code":"core_state","message":"Core state changed","diagnostic_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}`
	if string(encoded) != want {
		t.Fatalf("golden=%s\nwant=%s", encoded, want)
	}
}

func TestUnknownInternalVariantsFailClosed(t *testing.T) {
	adapter, _ := New()
	status := adapter.MapStatus(model.Status{Lifecycle: "new-private-state", ConnectivityDetail: "private-detail", Personality: "private-personality", QueuedMessageCount: maxQueueCapacity + 1})
	if status.Lifecycle != v1.LifecycleFailed || status.Connectivity != v1.ConnectivityUnknown || status.Personality != v1.SDKPersonalityUnset || status.QueuedMessageCount != 0 {
		t.Fatalf("unsafe fallback: %#v", status)
	}
	if _, err := adapter.MapEvent(model.Event{ID: "e", Name: "private.event", CreatedAt: time.Now(), Value: model.QueueCapacityEvent{}}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("unknown event error=%v", err)
	}
	if _, err := adapter.MapEvent(model.Event{ID: "e", Name: string(v1.EventDiagnosticLog), CreatedAt: time.Now(), Value: model.DiagnosticLogEvent{Level: "info", Code: "do-not-leak-private-cause"}}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("private diagnostic code error=%v", err)
	}
}

func TestEventNamesRejectMismatchedSemanticStatesInMapperAndEncoder(t *testing.T) {
	adapter, _ := New()
	now := time.Unix(1, 0).UTC()
	internal := []model.Event{
		{ID: "e1", Name: string(v1.EventMessageDeduplicated), CreatedAt: now, Value: model.MessageStateEvent{MessageID: "m", ConversationID: "c", State: string(v1.DeliveryStateQueued)}},
		{ID: "e3", Name: string(v1.EventPayloadTransferCompleted), CreatedAt: now, Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: string(v1.PayloadTransferStarted), Total: 1}},
	}
	for _, event := range internal {
		if _, err := adapter.MapEvent(event); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("mapper accepted %s mismatch: %v", event.Name, err)
		}
	}

	public := []v1.Event{
		{ID: "e1", Name: v1.EventPeerReachable, CreatedAt: now, Payload: v1.PeerReachabilityEvent{Peer: "a", Reachable: false}},
		{ID: "e2", Name: v1.EventMessageRetried, CreatedAt: now, Payload: v1.MessageStateEvent{MessageID: "m", ConversationID: "c", State: v1.DeliveryStateReady}},
		{ID: "e3", Name: v1.EventPayloadTransferFailed, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: v1.PayloadTransferCompleted, Total: 1}},
		{ID: "e4", Name: v1.EventCommandQueueFull, CreatedAt: now, Payload: v1.QueueCapacityEvent{Queue: v1.QueueNameEvent, Capacity: 1}},
	}
	for _, event := range public {
		if _, err := adapter.EncodeABIEvent(event); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("encoder accepted %s mismatch: %v", event.Name, err)
		}
	}
}

func TestDirectEventEncodingRejectsInvalidAssociatedData(t *testing.T) {
	adapter, _ := New()
	now := time.Unix(1, 0).UTC()
	payload := v1.Payload{Value: v1.NativePayload{Path: "/", Body: []byte("value")}}
	tests := []struct {
		name   string
		event  v1.Event
		target error
	}{
		{name: "malformed payload handle", event: v1.Event{ID: "e1", Name: v1.EventPayloadTransferStarted, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: "payload-1", State: v1.PayloadTransferStarted}}, target: ErrInvalidHandle},
		{name: "wrong-class payload handle", event: v1.Event{ID: "e2", Name: v1.EventPayloadTransferStarted, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: v1.PayloadHandle(validRequestHandle), State: v1.PayloadTransferStarted}}, target: ErrInvalidHandle},
		{name: "completed exceeds total", event: v1.Event{ID: "e3", Name: v1.EventPayloadTransferProgress, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: v1.PayloadTransferProgress, Completed: 2, Total: 1}}, target: ErrMalformedInput},
		{name: "total exceeds canonical maximum", event: v1.Event{ID: "e4", Name: v1.EventPayloadTransferProgress, CreatedAt: now, Payload: v1.PayloadTransferEvent{Handle: validPayloadHandle, State: v1.PayloadTransferProgress, Total: maxCanonicalPayloadSize + 1}}, target: ErrMalformedInput},
		{name: "rpc request missing handle", event: v1.Event{ID: "e5", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: v1.MessageModeRequest, Payload: payload}}, target: ErrMalformedInput},
		{name: "one-way request has handle", event: v1.Event{ID: "e6", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: v1.MessageModeOneWay, RequestHandle: validRequestHandle, Payload: payload}}, target: ErrMalformedInput},
		{name: "rpc wrong-class handle", event: v1.Event{ID: "e7", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: v1.MessageModeRequest, RequestHandle: v1.RequestHandle(validPayloadHandle), Payload: payload}}, target: ErrInvalidHandle},
		{name: "invalid lifecycle", event: v1.Event{ID: "e8", Name: v1.EventSessionStateChanged, CreatedAt: now, Payload: v1.SessionStateChangedEvent{Previous: "private", Current: v1.LifecycleReady}}, target: ErrMalformedInput},
		{name: "invalid connectivity", event: v1.Event{ID: "e9", Name: v1.EventConnectivityStateChanged, CreatedAt: now, Payload: v1.ConnectivityChangedEvent{Previous: "private", Current: v1.ConnectivityAvailable}}, target: ErrMalformedInput},
		{name: "invalid policy path", event: v1.Event{ID: "e10", Name: v1.EventPolicyRejected, CreatedAt: now, Payload: v1.PolicyRejectedEvent{MessageID: "m", Peer: "a", Path: "relative"}}, target: ErrMalformedInput},
		{name: "empty rpc command id", event: v1.Event{ID: "e11", Name: v1.EventRPCTimeout, CreatedAt: now, Payload: v1.RPCTimeoutEvent{}}, target: ErrMalformedInput},
		{name: "unnormalized core error", event: v1.Event{ID: "e12", Name: v1.EventCoreError, CreatedAt: now, Payload: v1.CoreErrorEvent{Error: v1.Error{Code: v1.ErrorCodeCore, Message: "private", Stage: v1.ErrorStageCommand, Location: v1.ErrorLocationLocal}}}, target: ErrMalformedInput},
		{name: "unnormalized diagnostic message", event: v1.Event{ID: "e13", Name: v1.EventDiagnosticLog, CreatedAt: now, Payload: v1.DiagnosticLogEvent{Level: v1.DiagnosticLogInfo, Code: v1.DiagnosticLogCoreState, Message: "private"}}, target: ErrMalformedInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.EncodeABIEvent(test.event)
			if !errors.Is(err, test.target) {
				t.Fatalf("error=%v want %v", err, test.target)
			}
		})
	}
}

func TestDirectCompletionEncodingRejectsInvalidAssociatedData(t *testing.T) {
	adapter, _ := New()
	invalid := []v1.Result{
		v1.Status{Lifecycle: "private_state", Connectivity: v1.ConnectivityAvailable, Personality: v1.SDKPersonalityNative},
		v1.AuthResult{AgentID: "agent", MeshID: "mesh", Personality: v1.SDKPersonalityUnset},
		v1.SendResult{MessageID: "message", ConversationID: "conversation", Accepted: false},
		v1.DeliveryQueueStatus{Queued: maxQueueCapacity + 1},
		v1.PayloadHandleResult{Handle: "payload-1", Size: 1},
		v1.PayloadHandleResult{Handle: v1.PayloadHandle(validRequestHandle), Size: 1},
		v1.PayloadHandleResult{Handle: validPayloadHandle, Size: maxCanonicalPayloadSize + 1},
		v1.PayloadHandleResult{Handle: validPayloadHandle, Size: 1, Chunk: []byte{0, 1}},
		v1.PayloadHandleResult{Handle: validPayloadHandle, Size: maxPayloadChunkBytes + 1, Chunk: make([]byte, maxPayloadChunkBytes+1)},
		v1.ConversationStatus{ConversationID: "conversation", MeshID: "mesh", Peer: "peer", DeliveryState: "private_state"},
		v1.ConversationListResult{Conversations: []v1.ConversationStatus{{ConversationID: "conversation", MeshID: "mesh", Peer: "peer", DeliveryState: "private_state"}}},
		v1.PeerStatus{Peer: "peer", Connectivity: "private_state"},
		v1.ConnectivityStatus{State: "private_state"},
		v1.DiagnosticSnapshot{Status: v1.Status{Lifecycle: v1.LifecycleReady, Connectivity: v1.ConnectivityAvailable, Personality: v1.SDKPersonalityNative}, PeerCount: maxQueueCapacity + 1},
		v1.EventResult{Event: v1.Event{Name: v1.EventCommandQueueFull, Payload: v1.QueueCapacityEvent{Queue: v1.QueueNameCommand, Capacity: 1}}},
	}
	for index, result := range invalid {
		if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: result}); err == nil {
			t.Errorf("invalid result case %d (%T) was encoded", index, result)
		}
	}
}

func TestDirectEncodersRejectUnnormalizedPublicErrors(t *testing.T) {
	adapter, _ := New()
	now := time.Unix(1, 0).UTC()
	invalid := []*v1.Error{
		{Code: "private_error", Message: "raw dependency failure at /private/path", Stage: "private_stage", Location: "private_location", DiagnosticID: "not-a-diagnostic-id"},
		{Code: v1.ErrorCodeCore, Message: "raw dependency failure despite known code", Stage: v1.ErrorStageCommand, Location: v1.ErrorLocationLocal},
		{Code: v1.ErrorCodeCore, Message: "An internal core error occurred", Stage: v1.ErrorStageCommand, Location: v1.ErrorLocationLocal, DiagnosticID: "not-a-diagnostic-id"},
	}
	for index, publicError := range invalid {
		if _, err := adapter.EncodeABIAdmission(v1.Admission{CommandID: "command", Error: publicError}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("admission error case %d=%v", index, err)
		}
		if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", Error: publicError}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("completion error case %d=%v", index, err)
		}
		if _, err := adapter.EncodeABIEvent(v1.Event{ID: "event", Name: v1.EventCoreError, CreatedAt: now, Payload: v1.CoreErrorEvent{Error: *publicError}}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("event error case %d=%v", index, err)
		}
	}
	if _, err := adapter.EncodeABIEvent(v1.Event{ID: "event", Name: v1.EventDiagnosticLog, CreatedAt: now, Payload: v1.DiagnosticLogEvent{Level: v1.DiagnosticLogError, Code: v1.DiagnosticLogConnectivity, Message: "raw dependency diagnostic"}}); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("raw diagnostic error=%v", err)
	}
}

func TestStatusResultHasAnExactDeterministicGolden(t *testing.T) {
	adapter, _ := New()
	data, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command-1", OK: true, Result: v1.Status{Lifecycle: v1.LifecycleReady, Connectivity: v1.ConnectivityAvailable, Personality: v1.SDKPersonalityNative}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"status","result":{"lifecycle":"ready","connectivity":"available","personality":"native","agent_id":"","mesh_id":"","mesh_endpoint":"","queued_message_count":0}}`
	if string(data) != want {
		t.Fatalf("golden=%s\nwant=%s", data, want)
	}
	if forbidden, found := forbiddenPublicVocabulary(string(data)); found {
		t.Fatalf("serialized fixture exposes %s", forbidden)
	}
}

func TestErrorMappingIsTotalAndCauseFree(t *testing.T) {
	adapter, _ := New()
	codes := map[string]v1.ErrorCode{"sdk_error": v1.ErrorCodeSDK, "command_error": v1.ErrorCodeCommand, "malformed_input": v1.ErrorCodeMalformedInput, "unsupported_version": v1.ErrorCodeUnsupportedVersion, "invalid_handle": v1.ErrorCodeInvalidHandle, "queue_full": v1.ErrorCodeQueueFull, "request_cancelled": v1.ErrorCodeRequestCancelled, "authentication_failed": v1.ErrorCodeAuthenticationFailed, "credential_missing": v1.ErrorCodeCredentialMissing, "authorization_rejected": v1.ErrorCodeAuthorizationRejected, "connectivity_unavailable": v1.ErrorCodeConnectivityUnavailable, "peer_unreachable": v1.ErrorCodePeerUnreachable, "payload_transfer_failed": v1.ErrorCodePayloadTransferFailed, "payload_too_large": v1.ErrorCodePayloadTooLarge, "payload_integrity_failed": v1.ErrorCodePayloadIntegrityFailed, "delivery_timeout": v1.ErrorCodeDeliveryTimeout, "duplicate_conflict": v1.ErrorCodeDuplicateConflict, "handler_error": v1.ErrorCodeHandler, "rpc_timeout": v1.ErrorCodeRPCTimeout, "shutdown_in_progress": v1.ErrorCodeShutdownInProgress, "shutdown_timeout": v1.ErrorCodeShutdownTimeout, "core_error": v1.ErrorCodeCore, "unknown_private_code": v1.ErrorCodeCore}
	for code, expected := range codes {
		private := "do-not-leak-" + code
		mapped := adapter.MapError(&model.Error{Code: code, Stage: "private-stage", Location: "private-location", Cause: errors.New(private)})
		if mapped.Code != expected || mapped.Message == "" || mapped.Stage != v1.ErrorStageCommand || mapped.Location != v1.ErrorLocationLocal || strings.Contains(mapped.Message, private) {
			t.Errorf("unsafe mapping for %s: %#v", code, mapped)
		}
	}
}

func TestEveryPublicErrorStageAndLifecycleHasAnExplicitMapping(t *testing.T) {
	adapter, _ := New()
	stages := map[string]v1.ErrorStage{"sdk": v1.ErrorStageSDK, "command": v1.ErrorStageCommand, "auth": v1.ErrorStageAuth, "policy": v1.ErrorStagePolicy, "connectivity": v1.ErrorStageConnectivity, "delivery": v1.ErrorStageDelivery, "payload": v1.ErrorStagePayload, "handler": v1.ErrorStageHandler, "rpc": v1.ErrorStageRPC, "shutdown": v1.ErrorStageShutdown}
	for private, expected := range stages {
		if got := adapter.MapError(&model.Error{Stage: private}).Stage; got != expected {
			t.Errorf("stage %s=%s want=%s", private, got, expected)
		}
	}
	lifecycles := map[model.LifecycleState]v1.LifecycleState{model.LifecycleCreated: v1.LifecycleCreated, model.LifecycleAuthenticating: v1.LifecycleConnecting, model.LifecycleMeshConnected: v1.LifecycleConnecting, model.LifecycleDurableReady: v1.LifecycleConnecting, model.LifecyclePeerLinkBuilding: v1.LifecycleConnecting, model.LifecycleReady: v1.LifecycleReady, model.LifecycleDegraded: v1.LifecycleDegraded, model.LifecycleClosing: v1.LifecycleClosing, model.LifecycleClosed: v1.LifecycleClosed, model.LifecycleFailed: v1.LifecycleFailed}
	for private, expected := range lifecycles {
		if got := adapter.MapStatus(model.Status{Lifecycle: private}).Lifecycle; got != expected {
			t.Errorf("lifecycle %s=%s want=%s", private, got, expected)
		}
	}
}

func TestPayloadProjectionDoesNotAliasInternalBytes(t *testing.T) {
	adapter, _ := New()
	body := []byte("value")
	headers := []model.Header{{Name: "x-a", Value: "one"}}
	public, err := adapter.MapPayload(model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/", Headers: headers, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	headers[0].Value = "changed"
	request, ok := public.Value.(v1.HTTPRequestPayload)
	if !ok || string(request.Body) != "value" || request.Headers[0].Value != "one" {
		t.Fatalf("public payload aliases internal data: %#v", public.Value)
	}
}

func TestPayloadProjectionRejectsLimitsBeforeCopyingAttackerSizedInput(t *testing.T) {
	adapter, _ := New()
	oversizedBody := model.Payload{Value: model.NativePayload{Path: "/", Body: make([]byte, maxInlinePayloadBytes+1)}}
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := adapter.MapPayload(oversizedBody); !errors.Is(err, ErrInputTooLarge) {
			t.Fatalf("body error=%v", err)
		}
	}); allocations > 1 {
		t.Fatalf("oversized body rejection allocated %.0f objects; validation must precede cloning", allocations)
	}

	tooManyHeaders := model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/", Headers: make([]model.Header, maxHeaderCount+1)}}
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := adapter.MapPayload(tooManyHeaders); !errors.Is(err, ErrInputTooLarge) {
			t.Fatalf("header error=%v", err)
		}
	}); allocations > 1 {
		t.Fatalf("oversized header rejection allocated %.0f objects; validation must precede projection", allocations)
	}
}

func TestPayloadProjectionNormalizesMalformedAndWrongClassHandles(t *testing.T) {
	adapter, _ := New()
	for _, handle := range []string{"payload-1", validRequestHandle, validCommandHandle} {
		_, err := adapter.MapPayload(model.Payload{Value: model.PayloadHandle{Handle: handle}})
		if !errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrMalformedInput) {
			t.Errorf("handle=%q error=%v want invalid handle only", handle, err)
		}
	}
}

func TestInternalProjectionBoundsFailClosed(t *testing.T) {
	adapter, _ := New()
	overlong := strings.Repeat("x", maxIdentifierLength+1)
	results := []model.ResultValue{
		model.ConfigResult{Config: model.RuntimeConfig{QueueLimit: 0}},
		model.ConfigResult{Config: model.RuntimeConfig{QueueLimit: maxQueueCapacity + 1}},
		model.AgentIDResult{AgentID: overlong},
		model.AddressResolution{Recipient: "a", Path: strings.Repeat("/", maxPathLength+1)},
		model.DeliveryQueueStatus{Queued: maxQueueCapacity + 1},
		model.PayloadHandleResult{Handle: validPayloadHandle, Size: maxCanonicalPayloadSize + 1},
		model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: 0},
		model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: maxQueueCapacity + 1},
	}
	for _, value := range results {
		if _, err := adapter.MapCompletion(model.Result{CommandID: "c", Value: value}); err == nil {
			t.Errorf("unbounded result %T was accepted", value)
		}
	}
	events := []model.Event{
		{ID: overlong, Name: string(v1.EventCommandQueueFull), CreatedAt: time.Now(), Value: model.QueueCapacityEvent{Queue: "command", Capacity: 1}},
		{ID: "e", Name: string(v1.EventPayloadTransferProgress), CreatedAt: time.Now(), Value: model.PayloadTransferEvent{Handle: validPayloadHandle, State: "progress", Total: maxCanonicalPayloadSize + 1}},
		{ID: "e", Name: string(v1.EventCommandQueueFull), CreatedAt: time.Now(), Value: model.QueueCapacityEvent{Queue: "command", Capacity: 0}},
		{ID: "e", Name: string(v1.EventEventQueueFull), CreatedAt: time.Now(), Value: model.QueueCapacityEvent{Queue: "event", Capacity: maxQueueCapacity + 1}},
	}
	for _, event := range events {
		if _, err := adapter.MapEvent(event); err == nil {
			t.Errorf("unbounded event %s was accepted", event.Name)
		}
	}
}

func TestPublicEncodersNeverSerializeZeroOrOversizedCapacities(t *testing.T) {
	adapter, _ := New()
	now := time.Unix(1, 0).UTC()
	results := []v1.Result{
		v1.ConfigResult{Config: v1.Config{QueueLimit: 0}},
		v1.ConfigResult{Config: v1.Config{QueueLimit: maxQueueCapacity + 1}},
		v1.CompletionChannelResult{ChannelID: "channel", MaxInFlight: 0},
		v1.CompletionChannelResult{ChannelID: "channel", MaxInFlight: maxQueueCapacity + 1},
	}
	for _, result := range results {
		if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "c", OK: true, Result: result}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("encoder accepted %T capacity: %v", result, err)
		}
	}
	for _, capacity := range []uint32{0, maxQueueCapacity + 1} {
		event := v1.Event{ID: "e", Name: v1.EventCommandQueueFull, CreatedAt: now, Payload: v1.QueueCapacityEvent{Queue: v1.QueueNameCommand, Capacity: capacity}}
		if _, err := adapter.EncodeABIEvent(event); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("encoder accepted event capacity=%d: %v", capacity, err)
		}
	}
}
