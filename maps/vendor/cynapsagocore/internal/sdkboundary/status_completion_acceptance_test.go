package sdkboundary

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestPrivateCoreStatusCompletionHasExactPublicABIProjection(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	private := model.Result{
		CommandID: "command-status",
		Value: model.StatusResult{Status: model.Status{
			Lifecycle:          model.LifecycleReady,
			ConnectivityDetail: "available",
			Personality:        "native",
			AgentID:            "agent-1",
			MeshID:             "mesh-1",
			MeshEndpoint:       "connect.example.test",
			QueuedMessageCount: 7,
		}},
	}
	completion, err := adapter.MapCompletion(private)
	if err != nil {
		t.Fatalf("map private status completion: %v", err)
	}
	wantStatus := v1.Status{
		Lifecycle:          v1.LifecycleReady,
		Connectivity:       v1.ConnectivityAvailable,
		Personality:        v1.SDKPersonalityNative,
		AgentID:            "agent-1",
		MeshID:             "mesh-1",
		MeshEndpoint:       "connect.example.test",
		QueuedMessageCount: 7,
	}
	gotStatus, ok := completion.Result.(v1.Status)
	if !ok {
		t.Fatalf("public result type = %T, want v1.Status", completion.Result)
	}
	if completion.CommandID != "command-status" || !completion.OK || completion.Error != nil || !reflect.DeepEqual(gotStatus, wantStatus) {
		t.Fatalf("public completion = %#v", completion)
	}

	encoded, err := adapter.EncodeABICompletion(completion)
	if err != nil {
		t.Fatalf("encode mapped status completion: %v", err)
	}
	wantABI := `{"abi_version":1,"command_id":"command-status","ok":true,"result_type":"status","result":{"lifecycle":"ready","connectivity":"available","personality":"native","agent_id":"agent-1","mesh_id":"mesh-1","mesh_endpoint":"connect.example.test","queued_message_count":7}}`
	if string(encoded) != wantABI {
		t.Fatalf("status ABI = %s\nwant       = %s", encoded, wantABI)
	}
}

// TestFrozenPrivateResultCatalogReachesABI proves that every frozen V1 result
// discriminator has at least one explicit private -> public -> ABI route. It
// intentionally does not assign a result to every command: V1 has not frozen a
// command-to-result matrix, and several commands can complete with EmptyResult.
func TestFrozenPrivateResultCatalogReachesABI(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	now := time.Unix(1, 0).UTC()
	status := model.Status{Lifecycle: model.LifecycleReady, ConnectivityDetail: "available", Personality: "native", AgentID: "agent", MeshID: "mesh", MeshEndpoint: "endpoint"}
	payload := model.Payload{Value: model.NativePayload{Path: "/", Body: []byte{0, 1}}}
	samples := []struct {
		want  string
		value model.ResultValue
	}{
		{want: "empty", value: model.EmptyResult{}},
		{want: "capabilities", value: model.CapabilitiesResult{Commands: []string{string(v1.CommandCoreStatus)}, Features: []string{string(v1.CapabilityRPC)}}},
		{want: "status", value: model.StatusResult{Status: status}},
		{want: "config", value: model.ConfigResult{Config: model.RuntimeConfig{CommandTimeout: 30 * time.Second, RPCTimeout: 30 * time.Second, QueueLimit: 1, PayloadLimit: 1}}},
		{want: "auth", value: model.AuthResult{AgentID: "agent", MeshID: "mesh", Personality: "native"}},
		{want: "agent_id", value: model.AgentIDResult{AgentID: "agent"}},
		{want: "mesh_list", value: model.MeshListResult{Meshes: []model.MeshSummary{{MeshID: "mesh", Active: true}}}},
		{want: "address_mappings", value: model.AddressMappingsResult{Mappings: []model.AddressMapping{{VirtualOrigin: "https://agent.example.test", Recipient: "agent"}}}},
		{want: "address_resolution", value: model.AddressResolution{Recipient: "agent", Path: "/path", Query: "x=1"}},
		{want: "send", value: model.SendResult{MessageID: "message", ConversationID: "conversation", Accepted: true}},
		{want: "response", value: model.ResponseResult{MessageID: "message", ConversationID: "conversation", FromAgentID: "agent", MeshID: "mesh", Payload: payload}},
		{want: "event", value: model.EventResult{Event: model.Event{ID: "event", Name: string(v1.EventCommandQueueFull), CreatedAt: now, Value: model.QueueCapacityEvent{Queue: string(v1.QueueNameCommand), Capacity: 1}}}},
		{want: "delivery_queue_status", value: model.DeliveryQueueStatus{Queued: 1}},
		{want: "payload_handle", value: model.PayloadHandleResult{Handle: validPayloadHandle}},
		{want: "conversation_status", value: model.ConversationStatus{ConversationID: "conversation", MeshID: "mesh", Peer: "agent", DeliveryState: string(v1.DeliveryStateReady)}},
		{want: "conversation_list", value: model.ConversationListResult{Conversations: []model.ConversationStatus{{ConversationID: "conversation", MeshID: "mesh", Peer: "agent", DeliveryState: string(v1.DeliveryStateReady)}}}},
		{want: "policy", value: model.PolicyResult{Rules: []model.PolicyRule{{Action: string(v1.PolicyActionAllow), Path: "/", AgentID: "agent"}}, Allowed: true}},
		{want: "peer_status", value: model.PeerStatus{Peer: "agent", Connectivity: "available", Reachable: true}},
		{want: "connectivity_status", value: model.ConnectivityStatus{State: "available"}},
		{want: "diagnostic_snapshot", value: model.DiagnosticSnapshot{Status: status}},
		{want: "core_init", value: model.CoreInitResult{SessionID: "session"}},
		{want: "completion_channel", value: model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: 1}},
		{want: "event_sink", value: model.EventSinkResult{SinkID: "sink"}},
	}
	seen := make(map[string]struct{}, len(samples))
	for _, sample := range samples {
		if _, duplicate := seen[sample.want]; duplicate {
			t.Fatalf("duplicate expected result discriminator %q", sample.want)
		}
		seen[sample.want] = struct{}{}
		completion, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: sample.value})
		if err != nil {
			t.Errorf("map private %T: %v", sample.value, err)
			continue
		}
		encoded, err := adapter.EncodeABICompletion(completion)
		if err != nil {
			t.Errorf("encode private %T: %v", sample.value, err)
			continue
		}
		var wire struct {
			ResultType string `json:"result_type"`
		}
		if err := json.Unmarshal(encoded, &wire); err != nil {
			t.Errorf("decode private %T ABI: %v", sample.value, err)
			continue
		}
		if wire.ResultType != sample.want {
			t.Errorf("private %T result_type = %q, want %q", sample.value, wire.ResultType, sample.want)
		}
	}
	if got, want := len(seen), 23; got != want {
		t.Fatalf("frozen private result discriminator count = %d, want %d", got, want)
	}
}
