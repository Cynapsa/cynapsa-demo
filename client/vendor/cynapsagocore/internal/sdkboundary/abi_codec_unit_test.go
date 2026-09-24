package sdkboundary

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestWirePayloadEmptyBodiesRemainCanonicalBase64(t *testing.T) {
	for _, payload := range []v1.Payload{
		{Value: v1.NativePayload{Path: "/empty"}},
		{Value: v1.HTTPRequestPayload{Method: "GET", Path: "/empty"}},
		{Value: v1.HTTPResponsePayload{StatusCode: 204, Reason: "No Content"}},
	} {
		wire, err := wirePayload(payload)
		if err != nil {
			t.Fatalf("encode payload: %v", err)
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		if !bytes.Contains(encoded, []byte(`"body":""`)) {
			t.Fatalf("empty body must be base64 text, got %s", encoded)
		}
	}
}

func TestDecodeABIConfigUsesOnlyLocalLimits(t *testing.T) {
	adapter, _ := New()
	config, err := adapter.DecodeABIConfig([]byte(`{"abi_version":1,"command_timeout_ms":1000,"rpc_timeout_ms":2000,"queue_limit":32,"payload_limit":4096}`))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if config.CommandTimeout != time.Second || config.RPCTimeout != 2*time.Second || config.QueueLimit != 32 || config.PayloadLimit != 4096 {
		t.Fatalf("unexpected config: %#v", config)
	}
	defaults, err := adapter.DecodeABIConfig([]byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1}`))
	if err != nil || defaults.CommandTimeout != 30*time.Second || defaults.RPCTimeout != 30*time.Second {
		t.Fatalf("zero timeout defaults = %#v, %v", defaults, err)
	}
	_, err = adapter.DecodeABIConfig([]byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1,"mesh_id":"not-creation-input"}`))
	if !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("credential field error = %v, want malformed", err)
	}
	_, err = adapter.DecodeABIConfig([]byte(`{"abi_version":1,"command_timeout_ms":-1,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1}`))
	if !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("negative timeout error = %v, want malformed", err)
	}
	for _, input := range []string{
		`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":0,"payload_limit":1}`,
		`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":65537,"payload_limit":1}`,
		`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":0}`,
		fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes+1),
	} {
		if _, err := adapter.DecodeABIConfig([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Fatalf("limit error = %v, want malformed", err)
		}
	}
	atLimit := fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes)
	if config, err := adapter.DecodeABIConfig([]byte(atLimit)); err != nil || config.PayloadLimit != v1.MaximumPayloadBytes {
		t.Fatalf("exact payload limit = %#v, %v", config, err)
	}
}

func TestConfigUpdateQueueLimitMustBePositiveAndBounded(t *testing.T) {
	adapter, _ := New()
	for _, queueLimit := range []string{"0", "65537"} {
		input := `{"abi_version":1,"command_id":"c","command_name":"config.update","sdk_session_id":"s","args":{"queue_limit":` + queueLimit + `}}`
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("queue_limit=%s error=%v", queueLimit, err)
		}
	}
	zero := uint32(0)
	if _, err := adapter.DecodeCommand(context.Background(), v1.ConfigUpdateCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, Update: v1.ConfigUpdate{QueueLimit: &zero}}); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("Go config update zero error=%v", err)
	}
	payloadZero := uint64(0)
	if _, err := adapter.DecodeCommand(context.Background(), v1.ConfigUpdateCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, Update: v1.ConfigUpdate{PayloadLimit: &payloadZero}}); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("Go payload update zero error=%v", err)
	}
	payloadAtLimit := uint64(v1.MaximumPayloadBytes)
	if _, err := adapter.DecodeCommand(context.Background(), v1.ConfigUpdateCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, Update: v1.ConfigUpdate{PayloadLimit: &payloadAtLimit}}); err != nil {
		t.Errorf("Go payload update exact maximum: %v", err)
	}
	payloadOverLimit := payloadAtLimit + 1
	if _, err := adapter.DecodeCommand(context.Background(), v1.ConfigUpdateCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, Update: v1.ConfigUpdate{PayloadLimit: &payloadOverLimit}}); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("Go payload update maximum + 1 error=%v", err)
	}
	atLimitABI := fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":"session.config.update","sdk_session_id":"s","args":{"payload_limit":%d}}`, v1.MaximumPayloadBytes)
	if _, err := adapter.DecodeABICommand([]byte(atLimitABI)); err != nil {
		t.Errorf("ABI payload update exact maximum: %v", err)
	}
	overLimitABI := fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":"session.config.update","sdk_session_id":"s","args":{"payload_limit":%d}}`, v1.MaximumPayloadBytes+1)
	if _, err := adapter.DecodeABICommand([]byte(overLimitABI)); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("ABI payload update maximum + 1 error=%v", err)
	}
	timeoutZero := time.Duration(0)
	command, err := adapter.DecodeCommand(context.Background(), v1.ConfigUpdateCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, Update: v1.ConfigUpdate{CommandTimeout: &timeoutZero}})
	command = takeFrozenCommandForTest(t, command)
	if err != nil || *command.Args.(model.ConfigUpdateArgs).CommandTimeout != 30*time.Second {
		t.Errorf("Go timeout update default=%#v, %v", command.Args, err)
	}
}

func TestDecodeABICommandCoversFrozenCatalog(t *testing.T) {
	adapter, _ := New()
	cases := map[v1.CommandName]string{
		v1.CommandCoreInit: `{}`, v1.CommandCoreCapabilities: `{}`, v1.CommandCoreStatus: `{}`, v1.CommandCoreShutdown: `{}`,
		v1.CommandConfigGet: `{}`, v1.CommandConfigUpdate: `{"command_timeout_ms":1}`,
		v1.CommandChannelRegister: `{"capacity":4}`, v1.CommandChannelClear: `{"channel_id":"channel-1"}`, v1.CommandCancel: `{"command_handle":"` + validCommandHandle + `"}`,
		v1.CommandEventSinkRegister: `{"capacity":4}`, v1.CommandEventSinkClear: `{"sink_id":"sink-1"}`, v1.CommandEventSinkBind: `{"sink_id":"sink-1"}`,
		v1.CommandAuthLogin: validAuthArgs(), v1.CommandAuthConnect: validAuthArgs(), v1.CommandAuthTokenLogin: validTokenAuthArgs(), v1.CommandAuthTokenConnect: validTokenAuthArgs(), v1.CommandAuthInstallationLogin: `{"profile_id":"default","mesh_id":"mesh-one"}`, v1.CommandAuthInstallationConnect: `{"profile_id":"default","mesh_id":"mesh-one"}`, v1.CommandAuthLogout: `{}`, v1.CommandAuthAgentID: `{}`,
		v1.CommandMeshList: `{}`, v1.CommandMeshRefresh: `{}`,
		v1.CommandAddressPut: `{"virtual_origin":"https://agent-a.test","recipient":"agent-a@example.test"}`, v1.CommandAddressRemove: `{"virtual_origin":"https://agent-a.test"}`, v1.CommandAddressList: `{}`, v1.CommandAddressResolve: `{"url":"https://agent-a.test/path?q=1"}`,
		v1.CommandMessageSend: validSendArgs(), v1.CommandMessageRequest: validRequestArgs(), v1.CommandMessageReply: validReplyArgs(),
		v1.CommandDeliveryNext: `{}`, v1.CommandDeliveryAccept: `{"event_id":"event-1"}`, v1.CommandDeliveryQueueStatus: `{}`, v1.CommandDeliveryRetry: `{"message_id":"message-1"}`, v1.CommandDeliveryPause: `{}`, v1.CommandDeliveryResume: `{}`, v1.CommandDeliveryDrop: `{"message_id":"message-1"}`,
		v1.CommandHandlerRegister: `{"path":"/orders"}`, v1.CommandHandlerUnregister: `{"path":"/orders"}`,
		v1.CommandPayloadOpen: `{}`, v1.CommandPayloadWrite: `{"handle":"` + validPayloadHandle + `","chunk":"eA=="}`, v1.CommandPayloadFinish: `{"handle":"` + validPayloadHandle + `"}`, v1.CommandPayloadCancel: `{"handle":"` + validPayloadHandle + `"}`, v1.CommandPayloadRead: `{"handle":"` + validPayloadHandle + `","offset":0,"limit":1}`, v1.CommandPayloadClose: `{"handle":"` + validPayloadHandle + `"}`, v1.CommandPayloadRetain: `{"handle":"` + validPayloadHandle + `"}`, v1.CommandPayloadRelease: `{"handle":"` + validPayloadHandle + `"}`,
		v1.CommandConversationList: `{}`, v1.CommandConversationStatus: `{"conversation_id":"conversation-1"}`, v1.CommandConversationClose: `{"conversation_id":"conversation-1"}`,
		v1.CommandPolicySet: `{"rules":[{"action":"allow","path":"/orders","agent_id":"agent-b@example.test"}]}`, v1.CommandPolicyGet: `{}`, v1.CommandPolicyTest: `{"input":` + validSendArgs() + `}`,
		v1.CommandDiagnosticsPeer: `{"peer":"agent-b@example.test"}`, v1.CommandDiagnosticsConnectivity: `{}`, v1.CommandDiagnosticsSnapshot: `{}`, v1.CommandDiagnosticsLogs: `{"enabled":true}`,
	}

	capabilities := adapter.PublicCapabilities()
	if len(cases) != len(capabilities.Commands) {
		t.Fatalf("catalog cases=%d capabilities=%d", len(cases), len(capabilities.Commands))
	}
	seen := make(map[v1.CommandName]struct{}, len(capabilities.Commands))
	vectors := make([]discriminatorVector, 0, len(capabilities.Commands))
	for _, name := range capabilities.Commands {
		if _, duplicate := seen[name]; duplicate {
			t.Errorf("duplicate capability command %s", name)
		}
		seen[name] = struct{}{}
		args, ok := cases[name]
		if !ok {
			t.Errorf("missing vector for %s", name)
			continue
		}
		input := fmt.Sprintf(`{"abi_version":1,"command_id":"command-1","command_name":%q,"sdk_session_id":"session-1","args":%s}`, name, args)
		vectors = append(vectors, discriminatorVector{Name: string(name), JSON: json.RawMessage(input)})
		command, err := adapter.DecodeABICommand([]byte(input))
		if err != nil {
			t.Errorf("decode %s: %v", name, err)
			continue
		}
		if command.Name() != name {
			t.Errorf("decoded name=%s want=%s", command.Name(), name)
		}
	}
	for name := range cases {
		if _, ok := seen[name]; !ok {
			t.Errorf("catalog command missing from capabilities: %s", name)
		}
	}
	assertConformanceVectors(t, "commands.json", vectors)
}

func TestPublicCapabilitiesReturnsFreshAllowlists(t *testing.T) {
	adapter, _ := New()
	first := adapter.PublicCapabilities()
	first.Commands[0] = "private.command"
	first.Features[0] = "private_feature"
	second := adapter.PublicCapabilities()
	if second.Commands[0] == first.Commands[0] || second.Features[0] == first.Features[0] {
		t.Fatal("capability slices alias across calls")
	}
}

func TestRetiredDeliveryCapabilityIsNotPublic(t *testing.T) {
	adapter, _ := New()
	for _, feature := range adapter.PublicCapabilities().Features {
		if feature == v1.Capability("ordered_delivery") {
			t.Fatal("retired delivery capability was advertised")
		}
	}
	if IsPublicCapability(v1.Capability("ordered_delivery")) {
		t.Fatal("retired delivery capability remained in the public allowlist")
	}
}

func TestRemovedCredentialAndPersonalityToggleCommandsFailClosed(t *testing.T) {
	adapter, _ := New()
	for _, name := range []string{"mesh.credentials.put", "mesh.credentials.remove", "http.bridge.enable", "http.bridge.disable"} {
		input := fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":%q,"sdk_session_id":"s","args":{}}`, name)
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("%s error=%v want malformed", name, err)
		}
	}
}

func TestABIDecoderRejectsUnknownDuplicateMalformedAndUnsupportedInput(t *testing.T) {
	adapter, _ := New()
	tests := []struct {
		name, input string
		target      error
	}{
		{"unknown top field", `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"extra":1}`, ErrMalformedInput},
		{"duplicate top field", `{"abi_version":1,"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{}}`, ErrMalformedInput},
		{"duplicate nested field", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","to":"b","payload":{"native":{"content_type":"","path":"/","body":"eA=="}}}}`, ErrMalformedInput},
		{"noncanonical field spelling", `{"ABI_VERSION":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{}}`, ErrMalformedInput},
		{"unsupported", `{"abi_version":2,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{}}`, ErrUnsupportedVersion},
		{"null envelope", `null`, ErrMalformedInput},
		{"negative request TTL", `{"abi_version":1,"command_id":"c","command_name":"message.request","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"ttl_ms":-1}}`, ErrMalformedInput},
		{"removed request timeout", `{"abi_version":1,"command_id":"c","command_name":"message.request","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"timeout_ms":1,"ttl_ms":0}}`, ErrMalformedInput},
		{"removed priority", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"priority":"normal"}}`, ErrMalformedInput},
		{"removed send TTL", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"ttl_ms":1}}`, ErrMalformedInput},
		{"removed reply TTL", `{"abi_version":1,"command_id":"c","command_name":"message.reply","sdk_session_id":"s","args":{"request_handle":"` + validRequestHandle + `","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"ttl_ms":1}}`, ErrMalformedInput},
		{"removed policy priority", `{"abi_version":1,"command_id":"c","command_name":"policy.test","sdk_session_id":"s","args":{"input":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="}},"priority":"normal"}}}`, ErrMalformedInput},
		{"unknown payload variant", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"private":{"body":"eA=="}}}}`, ErrMalformedInput},
		{"missing payload variant", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{}}}`, ErrMalformedInput},
		{"multiple payload variants", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"","path":"/","body":"eA=="},"payload_handle":"` + validPayloadHandle + `"}}}`, ErrMalformedInput},
		{"mapping mesh selector", `{"abi_version":1,"command_id":"c","command_name":"address.map.put","sdk_session_id":"s","args":{"virtual_origin":"https://a.test","recipient":"a@example.test","mesh_id":"mesh-2"}}`, ErrMalformedInput},
		{"message mesh selector", `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","mesh_id":"mesh-2","payload":{"native":{"content_type":"","path":"/","body":"eA=="}}}}`, ErrMalformedInput},
		{"invalid address URL", `{"abi_version":1,"command_id":"c","command_name":"address.resolve","sdk_session_id":"s","args":{"url":"relative/path"}}`, ErrMalformedInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.DecodeABICommand([]byte(test.input))
			if !errors.Is(err, test.target) {
				t.Fatalf("error=%v want %v", err, test.target)
			}
		})
	}
	if _, err := adapter.DecodeABICommand([]byte(strings.Repeat(" ", maxABIInputBytes+1))); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("oversized error=%v want input too large", err)
	}
	deep := strings.Repeat("[", maxJSONDepth+2) + "0" + strings.Repeat("]", maxJSONDepth+2)
	if _, err := adapter.DecodeABICommand([]byte(deep)); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("deep nesting error=%v want malformed", err)
	}
}

func TestCommandMappingCopiesMutablePayload(t *testing.T) {
	adapter, _ := New()
	body := []byte("value")
	command := v1.MessageSendCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}, To: "agent@example.test", Payload: v1.Payload{Value: v1.NativePayload{Path: "/", Body: body}}}
	internal, err := adapter.DecodeCommand(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	internal = takeFrozenCommandForTest(t, internal)
	body[0] = 'X'
	if got := string(internal.Args.(model.MessageSendArgs).Payload.Value.(model.NativePayload).Body); got != "value" {
		t.Fatalf("internal payload aliased caller: %q", got)
	}
}

func TestMessageRequestTTLRemainsUnresolvedAtBoundary(t *testing.T) {
	adapter, _ := New()
	base := v1.CommandBase{CommandID: "c", SDKSessionID: "s"}
	payload := v1.Payload{Value: v1.NativePayload{Path: "/", Body: []byte("value")}}

	for _, ttl := range []time.Duration{0, 2500 * time.Millisecond} {
		command, err := adapter.DecodeCommand(context.Background(), v1.MessageRequestCommand{
			CommandBase: base,
			To:          "agent@example.test",
			Payload:     payload,
			TTL:         ttl,
		})
		if err != nil {
			t.Fatalf("TTL %s: %v", ttl, err)
		}
		command = takeFrozenCommandForTest(t, command)
		if got := command.Args.(model.MessageRequestArgs).TTL; got != ttl {
			t.Fatalf("TTL mapped to %s, want unresolved %s", got, ttl)
		}
	}

	if _, err := adapter.DecodeCommand(context.Background(), v1.MessageRequestCommand{
		CommandBase: base,
		To:          "agent@example.test",
		Payload:     payload,
		TTL:         -time.Millisecond,
	}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("negative TTL error=%v, want malformed input", err)
	}
}

func TestCanonicalPayloadVariantsHaveStrictConformanceVectors(t *testing.T) {
	adapter, _ := New()
	vectors := []struct {
		name    string
		payload string
		check   func(t *testing.T, payload model.Payload)
	}{
		{name: "native", payload: `{"native":{"content_type":"application/octet-stream","path":"/native","body":"AP8="}}`, check: func(t *testing.T, payload model.Payload) {
			native, ok := payload.Value.(model.NativePayload)
			if !ok || string(native.Body) != "\x00\xff" {
				t.Fatalf("native=%#v", payload.Value)
			}
		}},
		{name: "http request", payload: `{"http_request":{"method":"POST","path":"/orders","query":"a=1&a=2","headers":[{"name":"x-a","value":"one"},{"name":"x-a","value":"two"}],"body":"AP8="}}`, check: func(t *testing.T, payload model.Payload) {
			request, ok := payload.Value.(model.HTTPRequestPayload)
			if !ok || string(request.Body) != "\x00\xff" || len(request.Headers) != 2 || request.Headers[0].Value != "one" || request.Headers[1].Value != "two" {
				t.Fatalf("http request=%#v", payload.Value)
			}
		}},
		{name: "http response legacy", payload: `{"http_response":{"status_code":201,"reason":"Created","headers":[{"name":"set-cookie","value":"a=1"},{"name":"set-cookie","value":"b=2"}],"body":"AP8="}}`, check: func(t *testing.T, payload model.Payload) {
			response, ok := payload.Value.(model.HTTPResponsePayload)
			if !ok || response.StatusCode != 201 || string(response.Body) != "\x00\xff" || len(response.Headers) != 2 || response.Headers[1].Value != "b=2" || response.Error != nil {
				t.Fatalf("legacy http response=%#v", payload.Value)
			}
		}},
		{name: "http response", payload: `{"http_response":{"status_code":404,"reason":"Not Found","headers":[{"name":"set-cookie","value":"a=1"},{"name":"set-cookie","value":"b=2"},{"name":"x-cynapsa-error","value":"ordinary application header"}],"body":"AP8=","error":{"code":"not_found","detail":"The record does not exist","details_json":"{\"record_id\":7}"}}}`, check: func(t *testing.T, payload model.Payload) {
			response, ok := payload.Value.(model.HTTPResponsePayload)
			if !ok || response.StatusCode != 404 || string(response.Body) != "\x00\xff" || response.Headers[1].Value != "b=2" || response.Headers[2].Name != "x-cynapsa-error" || response.Error == nil || response.Error.Code != "not_found" || response.Error.DetailsJSON != `{"record_id":7}` {
				t.Fatalf("http response=%#v", payload.Value)
			}
		}},
		{name: "snapshot handle", payload: `{"payload_handle":"` + validPayloadHandle + `"}`, check: func(t *testing.T, payload model.Payload) {
			handle, ok := payload.Value.(model.PayloadHandle)
			if !ok || handle.Handle != validPayloadHandle {
				t.Fatalf("handle=%#v", payload.Value)
			}
		}},
	}
	conformance := make([]discriminatorVector, 0, len(vectors))
	for _, vector := range vectors {
		conformance = append(conformance, discriminatorVector{Name: vector.name, JSON: json.RawMessage(vector.payload)})
		t.Run(vector.name, func(t *testing.T) {
			input := `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"agent","payload":` + vector.payload + `}}`
			command, err := adapter.DecodeABICommand([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			mapped, err := adapter.DecodeCommand(context.Background(), command)
			if err != nil {
				t.Fatal(err)
			}
			mapped = takeFrozenCommandForTest(t, mapped)
			vector.check(t, mapped.Args.(model.MessageSendArgs).Payload)
		})
	}
	assertConformanceVectors(t, "payloads.json", conformance)
}

func TestPayloadAndQueueBoundsAreEnforced(t *testing.T) {
	adapter, _ := New()
	base := v1.CommandBase{CommandID: "c", SDKSessionID: "s"}
	tooLargeInline := v1.MessageSendCommand{CommandBase: base, To: "agent", Payload: v1.Payload{Value: v1.NativePayload{Path: "/", Body: make([]byte, maxInlinePayloadBytes)}}}
	if _, err := adapter.DecodeCommand(context.Background(), tooLargeInline); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("inline error=%v", err)
	}
	tooLargeChunk := v1.PayloadWriteCommand{CommandBase: base, Handle: validPayloadHandle, Chunk: make([]byte, maxPayloadChunkBytes+1)}
	if _, err := adapter.DecodeCommand(context.Background(), tooLargeChunk); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("chunk error=%v", err)
	}
	tooLargeQueue := v1.CommandChannelRegisterCommand{CommandBase: base, Capacity: maxQueueCapacity + 1}
	if _, err := adapter.DecodeCommand(context.Background(), tooLargeQueue); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("queue error=%v", err)
	}
	for _, response := range []v1.HTTPResponsePayload{
		{StatusCode: 200, Error: &v1.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{}`}},
		{StatusCode: 400, Error: &v1.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{"x":1,"x":2}`}},
		{StatusCode: 400, Error: &v1.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `[]`}},
		{StatusCode: 400, Error: &v1.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{"x":"\ud800"}`}},
		{StatusCode: 400, Error: &v1.ApplicationError{Code: string([]byte{0xff}), Detail: "bad", DetailsJSON: `{}`}},
		{StatusCode: 400, Error: &v1.ApplicationError{Code: "bad", Detail: string([]byte{0xff}), DetailsJSON: `{}`}},
	} {
		command := v1.MessageReplyCommand{CommandBase: base, RequestHandle: validRequestHandle, Payload: v1.Payload{Value: response}}
		if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrMalformedInput) {
			t.Fatalf("invalid application error %#v: %v", response.Error, err)
		}
	}
	if _, err := adapter.MapPayload(model.Payload{Value: model.HTTPResponsePayload{
		StatusCode: 400,
		Error:      &model.ApplicationError{Code: string([]byte{0xff}), Detail: "safe", DetailsJSON: `{}`},
	}}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("private invalid UTF-8 application error was projected: %v", err)
	}
}

func TestCommandMappingAcceptsValuesAndPointersButRejectsTypedNil(t *testing.T) {
	adapter, _ := New()
	value := v1.CoreStatusCommand{CommandBase: v1.CommandBase{CommandID: "c", SDKSessionID: "s"}}
	if _, err := adapter.DecodeCommand(context.Background(), value); err != nil {
		t.Fatalf("value: %v", err)
	}
	if _, err := adapter.DecodeCommand(context.Background(), &value); err != nil {
		t.Fatalf("pointer: %v", err)
	}
	var typedNil *v1.CoreStatusCommand
	if _, err := adapter.DecodeCommand(context.Background(), typedNil); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("typed nil error=%v", err)
	}
}

func TestABIEncodersHaveDeterministicVersionedShapes(t *testing.T) {
	adapter, _ := New()
	admission, err := adapter.EncodeABIAdmission(v1.Admission{CommandID: "command-1", CommandHandle: validCommandHandle, Accepted: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(admission), `{"abi_version":1,"command_id":"command-1","command_handle":"`+validCommandHandle+`","accepted":true}`; got != want {
		t.Fatalf("admission=%s want=%s", got, want)
	}
	completion, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command-1", OK: true, Result: v1.SendResult{MessageID: "message-1", ConversationID: "conversation-1", Accepted: true}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(completion), `{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"send","result":{"message_id":"message-1","conversation_id":"conversation-1","accepted":true}}`; got != want {
		t.Fatalf("completion=%s want=%s", got, want)
	}
	status, err := adapter.EncodeABIStatus(v1.Status{Lifecycle: v1.LifecycleReady, Connectivity: v1.ConnectivityAvailable, Personality: v1.SDKPersonalityNative, AgentID: "agent@example.test", MeshID: "mesh-1", MeshEndpoint: "connect", QueuedMessageCount: 0})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(status), `"abi_version":1`) || strings.Contains(string(status), "password") {
		t.Fatalf("unsafe status: %s", status)
	}
}

func TestHTTPPayloadGoldenPreservesHeaderOrderDuplicatesAndBody(t *testing.T) {
	adapter, _ := New()
	payload := v1.Payload{Value: v1.HTTPRequestPayload{Method: "POST", Path: "/orders", Query: "a=1&a=2", Headers: []v1.Header{{Name: "x-a", Value: "one"}, {Name: "x-a", Value: "two"}}, Body: []byte{0, 255}}}
	data, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "c", OK: true, Result: v1.ResponseResult{MessageID: "m", ConversationID: "conversation", FromAgentID: "agent", MeshID: "mesh", Payload: payload}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"abi_version":1,"command_id":"c","ok":true,"result_type":"response","result":{"message_id":"m","conversation_id":"conversation","from_agent_id":"agent","mesh_id":"mesh","payload":{"http_request":{"method":"POST","path":"/orders","query":"a=1\u0026a=2","headers":[{"name":"x-a","value":"one"},{"name":"x-a","value":"two"}],"body":"AP8="}}}}`
	if string(data) != want {
		t.Fatalf("golden=%s\nwant=%s", data, want)
	}
	if forbidden, found := forbiddenPublicVocabulary(string(data)); found {
		t.Fatalf("serialized fixture exposes %s", forbidden)
	}
}

func TestAdmissionRequiresOpaqueCommandHandleOnlyWhenAccepted(t *testing.T) {
	adapter, _ := New()
	publicError := &v1.Error{Code: v1.ErrorCodeQueueFull, Message: "The local queue is full", Stage: v1.ErrorStageCommand, Location: v1.ErrorLocationLocal}
	tests := []struct {
		name      string
		admission v1.Admission
		wantError bool
	}{
		{name: "accepted handle missing", admission: v1.Admission{CommandID: "command-1", Accepted: true}, wantError: true},
		{name: "accepted malformed handle", admission: v1.Admission{CommandID: "command-1", CommandHandle: "command-1", Accepted: true}, wantError: true},
		{name: "rejected handle present", admission: v1.Admission{CommandID: "command-1", CommandHandle: validCommandHandle, Error: publicError}, wantError: true},
		{name: "rejected without handle", admission: v1.Admission{CommandID: "command-1", Error: publicError}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.EncodeABIAdmission(test.admission)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%v", err, test.wantError)
			}
		})
	}
	if _, err := adapter.EncodeABIAdmission(v1.Admission{CommandID: "command-1", CommandHandle: validRequestHandle, Accepted: true}); !errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrMalformedInput) {
		t.Fatalf("wrong-class admission error=%v want invalid handle only", err)
	}
}

func TestCommandCancelRejectsCommandIDInPlaceOfOpaqueHandle(t *testing.T) {
	adapter, _ := New()
	input := `{"abi_version":1,"command_id":"cancel-1","command_name":"command.cancel","sdk_session_id":"session-1","args":{"command_handle":"command-1"}}`
	if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrMalformedInput) {
		t.Fatalf("error=%v want invalid handle only", err)
	}
}

func TestPayloadAndRequestHandlesRequireOpaqueFormat(t *testing.T) {
	adapter, _ := New()
	inputs := []string{
		`{"abi_version":1,"command_id":"c","command_name":"payload.read","sdk_session_id":"s","args":{"handle":"payload-1","offset":0,"limit":1}}`,
		`{"abi_version":1,"command_id":"c","command_name":"message.reply","sdk_session_id":"s","args":{"request_handle":"request-1","payload":` + validPayload() + `}}`,
		`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"payload_handle":"payload-1"}}}`,
		`{"abi_version":1,"command_id":"c","command_name":"payload.read","sdk_session_id":"s","args":{"handle":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB","offset":0,"limit":1}}`,
		`{"abi_version":1,"command_id":"c","command_name":"payload.read","sdk_session_id":"s","args":{"handle":"` + validRequestHandle + `","offset":0,"limit":1}}`,
		`{"abi_version":1,"command_id":"c","command_name":"message.reply","sdk_session_id":"s","args":{"request_handle":"` + validPayloadHandle + `","payload":` + validPayload() + `}}`,
		`{"abi_version":1,"command_id":"c","command_name":"command.cancel","sdk_session_id":"s","args":{"command_handle":"` + validRequestHandle + `"}}`,
	}
	for _, input := range inputs {
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrMalformedInput) {
			t.Fatalf("error=%v want invalid handle only for %s", err, input)
		}
	}
}

func TestRegistrationCapacitiesMustBePositiveAndBounded(t *testing.T) {
	adapter, _ := New()
	for _, name := range []v1.CommandName{v1.CommandChannelRegister, v1.CommandEventSinkRegister} {
		for _, capacity := range []string{"0", "65537"} {
			input := fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":%q,"sdk_session_id":"s","args":{"capacity":%s}}`, name, capacity)
			if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
				t.Errorf("%s capacity=%s error=%v", name, capacity, err)
			}
		}
	}
}

func TestPrivateErrorCauseNeverCrossesBoundary(t *testing.T) {
	adapter, _ := New()
	privateText := "private dependency failure details"
	completion, err := adapter.MapCompletion(model.Result{CommandID: "command-1", Err: &model.Error{Code: "authentication_failed", Stage: "auth", Cause: errors.New(privateText), Location: "local"}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := adapter.EncodeABICompletion(completion)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), privateText) {
		t.Fatalf("private error leaked: %s", encoded)
	}
}

func validAuthArgs() string {
	return `{"mesh_endpoint":"mesh.example.test:5222","username":"agent-a@example.test","password":"secret","mesh_id":"mesh-1","agent_instance_id":"instance-1"}`
}

func TestABIInternalAuthUsesClearablePasswordBytes(t *testing.T) {
	encoded := []byte(`{"abi_version":1,"command_id":"auth","command_name":"auth.login","sdk_session_id":"session","args":{"mesh_endpoint":"mesh.example.test:5222","username":"agent@example.test","password":"s\u0065cret-\ud83d\udd10","mesh_id":"mesh-one","agent_instance_id":"instance"}}`)
	internal, err := new(Adapter).DecodeABIInternalCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	internal = takeFrozenCommandForTest(t, internal)
	args, ok := internal.Args.(model.AuthArgs)
	if !ok || string(args.Password) != "secret-🔐" {
		t.Fatalf("internal auth = %#v", internal.Args)
	}
	retained := args.Password
	model.ClearCommand(&internal)
	for index, value := range retained {
		if value != 0 {
			t.Fatalf("password byte %d was not cleared", index)
		}
	}
	public, err := new(Adapter).DecodeABICommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	login, ok := public.(v1.AuthLoginCommand)
	if !ok || login.Auth.Password != "secret-🔐" {
		t.Fatalf("public auth = %#v", public)
	}
}

func TestClearableSecretJSONRejectsMalformedAndOversizedValues(t *testing.T) {
	for _, encoded := range []string{`"\uD800"`, `"\uDC00"`, `"\x00"`, `"\xff"`} {
		decoded, err := decodeJSONStringBytes([]byte(encoded))
		if err == nil || decoded != nil {
			t.Fatalf("decodeJSONStringBytes(%q) = (%q, %v)", encoded, decoded, err)
		}
	}
	oversized := []byte(`"` + strings.Repeat("p", maxPublicString+1) + `"`)
	if decoded, err := decodeJSONStringBytes(oversized); !errors.Is(err, ErrInputTooLarge) || decoded != nil {
		t.Fatalf("oversized = (%q, %v)", decoded, err)
	}
}

func TestABISecretRedactionRemovesRawCredentialBeforeJSONDecode(t *testing.T) {
	raw := []byte(`{"args":{"pass\u0077ord":"s\u0065cret"}}`)
	sanitized, secret, found, err := redactABISecretField(raw, "password")
	if err != nil || !found || string(secret) != "secret" {
		t.Fatalf("redact = (%q, %q, %v, %v)", sanitized, secret, found, err)
	}
	defer clear(sanitized)
	defer clear(secret)
	if bytes.Contains(sanitized, []byte("secret")) || bytes.Contains(sanitized, []byte(`s\u0065cret`)) || !bytes.Contains(sanitized, []byte("redacted")) {
		t.Fatalf("sanitized credential = %q", sanitized)
	}
}
func validPayload() string {
	return `{"native":{"content_type":"application/json","path":"/orders","body":"eA=="}}`
}
func validSendArgs() string {
	return `{"to":"agent-b@example.test","payload":` + validPayload() + `}`
}
func validRequestArgs() string {
	return `{"to":"agent-b@example.test","payload":` + validPayload() + `,"ttl_ms":1000}`
}
func validReplyArgs() string {
	return `{"request_handle":"` + validRequestHandle + `","payload":` + validPayload() + `}`
}

const (
	validCommandHandle = "cmdh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	validRequestHandle = "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	validPayloadHandle = "payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	validDiagnosticID  = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

func TestEncodedCompletionIsStrictJSON(t *testing.T) {
	adapter, _ := New()
	data, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command-1", OK: true, Result: v1.EmptyResult{}})
	if err != nil {
		t.Fatal(err)
	}
	var wire completionWire
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if wire.ABIVersion != 1 || wire.ResultType != "empty" {
		t.Fatalf("unexpected wire: %#v", wire)
	}
}
