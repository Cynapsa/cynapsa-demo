package sdkboundary

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func qaExactUnicodePath() string {
	// 1 slash + 1,023 two-byte runes + 1 ASCII byte = 2,048 bytes.
	return "/" + strings.Repeat("é", 1023) + "a"
}

func qaCommandBase() v1.CommandBase {
	return v1.CommandBase{CommandID: "qa-command", SDKSessionID: "qa-session"}
}

func qaRequestHandle() v1.RequestHandle {
	return v1.RequestHandle(requestHandlePrefix + base64.RawURLEncoding.EncodeToString(make([]byte, requestHandleBytes)))
}

func qaABICommand(t *testing.T, name v1.CommandName, args any) []byte {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"abi_version":    v1.CurrentSchemaVersion,
		"command_id":     "qa-command",
		"command_name":   name,
		"sdk_session_id": "qa-session",
		"args":           args,
	})
	if err != nil {
		t.Fatalf("marshal ABI command: %v", err)
	}
	return encoded
}

func qaNativeWire(path, contentType string) map[string]any {
	return map[string]any{"native": map[string]any{"content_type": contentType, "path": path, "body": []byte{}}}
}

func qaHTTPRequestWire(path string) map[string]any {
	return map[string]any{"http_request": map[string]any{"method": "POST", "path": path, "query": "", "headers": []any{}, "body": []byte{}}}
}

func qaHTTPResponseWire(reason string) map[string]any {
	return map[string]any{"http_response": map[string]any{"status_code": 200, "reason": reason, "headers": []any{}, "body": []byte{}}}
}

func qaAssertError(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error=%v, want %v", err, target)
	}
}

func TestQAPathBoundaryAcrossAllCommandInputs(t *testing.T) {
	adapter, _ := New()
	exact := qaExactUnicodePath()
	over := exact + "b"
	if len(exact) != 2048 || len(over) != 2049 {
		t.Fatalf("bad QA fixture lengths: exact=%d over=%d", len(exact), len(over))
	}

	direct := []struct {
		name  string
		build func(string) v1.Command
	}{
		{"message send native", func(path string) v1.Command {
			return v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{Path: path}}}
		}},
		{"message request HTTP", func(path string) v1.Command {
			return v1.MessageRequestCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.HTTPRequestPayload{Method: "POST", Path: path}}}
		}},
		{"message reply native", func(path string) v1.Command {
			return v1.MessageReplyCommand{CommandBase: qaCommandBase(), RequestHandle: qaRequestHandle(), Payload: v1.Payload{Value: v1.NativePayload{Path: path}}}
		}},
		{"policy test HTTP", func(path string) v1.Command {
			return v1.PolicyTestCommand{CommandBase: qaCommandBase(), Input: v1.PolicyTestInput{To: "peer", Payload: v1.Payload{Value: v1.HTTPRequestPayload{Method: "POST", Path: path}}}}
		}},
		{"handler register", func(path string) v1.Command {
			return v1.HandlerRegisterCommand{CommandBase: qaCommandBase(), Path: path}
		}},
		{"handler unregister", func(path string) v1.Command {
			return v1.HandlerUnregisterCommand{CommandBase: qaCommandBase(), Path: path}
		}},
	}
	for _, tc := range direct {
		t.Run("direct/"+tc.name, func(t *testing.T) {
			if _, err := adapter.DecodeCommand(context.Background(), tc.build(exact)); err != nil {
				t.Fatalf("2,048-byte Unicode path rejected: %v", err)
			}
			_, err := adapter.DecodeCommand(context.Background(), tc.build(over))
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}

	abi := []struct {
		name string
		cmd  v1.CommandName
		args func(string) any
	}{
		{"message send native", v1.CommandMessageSend, func(path string) any {
			return map[string]any{"to": "peer", "payload": qaNativeWire(path, "")}
		}},
		{"message request HTTP", v1.CommandMessageRequest, func(path string) any {
			return map[string]any{"to": "peer", "payload": qaHTTPRequestWire(path), "ttl_ms": 0}
		}},
		{"message reply native", v1.CommandMessageReply, func(path string) any {
			return map[string]any{"request_handle": qaRequestHandle(), "payload": qaNativeWire(path, "")}
		}},
		{"policy test HTTP", v1.CommandPolicyTest, func(path string) any {
			return map[string]any{"input": map[string]any{"to": "peer", "payload": qaHTTPRequestWire(path)}}
		}},
		{"handler register", v1.CommandHandlerRegister, func(path string) any { return map[string]any{"path": path} }},
		{"handler unregister", v1.CommandHandlerUnregister, func(path string) any { return map[string]any{"path": path} }},
	}
	for _, tc := range abi {
		t.Run("ABI/"+tc.name, func(t *testing.T) {
			if _, err := adapter.DecodeABICommand(qaABICommand(t, tc.cmd, tc.args(exact))); err != nil {
				t.Fatalf("2,048-byte Unicode path rejected: %v", err)
			}
			_, err := adapter.DecodeABICommand(qaABICommand(t, tc.cmd, tc.args(over)))
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}
}

func TestQAAddressResolutionInputUsesTheSharedPathBoundary(t *testing.T) {
	adapter, _ := New()
	// Address resolution emits the URL's escaped path. Exercise Unicode through
	// its canonical percent-encoded representation: 1 + 341*6 + 1 = 2,048.
	exact := "/" + strings.Repeat("%C3%A9", 341) + "a"
	over := exact + "b"
	if len(exact) != maxPathLength || len(over) != maxPathLength+1 {
		t.Fatalf("bad escaped-path fixtures: exact=%d over=%d", len(exact), len(over))
	}
	for _, tc := range []struct {
		name string
		path string
		want error
	}{
		{"exact", exact, nil},
		{"over", over, ErrInputTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := "https://agent.example" + tc.path
			routes := []struct {
				name string
				run  func() error
			}{
				{"direct", func() error {
					_, err := adapter.DecodeCommand(context.Background(), v1.AddressResolveCommand{CommandBase: qaCommandBase(), URL: url})
					return err
				}},
				{"ABI", func() error {
					_, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandAddressResolve, map[string]any{"url": url}))
					return err
				}},
			}
			for _, route := range routes {
				t.Run(route.name, func(t *testing.T) {
					err := route.run()
					if tc.want == nil {
						if err != nil {
							t.Fatalf("2,048-byte address path rejected: %v", err)
						}
						return
					}
					qaAssertError(t, err, tc.want)
				})
			}
		})
	}

	// A 2,048-byte decoded Unicode path expands beyond the emitted escaped-path
	// ceiling and must therefore fail closed in both input forms.
	expandedURL := "https://agent.example" + qaExactUnicodePath()
	_, err := adapter.DecodeCommand(context.Background(), v1.AddressResolveCommand{CommandBase: qaCommandBase(), URL: expandedURL})
	qaAssertError(t, err, ErrInputTooLarge)
	_, err = adapter.DecodeABICommand(qaABICommand(t, v1.CommandAddressResolve, map[string]any{"url": expandedURL}))
	qaAssertError(t, err, ErrInputTooLarge)
}

func TestQAEmptyWildcardAndPathSyntaxAreContextSpecific(t *testing.T) {
	adapter, _ := New()
	for _, path := range []string{"", "/orders?state=open", "/orders#new"} {
		for _, command := range []v1.Command{
			v1.HandlerRegisterCommand{CommandBase: qaCommandBase(), Path: path},
			v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{Path: path}}},
		} {
			_, err := adapter.DecodeCommand(context.Background(), command)
			qaAssertError(t, err, ErrMalformedInput)
		}
		policy := v1.PolicySetCommand{CommandBase: qaCommandBase(), Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path}}}
		_, err := adapter.DecodeCommand(context.Background(), policy)
		if path == "" {
			if err != nil {
				t.Fatalf("empty policy wildcard rejected: %v", err)
			}
		} else {
			qaAssertError(t, err, ErrMalformedInput)
		}

		_, err = adapter.DecodeABICommand(qaABICommand(t, v1.CommandHandlerRegister, map[string]any{"path": path}))
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.DecodeABICommand(qaABICommand(t, v1.CommandMessageSend, map[string]any{"to": "peer", "payload": qaNativeWire(path, "")}))
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.DecodeABICommand(qaABICommand(t, v1.CommandPolicySet, map[string]any{"rules": []any{map[string]any{"action": "allow", "path": path, "agent_id": "peer"}}}))
		if path == "" {
			if err != nil {
				t.Fatalf("ABI empty policy wildcard rejected: %v", err)
			}
		} else {
			qaAssertError(t, err, ErrMalformedInput)
		}
	}

	exact, over := qaExactUnicodePath(), qaExactUnicodePath()+"b"
	for _, path := range []string{"", exact} {
		input := qaABICommand(t, v1.CommandPolicySet, map[string]any{"rules": []any{map[string]any{"action": "allow", "path": path, "agent_id": "peer"}}})
		if _, err := adapter.DecodeABICommand(input); err != nil {
			t.Fatalf("ABI policy path %q rejected: %v", path, err)
		}
	}
	_, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandPolicySet, map[string]any{"rules": []any{map[string]any{"action": "allow", "path": over, "agent_id": "peer"}}}))
	qaAssertError(t, err, ErrInputTooLarge)
}

func TestQAPrivateMapAndStrictABIOutputPathContract(t *testing.T) {
	adapter, _ := New()
	exact, over := qaExactUnicodePath(), qaExactUnicodePath()+"b"
	now := time.Unix(1, 0).UTC()

	privatePayloads := []func(string) model.Payload{
		func(path string) model.Payload { return model.Payload{Value: model.NativePayload{Path: path}} },
		func(path string) model.Payload {
			return model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: path}}
		},
	}
	for index, build := range privatePayloads {
		if _, err := adapter.MapPayload(build(exact)); err != nil {
			t.Fatalf("private payload %d exact path rejected: %v", index, err)
		}
		_, err := adapter.MapPayload(build(over))
		qaAssertError(t, err, ErrInputTooLarge)
	}

	privateResults := []struct {
		name  string
		build func(string) model.ResultValue
	}{
		{"address resolution", func(path string) model.ResultValue { return model.AddressResolution{Recipient: "peer", Path: path} }},
		{"policy", func(path string) model.ResultValue {
			return model.PolicyResult{Rules: []model.PolicyRule{{Action: "allow", Path: path, AgentID: "peer"}}, Allowed: true}
		}},
		{"response", func(path string) model.ResultValue {
			return model.ResponseResult{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Payload: model.Payload{Value: model.NativePayload{Path: path}}}
		}},
	}
	for _, tc := range privateResults {
		t.Run("private completion/"+tc.name, func(t *testing.T) {
			completion, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: tc.build(exact)})
			if err != nil {
				t.Fatalf("map exact result: %v", err)
			}
			if _, err := adapter.EncodeABICompletion(completion); err != nil {
				t.Fatalf("encode mapped exact result: %v", err)
			}
			_, err = adapter.MapCompletion(model.Result{CommandID: "command", Value: tc.build(over)})
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}

	privateEvents := []struct {
		name  string
		build func(string) model.Event
	}{
		{"message received", func(path string) model.Event {
			return model.Event{ID: "event", Name: string(v1.EventMessageReceived), CreatedAt: now, Value: model.MessageReceivedEvent{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Mode: string(v1.MessageModeOneWay), Payload: model.Payload{Value: model.NativePayload{Path: path}}}}
		}},
		{"policy rejected", func(path string) model.Event {
			return model.Event{ID: "event", Name: string(v1.EventPolicyRejected), CreatedAt: now, Value: model.PolicyRejectedEvent{MessageID: "message", Peer: "peer", Path: path}}
		}},
	}
	for _, tc := range privateEvents {
		t.Run("private event/"+tc.name, func(t *testing.T) {
			event, err := adapter.MapEvent(tc.build(exact))
			if err != nil {
				t.Fatalf("map exact event: %v", err)
			}
			if _, err := adapter.EncodeABIEvent(event); err != nil {
				t.Fatalf("encode mapped exact event: %v", err)
			}
			_, err = adapter.MapEvent(tc.build(over))
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}

	publicResults := []struct {
		name  string
		build func(string) v1.Result
	}{
		{"address resolution", func(path string) v1.Result { return v1.AddressResolution{Recipient: "peer", Path: path} }},
		{"policy", func(path string) v1.Result {
			return v1.PolicyResult{Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path, AgentID: "peer"}}, Allowed: true}
		}},
		{"response", func(path string) v1.Result {
			return v1.ResponseResult{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Payload: v1.Payload{Value: v1.NativePayload{Path: path}}}
		}},
	}
	for _, tc := range publicResults {
		t.Run("public ABI completion/"+tc.name, func(t *testing.T) {
			if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: tc.build(exact)}); err != nil {
				t.Fatalf("encode exact result: %v", err)
			}
			_, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: tc.build(over)})
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}

	publicEvents := []struct {
		name  string
		build func(string) v1.Event
	}{
		{"message received", func(path string) v1.Event {
			return v1.Event{ID: "event", Name: v1.EventMessageReceived, CreatedAt: now, Payload: v1.MessageReceivedEvent{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Mode: v1.MessageModeOneWay, Payload: v1.Payload{Value: v1.NativePayload{Path: path}}}}
		}},
		{"policy rejected", func(path string) v1.Event {
			return v1.Event{ID: "event", Name: v1.EventPolicyRejected, CreatedAt: now, Payload: v1.PolicyRejectedEvent{MessageID: "message", Peer: "peer", Path: path}}
		}},
	}
	for _, tc := range publicEvents {
		t.Run("public ABI event/"+tc.name, func(t *testing.T) {
			if _, err := adapter.EncodeABIEvent(tc.build(exact)); err != nil {
				t.Fatalf("encode exact event: %v", err)
			}
			_, err := adapter.EncodeABIEvent(tc.build(over))
			qaAssertError(t, err, ErrInputTooLarge)
		})
	}

	for _, path := range []string{"", "/bad?query", "/bad#fragment"} {
		_, err := adapter.MapPayload(model.Payload{Value: model.NativePayload{Path: path}})
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.MapCompletion(model.Result{CommandID: "command", Value: model.AddressResolution{Recipient: "peer", Path: path}})
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: v1.AddressResolution{Recipient: "peer", Path: path}})
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.MapEvent(privateEvents[0].build(path))
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.EncodeABIEvent(publicEvents[0].build(path))
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.MapEvent(privateEvents[1].build(path))
		qaAssertError(t, err, ErrMalformedInput)
		_, err = adapter.EncodeABIEvent(publicEvents[1].build(path))
		qaAssertError(t, err, ErrMalformedInput)
	}

	privateWildcard, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: model.PolicyResult{Rules: []model.PolicyRule{{Action: "allow", Path: "", AgentID: "peer"}}, Allowed: true}})
	if err != nil {
		t.Fatalf("private empty policy wildcard rejected: %v", err)
	}
	if _, err := adapter.EncodeABICompletion(privateWildcard); err != nil {
		t.Fatalf("mapped empty policy wildcard failed strict encoding: %v", err)
	}
	if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: v1.PolicyResult{Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: "", AgentID: "peer"}}, Allowed: true}}); err != nil {
		t.Fatalf("public empty policy wildcard failed strict encoding: %v", err)
	}
}

func TestQAContentTypeAndReasonAcrossBoundaryDirections(t *testing.T) {
	adapter, _ := New()
	exact := strings.Repeat("é", 256)
	over := exact + "a"
	if len(exact) != 512 || len(over) != 513 {
		t.Fatalf("bad text fixtures: exact=%d over=%d", len(exact), len(over))
	}
	tests := []struct {
		name    string
		valid   string
		invalid []string
		direct  func(string) v1.Command
		private func(string) model.Payload
		wire    func(string) map[string]any
	}{
		{
			name: "content type", valid: exact, invalid: []string{over, "text/plain\r\nx-private: value", string([]byte{0xff})},
			direct: func(value string) v1.Command {
				return v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{ContentType: value, Path: "/"}}}
			},
			private: func(value string) model.Payload {
				return model.Payload{Value: model.NativePayload{ContentType: value, Path: "/"}}
			},
			wire: func(value string) map[string]any { return qaNativeWire("/", value) },
		},
		{
			name: "reason", valid: exact, invalid: []string{over, "OK\r\nx-private: value", string([]byte{0xff})},
			direct: func(value string) v1.Command {
				return v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.HTTPResponsePayload{StatusCode: 200, Reason: value}}}
			},
			private: func(value string) model.Payload {
				return model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: value}}
			},
			wire: func(value string) map[string]any { return qaHTTPResponseWire(value) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := adapter.DecodeCommand(context.Background(), tc.direct(tc.valid)); err != nil {
				t.Fatalf("direct exact text rejected: %v", err)
			}
			if _, err := adapter.MapPayload(tc.private(tc.valid)); err != nil {
				t.Fatalf("private exact text rejected: %v", err)
			}
			args := map[string]any{"to": "peer", "payload": tc.wire(tc.valid)}
			if _, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandMessageSend, args)); err != nil {
				t.Fatalf("ABI exact text rejected: %v", err)
			}
			mappedPayload, err := adapter.MapPayload(tc.private(tc.valid))
			if err != nil {
				t.Fatalf("map exact payload for output: %v", err)
			}
			publicCompletion := func(payload v1.Payload) v1.Completion {
				return v1.Completion{CommandID: "command", OK: true, Result: v1.ResponseResult{MessageID: "message", ConversationID: "conversation", FromAgentID: "peer", MeshID: "mesh", Payload: payload}}
			}
			if _, err := adapter.EncodeABICompletion(publicCompletion(mappedPayload)); err != nil {
				t.Fatalf("strict output rejected exact metadata: %v", err)
			}
			for index, invalid := range tc.invalid {
				target := ErrMalformedInput
				if index == 0 {
					target = ErrInputTooLarge
				}
				_, err := adapter.DecodeCommand(context.Background(), tc.direct(invalid))
				qaAssertError(t, err, target)
				_, err = adapter.MapPayload(tc.private(invalid))
				qaAssertError(t, err, target)
				var invalidPublic v1.Payload
				switch value := tc.private(invalid).Value.(type) {
				case model.NativePayload:
					invalidPublic.Value = v1.NativePayload{ContentType: value.ContentType, Path: value.Path}
				case model.HTTPResponsePayload:
					invalidPublic.Value = v1.HTTPResponsePayload{StatusCode: value.StatusCode, Reason: value.Reason}
				}
				_, err = adapter.EncodeABICompletion(publicCompletion(invalidPublic))
				qaAssertError(t, err, target)
			}
		})
	}
}

func TestQAStrictABIRejectsRawInvalidUTF8Metadata(t *testing.T) {
	adapter, _ := New()
	cases := []struct {
		name   string
		prefix string
		suffix string
	}{
		{
			name:   "content type",
			prefix: `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"agent","payload":{"native":{"content_type":"`,
			suffix: `","path":"/","body":""}}}}`,
		},
		{
			name:   "reason",
			prefix: `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"agent","payload":{"http_response":{"status_code":200,"reason":"`,
			suffix: `","headers":[],"body":""}}}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := append([]byte(tc.prefix), 0xff)
			input = append(input, tc.suffix...)
			if _, err := adapter.DecodeABICommand(input); !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("raw invalid UTF-8 error=%v, want malformed input", err)
			}
		})
	}
}

func qaRawContentTypeCommand(value string) []byte {
	return []byte(fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"%s","path":"/","body":""}}}}`, value))
}

func TestQAStrictABISurrogateEscapeContract(t *testing.T) {
	adapter, _ := New()
	invalid := map[string]string{
		"lone high":           `application/\ud800`,
		"lone low":            `application/\udc00`,
		"reversed pair":       `application/\udc00\ud800`,
		"high then non-low":   `application/\ud800\u0041`,
		"high then text":      `application/\ud800x`,
		"incomplete hex":      `application/\ud80`,
		"incomplete low quad": `application/\ud800\u123`,
	}
	for name, value := range invalid {
		t.Run("reject/"+name, func(t *testing.T) {
			_, err := adapter.DecodeABICommand(qaRawContentTypeCommand(value))
			qaAssertError(t, err, ErrMalformedInput)
		})
	}

	valid := map[string]string{
		"valid pair":          `application/\ud83d\ude80`,
		"escaped replacement": `application/\ufffd`,
		"literal replacement": "application/�",
		"escaped backslash":   `application/\\ud800`,
	}
	for name, value := range valid {
		t.Run("accept/"+name, func(t *testing.T) {
			if _, err := adapter.DecodeABICommand(qaRawContentTypeCommand(value)); err != nil {
				t.Fatalf("valid Unicode representation rejected: %v", err)
			}
		})
	}

	invalidConfig := []byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue\ud800_limit":1,"payload_limit":1}`)
	if _, err := adapter.DecodeABIConfig(invalidConfig); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("config surrogate escape error=%v, want malformed input", err)
	}
}

func TestQAStrictABIMalformedJSONAndBase64FailClosed(t *testing.T) {
	adapter, _ := New()
	inputs := []string{
		`{"abi_version":1`,
		`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"","path":"/","path":"/duplicate","body":""}}}}`,
		`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"","path":"/","body":"%%%not-base64%%%"}}}}`,
	}
	for _, input := range inputs {
		_, err := adapter.DecodeABICommand([]byte(input))
		qaAssertError(t, err, ErrMalformedInput)
	}
}

func TestQAPolicyRuleCountIsBoundedInBothDirections(t *testing.T) {
	adapter, _ := New()
	publicRules := make([]v1.PolicyRule, 1025)
	privateRules := make([]model.PolicyRule, 1025)
	wireRules := make([]any, 1025)
	for index := range publicRules {
		publicRules[index] = v1.PolicyRule{Action: v1.PolicyActionAllow, Path: "", AgentID: "peer"}
		privateRules[index] = model.PolicyRule{Action: "allow", Path: "", AgentID: "peer"}
		wireRules[index] = map[string]any{"action": "allow", "path": "", "agent_id": "peer"}
	}

	t.Run("direct input", func(t *testing.T) {
		if _, err := adapter.DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: qaCommandBase(), Rules: publicRules[:1024]}); err != nil {
			t.Fatalf("1,024-rule policy rejected: %v", err)
		}
		_, err := adapter.DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: qaCommandBase(), Rules: publicRules})
		qaAssertError(t, err, ErrInputTooLarge)
	})
	t.Run("ABI input", func(t *testing.T) {
		if _, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandPolicySet, map[string]any{"rules": wireRules[:1024]})); err != nil {
			t.Fatalf("1,024-rule policy rejected: %v", err)
		}
		_, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandPolicySet, map[string]any{"rules": wireRules}))
		qaAssertError(t, err, ErrInputTooLarge)
	})
	t.Run("private projection", func(t *testing.T) {
		privateAtLimit, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: model.PolicyResult{Rules: privateRules[:1024], Allowed: true}})
		if err != nil {
			t.Fatalf("1,024-rule policy rejected: %v", err)
		}
		if _, err := adapter.EncodeABICompletion(privateAtLimit); err != nil {
			t.Fatalf("mapped 1,024-rule policy failed encoding: %v", err)
		}
		_, err = adapter.MapCompletion(model.Result{CommandID: "command", Value: model.PolicyResult{Rules: privateRules, Allowed: true}})
		qaAssertError(t, err, ErrInputTooLarge)
	})
	t.Run("public ABI output", func(t *testing.T) {
		if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: v1.PolicyResult{Rules: publicRules[:1024], Allowed: true}}); err != nil {
			t.Fatalf("1,024-rule policy failed encoding: %v", err)
		}
		_, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: v1.PolicyResult{Rules: publicRules, Allowed: true}})
		qaAssertError(t, err, ErrInputTooLarge)
	})
}

func TestQARejectedPathsAreValidatedBeforeOwnedCopies(t *testing.T) {
	adapter, _ := New()
	over := qaExactUnicodePath() + "b"
	body := make([]byte, maxInlinePayloadBytes-1)
	headers := make([]v1.Header, maxHeaderCount)
	for index := range headers {
		headers[index] = v1.Header{Name: "x"}
	}
	commands := []v1.Command{
		v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{Path: over, Body: body}}},
		v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.HTTPRequestPayload{Method: "POST", Path: over, Headers: headers, Body: body}}},
		v1.PolicySetCommand{CommandBase: qaCommandBase(), Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: over}}},
	}
	for index, command := range commands {
		allocations := testing.AllocsPerRun(25, func() {
			_, err := adapter.DecodeCommand(context.Background(), command)
			if !errors.Is(err, ErrInputTooLarge) {
				panic("unexpected path result")
			}
		})
		// One allocation is the normalized error/interface path itself. The large
		// caller-owned body/header storage must not be copied before rejection.
		if allocations > 1 {
			t.Fatalf("command %d allocated %.0f objects before rejecting its path", index, allocations)
		}
	}
}
