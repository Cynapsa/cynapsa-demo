package sdkboundary

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestCanonicalHTTPPayloadRoundTripPreservesExactApplicationData(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	tests := []struct {
		name        string
		payloadJSON string
		assert      func(*testing.T, model.Payload)
	}{
		{
			name: "request",
			payloadJSON: `{"http_request":{"method":"PATCH","path":"/orders/%2Fraw","query":"x=1&x=&x=two%20words","headers":[` +
				`{"name":"X-Mixed-Case","value":" first "},{"name":"Set-Cookie","value":"a=1; Path=/"},{"name":"Set-Cookie","value":"b=2; Path=/"}],` +
				`"body":"AAECf4D+/w=="}}`,
			assert: func(t *testing.T, payload model.Payload) {
				value, ok := payload.Value.(model.HTTPRequestPayload)
				if !ok {
					t.Fatalf("variant = %T, want HTTPRequestPayload", payload.Value)
				}
				wantHeaders := []model.Header{
					{Name: "X-Mixed-Case", Value: " first "},
					{Name: "Set-Cookie", Value: "a=1; Path=/"},
					{Name: "Set-Cookie", Value: "b=2; Path=/"},
				}
				if value.Method != "PATCH" || value.Path != "/orders/%2Fraw" || value.Query != "x=1&x=&x=two%20words" ||
					!reflect.DeepEqual(value.Headers, wantHeaders) || !reflect.DeepEqual(value.Body, []byte{0, 1, 2, 127, 128, 254, 255}) {
					t.Fatalf("request lost fidelity: %#v", value)
				}
			},
		},
		{
			name: "response",
			payloadJSON: `{"http_response":{"status_code":418,"reason":"I'm a teapot","headers":[` +
				`{"name":"Warning","value":"one"},{"name":"Warning","value":"two"},{"name":"Content-Type","value":"application/octet-stream"},{"name":"x-cynapsa-error","value":"ordinary application header"}],` +
				`"body":"/wCAfwIBAA==","error":{"code":"teapot","detail":"Short and stout","details_json":"{\"safe\":true}"}}}`,
			assert: func(t *testing.T, payload model.Payload) {
				value, ok := payload.Value.(model.HTTPResponsePayload)
				if !ok {
					t.Fatalf("variant = %T, want HTTPResponsePayload", payload.Value)
				}
				wantHeaders := []model.Header{{Name: "Warning", Value: "one"}, {Name: "Warning", Value: "two"}, {Name: "Content-Type", Value: "application/octet-stream"}, {Name: "x-cynapsa-error", Value: "ordinary application header"}}
				if value.StatusCode != 418 || value.Reason != "I'm a teapot" || !reflect.DeepEqual(value.Headers, wantHeaders) ||
					!reflect.DeepEqual(value.Body, []byte{255, 0, 128, 127, 2, 1, 0}) || value.Error == nil || value.Error.Code != "teapot" || value.Error.Detail != "Short and stout" || value.Error.DetailsJSON != `{"safe":true}` {
					t.Fatalf("response lost fidelity: %#v", value)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := `{"abi_version":1,"command_id":"command-1","command_name":"message.send","sdk_session_id":"session-1","args":{"to":"peer","payload":` + test.payloadJSON + `}}`
			publicCommand, err := adapter.DecodeABICommand([]byte(input))
			if err != nil {
				t.Fatalf("decode ABI command: %v", err)
			}
			privateCommand, err := adapter.DecodeCommand(context.Background(), publicCommand)
			if err != nil {
				t.Fatalf("map command: %v", err)
			}
			privateCommand = takeFrozenCommandForTest(t, privateCommand)
			privatePayload := privateCommand.Args.(model.MessageSendArgs).Payload
			test.assert(t, privatePayload)

			publicPayload, err := adapter.MapPayload(privatePayload)
			if err != nil {
				t.Fatalf("map payload: %v", err)
			}
			encoded, err := adapter.EncodeABICompletion(v1.Completion{
				CommandID: "command-1",
				OK:        true,
				Result: v1.ResponseResult{
					MessageID: "message-1", ConversationID: "conversation-1", FromAgentID: "peer", MeshID: "mesh", Payload: publicPayload,
				},
			})
			if err != nil {
				t.Fatalf("encode completion: %v", err)
			}

			var envelope struct {
				Result struct {
					Payload json.RawMessage `json:"payload"`
				} `json:"result"`
			}
			if err := json.Unmarshal(encoded, &envelope); err != nil {
				t.Fatalf("unmarshal encoded completion: %v", err)
			}
			var got, want any
			if err := json.Unmarshal(envelope.Result.Payload, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(test.payloadJSON), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip payload mismatch\n got: %s\nwant: %s", envelope.Result.Payload, test.payloadJSON)
			}
		})
	}
}

func TestApplicationErrorProjectionOwnsMetadataAndPreservesHeaders(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	metadata := &model.ApplicationError{Code: "not_found", Detail: "Missing", DetailsJSON: `{"id":7}`}
	private := model.Payload{Value: model.HTTPResponsePayload{
		StatusCode: 404,
		Headers:    []model.Header{{Name: "x-cynapsa-error", Value: "ordinary application header"}},
		Error:      metadata,
	}}
	public, err := adapter.MapPayload(private)
	if err != nil {
		t.Fatal(err)
	}
	metadata.Code = "mutated"
	response := public.Value.(v1.HTTPResponsePayload)
	if response.Error == nil || response.Error.Code != "not_found" || response.Headers[0].Value != "ordinary application header" {
		t.Fatalf("public response aliases private metadata: %#v", response)
	}
	response.Error.Detail = "changed"
	if private.Value.(model.HTTPResponsePayload).Error.Detail != "Missing" {
		t.Fatal("private metadata aliases public response")
	}
}

func TestPayloadProjectionOwnsHeadersAndExactBody(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	body := []byte{0, 1, 2, 3}
	headers := []model.Header{{Name: "x-duplicate", Value: "one"}, {Name: "x-duplicate", Value: "two"}}
	private := model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/", Query: "", Headers: headers, Body: body}}
	public, err := adapter.MapPayload(private)
	if err != nil {
		t.Fatalf("map payload: %v", err)
	}

	body[0] = 9
	headers[0].Name = "mutated"
	request := public.Value.(v1.HTTPRequestPayload)
	if !reflect.DeepEqual(request.Body, []byte{0, 1, 2, 3}) || request.Headers[0].Name != "x-duplicate" || request.Headers[1].Value != "two" {
		t.Fatalf("public payload aliases private input: %#v", request)
	}

	request.Body[1] = 8
	request.Headers[1].Value = "changed"
	privateRequest := private.Value.(model.HTTPRequestPayload)
	if !reflect.DeepEqual(privateRequest.Body, []byte{9, 1, 2, 3}) || privateRequest.Headers[1].Value != "two" {
		t.Fatalf("private input aliases public projection: %#v", privateRequest)
	}
}
