package sdkboundary

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestMessageTimingContractHasOneRequestTTLAndNoPublicPriority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		value  any
		fields []string
	}{
		{"send", v1.MessageSendCommand{}, []string{"CommandBase", "To", "Payload"}},
		{"request", v1.MessageRequestCommand{}, []string{"CommandBase", "To", "Payload", "TTL"}},
		{"reply", v1.MessageReplyCommand{}, []string{"CommandBase", "RequestHandle", "Payload"}},
		{"policy input", v1.PolicyTestInput{}, []string{"To", "Payload"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typeOf := reflect.TypeOf(test.value)
			if typeOf.NumField() != len(test.fields) {
				t.Fatalf("%s has %d fields, want %d", typeOf, typeOf.NumField(), len(test.fields))
			}
			for index, want := range test.fields {
				if got := typeOf.Field(index).Name; got != want {
					t.Fatalf("%s field %d = %q, want %q", typeOf, index, got, want)
				}
			}
			for _, removed := range []string{"Priority", "Timeout"} {
				if _, exists := typeOf.FieldByName(removed); exists {
					t.Fatalf("%s retains removed field %s", typeOf, removed)
				}
			}
			if test.name != "request" {
				if _, exists := typeOf.FieldByName("TTL"); exists {
					t.Fatalf("%s retains non-request TTL", typeOf)
				}
			}
		})
	}
}

func TestMessageRequestTTLTypedAndABIBoundaries(t *testing.T) {
	t.Parallel()
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	payload := v1.Payload{Value: v1.NativePayload{Path: "/", Body: []byte("request")}}
	for _, test := range []struct {
		name string
		ttl  time.Duration
	}{
		{name: "zero", ttl: 0},
		{name: "positive", ttl: 1250 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			typed := v1.MessageRequestCommand{CommandBase: qaCommandBase(), To: "peer", Payload: payload, TTL: test.ttl}
			mapped, err := adapter.DecodeCommand(context.Background(), typed)
			if err != nil {
				t.Fatalf("typed request: %v", err)
			}
			mapped = takeFrozenCommandForTest(t, mapped)
			args, ok := mapped.Args.(model.MessageRequestArgs)
			if !ok || args.TTL != test.ttl {
				t.Fatalf("typed private args = %#v", mapped.Args)
			}

			wire, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandMessageRequest, map[string]any{
				"to": "peer", "payload": qaNativeWire("/", ""), "ttl_ms": test.ttl.Milliseconds(),
			}))
			if err != nil {
				t.Fatalf("ABI request: %v", err)
			}
			request, ok := wire.(v1.MessageRequestCommand)
			if !ok || request.TTL != test.ttl {
				t.Fatalf("ABI public command = %#v", wire)
			}
			mapped, err = adapter.DecodeCommand(context.Background(), request)
			if err == nil {
				mapped = takeFrozenCommandForTest(t, mapped)
			}
			if err != nil || mapped.Args.(model.MessageRequestArgs).TTL != test.ttl {
				t.Fatalf("ABI private args = %#v, %v", mapped.Args, err)
			}
		})
	}

	negative := v1.MessageRequestCommand{CommandBase: qaCommandBase(), To: "peer", Payload: payload, TTL: -time.Millisecond}
	if _, err := adapter.DecodeCommand(context.Background(), negative); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("negative typed TTL error = %v", err)
	}
	if _, err := adapter.DecodeABICommand(qaABICommand(t, v1.CommandMessageRequest, map[string]any{
		"to": "peer", "payload": qaNativeWire("/", ""), "ttl_ms": -1,
	})); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("negative ABI TTL error = %v", err)
	}
}

func TestRemovedMessageTimingAndPriorityFieldsFailClosed(t *testing.T) {
	t.Parallel()
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	payload := qaNativeWire("/", "")
	tests := []struct {
		name string
		kind v1.CommandName
		args map[string]any
	}{
		{"send ttl", v1.CommandMessageSend, map[string]any{"to": "peer", "payload": payload, "ttl_ms": 1}},
		{"send priority", v1.CommandMessageSend, map[string]any{"to": "peer", "payload": payload, "priority": "normal"}},
		{"request timeout", v1.CommandMessageRequest, map[string]any{"to": "peer", "payload": payload, "ttl_ms": 1, "timeout_ms": 1}},
		{"request priority", v1.CommandMessageRequest, map[string]any{"to": "peer", "payload": payload, "ttl_ms": 1, "priority": "normal"}},
		{"reply ttl", v1.CommandMessageReply, map[string]any{"request_handle": qaRequestHandle(), "payload": payload, "ttl_ms": 1}},
		{"reply priority", v1.CommandMessageReply, map[string]any{"request_handle": qaRequestHandle(), "payload": payload, "priority": "normal"}},
		{"policy ttl", v1.CommandPolicyTest, map[string]any{"input": map[string]any{"to": "peer", "payload": payload, "ttl_ms": 1}}},
		{"policy priority", v1.CommandPolicyTest, map[string]any{"input": map[string]any{"to": "peer", "payload": payload, "priority": "normal"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := adapter.DecodeABICommand(qaABICommand(t, test.kind, test.args)); !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("removed field error = %v", err)
			}
		})
	}
}
