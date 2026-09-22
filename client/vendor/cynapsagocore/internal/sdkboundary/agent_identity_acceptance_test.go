package sdkboundary

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestMessageAndPolicyCommandsRejectMalformedTypedDestinations(t *testing.T) {
	t.Parallel()
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	payload := v1.Payload{Value: v1.NativePayload{Path: "/"}}
	malformed := []struct {
		name   string
		value  string
		target error
	}{
		{name: "empty", value: "", target: ErrMalformedInput},
		{name: "overlong", value: strings.Repeat("a", maxAgentIdentityLength+1), target: ErrInputTooLarge},
		{name: "invalid UTF-8", value: string([]byte{0xff}), target: ErrMalformedInput},
		{name: "NUL", value: "peer\x00injected", target: ErrMalformedInput},
		{name: "ASCII control", value: "peer\nidentity", target: ErrMalformedInput},
		{name: "Unicode control", value: "peer\u0085identity", target: ErrMalformedInput},
	}
	commands := []struct {
		name string
		make func(v1.AgentID) v1.Command
	}{
		{name: "message.send", make: func(to v1.AgentID) v1.Command {
			return v1.MessageSendCommand{CommandBase: qaCommandBase(), To: to, Payload: payload}
		}},
		{name: "message.request", make: func(to v1.AgentID) v1.Command {
			return v1.MessageRequestCommand{CommandBase: qaCommandBase(), To: to, Payload: payload}
		}},
		{name: "policy.test", make: func(to v1.AgentID) v1.Command {
			return v1.PolicyTestCommand{CommandBase: qaCommandBase(), Input: v1.PolicyTestInput{To: to, Payload: payload}}
		}},
	}
	for _, command := range commands {
		command := command
		for _, invalid := range malformed {
			invalid := invalid
			t.Run(command.name+"/"+invalid.name, func(t *testing.T) {
				t.Parallel()
				if _, err := adapter.DecodeCommand(context.Background(), command.make(v1.AgentID(invalid.value))); !errors.Is(err, invalid.target) {
					t.Fatalf("destination %q error=%v, want %v", invalid.name, err, invalid.target)
				}
			})
		}
	}

	exact := strings.Repeat("a", maxAgentIdentityLength)
	if _, err := adapter.DecodeCommand(context.Background(), v1.MessageSendCommand{CommandBase: qaCommandBase(), To: v1.AgentID(exact), Payload: payload}); err != nil {
		t.Fatalf("exact %d-byte destination rejected: %v", maxAgentIdentityLength, err)
	}
}

func TestMessageAndPolicyABIDecodersRejectMalformedDestinations(t *testing.T) {
	t.Parallel()
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	payload := qaNativeWire("/", "")
	commands := []struct {
		name v1.CommandName
		args func(string) any
	}{
		{name: v1.CommandMessageSend, args: func(to string) any {
			return map[string]any{"to": to, "payload": payload}
		}},
		{name: v1.CommandMessageRequest, args: func(to string) any {
			return map[string]any{"to": to, "payload": payload, "ttl_ms": 0}
		}},
		{name: v1.CommandPolicyTest, args: func(to string) any {
			return map[string]any{"input": map[string]any{"to": to, "payload": payload}}
		}},
	}
	for _, command := range commands {
		command := command
		for _, invalid := range []struct {
			name   string
			value  string
			target error
		}{
			{name: "empty", target: ErrMalformedInput},
			{name: "overlong", value: strings.Repeat("a", maxAgentIdentityLength+1), target: ErrInputTooLarge},
			{name: "NUL", value: "peer\x00injected", target: ErrMalformedInput},
			{name: "control", value: "peer\nidentity", target: ErrMalformedInput},
		} {
			invalid := invalid
			t.Run(string(command.name)+"/"+invalid.name, func(t *testing.T) {
				t.Parallel()
				if _, err := adapter.DecodeABICommand(qaABICommand(t, command.name, command.args(invalid.value))); !errors.Is(err, invalid.target) {
					t.Fatalf("destination %q error=%v, want %v", invalid.name, err, invalid.target)
				}
			})
		}

		t.Run(string(command.name)+"/invalid UTF-8", func(t *testing.T) {
			t.Parallel()
			wire := qaABICommand(t, command.name, command.args("INVALID_UTF8_MARKER"))
			wire = bytes.Replace(wire, []byte("INVALID_UTF8_MARKER"), []byte{0xff}, 1)
			if _, err := adapter.DecodeABICommand(wire); !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("invalid UTF-8 destination error=%v", err)
			}
		})
	}
}
