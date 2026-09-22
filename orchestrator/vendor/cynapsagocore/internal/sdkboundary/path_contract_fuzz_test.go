package sdkboundary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func FuzzQAApplicationPathContract(f *testing.F) {
	for _, seed := range []string{"", "/", "/orders", "/orders?x=1", "/orders#new", qaExactUnicodePath(), qaExactUnicodePath() + "b", "relative", "/交易"} {
		f.Add(seed)
	}
	adapter, _ := New()
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > maxPathLength+128 || !utf8.ValidString(path) {
			return
		}
		var target error
		switch {
		case path == "" || !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#"):
			target = ErrMalformedInput
		case len(path) > maxPathLength:
			target = ErrInputTooLarge
		}

		check := func(name string, err error) {
			t.Helper()
			if target == nil && err != nil {
				t.Fatalf("%s rejected valid %d-byte path: %v", name, len(path), err)
			}
			if target != nil && !errors.Is(err, target) {
				t.Fatalf("%s error=%v, want %v for %q", name, err, target, path)
			}
		}

		_, err := adapter.DecodeCommand(context.Background(), v1.HandlerRegisterCommand{CommandBase: qaCommandBase(), Path: path})
		check("handler", err)
		_, err = adapter.DecodeCommand(context.Background(), v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{Path: path}}})
		check("public payload", err)
		_, err = adapter.MapPayload(model.Payload{Value: model.NativePayload{Path: path}})
		check("private payload", err)
		input := qaABICommand(t, v1.CommandMessageSend, map[string]any{"to": "peer", "payload": qaNativeWire(path, "")})
		_, err = adapter.DecodeABICommand(input)
		check("ABI payload", err)

		_, policyErr := adapter.DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: qaCommandBase(), Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path}}})
		if path == "" {
			if policyErr != nil {
				t.Fatalf("policy wildcard rejected: %v", policyErr)
			}
		} else {
			check("policy", policyErr)
		}
	})
}

func FuzzQATextMetadataContract(f *testing.F) {
	for _, seed := range []string{"", "application/json", "OK", "line\r\nbreak", strings.Repeat("é", 256), strings.Repeat("é", 256) + "a", string([]byte{0xff})} {
		f.Add(seed)
	}
	adapter, _ := New()
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > maxIdentifierLength+64 {
			return
		}
		var target error
		switch {
		case len(value) > maxIdentifierLength:
			target = ErrInputTooLarge
		case !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n"):
			target = ErrMalformedInput
		}
		check := func(name string, err error) {
			t.Helper()
			if target == nil && err != nil {
				t.Fatalf("%s rejected valid metadata: %v", name, err)
			}
			if target != nil && !errors.Is(err, target) {
				t.Fatalf("%s error=%v, want %v", name, err, target)
			}
		}
		check("content validator", validateContentType(value))
		check("reason validator", validateReason(value))
		_, err := adapter.DecodeCommand(context.Background(), v1.MessageSendCommand{CommandBase: qaCommandBase(), To: "peer", Payload: v1.Payload{Value: v1.NativePayload{Path: "/", ContentType: value}}})
		check("public content type", err)
		_, err = adapter.MapPayload(model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: value}})
		check("private reason", err)
	})
}

func FuzzQAStrictABICommandBytes(f *testing.F) {
	f.Add([]byte(`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"","path":"/","body":""}}}}`))
	f.Add([]byte(`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"","path":"/","body":"%%%"}}}}`))
	adapter, _ := New()
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 16<<10 {
			return
		}
		command, err := adapter.DecodeABICommand(input)
		if err != nil {
			return
		}
		// Any successful decoding must also remain representable as strict JSON
		// and pass the direct public-to-private decoder.
		if _, err := json.Marshal(command); err != nil {
			t.Fatalf("successful command is not JSON representable: %v", err)
		}
		if _, err := adapter.DecodeCommand(context.Background(), command); err != nil {
			t.Fatalf("ABI decoder returned a command rejected by direct decode: %v", err)
		}
	})
}

func FuzzQAStrictABISurrogateSequences(f *testing.F) {
	for _, seed := range []struct {
		first, second uint16
		escaped       bool
	}{
		{0xd83d, 0xde80, false}, // valid pair
		{0xd800, 0x0041, false}, // high then non-low
		{0xdc00, 0xd800, false}, // reversed
		{0xfffd, 0x0041, false}, // replacement character
		{0xd800, 0xdc00, true},  // escaped backslashes are literal text
	} {
		f.Add(seed.first, seed.second, seed.escaped)
	}
	adapter, _ := New()
	f.Fuzz(func(t *testing.T, first, second uint16, escaped bool) {
		prefix := `\u`
		if escaped {
			prefix = `\\u`
		}
		value := fmt.Sprintf("application/%s%04x%s%04x", prefix, first, prefix, second)
		_, err := adapter.DecodeABICommand(qaRawContentTypeCommand(value))
		if escaped {
			if err != nil {
				t.Fatalf("escaped backslash sequence rejected: %v", err)
			}
			return
		}
		isHigh := func(value uint16) bool { return value >= 0xd800 && value <= 0xdbff }
		isLow := func(value uint16) bool { return value >= 0xdc00 && value <= 0xdfff }
		isSurrogate := func(value uint16) bool { return isHigh(value) || isLow(value) }
		validSequence := isHigh(first) && isLow(second) || !isSurrogate(first) && !isSurrogate(second)
		validMetadata := first != '\r' && first != '\n' && second != '\r' && second != '\n'
		if validSequence && validMetadata {
			if err != nil {
				t.Fatalf("valid sequence %04x %04x rejected: %v", first, second, err)
			}
			return
		}
		if !errors.Is(err, ErrMalformedInput) {
			t.Fatalf("invalid sequence %04x %04x error=%v, want malformed input", first, second, err)
		}
	})
}
