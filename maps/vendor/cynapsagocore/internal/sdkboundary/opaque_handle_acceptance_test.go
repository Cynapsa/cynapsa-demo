package sdkboundary

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestOpaqueHandleClassesHaveExactFrozenCanonicalShapes(t *testing.T) {
	var scope [16]byte
	for index := range scope {
		scope[index] = byte(0xf0 + index)
	}
	command, err := NewCommandHandle(scope, ^uint64(0))
	if err != nil {
		t.Fatalf("new command handle: %v", err)
	}
	request, err := NewRequestHandle()
	if err != nil {
		t.Fatalf("new request handle: %v", err)
	}
	payload, err := NewPayloadHandle()
	if err != nil {
		t.Fatalf("new payload handle: %v", err)
	}

	tests := []struct {
		name       string
		handle     string
		prefix     string
		decodedLen int
		validate   func(string) error
	}{
		{name: "command", handle: command, prefix: commandHandlePrefix, decodedLen: commandHandleBytes, validate: func(value string) error { return validateCommandHandle(v1.CommandHandle(value)) }},
		{name: "request", handle: request, prefix: requestHandlePrefix, decodedLen: requestHandleBytes, validate: func(value string) error { return validateRequestHandle(v1.RequestHandle(value)) }},
		{name: "payload", handle: payload, prefix: payloadHandlePrefix, decodedLen: payloadHandleBytes, validate: func(value string) error { return validatePayloadHandle(v1.PayloadHandle(value)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.validate(test.handle); err != nil {
				t.Fatalf("generated handle invalid: %v", err)
			}
			encoded := strings.TrimPrefix(test.handle, test.prefix)
			decoded, err := base64.RawURLEncoding.DecodeString(encoded)
			if err != nil || len(decoded) != test.decodedLen {
				t.Fatalf("decoded length = %d, error = %v", len(decoded), err)
			}
			if strings.Contains(encoded, "=") || test.prefix+base64.RawURLEncoding.EncodeToString(decoded) != test.handle {
				t.Fatalf("noncanonical handle = %q", test.handle)
			}

			mutations := []string{
				strings.TrimSuffix(test.handle, test.handle[len(test.handle)-1:]),
				test.handle + "A",
				test.handle + "=",
				strings.ToUpper(test.prefix) + encoded,
				"bad_" + encoded,
				test.prefix + strings.Repeat("!", len(encoded)),
			}
			for _, mutation := range mutations {
				if err := test.validate(mutation); !errors.Is(err, ErrInvalidHandle) {
					t.Errorf("mutation %q error = %v", mutation, err)
				}
			}
		})
	}

	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(command, commandHandlePrefix))
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded[:16]) != string(scope[:]) || binary.BigEndian.Uint64(decoded[16:24]) != ^uint64(0) {
		t.Fatalf("command scope/generation changed: %x", decoded[:24])
	}

	if validateCommandHandle(v1.CommandHandle(request)) == nil || validateCommandHandle(v1.CommandHandle(payload)) == nil ||
		validateRequestHandle(v1.RequestHandle(command)) == nil || validateRequestHandle(v1.RequestHandle(payload)) == nil ||
		validatePayloadHandle(v1.PayloadHandle(command)) == nil || validatePayloadHandle(v1.PayloadHandle(request)) == nil {
		t.Fatal("wrong-class opaque handle accepted")
	}
}

func TestOpaqueHandleGenerationDoesNotRepeatInBoundedAcceptanceSample(t *testing.T) {
	var scope [16]byte
	seen := make(map[string]struct{}, 6144)
	for generation := uint64(0); generation < 2048; generation++ {
		command, commandErr := NewCommandHandle(scope, generation)
		request, requestErr := NewRequestHandle()
		payload, payloadErr := NewPayloadHandle()
		if commandErr != nil || requestErr != nil || payloadErr != nil {
			t.Fatalf("generation %d errors: command=%v request=%v payload=%v", generation, commandErr, requestErr, payloadErr)
		}
		for _, handle := range []string{command, request, payload} {
			if _, duplicate := seen[handle]; duplicate {
				t.Fatalf("duplicate opaque handle at generation %d", generation)
			}
			seen[handle] = struct{}{}
		}
	}
}
