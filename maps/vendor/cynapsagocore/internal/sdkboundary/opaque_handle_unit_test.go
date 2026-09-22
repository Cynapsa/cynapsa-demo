package sdkboundary

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestCommandHandleHasFrozenScopeGenerationNonceLayout(t *testing.T) {
	var scope [16]byte
	for index := range scope {
		scope[index] = byte(index + 1)
	}
	handle, err := NewCommandHandle(scope, 42)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCommandHandle(v1.CommandHandle(handle)); err != nil {
		t.Fatalf("validate generated command handle: %v", err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(handle, commandHandlePrefix))
	if err != nil || len(decoded) != commandHandleBytes {
		t.Fatalf("command handle=%q decoded=%d error=%v", handle, len(decoded), err)
	}
	if string(decoded[:16]) != string(scope[:]) || binary.BigEndian.Uint64(decoded[16:24]) != 42 {
		t.Fatalf("command scope/generation layout changed: %x", decoded[:24])
	}
}

func TestRequestAndPayloadHandlesHaveDistinctFrozenFormats(t *testing.T) {
	request, err := NewRequestHandle()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := NewPayloadHandle()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRequestHandle(v1.RequestHandle(request)); err != nil {
		t.Fatalf("request handle: %v", err)
	}
	if err := validatePayloadHandle(v1.PayloadHandle(payload)); err != nil {
		t.Fatalf("payload handle: %v", err)
	}
	if validateRequestHandle(v1.RequestHandle(payload)) == nil || validatePayloadHandle(v1.PayloadHandle(request)) == nil || validateCommandHandle(v1.CommandHandle(request)) == nil {
		t.Fatal("wrong handle class was accepted")
	}
}

func TestRandomClassHandlesAreUnique(t *testing.T) {
	seen := make(map[string]struct{}, 2048)
	for index := 0; index < 1024; index++ {
		request, requestErr := NewRequestHandle()
		payload, payloadErr := NewPayloadHandle()
		if requestErr != nil || payloadErr != nil {
			t.Fatalf("generation errors: request=%v payload=%v", requestErr, payloadErr)
		}
		for _, handle := range []string{request, payload} {
			if _, duplicate := seen[handle]; duplicate {
				t.Fatalf("duplicate handle %q", handle)
			}
			seen[handle] = struct{}{}
		}
	}
}
