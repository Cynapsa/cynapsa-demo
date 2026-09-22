package sdkboundary

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestCanonicalPayloadAndChunkExactByteBoundaries(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	base := v1.CommandBase{CommandID: "command", SDKSessionID: "session"}

	tests := []struct {
		name    string
		atLimit v1.Payload
		over    v1.Payload
	}{
		{
			name:    "native",
			atLimit: v1.Payload{Value: v1.NativePayload{Path: "/", Body: make([]byte, maxInlinePayloadBytes-1)}},
			over:    v1.Payload{Value: v1.NativePayload{Path: "/", Body: make([]byte, maxInlinePayloadBytes)}},
		},
		{
			name:    "http-request",
			atLimit: v1.Payload{Value: v1.HTTPRequestPayload{Method: "G", Path: "/", Body: make([]byte, maxInlinePayloadBytes-2)}},
			over:    v1.Payload{Value: v1.HTTPRequestPayload{Method: "G", Path: "/", Body: make([]byte, maxInlinePayloadBytes-1)}},
		},
		{
			name:    "http-response",
			atLimit: v1.Payload{Value: v1.HTTPResponsePayload{StatusCode: 200, Reason: "O", Body: make([]byte, maxInlinePayloadBytes-1)}},
			over:    v1.Payload{Value: v1.HTTPResponsePayload{StatusCode: 200, Reason: "O", Body: make([]byte, maxInlinePayloadBytes)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			atLimit := v1.MessageSendCommand{CommandBase: base, To: "peer", Payload: test.atLimit}
			if _, err := adapter.DecodeCommand(context.Background(), atLimit); err != nil {
				t.Fatalf("exact 256 KiB canonical payload rejected: %v", err)
			}
			over := v1.MessageSendCommand{CommandBase: base, To: "peer", Payload: test.over}
			if _, err := adapter.DecodeCommand(context.Background(), over); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("first byte over canonical payload error = %v", err)
			}
		})
	}

	atChunkLimit := v1.PayloadWriteCommand{CommandBase: base, Handle: validPayloadHandle, Chunk: make([]byte, maxPayloadChunkBytes)}
	if _, err := adapter.DecodeCommand(context.Background(), atChunkLimit); err != nil {
		t.Fatalf("exact 1 MiB chunk rejected: %v", err)
	}
	overChunkLimit := v1.PayloadWriteCommand{CommandBase: base, Handle: validPayloadHandle, Chunk: make([]byte, maxPayloadChunkBytes+1)}
	if _, err := adapter.DecodeCommand(context.Background(), overChunkLimit); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("first byte over chunk error = %v", err)
	}
}

func TestCanonicalSnapshotAndABIInputExactBoundaries(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if _, err := adapter.MapConfig(v1.Config{QueueLimit: 1, PayloadLimit: maxCanonicalPayloadSize}); err != nil {
		t.Fatalf("exact %d-byte snapshot limit rejected: %v", v1.MaximumPayloadBytes, err)
	}
	if _, err := adapter.MapConfig(v1.Config{QueueLimit: 1, PayloadLimit: maxCanonicalPayloadSize + 1}); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("first byte over snapshot limit error = %v", err)
	}

	base := `{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1}`
	atLimit := base + strings.Repeat(" ", maxABIInputBytes-len(base))
	if len(atLimit) != maxABIInputBytes {
		t.Fatalf("test input length = %d", len(atLimit))
	}
	config, err := adapter.DecodeABIConfig([]byte(atLimit))
	if err != nil {
		t.Fatalf("exact maximum ABI input rejected: %v", err)
	}
	want := v1.Config{CommandTimeout: 30 * time.Second, RPCTimeout: 30 * time.Second, QueueLimit: 1, PayloadLimit: 1}
	if !reflect.DeepEqual(config, want) {
		t.Fatalf("normalized ABI config = %#v, want %#v", config, want)
	}
	overLimit := atLimit + " "
	if _, err := adapter.DecodeABIConfig([]byte(overLimit)); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("first byte over ABI input error = %v", err)
	}
}
