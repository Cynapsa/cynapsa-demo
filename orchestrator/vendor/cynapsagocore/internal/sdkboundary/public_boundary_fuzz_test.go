package sdkboundary

import (
	"context"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func FuzzStrictABICommandDecoder(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"abi_version":1,"command_id":"command","command_name":"core.status","sdk_session_id":"session","args":{}}`),
		[]byte(`{"abi_version":1,"command_id":"command","command_name":"message.send","sdk_session_id":"session","args":{"to":"peer","payload":{"native":{"content_type":"application/octet-stream","path":"/","body":"AAEC/w=="}}}}`),
		[]byte(`{"abi_version":1,"abi_version":1,"command_id":"command","command_name":"core.status","sdk_session_id":"session","args":{}}`),
		[]byte(`{"abi_version":1,"command_id":"command","command_name":"core.status","sdk_session_id":"session","args":{}} trailing`),
		[]byte{0xff, 0xfe, 0xfd},
		nil,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		adapter, err := New()
		if err != nil {
			t.Fatalf("new adapter: %v", err)
		}
		command, err := adapter.DecodeABICommand(data)
		if err != nil {
			return
		}
		if command == nil || !IsPublicCommand(command.Name()) {
			t.Fatalf("decoder admitted unknown command %#v", command)
		}
		if _, err := adapter.DecodeCommand(context.Background(), command); err != nil {
			t.Fatalf("accepted ABI command failed typed validation: %v", err)
		}
	})
}

func FuzzStrictABIConfigDecoder(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1}`),
		[]byte(`{"abi_version":1,"command_timeout_ms":1,"rpc_timeout_ms":2,"queue_limit":65536,"payload_limit":134217696}`),
		[]byte(`{"abi_version":1,"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":1}`),
		[]byte(`{"abi_version":1,"command_timeout_ms":-1,"rpc_timeout_ms":0,"queue_limit":0,"payload_limit":134217697}`),
		nil,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		adapter, err := New()
		if err != nil {
			t.Fatalf("new adapter: %v", err)
		}
		config, err := adapter.DecodeABIConfig(data)
		if err != nil {
			return
		}
		if _, err := adapter.MapConfig(config); err != nil {
			t.Fatalf("accepted ABI config failed typed validation: %v", err)
		}
	})
}

func FuzzOpaqueHandleClassValidation(f *testing.F) {
	var scope [16]byte
	command, err := NewCommandHandle(scope, 1)
	if err != nil {
		f.Fatal(err)
	}
	request, err := NewRequestHandle()
	if err != nil {
		f.Fatal(err)
	}
	payload, err := NewPayloadHandle()
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{command, request, payload, "", "cmdh_", "reqh_", "payh_", request + "=", "payload-1"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		accepted := 0
		if validateCommandHandle(v1.CommandHandle(value)) == nil {
			accepted++
		}
		if validateRequestHandle(v1.RequestHandle(value)) == nil {
			accepted++
		}
		if validatePayloadHandle(v1.PayloadHandle(value)) == nil {
			accepted++
		}
		if accepted > 1 {
			t.Fatalf("one opaque handle matched %d classes: %q", accepted, value)
		}
	})
}
