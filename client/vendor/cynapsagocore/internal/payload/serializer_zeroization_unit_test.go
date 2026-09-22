package payload

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/fxamacker/cbor/v2"
)

type serializerCanaryMode struct {
	cbor.EncMode
	captured   []byte
	marshalErr error
	panicValue any
}

func (mode *serializerCanaryMode) Marshal(value any) ([]byte, error) {
	switch wire := value.(type) {
	case canonicalNativeV1:
		mode.captured = wire.Body
	case canonicalHTTPRequestV1:
		mode.captured = wire.Body
	case canonicalHTTPResponseV1:
		mode.captured = wire.Body
	}
	if mode.panicValue != nil {
		panic(mode.panicValue)
	}
	if mode.marshalErr != nil {
		return nil, mode.marshalErr
	}
	return mode.EncMode.Marshal(value)
}

func TestSerializerClearsOwnedBodyCloneOnEveryExit(t *testing.T) {
	tests := []struct {
		name  string
		value func([]byte) model.Payload
	}{
		{name: "native", value: func(body []byte) model.Payload {
			return model.Payload{Value: model.NativePayload{Path: "/canary", Body: body}}
		}},
		{name: "request", value: func(body []byte) model.Payload {
			return model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/canary", Body: body}}
		}},
		{name: "response", value: func(body []byte) model.Payload {
			return model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Body: body}}
		}},
	}
	exits := []string{"success", "error", "panic"}
	for _, test := range tests {
		for _, exit := range exits {
			t.Run(test.name+"/"+exit, func(t *testing.T) {
				serializer, err := NewSerializer(1 << 20)
				if err != nil {
					t.Fatal(err)
				}
				realMode := serializer.encode
				mode := &serializerCanaryMode{EncMode: realMode}
				sentinel := errors.New("marshal panic")
				switch exit {
				case "error":
					mode.marshalErr = errors.New("marshal error")
				case "panic":
					mode.panicValue = sentinel
				}
				serializer.encode = mode
				body := bytes.Repeat([]byte("private-body-canary"), 32)
				want := clone(body)
				if exit == "panic" {
					func() {
						defer func() {
							if recovered := recover(); recovered != sentinel {
								t.Fatalf("recovered = %v; want sentinel", recovered)
							}
						}()
						_, _ = serializer.Serialize(test.value(body))
					}()
				} else {
					encoded, serializeErr := serializer.Serialize(test.value(body))
					if exit == "error" && !errors.Is(serializeErr, ErrMalformedCanonical) {
						t.Fatalf("Serialize error = %v", serializeErr)
					}
					if exit == "success" && (serializeErr != nil || len(encoded) == 0) {
						t.Fatalf("Serialize success = %d bytes, %v", len(encoded), serializeErr)
					}
					zero(encoded)
				}
				if !bytes.Equal(body, want) {
					t.Fatal("serializer mutated caller-owned body")
				}
				if len(mode.captured) != len(body) || !bytes.Equal(mode.captured, make([]byte, len(mode.captured))) {
					t.Fatal("serializer retained non-zero bytes in its owned body clone")
				}
				zero(want)
				zero(body)
			})
		}
	}
}

func TestSerializerZeroCopyValidateParityAndAllocations(t *testing.T) {
	serializer, err := NewSerializer(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	values := []model.Payload{
		{Value: model.NativePayload{Path: "/empty"}},
		{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/native", Body: bytes.Repeat([]byte{0xa5}, 4096)}},
		{Value: model.HTTPRequestPayload{Method: "GET", Path: "/empty"}},
		{Value: model.HTTPRequestPayload{
			Method: "POST", Path: "/request", Query: "name=%E2%9C%93",
			Headers: []model.Header{{Name: "Content-Type", Value: "application/octet-stream"}, {Name: "X-Test", Value: "value"}},
			Body:    bytes.Repeat([]byte{0x5a}, 4096),
		}},
		{Value: model.HTTPResponsePayload{StatusCode: 204}},
		{Value: model.HTTPResponsePayload{
			StatusCode: 206, Reason: "Partial Content",
			Headers: []model.Header{{Name: "Content-Type", Value: "application/octet-stream"}},
			Body:    bytes.Repeat([]byte{0x3c}, 4096),
		}},
	}
	for index, value := range values {
		encoded, err := serializer.Serialize(value)
		if err != nil {
			t.Fatalf("value %d Serialize: %v", index, err)
		}
		if err := serializer.Validate(encoded); err != nil {
			t.Fatalf("value %d Validate: %v", index, err)
		}
		decoded, err := serializer.Deserialize(encoded)
		if err != nil {
			t.Fatalf("value %d Deserialize: %v", index, err)
		}
		zeroModelPayload(decoded)
		allocations := testing.AllocsPerRun(1000, func() {
			if err := serializer.Validate(encoded); err != nil {
				panic(err)
			}
		})
		if allocations != 0 {
			t.Fatalf("value %d Validate allocations = %.2f; want 0", index, allocations)
		}
		for end := 0; end < len(encoded); end++ {
			if err := serializer.Validate(encoded[:end]); err == nil {
				t.Fatalf("value %d accepted truncation at %d", index, end)
			}
		}
		zero(encoded)
	}
}

func TestSerializerZeroCopyValidateRejectsAlternativeEncodingAndUnknownFields(t *testing.T) {
	serializer, err := NewSerializer(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := serializer.Serialize(model.Payload{Value: model.NativePayload{Path: "/", Body: []byte("body")}})
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		append(clone(encoded), 0),
		append([]byte{0xb8, 0x05}, encoded[1:]...),             // non-shortest map length
		append([]byte{encoded[0], 0x18, 0x00}, encoded[2:]...), // non-shortest first key
	}
	wrongKey := clone(encoded)
	wrongKey[1] = 1
	tests = append(tests, wrongKey)
	for index, candidate := range tests {
		if err := serializer.Validate(candidate); err == nil {
			t.Fatalf("accepted alternative encoding %d: %x", index, candidate)
		}
		zero(candidate)
	}
	zero(encoded)
}
