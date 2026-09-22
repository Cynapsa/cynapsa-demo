package payload

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/fxamacker/cbor/v2"
)

func TestAcceptanceCanonicalHostileDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	serializer, err := NewSerializer(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := serializer.Serialize(model.Payload{Value: model.NativePayload{
		ContentType: "application/octet-stream",
		Path:        "/safe",
		Body:        []byte("canonical-body"),
	}})
	if err != nil {
		t.Fatal(err)
	}

	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		t.Fatal(err)
	}
	marshal := func(value any) []byte {
		encoded, marshalErr := mode.Marshal(value)
		if marshalErr != nil {
			t.Fatalf("marshal hostile vector: %v", marshalErr)
		}
		return encoded
	}

	vectors := map[string][]byte{
		"empty":                     nil,
		"trailing value":            append(append([]byte(nil), valid...), 0x00),
		"tagged canonical":          append([]byte{0xd8, 0x00}, valid...),
		"indefinite map":            {0xbf, 0x00, 0x01, 0x01, 0x00, 0x02, 0x60, 0x03, 0x61, '/', 0x04, 0x40, 0xff},
		"duplicate discriminator":   {0xa6, 0x00, 0x01, 0x01, 0x00, 0x01, 0x00, 0x02, 0x60, 0x03, 0x61, '/', 0x04, 0x40},
		"missing body key":          marshal(map[uint64]any{0: uint64(1), 1: uint64(0), 2: "", 3: "/"}),
		"unknown key":               marshal(map[uint64]any{0: uint64(1), 1: uint64(0), 2: "", 3: "/", 4: []byte{}, 15: "private"}),
		"body is text":              marshal(map[uint64]any{0: uint64(1), 1: uint64(0), 2: "", 3: "/", 4: "not-bytes"}),
		"negative version":          marshal(map[uint64]any{0: int64(-1), 1: uint64(0), 2: "", 3: "/", 4: []byte{}}),
		"request short header pair": marshal(map[uint64]any{0: uint64(1), 1: uint64(1), 2: "GET", 3: "/", 4: "", 5: []any{[]any{"x"}}, 6: []byte{}}),
		"request long header pair":  marshal(map[uint64]any{0: uint64(1), 1: uint64(1), 2: "GET", 3: "/", 4: "", 5: []any{[]any{"x", "v", "extra"}}, 6: []byte{}}),
		"response signed status":    marshal(map[uint64]any{0: uint64(1), 1: uint64(2), 2: int64(-200), 3: "", 4: []any{}, 5: []byte{}}),
	}

	for name, encoded := range vectors {
		name, encoded := name, encoded
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if value, decodeErr := serializer.Deserialize(encoded); decodeErr == nil {
				t.Fatalf("hostile input accepted as %#v", value)
			}
		})
	}

	overLimit := bytes.Repeat([]byte{0}, (64<<10)+1)
	if _, err := serializer.Deserialize(overLimit); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized input error = %v, want %v", err, ErrPayloadTooLarge)
	}
}

func TestAcceptanceCanonicalMetadataExactBoundaries(t *testing.T) {
	t.Parallel()

	serializer, err := NewSerializer(MaximumCanonicalBytes)
	if err != nil {
		t.Fatal(err)
	}
	headers := make([]model.Header, MaximumHeaderCount)
	for i := range headers {
		headers[i] = model.Header{Name: "x", Value: "v"}
	}
	request := model.HTTPRequestPayload{
		Method:  strings.Repeat("A", maximumMethodBytes),
		Path:    "/" + strings.Repeat("p", maximumPathBytes-1),
		Query:   strings.Repeat("q", maximumQueryBytes),
		Headers: headers,
		Body:    []byte("body"),
	}
	encoded, err := serializer.Serialize(model.Payload{Value: request})
	if err != nil {
		t.Fatalf("exact metadata limits rejected: %v", err)
	}
	decoded, err := serializer.Deserialize(encoded)
	if err != nil {
		t.Fatalf("exact metadata limits failed decode: %v", err)
	}
	got := decoded.Value.(model.HTTPRequestPayload)
	if got.Method != request.Method || got.Path != request.Path || got.Query != request.Query || len(got.Headers) != MaximumHeaderCount || !bytes.Equal(got.Body, request.Body) {
		t.Fatal("boundary payload changed across canonical round trip")
	}

	mutations := map[string]func(*model.HTTPRequestPayload){
		"method bytes":      func(v *model.HTTPRequestPayload) { v.Method += "A" },
		"path bytes":        func(v *model.HTTPRequestPayload) { v.Path += "p" },
		"query bytes":       func(v *model.HTTPRequestPayload) { v.Query += "q" },
		"header count":      func(v *model.HTTPRequestPayload) { v.Headers = append(v.Headers, model.Header{Name: "x", Value: "v"}) },
		"path query marker": func(v *model.HTTPRequestPayload) { v.Path = "/forbidden?query" },
		"path fragment":     func(v *model.HTTPRequestPayload) { v.Path = "/forbidden#fragment" },
		"header injection":  func(v *model.HTTPRequestPayload) { v.Headers[0].Value = "safe\r\ninjected: yes" },
	}
	for name, mutate := range mutations {
		candidate := request
		candidate.Headers = append([]model.Header(nil), request.Headers...)
		mutate(&candidate)
		if _, err := serializer.Serialize(model.Payload{Value: candidate}); err == nil {
			t.Errorf("accepted metadata overflow/injection: %s", name)
		}
	}
}
