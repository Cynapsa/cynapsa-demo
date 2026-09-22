package payload

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/fxamacker/cbor/v2"
)

func TestSerializerGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()
	serializer, err := NewSerializer(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	values := []model.Payload{
		{Value: model.NativePayload{ContentType: "application/json", Path: "/orders", Body: []byte(`{"id":7}`)}},
		{Value: model.HTTPRequestPayload{Method: "POST", Path: "/calc", Query: "x=1", Headers: []model.Header{{Name: "x-a", Value: "1"}, {Name: "x-a", Value: "2"}}, Body: []byte{0, 1, 2}}},
		{Value: model.HTTPResponsePayload{StatusCode: 404, Reason: "Not Found", Headers: []model.Header{{Name: "x-cynapsa-error", Value: "ordinary application header"}}, Body: []byte{3, 2, 1}, Error: &model.ApplicationError{Code: "not_found", Detail: "The record does not exist", DetailsJSON: `{"record_id":7}`}}},
	}
	for _, value := range values {
		encoded, err := serializer.Serialize(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := serializer.Deserialize(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(value, decoded) {
			t.Fatalf("round trip mismatch: %#v != %#v", value, decoded)
		}
	}
	encoded, err := serializer.Serialize(values[1])
	if err != nil {
		t.Fatal(err)
	}
	const golden = "a7000101010264504f535403652f63616c630463783d3105828263782d6161318263782d6161320643000102"
	if got := hex.EncodeToString(encoded); golden != "" && got != golden {
		t.Fatalf("golden changed:\n%s", got)
	} else if golden == "" {
		t.Logf("golden=%s", got)
	}
}

func TestSerializerStrictRejections(t *testing.T) {
	t.Parallel()
	serializer, _ := NewSerializer(4096)
	valid, err := serializer.Serialize(model.Payload{Value: model.NativePayload{Path: "/", Body: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range valid {
		if _, err := serializer.Deserialize(valid[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	if _, err := serializer.Deserialize(append(valid, 0)); !errors.Is(err, ErrMalformedCanonical) && !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("trailing: %v", err)
	}

	mode, _ := cbor.CoreDetEncOptions().EncMode()
	unknown, _ := mode.Marshal(map[uint64]any{0: uint64(1), 1: uint64(0), 2: "", 3: "/", 4: []byte("x"), 7: true})
	if _, err := serializer.Deserialize(unknown); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("unknown field: %v", err)
	}
	unsupported, _ := mode.Marshal(map[uint64]any{0: uint64(2), 1: uint64(0), 2: "", 3: "/", 4: []byte("x")})
	if _, err := serializer.Deserialize(unsupported); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version: %v", err)
	}
	variant, _ := mode.Marshal(map[uint64]any{0: uint64(1), 1: uint64(9)})
	if _, err := serializer.Deserialize(variant); !errors.Is(err, ErrUnsupportedVariant) {
		t.Fatalf("variant: %v", err)
	}
	duplicate := []byte{0xa6, 0x00, 0x01, 0x00, 0x01, 0x01, 0x00, 0x02, 0x60, 0x03, 0x61, 0x2f, 0x04, 0x41, 0x78}
	if _, err := serializer.Deserialize(duplicate); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("duplicate: %v", err)
	}
	for _, response := range []model.HTTPResponsePayload{
		{StatusCode: 200, Error: &model.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{}`}},
		{StatusCode: 400, Error: &model.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{"x":1,"x":2}`}},
		{StatusCode: 400, Error: &model.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `[]`}},
		{StatusCode: 400, Error: &model.ApplicationError{Code: "bad", Detail: "bad", DetailsJSON: `{"x":"\ud800"}`}},
	} {
		if _, err := serializer.Serialize(model.Payload{Value: response}); !errors.Is(err, ErrMalformedCanonical) {
			t.Fatalf("invalid application error %#v: %v", response.Error, err)
		}
	}
	tooDeepDetails := `{"x":` + strings.Repeat(`[`, 33) + `{}` + strings.Repeat(`]`, 33) + `}`
	for _, detailsJSON := range []string{`{"x":1,"x":2}`, tooDeepDetails} {
		encoded, err := serializer.encode.Marshal(canonicalHTTPResponseV1{
			Version: CanonicalVersion1, Kind: CanonicalKindHTTPResponse, StatusCode: 400,
			Error: &canonicalApplicationErrorV1{Code: "bad", Detail: "bad", DetailsJSON: detailsJSON},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := serializer.Validate(encoded); !errors.Is(err, ErrMalformedCanonical) {
			t.Fatalf("fast validator accepted invalid application error details %q: %v", detailsJSON, err)
		}
	}
}

func TestSerializerDecodesLegacyResponseWithoutApplicationError(t *testing.T) {
	serializer, err := NewSerializer(MaximumCanonicalBytes)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := serializer.encode.Marshal(canonicalHTTPResponseV1{
		Version: CanonicalVersion1, Kind: CanonicalKindHTTPResponse, StatusCode: 404,
		Reason: "Not Found", Headers: [][2]string{{"x-test", "one"}, {"x-test", "two"}}, Body: []byte("legacy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serializer.Deserialize(encoded)
	if err != nil {
		t.Fatal(err)
	}
	response := decoded.Value.(model.HTTPResponsePayload)
	if response.Error != nil || response.StatusCode != 404 || string(response.Body) != "legacy" || len(response.Headers) != 2 {
		t.Fatalf("legacy response changed: %#v", response)
	}
}

func TestSerializerLimitsAndOwnership(t *testing.T) {
	t.Parallel()
	serializer, _ := NewSerializer(256)
	body := make([]byte, 80)
	value := model.Payload{Value: model.NativePayload{Path: "/", Body: body}}
	encoded, err := serializer.Serialize(value)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 9
	decoded, err := serializer.Deserialize(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Value.(model.NativePayload).Body[0] != 0 {
		t.Fatal("serializer retained caller bytes")
	}
	tooLarge := model.Payload{Value: model.NativePayload{Path: "/", Body: make([]byte, 256)}}
	if _, err := serializer.Serialize(tooLarge); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if _, err := serializer.Serialize(model.Payload{Value: model.PayloadHandle{Handle: "payh_private"}}); !errors.Is(err, ErrInvalidHandleState) {
		t.Fatalf("handle: %v", err)
	}
}

func TestSerializerLimitUsesExactFinalCanonicalSize(t *testing.T) {
	value := model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/", Headers: []model.Header{{Name: "x-a", Value: "1"}}, Body: bytes.Repeat([]byte{1}, 32)}}
	wide, _ := NewSerializer(1024)
	encoded, err := wide.Serialize(value)
	if err != nil {
		t.Fatal(err)
	}
	exact, _ := NewSerializer(int64(len(encoded)))
	if _, err := exact.Serialize(value); err != nil {
		t.Fatalf("exact limit %v", err)
	}
	narrow, _ := NewSerializer(int64(len(encoded) - 1))
	if _, err := narrow.Serialize(value); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("narrow %v", err)
	}
}

func TestSerializerMatchesFrozenHTTPBoundaryLimits(t *testing.T) {
	serializer, _ := NewSerializer(MaximumCanonicalBytes)
	request := model.HTTPRequestPayload{
		Method:  strings.Repeat("A", maximumMethodBytes),
		Path:    "/" + strings.Repeat("p", maximumPathBytes-1),
		Query:   strings.Repeat("q", maximumQueryBytes),
		Headers: []model.Header{{Name: strings.Repeat("a", maximumHeaderNameBytes), Value: strings.Repeat("v", maximumHeaderValueBytes)}},
	}
	if _, err := serializer.Serialize(model.Payload{Value: request}); err != nil {
		t.Fatalf("boundary request: %v", err)
	}
	mutations := []func(*model.HTTPRequestPayload){
		func(v *model.HTTPRequestPayload) { v.Method += "A" },
		func(v *model.HTTPRequestPayload) { v.Path += "p" },
		func(v *model.HTTPRequestPayload) { v.Path = "/bad?query" },
		func(v *model.HTTPRequestPayload) { v.Path = "/bad#fragment" },
		func(v *model.HTTPRequestPayload) { v.Query += "q" },
		func(v *model.HTTPRequestPayload) { v.Headers[0].Name += "a" },
		func(v *model.HTTPRequestPayload) { v.Headers[0].Value += "v" },
	}
	for index, mutate := range mutations {
		candidate := request
		candidate.Headers = append([]model.Header(nil), request.Headers...)
		mutate(&candidate)
		if _, err := serializer.Serialize(model.Payload{Value: candidate}); err == nil {
			t.Errorf("accepted request mutation %d", index)
		}
	}

	headers := make([]model.Header, MaximumHeaderCount)
	for i := range headers {
		headers[i] = model.Header{Name: "x", Value: "v"}
	}
	if _, err := serializer.Serialize(model.Payload{Value: model.HTTPRequestPayload{Method: "GET", Path: "/", Headers: headers}}); err != nil {
		t.Fatalf("header count boundary: %v", err)
	}
	headers = append(headers, model.Header{Name: "x", Value: "v"})
	if _, err := serializer.Serialize(model.Payload{Value: model.HTTPRequestPayload{Method: "GET", Path: "/", Headers: headers}}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("header count overflow: %v", err)
	}

	native := model.NativePayload{ContentType: strings.Repeat("c", maximumContentTypeBytes), Path: "/"}
	if _, err := serializer.Serialize(model.Payload{Value: native}); err != nil {
		t.Fatalf("content type boundary: %v", err)
	}
	native.ContentType = string([]byte{0xff})
	if _, err := serializer.Serialize(model.Payload{Value: native}); !errors.Is(err, ErrMalformedCanonical) {
		t.Fatalf("invalid content type utf8: %v", err)
	}
	response := model.HTTPResponsePayload{StatusCode: 200, Reason: strings.Repeat("r", maximumReasonBytes)}
	if _, err := serializer.Serialize(model.Payload{Value: response}); err != nil {
		t.Fatalf("reason boundary: %v", err)
	}
	response.Reason += "r"
	if _, err := serializer.Serialize(model.Payload{Value: response}); err == nil {
		t.Fatal("accepted oversized reason")
	}
}
