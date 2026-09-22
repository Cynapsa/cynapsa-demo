package payload

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strings"
	"unicode/utf8"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/fxamacker/cbor/v2"
)

const (
	CanonicalVersion1         uint16 = 1
	CanonicalKindNative       uint8  = 0
	CanonicalKindHTTPRequest  uint8  = 1
	CanonicalKindHTTPResponse uint8  = 2

	MaximumCanonicalBytes = int64(v1.MaximumPayloadBytes)
	// MaximumTransferredOverheadBytes reserves the V1 versioned AEAD
	// framing/tag allowance above the canonical payload ceiling.
	MaximumTransferredOverheadBytes int64 = 64
	// MaximumTransferredBytes permits optional versioned AEAD framing/tag
	// overhead above the canonical payload ceiling.
	MaximumTransferredBytes = MaximumCanonicalBytes + MaximumTransferredOverheadBytes
	MaximumHeaderCount      = 4096
	maximumMethodBytes      = 32
	maximumContentTypeBytes = 512
	maximumHeaderNameBytes  = 512
	maximumHeaderValueBytes = 8192
	maximumReasonBytes      = 512
	maximumPathBytes        = 2048
	maximumQueryBytes       = 8192
	ProfileNative           = "aztm.native"
	ProfileHTTPRequest      = "http.request"
	ProfileHTTPResponse     = "http.response"
)

type canonicalDiscriminator struct {
	Version uint16 `cbor:"0,keyasint"`
	Kind    uint8  `cbor:"1,keyasint"`
}

type canonicalNativeV1 struct {
	Version     uint16 `cbor:"0,keyasint"`
	Kind        uint8  `cbor:"1,keyasint"`
	ContentType string `cbor:"2,keyasint"`
	Path        string `cbor:"3,keyasint"`
	Body        []byte `cbor:"4,keyasint"`
}

type canonicalHTTPRequestV1 struct {
	Version uint16      `cbor:"0,keyasint"`
	Kind    uint8       `cbor:"1,keyasint"`
	Method  string      `cbor:"2,keyasint"`
	Path    string      `cbor:"3,keyasint"`
	Query   string      `cbor:"4,keyasint"`
	Headers [][2]string `cbor:"5,keyasint"`
	Body    []byte      `cbor:"6,keyasint"`
}

type canonicalHTTPResponseV1 struct {
	Version    uint16                       `cbor:"0,keyasint"`
	Kind       uint8                        `cbor:"1,keyasint"`
	StatusCode uint16                       `cbor:"2,keyasint"`
	Reason     string                       `cbor:"3,keyasint"`
	Headers    [][2]string                  `cbor:"4,keyasint"`
	Body       []byte                       `cbor:"5,keyasint"`
	Error      *canonicalApplicationErrorV1 `cbor:"6,keyasint,omitempty"`
}

type canonicalApplicationErrorV1 struct {
	Code        string `cbor:"0,keyasint"`
	Detail      string `cbor:"1,keyasint"`
	DetailsJSON string `cbor:"2,keyasint"`
}

// Serializer encodes only the closed internal/model payload variants. It
// never reflects arbitrary runtime or SDK values onto the private wire.
type Serializer struct {
	maximum  int64
	encode   cbor.EncMode
	discover cbor.DecMode
	strict   cbor.DecMode
}

// NewSerializer constructs a bounded RFC 8949 Core Deterministic serializer.
func NewSerializer(maximum int64) (*Serializer, error) {
	if maximum <= 0 || maximum > MaximumCanonicalBytes {
		return nil, ErrInvalidLimits
	}
	encOpts := cbor.CoreDetEncOptions()
	encOpts.IndefLength = cbor.IndefLengthForbidden
	encOpts.TagsMd = cbor.TagsForbidden
	encode, err := encOpts.EncMode()
	if err != nil {
		return nil, ErrMalformedCanonical
	}
	base := cbor.DecOptions{
		DupMapKey:        cbor.DupMapKeyEnforcedAPF,
		MaxNestedLevels:  4,
		MaxArrayElements: 2 * MaximumHeaderCount,
		MaxMapPairs:      16,
		IndefLength:      cbor.IndefLengthForbidden,
		TagsMd:           cbor.TagsForbidden,
		UTF8:             cbor.UTF8RejectInvalid,
	}
	discover, err := base.DecMode()
	if err != nil {
		return nil, ErrMalformedCanonical
	}
	base.ExtraReturnErrors = cbor.ExtraDecErrorUnknownField
	strict, err := base.DecMode()
	if err != nil {
		return nil, ErrMalformedCanonical
	}
	return &Serializer{maximum: maximum, encode: encode, discover: discover, strict: strict}, nil
}

// Serialize creates the stable, versioned canonical snapshot used for size
// selection and transfer integrity.
func (s *Serializer) Serialize(value model.Payload) ([]byte, error) {
	if s == nil {
		return nil, ErrInvalidLimits
	}
	var wire any
	var expectedSize int64
	var ownedBody []byte
	defer func() {
		zero(ownedBody)
	}()
	switch payload := value.Value.(type) {
	case model.NativePayload:
		if err := validateNative(payload); err != nil {
			return nil, err
		}
		expectedSize = 1 + 5 + 1 + 1 + cborValueSize(len(payload.ContentType)) + cborValueSize(len(payload.Path)) + cborValueSize(len(payload.Body))
		if expectedSize < 0 || expectedSize > s.maximum {
			return nil, ErrPayloadTooLarge
		}
		ownedBody = clone(payload.Body)
		wire = canonicalNativeV1{CanonicalVersion1, CanonicalKindNative, payload.ContentType, payload.Path, ownedBody}
	case model.HTTPRequestPayload:
		if err := validateRequest(payload); err != nil {
			return nil, err
		}
		expectedSize = 1 + 7 + 1 + 1 + cborValueSize(len(payload.Method)) + cborValueSize(len(payload.Path)) + cborValueSize(len(payload.Query)) + headerWireSize(payload.Headers) + cborValueSize(len(payload.Body))
		if expectedSize < 0 || expectedSize > s.maximum {
			return nil, ErrPayloadTooLarge
		}
		ownedBody = clone(payload.Body)
		wire = canonicalHTTPRequestV1{CanonicalVersion1, CanonicalKindHTTPRequest, payload.Method, payload.Path, payload.Query, headersToWire(payload.Headers), ownedBody}
	case model.HTTPResponsePayload:
		if err := validateResponse(payload); err != nil {
			return nil, err
		}
		expectedSize = 1 + 6 + 1 + 1 + cborUnsignedSize(uint64(payload.StatusCode)) + cborValueSize(len(payload.Reason)) + headerWireSize(payload.Headers) + cborValueSize(len(payload.Body)) + canonicalApplicationErrorSize(payload.Error)
		if expectedSize < 0 || expectedSize > s.maximum {
			return nil, ErrPayloadTooLarge
		}
		ownedBody = clone(payload.Body)
		wire = canonicalHTTPResponseV1{Version: CanonicalVersion1, Kind: CanonicalKindHTTPResponse, StatusCode: payload.StatusCode, Reason: payload.Reason, Headers: headersToWire(payload.Headers), Body: ownedBody, Error: canonicalApplicationError(payload.Error)}
	case model.PayloadHandle:
		return nil, ErrInvalidHandleState
	default:
		return nil, ErrUnsupportedVariant
	}
	encoded, err := s.encode.Marshal(wire)
	if err != nil {
		zero(encoded)
		return nil, ErrMalformedCanonical
	}
	if int64(len(encoded)) > s.maximum {
		zero(encoded)
		return nil, ErrPayloadTooLarge
	}
	if int64(len(encoded)) != expectedSize {
		zero(encoded)
		return nil, ErrMalformedCanonical
	}
	return encoded, nil
}

// Deserialize strictly reconstructs one closed payload variant. Canonical
// re-encoding equality rejects non-shortest values and alternative encodings.
func (s *Serializer) Deserialize(encoded []byte) (model.Payload, error) {
	if s == nil {
		return model.Payload{}, ErrInvalidLimits
	}
	if len(encoded) == 0 {
		return model.Payload{}, ErrMalformedCanonical
	}
	if int64(len(encoded)) > s.maximum {
		return model.Payload{}, ErrPayloadTooLarge
	}
	var discriminator canonicalDiscriminator
	if err := s.discover.Unmarshal(encoded, &discriminator); err != nil {
		return model.Payload{}, ErrMalformedCanonical
	}
	if discriminator.Version != CanonicalVersion1 {
		return model.Payload{}, ErrUnsupportedVersion
	}
	var result model.Payload
	succeeded := false
	defer func() {
		if !succeeded {
			zeroModelPayload(result)
		}
	}()
	switch discriminator.Kind {
	case CanonicalKindNative:
		var wire canonicalNativeV1
		if err := s.strict.Unmarshal(encoded, &wire); err != nil {
			zero(wire.Body)
			return model.Payload{}, ErrMalformedCanonical
		}
		value := model.NativePayload{ContentType: wire.ContentType, Path: wire.Path, Body: wire.Body}
		result.Value = value
		if err := validateNative(model.NativePayload{ContentType: wire.ContentType, Path: wire.Path}); err != nil {
			return model.Payload{}, err
		}
	case CanonicalKindHTTPRequest:
		var wire canonicalHTTPRequestV1
		if err := s.strict.Unmarshal(encoded, &wire); err != nil {
			zero(wire.Body)
			return model.Payload{}, ErrMalformedCanonical
		}
		value := model.HTTPRequestPayload{Method: wire.Method, Path: wire.Path, Query: wire.Query, Body: wire.Body}
		result.Value = value
		if err := validateWireHeaders(wire.Headers); err != nil {
			return model.Payload{}, err
		}
		if err := validateRequest(value); err != nil {
			return model.Payload{}, err
		}
		value.Headers = headersFromWire(wire.Headers)
		result.Value = value
	case CanonicalKindHTTPResponse:
		var wire canonicalHTTPResponseV1
		if err := s.strict.Unmarshal(encoded, &wire); err != nil {
			zero(wire.Body)
			return model.Payload{}, ErrMalformedCanonical
		}
		value := model.HTTPResponsePayload{StatusCode: wire.StatusCode, Reason: wire.Reason, Body: wire.Body, Error: modelApplicationError(wire.Error)}
		result.Value = value
		if err := validateWireHeaders(wire.Headers); err != nil {
			return model.Payload{}, err
		}
		if err := validateResponse(value); err != nil {
			return model.Payload{}, err
		}
		value.Headers = headersFromWire(wire.Headers)
		result.Value = value
	default:
		return model.Payload{}, ErrUnsupportedVariant
	}
	reencoded, err := s.Serialize(result)
	if err != nil {
		return model.Payload{}, err
	}
	if !bytes.Equal(encoded, reencoded) {
		zero(reencoded)
		return model.Payload{}, ErrNonCanonical
	}
	zero(reencoded)
	succeeded = true
	return result, nil
}

// Profile returns the frozen private descriptor profile for a payload variant.
func Profile(value model.Payload) (string, error) {
	switch value.Value.(type) {
	case model.NativePayload:
		return ProfileNative, nil
	case model.HTTPRequestPayload:
		return ProfileHTTPRequest, nil
	case model.HTTPResponsePayload:
		return ProfileHTTPResponse, nil
	default:
		return "", ErrUnsupportedVariant
	}
}

func validCanonicalProfile(value string) bool {
	return value == ProfileNative || value == ProfileHTTPRequest || value == ProfileHTTPResponse
}

func SerializeCanonical(value model.Payload) ([]byte, error) {
	s, err := NewSerializer(MaximumCanonicalBytes)
	if err != nil {
		return nil, err
	}
	return s.Serialize(value)
}

func DeserializeCanonical(encoded []byte) (model.Payload, error) {
	s, err := NewSerializer(MaximumCanonicalBytes)
	if err != nil {
		return model.Payload{}, err
	}
	return s.Deserialize(encoded)
}

func ValidateCanonicalProfile(encoded []byte, expectedProfile string, maximum int64) (model.Payload, error) {
	serializer, err := NewSerializer(maximum)
	if err != nil {
		return model.Payload{}, err
	}
	value, err := serializer.Deserialize(encoded)
	if err != nil {
		return model.Payload{}, err
	}
	profile, err := Profile(value)
	if err != nil || profile != expectedProfile {
		return model.Payload{}, ErrMalformedCanonical
	}
	return value, nil
}

func validateCanonicalProfileOnly(encoded []byte, expectedProfile string, maximum int64) error {
	value, err := ValidateCanonicalProfile(encoded, expectedProfile, maximum)
	zeroModelPayload(value)
	return err
}

func validateNative(value model.NativePayload) error {
	if len(value.ContentType) > maximumContentTypeBytes || !utf8.ValidString(value.ContentType) || strings.ContainsAny(value.ContentType, "\r\n") || !validPath(value.Path) {
		return ErrMalformedCanonical
	}
	return nil
}

func validateRequest(value model.HTTPRequestPayload) error {
	if value.Method == "" || len(value.Method) > maximumMethodBytes || !httpToken(value.Method) || !validPath(value.Path) || len(value.Query) > maximumQueryBytes || !utf8.ValidString(value.Query) {
		return ErrMalformedCanonical
	}
	return validateHeaders(value.Headers)
}

func validateResponse(value model.HTTPResponsePayload) error {
	if value.StatusCode < 100 || value.StatusCode > 599 || len(value.Reason) > maximumReasonBytes || !utf8.ValidString(value.Reason) || strings.ContainsAny(value.Reason, "\r\n") {
		return ErrMalformedCanonical
	}
	if value.Error != nil && value.StatusCode < 400 {
		return ErrMalformedCanonical
	}
	if err := validateHeaders(value.Headers); err != nil {
		return err
	}
	return validateApplicationError(value.Error)
}

func validateApplicationError(value *model.ApplicationError) error {
	if value == nil {
		return nil
	}
	if value.Code == "" || len(value.Code) > maximumHeaderNameBytes || !utf8.ValidString(value.Code) ||
		value.Detail == "" || len(value.Detail) > maximumHeaderValueBytes || !utf8.ValidString(value.Detail) ||
		len(value.DetailsJSON) > maximumHeaderValueBytes || !utf8.ValidString(value.DetailsJSON) || !validApplicationErrorUnicodeEscapes([]byte(value.DetailsJSON)) || !validApplicationErrorJSONObject(value.DetailsJSON) {
		return ErrMalformedCanonical
	}
	return nil
}

func validApplicationErrorUnicodeEscapes(data []byte) bool {
	inString := false
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(data) || data[index] != 'u' {
				continue
			}
			value, ok := applicationErrorHexQuad(data, index+1)
			if !ok {
				return false
			}
			index += 4
			if value >= 0xdc00 && value <= 0xdfff {
				return false
			}
			if value < 0xd800 || value > 0xdbff {
				continue
			}
			if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
				return false
			}
			low, ok := applicationErrorHexQuad(data, index+3)
			if !ok || low < 0xdc00 || low > 0xdfff {
				return false
			}
			index += 6
		}
	}
	return true
}

func applicationErrorHexQuad(data []byte, offset int) (uint16, bool) {
	if offset < 0 || offset+4 > len(data) {
		return 0, false
	}
	var value uint16
	for index := offset; index < offset+4; index++ {
		var digit byte
		switch {
		case data[index] >= '0' && data[index] <= '9':
			digit = data[index] - '0'
		case data[index] >= 'a' && data[index] <= 'f':
			digit = data[index] - 'a' + 10
		case data[index] >= 'A' && data[index] <= 'F':
			digit = data[index] - 'A' + 10
		default:
			return 0, false
		}
		value = value<<4 | uint16(digit)
	}
	return value, true
}

func validApplicationErrorJSONObject(value string) bool {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	if !consumeApplicationErrorJSON(decoder, 0, true) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func consumeApplicationErrorJSON(decoder *json.Decoder, depth int, requireObject bool) bool {
	if depth > 32 {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, compound := token.(json.Delim)
	if requireObject && (!compound || delimiter != '{') {
		return false
	}
	if !compound {
		switch token.(type) {
		case nil, bool, string, json.Number:
			return !requireObject
		default:
			return false
		}
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			key, ok := keyToken.(string)
			if keyErr != nil || !ok {
				return false
			}
			if _, duplicate := seen[key]; duplicate {
				return false
			}
			seen[key] = struct{}{}
			if !consumeApplicationErrorJSON(decoder, depth+1, false) {
				return false
			}
		}
		closing, closeErr := decoder.Token()
		return closeErr == nil && closing == json.Delim('}')
	case '[':
		for decoder.More() {
			if !consumeApplicationErrorJSON(decoder, depth+1, false) {
				return false
			}
		}
		closing, closeErr := decoder.Token()
		return !requireObject && closeErr == nil && closing == json.Delim(']')
	default:
		return false
	}
}

func canonicalApplicationError(value *model.ApplicationError) *canonicalApplicationErrorV1 {
	if value == nil {
		return nil
	}
	return &canonicalApplicationErrorV1{Code: value.Code, Detail: value.Detail, DetailsJSON: value.DetailsJSON}
}

func modelApplicationError(value *canonicalApplicationErrorV1) *model.ApplicationError {
	if value == nil {
		return nil
	}
	return &model.ApplicationError{Code: value.Code, Detail: value.Detail, DetailsJSON: value.DetailsJSON}
}

func canonicalApplicationErrorSize(value *model.ApplicationError) int64 {
	if value == nil {
		return 0
	}
	// One key in the parent map, followed by a three-field deterministic map.
	return 1 + 1 + 3 + cborValueSize(len(value.Code)) + cborValueSize(len(value.Detail)) + cborValueSize(len(value.DetailsJSON))
}

func validateHeaders(headers []model.Header) error {
	if len(headers) > MaximumHeaderCount {
		return ErrPayloadTooLarge
	}
	for _, header := range headers {
		if header.Name == "" || len(header.Name) > maximumHeaderNameBytes || !httpToken(header.Name) || len(header.Value) > maximumHeaderValueBytes || !utf8.ValidString(header.Value) || strings.ContainsAny(header.Value, "\r\n") {
			return ErrMalformedCanonical
		}
	}
	return nil
}

func validateWireHeaders(headers [][2]string) error {
	if len(headers) > MaximumHeaderCount {
		return ErrPayloadTooLarge
	}
	for _, header := range headers {
		if header[0] == "" || len(header[0]) > maximumHeaderNameBytes || !httpToken(header[0]) || len(header[1]) > maximumHeaderValueBytes || !utf8.ValidString(header[1]) || strings.ContainsAny(header[1], "\r\n") {
			return ErrMalformedCanonical
		}
	}
	return nil
}

func validPath(path string) bool {
	return path != "" && len(path) <= maximumPathBytes && path[0] == '/' && utf8.ValidString(path) && !strings.ContainsAny(path, "?#")
}

func httpToken(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c <= 0x20 || c >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(c)) {
			return false
		}
	}
	return true
}

func headersToWire(headers []model.Header) [][2]string {
	out := make([][2]string, len(headers))
	for i, header := range headers {
		out[i] = [2]string{header.Name, header.Value}
	}
	return out
}

func headersFromWire(headers [][2]string) []model.Header {
	out := make([]model.Header, len(headers))
	for i, header := range headers {
		out[i] = model.Header{Name: header[0], Value: header[1]}
	}
	return out
}

func cborUnsignedSize(value uint64) int64 {
	switch {
	case value < 24:
		return 1
	case value <= 0xff:
		return 2
	case value <= 0xffff:
		return 3
	case value <= 0xffffffff:
		return 5
	default:
		return 9
	}
}
func cborValueSize(length int) int64 {
	if length < 0 {
		return -1
	}
	return cborUnsignedSize(uint64(length)) + int64(length)
}
func headerWireSize(headers []model.Header) int64 {
	total := cborUnsignedSize(uint64(len(headers)))
	for _, header := range headers {
		part := int64(1) + cborValueSize(len(header.Name)) + cborValueSize(len(header.Value))
		if part < 0 || total > math.MaxInt64-part {
			return -1
		}
		total += part
	}
	return total
}

func clone(value []byte) []byte { return append([]byte(nil), value...) }

func zeroModelPayload(value model.Payload) {
	switch payload := value.Value.(type) {
	case model.NativePayload:
		zero(payload.Body)
	case model.HTTPRequestPayload:
		zero(payload.Body)
	case model.HTTPResponsePayload:
		zero(payload.Body)
		if payload.Error != nil {
			*payload.Error = model.ApplicationError{}
		}
	}
}

func checkedAdd(current int64, increment int) (int64, error) {
	if increment < 0 || current < 0 || int64(increment) > math.MaxInt64-current {
		return 0, ErrPayloadTooLarge
	}
	return current + int64(increment), nil
}
