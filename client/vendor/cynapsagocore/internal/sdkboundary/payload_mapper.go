package sdkboundary

import (
	"strings"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapPayload materializes one closed canonical payload or returns an opaque
// handle for the entire canonical snapshot.
func (a *Adapter) MapPayload(payload model.Payload) (v1.Payload, error) {
	var public v1.Payload
	switch value := payload.Value.(type) {
	case model.NativePayload:
		if err := validateModelNativePayload(value); err != nil {
			return v1.Payload{}, err
		}
		public.Value = v1.NativePayload{ContentType: value.ContentType, Path: value.Path, Body: cloneBytes(value.Body)}
	case model.HTTPRequestPayload:
		if err := validateModelHTTPRequestPayload(value); err != nil {
			return v1.Payload{}, err
		}
		public.Value = v1.HTTPRequestPayload{Method: value.Method, Path: value.Path, Query: value.Query, Headers: mapHeaders(value.Headers), Body: cloneBytes(value.Body)}
	case model.HTTPResponsePayload:
		if err := validateModelHTTPResponsePayload(value); err != nil {
			return v1.Payload{}, err
		}
		public.Value = v1.HTTPResponsePayload{StatusCode: value.StatusCode, Reason: value.Reason, Headers: mapHeaders(value.Headers), Body: cloneBytes(value.Body), Error: mapApplicationError(value.Error)}
	case model.PayloadHandle:
		handle := v1.PayloadHandle(value.Handle)
		if err := validatePayloadHandle(handle); err != nil {
			return v1.Payload{}, err
		}
		public.Value = handle
	default:
		return v1.Payload{}, malformed("payload variant")
	}
	return public, nil
}

func validateModelNativePayload(value model.NativePayload) error {
	if err := validatePath(value.Path); err != nil {
		return err
	}
	if err := validateContentType(value.ContentType); err != nil {
		return err
	}
	if canonicalSize(value.ContentType, value.Path)+len(value.Body) > maxInlinePayloadBytes {
		return ErrInputTooLarge
	}
	return nil
}

func validateModelHTTPRequestPayload(value model.HTTPRequestPayload) error {
	if err := validateHTTPMethod(value.Method); err != nil {
		return err
	}
	if err := validatePath(value.Path); err != nil {
		return err
	}
	if len(value.Query) > maxPublicString {
		return ErrInputTooLarge
	}
	headerSize, err := validateModelHeaders(value.Headers)
	if err != nil {
		return err
	}
	if headerSize+canonicalSize(value.Method, value.Path, value.Query)+len(value.Body) > maxInlinePayloadBytes {
		return ErrInputTooLarge
	}
	return nil
}

func validateModelHTTPResponsePayload(value model.HTTPResponsePayload) error {
	if value.StatusCode < 100 || value.StatusCode > 599 {
		return malformed("http response")
	}
	if err := validateReason(value.Reason); err != nil {
		return err
	}
	if value.Error != nil && value.StatusCode < 400 {
		return malformed("http response application error")
	}
	headerSize, err := validateModelHeaders(value.Headers)
	if err != nil {
		return err
	}
	errorSize, err := validateModelApplicationError(value.Error)
	if err != nil {
		return err
	}
	if headerSize+len(value.Reason)+len(value.Body)+errorSize > maxInlinePayloadBytes {
		return ErrInputTooLarge
	}
	return nil
}

func validateModelHeaders(values []model.Header) (int, error) {
	if len(values) > maxHeaderCount {
		return 0, ErrInputTooLarge
	}
	total := 0
	for _, value := range values {
		if len(value.Name) > maxIdentifierLength || len(value.Value) > maxPublicString {
			return 0, ErrInputTooLarge
		}
		if !validHTTPToken(value.Name) || strings.ContainsAny(value.Value, "\r\n") {
			return 0, malformed("header")
		}
		total += len(value.Name) + len(value.Value)
		if total > maxInlinePayloadBytes {
			return 0, ErrInputTooLarge
		}
	}
	return total, nil
}

func mapHeaders(values []model.Header) []v1.Header {
	output := make([]v1.Header, len(values))
	for i, value := range values {
		output[i] = v1.Header{Name: value.Name, Value: value.Value}
	}
	return output
}
