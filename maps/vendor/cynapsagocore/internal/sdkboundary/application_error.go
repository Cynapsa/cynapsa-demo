package sdkboundary

import (
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const maximumApplicationErrorJSONDepth = 32

func validateApplicationError(code, detail, detailsJSON string) (int, error) {
	if err := requiredBounded("application_error.code", code, maxIdentifierLength); err != nil {
		return 0, err
	}
	if !utf8.ValidString(code) {
		return 0, malformed("application error code")
	}
	if err := requiredBounded("application_error.detail", detail, maxPublicString); err != nil {
		return 0, err
	}
	if !utf8.ValidString(detail) {
		return 0, malformed("application error detail")
	}
	if len(detailsJSON) > maxPublicString {
		return 0, ErrInputTooLarge
	}
	if !utf8.ValidString(detailsJSON) {
		return 0, malformed("application error details")
	}
	if err := validateJSONStringUnicodeEscapes([]byte(detailsJSON)); err != nil {
		return 0, malformed("application error details")
	}
	if !validJSONObject(detailsJSON) {
		return 0, malformed("application error details")
	}
	return len(code) + len(detail) + len(detailsJSON), nil
}

func validJSONObject(value string) bool {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	if !consumeJSONValue(decoder, 0, true) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func consumeJSONValue(decoder *json.Decoder, depth int, requireObject bool) bool {
	if depth > maximumApplicationErrorJSONDepth {
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
			if !consumeJSONValue(decoder, depth+1, false) {
				return false
			}
		}
		closing, closeErr := decoder.Token()
		return closeErr == nil && closing == json.Delim('}')
	case '[':
		for decoder.More() {
			if !consumeJSONValue(decoder, depth+1, false) {
				return false
			}
		}
		closing, closeErr := decoder.Token()
		return !requireObject && closeErr == nil && closing == json.Delim(']')
	default:
		return false
	}
}

func validateWireApplicationError(value *applicationErrorWire) (int, error) {
	if value == nil {
		return 0, nil
	}
	return validateApplicationError(value.Code, value.Detail, value.DetailsJSON)
}

func validatePublicApplicationError(value *v1.ApplicationError) (int, error) {
	if value == nil {
		return 0, nil
	}
	return validateApplicationError(value.Code, value.Detail, value.DetailsJSON)
}

func validateModelApplicationError(value *model.ApplicationError) (int, error) {
	if value == nil {
		return 0, nil
	}
	return validateApplicationError(value.Code, value.Detail, value.DetailsJSON)
}

func publicApplicationError(value *applicationErrorWire) *v1.ApplicationError {
	if value == nil {
		return nil
	}
	return &v1.ApplicationError{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}

func wireApplicationError(value *v1.ApplicationError) *applicationErrorWire {
	if value == nil {
		return nil
	}
	return &applicationErrorWire{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}

func decodeApplicationError(value *v1.ApplicationError) *model.ApplicationError {
	if value == nil {
		return nil
	}
	return &model.ApplicationError{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}

func mapApplicationError(value *model.ApplicationError) *v1.ApplicationError {
	if value == nil {
		return nil
	}
	return &v1.ApplicationError{Code: strings.Clone(value.Code), Detail: strings.Clone(value.Detail), DetailsJSON: strings.Clone(value.DetailsJSON)}
}
