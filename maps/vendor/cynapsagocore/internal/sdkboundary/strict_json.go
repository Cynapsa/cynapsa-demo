package sdkboundary

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	maxABIInputBytes = 2 << 20
	maxJSONDepth     = 32
)

func strictDecode[T any](data []byte, destination *T) error {
	if len(data) == 0 || len(data) > maxABIInputBytes {
		if len(data) > maxABIInputBytes {
			return ErrInputTooLarge
		}
		return ErrMalformedInput
	}
	// encoding/json replaces malformed UTF-8 with U+FFFD. Reject the raw ABI
	// bytes first so every string-bearing input fails closed instead.
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: invalid UTF-8", ErrMalformedInput)
	}
	if err := validateJSONStringUnicodeEscapes(data); err != nil {
		return err
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return ErrMalformedInput
	}
	if err := rejectDuplicateJSONFields(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		if errors.Is(err, ErrInputTooLarge) {
			return ErrInputTooLarge
		}
		return fmt.Errorf("%w: invalid JSON", ErrMalformedInput)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	return nil
}

// validateJSONStringUnicodeEscapes prevents encoding/json from replacing
// invalid UTF-16 surrogate escapes with U+FFFD. It intentionally leaves the
// rest of JSON grammar to the strict decoder below.
func validateJSONStringUnicodeEscapes(data []byte) error {
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
			value, ok := parseJSONHexQuad(data, index+1)
			if !ok {
				return malformedUnicodeEscape()
			}
			index += 4
			if value >= 0xdc00 && value <= 0xdfff {
				return malformedUnicodeEscape()
			}
			if value < 0xd800 || value > 0xdbff {
				continue
			}
			if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
				return malformedUnicodeEscape()
			}
			low, ok := parseJSONHexQuad(data, index+3)
			if !ok || low < 0xdc00 || low > 0xdfff {
				return malformedUnicodeEscape()
			}
			index += 6
		}
	}
	return nil
}

func parseJSONHexQuad(data []byte, offset int) (uint16, bool) {
	if offset < 0 || offset+4 > len(data) {
		return 0, false
	}
	var value uint16
	for index := offset; index < offset+4; index++ {
		digit, ok := jsonHexDigit(data[index])
		if !ok {
			return 0, false
		}
		value = value<<4 | uint16(digit)
	}
	return value, true
}

func jsonHexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func malformedUnicodeEscape() error {
	return fmt.Errorf("%w: invalid Unicode surrogate escape", ErrMalformedInput)
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 0); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("%w: JSON nesting", ErrMalformedInput)
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: invalid JSON", ErrMalformedInput)
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("%w: invalid object", ErrMalformedInput)
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%w: invalid object key", ErrMalformedInput)
			}
			if !validJSONFieldName(key) {
				return fmt.Errorf("%w: invalid field name", ErrMalformedInput)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("%w: duplicate field", ErrMalformedInput)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("%w: invalid object", ErrMalformedInput)
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("%w: invalid array", ErrMalformedInput)
		}
	default:
		return fmt.Errorf("%w: invalid delimiter", ErrMalformedInput)
	}
	return nil
}

func validJSONFieldName(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for index := 1; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON data", ErrMalformedInput)
	}
	return nil
}
