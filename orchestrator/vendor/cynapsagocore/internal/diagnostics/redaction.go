package diagnostics

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxRedactionDepth   = 4
	maxDiagnosticFields = 64
	maxDiagnosticList   = 64
	maxDiagnosticString = 256
	redactedValue       = "[redacted]"
)

var sensitiveKeys = map[string]struct{}{
	"authorization":   {},
	"body":            {},
	"bootstrapdata":   {},
	"cookie":          {},
	"cookies":         {},
	"credential":      {},
	"credentialproof": {},
	"credentials":     {},
	"data":            {},
	"encryptionref":   {},
	"header":          {},
	"headers":         {},
	"password":        {},
	"payload":         {},
	"proof":           {},
	"reference":       {},
	"secret":          {},
	"token":           {},
	"url":             {},
}

// Redact returns a bounded deep copy with secret-bearing fields removed.
// Unknown value types and errors are redacted instead of stringified.
func Redact(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	return redactMap(input, 0)
}

func redactMap(input map[string]any, depth int) map[string]any {
	result := make(map[string]any, min(len(input), maxDiagnosticFields)+1)
	keys := make([]string, 0, min(len(input), maxDiagnosticFields))
	for key := range input {
		if len(keys) < maxDiagnosticFields {
			keys = append(keys, key)
			continue
		}
		largest := 0
		for index := 1; index < len(keys); index++ {
			if keys[index] > keys[largest] {
				largest = index
			}
		}
		if key < keys[largest] {
			keys[largest] = key
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := input[key]
		safeKey := boundString(key)
		if sensitiveKey(key) {
			result[safeKey] = redactedValue
			continue
		}
		result[safeKey] = redactValue(value, depth+1)
	}
	if len(input) > len(keys) {
		result["_truncated"] = true
	}
	return result
}

func sensitiveKey(value string) bool {
	normalized := normalizeKey(value)
	if _, sensitive := sensitiveKeys[normalized]; sensitive {
		return true
	}
	for _, fragment := range []string{"authorization", "cookie", "credential", "password", "secret", "token"} {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

func redactValue(value any, depth int) any {
	if depth > maxRedactionDepth {
		return redactedValue
	}
	switch typed := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return typed
	case string:
		return boundString(typed)
	case time.Time:
		return typed
	case map[string]any:
		return redactMap(typed, depth)
	case []any:
		limit := min(len(typed), maxDiagnosticList)
		result := make([]any, 0, limit+1)
		for _, item := range typed[:limit] {
			result = append(result, redactValue(item, depth+1))
		}
		if len(typed) > limit {
			result = append(result, redactedValue)
		}
		return result
	case []string:
		limit := min(len(typed), maxDiagnosticList)
		result := make([]string, 0, limit+1)
		for _, item := range typed[:limit] {
			result = append(result, boundString(item))
		}
		if len(typed) > limit {
			result = append(result, redactedValue)
		}
		return result
	case error, fmt.Stringer, []byte:
		return redactedValue
	default:
		return redactedValue
	}
}

func normalizeKey(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, value)
}

func boundString(value string) string {
	if len(value) <= maxDiagnosticString {
		return value
	}
	value = value[:maxDiagnosticString]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
