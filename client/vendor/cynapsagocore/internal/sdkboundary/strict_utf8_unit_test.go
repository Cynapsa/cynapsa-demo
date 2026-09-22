package sdkboundary

import (
	"errors"
	"fmt"
	"testing"
)

func invalidUTF8JSON(prefix, suffix string) []byte {
	input := make([]byte, 0, len(prefix)+1+len(suffix))
	input = append(input, prefix...)
	input = append(input, 0xff)
	return append(input, suffix...)
}

func TestStrictABIRejectsRawInvalidUTF8BeforeJSONReplacement(t *testing.T) {
	adapter, _ := New()
	tests := map[string][]byte{
		"native content type": invalidUTF8JSON(
			`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"`,
			`","path":"/","body":""}}}}`,
		),
		"HTTP response reason": invalidUTF8JSON(
			`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"http_response":{"status_code":200,"reason":"`,
			`","headers":[],"body":""}}}}`,
		),
		"command identifier": invalidUTF8JSON(
			`{"abi_version":1,"command_id":"`,
			`","command_name":"core.status","sdk_session_id":"s","args":{}}`,
		),
		"address URL": invalidUTF8JSON(
			`{"abi_version":1,"command_id":"c","command_name":"address.resolve","sdk_session_id":"s","args":{"url":"https://agent.example/`,
			`"}}`,
		),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := adapter.DecodeABICommand(input); !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("error=%v, want malformed input", err)
			}
		})
	}

	config := invalidUTF8JSON(
		`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue`,
		`_limit":1,"payload_limit":1}`,
	)
	if _, err := adapter.DecodeABIConfig(config); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("config raw UTF-8 error=%v, want malformed input", err)
	}
}

func TestStrictABIRejectsInvalidSurrogateEscapesEverywhere(t *testing.T) {
	adapter, _ := New()
	command := func(field, payload string) []byte {
		return []byte(fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{%q:%s}}}`, field, payload))
	}
	tests := map[string][]byte{
		"native lone high":  command("native", `{"content_type":"application/\ud800","path":"/","body":""}`),
		"response lone low": command("http_response", `{"status_code":200,"reason":"\udc00","headers":[],"body":""}`),
		"identifier reversed pair": []byte(
			`{"abi_version":1,"command_id":"\udc00\ud800","command_name":"core.status","sdk_session_id":"s","args":{}}`,
		),
		"URL high then non-low": []byte(
			`{"abi_version":1,"command_id":"c","command_name":"address.resolve","sdk_session_id":"s","args":{"url":"https://agent.example/\ud800\u0041"}}`,
		),
		"malformed hex after high": command("native", `{"content_type":"\ud800\uZZZZ","path":"/","body":""}`),
		"config field lone high": []byte(
			`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit\ud800":1}`,
		),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := adapter.DecodeABICommand(input); name != "config field lone high" && !errors.Is(err, ErrMalformedInput) {
				t.Fatalf("command error=%v, want malformed input", err)
			}
			if name == "config field lone high" {
				if _, err := adapter.DecodeABIConfig(input); !errors.Is(err, ErrMalformedInput) {
					t.Fatalf("config error=%v, want malformed input", err)
				}
			}
		})
	}
}

func TestStrictABIAcceptsValidUnicodePairsAndReplacementCharacters(t *testing.T) {
	adapter, _ := New()
	values := map[string]string{
		"surrogate pair":      `application/\ud83d\ude80`,
		"escaped replacement": `application/\ufffd`,
		"literal replacement": "application/�",
		"literal Unicode":     "application/世界",
	}
	for name, contentType := range values {
		t.Run(name, func(t *testing.T) {
			input := []byte(fmt.Sprintf(`{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"a","payload":{"native":{"content_type":"%s","path":"/","body":""}}}}`, contentType))
			if _, err := adapter.DecodeABICommand(input); err != nil {
				t.Fatalf("valid Unicode rejected: %v", err)
			}
		})
	}

	config := []byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue\u005flimit":1,"payload\u005flimit":1}`)
	if _, err := adapter.DecodeABIConfig(config); err != nil {
		t.Fatalf("valid config Unicode escape rejected: %v", err)
	}
}

func TestJSONStringEscapeValidatorAcceptsValidPairsAndRejectsInvalidOrder(t *testing.T) {
	for _, input := range []string{
		`{"value":"\ud800\udc00"}`,
		`{"value":"\udbff\udfff"}`,
		`{"value":"\ufffd"}`,
		`{"value":"�"}`,
		`{"value":"\\ud800"}`,
	} {
		if err := validateJSONStringUnicodeEscapes([]byte(input)); err != nil {
			t.Errorf("valid input %q rejected: %v", input, err)
		}
	}
	for _, input := range []string{
		`{"value":"\ud800"}`,
		`{"value":"\udc00"}`,
		`{"value":"\udc00\ud800"}`,
		`{"value":"\ud800\u0041"}`,
		`{"value":"\ud800x"}`,
	} {
		if err := validateJSONStringUnicodeEscapes([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("invalid input %q error=%v", input, err)
		}
	}
}
