package sdkboundary

import (
	"errors"
	"strings"
	"testing"
)

// TestStrictJSONAdversarialAcceptance exercises hostile JSON at the public ABI
// boundary.  These cases deliberately mutate otherwise valid commands so each
// rejection is attributable to strict decoding rather than command semantics.
func TestStrictJSONAdversarialAcceptance(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	validStatus := `{"abi_version":1,"command_id":"command-1","command_name":"core.status","sdk_session_id":"session-1","args":{}}`
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{name: "empty", input: ``, want: ErrMalformedInput},
		{name: "whitespace", input: " \n\t ", want: ErrMalformedInput},
		{name: "top-level-array", input: `[]`, want: ErrMalformedInput},
		{name: "top-level-string", input: `"value"`, want: ErrMalformedInput},
		{name: "trailing-object", input: validStatus + `{}`, want: ErrMalformedInput},
		{name: "trailing-scalar", input: validStatus + ` true`, want: ErrMalformedInput},
		{name: "trailing-garbage", input: validStatus + ` xyz`, want: ErrMalformedInput},
		{name: "unterminated-envelope", input: strings.TrimSuffix(validStatus, `}`), want: ErrMalformedInput},
		{name: "duplicate-command-id", input: `{"abi_version":1,"command_id":"a","command_id":"b","command_name":"core.status","sdk_session_id":"s","args":{}}`, want: ErrMalformedInput},
		{name: "escaped-duplicate-command-id", input: `{"abi_version":1,"command_id":"a","command\u005fid":"b","command_name":"core.status","sdk_session_id":"s","args":{}}`, want: ErrMalformedInput},
		{name: "unknown-empty-field", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"":0}`, want: ErrMalformedInput},
		{name: "unknown-upper-field", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"Extra":0}`, want: ErrMalformedInput},
		{name: "unknown-hyphen-field", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"extra-field":0}`, want: ErrMalformedInput},
		{name: "unknown-validly-spelled-field", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{},"extra_field":0}`, want: ErrMalformedInput},
		{name: "noninteger-version", input: `{"abi_version":1.5,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{}}`, want: ErrMalformedInput},
		{name: "version-overflow", input: `{"abi_version":4294967296,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{}}`, want: ErrMalformedInput},
		{name: "args-null", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":null}`, want: ErrMalformedInput},
		{name: "unknown-empty-args-member", input: `{"abi_version":1,"command_id":"c","command_name":"core.status","sdk_session_id":"s","args":{"unexpected":true}}`, want: ErrMalformedInput},
		{name: "duplicate-auth-secret", input: `{"abi_version":1,"command_id":"c","command_name":"auth.connect","sdk_session_id":"s","args":{"mesh_endpoint":"connect.example","username":"agent","password":"one","password":"two","mesh_id":"mesh","agent_instance_id":"instance"}}`, want: ErrMalformedInput},
		{name: "duplicate-header-value", input: `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"http_request":{"method":"POST","path":"/","query":"","headers":[{"name":"x-test","value":"one","value":"two"}],"body":"eA=="}}}}`, want: ErrMalformedInput},
		{name: "unknown-payload-member", input: `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"application/octet-stream","path":"/","body":"eA==","reference":"hidden"}}}}`, want: ErrMalformedInput},
		{name: "malformed-base64-body", input: `{"abi_version":1,"command_id":"c","command_name":"message.send","sdk_session_id":"s","args":{"to":"peer","payload":{"native":{"content_type":"application/octet-stream","path":"/","body":"%%%"}}}}`, want: ErrMalformedInput},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, got := adapter.DecodeABICommand([]byte(test.input))
			if !errors.Is(got, test.want) {
				t.Fatalf("DecodeABICommand() error = %v, want %v", got, test.want)
			}
		})
	}

	if _, err := adapter.DecodeABICommand([]byte(validStatus)); err != nil {
		t.Fatalf("valid control rejected: %v", err)
	}
}

func TestStrictJSONRejectsEveryExcessNestingBoundary(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	for extra := 1; extra <= 4; extra++ {
		depth := maxJSONDepth + extra
		input := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Fatalf("depth %d error = %v, want malformed", depth, err)
		}
	}
}
