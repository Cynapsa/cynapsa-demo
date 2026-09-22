package rpc

import (
	"encoding/base64"
	"testing"
)

func FuzzRequestHandleValidation(f *testing.F) {
	valid := requestHandlePrefix + base64.RawURLEncoding.EncodeToString(make([]byte, requestHandleEntropyBytes))
	for _, seed := range []string{"", "reqh_", valid, valid + "=", "payh_" + valid[len(requestHandlePrefix):], "reqh_\x00secret"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		err := ValidateRequestHandle(input)
		if err != nil {
			return
		}
		if len(input) != len(requestHandlePrefix)+43 || input[:len(requestHandlePrefix)] != requestHandlePrefix {
			t.Fatalf("accepted noncanonical handle %q", input)
		}
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(input[len(requestHandlePrefix):])
		if decodeErr != nil || len(decoded) != requestHandleEntropyBytes {
			t.Fatalf("accepted undecodable handle %q: %v", input, decodeErr)
		}
		if canonical := requestHandlePrefix + base64.RawURLEncoding.EncodeToString(decoded); canonical != input {
			t.Fatalf("accepted alias %q for %q", input, canonical)
		}
		if err := ValidateRequestHandle(input); err != nil {
			t.Fatalf("validation was unstable: %v", err)
		}
	})
}
