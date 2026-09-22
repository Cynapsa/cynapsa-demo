package enrollment

import (
	"bytes"
	"encoding/base64"
	"testing"
)

const testToken = "cpsa_e1.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestPublicTokenRequiresExactPrefixAndCanonicalBody(t *testing.T) {
	if len(testToken) != 88 {
		t.Fatalf("full token length = %d", len(testToken))
	}
	bare, err := ParsePublicToken([]byte(testToken))
	if err != nil || len(bare) != 83 || string(bare) != testToken[5:] {
		t.Fatalf("parse = %q, %v", bare, err)
	}
	cases := []string{
		testToken[5:],
		"aztm_" + testToken[5:],
		"other_" + testToken[5:],
		"cpsa_extra_" + testToken[5:],
		"CPSA_" + testToken[5:],
		"cpsa_e2" + testToken[7:],
		"cpsa_e1.01234567-89AB-4def-8123-456789abcdef." + testToken[len(testToken)-43:],
		testToken + "x",
		testToken[:len(testToken)-1],
	}
	for _, value := range cases {
		if parsed, parseErr := ParsePublicToken([]byte(value)); parseErr == nil {
			t.Fatalf("accepted %q as %q", value, parsed)
		}
	}
}

func TestPublicTokenReturnsIndependentOwnership(t *testing.T) {
	input := []byte(testToken)
	bare, err := ParsePublicToken(input)
	if err != nil {
		t.Fatal(err)
	}
	input[5] = 'x'
	if string(bare) != testToken[5:] {
		t.Fatal("returned token aliases caller input")
	}
	clear(bare)
}

func TestGenerateInstallationUsesExactlyFreshCanonicalEntropy(t *testing.T) {
	entropy := bytes.Repeat([]byte{0x11}, 48)
	id, secret, err := GenerateInstallation(bytes.NewReader(entropy))
	if err != nil {
		t.Fatal(err)
	}
	if id != "11111111-1111-4111-9111-111111111111" || !ValidCanonicalUUID(id) {
		t.Fatalf("id = %q", id)
	}
	wantSecret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32))
	if string(secret) != wantSecret || len(secret) != 43 {
		t.Fatalf("secret shape = %q", secret)
	}
	clear(secret)
	if _, _, err := GenerateInstallation(bytes.NewReader(make([]byte, 47))); err == nil {
		t.Fatal("accepted short entropy")
	}
}
