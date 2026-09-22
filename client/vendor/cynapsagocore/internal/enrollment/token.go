// Package enrollment owns the private enrollment protocol and credentials.
package enrollment

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
)

const (
	publicTokenPrefix       = "cpsa_"
	tokenVersion            = "e1"
	CanonicalTokenBodyBytes = 83
	CanonicalSecretBytes    = 43
)

var ErrInvalidToken = errors.New("invalid enrollment token")

// ParsePublicToken accepts only cpsa_e1.<canonical UUID>.<32-byte base64url>.
// The returned bare e1 body is independently owned by the caller.
func ParsePublicToken(input []byte) ([]byte, error) {
	if len(input) != len(publicTokenPrefix)+CanonicalTokenBodyBytes || !bytes.HasPrefix(input, []byte(publicTokenPrefix)) {
		return nil, ErrInvalidToken
	}
	body := input[len(publicTokenPrefix):]
	if !validTokenBody(body) {
		return nil, ErrInvalidToken
	}
	return append([]byte(nil), body...), nil
}

func validTokenBody(body []byte) bool {
	if len(body) != CanonicalTokenBodyBytes || !bytes.HasPrefix(body, []byte(tokenVersion+".")) {
		return false
	}
	id := body[3:39]
	secret := body[40:]
	return body[39] == '.' && ValidCanonicalUUID(string(id)) && validCanonicalSecret(secret)
}

// ValidTokenBody validates the prefix-free token accepted by the service.
func ValidTokenBody(body []byte) bool { return validTokenBody(body) }

func validCanonicalSecret(secret []byte) bool {
	if len(secret) != CanonicalSecretBytes {
		return false
	}
	var decoded [32]byte
	n, err := base64.RawURLEncoding.Decode(decoded[:], secret)
	if err != nil || n != len(decoded) {
		clear(decoded[:])
		return false
	}
	var canonical [CanonicalSecretBytes]byte
	base64.RawURLEncoding.Encode(canonical[:], decoded[:])
	ok := bytes.Equal(canonical[:], secret)
	clear(decoded[:])
	clear(canonical[:])
	return ok
}

// ValidCanonicalUUID accepts the lowercase, hyphenated RFC 4122 text form.
func ValidCanonicalUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index := 0; index < len(value); index++ {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((value[index] >= '0' && value[index] <= '9') || (value[index] >= 'a' && value[index] <= 'f')) {
			return false
		}
	}
	return true
}

// GenerateInstallation returns one UUIDv4 and one canonical 32-byte secret.
func GenerateInstallation(random io.Reader) (string, []byte, error) {
	if random == nil {
		return "", nil, errors.New("enrollment entropy unavailable")
	}
	var id [16]byte
	var secretRaw [32]byte
	if _, err := io.ReadFull(random, id[:]); err != nil {
		return "", nil, errors.New("enrollment entropy unavailable")
	}
	if _, err := io.ReadFull(random, secretRaw[:]); err != nil {
		clear(id[:])
		return "", nil, errors.New("enrollment entropy unavailable")
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	identifier := formatUUID(id)
	secret := make([]byte, CanonicalSecretBytes)
	base64.RawURLEncoding.Encode(secret, secretRaw[:])
	clear(id[:])
	clear(secretRaw[:])
	return identifier, secret, nil
}

func formatUUID(value [16]byte) string {
	var encoded [32]byte
	hex.Encode(encoded[:], value[:])
	output := make([]byte, 0, 36)
	output = append(output, encoded[0:8]...)
	output = append(output, '-')
	output = append(output, encoded[8:12]...)
	output = append(output, '-')
	output = append(output, encoded[12:16]...)
	output = append(output, '-')
	output = append(output, encoded[16:20]...)
	output = append(output, '-')
	output = append(output, encoded[20:32]...)
	clear(encoded[:])
	return string(output)
}
