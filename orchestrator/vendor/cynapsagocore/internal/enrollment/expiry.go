package enrollment

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"
)

const maximumJWTPayloadBytes = 16 << 10

// UsableUntil conservatively bounds a received credential by both the
// provider's relative lifetime and the JWT payload's exp claim. Parsing the
// payload establishes a local deadline only; it does not verify the JWT's
// signature. The server remains responsible for cryptographic verification.
func UsableUntil(accessToken []byte, receivedAt time.Time, expiresIn int64) (time.Time, error) {
	if receivedAt.IsZero() || expiresIn <= 0 || expiresIn > maximumExpirySeconds {
		return time.Time{}, errors.New("invalid credential lifetime")
	}
	expiry, err := jwtExpiry(accessToken)
	if err != nil {
		return time.Time{}, err
	}
	relative := receivedAt.UTC().Add(time.Duration(expiresIn) * time.Second)
	if expiry.Before(relative) {
		return expiry, nil
	}
	return relative, nil
}

func jwtExpiry(token []byte) (time.Time, error) {
	parts := bytes.Split(token, []byte("."))
	if len(parts) != 3 || len(parts[0]) == 0 || len(parts[1]) == 0 || len(parts[2]) == 0 {
		return time.Time{}, errors.New("invalid credential token")
	}
	if base64.RawURLEncoding.DecodedLen(len(parts[1])) > maximumJWTPayloadBytes {
		return time.Time{}, errors.New("invalid credential token")
	}
	payload := make([]byte, base64.RawURLEncoding.DecodedLen(len(parts[1])))
	n, err := base64.RawURLEncoding.Decode(payload, parts[1])
	if err != nil {
		clear(payload)
		return time.Time{}, errors.New("invalid credential token")
	}
	payload = payload[:n]
	defer clear(payload)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return time.Time{}, errors.New("invalid credential token")
	}
	seen := make(map[string]struct{}, 16)
	var expiration int64
	found := false
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return time.Time{}, errors.New("invalid credential token")
		}
		if _, duplicate := seen[key]; duplicate {
			return time.Time{}, errors.New("invalid credential token")
		}
		seen[key] = struct{}{}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return time.Time{}, errors.New("invalid credential token")
		}
		if key != "exp" {
			continue
		}
		text := string(raw)
		if text == "" || bytes.IndexFunc(raw, func(character rune) bool {
			return character < '0' || character > '9'
		}) >= 0 {
			return time.Time{}, errors.New("invalid credential token")
		}
		expiration, err = strconv.ParseInt(text, 10, 64)
		if err != nil || expiration <= 0 {
			return time.Time{}, errors.New("invalid credential token")
		}
		found = true
	}
	if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') || !found {
		return time.Time{}, errors.New("invalid credential token")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return time.Time{}, errors.New("invalid credential token")
	}
	expires := time.Unix(expiration, 0).UTC()
	if expires.Unix() != expiration {
		return time.Time{}, errors.New("invalid credential token")
	}
	return expires, nil
}
