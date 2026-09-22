package rpc

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"strings"
)

const (
	requestHandlePrefix       = "reqh_"
	requestHandleEntropyBytes = 32
	requestHandleAttempts     = 8
	requestHandleLength       = len(requestHandlePrefix) + (requestHandleEntropyBytes*8+5)/6
)

// HandleGenerator creates exact ADR 0004 request handles from injectable
// entropy. Exists lets the owning session table reject a rare collision.
type HandleGenerator struct {
	Reader      io.Reader
	MaxAttempts int
	Exists      func(string) bool
}

// Generate returns reqh_ plus unpadded base64url encoding of 32 random bytes.
func (g HandleGenerator) Generate() (string, error) {
	reader := g.Reader
	if reader == nil {
		reader = rand.Reader
	}
	attempts := g.MaxAttempts
	if attempts == 0 {
		attempts = requestHandleAttempts
	}
	if attempts < 1 || attempts > requestHandleAttempts {
		return "", ErrHandleCollision
	}

	var entropy [requestHandleEntropyBytes]byte
	for range attempts {
		if _, err := io.ReadFull(reader, entropy[:]); err != nil {
			return "", ErrHandleEntropy
		}
		handle := requestHandlePrefix + base64.RawURLEncoding.EncodeToString(entropy[:])
		if g.Exists == nil || !g.Exists(handle) {
			return handle, nil
		}
	}
	return "", ErrHandleCollision
}

// NewRequestHandle creates one opaque process-local request handle.
func NewRequestHandle() (string, error) {
	return (HandleGenerator{}).Generate()
}

// ValidateRequestHandle validates the exact class, size, alphabet, and
// canonical unpadded representation without revealing table membership.
func ValidateRequestHandle(handle string) error {
	if !strings.HasPrefix(handle, requestHandlePrefix) {
		return ErrInvalidHandle
	}
	encoded := handle[len(requestHandlePrefix):]
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != requestHandleEntropyBytes || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return ErrInvalidHandle
	}
	return nil
}
