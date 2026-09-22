package protocol

import (
	"crypto/rand"
	"encoding/base64"
	"io"
)

const (
	identifierEntropyBytes = 16
	defaultCollisionTries  = 8
)

// IDGenerator creates private collision-resistant identifiers from injectable
// entropy. Exists, when non-nil, allows the owning registry to reject a rare
// collision before the identifier is returned.
type IDGenerator struct {
	Reader      io.Reader
	MaxAttempts int
	Exists      func(string) bool
}

// Generate returns a prefix plus 128 bits encoded with unpadded base64url.
func (g IDGenerator) Generate(prefix string) (string, error) {
	if prefix != "msg_" && prefix != "cor_" {
		return "", ErrInvalidIdentifier
	}
	reader := g.Reader
	if reader == nil {
		reader = rand.Reader
	}
	attempts := g.MaxAttempts
	if attempts == 0 {
		attempts = defaultCollisionTries
	}
	if attempts < 0 || attempts > defaultCollisionTries {
		return "", ErrIdentifierCollision
	}

	var entropy [identifierEntropyBytes]byte
	for range attempts {
		if _, err := io.ReadFull(reader, entropy[:]); err != nil {
			return "", ErrIdentifierEntropy
		}
		id := prefix + base64.RawURLEncoding.EncodeToString(entropy[:])
		if g.Exists == nil || !g.Exists(id) {
			return id, nil
		}
	}
	return "", ErrIdentifierCollision
}

// NewMessageID generates one stable unique message identifier.
func NewMessageID() (string, error) {
	return (IDGenerator{}).Generate("msg_")
}

// NewCorrelationID generates a private RPC correlation identifier.
func NewCorrelationID() (string, error) {
	return (IDGenerator{}).Generate("cor_")
}
