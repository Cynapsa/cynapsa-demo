package payload

import (
	"crypto/sha256"
	"crypto/subtle"
)

// Digest returns the V1 canonical SHA-256 digest.
func Digest(value []byte) [sha256.Size]byte { return sha256.Sum256(value) }

// Verify checks exact size and digest before materialization. A fresh digest is
// calculated over the bytes actually received rather than declared metadata.
func Verify(value []byte, expectedSize int64, expectedDigest []byte) error {
	if expectedSize < 0 || int64(len(value)) != expectedSize || len(expectedDigest) != sha256.Size {
		return ErrIntegrity
	}
	actual := sha256.Sum256(value)
	if subtle.ConstantTimeCompare(actual[:], expectedDigest) != 1 {
		return ErrIntegrity
	}
	return nil
}
