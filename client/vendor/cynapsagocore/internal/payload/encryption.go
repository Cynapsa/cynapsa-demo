package payload

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"io"
	"strings"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	encryptionVersion1        byte = 1
	encryptionRefPrefix            = "enc1_"
	dataKeyBytes                   = 32
	nonceBytes                     = 12
	maximumEncryptionRefBytes      = 256
	maximumWrappedKeyBytes         = 188
	encryptionFrameOverhead        = 1 + nonceBytes + 16
)

var encryptionDomain = []byte("CYNAPSA-PAYLOAD-AEAD-V2\x00")

// KeyContext is the authenticated, carrier-invariant peer context supplied to
// the real identity-key provider. It contains no password or payload bytes.
type KeyContext struct {
	MeshID          string
	SenderID        string
	RecipientID     string
	MessageID       string
	TransferID      string
	CanonicalSize   int64
	CanonicalDigest [32]byte
}

// KeySealer wraps one ephemeral data key for the authenticated recipient. It
// may use key only for the duration of the call and must return opaque,
// authenticated bytes. Concrete identity-key discovery is outside Pod 5.
type KeySealer interface {
	SealKey(context.Context, KeyContext, []byte) ([]byte, error)
}

// KeyResolver authenticates and unwraps one peer-provided ephemeral data key.
// It must return exactly 32 newly owned bytes.
type KeyResolver interface {
	OpenKey(context.Context, KeyContext, []byte) ([]byte, error)
}

// AEADCipher applies the approved V1 AES-256-GCM construction. Key wrapping is
// injected and remains private to the authenticated identity security layer.
type AEADCipher struct {
	sealer   KeySealer
	resolver KeyResolver
	random   io.Reader
	randomMu sync.Mutex
}

func NewAEADCipher(sealer KeySealer, resolver KeyResolver, random io.Reader) (*AEADCipher, error) {
	if sealer == nil || resolver == nil {
		return nil, ErrInvalidLimits
	}
	if random == nil {
		random = rand.Reader
	}
	return &AEADCipher{sealer: sealer, resolver: resolver, random: random}, nil
}

func (c *AEADCipher) Encrypt(ctx context.Context, binding TransferBinding, plaintext []byte) ([]byte, string, error) {
	if c == nil || ctx == nil || validateEncryptionBinding(binding) != nil {
		return nil, "", ErrEncryption
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := Verify(plaintext, binding.CanonicalSize, binding.CanonicalDigest[:]); err != nil {
		return nil, "", ErrIntegrity
	}
	key := make([]byte, dataKeyBytes)
	nonce := make([]byte, nonceBytes)
	c.randomMu.Lock()
	_, keyErr := io.ReadFull(c.random, key)
	_, nonceErr := io.ReadFull(c.random, nonce)
	c.randomMu.Unlock()
	if keyErr != nil || nonceErr != nil {
		zero(key)
		zero(nonce)
		return nil, "", ErrEncryption
	}
	defer zero(key)
	defer zero(nonce)

	keyContext := keyContextFromBinding(binding)
	wrapped, err := c.sealer.SealKey(ctx, keyContext, key)
	if err != nil || len(wrapped) == 0 || len(wrapped) > maximumWrappedKeyBytes {
		zero(wrapped)
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return nil, "", contextErr
		}
		return nil, "", ErrEncryption
	}
	ref := encryptionRefPrefix + base64.RawURLEncoding.EncodeToString(wrapped)
	zero(wrapped)
	if len(ref) > maximumEncryptionRefBytes {
		return nil, "", ErrEncryption
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, "", ErrEncryption
	}
	aead, err := cipher.NewGCMWithNonceSize(block, nonceBytes)
	if err != nil {
		return nil, "", ErrEncryption
	}
	aad, err := encryptionAssociatedData(keyContext)
	if err != nil {
		return nil, "", err
	}
	output := make([]byte, 1+nonceBytes, 1+nonceBytes+len(plaintext)+aead.Overhead())
	output[0] = encryptionVersion1
	copy(output[1:], nonce)
	output = aead.Seal(output, nonce, plaintext, aad)
	return output, ref, nil
}

func (c *AEADCipher) Decrypt(ctx context.Context, binding TransferBinding, ciphertext []byte, encryptionRef string) ([]byte, error) {
	if c == nil || ctx == nil || validateEncryptionBinding(binding) != nil || int64(len(ciphertext)) != binding.CanonicalSize+encryptionFrameOverhead || ciphertext[0] != encryptionVersion1 {
		return nil, ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wrapped, err := decodeEncryptionRef(encryptionRef)
	if err != nil {
		return nil, ErrAuthentication
	}
	defer zero(wrapped)
	keyContext := keyContextFromBinding(binding)
	key, err := c.resolver.OpenKey(ctx, keyContext, wrapped)
	if err != nil || len(key) != dataKeyBytes {
		zero(key)
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return nil, contextErr
		}
		return nil, ErrAuthentication
	}
	defer zero(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrAuthentication
	}
	aead, err := cipher.NewGCMWithNonceSize(block, nonceBytes)
	if err != nil {
		return nil, ErrAuthentication
	}
	aad, err := encryptionAssociatedData(keyContext)
	if err != nil {
		return nil, ErrAuthentication
	}
	nonce := ciphertext[1 : 1+nonceBytes]
	plaintext, err := aead.Open(nil, nonce, ciphertext[1+nonceBytes:], aad)
	if err != nil {
		zero(plaintext)
		return nil, ErrAuthentication
	}
	if err := Verify(plaintext, binding.CanonicalSize, binding.CanonicalDigest[:]); err != nil {
		zero(plaintext)
		return nil, ErrIntegrity
	}
	return plaintext, nil
}

func validateEncryptionBinding(binding TransferBinding) error {
	if !validTransferID(binding.TransferID) || !validMessageID(binding.MessageID) || protocol.ValidateMeshID(binding.MeshID) != nil || protocol.ValidateAgentIdentity(binding.SenderID) != nil || protocol.ValidateAgentIdentity(binding.RecipientID) != nil || binding.CanonicalSize < 0 || binding.CanonicalSize > MaximumCanonicalBytes {
		return ErrAuthentication
	}
	return nil
}

func keyContextFromBinding(binding TransferBinding) KeyContext {
	return KeyContext{MeshID: binding.MeshID, SenderID: binding.SenderID, RecipientID: binding.RecipientID, MessageID: binding.MessageID, TransferID: binding.TransferID, CanonicalSize: binding.CanonicalSize, CanonicalDigest: binding.CanonicalDigest}
}

// encryptionAssociatedData freezes the V1 authenticated preimage as a domain
// separator, five uint32-length-prefixed UTF-8 fields, uint64 size, and digest.
func encryptionAssociatedData(value KeyContext) ([]byte, error) {
	if value.CanonicalSize < 0 {
		return nil, ErrAuthentication
	}
	fields := []string{value.MeshID, value.SenderID, value.RecipientID, value.MessageID, value.TransferID}
	size := len(encryptionDomain) + 16 + len(value.CanonicalDigest)
	for _, field := range fields {
		if uint64(len(field)) > uint64(^uint32(0)) {
			return nil, ErrAuthentication
		}
		size += 4 + len(field)
	}
	output := make([]byte, 0, size)
	output = append(output, encryptionDomain...)
	var scratch [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(field)))
		output = append(output, scratch[:4]...)
		output = append(output, field...)
	}
	binary.BigEndian.PutUint64(scratch[:], uint64(value.CanonicalSize))
	output = append(output, scratch[:]...)
	output = append(output, value.CanonicalDigest[:]...)
	return output, nil
}

func decodeEncryptionRef(value string) ([]byte, error) {
	if !strings.HasPrefix(value, encryptionRefPrefix) || len(value) > maximumEncryptionRefBytes {
		return nil, ErrAuthentication
	}
	encoded := strings.TrimPrefix(value, encryptionRefPrefix)
	wrapped, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(wrapped) == 0 || base64.RawURLEncoding.EncodeToString(wrapped) != encoded {
		zero(wrapped)
		return nil, ErrAuthentication
	}
	return wrapped, nil
}
