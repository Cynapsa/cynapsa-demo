package payload

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/fxamacker/cbor/v2"
)

const (
	TransferVersion1     uint16 = 1
	transferIDPrefix            = "xfer_"
	transferIDBytes             = 16
	maximumManifestBytes        = 16 << 10
)

type TransferManifest struct {
	Version           uint16
	TransferID        string
	MessageID         string
	MeshID            string
	SenderID          string
	RecipientID       string
	CanonicalSize     int64
	CanonicalDigest   []byte
	TransferredSize   int64
	TransferredDigest []byte
	ChunkSize         int
	ChunkCount        uint32
	EncryptionRef     string
	ExpiresAt         time.Time
}

type TransferChunk struct {
	TransferID string
	Index      uint32
	Offset     int64
	Bytes      []byte
	Digest     []byte
}

type wireManifestV1 struct {
	Version           uint16 `cbor:"0,keyasint"`
	TransferID        string `cbor:"1,keyasint"`
	MessageID         string `cbor:"2,keyasint"`
	MeshID            string `cbor:"3,keyasint"`
	SenderID          string `cbor:"4,keyasint"`
	RecipientID       string `cbor:"5,keyasint"`
	CanonicalSize     uint64 `cbor:"6,keyasint"`
	CanonicalDigest   []byte `cbor:"7,keyasint"`
	TransferredSize   uint64 `cbor:"8,keyasint"`
	TransferredDigest []byte `cbor:"9,keyasint"`
	ChunkSize         uint32 `cbor:"10,keyasint"`
	ChunkCount        uint32 `cbor:"11,keyasint"`
	EncryptionRef     string `cbor:"12,keyasint,omitempty"`
	ExpiresAtMS       int64  `cbor:"13,keyasint"`
}

type wireChunkV1 struct {
	Version    uint16 `cbor:"0,keyasint"`
	TransferID string `cbor:"1,keyasint"`
	Index      uint32 `cbor:"2,keyasint"`
	Offset     uint64 `cbor:"3,keyasint"`
	Bytes      []byte `cbor:"4,keyasint"`
	Digest     []byte `cbor:"5,keyasint"`
}

func NewTransferID(random io.Reader) (string, error) {
	if random == nil {
		random = rand.Reader
	}
	raw := make([]byte, transferIDBytes)
	if _, err := io.ReadFull(random, raw); err != nil {
		zero(raw)
		return "", ErrInvalidManifest
	}
	value := transferIDPrefix + base64.RawURLEncoding.EncodeToString(raw)
	zero(raw)
	return value, nil
}

func BuildManifest(binding TransferBinding, canonical, transferred []byte, chunkSize int, encryptionRef string, expiresAt time.Time, random io.Reader) (TransferManifest, error) {
	if binding.TransferID == "" {
		id, err := NewTransferID(random)
		if err != nil {
			return TransferManifest{}, err
		}
		binding.TransferID = id
	}
	if validateEncryptionBinding(binding) != nil || Verify(canonical, binding.CanonicalSize, binding.CanonicalDigest[:]) != nil {
		return TransferManifest{}, ErrInvalidManifest
	}
	if chunkSize <= 0 || len(transferred) == 0 || int64(len(transferred)) > MaximumTransferredBytes {
		return TransferManifest{}, ErrInvalidManifest
	}
	if encryptionRef == "" {
		if len(transferred) != len(canonical) {
			return TransferManifest{}, ErrInvalidManifest
		}
	} else {
		wrapped, err := decodeEncryptionRef(encryptionRef)
		zero(wrapped)
		if err != nil || len(transferred) != len(canonical)+encryptionFrameOverhead {
			return TransferManifest{}, ErrInvalidManifest
		}
	}
	count64 := int64((len(transferred)-1)/chunkSize + 1)
	if count64 > math.MaxUint32 {
		return TransferManifest{}, ErrInvalidManifest
	}
	canonicalDigest := sha256.Sum256(canonical)
	transferredDigest := sha256.Sum256(transferred)
	manifest := TransferManifest{
		Version: TransferVersion1, TransferID: binding.TransferID, MessageID: binding.MessageID, MeshID: binding.MeshID, SenderID: binding.SenderID, RecipientID: binding.RecipientID,
		CanonicalSize: int64(len(canonical)), CanonicalDigest: clone(canonicalDigest[:]), TransferredSize: int64(len(transferred)), TransferredDigest: clone(transferredDigest[:]),
		ChunkSize: chunkSize, ChunkCount: uint32(count64), EncryptionRef: encryptionRef, ExpiresAt: expiresAt.UTC().Truncate(time.Millisecond),
	}
	return manifest, nil
}

// ForEachChunk deterministically generates at most one copied bounded chunk at
// a time. It never duplicates the complete transfer in a second allocation.
func ForEachChunk(manifest TransferManifest, transferred []byte, yield func(TransferChunk) error) error {
	if yield == nil || manifest.ChunkSize <= 0 || manifest.ChunkCount == 0 || int64(len(transferred)) != manifest.TransferredSize {
		return ErrInvalidChunk
	}
	for index, offset := uint32(0), 0; offset < len(transferred); index, offset = index+1, offset+manifest.ChunkSize {
		end := offset + manifest.ChunkSize
		if end > len(transferred) {
			end = len(transferred)
		}
		part := clone(transferred[offset:end])
		digest := sha256.Sum256(part)
		chunk := TransferChunk{TransferID: manifest.TransferID, Index: index, Offset: int64(offset), Bytes: part, Digest: clone(digest[:])}
		if err := ValidateTransferChunk(manifest, chunk); err != nil {
			zero(part)
			return err
		}
		if err := yield(chunk); err != nil {
			zero(part)
			return err
		}
		zero(part)
	}
	return nil
}

func ValidateTransferManifest(manifest TransferManifest, binding TransferBinding, limits Limits, now time.Time) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if manifest.Version != TransferVersion1 || !validTransferID(manifest.TransferID) || !validMessageID(manifest.MessageID) || protocol.ValidateMeshID(manifest.MeshID) != nil || protocol.ValidateAgentIdentity(manifest.SenderID) != nil || protocol.ValidateAgentIdentity(manifest.RecipientID) != nil || !validCanonicalProfile(binding.Profile) || manifest.TransferID != binding.TransferID || manifest.MessageID != binding.MessageID || manifest.MeshID != binding.MeshID || manifest.SenderID != binding.SenderID || manifest.RecipientID != binding.RecipientID || manifest.CanonicalSize != binding.CanonicalSize || !bytes.Equal(manifest.CanonicalDigest, binding.CanonicalDigest[:]) {
		return ErrInvalidManifest
	}
	if manifest.CanonicalSize < 0 || manifest.CanonicalSize > limits.MaximumPayloadBytes || manifest.TransferredSize <= 0 || manifest.TransferredSize > limits.ReassemblyBytesPerPeer || manifest.TransferredSize > limits.ReassemblyBytes || len(manifest.CanonicalDigest) != sha256.Size || len(manifest.TransferredDigest) != sha256.Size || manifest.ChunkSize <= 0 || manifest.ChunkSize > limits.ChunkBytes || manifest.ChunkCount == 0 || manifest.ChunkCount > limits.MaximumChunks || len(manifest.EncryptionRef) > 256 || !validPrivateText(manifest.EncryptionRef) {
		return ErrInvalidManifest
	}
	if manifest.EncryptionRef != "" {
		wrapped, err := decodeEncryptionRef(manifest.EncryptionRef)
		zero(wrapped)
		if err != nil {
			return ErrInvalidManifest
		}
		if manifest.TransferredSize != manifest.CanonicalSize+encryptionFrameOverhead {
			return ErrInvalidManifest
		}
	} else if manifest.TransferredSize != manifest.CanonicalSize {
		return ErrInvalidManifest
	}
	expected := (manifest.TransferredSize + int64(manifest.ChunkSize) - 1) / int64(manifest.ChunkSize)
	if expected != int64(manifest.ChunkCount) || manifest.ExpiresAt.IsZero() || manifest.ExpiresAt.Location() != time.UTC || manifest.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 || !manifest.ExpiresAt.After(now) || manifest.ExpiresAt.Sub(now) > limits.TransferLifetime {
		return ErrInvalidManifest
	}
	return nil
}

func ValidateTransferChunk(manifest TransferManifest, chunk TransferChunk) error {
	if chunk.TransferID != manifest.TransferID || chunk.Index >= manifest.ChunkCount || chunk.Offset < 0 || len(chunk.Digest) != sha256.Size {
		return ErrInvalidChunk
	}
	expectedOffset := int64(chunk.Index) * int64(manifest.ChunkSize)
	if expectedOffset < 0 || chunk.Offset != expectedOffset || chunk.Offset >= manifest.TransferredSize {
		return ErrInvalidChunk
	}
	expectedLength := manifest.ChunkSize
	remaining := manifest.TransferredSize - chunk.Offset
	if remaining < int64(expectedLength) {
		expectedLength = int(remaining)
	}
	if len(chunk.Bytes) != expectedLength {
		return ErrInvalidChunk
	}
	digest := sha256.Sum256(chunk.Bytes)
	if !bytes.Equal(digest[:], chunk.Digest) {
		return ErrIntegrity
	}
	return nil
}

func EncodeManifest(manifest TransferManifest) ([]byte, error) {
	if manifest.Version != TransferVersion1 || !validTransferID(manifest.TransferID) || !validMessageID(manifest.MessageID) || protocol.ValidateMeshID(manifest.MeshID) != nil || protocol.ValidateAgentIdentity(manifest.SenderID) != nil || protocol.ValidateAgentIdentity(manifest.RecipientID) != nil || manifest.CanonicalSize < 0 || manifest.CanonicalSize > MaximumCanonicalBytes || manifest.TransferredSize <= 0 || manifest.TransferredSize > MaximumTransferredBytes || len(manifest.CanonicalDigest) != sha256.Size || len(manifest.TransferredDigest) != sha256.Size || manifest.ChunkSize <= 0 || manifest.ChunkSize > 1<<20 || manifest.ChunkCount == 0 || manifest.ChunkCount > MaximumTransferChunks || len(manifest.EncryptionRef) > maximumEncryptionRefBytes || !validPrivateText(manifest.EncryptionRef) || manifest.ExpiresAt.IsZero() || manifest.ExpiresAt.Location() != time.UTC || manifest.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 || manifest.ExpiresAt.Year() < 1970 || manifest.ExpiresAt.Year() > 9999 {
		return nil, ErrInvalidManifest
	}
	if manifest.EncryptionRef != "" {
		wrapped, err := decodeEncryptionRef(manifest.EncryptionRef)
		zero(wrapped)
		if err != nil {
			return nil, ErrInvalidManifest
		}
	}
	mode, err := canonicalEncMode()
	if err != nil {
		return nil, err
	}
	encoded, err := mode.Marshal(wireManifestV1{manifest.Version, manifest.TransferID, manifest.MessageID, manifest.MeshID, manifest.SenderID, manifest.RecipientID, uint64(manifest.CanonicalSize), manifest.CanonicalDigest, uint64(manifest.TransferredSize), manifest.TransferredDigest, uint32(manifest.ChunkSize), manifest.ChunkCount, manifest.EncryptionRef, manifest.ExpiresAt.UnixMilli()})
	if err != nil || len(encoded) > maximumManifestBytes {
		zero(encoded)
		return nil, ErrInvalidManifest
	}
	return encoded, nil
}

func DecodeManifest(encoded []byte) (TransferManifest, error) {
	if len(encoded) == 0 || len(encoded) > maximumManifestBytes {
		return TransferManifest{}, ErrInvalidManifest
	}
	mode, err := strictDecMode(16, 16)
	if err != nil {
		return TransferManifest{}, err
	}
	var wire wireManifestV1
	defer func() {
		zero(wire.CanonicalDigest)
		zero(wire.TransferredDigest)
	}()
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.CanonicalSize > math.MaxInt64 || wire.TransferredSize > math.MaxInt64 || uint64(wire.ChunkSize) > uint64(^uint(0)>>1) || len(wire.CanonicalDigest) != sha256.Size || len(wire.TransferredDigest) != sha256.Size || len(wire.EncryptionRef) > maximumEncryptionRefBytes {
		return TransferManifest{}, ErrInvalidManifest
	}
	manifest := TransferManifest{wire.Version, wire.TransferID, wire.MessageID, wire.MeshID, wire.SenderID, wire.RecipientID, int64(wire.CanonicalSize), clone(wire.CanonicalDigest), int64(wire.TransferredSize), clone(wire.TransferredDigest), int(wire.ChunkSize), wire.ChunkCount, wire.EncryptionRef, time.UnixMilli(wire.ExpiresAtMS).UTC()}
	reencoded, err := EncodeManifest(manifest)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		zero(reencoded)
		zero(manifest.CanonicalDigest)
		zero(manifest.TransferredDigest)
		return TransferManifest{}, ErrNonCanonical
	}
	zero(reencoded)
	return manifest, nil
}

func EncodeChunk(chunk TransferChunk) ([]byte, error) {
	if !validTransferID(chunk.TransferID) || chunk.Offset < 0 || len(chunk.Bytes) == 0 || len(chunk.Bytes) > 1<<20 || len(chunk.Digest) != sha256.Size {
		return nil, ErrInvalidChunk
	}
	mode, err := canonicalEncMode()
	if err != nil {
		return nil, err
	}
	encoded, err := mode.Marshal(wireChunkV1{TransferVersion1, chunk.TransferID, chunk.Index, uint64(chunk.Offset), chunk.Bytes, chunk.Digest})
	if err != nil {
		zero(encoded)
		return nil, ErrInvalidChunk
	}
	return encoded, nil
}

func DecodeChunk(encoded []byte, maximum int) (TransferChunk, error) {
	if maximum <= 0 || len(encoded) == 0 || len(encoded) > maximum+4096 {
		return TransferChunk{}, ErrInvalidChunk
	}
	mode, err := strictDecMode(8, 8)
	if err != nil {
		return TransferChunk{}, err
	}
	var wire wireChunkV1
	defer func() {
		zero(wire.Bytes)
		zero(wire.Digest)
	}()
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.Version != TransferVersion1 || wire.Offset > math.MaxInt64 || len(wire.Bytes) > maximum || len(wire.Digest) != sha256.Size || !validTransferID(wire.TransferID) {
		return TransferChunk{}, ErrInvalidChunk
	}
	chunk := TransferChunk{wire.TransferID, wire.Index, int64(wire.Offset), clone(wire.Bytes), clone(wire.Digest)}
	reencoded, err := EncodeChunk(chunk)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		zero(reencoded)
		zero(chunk.Bytes)
		zero(chunk.Digest)
		return TransferChunk{}, ErrNonCanonical
	}
	zero(reencoded)
	return chunk, nil
}

func EncodeTextChunk(chunk TransferChunk) (string, error) {
	encoded, err := EncodeChunk(chunk)
	if err != nil {
		zero(encoded)
		return "", err
	}
	value := base64.RawStdEncoding.EncodeToString(encoded)
	zero(encoded)
	return value, nil
}

func EncodeTextManifest(manifest TransferManifest) (string, error) {
	encoded, err := EncodeManifest(manifest)
	if err != nil {
		zero(encoded)
		return "", err
	}
	value := base64.RawStdEncoding.EncodeToString(encoded)
	zero(encoded)
	return value, nil
}
func DecodeTextManifest(value string) (TransferManifest, error) {
	if len(value) > 2*maximumManifestBytes {
		return TransferManifest{}, ErrInvalidManifest
	}
	encoded, err := base64.RawStdEncoding.Strict().DecodeString(value)
	if err != nil || base64.RawStdEncoding.EncodeToString(encoded) != value {
		zero(encoded)
		return TransferManifest{}, ErrInvalidManifest
	}
	manifest, decodeErr := DecodeManifest(encoded)
	zero(encoded)
	return manifest, decodeErr
}
func DecodeTextChunk(value string, maximum int) (TransferChunk, error) {
	if len(value) > 2*(maximum+4096) {
		return TransferChunk{}, ErrInvalidChunk
	}
	encoded, err := base64.RawStdEncoding.Strict().DecodeString(value)
	if err != nil || base64.RawStdEncoding.EncodeToString(encoded) != value {
		zero(encoded)
		return TransferChunk{}, ErrInvalidChunk
	}
	chunk, decodeErr := DecodeChunk(encoded, maximum)
	zero(encoded)
	return chunk, decodeErr
}

func canonicalEncMode() (cbor.EncMode, error) {
	opts := cbor.CoreDetEncOptions()
	opts.IndefLength = cbor.IndefLengthForbidden
	opts.TagsMd = cbor.TagsForbidden
	mode, err := opts.EncMode()
	if err != nil {
		return nil, ErrMalformedCanonical
	}
	return mode, nil
}
func strictDecMode(arrays, pairs int) (cbor.DecMode, error) {
	if arrays < 16 {
		arrays = 16
	}
	if pairs < 16 {
		pairs = 16
	}
	mode, err := (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, MaxNestedLevels: 4, MaxArrayElements: arrays, MaxMapPairs: pairs, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden, ExtraReturnErrors: cbor.ExtraDecErrorUnknownField, UTF8: cbor.UTF8RejectInvalid}).DecMode()
	if err != nil {
		return nil, ErrMalformedCanonical
	}
	return mode, nil
}
func validTransferID(value string) bool {
	if !strings.HasPrefix(value, transferIDPrefix) {
		return false
	}
	encoded := strings.TrimPrefix(value, transferIDPrefix)
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	valid := err == nil && len(decoded) == transferIDBytes && base64.RawURLEncoding.EncodeToString(decoded) == encoded
	zero(decoded)
	return valid
}
func validMessageID(value string) bool {
	if !strings.HasPrefix(value, "msg_") {
		return false
	}
	encoded := strings.TrimPrefix(value, "msg_")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	valid := err == nil && len(decoded) == 16 && base64.RawURLEncoding.EncodeToString(decoded) == encoded
	zero(decoded)
	return valid
}
func validPrivateText(value string) bool {
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
