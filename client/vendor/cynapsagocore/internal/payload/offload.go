package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const objectReferencePrefix = "obj1_"

type wireObjectReferenceV1 struct {
	Version    uint16 `cbor:"0,keyasint"`
	TransferID string `cbor:"1,keyasint"`
	URL        string `cbor:"2,keyasint"`
}

// PackageInline chooses inline solely from canonical byte size. Oversized data
// returns ErrInlineLimitExceeded for the coordinator, never a guessed reference.
func PackageInline(profile string, canonical []byte, inlineLimit int64) (protocol.PayloadDescriptor, error) {
	if inlineLimit < 0 || inlineLimit > protocol.MaxInlinePayloadBytes {
		return protocol.PayloadDescriptor{}, ErrInvalidLimits
	}
	if int64(len(canonical)) > inlineLimit {
		return protocol.PayloadDescriptor{}, ErrInlineLimitExceeded
	}
	if err := validateCanonicalProfileOnly(canonical, profile, protocol.MaxInlinePayloadBytes); err != nil {
		return protocol.PayloadDescriptor{}, err
	}
	return protocol.NewInlinePayload(profile, canonical)
}

func encodeObjectReference(transferID, rawURL string) (string, error) {
	if !validTransferID(transferID) || rawURL == "" || len(rawURL) > protocol.MaxPrivateReferenceBytes || !utf8.ValidString(rawURL) {
		return "", ErrInvalidManifest
	}
	// CBOR and Base64 sizes are computed before either encoder allocates.
	wireBytes := int64(1+2+2) + cborValueSize(len(transferID)) + cborValueSize(len(rawURL))
	if wireBytes <= 0 || wireBytes > int64(^uint(0)>>1) || base64.RawURLEncoding.EncodedLen(int(wireBytes)) > protocol.MaxPrivateReferenceBytes-len(objectReferencePrefix) {
		return "", ErrPayloadTooLarge
	}
	mode, err := canonicalEncMode()
	if err != nil {
		return "", err
	}
	encoded, err := mode.Marshal(wireObjectReferenceV1{TransferVersion1, transferID, rawURL})
	if err != nil {
		zero(encoded)
		return "", ErrInvalidManifest
	}
	value := objectReferencePrefix + base64.RawURLEncoding.EncodeToString(encoded)
	zero(encoded)
	if len(value) > protocol.MaxPrivateReferenceBytes {
		return "", ErrPayloadTooLarge
	}
	return value, nil
}

func decodeObjectReference(value string) (string, string, error) {
	if !strings.HasPrefix(value, objectReferencePrefix) || len(value) > protocol.MaxPrivateReferenceBytes {
		return "", "", ErrInvalidManifest
	}
	body := strings.TrimPrefix(value, objectReferencePrefix)
	if body == "" || strings.ContainsAny(body, "\r\n") {
		return "", "", ErrInvalidManifest
	}
	encoded, err := base64.RawURLEncoding.Strict().DecodeString(body)
	if err != nil {
		return "", "", ErrInvalidManifest
	}
	defer zero(encoded)
	mode, err := strictDecMode(4, 4)
	if err != nil {
		return "", "", err
	}
	var wire wireObjectReferenceV1
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.Version != TransferVersion1 || !validTransferID(wire.TransferID) || wire.URL == "" {
		return "", "", ErrInvalidManifest
	}
	reencoded, err := canonicalEncMode()
	if err != nil {
		return "", "", err
	}
	canonical, err := reencoded.Marshal(wire)
	defer zero(canonical)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return "", "", ErrNonCanonical
	}
	if objectReferencePrefix+base64.RawURLEncoding.EncodeToString(canonical) != value {
		return "", "", ErrNonCanonical
	}
	return wire.TransferID, wire.URL, nil
}

// ParseObjectReference validates one private canonical reference and returns
// newly owned immutable strings for internal authenticated control adapters.
// It is not part of the SDK boundary.
func ParseObjectReference(value string) (transferID, rawURL string, err error) {
	return decodeObjectReference(value)
}

// Materializer resolves private descriptors and never exposes their references.
type Materializer struct {
	maximum   int64
	objects   ObjectStore
	cipher    PayloadCipher
	transfers *Reassembler
	ingressMu sync.Mutex
	ingresses map[string]*objectIngressFlight
}

type objectIngressFlight struct {
	privateReference string
	manifest         TransferManifest
	binding          TransferBinding
	operation        context.Context
	cleanup          func()
	finish           func()
	done             chan struct{}
	evidence         CompletionEvidence
	err              error
	waiters          int
}

func NewMaterializer(maximum int64, objects ObjectStore, cipher PayloadCipher, transfers *Reassembler) (*Materializer, error) {
	if maximum <= 0 || maximum > MaximumCanonicalBytes {
		return nil, ErrInvalidLimits
	}
	return &Materializer{maximum: maximum, objects: objects, cipher: cipher, transfers: transfers, ingresses: make(map[string]*objectIngressFlight)}, nil
}

func (m *Materializer) Materialize(ctx context.Context, descriptor protocol.PayloadDescriptor, binding TransferBinding) ([]byte, error) {
	return m.MaterializeUntil(ctx, descriptor, binding, time.Time{})
}

// MaterializeUntil resolves a logical descriptor without permitting an
// envelope-first transfer wait to outlive the authenticated request expiry.
func (m *Materializer) MaterializeUntil(ctx context.Context, descriptor protocol.PayloadDescriptor, binding TransferBinding, requestExpiry time.Time) ([]byte, error) {
	if m == nil || ctx == nil {
		return nil, ErrInvalidLimits
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if descriptor.Size < 0 || descriptor.Size > m.maximum {
		return nil, ErrPayloadTooLarge
	}
	if err := protocol.ValidatePayloadDescriptor(descriptor); err != nil {
		return nil, ErrInvalidManifest
	}
	switch descriptor.Kind {
	case protocol.PayloadInline:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := descriptor.Materialize()
		if err != nil {
			return nil, ErrIntegrity
		}
		if err := Verify(value, descriptor.Size, descriptor.Digest[:]); err != nil {
			zero(value)
			return nil, err
		}
		if binding.Profile != "" && binding.Profile != descriptor.Profile {
			zero(value)
			return nil, ErrAuthentication
		}
		if err := validateCanonicalProfileOnly(value, descriptor.Profile, m.maximum); err != nil {
			zero(value)
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			zero(value)
			return nil, err
		}
		return value, nil
	case protocol.PayloadObjectReference:
		if m.objects == nil {
			return nil, ErrCarrierUnavailable
		}
		transferID, rawURL, err := decodeObjectReference(descriptor.Reference)
		if err != nil {
			return nil, err
		}
		if binding.TransferID != transferID || binding.Profile != descriptor.Profile || binding.CanonicalSize != descriptor.Size || !bytes.Equal(binding.CanonicalDigest[:], descriptor.Digest[:]) {
			return nil, ErrAuthentication
		}
		transferred, err := m.objects.Download(ctx, rawURL)
		if err != nil {
			zero(transferred)
			if contextErr := dependencyContextError(ctx, err); contextErr != nil {
				return nil, contextErr
			}
			return nil, ErrCarrierMaterialization
		}
		expected := descriptor.Size
		if descriptor.EncryptionRef != "" {
			expected += encryptionFrameOverhead
		}
		if int64(len(transferred)) != expected {
			zero(transferred)
			return nil, ErrIntegrity
		}
		canonical := transferred
		if descriptor.EncryptionRef != "" {
			if m.cipher == nil {
				zero(transferred)
				return nil, ErrCarrierUnavailable
			}
			canonical, err = m.cipher.Decrypt(ctx, binding, transferred, descriptor.EncryptionRef)
			zero(transferred)
			if err != nil {
				zero(canonical)
				if contextErr := dependencyContextError(ctx, err); contextErr != nil {
					return nil, contextErr
				}
				return nil, ErrAuthentication
			}
		}
		if err := Verify(canonical, descriptor.Size, descriptor.Digest[:]); err != nil {
			zero(canonical)
			return nil, err
		}
		if err := validateCanonicalProfileOnly(canonical, descriptor.Profile, m.maximum); err != nil {
			zero(canonical)
			return nil, err
		}
		return canonical, nil
	case protocol.PayloadTransferReference:
		if m.transfers == nil || !validTransferID(descriptor.Reference) {
			return nil, ErrCarrierUnavailable
		}
		if binding.TransferID != descriptor.Reference || binding.Profile != descriptor.Profile || binding.CanonicalSize != descriptor.Size || !bytes.Equal(binding.CanonicalDigest[:], descriptor.Digest[:]) {
			return nil, ErrAuthentication
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		canonical, err := m.transfers.WaitConsume(ctx, descriptor.Reference, binding, requestExpiry)
		if err != nil {
			return nil, err
		}
		if err := Verify(canonical, descriptor.Size, descriptor.Digest[:]); err != nil {
			zero(canonical)
			return nil, err
		}
		if err := validateCanonicalProfileOnly(canonical, descriptor.Profile, m.maximum); err != nil {
			zero(canonical)
			return nil, err
		}
		return canonical, nil
	default:
		return nil, ErrInvalidManifest
	}
}

// IngestObject materializes one authenticated private object into the bounded
// completed-transfer registry. It never delivers bytes directly to an SDK.
func (m *Materializer) IngestObject(ctx context.Context, manifest TransferManifest, binding TransferBinding, privateReference string) (CompletionEvidence, error) {
	if m == nil || ctx == nil || m.objects == nil || m.transfers == nil {
		return CompletionEvidence{}, ErrCarrierUnavailable
	}
	if err := ctx.Err(); err != nil {
		return CompletionEvidence{}, err
	}
	transferID, rawURL, err := decodeObjectReference(privateReference)
	if err != nil || transferID != manifest.TransferID || transferID != binding.TransferID {
		return CompletionEvidence{}, ErrAuthentication
	}
	m.ingressMu.Lock()
	if flight, exists := m.ingresses[transferID]; exists {
		if flight.privateReference != privateReference || !manifestsEqual(flight.manifest, manifest) || flight.binding != binding {
			m.ingressMu.Unlock()
			return CompletionEvidence{}, ErrAuthentication
		}
		m.ingressMu.Unlock()
		return m.waitObjectIngress(ctx, flight)
	}
	if err := m.transfers.Begin(manifest, binding, CarrierObjectUpload); err != nil {
		m.ingressMu.Unlock()
		return CompletionEvidence{}, err
	}
	if evidence, completed, err := m.transfers.Completion(transferID, binding); err == nil && completed {
		m.ingressMu.Unlock()
		return evidence, nil
	}
	operation, cleanup, finish, err := m.transfers.beginObjectOperation(manifest, binding)
	if err != nil {
		m.ingressMu.Unlock()
		return CompletionEvidence{}, err
	}
	flight := &objectIngressFlight{privateReference: privateReference, manifest: cloneManifest(manifest), binding: binding, operation: operation, cleanup: cleanup, finish: finish, done: make(chan struct{})}
	m.ingresses[transferID] = flight
	m.ingressMu.Unlock()

	go m.runObjectIngress(flight, rawURL)
	return m.waitObjectIngress(ctx, flight)
}

func (m *Materializer) waitObjectIngress(ctx context.Context, flight *objectIngressFlight) (CompletionEvidence, error) {
	m.ingressMu.Lock()
	flight.waiters++
	m.ingressMu.Unlock()
	defer func() {
		m.ingressMu.Lock()
		flight.waiters--
		m.ingressMu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return CompletionEvidence{}, ctx.Err()
	case <-flight.done:
		return flight.evidence, flight.err
	}
}

func (m *Materializer) runObjectIngress(flight *objectIngressFlight, rawURL string) {
	defer flight.finish()
	evidence, err := m.executeObjectIngress(flight.operation, flight.manifest, flight.binding, rawURL)
	flight.cleanup()
	m.ingressMu.Lock()
	flight.evidence = evidence
	flight.err = err
	if m.ingresses[flight.manifest.TransferID] == flight {
		delete(m.ingresses, flight.manifest.TransferID)
	}
	zero(flight.manifest.CanonicalDigest)
	zero(flight.manifest.TransferredDigest)
	close(flight.done)
	m.ingressMu.Unlock()
	flight.finish()
}

func (m *Materializer) executeObjectIngress(operation context.Context, manifest TransferManifest, binding TransferBinding, rawURL string) (CompletionEvidence, error) {
	if evidence, completed, err := m.transfers.Completion(manifest.TransferID, binding); err == nil && completed {
		return evidence, nil
	}
	ciphertext, err := m.objects.Download(operation, rawURL)
	if err != nil {
		zero(ciphertext)
		m.transfers.abortObjectFromOwner(manifest.TransferID)
		if contextErr := dependencyContextError(operation, err); contextErr != nil {
			return CompletionEvidence{}, contextErr
		}
		return CompletionEvidence{}, ErrCarrierMaterialization
	}
	if int64(len(ciphertext)) != manifest.TransferredSize {
		zero(ciphertext)
		m.transfers.abortObjectFromOwner(manifest.TransferID)
		return CompletionEvidence{}, ErrIntegrity
	}
	if contextErr := operation.Err(); contextErr != nil {
		zero(ciphertext)
		m.transfers.abortObjectFromOwner(manifest.TransferID)
		return CompletionEvidence{}, contextErr
	}
	if err := m.transfers.CompleteObject(operation, manifest.TransferID, ciphertext); err != nil {
		m.transfers.abortObjectFromOwner(manifest.TransferID)
		return CompletionEvidence{}, err
	}
	evidence, completed, err := m.transfers.Completion(manifest.TransferID, binding)
	if err != nil || !completed {
		return CompletionEvidence{}, ErrCarrierMaterialization
	}
	return evidence, nil
}

func referencedDescriptor(kind protocol.PayloadKind, profile, reference, encryptionRef string, canonical []byte) (protocol.PayloadDescriptor, error) {
	digest := sha256.Sum256(canonical)
	return protocol.NewReferencedPayload(kind, profile, reference, int64(len(canonical)), digest, encryptionRef)
}
