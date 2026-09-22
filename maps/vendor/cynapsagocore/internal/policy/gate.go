// Package policy enforces zero-trust authorization before delivery.
package policy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// MembershipVerifier checks current trusted server-derived membership.
type MembershipVerifier interface {
	RequireAll(meshID string, agentIDs ...string) error
}

// ApplicationPathExtractor derives the exact authorization path from one
// verified canonical payload snapshot. Implementations are trusted private
// adapters selected for the canonical payload profile.
type ApplicationPathExtractor interface {
	ExtractApplicationPath(profile string, canonical []byte) (string, error)
}

type RuleAuthorizer interface {
	Allows(agentID, applicationPath string) bool
}

type GateConfig struct {
	MeshID        string
	LocalIdentity string
	Memberships   MembershipVerifier
	Paths         ApplicationPathExtractor
	Rules         *Compiled
	RuleStore     *Store
	Authorizer    RuleAuthorizer
	Entropy       io.Reader
	Now           func() time.Time
}

// Gate owns one immutable session policy configuration.
type Gate struct {
	meshID        string
	localIdentity string
	memberships   MembershipVerifier
	paths         ApplicationPathExtractor
	rules         RuleAuthorizer
	scope         [16]byte
	now           func() time.Time
}

// InboundPermit proves the pre-materialization trust stage completed under
// this Gate. Fields are private and bind the later application authorization.
type InboundPermit struct {
	scope      [16]byte
	digest     [sha256.Size]byte
	integrity  [sha256.Size]byte
	wire       [sha256.Size]byte
	provenance [sha256.Size]byte
	sender     string
	meshID     string
}

// MaterializationResult is an opaque proof that canonical payload bytes were
// verified against the authenticated original descriptor and that the exact
// application path was extracted from that same immutable snapshot.
type MaterializationResult struct {
	scope       [16]byte
	digest      [sha256.Size]byte
	integrity   [sha256.Size]byte
	wire        [sha256.Size]byte
	provenance  [sha256.Size]byte
	payloadHash [sha256.Size]byte
	payloadSize int64
	path        string
	seal        [sha256.Size]byte
}

func NewGate(config GateConfig) (*Gate, error) {
	configuredRules := 0
	if config.Rules != nil {
		configuredRules++
	}
	if config.RuleStore != nil {
		configuredRules++
	}
	if config.Authorizer != nil {
		configuredRules++
	}
	if protocol.ValidateMeshID(config.MeshID) != nil || protocol.ValidateAgentIdentity(config.LocalIdentity) != nil || config.Memberships == nil || config.Paths == nil || config.Now == nil || configuredRules != 1 {
		return nil, ErrInvalidGate
	}
	reader := config.Entropy
	if reader == nil {
		reader = rand.Reader
	}
	now := config.Now
	var rules RuleAuthorizer = config.Authorizer
	if rules == nil && config.RuleStore != nil {
		rules = config.RuleStore
	}
	if rules == nil {
		var err error
		rules, err = NewStore(config.Rules.Rules())
		if err != nil {
			return nil, ErrInvalidGate
		}
	}
	gate := &Gate{meshID: config.MeshID, localIdentity: config.LocalIdentity, memberships: config.Memberships, paths: config.Paths, rules: rules, now: now}
	if _, err := io.ReadFull(reader, gate.scope[:]); err != nil {
		return nil, ErrInvalidGate
	}
	return gate, nil
}

// EvaluateInbound performs the pre-materialization stage in fail-closed order:
// syntax, typed carrier provenance and identity/mesh binding, deterministic
// integrity, calibrated time, then authoritative membership. The composed
// service checks sender uncertainty after this provenance stage because the
// Gate does not own the local clock bound.
func (gate *Gate) EvaluateInbound(envelope protocol.Envelope, provenance AuthenticatedProvenance) (InboundPermit, error) {
	if gate == nil {
		return InboundPermit{}, ErrInvalidGate
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return InboundPermit{}, errors.Join(ErrIntegrity, err)
	}
	if envelope.MeshID != gate.meshID || envelope.Recipient != gate.localIdentity {
		return InboundPermit{}, ErrMesh
	}
	if provenance.validateFor(envelope.Sender, gate.localIdentity, gate.meshID) != nil {
		return InboundPermit{}, ErrProvenance
	}
	if _, err := conversation.VerifyEnvelopeBinding(envelope, conversation.AuthenticatedBinding{MeshID: gate.meshID, AuthenticatedSender: provenance.sender, Recipient: gate.localIdentity}); err != nil {
		return InboundPermit{}, errors.Join(ErrIdentity, err)
	}
	canonical, err := protocol.CanonicalIntegrityBytes(envelope)
	if err != nil {
		return InboundPermit{}, errors.Join(ErrIntegrity, err)
	}
	defer clearBytes(canonical)
	now := gate.now()
	if now.IsZero() || now.Location() != time.UTC {
		return InboundPermit{}, ErrClock
	}
	if err := gate.memberships.RequireAll(gate.meshID, provenance.sender, gate.localIdentity); err != nil {
		return InboundPermit{}, ErrMembership
	}
	digest, err := protocol.LogicalMessageDigest(envelope)
	if err != nil {
		return InboundPermit{}, errors.Join(ErrIntegrity, err)
	}
	integrity := sha256.Sum256(canonical)
	codec, err := protocol.NewCodec()
	if err != nil {
		return InboundPermit{}, errors.Join(ErrIntegrity, err)
	}
	wireBytes, err := codec.Encode(envelope)
	if err != nil {
		return InboundPermit{}, errors.Join(ErrIntegrity, err)
	}
	defer clearBytes(wireBytes)
	return InboundPermit{
		scope:      gate.scope,
		digest:     digest,
		integrity:  integrity,
		wire:       sha256.Sum256(wireBytes),
		provenance: provenance.digest(),
		sender:     provenance.sender,
		meshID:     gate.meshID,
	}, nil
}

// VerifyMaterialization validates canonical bytes against the authenticated
// original descriptor without rewriting that envelope or descriptor.
func (gate *Gate) VerifyMaterialization(permit InboundPermit, original protocol.Envelope, canonical []byte) (MaterializationResult, error) {
	if err := gate.validatePermit(permit, original); err != nil {
		return MaterializationResult{}, err
	}
	if int64(len(canonical)) != original.Payload.Size {
		return MaterializationResult{}, ErrMaterializationRejected
	}
	snapshot := append([]byte(nil), canonical...)
	defer clearBytes(snapshot)
	if sha256.Sum256(snapshot) != original.Payload.Digest {
		return MaterializationResult{}, ErrMaterializationRejected
	}
	applicationPath, err := gate.paths.ExtractApplicationPath(original.Payload.Profile, snapshot)
	if err != nil || validateApplicationPath(applicationPath) != nil {
		return MaterializationResult{}, ErrMaterializationRejected
	}
	return gate.verifyMaterializationAtPath(permit, original, snapshot, applicationPath)
}

// VerifyResponseMaterialization binds a response payload to the trusted local
// application path retained with its pending outbound request. Response
// payload formats such as http.response do not carry a request path and may
// not manufacture one.
func (gate *Gate) VerifyResponseMaterialization(permit InboundPermit, original protocol.Envelope, canonical []byte, trustedRequestPath string) (MaterializationResult, error) {
	if original.Mode != protocol.ModeResponse || validateApplicationPath(trustedRequestPath) != nil {
		return MaterializationResult{}, ErrMaterializationRejected
	}
	snapshot := append([]byte(nil), canonical...)
	defer clearBytes(snapshot)
	return gate.verifyMaterializationAtPath(permit, original, snapshot, trustedRequestPath)
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func (gate *Gate) verifyMaterializationAtPath(permit InboundPermit, original protocol.Envelope, snapshot []byte, applicationPath string) (MaterializationResult, error) {
	if err := gate.validatePermit(permit, original); err != nil {
		return MaterializationResult{}, err
	}
	if int64(len(snapshot)) != original.Payload.Size || sha256.Sum256(snapshot) != original.Payload.Digest || validateApplicationPath(applicationPath) != nil {
		return MaterializationResult{}, ErrMaterializationRejected
	}
	result := MaterializationResult{
		scope:       gate.scope,
		digest:      permit.digest,
		integrity:   permit.integrity,
		wire:        permit.wire,
		provenance:  permit.provenance,
		payloadHash: original.Payload.Digest,
		payloadSize: original.Payload.Size,
		path:        applicationPath,
	}
	result.seal = gate.sealMaterialization(result)
	return result, nil
}

// AuthorizeApplication performs the final, post-materialization membership
// recheck and authorizes only the path bound into the verified result.
func (gate *Gate) AuthorizeApplication(permit InboundPermit, original protocol.Envelope, materialization MaterializationResult) error {
	if err := gate.validatePermit(permit, original); err != nil {
		return err
	}
	if materialization.scope != gate.scope || materialization.digest != permit.digest || materialization.integrity != permit.integrity || materialization.wire != permit.wire || materialization.provenance != permit.provenance || materialization.payloadHash != original.Payload.Digest || materialization.payloadSize != original.Payload.Size || validateApplicationPath(materialization.path) != nil {
		return ErrMaterializationRejected
	}
	expectedSeal := gate.sealMaterialization(materialization)
	if !hmac.Equal(materialization.seal[:], expectedSeal[:]) {
		return ErrMaterializationRejected
	}
	if err := gate.memberships.RequireAll(gate.meshID, permit.sender, gate.localIdentity); err != nil {
		return ErrMembership
	}
	if !gate.rules.Allows(original.Sender, materialization.path) {
		return ErrApplicationDenied
	}
	return nil
}

func (gate *Gate) sealMaterialization(result MaterializationResult) [sha256.Size]byte {
	hash := hmac.New(sha256.New, gate.scope[:])
	hash.Write([]byte("aztm-policy-materialization-v1"))
	hash.Write(result.digest[:])
	hash.Write(result.integrity[:])
	hash.Write(result.wire[:])
	hash.Write(result.provenance[:])
	hash.Write(result.payloadHash[:])
	var numeric [8]byte
	binary.BigEndian.PutUint64(numeric[:], uint64(result.payloadSize))
	hash.Write(numeric[:])
	binary.BigEndian.PutUint64(numeric[:], uint64(len(result.path)))
	hash.Write(numeric[:])
	hash.Write([]byte(result.path))
	var seal [sha256.Size]byte
	copy(seal[:], hash.Sum(nil))
	return seal
}

func (gate *Gate) validatePermit(permit InboundPermit, original protocol.Envelope) error {
	if gate == nil || permit.scope != gate.scope || permit.meshID != gate.meshID || permit.sender != original.Sender || permit.provenance == [sha256.Size]byte{} {
		return ErrInvalidPermit
	}
	digest, err := protocol.LogicalMessageDigest(original)
	if err != nil || digest != permit.digest {
		return ErrInvalidPermit
	}
	integrity, err := protocol.EnvelopeIntegrityDigest(original)
	if err != nil || integrity != permit.integrity {
		return ErrInvalidPermit
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		return ErrInvalidPermit
	}
	wireBytes, err := codec.Encode(original)
	if err != nil || sha256.Sum256(wireBytes) != permit.wire {
		return ErrInvalidPermit
	}
	return nil
}

// EvaluateOutbound is optional fail-fast preflight, never inbound trust proof.
func (gate *Gate) EvaluateOutbound(envelope protocol.Envelope) error {
	if gate == nil {
		return ErrInvalidGate
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return err
	}
	if envelope.MeshID != gate.meshID || envelope.Sender != gate.localIdentity {
		return ErrMesh
	}
	if err := gate.memberships.RequireAll(gate.meshID, gate.localIdentity, envelope.Recipient); err != nil {
		return ErrMembership
	}
	return nil
}
