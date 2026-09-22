package policy

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// CarrierAuthentication is the closed set of V1 carrier trust boundaries.
// It is private protocol state and must never reach an SDK result or event.
type CarrierAuthentication uint8

const (
	CarrierGroupRank2 CarrierAuthentication = iota + 1
	CarrierBoundRank1
)

// AuthenticatedProvenance is immutable evidence supplied by a trusted carrier
// adapter after it has authenticated the exact route. It is intentionally not
// serialized in the logical envelope and is not a reusable credential.
type AuthenticatedProvenance struct {
	carrier        CarrierAuthentication
	sender         string
	recipient      string
	meshID         string
	channelBinding [sha256.Size]byte
}

// NewGroupRank2Provenance records authentication by the outer durable stanza.
// The trusted adapter must call it only after server authentication and exact
// recipient validation; this constructor validates shape, not server truth.
func NewGroupRank2Provenance(sender, recipient, meshID string) (AuthenticatedProvenance, error) {
	provenance := AuthenticatedProvenance{
		carrier: CarrierGroupRank2, sender: sender, recipient: recipient, meshID: meshID,
	}
	if provenance.validateShape() != nil {
		return AuthenticatedProvenance{}, ErrProvenance
	}
	return provenance, nil
}

// NewBoundRank1Provenance records one authenticated direct session. The group
// binding is an output of the Rank 2-authenticated key schedule.
func NewBoundRank1Provenance(sender, recipient, meshID string, channelBinding [sha256.Size]byte) (AuthenticatedProvenance, error) {
	provenance := AuthenticatedProvenance{
		carrier: CarrierBoundRank1, sender: sender, recipient: recipient, meshID: meshID,
		channelBinding: channelBinding,
	}
	if provenance.validateShape() != nil {
		return AuthenticatedProvenance{}, ErrProvenance
	}
	return provenance, nil
}

func (provenance AuthenticatedProvenance) Sender() string    { return provenance.sender }
func (provenance AuthenticatedProvenance) Recipient() string { return provenance.recipient }
func (provenance AuthenticatedProvenance) MeshID() string    { return provenance.meshID }

func (provenance AuthenticatedProvenance) validateFor(sender, recipient, meshID string) error {
	if provenance.validateShape() != nil || provenance.sender != sender || provenance.recipient != recipient || provenance.meshID != meshID {
		return ErrProvenance
	}
	return nil
}

func (provenance AuthenticatedProvenance) validateShape() error {
	if protocol.ValidateMeshID(provenance.meshID) != nil || strings.Contains(provenance.meshID, "/") || !validEndpointIdentity(provenance.sender) || !validEndpointIdentity(provenance.recipient) {
		return ErrProvenance
	}
	zeroBinding := provenance.channelBinding == [sha256.Size]byte{}
	switch provenance.carrier {
	case CarrierGroupRank2:
		if !zeroBinding {
			return ErrProvenance
		}
	case CarrierBoundRank1:
		if zeroBinding {
			return ErrProvenance
		}
	default:
		return ErrProvenance
	}
	return nil
}

// validEndpointIdentity validates only the full-identity shape. The opaque
// resource is a session address and never evidence of mesh membership. Mesh
// authority comes from the authenticated carrier provenance and the current
// server-owned topology checked by Gate.
func validEndpointIdentity(full string) bool {
	if protocol.ValidateAgentIdentity(full) != nil {
		return false
	}
	slash := strings.LastIndexByte(full, '/')
	if slash <= 0 || slash == len(full)-1 {
		return false
	}
	bare := full[:slash]
	return !strings.Contains(bare, "/") && protocol.ValidateAgentIdentity(bare) == nil
}

func (provenance AuthenticatedProvenance) digest() [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("CYNAPSA-CARRIER-PROVENANCE-V1\x00"))
	hash.Write([]byte{byte(provenance.carrier)})
	writeProvenanceText(hash, provenance.sender)
	writeProvenanceText(hash, provenance.recipient)
	writeProvenanceText(hash, provenance.meshID)
	hash.Write(provenance.channelBinding[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

type provenanceHash interface{ Write([]byte) (int, error) }

func writeProvenanceText(hash provenanceHash, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(value))
}
