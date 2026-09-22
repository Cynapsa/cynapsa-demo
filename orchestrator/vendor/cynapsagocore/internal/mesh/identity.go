// Package mesh owns private identity, credential, membership, and directory state.
package mesh

import (
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type Identity struct {
	AgentID  string
	Internal string
}

func NewIdentity(agentID, internal string) (Identity, error) {
	if protocol.ValidateAgentIdentity(agentID) != nil || protocol.ValidateAgentIdentity(internal) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	return Identity{AgentID: strings.Clone(agentID), Internal: strings.Clone(internal)}, nil
}

// BoundIdentityVerifier is implemented by connectivity. It authenticates a
// bare account, binds either a server-issued installation resource or the
// transitional legacy mesh resource, and verifies the returned full identity.
type BoundIdentityVerifier interface {
	VerifyBoundIdentity(authenticatedBare, meshID, returnedFull string) error
}

type SessionIdentity struct {
	agentID           string
	meshID            string
	authenticatedBare string
	boundFull         string
}

func NewSessionIdentity(meshID, authenticatedBare, returnedFull string, verifier BoundIdentityVerifier) (SessionIdentity, error) {
	if verifier == nil || protocol.ValidateMeshID(meshID) != nil || protocol.ValidateAgentIdentity(authenticatedBare) != nil || protocol.ValidateAgentIdentity(returnedFull) != nil {
		return SessionIdentity{}, ErrInvalidIdentity
	}
	if err := verifier.VerifyBoundIdentity(authenticatedBare, meshID, returnedFull); err != nil {
		return SessionIdentity{}, ErrIdentityBinding
	}
	// The verified full identity is the private session identity used by
	// envelopes and conversation derivation. No independent caller-supplied
	// alias can bypass the authenticated resource binding.
	return SessionIdentity{
		agentID: strings.Clone(authenticatedBare), meshID: strings.Clone(meshID),
		authenticatedBare: strings.Clone(authenticatedBare), boundFull: strings.Clone(returnedFull),
	}, nil
}

func (identity SessionIdentity) AgentID() string           { return identity.agentID }
func (identity SessionIdentity) MeshID() string            { return identity.meshID }
func (identity SessionIdentity) AuthenticatedBare() string { return identity.authenticatedBare }
func (identity SessionIdentity) BoundFull() string         { return identity.boundFull }
