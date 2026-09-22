package rank2xmpp

import (
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// AuthenticatedIdentity is the immutable identity committed by the current
// authenticated durable session. It intentionally excludes authentication
// proof, password, session, and connection state.
type AuthenticatedIdentity struct {
	BareIdentity  string
	BoundIdentity string
	MeshID        string
}

// VerifyBoundIdentity enforces the approved standard sequence's result:
// authenticate the exact bare account, bind the server-issued resource (or
// legacy resource=mesh_id), and require the
// server to return that exact full identity.
func (c *Client) VerifyBoundIdentity(authenticatedBare, meshID, returnedFull string) error {
	resource := meshID
	if c != nil { resource = c.boundResource() }
	if c == nil || protocol.ValidateAgentIdentity(authenticatedBare) != nil || protocol.ValidateMeshID(meshID) != nil || protocol.ValidateAgentIdentity(returnedFull) != nil || strings.Contains(authenticatedBare, "/") || strings.Contains(meshID, "/") || authenticatedBare != c.config.Auth.Username || meshID != c.config.Auth.MeshID || returnedFull != authenticatedBare+"/"+resource {
		return ErrIdentityBinding
	}
	return nil
}

func (c *Client) boundResource() string {
	if c != nil && c.config.Auth.SessionResource != "" { return c.config.Auth.SessionResource }
	if c == nil { return "" }
	return c.config.Auth.MeshID
}

// AuthenticatedIdentity returns a fresh value describing only the identity
// committed by a successful Start or reconnect. Configuration alone is not
// authentication, so the identity is unavailable before Start completes and
// after Close begins.
func (c *Client) AuthenticatedIdentity() (AuthenticatedIdentity, error) {
	if c == nil {
		return AuthenticatedIdentity{}, ErrUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.closed || c.session == nil || c.VerifyBoundIdentity(c.identity.BareIdentity, c.config.Auth.MeshID, c.identity.BoundIdentity) != nil {
		return AuthenticatedIdentity{}, ErrUnavailable
	}
	return AuthenticatedIdentity{
		BareIdentity:  c.identity.BareIdentity,
		BoundIdentity: c.identity.BoundIdentity,
		MeshID:        c.config.Auth.MeshID,
	}, nil
}
