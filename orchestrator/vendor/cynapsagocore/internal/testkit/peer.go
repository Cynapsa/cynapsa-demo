// Package testkit provides private deterministic fixtures unavailable in production APIs.
package testkit

import "github.com/Cynapsa/cynapsagocore/internal/protocol"

// Peer describes one test-only opaque agent fixture.
type Peer struct {
	AgentID string
	MeshID  string
}

// NewPeer will construct a deterministic test peer.
func NewPeer(agentID string, meshID string) Peer {
	if err := protocol.ValidateAgentIdentity(agentID); err != nil {
		panic(err)
	}
	if err := protocol.ValidateMeshID(meshID); err != nil {
		panic(err)
	}
	return Peer{AgentID: agentID, MeshID: meshID}
}
