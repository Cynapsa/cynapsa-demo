package cynapsagocore

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

// rank2PeerResolver is the private composition seam between one authenticated
// server handshake and MessagingService's peer-session admission. A result is
// never inferred from the requested name or retained across a reconnect.
type rank2PeerResolver struct{ client *rank2xmpp.Client }

func (resolver rank2PeerResolver) ResolvePeer(ctx context.Context, bare string) (mesh.ResolvedPeer, mesh.TopologyDisposition) {
	if resolver.client == nil || ctx == nil {
		return mesh.ResolvedPeer{}, mesh.TopologyUnavailable
	}
	peer, err := resolver.client.ResolveAuthorizedPeer(ctx, bare)
	return resolvedPeerResult(bare, peer, err)
}

func (resolver rank2PeerResolver) ResolveExactPeer(ctx context.Context, full string) (mesh.ResolvedPeer, mesh.TopologyDisposition) {
	if resolver.client == nil || ctx == nil {
		return mesh.ResolvedPeer{}, mesh.TopologyUnavailable
	}
	peer, err := resolver.client.ResolveAuthorizedExactPeer(ctx, full)
	return resolvedPeerResult(full, peer, err)
}

func resolvedPeerResult(requested string, peer rank2xmpp.AuthorizedPeer, err error) (mesh.ResolvedPeer, mesh.TopologyDisposition) {
	if err != nil {
		if errors.Is(err, rank2xmpp.ErrAuthentication) {
			return mesh.ResolvedPeer{}, mesh.TopologyRejected
		}
		return mesh.ResolvedPeer{}, mesh.TopologyUnavailable
	}
	bare := requested
	for index := range requested {
		if requested[index] == '/' {
			bare = requested[:index]
			break
		}
	}
	return mesh.ResolvedPeer{
		Bare: bare, Full: peer.FullJID,
		InstallationID:    peer.InstallationID,
		SessionGeneration: peer.SessionGeneration,
	}, mesh.TopologyReady
}
