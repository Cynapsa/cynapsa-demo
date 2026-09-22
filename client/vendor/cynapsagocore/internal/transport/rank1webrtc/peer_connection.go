package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

// DataChannelConfig makes reliability explicit. Reliable requires both retry
// limits unset; Ordered is deliberately false because application delivery is
// independent and duplicate safety comes from stable message identity.
type DataChannelConfig struct {
	Label              string
	Ordered            bool
	MaximumRetransmits *uint16
	MaximumLifetimeMS  *uint16
	MaximumFrameBytes  int
}

func (c DataChannelConfig) valid() bool {
	return c.Label == "aztm" && !c.Ordered && c.MaximumRetransmits == nil && c.MaximumLifetimeMS == nil && c.MaximumFrameBytes >= transport.MinimumRank1MessageBytes && c.MaximumFrameBytes <= transport.MaximumControlFrameBytes
}

// PeerConnection is the small injected seam for an approved WebRTC stack.
type PeerConnection interface {
	OpenDataChannel(context.Context, DataChannelConfig) (DataChannel, error)
	ChannelBinding() ([sha256.Size]byte, bool)
	Close(context.Context) error
}

// RestartablePeerConnection is an optional private capability implemented by
// connections that can refresh their network path without replacing the
// authenticated data channel.
type RestartablePeerConnection interface {
	PeerConnection
	Restart(context.Context) error
	AcceptRestart(context.Context, rank2xmpp.Jingle) error
}

// ICEServer is private Core-owned ICE input. TURN credentials stay in
// clearable storage until the final Pion call boundary.
type ICEServer struct {
	URLs       []string
	Username   string
	Credential []byte
}

// ICEConfiguration is a private, ownership-safe snapshot obtained from the
// authenticated server authority. ExpiresAt is the earliest credential
// expiry; a zero value is valid only when no credential is time-limited.
type ICEConfiguration struct {
	Servers   []ICEServer
	Policy    webrtc.ICETransportPolicy
	ExpiresAt time.Time
}

// AuthenticatedICEConfigurationSource resolves connectivity data through the
// exact authenticated Rank2 session.
type AuthenticatedICEConfigurationSource interface {
	ResolveICEConfiguration(context.Context) (ICEConfiguration, error)
}

// ReconfigurableRestartablePeerConnection atomically installs freshly
// authenticated ICE servers before beginning an in-place Jingle restart.
type ReconfigurableRestartablePeerConnection interface {
	RestartablePeerConnection
	RestartWithConfiguration(context.Context, ICEConfiguration) error
	AcceptRestartWithConfiguration(context.Context, rank2xmpp.Jingle, ICEConfiguration) error
}

type DataChannel interface {
	MaximumFrameBytes() int
	// Send borrows frame only until it returns. An implementation must clone
	// the frame before retaining it or handing it to asynchronous work.
	Send(context.Context, Frame) error
	Receive(context.Context) (Frame, error)
	Observe() transport.Observation
	Close(context.Context) error
}
