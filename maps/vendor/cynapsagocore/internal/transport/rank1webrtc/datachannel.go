package rank1webrtc

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type Frame = transport.ControlFrame
type FrameKind = transport.FrameKind

const (
	FrameEnvelope           = transport.FrameEnvelope
	FrameTransferManifest   = transport.FrameTransferManifest
	FrameTransferChunk      = transport.FrameTransferChunk
	FrameTransferFinish     = transport.FrameTransferFinish
	FrameTransferCompletion = transport.FrameTransferCompletion
	FrameTransferAbort      = transport.FrameTransferAbort
	FrameHealthProbe        = transport.FrameHealthProbe
	FrameHealthReply        = transport.FrameHealthReply
	FrameEnvelopeReceipt    = transport.FrameEnvelopeReceipt
)

func createDataChannel(ctx context.Context, connection PeerConnection, maximumFrameBytes int) (DataChannel, error) {
	if ctx == nil || connection == nil {
		return nil, transport.ErrInvalidConfig
	}
	config := DataChannelConfig{Label: "aztm", Ordered: false, MaximumFrameBytes: maximumFrameBytes}
	if !config.valid() {
		return nil, transport.ErrInvalidConfig
	}
	return openChannel(connection, ctx, config)
}

func openChannel(connection PeerConnection, ctx context.Context, config DataChannelConfig) (channel DataChannel, err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrUnavailable
		}
	}()
	return connection.OpenDataChannel(ctx, config)
}
func receiveChannel(channel DataChannel, ctx context.Context) (frame Frame, err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrReceive
		}
	}()
	return channel.Receive(ctx)
}

type inboundFrame struct {
	frame     Frame
	lease     *transport.InboundLease
	validated bool
}

type leasedFrameReceiver interface {
	receiveLeased(context.Context) (inboundFrame, error)
}

func receiveChannelOwned(channel DataChannel, ctx context.Context) (owned inboundFrame, err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrReceive
		}
	}()
	if leased, ok := channel.(leasedFrameReceiver); ok {
		return leased.receiveLeased(ctx)
	}
	owned.frame, err = channel.Receive(ctx)
	return owned, err
}

func clearInboundFrame(owned *inboundFrame) {
	if owned == nil {
		return
	}
	clearOwnedFrame(&owned.frame)
	if owned.lease != nil {
		owned.lease.Release()
	}
	*owned = inboundFrame{}
}
func sendChannel(channel DataChannel, ctx context.Context, frame Frame) (err error) {
	// DataChannel.Send may inspect this dependency-owned snapshot only for the
	// duration of the call. Implementations that retain or process it later
	// must clone it before returning.
	owned := frame.Clone()
	defer clearOwnedFrame(&owned)
	defer func() {
		if recover() != nil {
			err = transport.ErrSendAmbiguous
		}
	}()
	return channel.Send(ctx, owned)
}
func observeChannel(channel DataChannel) (observation transport.Observation) {
	observation = transport.Observation{State: transport.HealthFailed}
	defer func() { _ = recover() }()
	return channel.Observe()
}
func channelMaximum(channel DataChannel) (maximum int) {
	defer func() {
		if recover() != nil {
			maximum = 0
		}
	}()
	return channel.MaximumFrameBytes()
}
func closeChannel(channel DataChannel, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrClosed
		}
	}()
	return channel.Close(ctx)
}
func closeConnection(connection PeerConnection, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = transport.ErrClosed
		}
	}()
	return connection.Close(ctx)
}
