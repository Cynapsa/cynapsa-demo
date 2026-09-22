package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type valueErrorDataChannel struct {
	secret []byte
	once   sync.Once
	done   chan struct{}
}

func (*valueErrorDataChannel) MaximumFrameBytes() int { return 1024 }
func (*valueErrorDataChannel) Send(context.Context, Frame) error {
	return nil
}
func (channel *valueErrorDataChannel) Receive(ctx context.Context) (Frame, error) {
	returned := false
	channel.once.Do(func() { returned = true })
	if returned {
		close(channel.done)
		return Frame{Kind: FrameEnvelope, Data: channel.secret}, errors.New("dependency value and error")
	}
	<-ctx.Done()
	return Frame{}, ctx.Err()
}
func (*valueErrorDataChannel) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (*valueErrorDataChannel) Close(context.Context) error { return nil }

type valueErrorPeerConnection struct{ channel DataChannel }

func (connection *valueErrorPeerConnection) OpenDataChannel(context.Context, DataChannelConfig) (DataChannel, error) {
	return connection.channel, nil
}
func (*valueErrorPeerConnection) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("value-error-binding")), true
}
func (*valueErrorPeerConnection) Close(context.Context) error { return nil }

func TestLinkClearsFrameReturnedWithDependencyError(t *testing.T) {
	secret := []byte("frame-value-with-error")
	channel := &valueErrorDataChannel{secret: secret, done: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: rank1TestClock}, &valueErrorPeerConnection{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-channel.done
	if err = link.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, value := range secret {
		if value != 0 {
			t.Fatal("frame returned alongside dependency error was not cleared")
		}
	}
}
