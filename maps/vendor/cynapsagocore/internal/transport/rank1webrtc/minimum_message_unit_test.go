package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/pion/webrtc/v4"
)

func minimumJingleConfig(maximum int) JingleNegotiatorConfig {
	return JingleNegotiatorConfig{
		LocalIdentity: "a/m", PeerIdentity: "b/m", MeshID: "m",
		SID: "sid", SCTPStreams: 8,
		MaximumMessageBytes: maximum,
	}
}

func minimumLinkConfig(maximum int) Config {
	return Config{
		MeshID: "m", LocalIdentity: "a/m", PeerID: "b/m", MaximumFrameBytes: maximum, ReceiveCapacity: 2, TransferWorkers: 1,
		TransferQueue: 2, Clock: rank1TestClock,
	}
}

func minimumEnvelope(t *testing.T) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("p", nil)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		Version:        protocol.Version2,
		MessageID:      "msg_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16)),
		ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		Sender:         "a/m", Recipient: "b/m", MeshID: "m", Mode: protocol.ModeMessage, CreatedAt: time.Unix(0, 0).UTC(),
		ClockUncertainty: time.Microsecond, Payload: payload,
	}
}

func TestRank1LocalMaximumMessageBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		maximum int
		valid   bool
	}{
		{name: "minimum minus one", maximum: transport.MinimumRank1MessageBytes - 1},
		{name: "minimum", maximum: transport.MinimumRank1MessageBytes, valid: true},
		{name: "minimum plus one", maximum: transport.MinimumRank1MessageBytes + 1, valid: true},
		{name: "maximum", maximum: transport.MaximumControlFrameBytes, valid: true},
		{name: "maximum plus one", maximum: transport.MaximumControlFrameBytes + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, jingleErr := NewJingleNegotiator(minimumJingleConfig(test.maximum), fakeExchange{})
			channelValid := (DataChannelConfig{Label: "aztm", MaximumFrameBytes: test.maximum}).valid()
			channel := &fakeChannel{maximum: test.maximum, inbound: make(chan Frame, 2), state: transport.HealthHealthy, closed: make(chan struct{})}
			link, linkErr := NewLink(minimumLinkConfig(test.maximum), &fakePC{channel: channel}, nil, nil)
			if test.valid {
				if jingleErr != nil || !channelValid || linkErr != nil {
					t.Fatalf("valid boundary rejected: jingle=%v data-channel=%t link=%v", jingleErr, channelValid, linkErr)
				}
				_ = link.Close(context.Background())
				return
			}
			if !errors.Is(jingleErr, transport.ErrInvalidConfig) || channelValid || !errors.Is(linkErr, transport.ErrInvalidConfig) {
				t.Fatalf("invalid local boundary classifications: jingle=%v data-channel=%t link=%v", jingleErr, channelValid, linkErr)
			}
		})
	}
}

func TestHandshakeAssemblyMaximumMessageBoundaries(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(2, durable)
	if err != nil {
		t.Fatal(err)
	}
	base := HandshakeAssemblyConfig{
		MeshID: "m", LocalIdentity: "a/m", Manager: manager,
		Authority: staticRank1Authority(1), Exchange: fakeExchange{},
		ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2,
		SCTPStreams: 8, Clock: rank1TestClock,
	}
	for _, test := range []struct {
		maximum int
		valid   bool
	}{
		{maximum: transport.MinimumRank1MessageBytes - 1},
		{maximum: transport.MinimumRank1MessageBytes, valid: true},
		{maximum: transport.MaximumControlFrameBytes, valid: true},
		{maximum: transport.MaximumControlFrameBytes + 1},
	} {
		config := base
		config.MaximumMessageBytes = test.maximum
		_, err = NewHandshakeNegotiator(config)
		if test.valid && err != nil {
			t.Fatalf("maximum %d rejected: %v", test.maximum, err)
		}
		if !test.valid && !errors.Is(err, transport.ErrInvalidConfig) {
			t.Fatalf("maximum %d error = %v", test.maximum, err)
		}
	}
}

func TestJingleEffectiveMaximumUsesEachSmallestSource(t *testing.T) {
	for _, test := range []struct {
		name   string
		local  int
		remote uint32
		want   int
	}{
		{name: "local smallest", local: transport.MinimumRank1MessageBytes, remote: transport.MinimumRank1MessageBytes + 1, want: transport.MinimumRank1MessageBytes},
		{name: "remote smallest", local: transport.MinimumRank1MessageBytes + 1, remote: transport.MinimumRank1MessageBytes, want: transport.MinimumRank1MessageBytes},
		{name: "upper bound", local: transport.MaximumControlFrameBytes, remote: transport.MaximumControlFrameBytes, want: transport.MaximumControlFrameBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			negotiator, err := NewJingleNegotiator(minimumJingleConfig(test.local), fakeExchange{})
			if err != nil {
				t.Fatal(err)
			}
			if err = negotiator.setRemoteMaximum(test.remote); err != nil {
				t.Fatal(err)
			}
			if got := negotiator.EffectiveMaximumFrameBytes(); got != test.want {
				t.Fatalf("effective maximum = %d, want %d", got, test.want)
			}
		})
	}
}

func TestJinglePeerMaximumBelowMinimumIsProtocolError(t *testing.T) {
	negotiator, err := NewJingleNegotiator(minimumJingleConfig(transport.MaximumControlFrameBytes), fakeExchange{})
	if err != nil {
		t.Fatal(err)
	}
	for _, maximum := range []uint32{0, transport.MinimumRank1MessageBytes - 1, transport.MaximumControlFrameBytes + 1} {
		if err = negotiator.setRemoteMaximum(maximum); !errors.Is(err, transport.ErrProtocol) {
			t.Fatalf("peer maximum %d error = %v", maximum, err)
		}
	}
}

func TestLinkExactMinimumCarriesEnvelopeAndMandatoryHealth(t *testing.T) {
	channel := &fakeChannel{
		maximum: transport.MinimumRank1MessageBytes, inbound: make(chan Frame, 2),
		sentNote: make(chan Frame, 4), state: transport.HealthHealthy, closed: make(chan struct{}),
	}
	link, err := NewLink(minimumLinkConfig(transport.MinimumRank1MessageBytes), &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	if err = link.Send(context.Background(), minimumEnvelope(t)); err != nil {
		t.Fatalf("exact-minimum envelope send: %v", err)
	}
	select {
	case frame := <-channel.sentNote:
		if frame.Kind != FrameEnvelope {
			t.Fatalf("first frame kind = %v", frame.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("exact-minimum envelope was not sent")
	}
	nonce := make([]byte, 16)
	channel.inbound <- Frame{Kind: FrameHealthProbe, ProbeNonce: nonce}
	select {
	case frame := <-channel.sentNote:
		if frame.Kind != FrameHealthReply || string(frame.ProbeNonce) != string(nonce) {
			t.Fatalf("health reply = %#v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("exact-minimum mandatory health reply was not sent")
	}
	if state := link.Observe().State; state != transport.HealthHealthy {
		t.Fatalf("health state = %v", state)
	}
}

func TestLinkRejectsNegotiatedDataChannelBelowMinimumBeforeStart(t *testing.T) {
	channel := &fakeChannel{maximum: transport.MinimumRank1MessageBytes - 1, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(minimumLinkConfig(transport.MaximumControlFrameBytes), &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = link.Start(context.Background()); !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("undersized negotiated channel error = %v", err)
	}
	if link.started {
		t.Fatal("undersized negotiated channel started the link")
	}
}

type boundaryPionNegotiator struct {
	err     error
	maximum int
}

func (n boundaryPionNegotiator) Negotiate(context.Context, *webrtc.PeerConnection, bool) error {
	return n.err
}

func (n boundaryPionNegotiator) EffectiveMaximumFrameBytes() int { return n.maximum }

func (boundaryPionNegotiator) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("minimum-message-test")), true
}

func TestPionPreservesPeerAndNegotiatedProtocolClassification(t *testing.T) {
	for _, test := range []struct {
		name       string
		negotiator boundaryPionNegotiator
	}{
		{name: "wrapped peer protocol", negotiator: boundaryPionNegotiator{err: fmt.Errorf("peer maximum: %w", transport.ErrProtocol), maximum: transport.MinimumRank1MessageBytes}},
		{name: "undersized effective maximum", negotiator: boundaryPionNegotiator{maximum: transport.MinimumRank1MessageBytes - 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, err := NewPionPeerConnection(PionConfig{Initiator: true, ReceiveCapacity: 1, Clock: rank1TestClock}, test.negotiator)
			if err != nil {
				t.Fatal(err)
			}
			channel, err := connection.OpenDataChannel(context.Background(), DataChannelConfig{Label: "aztm", MaximumFrameBytes: transport.MinimumRank1MessageBytes})
			if channel != nil || !errors.Is(err, transport.ErrProtocol) {
				t.Fatalf("channel=%T error=%v", channel, err)
			}
		})
	}
}

func TestHandshakeDoesNotInstallUndersizedNegotiatedLink(t *testing.T) {
	durable := &assemblyDurable{done: make(chan struct{})}
	manager, err := transport.NewManager(2, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	assembly, err := NewHandshakeNegotiator(HandshakeAssemblyConfig{
		MeshID: "m", LocalIdentity: "a/m", Manager: manager,
		Authority: staticRank1Authority(1), Exchange: fakeExchange{},
		MaximumMessageBytes: transport.MaximumControlFrameBytes,
		ReceiveCapacity:     2, TransferWorkers: 1, TransferQueue: 2,
		SCTPStreams: 8, Clock: rank1TestClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	assembly.newPeer = func(PionConfig, PionNegotiator) (PeerConnection, error) {
		channel := &fakeChannel{maximum: transport.MinimumRank1MessageBytes - 1, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
		return &fakePC{channel: channel}, nil
	}
	attempt := handshake.Attempt{
		ID: "hsk_0123456789012345678901", PeerID: "b/m", InitiatorID: "a/m",
		StartedAt: rank1TestClock.Now(), Deadline: rank1TestClock.Now().Add(time.Second),
		State: handshake.AttemptRunning,
	}
	if err = assembly.Establish(context.Background(), attempt); !errors.Is(err, handshake.ErrFailed) {
		t.Fatalf("undersized handshake error = %v", err)
	}
	if state := manager.ObservePeer(transport.KindLive, "b/m").State; state != transport.HealthUnknown {
		t.Fatalf("undersized live link was installed: %v", state)
	}
}
