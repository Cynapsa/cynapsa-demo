package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/pion/webrtc/v4"
)

var qaLiveClock = transport.ClockFunc(func() time.Time {
	return time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
})

func qaLiveRoute() payload.CarrierRoute {
	return payload.CarrierRoute{
		PeerID:      "b",
		MeshID:      "mesh",
		SenderID:    "a",
		RecipientID: "b",
		MessageID:   "msg_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16)),
	}
}

func qaLiveLink(t *testing.T, maximumTransfers int) (*Link, *fakeChannel) {
	t.Helper()
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 32), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{
		MeshID:            "mesh",
		LocalIdentity:     "a",
		PeerID:            "b",
		MaximumFrameBytes: 1024,
		ReceiveCapacity:   8,
		TransferWorkers:   1,
		TransferQueue:     8,
		Clock:             qaLiveClock,
	}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	return link, channel
}

func TestQADirectFinishCancellationReleasesTransferCapacity(t *testing.T) {
	link, _ := qaLiveLink(t, 1)
	carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 256, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	route := qaLiveRoute()
	first := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: first, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := carrier.Finish(ctx, route, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finish cancellation error = %v", err)
	}
	second := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("fedcba9876543210"))
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: second, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
		t.Fatalf("cancelled transfer retained capacity: %v", err)
	}
	_ = carrier.Abort(context.Background(), route, second)
}

func TestQADirectAbortUnblocksConcurrentFinish(t *testing.T) {
	link, channel := qaLiveLink(t, 1)
	carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 256, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	route := qaLiveRoute()
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("abortdirecttest!"))
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
		t.Fatal(err)
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finishResult := make(chan error, 1)
	go func() {
		_, finishErr := carrier.Finish(finishCtx, route, transferID)
		finishResult <- finishErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		channel.mu.Lock()
		sawFinish := false
		for _, frame := range channel.sent {
			if frame.Kind == FrameTransferFinish && frame.TransferID == transferID {
				sawFinish = true
				break
			}
		}
		channel.mu.Unlock()
		if sawFinish {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Finish frame was not sent")
		}
		time.Sleep(time.Millisecond)
	}
	if err := carrier.Abort(context.Background(), route, transferID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finishResult:
	case <-time.After(50 * time.Millisecond):
		cancel()
		<-finishResult
		t.Fatal("Abort did not unblock concurrent Finish")
	}
}

func TestQAConcurrentSendAndCloseReturnWithoutPanic(t *testing.T) {
	link, _ := qaLiveLink(t, 1)
	envelope := liveEnvelope(t, "a", "b")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = link.Send(ctx, envelope)
		}()
	}
	close(start)
	_ = link.Close(ctx)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent sends did not return after close")
	}
}

func TestQAPionCandidateParserAcceptsCandidateScopedUfrag(t *testing.T) {
	// Pion emits candidate-scoped ufrag in current ICE gathering output. It is
	// signaling metadata, not an unknown attacker-controlled extension.
	_, err := parseCandidate("1 1 udp 2130706431 192.0.2.10 5000 typ host ufrag peerUfrag generation 0", "peerUfrag")
	if err != nil {
		t.Fatalf("Pion-compatible candidate rejected: %v", err)
	}
}

type qaBlockingPeerConnection struct {
	entered chan struct{}
	release chan struct{}
	channel DataChannel
	once    sync.Once
}

func (p *qaBlockingPeerConnection) OpenDataChannel(ctx context.Context, _ DataChannelConfig) (DataChannel, error) {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
		return p.channel, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (*qaBlockingPeerConnection) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("rank1-qa-binding")), true
}
func (p *qaBlockingPeerConnection) Close(context.Context) error { return nil }

func TestQALinkCloseDeadlineIsHonoredDuringInFlightStart(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame), state: transport.HealthConnecting, closed: make(chan struct{})}
	connection := &qaBlockingPeerConnection{entered: make(chan struct{}), release: make(chan struct{}), channel: channel}
	link, err := NewLink(Config{
		MeshID:            "mesh",
		LocalIdentity:     "a",
		PeerID:            "b",
		MaximumFrameBytes: 1024,
		ReceiveCapacity:   1,
		TransferWorkers:   1,
		TransferQueue:     1,
		Clock:             qaLiveClock,
	}, connection, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- link.Start(context.Background()) }()
	<-connection.entered

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() { closeResult <- link.Close(closeCtx) }()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) && err != nil {
			t.Fatalf("close error = %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		close(connection.release)
		<-startResult
		<-closeResult
		t.Fatal("Close blocked on Start dependency beyond its deadline")
	}
	close(connection.release)
	<-startResult
	_ = link.Close(context.Background())
}

type qaRemoteProfileNegotiator struct {
	remote *webrtc.PeerConnection
}

func (n *qaRemoteProfileNegotiator) EffectiveMaximumFrameBytes() int { return 1024 }
func (*qaRemoteProfileNegotiator) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("rank1-qa-binding")), true
}
func (n *qaRemoteProfileNegotiator) Negotiate(ctx context.Context, responder *webrtc.PeerConnection, initiator bool) error {
	if initiator {
		return errors.New("QA negotiator requires responder path")
	}
	remote, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	n.remote = remote
	ordered := true
	if _, err := remote.CreateDataChannel("aztm", &webrtc.DataChannelInit{Ordered: &ordered}); err != nil {
		return err
	}
	offer, err := remote.CreateOffer(nil)
	if err != nil {
		return err
	}
	remoteGathered := webrtc.GatheringCompletePromise(remote)
	if err := remote.SetLocalDescription(offer); err != nil {
		return err
	}
	select {
	case <-remoteGathered:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := responder.SetRemoteDescription(*remote.LocalDescription()); err != nil {
		return err
	}
	answer, err := responder.CreateAnswer(nil)
	if err != nil {
		return err
	}
	responderGathered := webrtc.GatheringCompletePromise(responder)
	if err := responder.SetLocalDescription(answer); err != nil {
		return err
	}
	select {
	case <-responderGathered:
	case <-ctx.Done():
		return ctx.Err()
	}
	return remote.SetRemoteDescription(*responder.LocalDescription())
}

func TestQAPionResponderRejectsRemoteOrderedDataChannel(t *testing.T) {
	negotiator := &qaRemoteProfileNegotiator{}
	connection, err := NewPionPeerConnection(PionConfig{Initiator: false, ReceiveCapacity: 4, Clock: qaLiveClock}, negotiator)
	if err != nil {
		t.Fatal(err)
	}
	// Live ICE/DTLS/SCTP setup is intentionally exercised here. Race
	// instrumentation and package-level parallelism can push that setup beyond
	// five seconds without changing the protocol rejection under test.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	channel, err := connection.OpenDataChannel(ctx, DataChannelConfig{Label: "aztm", Ordered: false, MaximumFrameBytes: 1024})
	if channel != nil {
		_ = channel.Close(context.Background())
	}
	if negotiator.remote != nil {
		_ = negotiator.remote.Close()
	}
	if !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("ordered remote channel accepted: channel=%T err=%v", channel, err)
	}
}
