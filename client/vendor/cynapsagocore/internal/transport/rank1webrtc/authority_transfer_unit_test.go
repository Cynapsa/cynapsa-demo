package rank1webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type authorityRefreshPC struct {
	channel *fakeChannel
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type preStartAuthorityLink struct {
	*Link
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (link *preStartAuthorityLink) Start(ctx context.Context) error {
	link.once.Do(func() { close(link.entered) })
	select {
	case <-link.release:
		return link.Link.Start(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (connection *authorityRefreshPC) OpenDataChannel(ctx context.Context, _ DataChannelConfig) (DataChannel, error) {
	connection.once.Do(func() { close(connection.entered) })
	select {
	case <-connection.release:
		return connection.channel, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*authorityRefreshPC) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("authority-refresh-binding")), true
}

func (*authorityRefreshPC) Close(context.Context) error { return nil }

func TestPendingLinkRebindsAcrossAuthorityRefreshBeforeStart(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 2), state: transport.HealthHealthy, closed: make(chan struct{})}
	base, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	link := &preStartAuthorityLink{Link: base, entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), "b", link) }()
	select {
	case <-link.entered:
	case <-time.After(time.Second):
		t.Fatal("pending installation did not reach the pre-Start handoff")
	}
	epoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{"b"}); err != nil {
		t.Fatalf("reconcile pre-Start pending link: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish pre-Start pending authority: %v", err)
	}
	close(link.release)
	select {
	case err = <-installed:
		if err != nil {
			t.Fatalf("install pre-Start pending link after refresh: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-Start pending link did not publish")
	}
	if got := manager.ObservePeer(transport.KindLive, "b").State; got != transport.HealthConnecting {
		t.Fatalf("pre-Start pending link state=%v", got)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartingLinkRebindsAcrossAuthorityRefresh(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 2), state: transport.HealthHealthy, closed: make(chan struct{})}
	connection := &authorityRefreshPC{channel: channel, entered: make(chan struct{}), release: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 2, Clock: rank1TestClock}, connection, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), "b", link) }()
	select {
	case <-connection.entered:
	case <-time.After(time.Second):
		t.Fatal("link did not enter data-channel establishment")
	}
	epoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{"b"}); err != nil {
		t.Fatalf("reconcile starting link: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish starting link: %v", err)
	}
	close(connection.release)
	select {
	case err = <-installed:
		if err != nil {
			t.Fatalf("install starting link after refresh: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("starting link did not publish")
	}
	// A freshly opened link remains connecting until its first authenticated
	// peer health frame, but it must already be published and observable.
	if got := manager.ObservePeer(transport.KindLive, "b").State; got != transport.HealthConnecting {
		t.Fatalf("starting link state=%v", got)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedLinkSurvivesStaleOldEpochAndResumesRawReceive(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 8), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 4, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = manager.InstallLive(context.Background(), "b", link); err != nil {
		t.Fatal(err)
	}
	epoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{"b"}); err != nil {
		t.Fatalf("rebind retained link: %v", err)
	}
	select {
	case <-channel.closed:
		t.Fatal("retained rebind closed the raw data channel")
	default:
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish rebound link: %v", err)
	}

	lateEnvelope := liveEnvelope(t, "b", "a")
	late := transport.AuthenticatedReceived{Envelope: lateEnvelope, LiveAuthorityEpoch: epoch - 1}
	if !link.publishAuthenticated(&late) {
		t.Fatal("Link retained ownership decision instead of transferring stale input to Manager")
	}
	if late.Envelope.MessageID != "" {
		t.Fatal("transferred old-epoch ingress ownership was not cleared")
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	fresh := liveEnvelope(t, "b", "a")
	encoded, err := codec.Encode(fresh)
	if err != nil {
		t.Fatal(err)
	}
	channel.inbound <- Frame{Kind: FrameEnvelope, Data: encoded}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	received, receiveErr := manager.ReceiveWithKind(ctx)
	cancel()
	if receiveErr != nil || received.Envelope.MessageID != fresh.MessageID {
		t.Fatalf("rebound raw receive=%#v err=%v", received, receiveErr)
	}
	select {
	case <-channel.closed:
		t.Fatal("stale old-epoch input closed the retained raw data channel")
	default:
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityBlockRejectsLatePayloadCompletion(t *testing.T) {
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 8), sentNote: make(chan Frame, 16), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 2, TransferWorkers: 1, TransferQueue: 4, Clock: rank1TestClock}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := transport.NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	if err = manager.InstallLive(context.Background(), "b", link); err != nil {
		t.Fatal(err)
	}
	carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 512, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
	messageID := "msg_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 16))
	route := payload.CarrierRoute{PeerID: "b", MeshID: "mesh", SenderID: "a", RecipientID: "b", MessageID: messageID}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: []byte("manifest")}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, finishErr := carrier.Finish(context.Background(), route, transferID); result <- finishErr }()
	for {
		select {
		case sent := <-channel.sentNote:
			if sent.Kind == FrameTransferFinish {
				goto sent
			}
		case <-time.After(time.Second):
			t.Fatal("finish frame was not sent")
		}
	}
sent:
	manager.BlockLiveAuthority()
	managed, managedErr := NewManagedPayloadTransfer(manager, PayloadTransferConfig{MaximumChunkBytes: 512, InFlightChunks: 1, MaximumTransfers: 1})
	if managedErr != nil {
		t.Fatal(managedErr)
	}
	blockedID := "xfer_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 16))
	if beginErr := managed.Begin(context.Background(), route, payload.CarrierFrame{TransferID: blockedID, Encoding: payload.FrameBinary, Data: []byte("blocked")}); !errors.Is(beginErr, payload.ErrCarrierUnavailable) {
		t.Fatalf("managed payload admission after fence=%v", beginErr)
	}
	digest := sha256.Sum256([]byte("late"))
	channel.inbound <- Frame{Kind: FrameTransferCompletion, TransferID: transferID, Evidence: payload.CompletionEvidence{TransferID: transferID, MessageID: messageID, Digest: digest}}
	select {
	case finishErr := <-result:
		if !errors.Is(finishErr, payload.ErrCarrierUnavailable) {
			t.Fatalf("late completion result=%v", finishErr)
		}
	case <-time.After(time.Second):
		t.Fatal("authority block did not release completion waiter")
	}
}
