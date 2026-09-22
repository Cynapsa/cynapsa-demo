package rank1webrtc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func bufferedRank1Received(t *testing.T) transport.AuthenticatedReceived {
	t.Helper()
	envelope := liveEnvelope(t, "b@example.test/mesh", "a@example.test/mesh")
	return transport.AuthenticatedReceived{
		Envelope: envelope,
		Authentication: transport.LiveAuthentication{
			Peer:           envelope.Sender,
			MeshID:         envelope.MeshID,
			ChannelBinding: sha256.Sum256([]byte("buffered-close-binding")),
		},
	}
}

func bufferedTestLink(t *testing.T) *Link {
	t.Helper()
	channel := &fakeChannel{maximum: 1024, inbound: make(chan Frame, 1), state: transport.HealthHealthy, closed: make(chan struct{})}
	link, err := NewLink(Config{
		MeshID: "mesh", LocalIdentity: "a@example.test/mesh", PeerID: "b@example.test/mesh", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1, TransferQueue: 1, Clock: rank1TestClock,
	}, &fakePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func TestBufferedAuthenticatedEnvelopeWinsLinkClose(t *testing.T) {
	for range 1000 {
		link := bufferedTestLink(t)
		want := bufferedRank1Received(t)
		link.envelopes <- want
		if err := link.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		got, err := link.ReceiveAuthenticated(context.Background())
		if err != nil || got.Envelope.MessageID != want.Envelope.MessageID || got.Authentication.Peer != "b@example.test/mesh" {
			t.Fatalf("buffered receive=%#v err=%v", got, err)
		}
		if _, err = link.ReceiveAuthenticated(context.Background()); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("drained receive=%v", err)
		}
	}
}

func TestLinkTerminalAuthenticatedEnvelopeDrainsAfterFullQueueFIFO(t *testing.T) {
	for range 1000 {
		link := bufferedTestLink(t)
		first := bufferedRank1Received(t)
		second := bufferedRank1Received(t)
		second.Envelope.Payload.Inline = []byte("terminal-second")
		link.envelopes <- first
		link.pumpStarted = true
		link.cancel()
		if link.publishAuthenticated(&second) {
			t.Fatal("terminal authenticated publication unexpectedly continued")
		}
		close(link.pumpDone)
		gotFirst, err := link.ReceiveAuthenticated(context.Background())
		if err != nil || gotFirst.Envelope.MessageID != first.Envelope.MessageID {
			t.Fatalf("first=%#v err=%v", gotFirst, err)
		}
		gotSecond, err := link.ReceiveAuthenticated(context.Background())
		if err != nil || string(gotSecond.Envelope.Payload.Inline) != "terminal-second" {
			t.Fatalf("second=%#v err=%v", gotSecond, err)
		}
		if _, err = link.ReceiveAuthenticated(context.Background()); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("drained=%v", err)
		}
		if link.terminalOwned {
			t.Fatal("terminal envelope ownership remained after drain")
		}
	}
}

func TestCancelledReceiveDoesNotTransferBufferedOwnership(t *testing.T) {
	link := bufferedTestLink(t)
	owned := bufferedRank1Received(t)
	canary := append([]byte(nil), owned.Envelope.Payload.Inline...)
	link.envelopes <- owned
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := link.ReceiveAuthenticated(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled receive=%v", err)
	}
	if len(link.envelopes) != 1 {
		t.Fatal("pre-cancelled receive consumed buffered ownership")
	}
	remaining := <-link.envelopes
	if string(remaining.Envelope.Payload.Inline) != string(canary) {
		t.Fatal("pre-cancelled receive mutated buffered ownership")
	}
	clearAuthenticatedReceived(&remaining)
	if err := link.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerReplacementRejectsOldSourceAndDeliversCurrentSource(t *testing.T) {
	for range 250 {
		manager, err := transport.NewLiveManager(2)
		if err != nil {
			t.Fatal(err)
		}
		if err = manager.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		old := bufferedTestLink(t)
		replacement := bufferedTestLink(t)
		want := bufferedRank1Received(t)
		old.envelopes <- want
		if err = manager.InstallLive(context.Background(), "b@example.test/mesh", old); err != nil {
			t.Fatal(err)
		}
		if err = manager.InstallLive(context.Background(), "b@example.test/mesh", replacement); err != nil {
			t.Fatal(err)
		}
		fresh := bufferedRank1Received(t)
		fresh.Envelope.Payload.Inline = []byte("replacement-current-source")
		replacement.envelopes <- fresh
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, receiveErr := manager.ReceiveWithKind(ctx)
		cancel()
		if receiveErr != nil || string(got.Envelope.Payload.Inline) != "replacement-current-source" || got.Authentication == nil || got.Authentication.Peer != "b@example.test/mesh" {
			t.Fatalf("replacement receive=%#v err=%v", got, receiveErr)
		}
		for deadline := time.Now().Add(time.Second); ; {
			old.mu.Lock()
			discarded := old.channel == nil
			old.mu.Unlock()
			if discarded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("old Link receiver drained without terminal discard")
			}
			time.Sleep(time.Millisecond)
		}
		short, shortCancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, receiveErr = manager.ReceiveWithKind(short)
		shortCancel()
		if !errors.Is(receiveErr, context.DeadlineExceeded) {
			t.Fatalf("duplicate receive=%v", receiveErr)
		}
		if err = manager.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAuthorityFenceRejectsOldBufferedEnvelopeAtEveryQueue(t *testing.T) {
	manager, err := transport.NewLiveManager(4)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := bufferedTestLink(t)
	linkBuffered := bufferedRank1Received(t)
	old.envelopes <- linkBuffered
	if err = manager.InstallLive(context.Background(), "b@example.test/mesh", old); err != nil {
		t.Fatal(err)
	}
	epoch := manager.BlockLiveAuthority()
	manager.QuarantineLiveAuthority()
	if err = manager.RetireAllLive(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatal(err)
	}
	replacement := bufferedTestLink(t)
	if err = manager.InstallLive(context.Background(), "b@example.test/mesh", replacement); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err = manager.ReceiveWithKind(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-fence envelope escaped retirement: %v", err)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPionBufferedFrameWinsChannelCloseAndCallerCancelWins(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	channel := &pionDataChannel{receive: make(chan Frame, 1), done: closed}
	channel.receive <- Frame{Kind: FrameEnvelope, Data: []byte("owned")}
	frame, err := channel.Receive(context.Background())
	if err != nil || frame.Kind != FrameEnvelope || string(frame.Data) != "owned" {
		t.Fatalf("closed buffered frame=%#v err=%v", frame, err)
	}
	channel.receive <- Frame{Kind: FrameEnvelope, Data: []byte("cancelled")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = channel.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled buffered frame=%v", err)
	}
	if len(channel.receive) != 1 {
		t.Fatal("pre-cancelled pion receive consumed frame")
	}
	remaining := <-channel.receive
	clearOwnedFrame(&remaining)
	channel.receive <- Frame{Kind: FrameEnvelope, Data: []byte("owned-cancel")}
	ownedCtx, ownedCancel := context.WithCancel(context.Background())
	ownedCancel()
	frame, err = channel.receiveOwned(ownedCtx)
	if err != nil || string(frame.Data) != "owned-cancel" {
		t.Fatalf("owned buffered cancel frame=%#v err=%v", frame, err)
	}
}

type terminalRaceDetached struct {
	encoded  []byte
	returned chan struct{}
	once     sync.Once
}

func (d *terminalRaceDetached) Read([]byte) (int, error)  { return 0, errors.New("unused") }
func (d *terminalRaceDetached) Write([]byte) (int, error) { return 0, errors.New("unused") }
func (d *terminalRaceDetached) ReadDataChannel(buffer []byte) (int, bool, error) {
	first := false
	d.once.Do(func() {
		copy(buffer, d.encoded)
		close(d.returned)
		first = true
	})
	if first {
		return len(d.encoded), false, nil
	}
	return 0, false, io.EOF
}
func (d *terminalRaceDetached) WriteDataChannel([]byte, bool) (int, error) {
	return 0, errors.New("unused")
}
func (d *terminalRaceDetached) Close() error                     { return nil }
func (d *terminalRaceDetached) SetReadDeadline(time.Time) error  { return nil }
func (d *terminalRaceDetached) SetWriteDeadline(time.Time) error { return nil }

func TestPionTerminalDrainsBlockedDecodedFrameAfterQueueFIFO(t *testing.T) {
	for range 1000 {
		responseNonce := bytes.Repeat([]byte{0x2}, 16)
		encoded, err := transport.EncodeControlFrame(Frame{Kind: FrameHealthReply, ProbeNonce: responseNonce}, 1024)
		if err != nil {
			t.Fatal(err)
		}
		detached := &terminalRaceDetached{encoded: encoded, returned: make(chan struct{})}
		channel := &pionDataChannel{
			maximum: 1024, receiveMaximum: 1024, receive: make(chan Frame, 1), done: make(chan struct{}),
			readDone: make(chan struct{}), readStarted: true, detached: detached, state: transport.HealthHealthy, clock: rank1TestClock,
		}
		channel.receive <- Frame{Kind: FrameHealthReply, ProbeNonce: make([]byte, 16)}
		go channel.readLoop(detached)
		<-detached.returned
		channel.markClosed()
		first, receiveErr := channel.receiveOwned(context.Background())
		if receiveErr != nil || first.Kind != FrameHealthReply {
			t.Fatalf("first frame=%#v err=%v", first, receiveErr)
		}
		second, receiveErr := channel.receiveOwned(context.Background())
		if receiveErr != nil || second.Kind != FrameHealthReply || !bytes.Equal(second.ProbeNonce, responseNonce) {
			t.Fatalf("decoded terminal frame=%#v err=%v", second, receiveErr)
		}
		if _, receiveErr = channel.receiveOwned(context.Background()); !errors.Is(receiveErr, transport.ErrClosed) {
			t.Fatalf("drained terminal receive=%v", receiveErr)
		}
		select {
		case <-channel.readDone:
		default:
			t.Fatal("terminal returned before read producer quiesced")
		}
		if len(channel.receive) != 0 || channel.terminalOwned {
			t.Fatal("decoded frame was enqueued after terminal return")
		}
		clear(first.ProbeNonce)
		clear(second.ProbeNonce)
		clear(responseNonce)
		clear(encoded)
	}
}

func TestGlobalShutdownDiscardClearsLinkAndPionOwnedQueues(t *testing.T) {
	link := bufferedTestLink(t)
	queued := bufferedRank1Received(t)
	queued.Envelope.Payload.Inline = []byte("link-queued-secret")
	queuedCanary := queued.Envelope.Payload.Inline
	terminal := bufferedRank1Received(t)
	terminal.Envelope.Payload.Inline = []byte("link-terminal-secret")
	terminalCanary := terminal.Envelope.Payload.Inline
	link.envelopes <- queued
	link.mu.Lock()
	link.closed = true
	link.terminalEnvelope = terminal
	link.terminalOwned = true
	link.mu.Unlock()

	pionQueued := []byte("pion-queued-secret")
	pionTerminal := []byte("pion-terminal-secret")
	closed := make(chan struct{})
	close(closed)
	pion := &pionDataChannel{receive: make(chan Frame, 1), done: closed, terminal: true}
	pion.receive <- Frame{Kind: FrameEnvelope, Data: pionQueued}
	pion.terminalFrame = Frame{Kind: FrameEnvelope, Data: pionTerminal}
	pion.terminalOwned = true
	link.mu.Lock()
	link.channel = pion
	link.mu.Unlock()

	link.DiscardShutdownOwned()
	if len(link.envelopes) != 0 || link.terminalOwned || len(pion.receive) != 0 || pion.terminalOwned {
		t.Fatal("global shutdown retained lower receive ownership")
	}
	for name, canary := range map[string][]byte{
		"link queue": queuedCanary, "link terminal": terminalCanary,
		"pion queue": pionQueued, "pion terminal": pionTerminal,
	} {
		for _, value := range canary {
			if value != 0 {
				t.Fatalf("%s was not cleared", name)
			}
		}
	}
}

func TestManagerGlobalShutdownClearsSaturatedLinkIngress(t *testing.T) {
	link := bufferedTestLink(t)
	manager, err := transport.NewManager(1, link)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		link.envelopes <- bufferedRank1Received(t)
		deadline := time.Now().Add(time.Second)
		for len(link.envelopes) != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(link.envelopes) != 0 {
			t.Fatal("manager receive loop did not take saturation record")
		}
	}
	retained := bufferedRank1Received(t)
	retained.Envelope.Payload.Inline = []byte("manager-close-lower-retained-secret")
	canary := retained.Envelope.Payload.Inline
	link.envelopes <- retained
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(link.envelopes) != 0 {
		t.Fatal("joined manager shutdown retained Link ownership")
	}
	for _, value := range canary {
		if value != 0 {
			t.Fatal("joined manager shutdown did not clear Link payload")
		}
	}
}
