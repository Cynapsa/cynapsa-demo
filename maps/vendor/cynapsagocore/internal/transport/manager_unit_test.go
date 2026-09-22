package transport

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type fakeAdapter struct {
	kind            Kind
	mu              sync.Mutex
	started, closed bool
	recv            chan protocol.Envelope
	sent            []protocol.Envelope
	startErr        error
	sendErr         error
	closeErr        error
	panicStart      bool
}

type fakeBoundAdapter struct {
	*fakeAdapter
	auth chan AuthenticatedReceived
}

func (adapter *fakeBoundAdapter) ReceiveAuthenticated(ctx context.Context) (AuthenticatedReceived, error) {
	select {
	case received := <-adapter.auth:
		return received, nil
	case <-ctx.Done():
		return AuthenticatedReceived{}, ctx.Err()
	}
}

func (f *fakeAdapter) Kind() Kind { return f.kind }
func (f *fakeAdapter) Start(context.Context) error {
	if f.panicStart {
		panic("dependency")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return f.startErr
	}
	f.started = true
	return nil
}

func TestManagerContainsAdapterPanic(t *testing.T) {
	adapter := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope), panicStart: true}
	m, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Start(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("panic=%v", err)
	}
}
func (f *fakeAdapter) Send(_ context.Context, e protocol.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, e.Clone())
	return f.sendErr
}
func (f *fakeAdapter) Receive(ctx context.Context) (protocol.Envelope, error) {
	select {
	case e := <-f.recv:
		return e.Clone(), nil
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	}
}
func (f *fakeAdapter) Observe() Observation { return Observation{State: HealthHealthy} }
func (f *fakeAdapter) Close(context.Context) error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return f.closeErr
}

type blockingStartAdapter struct {
	kind    Kind
	entered chan struct{}
	once    sync.Once
}

func (b *blockingStartAdapter) Kind() Kind { return b.kind }
func (b *blockingStartAdapter) Start(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return ctx.Err()
}
func (b *blockingStartAdapter) Send(context.Context, protocol.Envelope) error { return nil }
func (b *blockingStartAdapter) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (b *blockingStartAdapter) Observe() Observation        { return Observation{State: HealthConnecting} }
func (b *blockingStartAdapter) Close(context.Context) error { return nil }

func TestManagerCloseWinsInFlightStart(t *testing.T) {
	adapter := &blockingStartAdapter{kind: KindDurable, entered: make(chan struct{})}
	m, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- m.Start(context.Background()) }()
	<-adapter.entered
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := m.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("late start=%v", err)
	}
}

func TestManagerNormalizesByOperationAndRedacts(t *testing.T) {
	canary := errors.New("secret dependency canary")
	start, _ := NewManager(1, &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope), startErr: canary})
	if err := start.Start(context.Background()); !errors.Is(err, ErrUnavailable) || err.Error() == canary.Error() {
		t.Fatalf("start=%v", err)
	}
	sendAdapter := &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope), sendErr: canary, closeErr: canary}
	m, _ := NewManager(1, sendAdapter)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Send(context.Background(), KindDurable, managerEnvelope(t)); !errors.Is(err, ErrSendAmbiguous) || err.Error() == canary.Error() {
		t.Fatalf("send=%v", err)
	}
	if err := m.Close(context.Background()); !errors.Is(err, ErrClosed) || err.Error() == canary.Error() {
		t.Fatalf("close=%v", err)
	}
}

func TestManagerPeerScopedLiveLinksAreIndependent(t *testing.T) {
	durable := &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope)}
	m, err := NewManager(4, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Close(context.Background())
	peers := []string{"agent1", "agent2", "agent3"}
	links := make(map[string]*fakeAdapter)
	for _, peer := range peers {
		links[peer] = &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
		if err = m.InstallLive(context.Background(), peer, links[peer]); err != nil {
			t.Fatal(err)
		}
	}
	for _, peer := range peers {
		e := managerEnvelope(t)
		e.Recipient = peer
		if err = m.Send(context.Background(), KindLive, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, peer := range peers {
		links[peer].mu.Lock()
		count := len(links[peer].sent)
		links[peer].mu.Unlock()
		if count != 1 {
			t.Fatalf("%s sends=%d", peer, count)
		}
	}
	if err = m.RemoveLive(context.Background(), "agent2"); err != nil {
		t.Fatal(err)
	}
	e := managerEnvelope(t)
	e.Recipient = "agent2"
	if err = m.Send(context.Background(), KindLive, e); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("removed=%v", err)
	}
}

func TestManagerConditionalRemovalCannotDeleteReplacement(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	old := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	fresh := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	if err = manager.InstallLive(context.Background(), "peer/mesh", old); err != nil {
		t.Fatal(err)
	}
	if err = manager.InstallLive(context.Background(), "peer/mesh", fresh); err != nil {
		t.Fatal(err)
	}
	if err = manager.RemoveLiveIf(context.Background(), "peer/mesh", old); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	envelope.Recipient = "peer/mesh"
	if err = manager.Send(context.Background(), KindLive, envelope); err != nil {
		t.Fatalf("late removal deleted replacement: %v", err)
	}
	fresh.mu.Lock()
	freshSends, freshClosed := len(fresh.sent), fresh.closed
	fresh.mu.Unlock()
	if freshSends != 1 || freshClosed {
		t.Fatalf("replacement sends=%d closed=%v", freshSends, freshClosed)
	}
	if err = manager.RemoveLiveIf(context.Background(), "peer/mesh", fresh); err != nil {
		t.Fatal(err)
	}
	if err = manager.Send(context.Background(), KindLive, envelope); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exact conditional removal = %v", err)
	}
}

func managerEnvelope(t *testing.T) protocol.Envelope {
	t.Helper()
	p, err := protocol.NewInlinePayload("native", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("conversation"))
	e, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(h[:]), Sender: "a", Recipient: "b", MeshID: "m", Mode: protocol.ModeMessage, CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond, Payload: p, CredentialProof: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestLiveManagerPreservesAuthenticationAndRetiresOnAuthorityLoss(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	binding := sha256.Sum256([]byte("bound-live"))
	link := &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived, 1)}
	if err = manager.InstallLive(context.Background(), "b", link); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	if err = manager.Send(context.Background(), KindLive, envelope); err != nil {
		t.Fatal(err)
	}
	inbound := envelope
	inbound.Sender, inbound.Recipient = envelope.Recipient, envelope.Sender
	link.auth <- AuthenticatedReceived{Envelope: inbound, Authentication: LiveAuthentication{Peer: "b", MeshID: "m", ChannelBinding: binding}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received, err := manager.ReceiveWithKind(ctx)
	if err != nil || received.Authentication == nil || received.Authentication.Peer != "b" || received.Authentication.ChannelBinding != binding {
		t.Fatalf("authenticated receive = %#v, %v", received, err)
	}
	manager.BlockLiveAuthority()
	manager.QuarantineLiveAuthority()
	if err = manager.RetireAllLive(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !link.closed || manager.ObservePeer(KindLive, "b").State != HealthUnknown {
		t.Fatal("authority-retired link remained installed")
	}
}

func TestManagerStartsRoutesReceivesAndCloses(t *testing.T) {
	live := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope, 1)}
	durable := &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope, 1)}
	m, err := NewManager(2, live, durable)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	e := managerEnvelope(t)
	if err := m.Send(ctx, KindLive, e); err != nil {
		t.Fatal(err)
	}
	live.recv <- e
	rctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	got, err := m.ReceiveWithKind(rctx)
	if err != nil || got.Kind != KindLive || got.Envelope.MessageID != e.MessageID {
		t.Fatalf("receive = %#v, %v", got, err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !live.closed || !durable.closed {
		t.Fatal("adapters not closed")
	}
	if _, err := m.ReceiveWithKind(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed receive = %v", err)
	}
}

func TestManagerKeepsDurableWhenOptionalLiveStartFails(t *testing.T) {
	durable := &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope)}
	live := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope), startErr: errors.New("boom")}
	m, err := NewManager(1, durable, live)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start = %v", err)
	}
	if durable.closed || !durable.started {
		t.Fatal("durable path did not survive optional live failure")
	}
	_ = m.Close(context.Background())
}
