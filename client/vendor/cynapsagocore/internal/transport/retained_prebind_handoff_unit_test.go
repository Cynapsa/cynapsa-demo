package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// retainedHeldAdapter models a Link that already authenticated one old-epoch
// frame before rebind, but whose replacement Manager receiver cannot take that
// adapter-owned value until after the new epoch is published.
type retainedHeldAdapter struct {
	peer       string
	mu         sync.Mutex
	rebound    bool
	held       AuthenticatedReceived
	oldEntered chan struct{}
	newEntered chan struct{}
	release    chan struct{}
	oldOnce    sync.Once
	newOnce    sync.Once
}

func (*retainedHeldAdapter) Kind() Kind                                    { return KindLive }
func (*retainedHeldAdapter) Start(context.Context) error                   { return nil }
func (*retainedHeldAdapter) Send(context.Context, protocol.Envelope) error { return nil }
func (*retainedHeldAdapter) Observe() Observation                          { return Observation{State: HealthHealthy} }
func (*retainedHeldAdapter) Close(context.Context) error                   { return nil }
func (*retainedHeldAdapter) Receive(context.Context) (protocol.Envelope, error) {
	return protocol.Envelope{}, ErrReceive
}
func (adapter *retainedHeldAdapter) ReceiveAuthenticated(ctx context.Context) (AuthenticatedReceived, error) {
	adapter.mu.Lock()
	rebound := adapter.rebound
	adapter.mu.Unlock()
	if !rebound {
		adapter.oldOnce.Do(func() { close(adapter.oldEntered) })
		<-ctx.Done()
		return AuthenticatedReceived{}, ctx.Err()
	}
	adapter.newOnce.Do(func() { close(adapter.newEntered) })
	select {
	case <-adapter.release:
	case <-ctx.Done():
		return AuthenticatedReceived{}, ctx.Err()
	}
	adapter.mu.Lock()
	value := adapter.held
	adapter.held = AuthenticatedReceived{}
	adapter.mu.Unlock()
	return value, nil
}
func (*retainedHeldAdapter) BindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	return gate != nil && gate.Admit(epoch)
}
func (adapter *retainedHeldAdapter) RebindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	if gate == nil || !gate.Rebindable(epoch) {
		return false
	}
	adapter.mu.Lock()
	adapter.rebound = true
	adapter.mu.Unlock()
	return true
}

func TestRetainedPreRebindInputCannotBeDroppedAfterPublish(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	peer := "retained@example.test/mesh"
	adapter := &retainedHeldAdapter{peer: peer, oldEntered: make(chan struct{}), newEntered: make(chan struct{}), release: make(chan struct{})}
	if err = manager.InstallLive(context.Background(), peer, adapter); err != nil {
		t.Fatal(err)
	}
	<-adapter.oldEntered
	epoch := manager.BlockLiveAuthority()
	adapter.mu.Lock()
	adapter.held = AuthenticatedReceived{
		Envelope:           authorityTestEnvelope(t, peer),
		Authentication:     LiveAuthentication{Peer: peer, MeshID: "mesh"},
		LiveAuthorityEpoch: epoch - 1,
	}
	adapter.mu.Unlock()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{peer}); err != nil {
		t.Fatal(err)
	}
	<-adapter.newEntered
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatal(err)
	}
	close(adapter.release)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	received, receiveErr := manager.ReceiveWithKind(ctx)
	if receiveErr != nil || received.Envelope.MessageID == "" || !received.pausedAdmission {
		t.Fatalf("pre-rebind authenticated input dropped after publish: received=%#v err=%v", received, receiveErr)
	}
}

func TestLiveAuthorityCaptureIsLimitedToOneClosedFence(t *testing.T) {
	gate := newLiveAuthorityGate()
	oldEpoch := gate.epoch.Load()
	beforeFence := AuthenticatedReceived{}
	if !beforeFence.CaptureLiveAuthority(gate, oldEpoch) {
		t.Fatal("current authority epoch was not capturable")
	}

	current := gate.Block()
	if current != oldEpoch+1 {
		t.Fatalf("blocked epoch = %d, want %d", current, oldEpoch+1)
	}
	duringFence := AuthenticatedReceived{}
	if !duringFence.CaptureLiveAuthority(gate, oldEpoch) {
		t.Fatal("immediately previous epoch was not capturable while fence was closed")
	}
	if !gate.Publish(current) {
		t.Fatal("publish failed")
	}

	lateOld := AuthenticatedReceived{}
	if lateOld.CaptureLiveAuthority(gate, oldEpoch) {
		t.Fatal("old epoch first presented after publication received a capture capability")
	}
	fresh := AuthenticatedReceived{}
	if !fresh.CaptureLiveAuthority(gate, current) {
		t.Fatal("published current epoch was not capturable")
	}

	next := gate.Block()
	if next != current+1 || !gate.Publish(next) {
		t.Fatal("second authority fence failed")
	}
	tooOld := AuthenticatedReceived{}
	if tooOld.CaptureLiveAuthority(gate, oldEpoch) {
		t.Fatal("capture capability crossed more than one authority fence")
	}
}
