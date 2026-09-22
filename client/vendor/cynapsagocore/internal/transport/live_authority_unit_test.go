package transport

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type selectiveAuthorityAdapter struct {
	peer                string
	auth                chan AuthenticatedReceived
	stuck               <-chan struct{}
	entered             chan struct{}
	enterOnce           sync.Once
	startEntered        chan struct{}
	startRelease        chan struct{}
	startOnce           sync.Once
	closeCalls          atomic.Int32
	blockCalls          atomic.Int32
	rebinds             atomic.Int32
	authorityBlocked    atomic.Bool
	authorityDone       chan struct{}
	authorityOnce       sync.Once
	closeErr            error
	ignoreReceiveCancel bool
	mu                  sync.Mutex
	gate                *LiveAuthorityGate
	epoch               uint64
}

func (adapter *selectiveAuthorityAdapter) Kind() Kind { return KindLive }
func (adapter *selectiveAuthorityAdapter) Start(ctx context.Context) error {
	if adapter.startEntered == nil {
		return nil
	}
	adapter.startOnce.Do(func() { close(adapter.startEntered) })
	select {
	case <-adapter.startRelease:
		return nil
	case <-adapter.authorityDone:
		return ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (adapter *selectiveAuthorityAdapter) Send(context.Context, protocol.Envelope) error { return nil }
func (adapter *selectiveAuthorityAdapter) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (adapter *selectiveAuthorityAdapter) Close(context.Context) error {
	adapter.closeCalls.Add(1)
	return adapter.closeErr
}
func (adapter *selectiveAuthorityAdapter) Receive(context.Context) (protocol.Envelope, error) {
	return protocol.Envelope{}, ErrReceive
}
func (adapter *selectiveAuthorityAdapter) ReceiveAuthenticated(ctx context.Context) (AuthenticatedReceived, error) {
	if adapter.authorityBlocked.Load() {
		return AuthenticatedReceived{}, ErrUnavailable
	}
	if adapter.stuck != nil {
		adapter.enterOnce.Do(func() { close(adapter.entered) })
		<-adapter.stuck
		return AuthenticatedReceived{}, ErrClosed
	}
	if adapter.ignoreReceiveCancel {
		select {
		case received := <-adapter.auth:
			if adapter.authorityBlocked.Load() {
				return AuthenticatedReceived{}, ErrUnavailable
			}
			return received, nil
		case <-adapter.authorityDone:
			return AuthenticatedReceived{}, ErrUnavailable
		}
	}
	select {
	case received := <-adapter.auth:
		if adapter.authorityBlocked.Load() {
			return AuthenticatedReceived{}, ErrUnavailable
		}
		return received, nil
	case <-ctx.Done():
		return AuthenticatedReceived{}, ctx.Err()
	}
}
func (adapter *selectiveAuthorityAdapter) BindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	if gate == nil || !gate.Admit(epoch) {
		return false
	}
	adapter.mu.Lock()
	adapter.gate, adapter.epoch = gate, epoch
	adapter.mu.Unlock()
	return true
}
func (adapter *selectiveAuthorityAdapter) RebindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	if gate == nil || !gate.Rebindable(epoch) {
		return false
	}
	adapter.mu.Lock()
	adapter.gate, adapter.epoch = gate, epoch
	adapter.mu.Unlock()
	adapter.rebinds.Add(1)
	return true
}
func (adapter *selectiveAuthorityAdapter) BlockLiveAuthority() {
	adapter.blockCalls.Add(1)
	adapter.authorityBlocked.Store(true)
	if adapter.authorityDone != nil {
		adapter.authorityOnce.Do(func() { close(adapter.authorityDone) })
	}
}
func (adapter *selectiveAuthorityAdapter) LiveAuthorityDone() <-chan struct{} {
	return adapter.authorityDone
}

func TestPendingInstallRebindsAcrossAuthorityRefreshBeforePublication(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := "pending@example.test/mesh"
	adapter := &selectiveAuthorityAdapter{
		peer: peer, auth: make(chan AuthenticatedReceived, 1), authorityDone: make(chan struct{}),
		startEntered: make(chan struct{}), startRelease: make(chan struct{}),
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), peer, adapter) }()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("pending install did not enter Start")
	}

	epoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{peer}); err != nil {
		t.Fatalf("reconcile pending install: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish pending install authority: %v", err)
	}
	close(adapter.startRelease)
	select {
	case err = <-installed:
		if err != nil {
			t.Fatalf("pending install after refresh: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending install did not publish")
	}
	if got := adapter.rebinds.Load(); got != 1 {
		t.Fatalf("pending rebinds=%d, want 1", got)
	}
	if got := adapter.closeCalls.Load(); got != 0 {
		t.Fatalf("retained pending close calls=%d, want 0", got)
	}
	if got := manager.ObservePeer(KindLive, peer).State; got != HealthHealthy {
		t.Fatalf("pending installed state=%v", got)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPendingInstallThatStartsDuringFenceWaitsForReconciliation(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := "fenced-pending@example.test/mesh"
	adapter := &selectiveAuthorityAdapter{
		peer: peer, auth: make(chan AuthenticatedReceived, 1), authorityDone: make(chan struct{}),
		startEntered: make(chan struct{}), startRelease: make(chan struct{}),
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), peer, adapter) }()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("pending install did not enter Start")
	}
	epoch := manager.BlockLiveAuthority()
	close(adapter.startRelease)
	select {
	case err = <-installed:
		t.Fatalf("pending install escaped closed authority gate: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{peer}); err != nil {
		t.Fatalf("reconcile completed pending install: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish completed pending install: %v", err)
	}
	select {
	case err = <-installed:
		if err != nil {
			t.Fatalf("pending install after publication: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending install remained blocked after publication")
	}
	if got := manager.ObservePeer(KindLive, peer).State; got != HealthHealthy {
		t.Fatalf("completed pending state=%v", got)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPendingInstallRemovedDuringAuthorityRefreshIsClosedOnce(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := "removed-pending@example.test/mesh"
	adapter := &selectiveAuthorityAdapter{
		peer: peer, auth: make(chan AuthenticatedReceived, 1), authorityDone: make(chan struct{}),
		startEntered: make(chan struct{}), startRelease: make(chan struct{}),
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), peer, adapter) }()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("pending install did not enter Start")
	}

	epoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, nil); err != nil {
		t.Fatalf("remove pending install: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish removal: %v", err)
	}
	select {
	case err = <-installed:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("removed pending install=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("removed pending install did not return")
	}
	if got := adapter.blockCalls.Load(); got != 1 {
		t.Fatalf("removed pending authority blocks=%d, want 1", got)
	}
	if got := adapter.closeCalls.Load(); got != 1 {
		t.Fatalf("removed pending close calls=%d, want 1", got)
	}
	if got := manager.ObservePeer(KindLive, peer).State; got != HealthUnknown {
		t.Fatalf("removed pending state=%v", got)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileLiveAuthorityRetainsCurrentPeerAndClosesOnlyAbsentPeer(t *testing.T) {
	manager, err := NewLiveManager(8)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	retainedPeer := "retained@example.test/mesh"
	absentPeer := "absent@example.test/mesh"
	retained := &selectiveAuthorityAdapter{peer: retainedPeer, auth: make(chan AuthenticatedReceived, 2)}
	absent := &selectiveAuthorityAdapter{peer: absentPeer, auth: make(chan AuthenticatedReceived, 1)}
	if err = manager.InstallLive(context.Background(), retainedPeer, retained); err != nil {
		t.Fatal(err)
	}
	if err = manager.InstallLive(context.Background(), absentPeer, absent); err != nil {
		t.Fatal(err)
	}

	epoch := manager.BlockLiveAuthority()
	if epoch == 0 {
		t.Fatal("authority fence did not return immediately with an epoch")
	}
	retained.auth <- AuthenticatedReceived{Envelope: authorityTestEnvelope(t, retainedPeer), Authentication: LiveAuthentication{Peer: retainedPeer, MeshID: "mesh"}}
	for deadline := time.Now().Add(time.Second); len(manager.inbound) != 1; {
		if time.Now().After(deadline) {
			t.Fatal("paused authenticated input remained Link-owned instead of reaching Manager handoff")
		}
		time.Sleep(time.Millisecond)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{retainedPeer}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := absent.closeCalls.Load(); got != 1 {
		t.Fatalf("absent raw close calls=%d, want 1", got)
	}
	if got := absent.blockCalls.Load(); got != 1 {
		t.Fatalf("absent authority blocks=%d, want 1", got)
	}
	if got := retained.closeCalls.Load(); got != 0 {
		t.Fatalf("retained raw close calls=%d, want 0", got)
	}
	if got := retained.rebinds.Load(); got != 1 {
		t.Fatalf("retained rebinds=%d, want 1", got)
	}
	if err = manager.PublishLiveAuthority(epoch - 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("old epoch publication=%v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish current epoch: %v", err)
	}
	fresh := authorityTestEnvelope(t, retainedPeer)
	retained.auth <- AuthenticatedReceived{Envelope: fresh, Authentication: LiveAuthentication{Peer: retainedPeer, MeshID: "mesh"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	paused, receiveErr := manager.ReceiveWithKind(ctx)
	if receiveErr != nil || !paused.pausedAdmission || paused.Authentication == nil || paused.Authentication.Peer != retainedPeer {
		cancel()
		t.Fatalf("paused retained receive=%#v err=%v", paused, receiveErr)
	}
	clearReceived(&paused)
	received, receiveErr := manager.ReceiveWithKind(ctx)
	cancel()
	if receiveErr != nil || received.Envelope.MessageID != fresh.MessageID || received.pausedAdmission {
		t.Fatalf("retained receive=%#v err=%v", received, receiveErr)
	}
	clearReceived(&received)
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileLiveAuthorityNoncooperativeReceiveLeavesFenceClosed(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	adapter := &selectiveAuthorityAdapter{peer: "stuck@example.test/mesh", stuck: release, entered: make(chan struct{})}
	if err = manager.InstallLive(context.Background(), adapter.peer, adapter); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("receive loop did not enter dependency")
	}
	fenced := make(chan uint64, 1)
	go func() { fenced <- manager.BlockLiveAuthority() }()
	var epoch uint64
	select {
	case epoch = <-fenced:
	case <-time.After(time.Second):
		t.Fatal("constant-time authority fence waited for noncooperative receive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = manager.ReconcileLiveAuthority(ctx, epoch, []string{adapter.peer})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded rebind=%v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("noncooperative rebind reopened authority: %v", err)
	}
	if got := adapter.closeCalls.Load(); got != 0 {
		t.Fatalf("retained noncooperative adapter close calls=%d", got)
	}
	close(release)
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPublishLiveAuthorityIgnoresIngressInertObsoleteQuarantine(t *testing.T) {
	manager, err := NewLiveManager(4)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	obsoletePeer := "obsolete@example.test/mesh"
	currentPeer := "current@example.test/mesh"
	closeFailure := errors.New("obsolete close did not cooperate")
	obsolete := &selectiveAuthorityAdapter{
		peer: obsoletePeer, auth: make(chan AuthenticatedReceived, 1), authorityDone: make(chan struct{}),
		closeErr: closeFailure, ignoreReceiveCancel: true,
	}
	current := &selectiveAuthorityAdapter{peer: currentPeer, auth: make(chan AuthenticatedReceived, 1)}
	if err = manager.InstallLive(context.Background(), obsoletePeer, obsolete); err != nil {
		t.Fatalf("install obsolete: %v", err)
	}
	if err = manager.InstallLive(context.Background(), currentPeer, current); err != nil {
		t.Fatalf("install current: %v", err)
	}
	if err = manager.RemoveLive(context.Background(), obsoletePeer); !errors.Is(err, ErrClosed) {
		t.Fatalf("detach obsolete close failure=%v", err)
	}
	if got := obsolete.blockCalls.Load(); got != 1 {
		t.Fatalf("obsolete authority blocks=%d, want 1", got)
	}
	select {
	case <-obsolete.authorityDone:
	case <-time.After(time.Second):
		t.Fatal("obsolete adapter authority fence remained open")
	}
	if observation := manager.ObservePeer(KindLive, obsoletePeer); observation.State != HealthUnknown {
		t.Fatalf("obsolete adapter remains addressable: %#v", observation)
	}

	epoch := manager.BlockLiveAuthority()
	if epoch == 0 {
		t.Fatal("authority fence did not allocate an epoch")
	}
	if err = manager.PublishLiveAuthority(epoch); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("publication without exact preparation=%v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch+1, []string{currentPeer}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wrong-epoch reconciliation=%v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{currentPeer}); err != nil {
		t.Fatalf("exact reconciliation: %v", err)
	}
	if err = manager.PublishLiveAuthority(epoch + 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wrong-epoch publication=%v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("quarantined close failure blocked exact publication: %v", err)
	}

	obsolete.auth <- AuthenticatedReceived{
		Envelope:       authorityTestEnvelope(t, obsoletePeer),
		Authentication: LiveAuthentication{Peer: obsoletePeer, MeshID: "mesh"},
	}
	fresh := authorityTestEnvelope(t, currentPeer)
	current.auth <- AuthenticatedReceived{
		Envelope:       fresh,
		Authentication: LiveAuthentication{Peer: currentPeer, MeshID: "mesh"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	received, receiveErr := manager.ReceiveWithKind(ctx)
	cancel()
	if receiveErr != nil || received.Envelope.MessageID != fresh.MessageID || received.Authentication == nil || received.Authentication.Peer != currentPeer {
		t.Fatalf("current authorized ingress=%#v err=%v", received, receiveErr)
	}
	clearReceived(&received)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	received, receiveErr = manager.ReceiveWithKind(ctx)
	cancel()
	clearReceived(&received)
	if !errors.Is(receiveErr, context.DeadlineExceeded) {
		t.Fatalf("quarantined adapter published after reopen: %v", receiveErr)
	}

	if closeErr := manager.Close(context.Background()); !errors.Is(closeErr, ErrClosed) {
		t.Fatalf("manager close=%v, want normalized obsolete close failure", closeErr)
	}
}

func TestLiveAuthorityFenceCapturesExactPeersAndOnlyExactOwnerCanPublish(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := "peer@example.test/mesh"
	old := &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}
	if err := manager.InstallLive(context.Background(), peer, old); err != nil {
		t.Fatalf("install old: %v", err)
	}
	epoch := manager.BlockLiveAuthority()
	if epoch == 0 {
		t.Fatal("authority fence did not allocate an epoch")
	}
	if err := manager.Send(context.Background(), KindLive, authorityTestEnvelope(t, peer)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("send after fence=%v", err)
	}
	if peers := manager.QuarantineLiveAuthority(); !reflect.DeepEqual(peers, []string{peer}) {
		t.Fatalf("captured peers=%q", peers)
	}
	newLink := &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}
	if err := manager.InstallLive(context.Background(), peer, newLink); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("install while blocked=%v", err)
	}
	if err := manager.RetireAllLive(context.Background()); err != nil {
		t.Fatalf("retire old authority: %v", err)
	}
	if err := manager.PublishLiveAuthority(epoch + 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("stale owner publication=%v", err)
	}
	if err := manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish exact owner: %v", err)
	}
	if err := manager.InstallLive(context.Background(), peer, newLink); err != nil {
		t.Fatalf("install current link: %v", err)
	}
	if observation := manager.ObservePeer(KindLive, peer); observation.State != HealthHealthy {
		t.Fatalf("exact owner publication did not retain current link: %#v", observation)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedLiveAuthorityFenceSupersedesPreparedPublication(t *testing.T) {
	manager, err := NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	first := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), first, nil); err != nil {
		t.Fatal(err)
	}
	if !manager.LiveAuthorityPublicationCurrent(first) {
		t.Fatal("prepared publication capability was not current")
	}
	second := manager.BlockLiveAuthority()
	if second == 0 || second == first {
		t.Fatalf("second fence epoch=%d, first=%d", second, first)
	}
	if manager.LiveAuthorityPublicationCurrent(first) {
		t.Fatal("stale prepared publication survived the second fence")
	}
	if err = manager.PublishLiveAuthority(first); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("stale publication = %v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), second, nil); err != nil {
		t.Fatal(err)
	}
	if !manager.LiveAuthorityPublicationCurrent(second) {
		t.Fatal("replacement publication capability was not current")
	}
}

func TestLiveAuthorityEpochExhaustionLatchesBlockedWithoutZeroReuse(t *testing.T) {
	gate := newLiveAuthorityGate()
	gate.epoch.Store(^uint64(0))
	loss := gate.Loss(^uint64(0))
	if epoch := gate.Block(); epoch != 0 {
		t.Fatalf("exhausted epoch=%d", epoch)
	}
	if !gate.exhausted.Load() || gate.Admit(^uint64(0)) || gate.Publish(^uint64(0)) || gate.epoch.Load() != ^uint64(0) {
		t.Fatalf("exhausted gate reopened: exhausted=%t blocked=%t epoch=%d", gate.exhausted.Load(), gate.blocked.Load(), gate.epoch.Load())
	}
	if epoch := gate.Block(); epoch != 0 || gate.epoch.Load() != ^uint64(0) {
		t.Fatalf("repeated exhausted block epoch=%d stored=%d", epoch, gate.epoch.Load())
	}
	select {
	case <-loss:
	default:
		t.Fatal("exhausted authority did not wake the final epoch waiter")
	}
}

func authorityTestEnvelope(t *testing.T, sender string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("native", []byte("authority"))
	if err != nil {
		t.Fatal(err)
	}
	recipient := "local@example.test/mesh"
	conversationID, err := conversation.DeriveID("mesh", sender, recipient)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: conversationID,
		Sender:         sender, Recipient: recipient, MeshID: "mesh",
		Mode:      protocol.ModeMessage,
		CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}
