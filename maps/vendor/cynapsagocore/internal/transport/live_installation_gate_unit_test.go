package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestLiveInstallationGatePreservesInstalledDataPlane(t *testing.T) {
	manager, err := NewLiveManager(4)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	peer := "peer@example.test/resource"
	installed := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope, 1)}
	if err = manager.InstallLive(context.Background(), peer, installed); err != nil {
		t.Fatalf("initial install: %v", err)
	}

	epoch := manager.BlockLiveInstallation()
	if epoch == 0 {
		t.Fatal("setup fence did not allocate an epoch")
	}
	envelope := managerEnvelope(t)
	envelope.Recipient = peer
	if err = manager.Send(context.Background(), KindLive, envelope); err != nil {
		t.Fatalf("installed send while setup blocked: %v", err)
	}
	if observation := manager.ObservePeer(KindLive, peer); observation.State != HealthHealthy {
		t.Fatalf("installed observation while setup blocked: %#v", observation)
	}
	inbound := managerEnvelope(t)
	installed.recv <- inbound
	receiveContext, cancel := context.WithTimeout(context.Background(), time.Second)
	received, receiveErr := manager.ReceiveWithKind(receiveContext)
	cancel()
	if receiveErr != nil || received.Envelope.MessageID != inbound.MessageID {
		t.Fatalf("installed receive while setup blocked: %#v, %v", received, receiveErr)
	}
	clearReceived(&received)

	rejected := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	if err = manager.InstallLive(context.Background(), "new@example.test/resource", rejected); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("new install while setup blocked: %v", err)
	}
	rejected.mu.Lock()
	rejectedStarted := rejected.started
	rejected.mu.Unlock()
	if rejectedStarted {
		t.Fatal("setup-blocked adapter entered Start")
	}
	if err = manager.ResumeLiveInstallation(epoch + 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wrong setup resume: %v", err)
	}
	if err = manager.ResumeLiveInstallation(epoch); err != nil {
		t.Fatalf("exact setup resume: %v", err)
	}
	if err = manager.InstallLive(context.Background(), "new@example.test/resource", rejected); err != nil {
		t.Fatalf("install after exact resume: %v", err)
	}
}

func TestLiveInstallationResumeCannotCrossNewerOrAuthorityFence(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	first := manager.BlockLiveInstallation()
	second := manager.BlockLiveInstallation()
	if first == 0 || second == 0 || first == second {
		t.Fatalf("setup epochs first=%d second=%d", first, second)
	}
	if err = manager.PublishLiveInstallation(first); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("stale setup publication: %v", err)
	}
	if err = manager.PublishLiveInstallation(second); err != nil {
		t.Fatalf("current setup publication: %v", err)
	}

	stale := manager.BlockLiveInstallation()
	authorityEpoch := manager.BlockLiveAuthority()
	if authorityEpoch == 0 {
		t.Fatal("authority fence did not allocate an epoch")
	}
	if err = manager.ResumeLiveInstallation(stale); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("setup resume crossed authority fence: %v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), authorityEpoch, nil); err != nil {
		t.Fatalf("authority reconciliation: %v", err)
	}
	if err = manager.PublishLiveAuthority(authorityEpoch); err != nil {
		t.Fatalf("authority publication: %v", err)
	}
	if err = manager.InstallLive(context.Background(), "peer@example.test/resource", &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}); err != nil {
		t.Fatalf("authority publication did not reopen current setup epoch: %v", err)
	}
}

func TestAuthorityPublicationCannotOverrideNewerSetupFence(t *testing.T) {
	manager, err := NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	authorityEpoch := manager.BlockLiveAuthority()
	newerSetupEpoch := manager.BlockLiveInstallation()
	if authorityEpoch == 0 || newerSetupEpoch == 0 {
		t.Fatalf("fence epochs authority=%d setup=%d", authorityEpoch, newerSetupEpoch)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), authorityEpoch, nil); err != nil {
		t.Fatalf("authority reconciliation: %v", err)
	}
	if err = manager.PublishLiveAuthority(authorityEpoch); err != nil {
		t.Fatalf("authority publication: %v", err)
	}
	peer := "peer@example.test/resource"
	if err = manager.InstallLive(context.Background(), peer, &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("authority publication overrode newer setup fence: %v", err)
	}
	if err = manager.ResumeLiveInstallation(newerSetupEpoch); err != nil {
		t.Fatalf("newer setup owner resume: %v", err)
	}
	if err = manager.InstallLive(context.Background(), peer, &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}); err != nil {
		t.Fatalf("install after newer setup resume: %v", err)
	}
}

func TestConcurrentInstallAndSetupBlockLinearizeAtPublication(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	peer := "pending@example.test/resource"
	adapter := &selectiveAuthorityAdapter{
		auth:          make(chan AuthenticatedReceived),
		authorityDone: make(chan struct{}),
		startEntered:  make(chan struct{}),
		startRelease:  make(chan struct{}),
	}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), peer, adapter) }()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("pending install did not enter Start")
	}

	epoch := manager.BlockLiveInstallation()
	close(adapter.startRelease)
	select {
	case installErr := <-installed:
		if !errors.Is(installErr, ErrUnavailable) {
			t.Fatalf("pending install after setup fence: %v", installErr)
		}
	case <-time.After(time.Second):
		t.Fatal("pending setup-blocked install did not terminate")
	}
	if _, current := manager.LivePeer(peer); current {
		t.Fatal("setup-blocked pending adapter was published")
	}
	if calls := adapter.closeCalls.Load(); calls != 1 {
		t.Fatalf("unpublished adapter close calls=%d, want 1", calls)
	}
	if err = manager.ResumeLiveInstallation(epoch); err != nil {
		t.Fatalf("resume after concurrent fence: %v", err)
	}
	if err = manager.InstallLive(context.Background(), peer, &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}); err != nil {
		t.Fatalf("fresh install after resume: %v", err)
	}
}

func TestLiveInstallationEpochExhaustionFailsClosed(t *testing.T) {
	manager, err := NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	manager.mu.Lock()
	manager.liveInstallationEpoch = ^uint64(0)
	manager.mu.Unlock()
	if epoch := manager.BlockLiveInstallation(); epoch != 0 {
		t.Fatalf("exhausted setup epoch=%d", epoch)
	}
	if err = manager.ResumeLiveInstallation(^uint64(0)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("exhausted setup resume: %v", err)
	}

	authorityEpoch := manager.BlockLiveAuthority()
	if err = manager.ReconcileLiveAuthority(context.Background(), authorityEpoch, nil); err != nil {
		t.Fatalf("authority reconciliation at setup exhaustion: %v", err)
	}
	if err = manager.PublishLiveAuthority(authorityEpoch); err != nil {
		t.Fatalf("authority publication at setup exhaustion: %v", err)
	}
	if err = manager.InstallLive(context.Background(), "peer@example.test/resource", &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("install after exhausted authority publication: %v", err)
	}
}
