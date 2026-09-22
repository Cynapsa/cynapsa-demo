package peer

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T, count uint64, bytes uint64, closePeer func(string)) *Registry {
	t.Helper()
	registry, err := NewRegistry(RegistryConfig{
		MailboxCapacity: 8, LaneByteCapacity: bytes, GlobalCountCapacity: count,
		GlobalByteCapacity: bytes, MemberCapacity: 32, IdleTimeout: time.Hour, ClosePeer: closePeer,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func installAndPublishTestSnapshot(registry *Registry, session *AuthoritySession, members []string) error {
	if err := registry.InstallSnapshot(session, members); err != nil {
		return err
	}
	return registry.Publish(session)
}

func testWork(kind WorkKind, size uint64, ran *atomic.Int32, cleared *atomic.Int32) Work {
	return Work{Kind: kind, OwnedBytes: size, Run: func(_ context.Context, commit Commit) error {
		return commit(func() error {
			ran.Add(1)
			return nil
		})
	}, Clear: func() { cleared.Add(1) }}
}

func TestRegistryMemberEpochFailsClosedAtExhaustion(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	registry.nextMemberEpoch = math.MaxUint64
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallSnapshot(session, []string{"a@example.test"}); !errors.Is(err, ErrEpochExhausted) {
		t.Fatalf("install exhausted epoch=%v", err)
	}
	if epoch, ok := registry.PeerEpoch("a@example.test"); ok || epoch != 0 {
		t.Fatalf("exhausted epoch published=(%d,%t)", epoch, ok)
	}
}

func TestResultWaitPrefersPublishedTerminalOutcomeOverCancellation(t *testing.T) {
	for range 1000 {
		done := make(chan error, 1)
		done <- nil
		close(done)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := (Result{done: done}).Wait(ctx); err != nil {
			t.Fatalf("published result hidden by cancellation: %v", err)
		}
	}
}

func TestRegistryInstallRemainsPausedUntilExactSessionPublish(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallSnapshot(session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if registry.AuthorityState() != AuthoritySynchronizing || ran.Load() != 0 {
		t.Fatalf("install published early: state=%v ran=%d", registry.AuthorityState(), ran.Load())
	}
	stale := session
	if err = registry.Publish(session); err != nil {
		t.Fatal(err)
	}
	if err = result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	fresh, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallSnapshot(fresh, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err = registry.Publish(stale); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stale publish=%v", err)
	}
	if registry.AuthorityState() != AuthoritySynchronizing {
		t.Fatalf("stale publish changed state=%v", registry.AuthorityState())
	}
	if err = registry.Publish(fresh); err != nil {
		t.Fatal(err)
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrySuspendQueuesKnownMemberUntilExactResume(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err = registry.Suspend(session); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.AdmitOutbound(context.Background(), "b@example.test", testWork(WorkOutbound, 1, &ran, &cleared)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("nonmember admitted while suspended: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = result.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) || ran.Load() != 0 {
		t.Fatalf("suspended work result=%v ran=%d", err, ran.Load())
	}
	if err = registry.Resume(session); err != nil {
		t.Fatal(err)
	}
	if err = result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryHardFenceSupersedesSuspension(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err = registry.Suspend(session); err != nil {
		t.Fatal(err)
	}
	registry.BlockAdmission()
	if err = registry.Resume(session); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("resume after hard fence=%v", err)
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryPublishResumesLaneCreatedDuringAllowCallbacks(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var block atomic.Bool
	registry, err := NewRegistry(RegistryConfig{
		MailboxCapacity: 4, LaneByteCapacity: 32, GlobalCountCapacity: 4,
		GlobalByteCapacity: 32, MemberCapacity: 4, IdleTimeout: time.Hour,
		AllowPeer: func(peerID string) {
			if peerID == "a@example.test" && block.Load() {
				close(entered)
				<-release
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, initial, []string{"a@example.test", "b@example.test"}); err != nil {
		t.Fatal(err)
	}
	var existingRan, existingCleared atomic.Int32
	existing, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &existingRan, &existingCleared))
	if err != nil {
		t.Fatal(err)
	}
	if err = existing.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallSnapshot(session, []string{"a@example.test", "b@example.test"}); err != nil {
		t.Fatal(err)
	}
	block.Store(true)
	published := make(chan error, 1)
	go func() { published <- registry.Publish(session) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("allow callback did not start")
	}

	var createdRan, createdCleared atomic.Int32
	created, err := registry.AdmitOutbound(context.Background(), "b@example.test", testWork(WorkOutbound, 1, &createdRan, &createdCleared))
	if err != nil {
		t.Fatal(err)
	}
	if createdRan.Load() != 0 {
		t.Fatal("lane created during synchronization ran before publication")
	}
	close(release)
	if err = <-published; err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = created.Wait(waitCtx); err != nil {
		t.Fatalf("new lane remained paused after publication: %v", err)
	}
	if createdRan.Load() != 1 || createdCleared.Load() != 1 {
		t.Fatalf("created lane ran=%d cleared=%d", createdRan.Load(), createdCleared.Load())
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRejectsPeerExcludedByInstalledSnapshotBeforePublish(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	initial, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, initial, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}

	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallSnapshot(session, nil); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	if _, err = registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &ran, &cleared)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("excluded peer admission=%v", err)
	}
	if ran.Load() != 0 || cleared.Load() != 0 || registry.ActiveLaneCount() != 0 {
		t.Fatalf("excluded peer state ran=%d cleared=%d lanes=%d", ran.Load(), cleared.Load(), registry.ActiveLaneCount())
	}
	if err = registry.Publish(session); err != nil {
		t.Fatalf("excluded peer admission poisoned publication: %v", err)
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryEpochAdmissionRejectsRemoveReAddABAWithoutLaneAllocation(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	initial, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, initial, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	firstEpoch, ok := registry.PeerEpoch("a@example.test")
	if !ok {
		t.Fatal("initial epoch missing")
	}
	retained, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, retained, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if retainedEpoch, ok := registry.PeerEpoch("a@example.test"); !ok || retainedEpoch != firstEpoch {
		t.Fatalf("retained epoch=(%d,%t) want=%d", retainedEpoch, ok, firstEpoch)
	}
	removed, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, removed, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.PeerEpoch("a@example.test"); ok {
		t.Fatal("removed peer retained epoch")
	}
	readded, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, readded, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	secondEpoch, ok := registry.PeerEpoch("a@example.test")
	if !ok || secondEpoch == firstEpoch {
		t.Fatalf("readded epoch=(%d,%t) initial=%d", secondEpoch, ok, firstEpoch)
	}

	var ran, cleared atomic.Int32
	if _, err = registry.AdmitOutboundEpoch(context.Background(), "a@example.test", firstEpoch, testWork(WorkOutbound, 1, &ran, &cleared)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stale epoch admission=%v", err)
	}
	if registry.ActiveLaneCount() != 0 || ran.Load() != 0 || cleared.Load() != 0 {
		t.Fatalf("stale epoch allocated work lanes=%d ran=%d cleared=%d", registry.ActiveLaneCount(), ran.Load(), cleared.Load())
	}
	result, err := registry.AdmitOutboundEpoch(context.Background(), "a@example.test", secondEpoch, testWork(WorkOutbound, 1, &ran, &cleared))
	if err != nil {
		t.Fatalf("fresh epoch admission=%v", err)
	}
	if err = result.Wait(context.Background()); err != nil {
		t.Fatalf("fresh epoch wait=%v", err)
	}
	if ran.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("fresh epoch ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryIdleLaneRetiresAndLaterAdmissionRecreates(t *testing.T) {
	closed := make(chan string, 2)
	registry, err := NewRegistry(RegistryConfig{
		MailboxCapacity: 4, LaneByteCapacity: 32, GlobalCountCapacity: 4,
		GlobalByteCapacity: 32, MemberCapacity: 4, IdleTimeout: 10 * time.Millisecond,
		ClosePeer: func(peerID string) { closed <- peerID },
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if registry.ActiveLaneCount() != 0 {
		t.Fatal("snapshot-only member allocated a lane")
	}
	var ran, cleared atomic.Int32
	for attempt := 1; attempt <= 2; attempt++ {
		result, admitErr := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &ran, &cleared))
		if admitErr != nil {
			t.Fatalf("admit %d: %v", attempt, admitErr)
		}
		if waitErr := result.Wait(context.Background()); waitErr != nil {
			t.Fatalf("wait %d: %v", attempt, waitErr)
		}
		deadline := time.Now().Add(time.Second)
		for registry.ActiveLaneCount() != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if registry.ActiveLaneCount() != 0 {
			t.Fatalf("lane %d did not retire", attempt)
		}
		select {
		case peerID := <-closed:
			if peerID != "a@example.test" {
				t.Fatalf("closed peer=%q", peerID)
			}
		case <-time.After(time.Second):
			t.Fatalf("lane %d did not close peer transport", attempt)
		}
	}
	if ran.Load() != 2 || cleared.Load() != 2 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryDependencyCallbacksDoNotHoldAuthorityLock(t *testing.T) {
	t.Run("fence", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		registry, err := NewRegistry(RegistryConfig{
			MailboxCapacity: 4, LaneByteCapacity: 32, GlobalCountCapacity: 4,
			GlobalByteCapacity: 32, MemberCapacity: 4, IdleTimeout: time.Hour,
			FencePeer: func(peerID string) {
				if peerID == "a@example.test" {
					close(entered)
					<-release
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		initial, _ := registry.BeginSession()
		if err = installAndPublishTestSnapshot(registry, initial, []string{"a@example.test"}); err != nil {
			t.Fatal(err)
		}
		stale, _ := registry.BeginSession()
		installed := make(chan error, 1)
		go func() { installed <- registry.InstallSnapshot(stale, nil) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("fence callback did not start")
		}
		freshDone := make(chan *AuthoritySession, 1)
		go func() {
			fresh, _ := registry.BeginSession()
			freshDone <- fresh
		}()
		var fresh *AuthoritySession
		select {
		case fresh = <-freshDone:
		case <-time.After(time.Second):
			t.Fatal("blocking fence held authority lock")
		}
		close(release)
		if err = <-installed; !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("superseded install=%v", err)
		}
		if err = installAndPublishTestSnapshot(registry, fresh, nil); err != nil {
			t.Fatal(err)
		}
		if err = registry.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("allow", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var block atomic.Bool
		registry, err := NewRegistry(RegistryConfig{
			MailboxCapacity: 4, LaneByteCapacity: 32, GlobalCountCapacity: 4,
			GlobalByteCapacity: 32, MemberCapacity: 4, IdleTimeout: time.Hour,
			AllowPeer: func(peerID string) {
				if peerID == "a@example.test" && block.Load() {
					close(entered)
					<-release
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		initial, _ := registry.BeginSession()
		if err = installAndPublishTestSnapshot(registry, initial, []string{"a@example.test"}); err != nil {
			t.Fatal(err)
		}
		stale, _ := registry.BeginSession()
		if err = registry.InstallSnapshot(stale, []string{"a@example.test"}); err != nil {
			t.Fatal(err)
		}
		block.Store(true)
		published := make(chan error, 1)
		go func() { published <- registry.Publish(stale) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("allow callback did not start")
		}
		freshDone := make(chan *AuthoritySession, 1)
		go func() {
			fresh, _ := registry.BeginSession()
			freshDone <- fresh
		}()
		var fresh *AuthoritySession
		select {
		case fresh = <-freshDone:
		case <-time.After(time.Second):
			t.Fatal("blocking allow held authority lock")
		}
		close(release)
		if err = <-published; !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("superseded publish=%v", err)
		}
		block.Store(false)
		if err = installAndPublishTestSnapshot(registry, fresh, []string{"a@example.test"}); err != nil {
			t.Fatal(err)
		}
		if err = registry.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRegistrySnapshotIsLazyAndResumesPausedAuthenticatedWork(t *testing.T) {
	registry := newTestRegistry(t, 8, 64, nil)
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test", "b@example.test"}); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveLaneCount(); got != 0 {
		t.Fatalf("snapshot eagerly created %d lanes", got)
	}
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	result, err := registry.AdmitAuthenticatedInbound(context.Background(), "a@example.test", testWork(WorkInbound, 7, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("paused work ran=%d", got)
	}
	fresh, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err := installAndPublishTestSnapshot(registry, fresh, []string{"a@example.test", "b@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	if count, bytes := registry.Usage(); count != 0 || bytes != 0 {
		t.Fatalf("usage=(%d,%d)", count, bytes)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryCompleteSnapshotFencesMultipleAbsentPeersBeforeResume(t *testing.T) {
	closed := make(chan string, 2)
	registry := newTestRegistry(t, 8, 64, func(peer string) { closed <- peer })
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test", "b@example.test", "c@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	a, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 3, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	b, err := registry.AdmitAuthenticatedInbound(context.Background(), "b@example.test", testWork(WorkInbound, 5, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	c, err := registry.AdmitOutbound(context.Background(), "c@example.test", testWork(WorkOutbound, 7, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, fresh, []string{"c@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Wait(context.Background()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("a result=%v", err)
	}
	if err := b.Wait(context.Background()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("b result=%v", err)
	}
	if err := c.Wait(context.Background()); err != nil {
		t.Fatalf("c result=%v", err)
	}
	if ran.Load() != 1 || cleared.Load() != 3 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	seen := map[string]int{}
	for range 2 {
		select {
		case peer := <-closed:
			seen[peer]++
		case <-time.After(time.Second):
			t.Fatal("removed lane did not close")
		}
	}
	if seen["a@example.test"] != 1 || seen["b@example.test"] != 1 {
		t.Fatalf("closed=%v", seen)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRunsWholeBatchFenceBeforeAnyPresentLaneResumes(t *testing.T) {
	var fenced atomic.Int32
	registry, err := NewRegistry(RegistryConfig{
		MailboxCapacity: 8, LaneByteCapacity: 64, GlobalCountCapacity: 8,
		GlobalByteCapacity: 64, MemberCapacity: 8, IdleTimeout: time.Hour,
		FencePeer: func(string) { fenced.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test", "b@example.test", "c@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	var cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "c@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 1,
		Run: func(_ context.Context, commit Commit) error {
			return commit(func() error {
				if got := fenced.Load(); got != 2 {
					return errors.New("allowed lane resumed before removal batch fenced")
				}
				return nil
			})
		},
		Clear: func() { cleared.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, fresh, []string{"c@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fenced.Load() != 2 || cleared.Load() != 1 {
		t.Fatalf("fenced=%d cleared=%d", fenced.Load(), cleared.Load())
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRetiredSessionCannotOverwriteFreshSnapshot(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	old, _ := registry.BeginSession()
	fresh, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, fresh, []string{"current@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := installAndPublishTestSnapshot(registry, old, []string{"old@example.test"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stale install=%v", err)
	}
	if err := registry.Authorize("current@example.test"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Authorize("old@example.test"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old authority=%v", err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryGlobalBudgetBoundsIndependentPausedLanes(t *testing.T) {
	registry := newTestRegistry(t, 2, 10, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test", "b@example.test", "c@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	if _, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 5, &ran, &cleared)); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.AdmitOutbound(context.Background(), "b@example.test", testWork(WorkOutbound, 5, &ran, &cleared)); err != nil {
		t.Fatal(err)
	}
	third := testWork(WorkOutbound, 1, &ran, &cleared)
	if _, err := registry.AdmitOutbound(context.Background(), "c@example.test", third); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third admission=%v", err)
	}
	if cleared.Load() != 0 {
		t.Fatal("failed admission stole caller-owned work")
	}
	if count, bytes := registry.Usage(); count != 2 || bytes != 10 {
		t.Fatalf("usage=(%d,%d)", count, bytes)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count, bytes := registry.Usage(); count != 0 || bytes != 0 {
		t.Fatalf("closed usage=(%d,%d)", count, bytes)
	}
}

func TestLanePauseCancelsInFlightAndAbsentSnapshotWinsOnce(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var enterOnce sync.Once
	var cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 9,
		Run: func(ctx context.Context, commit Commit) error {
			enterOnce.Do(func() { close(entered) })
			<-ctx.Done()
			// Commit is deliberately the final external transfer. The absent
			// snapshot must fence it after canceling preparation.
			return commit(func() error { return nil })
		},
		Clear: func() { cleared.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	fresh, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, fresh, nil); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("result=%v", err)
	}
	if cleared.Load() != 1 {
		t.Fatalf("clear count=%d", cleared.Load())
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPausedCompletedWorkResumesWithoutRepeatingExternalCommit(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	committed := make(chan struct{})
	returnGate := make(chan struct{})
	var effects, cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 3,
		Run: func(_ context.Context, commit Commit) error {
			if err := commit(func() error {
				effects.Add(1)
				close(committed)
				return nil
			}); err != nil {
				return err
			}
			<-returnGate
			return nil
		},
		Clear: func() { cleared.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	<-committed
	if err := registry.Pause(session); err != nil {
		t.Fatal(err)
	}
	close(returnGate)
	fresh, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, fresh, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 1 || cleared.Load() != 1 {
		t.Fatalf("effects=%d cleared=%d", effects.Load(), cleared.Load())
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLaneCannotReportSuccessWithoutCurrentMembershipCommit(t *testing.T) {
	registry := newTestRegistry(t, 2, 16, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	var cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 4,
		Run:   func(context.Context, Commit) error { return nil },
		Clear: func() { cleared.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("result=%v", err)
	}
	if cleared.Load() != 1 {
		t.Fatalf("clear count=%d", cleared.Load())
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedBlockingEffectDoesNotHoldMembershipFence(t *testing.T) {
	registry := newTestRegistry(t, 2, 16, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 4,
		Run: func(_ context.Context, commit Commit) error {
			return commit(func() error {
				close(entered)
				<-release
				return nil
			})
		},
		Clear: func() {},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	paused := make(chan error, 1)
	go func() { paused <- registry.Pause(session) }()
	select {
	case err := <-paused:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("membership fence blocked behind committed dependency effect")
	}
	close(release)
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRemovalWinsPostCommitRecheckWithoutWaitingForBlockingEffect(t *testing.T) {
	registry := newTestRegistry(t, 2, 16, nil)
	session, _ := registry.BeginSession()
	if err := installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", Work{
		Kind: WorkOutbound, OwnedBytes: 4,
		Run: func(_ context.Context, commit Commit) error {
			return commit(func() error {
				close(entered)
				<-release
				return nil
			})
		},
		Clear: func() {},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	fresh, _ := registry.BeginSession()
	installed := make(chan error, 1)
	go func() { installed <- installAndPublishTestSnapshot(registry, fresh, nil) }()
	select {
	case err := <-installed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("removal blocked behind dependency effect")
	}
	close(release)
	if err := result.Wait(context.Background()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("result=%v", err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPausedLaneCallerDeadlineContinues(t *testing.T) {
	registry := newTestRegistry(t, 2, 16, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var ran, cleared atomic.Int32
	result, err := registry.AdmitOutbound(ctx, "a@example.test", testWork(WorkOutbound, 4, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result=%v", err)
	}
	if ran.Load() != 0 || cleared.Load() != 1 {
		t.Fatalf("ran=%d cleared=%d", ran.Load(), cleared.Load())
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryCloseReplaysOneComponentOwnedJoinAfterCallerTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var closes atomic.Int32
	registry := newTestRegistry(t, 2, 16, func(string) {
		closes.Add(1)
		close(entered)
		<-release
	})
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, session, []string{"a@example.test"}); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), "a@example.test", testWork(WorkOutbound, 1, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	first, cancelFirst := context.WithTimeout(context.Background(), time.Second)
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() { firstResult <- registry.Close(first) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("peer close did not start")
	}
	cancelFirst()
	if err = <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first close=%v", err)
	}

	second, cancelSecond := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSecond()
	if err = registry.Close(second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated close reported completion early: %v", err)
	}
	close(release)
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatalf("peer close count=%d", closes.Load())
	}
}

func TestRegistryGlobalFenceBlocksEveryLaneCommitBeforePerLanePause(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	session, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	members := []string{"a@example.test", "b@example.test"}
	if err = installAndPublishTestSnapshot(registry, session, members); err != nil {
		t.Fatal(err)
	}
	prepared := make(chan struct{})
	release := make(chan struct{})
	effect := make(chan struct{}, 1)
	result, err := registry.AdmitOutbound(context.Background(), members[1], Work{
		Kind: WorkOutbound, OwnedBytes: 1,
		Run: func(_ context.Context, commit Commit) error {
			select {
			case prepared <- struct{}{}:
			default:
			}
			<-release
			return commit(func() error {
				effect <- struct{}{}
				return nil
			})
		},
		Clear: func() {},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-prepared
	registry.BlockAdmission()
	close(release)
	select {
	case <-effect:
		t.Fatal("peer effect crossed the global membership fence")
	case <-time.After(20 * time.Millisecond):
	}
	fresh, err := registry.BeginSession()
	if err != nil {
		t.Fatal(err)
	}
	if err = installAndPublishTestSnapshot(registry, fresh, members); err != nil {
		t.Fatal(err)
	}
	if err = result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-effect:
	default:
		t.Fatal("authorized work did not resume after exact publication")
	}
	if err = registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
