package handshake

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type testNegotiator struct {
	mu                sync.Mutex
	establish         func(context.Context, Attempt) error
	accept            func(context.Context, Attempt, Signal) error
	accepted, applied int
}

func (n *testNegotiator) Establish(ctx context.Context, a Attempt) error {
	if n.establish != nil {
		return n.establish(ctx, a)
	}
	return nil
}
func (n *testNegotiator) Accept(ctx context.Context, attempt Attempt, signal Signal) error {
	n.mu.Lock()
	n.accepted++
	n.mu.Unlock()
	if n.accept != nil {
		return n.accept(ctx, attempt, signal)
	}
	return nil
}
func (n *testNegotiator) Apply(context.Context, Attempt, Signal) error {
	n.mu.Lock()
	n.applied++
	n.mu.Unlock()
	return nil
}

func testConfig(now *time.Time) Config {
	return Config{LocalIdentity: "a", MaximumPeers: 4, MaximumAttempts: 16, AttemptTimeout: time.Second, CooldownInitial: time.Second, CooldownMaximum: 4 * time.Second, Clock: transport.ClockFunc(func() time.Time { return *now }), Random: bytes.NewReader(make([]byte, 16*32))}
}

func TestSingleFlightTimeoutCooldownAndRecovery(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	entered := make(chan struct{})
	release := make(chan struct{})
	engine := &testNegotiator{establish: func(ctx context.Context, _ Attempt) error {
		close(entered)
		select {
		case <-release:
			return ErrFailed
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	m, err := NewManager(testConfig(&now), engine)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := m.Start(context.Background(), "b"); done <- err }()
	<-entered
	if _, err := m.Start(context.Background(), "b"); !errors.Is(err, ErrInFlight) {
		t.Fatalf("second start = %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrFailed) {
		t.Fatalf("first = %v", err)
	}
	if _, err := m.Start(context.Background(), "b"); !errors.Is(err, ErrCooldown) {
		t.Fatalf("cooldown = %v", err)
	}
	now = now.Add(time.Second)
	engine.establish = nil
	if attempt, err := m.Start(context.Background(), "b"); err != nil || attempt.State != AttemptSucceeded {
		t.Fatalf("recovery = %#v, %v", attempt, err)
	}
}

func TestGlareStaleAndPanicContainment(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	engine := &testNegotiator{}
	m, err := NewManager(testConfig(&now), engine)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := NewAttempt("a", "b", now, time.Second, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.HandleSignal(context.Background(), Signal{Attempt: incoming, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := m.HandleSignal(context.Background(), Signal{Attempt: incoming, Payload: []byte("x")}); !errors.Is(err, ErrStale) {
		t.Fatalf("duplicate = %v", err)
	}
	panicEngine := &testNegotiator{establish: func(context.Context, Attempt) error { panic("dependency") }}
	m2, _ := NewManager(testConfig(&now), panicEngine)
	if _, err := m2.Start(context.Background(), "c"); !errors.Is(err, ErrFailed) {
		t.Fatalf("panic = %v", err)
	}
	now = now.Add(2 * time.Second)
	if err := m.HandleSignal(context.Background(), Signal{Attempt: incoming, Payload: []byte("x")}); !errors.Is(err, ErrStale) {
		t.Fatalf("expired = %v", err)
	}
}

func TestRemoteGlareWinnerWaitsForCancelledLocalFlight(t *testing.T) {
	now := time.Unix(250, 0).UTC()
	establishEntered := make(chan struct{})
	cancelObserved := make(chan struct{})
	releaseCleanup := make(chan struct{})
	establishExited := make(chan struct{})
	acceptEntered := make(chan struct{})
	engine := &testNegotiator{
		establish: func(ctx context.Context, _ Attempt) error {
			close(establishEntered)
			<-ctx.Done()
			close(cancelObserved)
			<-releaseCleanup
			close(establishExited)
			return ctx.Err()
		},
		accept: func(context.Context, Attempt, Signal) error {
			select {
			case <-establishExited:
			default:
				return errors.New("Accept overlapped the cancelled Establish operation")
			}
			close(acceptEntered)
			return nil
		},
	}
	config := testConfig(&now)
	config.LocalIdentity = "z"
	m, err := NewManager(config, engine)
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() {
		_, startErr := m.Start(context.Background(), "a")
		startDone <- startErr
	}()
	<-establishEntered

	remote, err := NewAttempt("z", "a", now, time.Second, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	acceptDone := make(chan error, 1)
	go func() {
		acceptDone <- m.HandleSignal(context.Background(), Signal{Attempt: remote, Payload: []byte("jingle")})
	}()
	<-cancelObserved
	select {
	case <-acceptEntered:
		t.Fatal("Accept began before cancelled Establish released peer resources")
	default:
	}

	close(releaseCleanup)
	if err := <-acceptDone; err != nil {
		t.Fatalf("remote glare winner = %v", err)
	}
	if err := <-startDone; !errors.Is(err, ErrStale) {
		t.Fatalf("cancelled local flight = %v, want %v", err, ErrStale)
	}
}

func TestAttemptIdentityAndGlareWinner(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	a, _ := NewAttempt("b", "a", now, time.Second, bytes.NewReader(make([]byte, 16)))
	b, _ := NewAttempt("a", "b", now, time.Second, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	if got := ResolveGlare(a, b); got.ID != a.ID {
		t.Fatalf("winner = %s", got.ID)
	}
	if !validAttempt(a) || a.ID == b.ID {
		t.Fatal("attempt identity invalid or not unique")
	}
}

func TestRemoteAttemptLifetimeAndFutureTimestampRejected(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	engine := &testNegotiator{}
	m, err := NewManager(testConfig(&now), engine)
	if err != nil {
		t.Fatal(err)
	}
	makeIncoming := func(start time.Time, timeout time.Duration, value byte) Attempt {
		attempt, attemptErr := NewAttempt("a", "b", start, timeout, bytes.NewReader(bytes.Repeat([]byte{value}, 16)))
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		return attempt
	}
	for name, attempt := range map[string]Attempt{
		"future":        makeIncoming(now.Add(time.Nanosecond), time.Second, 1),
		"long lifetime": makeIncoming(now, time.Second+time.Nanosecond, 2),
	} {
		t.Run(name, func(t *testing.T) {
			if err := m.HandleSignal(context.Background(), Signal{Attempt: attempt, Payload: []byte("jingle")}); !errors.Is(err, ErrStale) {
				t.Fatalf("HandleSignal() = %v", err)
			}
		})
	}
	engine.mu.Lock()
	accepted := engine.accepted
	engine.mu.Unlock()
	if accepted != 0 {
		t.Fatalf("accepted %d invalid remote attempts", accepted)
	}
}

func TestRemoteFailureCooldownCannotBeBypassedByNewAttempt(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	engine := &testNegotiator{accept: func(context.Context, Attempt, Signal) error { return ErrFailed }}
	m, err := NewManager(testConfig(&now), engine)
	if err != nil {
		t.Fatal(err)
	}
	remote := func(value byte) Signal {
		attempt, attemptErr := NewAttempt("a", "b", now, time.Second, bytes.NewReader(bytes.Repeat([]byte{value}, 16)))
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		return Signal{Attempt: attempt, Payload: []byte("jingle")}
	}
	if err := m.HandleSignal(context.Background(), remote(1)); !errors.Is(err, ErrFailed) {
		t.Fatalf("first HandleSignal() = %v", err)
	}
	if err := m.HandleSignal(context.Background(), remote(2)); !errors.Is(err, ErrCooldown) {
		t.Fatalf("second HandleSignal() = %v", err)
	}
	engine.mu.Lock()
	accepted := engine.accepted
	engine.mu.Unlock()
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
}

func TestFailedResponderCleanupPastDeadlineDoesNotExtendManagerCooldown(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	entered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	engine := &testNegotiator{}
	engine.accept = func(context.Context, Attempt, Signal) error {
		engine.mu.Lock()
		accepted := engine.accepted
		engine.mu.Unlock()
		if accepted == 1 {
			close(entered)
			<-releaseCleanup
			return ErrFailed
		}
		return nil
	}
	config := testConfig(&now)
	manager, err := NewManager(config, engine)
	if err != nil {
		t.Fatal(err)
	}
	remote := func(start time.Time, value byte) Signal {
		attempt, attemptErr := NewAttempt("a", "b", start, config.AttemptTimeout, bytes.NewReader(bytes.Repeat([]byte{value}, 16)))
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		return Signal{Attempt: attempt, Payload: []byte("jingle")}
	}

	first := remote(now, 1)
	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.HandleSignal(context.Background(), first) }()
	<-entered

	// By the time cleanup returns, both the attempt deadline and the cooldown
	// measured from that deadline have elapsed. SignalPump queue ownership is
	// covered by the rank1webrtc integration test.
	now = first.Attempt.Deadline.Add(config.CooldownInitial)
	replacement := remote(now, 2)
	replacementDone := make(chan error, 1)
	go func() {
		if firstErr := <-firstDone; !errors.Is(firstErr, ErrFailed) {
			replacementDone <- errors.New("first responder attempt did not fail")
			return
		}
		replacementDone <- manager.HandleSignal(context.Background(), replacement)
	}()
	close(releaseCleanup)
	if err = <-replacementDone; err != nil {
		t.Fatalf("post-cleanup replacement = %v", err)
	}
	engine.mu.Lock()
	accepted := engine.accepted
	engine.mu.Unlock()
	if accepted != 2 {
		t.Fatalf("Accept calls = %d, want 2", accepted)
	}
}

func TestFailedAttemptCooldownUsesExactBoundaryAndPreDeadlineCompletion(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	fail := true
	engine := &testNegotiator{accept: func(context.Context, Attempt, Signal) error {
		if fail {
			return ErrFailed
		}
		return nil
	}}
	config := testConfig(&now)
	manager, err := NewManager(config, engine)
	if err != nil {
		t.Fatal(err)
	}
	remote := func(value byte) Signal {
		attempt, attemptErr := NewAttempt("a", "b", now, config.AttemptTimeout, bytes.NewReader(bytes.Repeat([]byte{value}, 16)))
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		return Signal{Attempt: attempt, Payload: []byte("jingle")}
	}
	if err = manager.HandleSignal(context.Background(), remote(1)); !errors.Is(err, ErrFailed) {
		t.Fatalf("first failure = %v", err)
	}

	// Cleanup completed before the attempt deadline, so ordinary cooldown is
	// measured from the observed completion time. Ready is false immediately
	// before the boundary and true at the exact boundary.
	now = now.Add(config.CooldownInitial - time.Nanosecond)
	if err = manager.HandleSignal(context.Background(), remote(2)); !errors.Is(err, ErrCooldown) {
		t.Fatalf("pre-boundary replacement = %v", err)
	}
	now = now.Add(time.Nanosecond)
	fail = false
	if err = manager.HandleSignal(context.Background(), remote(3)); err != nil {
		t.Fatalf("boundary replacement = %v", err)
	}
}
