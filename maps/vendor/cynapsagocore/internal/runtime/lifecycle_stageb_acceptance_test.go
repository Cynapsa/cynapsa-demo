package runtime

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestStageBAcceptanceCallerTimeoutDoesNotPoisonSharedCleanup(t *testing.T) {
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	var stopCalls atomic.Int32
	component := &testComponent{name: "blocked", stop: func(context.Context) error {
		stopCalls.Add(1)
		close(stopEntered)
		<-releaseStop
		return nil
	}}
	r := testRuntime(t, 16, Dependencies{Clock: coreclock.NewFake(time.Unix(100, 0)), Components: []Component{component}})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- r.Shutdown(firstCtx) }()
	<-stopEntered
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if got := r.Status().Lifecycle; got != stateClosing {
		t.Fatalf("status after caller timeout = %q, want closing", got)
	}

	const joiners = 12
	joined := make(chan error, joiners)
	for range joiners {
		go func() { joined <- r.Shutdown(context.Background()) }()
	}
	select {
	case err := <-joined:
		t.Fatalf("join completed before owned cleanup: %v", err)
	default:
	}

	close(releaseStop)
	drainStageBEventsUntilClosed(t, r)
	for range joiners {
		if err := <-joined; err != nil {
			t.Fatalf("later Shutdown() error = %v", err)
		}
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("component stop count = %d, want exactly one", got)
	}
	if got := r.Status().Lifecycle; got != stateClosed {
		t.Fatalf("final status = %q, want closed", got)
	}
	if !r.gate.Stats().Closed || !r.events.Stats().Closed {
		t.Fatalf("resources not closed: gate=%+v events=%+v", r.gate.Stats(), r.events.Stats())
	}
}

func TestStageBAcceptanceOwnedCleanupDeadlineClosesBoundedOutput(t *testing.T) {
	stopEntered := make(chan struct{})
	component := &testComponent{name: "deadline", stop: func(ctx context.Context) error {
		close(stopEntered)
		<-ctx.Done()
		return ctx.Err()
	}}
	config := testConfig(1)
	config.CleanupTimeout = 15 * time.Millisecond
	r, err := NewWithDependencies(config, testGate(t, 1), Dependencies{
		Clock:      coreclock.NewFake(time.Unix(200, 0)),
		Components: []Component{component},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.PublishEvent(context.Background(), model.Event{
		ID: "occupied", Name: "core.error", CreatedAt: time.Unix(201, 0), Value: model.CoreErrorEvent{},
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Shutdown(context.Background()) }()
	<-stopEntered
	if err := <-done; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrComponentStopFailed) {
		t.Fatalf("Shutdown() error = %v, want owned deadline and component failure", err)
	}
	if got := r.Status().Lifecycle; got != stateClosed {
		t.Fatalf("status = %q, want closed", got)
	}
	if stats := r.events.Stats(); !stats.Closed || stats.Depth != 0 {
		t.Fatalf("event output was not abandoned at owned deadline: %+v", stats)
	}
	if stats := r.gate.Stats(); !stats.Closed || stats.RegistryEntries != 0 {
		t.Fatalf("gate was not finalized at owned deadline: %+v", stats)
	}
}

func TestStageBAcceptanceLateStartRollsBackExactlyOnceBeforeClosed(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	recorder := &callRecorder{}
	components := []Component{
		&testComponent{name: "first", calls: recorder},
		&testComponent{name: "late", calls: recorder, start: func(context.Context) error {
			close(entered)
			<-release
			return nil
		}},
		&testComponent{name: "must-not-start", calls: recorder},
	}
	r := testRuntime(t, 16, Dependencies{Clock: coreclock.NewFake(time.Unix(300, 0)), Components: components})
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(context.Background()) }()
	<-entered

	r.BeginShutdown()
	if got := r.Status().Lifecycle; got != stateClosing {
		t.Fatalf("status during noncooperative Start = %q, want closing", got)
	}
	select {
	case <-r.shutdownDone:
		t.Fatal("cleanup completed while component Start still owned activity")
	default:
	}
	close(release)
	if err := <-startDone; !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrClosing) {
		t.Fatalf("Start() error = %v; calls=%v", err, recorder.snapshot())
	}
	drainStageBEventsUntilClosed(t, r)
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	want := []string{"start:first", "start:late", "stop:late", "stop:first"}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("component calls = %v, want %v", got, want)
	}
	for range 64 {
		runtime.Gosched()
	}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("late component activity after closed: %v", got)
	}
}

func TestStageBAcceptanceComponentStartAdmissionLinearizesWithShutdown(t *testing.T) {
	const iterations = 100
	for iteration := range iterations {
		var r *Runtime
		beginReturned := make(chan struct{})
		nextStarted := make(chan struct{}, 1)
		var initiatorStops atomic.Int32
		components := []Component{
			&testComponent{name: "initiator", start: func(context.Context) error {
				r.BeginShutdown()
				close(beginReturned)
				return nil
			}, stop: func(context.Context) error {
				initiatorStops.Add(1)
				return nil
			}},
			&testComponent{name: "must-not-start", start: func(context.Context) error {
				nextStarted <- struct{}{}
				return nil
			}},
		}
		var err error
		r, err = NewWithDependencies(testConfig(8), testGate(t, 8), Dependencies{
			Clock: coreclock.NewFake(time.Unix(350+int64(iteration), 0)), Components: components,
		})
		if err != nil {
			t.Fatal(err)
		}
		startDone := make(chan error, 1)
		go func() { startDone <- r.Start(context.Background()) }()
		<-beginReturned
		if err := <-startDone; !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrClosing) {
			t.Fatalf("iteration %d Start() error = %v", iteration, err)
		}
		select {
		case <-nextStarted:
			t.Fatalf("iteration %d admitted component after BeginShutdown returned", iteration)
		default:
		}
		drainStageBEventsUntilClosed(t, r)
		if err := r.Shutdown(context.Background()); err != nil {
			t.Fatalf("iteration %d Shutdown() error = %v", iteration, err)
		}
		if got := initiatorStops.Load(); got != 1 {
			t.Fatalf("iteration %d initiator Stop calls = %d, want 1", iteration, got)
		}
	}
}

func TestStageBAcceptanceNoncooperativeStopCannotPublishClosedEarly(t *testing.T) {
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	var stopCalls atomic.Int32
	component := &testComponent{name: "late-stop", stop: func(context.Context) error {
		stopCalls.Add(1)
		close(stopEntered)
		<-releaseStop
		return nil
	}}
	config := testConfig(8)
	config.CleanupTimeout = time.Nanosecond
	r, err := NewWithDependencies(config, testGate(t, 8), Dependencies{
		Clock: coreclock.NewFake(time.Unix(400, 0)), Components: []Component{component},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Shutdown(context.Background()) }()
	<-stopEntered
	if got := r.Status().Lifecycle; got != stateClosing {
		t.Fatalf("status during late Stop = %q, want closing", got)
	}
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while Stop remained active: %v", err)
	default:
	}
	close(releaseStop)
	if err := <-done; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want owned timeout after late Stop", err)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("Stop calls = %d, want one", got)
	}
	if got := r.Status().Lifecycle; got != stateClosed {
		t.Fatalf("final status = %q, want closed", got)
	}
}

func TestStageBAcceptanceClosedStatusPrecedesClosedEvent(t *testing.T) {
	clock := newStageBBarrierClock(time.Unix(500, 0), 2)
	r := testRuntime(t, 4, Dependencies{Clock: clock})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Shutdown(context.Background()) }()
	<-clock.entered
	if got := r.Status().Lifecycle; got != stateClosed {
		t.Fatalf("status while closed-event construction is blocked = %q, want closed", got)
	}
	close(clock.release)

	var observed []model.LifecycleState
	for {
		event, err := r.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		change, ok := event.Value.(model.SessionStateChangedEvent)
		if !ok {
			t.Fatalf("unexpected event value %T", event.Value)
		}
		observed = append(observed, change.Current)
		if change.Current == stateClosed && r.Status().Lifecycle != stateClosed {
			t.Fatalf("closed event preceded closed status: %+v", r.Status())
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if want := []model.LifecycleState{stateClosing, stateClosed}; !reflect.DeepEqual(observed, want) {
		t.Fatalf("lifecycle events = %v, want %v", observed, want)
	}
}

func TestStageBAcceptanceAuthenticatedCommitFailuresLeaveNoPartialGraph(t *testing.T) {
	tests := []struct {
		name      string
		publisher func(*atomic.Bool) AuthenticatedPublisher
		want      error
	}{
		{
			name: "error",
			publisher: func(visible *atomic.Bool) AuthenticatedPublisher {
				return func(func() error) error {
					visible.Store(true)
					defer visible.Store(false)
					return errors.New("injected publisher error")
				}
			},
			want: ErrAuthenticatedCommit,
		},
		{
			name: "panic",
			publisher: func(visible *atomic.Bool) AuthenticatedPublisher {
				return func(func() error) error {
					visible.Store(true)
					defer visible.Store(false)
					panic("secret panic value")
				}
			},
			want: ErrPublisherPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := testRuntime(t, 8, Dependencies{Clock: coreclock.NewFake(time.Unix(600, 0))})
			if err := r.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			var visible atomic.Bool
			err := r.CommitAuthenticated(context.Background(), tc.publisher(&visible))
			if !errors.Is(err, tc.want) {
				t.Fatalf("CommitAuthenticated() error = %v, want %v", err, tc.want)
			}
			if visible.Load() {
				t.Fatal("failed publisher left staged graph visible")
			}
			if got := r.Status().Lifecycle; got != stateCreated {
				t.Fatalf("status after failed publisher = %q, want created", got)
			}
			if stats := r.events.Stats(); stats.Depth != 0 {
				t.Fatalf("failed publisher emitted lifecycle observations: %+v", stats)
			}
			forceShutdown(t, r)
		})
	}
}

func TestStageBAcceptanceAuthenticatedCommitCancellationLeavesNoPartialGraph(t *testing.T) {
	r := testRuntime(t, 8, Dependencies{Clock: coreclock.NewFake(time.Unix(650, 0))})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var visible atomic.Bool
	err := r.CommitAuthenticated(ctx, func(commitReady func() error) error {
		visible.Store(true)
		defer visible.Store(false)
		cancel()
		return commitReady()
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrAuthenticatedCommit) {
		t.Fatalf("CommitAuthenticated() error = %v, want cancelled authenticated commit", err)
	}
	if visible.Load() || r.Status().Lifecycle != stateCreated || r.events.Stats().Depth != 0 {
		t.Fatalf("cancelled commit left partial visibility: graph=%v status=%+v events=%+v", visible.Load(), r.Status(), r.events.Stats())
	}
	forceShutdown(t, r)
}

func TestStageBAcceptanceAuthenticatedCommitShutdownAndRetryBoundaries(t *testing.T) {
	t.Run("shutdown already owns lifecycle", func(t *testing.T) {
		r := testRuntime(t, 8, Dependencies{Clock: coreclock.NewFake(time.Unix(700, 0))})
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		r.BeginShutdown()
		var called atomic.Bool
		err := r.CommitAuthenticated(context.Background(), func(func() error) error {
			called.Store(true)
			return nil
		})
		if !errors.Is(err, ErrClosing) || called.Load() {
			t.Fatalf("CommitAuthenticated after shutdown = %v, publisher called=%v", err, called.Load())
		}
		drainStageBEventsUntilClosed(t, r)
		if err := r.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed transaction can retry", func(t *testing.T) {
		r := testRuntime(t, 8, Dependencies{Clock: coreclock.NewFake(time.Unix(800, 0))})
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.CommitAuthenticated(context.Background(), func(func() error) error {
			panic("injected")
		}); !errors.Is(err, ErrPublisherPanic) {
			t.Fatalf("first commit error = %v", err)
		}
		var graphVisible atomic.Bool
		if err := r.CommitAuthenticated(context.Background(), func(commitReady func() error) error {
			graphVisible.Store(true)
			if err := commitReady(); err != nil {
				graphVisible.Store(false)
				return err
			}
			return nil
		}); err != nil {
			t.Fatalf("retry error = %v", err)
		}
		if !graphVisible.Load() || r.Status().Lifecycle != stateReady {
			t.Fatalf("retry did not atomically publish graph and ready: visible=%v status=%+v", graphVisible.Load(), r.Status())
		}
		forceShutdown(t, r)
	})
}

func TestStageBAcceptanceQueueOneAuthenticationOverflowIsBounded(t *testing.T) {
	r := testRuntime(t, 1, Dependencies{Clock: coreclock.NewFake(time.Unix(900, 0))})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var graphVisible atomic.Bool
	if err := r.CommitAuthenticated(context.Background(), func(commitReady func() error) error {
		graphVisible.Store(true)
		if err := commitReady(); err != nil {
			graphVisible.Store(false)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !graphVisible.Load() || r.Status().Lifecycle != stateReady {
		t.Fatalf("commit visibility mismatch: graph=%v status=%+v", graphVisible.Load(), r.Status())
	}
	event, err := r.NextEvent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	change, ok := event.Value.(model.SessionStateChangedEvent)
	if !ok || change.Previous != stateCreated || change.Current != stateAuthenticating {
		t.Fatalf("retained event = %#v", event.Value)
	}
	metrics := r.Diagnostics().Metrics
	if got := metrics[diagnostics.MetricEventsPublished]; got != 1 {
		t.Fatalf("published metric = %v, want 1", got)
	}
	if got := metrics[diagnostics.MetricEventsRejected]; got != float64(len(authenticatedPath)-1) {
		t.Fatalf("rejected metric = %v, want %d", got, len(authenticatedPath)-1)
	}
	forceShutdown(t, r)
}

func drainStageBEventsUntilClosed(t *testing.T, r *Runtime) {
	t.Helper()
	for {
		_, err := r.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			return
		}
		if err != nil {
			t.Fatalf("NextEvent() error = %v", err)
		}
	}
}

type stageBBarrierClock struct {
	base    *coreclock.Fake
	entered chan struct{}
	release chan struct{}
	blockOn uint64
	calls   atomic.Uint64
	once    sync.Once
}

func newStageBBarrierClock(now time.Time, blockOn uint64) *stageBBarrierClock {
	return &stageBBarrierClock{
		base: coreclock.NewFake(now), entered: make(chan struct{}), release: make(chan struct{}), blockOn: blockOn,
	}
}

func (c *stageBBarrierClock) Now() time.Time {
	if c.calls.Add(1) == c.blockOn {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.base.Now()
}

func (c *stageBBarrierClock) After(duration time.Duration) <-chan time.Time {
	return c.base.After(duration)
}

func (c *stageBBarrierClock) NewTimer(duration time.Duration) coreclock.Timer {
	return c.base.NewTimer(duration)
}
