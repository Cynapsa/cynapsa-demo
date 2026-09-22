package runtime

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestStageBConfigRequiresPrivateCleanupTimeout(t *testing.T) {
	config := testConfig(8)
	config.CleanupTimeout = 0
	if runtime, err := New(config, testGate(t, 8)); runtime != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(zero cleanup timeout) = (%v, %v)", runtime, err)
	}
	config.CleanupTimeout = -time.Second
	if runtime, err := New(config, testGate(t, 8)); runtime != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(negative cleanup timeout) = (%v, %v)", runtime, err)
	}
}

func TestStageBShutdownCallerTimeoutLeavesCleanupRunningForLateJoin(t *testing.T) {
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	component := &testComponent{name: "blocked", stop: func(context.Context) error {
		close(stopEntered)
		<-releaseStop
		return nil
	}}
	runtime := testRuntime(t, 8, Dependencies{Components: []Component{component}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- runtime.Shutdown(firstCtx) }()
	<-stopEntered
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if got := runtime.Status().Lifecycle; got != stateClosing {
		t.Fatalf("lifecycle after caller cancellation = %q, want closing", got)
	}

	lateDone := make(chan error, 1)
	go func() { lateDone <- runtime.Shutdown(context.Background()) }()
	select {
	case err := <-lateDone:
		t.Fatalf("late join returned before cleanup: %v", err)
	default:
	}
	close(releaseStop)
	drainRuntimeEvents(t, runtime)
	if err := <-lateDone; err != nil {
		t.Fatalf("late Shutdown() error = %v", err)
	}
	if got := runtime.Status().Lifecycle; got != stateClosed {
		t.Fatalf("final lifecycle = %q, want closed", got)
	}
}

func TestStageBClosedEventNeverPrecedesClosedStatus(t *testing.T) {
	clock := newBlockingClockOn(time.Unix(900, 0), 2)
	runtime := testRuntime(t, 4, Dependencies{Clock: clock})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Shutdown(context.Background()) }()
	<-clock.entered
	if got := runtime.Status().Lifecycle; got != stateClosed {
		t.Fatalf("status while closed event construction is blocked = %q, want closed", got)
	}
	select {
	case err := <-done:
		t.Fatalf("Shutdown() returned before post-cleanup event disposition: %v", err)
	default:
	}
	close(clock.release)
	for index, want := range []model.LifecycleState{stateClosing, stateClosed} {
		event, err := runtime.NextEvent(context.Background())
		if err != nil {
			t.Fatalf("NextEvent(%d) error = %v", index, err)
		}
		change := event.Value.(model.SessionStateChangedEvent)
		if change.Current != want {
			t.Fatalf("event %d current = %q, want %q", index, change.Current, want)
		}
		if want == stateClosed && runtime.Status().Lifecycle != stateClosed {
			t.Fatalf("closed event observed with status %+v", runtime.Status())
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestStageBLifecycleClockPanicCannotStrandShutdown(t *testing.T) {
	runtime := testRuntime(t, 4, Dependencies{Clock: panicClock{}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(context.Background()); !errors.Is(err, ErrLifecycleEvent) {
		t.Fatalf("Shutdown() error = %v, want lifecycle event error", err)
	}
	if runtime.Status().Lifecycle != stateClosed || !runtime.gate.Stats().Closed || !runtime.events.Stats().Closed {
		t.Fatalf("panic cleanup state: status=%+v gate=%+v events=%+v", runtime.Status(), runtime.gate.Stats(), runtime.events.Stats())
	}
}

func TestStageBCleanupDeadlineOwnsAbandonmentAndFinalClose(t *testing.T) {
	stopEntered := make(chan struct{})
	component := &testComponent{name: "deadline", stop: func(ctx context.Context) error {
		close(stopEntered)
		<-ctx.Done()
		return ctx.Err()
	}}
	config := testConfig(2)
	config.CleanupTimeout = 20 * time.Millisecond
	runtime, err := NewWithDependencies(config, testGate(t, 2), Dependencies{Components: []Component{component}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PublishEvent(context.Background(), model.Event{ID: "queued", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{}}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runtime.Shutdown(context.Background()) }()
	<-stopEntered
	if got := runtime.Status().Lifecycle; got != stateClosing {
		t.Fatalf("lifecycle during cleanup = %q, want closing", got)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrComponentStopFailed) {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := runtime.Status().Lifecycle; got != stateClosed {
		t.Fatalf("lifecycle after owned deadline = %q, want closed", got)
	}
	if !runtime.gate.Stats().Closed || !runtime.events.Stats().Closed {
		t.Fatalf("owned resources not closed: gate=%+v events=%+v", runtime.gate.Stats(), runtime.events.Stats())
	}
}

func TestStageBShutdownDeadlineWaitsForLateStartupRollbackExactlyOnce(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	recorder := &callRecorder{}
	components := []Component{
		&testComponent{name: "first", calls: recorder},
		&testComponent{name: "late", calls: recorder, start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		}},
	}
	config := testConfig(8)
	config.CleanupTimeout = 20 * time.Millisecond
	runtime, err := NewWithDependencies(config, testGate(t, 8), Dependencies{Components: components})
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- runtime.Start(context.Background()) }()
	<-startEntered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(context.Background()) }()

	// Let the Runtime-owned cleanup deadline expire while the deliberately
	// non-cooperative Start call still owns its component. Cleanup must remain
	// closing and must not steal or double-stop the earlier component.
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown() completed before late startup rollback: %v", err)
	default:
	}
	if got := runtime.Status().Lifecycle; got != stateClosing {
		t.Fatalf("lifecycle during late Start = %q, want closing", got)
	}
	if got := recorder.snapshot(); len(got) != 2 {
		t.Fatalf("cleanup ran before startup released ownership: %v", got)
	}

	close(releaseStart)
	if err := <-startDone; !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrClosing) {
		t.Fatalf("Start() error = %v", err)
	}
	drainRuntimeEvents(t, runtime)
	if err := <-shutdownDone; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v", err)
	}
	want := []string{"start:first", "start:late", "stop:late", "stop:first"}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("component calls = %v, want exactly once %v", got, want)
	}
	if runtime.Status().Lifecycle != stateClosed {
		t.Fatalf("final lifecycle = %q, want closed", runtime.Status().Lifecycle)
	}
}

func TestStageBBeginShutdownSynchronouslyPreventsLaterComponentStart(t *testing.T) {
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdEntered := make(chan struct{}, 1)
	recorder := &callRecorder{}
	components := []Component{
		&testComponent{name: "first", calls: recorder},
		&testComponent{name: "second", calls: recorder, start: func(context.Context) error {
			close(secondEntered)
			<-releaseSecond
			return nil
		}},
		&testComponent{name: "must-not-start", calls: recorder, start: func(context.Context) error {
			thirdEntered <- struct{}{}
			return nil
		}},
	}
	runtime := testRuntime(t, 8, Dependencies{Components: components})
	startDone := make(chan error, 1)
	go func() { startDone <- runtime.Start(context.Background()) }()
	<-secondEntered

	// BeginShutdown sets shutdown ownership and cancels rootCtx before it
	// returns. Releasing the non-cooperative Start immediately afterward must
	// still force rollback; correctness cannot depend on an AfterFunc goroutine.
	runtime.BeginShutdown()
	close(releaseSecond)
	if err := <-startDone; !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrClosing) {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-thirdEntered:
		t.Fatal("component after the shutdown boundary was started")
	default:
	}
	drainRuntimeEvents(t, runtime)
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("component calls = %v, want %v", got, want)
	}
}

func TestStageBComponentTriggeredShutdownPreventsNextStartAdmission(t *testing.T) {
	var runtime *Runtime
	shutdownReturned := make(chan struct{})
	nextStarted := make(chan struct{}, 1)
	recorder := &callRecorder{}
	components := []Component{
		&testComponent{name: "initiator", calls: recorder, start: func(context.Context) error {
			runtime.BeginShutdown()
			close(shutdownReturned)
			return nil
		}},
		&testComponent{name: "must-not-start", calls: recorder, start: func(context.Context) error {
			nextStarted <- struct{}{}
			return nil
		}},
	}
	var err error
	runtime, err = NewWithDependencies(testConfig(8), testGate(t, 8), Dependencies{Components: components})
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- runtime.Start(context.Background()) }()
	<-shutdownReturned
	if err := <-startDone; !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrClosing) {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-nextStarted:
		t.Fatal("component was admitted after BeginShutdown returned")
	default:
	}
	drainRuntimeEvents(t, runtime)
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	want := []string{"start:initiator", "stop:initiator"}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("component calls = %v, want %v", got, want)
	}
}

func TestStageBCommitAuthenticatedQueueOneCommitsAndUsesOverflowPolicy(t *testing.T) {
	runtime := testRuntime(t, 1, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var publisherCalls atomic.Int32
	published := false
	if err := runtime.CommitAuthenticated(context.Background(), func(commitReady func() error) error {
		publisherCalls.Add(1)
		published = true
		if err := commitReady(); err != nil {
			published = false
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("CommitAuthenticated() error = %v", err)
	}
	if !published || publisherCalls.Load() != 1 || runtime.Status().Lifecycle != stateReady {
		t.Fatalf("commit state: published=%v calls=%d status=%+v", published, publisherCalls.Load(), runtime.Status())
	}
	event, err := runtime.NextEvent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	change := event.Value.(model.SessionStateChangedEvent)
	if change.Previous != stateCreated || change.Current != stateAuthenticating {
		t.Fatalf("retained lifecycle prefix = %+v", change)
	}
	metrics := runtime.Diagnostics().Metrics
	if metrics[diagnostics.MetricEventsPublished] != 1 || metrics[diagnostics.MetricEventsRejected] != 4 {
		t.Fatalf("event metrics = %+v", metrics)
	}
	forceShutdown(t, runtime)
}

func TestStageBCommitAuthenticatedFailureCancellationAndPanicLeaveCreated(t *testing.T) {
	tests := []struct {
		name      string
		ctx       func() context.Context
		publisher AuthenticatedPublisher
		want      error
	}{
		{
			name: "publisher failure",
			ctx:  context.Background,
			publisher: func(func() error) error {
				return errors.New("injected")
			},
			want: ErrAuthenticatedCommit,
		},
		{
			name: "publisher panic",
			ctx:  context.Background,
			publisher: func(func() error) error {
				panic("injected")
			},
			want: ErrPublisherPanic,
		},
		{
			name: "pre-cancelled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			publisher: func(func() error) error {
				t.Fatal("publisher called for cancelled commit")
				return nil
			},
			want: context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := testRuntime(t, 8, Dependencies{})
			if err := runtime.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			err := runtime.CommitAuthenticated(test.ctx(), test.publisher)
			if !errors.Is(err, test.want) {
				t.Fatalf("CommitAuthenticated() error = %v, want %v", err, test.want)
			}
			if runtime.Status().Lifecycle != stateCreated || runtime.events.Stats().Depth != 0 {
				t.Fatalf("failed commit changed runtime: status=%+v events=%+v", runtime.Status(), runtime.events.Stats())
			}
			forceShutdown(t, runtime)
		})
	}
}

func TestStageBCommitAuthenticatedCancellationRollsBackPublisher(t *testing.T) {
	runtime := testRuntime(t, 8, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	published := false
	err := runtime.CommitAuthenticated(ctx, func(commitReady func() error) error {
		published = true
		cancel()
		if err := commitReady(); err != nil {
			published = false
			return err
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || published {
		t.Fatalf("CommitAuthenticated() = %v, published=%v", err, published)
	}
	if runtime.Status().Lifecycle != stateCreated || runtime.events.Stats().Depth != 0 {
		t.Fatalf("cancelled commit changed runtime: status=%+v events=%+v", runtime.Status(), runtime.events.Stats())
	}
	forceShutdown(t, runtime)
}

func TestStageBCommitAuthenticatedSerializesWithShutdown(t *testing.T) {
	runtime := testRuntime(t, 8, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- runtime.CommitAuthenticated(context.Background(), func(commitReady func() error) error {
			close(entered)
			<-release
			return commitReady()
		})
	}()
	<-entered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown overtook authentication commit: %v", err)
	default:
	}
	close(release)
	if err := <-commitDone; err != nil {
		t.Fatalf("CommitAuthenticated() error = %v", err)
	}
	drainRuntimeEvents(t, runtime)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if runtime.Status().Lifecycle != stateClosed {
		t.Fatalf("final lifecycle = %q", runtime.Status().Lifecycle)
	}
}

func drainRuntimeEvents(t *testing.T, runtime *Runtime) {
	t.Helper()
	for {
		_, err := runtime.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			return
		}
		if err != nil {
			t.Fatalf("NextEvent() error = %v", err)
		}
	}
}

func TestStageBConcurrentShutdownRunsComponentCleanupExactlyOnce(t *testing.T) {
	var stops atomic.Int32
	component := &testComponent{name: "once", stop: func(context.Context) error {
		stops.Add(1)
		return nil
	}}
	runtime := testRuntime(t, 64, Dependencies{Components: []Component{component}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const callers = 32
	results := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- runtime.Shutdown(context.Background())
		}()
	}
	drainRuntimeEvents(t, runtime)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}
	if got := stops.Load(); got != 1 {
		t.Fatalf("component cleanup calls = %d, want 1", got)
	}
}

type panicClock struct{}

func (panicClock) Now() time.Time                         { panic("injected clock panic") }
func (panicClock) After(time.Duration) <-chan time.Time   { return make(chan time.Time) }
func (panicClock) NewTimer(time.Duration) coreclock.Timer { panic("unused") }
