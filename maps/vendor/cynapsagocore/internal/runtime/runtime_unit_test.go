package runtime

import (
	"context"
	"errors"
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestNewValidatesConfigGateAndDependencies(t *testing.T) {
	gate := testGate(t, 2)
	valid := testConfig(2)
	if runtime, err := New(testConfig(0), gate); runtime != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(invalid config) = (%v, %v)", runtime, err)
	}
	if runtime, err := New(valid, nil); runtime != nil || !errors.Is(err, ErrNilGate) {
		t.Fatalf("New(nil gate) = (%v, %v)", runtime, err)
	}
	if runtime, err := New(testConfig(1), gate); runtime != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(mismatched capacity) = (%v, %v)", runtime, err)
	}
	if runtime, err := NewWithDependencies(valid, gate, Dependencies{Handlers: map[string]Handler{"private.unknown": testHandler}}); runtime != nil || !errors.Is(err, ErrUnknownHandler) {
		t.Fatalf("New(unknown handler) = (%v, %v)", runtime, err)
	}
	if runtime, err := NewWithDependencies(valid, gate, Dependencies{Handlers: map[string]Handler{"message.send": nil}}); runtime != nil || !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil handler) = (%v, %v)", runtime, err)
	}
	duplicate := []*testComponent{{name: "same"}, {name: "same"}}
	if runtime, err := NewWithDependencies(valid, gate, Dependencies{Components: []Component{duplicate[0], duplicate[1]}}); runtime != nil || !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(duplicate component) = (%v, %v)", runtime, err)
	}
	var nilComponent *testComponent
	if runtime, err := NewWithDependencies(valid, gate, Dependencies{Components: []Component{nilComponent}}); runtime != nil || !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(typed nil component) = (%v, %v)", runtime, err)
	}
}

func TestRuntimePayloadLimitExactBoundary(t *testing.T) {
	gate := testGate(t, 1)
	config := testConfig(1)
	config.PayloadLimit = v1.MaximumPayloadBytes
	runtime, err := New(config, gate)
	if err != nil {
		t.Fatalf("exact maximum rejected: %v", err)
	}
	forceShutdown(t, runtime)

	gate = testGate(t, 1)
	config = testConfig(1)
	config.PayloadLimit = v1.MaximumPayloadBytes + 1
	if runtime, err := New(config, gate); runtime != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("maximum + 1 = (%v, %v), want invalid config", runtime, err)
	}
}

func TestStartStartsComponentsOnceAndRemainsCreated(t *testing.T) {
	recorder := &callRecorder{}
	components := []Component{
		&testComponent{name: "first", calls: recorder},
		&testComponent{name: "second", calls: recorder},
	}
	runtime := testRuntime(t, 8, Dependencies{Components: components})

	const callers = 16
	start := make(chan struct{})
	results := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- runtime.Start(context.Background())
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}
	if got := recorder.snapshot(); fmt.Sprint(got) != "[start:first start:second]" {
		t.Fatalf("component calls = %v", got)
	}
	if status := runtime.Status(); status.Lifecycle != stateCreated {
		t.Fatalf("Start lifecycle = %q, want created", status.Lifecycle)
	}
	if snapshot := runtime.Diagnostics(); snapshot.WorkerCount != 1 || snapshot.EventQueueCapacity != 8 {
		t.Fatalf("Diagnostics() = %+v", snapshot)
	}
	forceShutdown(t, runtime)
	if got := recorder.snapshot(); fmt.Sprint(got) != "[start:first start:second stop:second stop:first]" {
		t.Fatalf("shutdown calls = %v", got)
	}
}

func TestStartupFailureRollsBackEveryEarlierComponentInReverse(t *testing.T) {
	for failAt := range 3 {
		failAt := failAt
		t.Run(fmt.Sprintf("stage_%d", failAt), func(t *testing.T) {
			recorder := &callRecorder{}
			components := make([]Component, 3)
			for index := range components {
				component := &testComponent{name: fmt.Sprintf("c%d", index), calls: recorder}
				if index == failAt {
					component.startErr = errors.New("injected startup failure")
				}
				components[index] = component
			}
			runtime := testRuntime(t, 8, Dependencies{Components: components})
			err := runtime.Start(context.Background())
			if !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrComponentStartFailed) {
				t.Fatalf("Start() error = %v", err)
			}
			want := make([]string, 0, failAt*2+1)
			for index := 0; index <= failAt; index++ {
				want = append(want, fmt.Sprintf("start:c%d", index))
			}
			for index := failAt - 1; index >= 0; index-- {
				want = append(want, fmt.Sprintf("stop:c%d", index))
			}
			if got := recorder.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("calls = %v, want %v", got, want)
			}
			if runtime.Status().Lifecycle != stateFailed || runtime.Diagnostics().WorkerCount != 0 {
				t.Fatalf("failed runtime status = %+v diagnostics = %+v", runtime.Status(), runtime.Diagnostics())
			}
			forceShutdown(t, runtime)
		})
	}
}

func TestStartupCancellationRollsBackStartedComponents(t *testing.T) {
	recorder := &callRecorder{}
	entered := make(chan struct{})
	components := []Component{
		&testComponent{name: "started", calls: recorder},
		&testComponent{name: "cancelled", calls: recorder, start: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	runtime := testRuntime(t, 4, Dependencies{Components: components})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Start(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrStartupFailed) {
		t.Fatalf("Start() error = %v", err)
	}
	if got := recorder.snapshot(); fmt.Sprint(got) != "[start:started start:cancelled stop:started]" {
		t.Fatalf("calls = %v", got)
	}
	forceShutdown(t, runtime)
}

func TestComponentPanicsAreContainedAtStartAndShutdown(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		runtime := testRuntime(t, 4, Dependencies{Components: []Component{&testComponent{name: "panic", panicStart: true}}})
		if err := runtime.Start(context.Background()); !errors.Is(err, ErrComponentPanic) {
			t.Fatalf("Start() error = %v", err)
		}
		if runtime.Status().Lifecycle != stateFailed {
			t.Fatalf("lifecycle = %q", runtime.Status().Lifecycle)
		}
		forceShutdown(t, runtime)
	})
	t.Run("shutdown", func(t *testing.T) {
		runtime := testRuntime(t, 4, Dependencies{Components: []Component{&testComponent{name: "panic", panicStop: true}}})
		if err := runtime.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := shutdownAndDrainEvents(t, runtime); !errors.Is(err, ErrComponentPanic) || !errors.Is(err, ErrComponentStopFailed) {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})
}

func TestRuntimeWorkerDispatchesTypedCommandsAndContainsHandlerPanic(t *testing.T) {
	var calls atomic.Int32
	handlers := map[string]Handler{
		"message.send": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			calls.Add(1)
			args := command.Args.(model.MessageSendArgs)
			return model.Result{Value: model.SendResult{MessageID: args.To, Accepted: true}}, nil
		},
		"core.status": func(context.Context, Services, model.Command) (model.Result, error) {
			panic("private panic must not escape")
		},
	}
	runtime := testRuntime(t, 4, Dependencies{Handlers: handlers})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	command := model.Command{ID: "send", Name: "message.send", Args: model.MessageSendArgs{To: "agent"}}
	if admission, err := runtime.gate.Submit(context.Background(), command); err != nil || !admission.Accepted {
		t.Fatalf("Submit() = (%+v, %v)", admission, err)
	}
	result, err := runtime.gate.NextCompletion(context.Background())
	if err != nil || result.CommandID != "send" || result.Err != nil {
		t.Fatalf("completion = (%+v, %v)", result, err)
	}
	send, ok := result.Value.(model.SendResult)
	if !ok || !send.Accepted || send.MessageID != "agent" || calls.Load() != 1 {
		t.Fatalf("send result = %#v calls=%d", result.Value, calls.Load())
	}

	if admission, err := runtime.gate.Submit(context.Background(), model.Command{ID: "panic", Name: "core.status", Args: model.EmptyArgs{}}); err != nil || !admission.Accepted {
		t.Fatalf("panic Submit() = (%+v, %v)", admission, err)
	}
	panicResult, err := runtime.gate.NextCompletion(context.Background())
	if err != nil || panicResult.Err == nil || panicResult.Err.Code != "command_handler_panic" {
		t.Fatalf("panic completion = (%+v, %v)", panicResult, err)
	}
	forceShutdown(t, runtime)
}

func TestSingleWorkerDoesNotStartSecondHandlerUntilFirstReturns(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
		entered <- command.ID
		if command.ID == "first" {
			<-release
		}
		return model.Result{Value: model.EmptyResult{}}, nil
	}
	runtime := testRuntime(t, 2, Dependencies{Handlers: map[string]Handler{"core.status": handler}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if _, err := runtime.gate.Submit(context.Background(), model.Command{ID: id, Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
			t.Fatal(err)
		}
	}
	if got := <-entered; got != "first" {
		t.Fatalf("first handler = %q", got)
	}
	select {
	case got := <-entered:
		t.Fatalf("second handler started early: %q", got)
	default:
	}
	close(release)
	if got := <-entered; got != "second" {
		t.Fatalf("second handler = %q", got)
	}
	for range 2 {
		if _, err := runtime.gate.NextCompletion(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	forceShutdown(t, runtime)
}

func TestShutdownCancelsRunningHandlerAndGateKeepsOneCompletion(t *testing.T) {
	entered := make(chan struct{})
	handler := func(ctx context.Context, _ Services, _ model.Command) (model.Result, error) {
		close(entered)
		<-ctx.Done()
		return model.Result{}, ctx.Err()
	}
	runtime := testRuntime(t, 1, Dependencies{Handlers: map[string]Handler{"core.status": handler}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.gate.Submit(context.Background(), model.Command{ID: "running", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	<-entered
	runtime.BeginShutdown()
	completion, err := runtime.gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "running" || completion.Err == nil || completion.Err.Code != "command_shutdown" {
		t.Fatalf("shutdown completion = (%+v, %v)", completion, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = runtime.Shutdown(ctx)
	if stats := runtime.gate.Stats(); stats.CompletionQueueDepth != 0 || stats.RegistryEntries != 0 {
		t.Fatalf("duplicate or retained completion: %+v", stats)
	}
}

func TestOuterWorkerPanicIsContainedAndTransitionsRuntimeFailed(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	gate := runtime.gate
	runtime.gate = nil
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-runtime.workerDone
	runtime.gate = gate
	if runtime.Status().Lifecycle != stateFailed {
		t.Fatalf("lifecycle = %q, want failed", runtime.Status().Lifecycle)
	}
	if runtime.Diagnostics().Metrics[diagnostics.MetricWorkerPanics] != 1 {
		t.Fatalf("worker panic metrics = %+v", runtime.Diagnostics().Metrics)
	}
	forceShutdown(t, runtime)
}

func TestDispatchFailsClosedForUnknownUnavailableArgumentsAndResults(t *testing.T) {
	runtime := testRuntime(t, 4, Dependencies{Handlers: map[string]Handler{
		"core.status": func(context.Context, Services, model.Command) (model.Result, error) {
			return model.Result{}, nil
		},
	}})
	tests := []struct {
		name    string
		command model.Command
		code    string
		err     error
	}{
		{name: "unknown", command: model.Command{ID: "1", Name: "private.unknown", Args: model.EmptyArgs{}}, code: "unknown_command"},
		{name: "unavailable", command: model.Command{ID: "2", Name: "message.send", Args: model.MessageSendArgs{}}, code: "command_unavailable"},
		{name: "arguments", command: model.Command{ID: "3", Name: "core.status", Args: model.MessageSendArgs{}}, code: "invalid_command_arguments"},
		{name: "result", command: model.Command{ID: "4", Name: "core.status", Args: model.EmptyArgs{}}, err: ErrInvalidHandlerResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := runtime.Dispatch(context.Background(), test.command)
			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Fatalf("Dispatch() error = %v, want %v", err, test.err)
				}
				return
			}
			if err != nil || result.Err == nil || result.Err.Code != test.code {
				t.Fatalf("Dispatch() = (%+v, %v), want code %q", result, err, test.code)
			}
		})
	}
}

func TestLifecycleTransitionTableAndBoundedOverflow(t *testing.T) {
	fake := coreclock.NewFake(time.Unix(100, 0))
	runtime := testRuntime(t, 1, Dependencies{Clock: fake})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	chain := []model.LifecycleState{stateAuthenticating, stateMeshConnected, stateDurableReady, statePeerLinkBuilding, stateReady, stateDegraded, stateReady}
	for _, state := range chain {
		if err := runtime.Transition(state); err != nil {
			t.Fatalf("Transition(%q) error = %v", state, err)
		}
	}
	if runtime.Status().Lifecycle != stateReady {
		t.Fatalf("lifecycle = %q", runtime.Status().Lifecycle)
	}
	if err := runtime.Transition(stateCreated); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("illegal Transition() error = %v", err)
	}
	if err := runtime.Transition(stateClosing); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("handler-owned closing transition error = %v", err)
	}
	if stats := runtime.events.Stats(); stats.Depth != 1 || stats.Capacity != 1 {
		t.Fatalf("event Stats() = %+v", stats)
	}
	metrics := runtime.Diagnostics().Metrics
	if metrics[diagnostics.MetricEventsRejected] != float64(len(chain)-1) {
		t.Fatalf("rejected events = %v, want %d", metrics[diagnostics.MetricEventsRejected], len(chain)-1)
	}
	forceShutdown(t, runtime)
}

func TestLifecycleTransitionTableIsExplicitAndClosed(t *testing.T) {
	states := []model.LifecycleState{stateCreated, stateAuthenticating, stateMeshConnected, stateDurableReady, statePeerLinkBuilding, stateReady, stateDegraded, stateFailed, stateClosing, stateClosed, "unknown"}
	allowed := map[model.LifecycleState]map[model.LifecycleState]bool{
		stateCreated:          {stateAuthenticating: true, stateClosing: true, stateFailed: true},
		stateAuthenticating:   {stateMeshConnected: true, stateClosing: true, stateFailed: true},
		stateMeshConnected:    {stateDurableReady: true, stateDegraded: true, stateClosing: true, stateFailed: true},
		stateDurableReady:     {statePeerLinkBuilding: true, stateReady: true, stateDegraded: true, stateClosing: true, stateFailed: true},
		statePeerLinkBuilding: {stateReady: true, stateDegraded: true, stateClosing: true, stateFailed: true},
		stateReady:            {stateDegraded: true, stateClosing: true, stateFailed: true},
		stateDegraded:         {stateReady: true, stateClosing: true, stateFailed: true},
		stateFailed:           {stateClosing: true},
		stateClosing:          {stateClosed: true},
	}
	for _, from := range states {
		for _, to := range states {
			if got, want := legalTransition(from, to), allowed[from][to]; got != want {
				t.Fatalf("legalTransition(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}

	runtime := testRuntime(t, 2, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Transition(stateCreated); err != nil {
		t.Fatalf("same-state transition error = %v", err)
	}
	if runtime.events.Stats().Depth != 0 {
		t.Fatal("same-state transition emitted an event")
	}
	forceShutdown(t, runtime)
}

func TestPublishAndNextEventUseOneBoundedQueue(t *testing.T) {
	runtime := testRuntime(t, 1, Dependencies{})
	value := &model.CoreErrorEvent{Err: &model.Error{Code: "owned"}}
	event := model.Event{ID: "event", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: value}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PublishEvent(context.Background(), model.Event{ID: "overflow", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{}}); !errors.Is(err, delivery.ErrQueueFull) {
		t.Fatalf("overflow error = %v", err)
	}
	got, err := runtime.NextEvent(context.Background())
	gotValue, ok := got.Value.(model.CoreErrorEvent)
	if err != nil || !ok || gotValue.Err == nil || gotValue.Err.Code != "owned" || gotValue.Err == value.Err {
		t.Fatalf("NextEvent() = (%+v, %v)", got, err)
	}
	forceShutdown(t, runtime)
}

func TestShutdownDrainsInOrderAndRepeatedCallIsIdempotent(t *testing.T) {
	recorder := &callRecorder{}
	runtime := testRuntime(t, 4, Dependencies{Components: []Component{
		&testComponent{name: "first", calls: recorder},
		&testComponent{name: "second", calls: recorder},
	}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Shutdown(context.Background()) }()
	first, err := runtime.NextEvent(context.Background())
	if err != nil || first.Value.(model.SessionStateChangedEvent).Current != stateClosing {
		t.Fatalf("first shutdown event = (%+v, %v)", first, err)
	}
	second, err := runtime.NextEvent(context.Background())
	if err != nil || second.Value.(model.SessionStateChangedEvent).Current != stateClosed {
		t.Fatalf("second shutdown event = (%+v, %v)", second, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown() error = %v", err)
	}
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() after closed error = %v", err)
	}
	if got := recorder.snapshot(); fmt.Sprint(got) != "[start:first start:second stop:second stop:first]" {
		t.Fatalf("calls = %v", got)
	}
	if status := runtime.Status(); status.Lifecycle != stateClosed {
		t.Fatalf("status = %+v", status)
	}
}

func TestConcurrentShutdownCallersShareOneCleanup(t *testing.T) {
	recorder := &callRecorder{}
	runtime := testRuntime(t, 8, Dependencies{Components: []Component{&testComponent{name: "only", calls: recorder}}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	results := make(chan error, callers)
	start := make(chan struct{})
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- runtime.Shutdown(context.Background())
		}()
	}
	close(start)
	for range 2 {
		if _, err := runtime.NextEvent(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}
	if got := recorder.snapshot(); fmt.Sprint(got) != "[start:only stop:only]" {
		t.Fatalf("calls = %v", got)
	}
}

func TestConcurrentShutdownCannotCloseEventsBeforeClosingPublication(t *testing.T) {
	previousProcs := goruntime.GOMAXPROCS(1)
	t.Cleanup(func() { goruntime.GOMAXPROCS(previousProcs) })

	blocking := newBlockingClock(time.Unix(500, 0))
	runtime := testRuntime(t, 4, Dependencies{Clock: blocking})
	beginDone := make(chan struct{})
	go func() {
		runtime.BeginShutdown()
		close(beginDone)
	}()
	<-blocking.entered

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownStarted := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		close(shutdownStarted)
		shutdownDone <- runtime.Shutdown(ctx)
	}()
	<-shutdownStarted
	// With one P, yielding here lets the second caller run until it either
	// blocks behind initiation ownership or incorrectly starts cleanup.
	goruntime.Gosched()
	select {
	case err := <-shutdownDone:
		t.Fatalf("concurrent Shutdown completed before closing event publication: %v", err)
	default:
	}
	if runtime.events.Stats().Closed {
		t.Fatal("event dispatcher closed while closing event publication was blocked")
	}

	close(blocking.release)
	<-beginDone
	if err := <-shutdownDone; !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v", err)
	}
	for range 2 {
		if _, err := runtime.NextEvent(context.Background()); err != nil {
			t.Fatalf("drain shutdown event: %v", err)
		}
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("late Shutdown() error = %v", err)
	}
	metrics := runtime.Diagnostics().Metrics
	if metrics[diagnostics.MetricEventsPublished] != 2 || metrics[diagnostics.MetricEventsRejected] != 0 {
		t.Fatalf("shutdown event metrics = %+v, want two published and none rejected", metrics)
	}
}

func TestLifecyclePublicationRemainsOrderedAcrossConcurrentShutdown(t *testing.T) {
	previousProcs := goruntime.GOMAXPROCS(1)
	t.Cleanup(func() { goruntime.GOMAXPROCS(previousProcs) })

	// The fifth lifecycle event is ready. Block its construction after the
	// state mutation to force shutdown to contend at the publication boundary.
	blocking := newBlockingClockOn(time.Unix(600, 0), 5)
	runtime := testRuntime(t, 8, Dependencies{Clock: blocking})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.LifecycleState{stateAuthenticating, stateMeshConnected, stateDurableReady, statePeerLinkBuilding} {
		if err := runtime.Transition(state); err != nil {
			t.Fatalf("Transition(%q) error = %v", state, err)
		}
		if _, err := runtime.NextEvent(context.Background()); err != nil {
			t.Fatalf("drain Transition(%q) event: %v", state, err)
		}
	}

	readyDone := make(chan error, 1)
	go func() { readyDone <- runtime.Transition(stateReady) }()
	<-blocking.entered

	shutdownStarted := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		close(shutdownStarted)
		shutdownDone <- runtime.Shutdown(context.Background())
	}()
	<-shutdownStarted
	goruntime.Gosched()
	if status := runtime.Status().Lifecycle; status != stateReady {
		t.Fatalf("lifecycle while ready publication blocked = %q, want ready", status)
	}

	close(blocking.release)
	if err := <-readyDone; err != nil {
		t.Fatalf("Transition(ready) error = %v", err)
	}
	want := []model.LifecycleState{stateReady, stateClosing, stateClosed}
	for index, expected := range want {
		event, err := runtime.NextEvent(context.Background())
		if err != nil {
			t.Fatalf("NextEvent(%d) error = %v", index, err)
		}
		change, ok := event.Value.(model.SessionStateChangedEvent)
		if !ok || change.Current != expected {
			t.Fatalf("event %d = %+v, want current %q", index, event, expected)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	metrics := runtime.Diagnostics().Metrics
	if metrics[diagnostics.MetricEventsPublished] != 7 || metrics[diagnostics.MetricEventsRejected] != 0 {
		t.Fatalf("lifecycle event metrics = %+v, want seven published and none rejected", metrics)
	}
}

func TestShutdownCallerDeadlineDoesNotCancelCleanup(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PublishEvent(context.Background(), model.Event{ID: "pending", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Shutdown(ctx); !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if runtime.Status().Lifecycle != stateClosing {
		t.Fatalf("status after caller deadline = %+v", runtime.Status())
	}
	for {
		_, err := runtime.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			break
		}
		if err != nil {
			t.Fatalf("drain pending event: %v", err)
		}
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("post-deadline Shutdown() error = %v", err)
	}
	if runtime.Status().Lifecycle != stateClosed || !runtime.gate.Stats().Closed || !runtime.events.Stats().Closed {
		t.Fatalf("resources after cleanup: status=%+v gate=%+v events=%+v", runtime.Status(), runtime.gate.Stats(), runtime.events.Stats())
	}
}

func TestStartAndShutdownValidateContextsAndState(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	if err := runtime.Start(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Start(nil) error = %v", err)
	}
	if err := runtime.Shutdown(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Shutdown(nil) error = %v", err)
	}
	runtime.BeginShutdown()
	// BeginShutdown owns asynchronous cleanup. Start may observe either the
	// closing edge or the already-completed closed edge; both reject startup.
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrClosing) && !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() after BeginShutdown error = %v", err)
	}
	forceShutdown(t, runtime)
}

type testComponent struct {
	name       string
	calls      *callRecorder
	startErr   error
	stopErr    error
	panicStart bool
	panicStop  bool
	start      func(context.Context) error
	stop       func(context.Context) error
}

func (c *testComponent) Name() string { return c.name }
func (c *testComponent) Start(ctx context.Context) error {
	if c.calls != nil {
		c.calls.add("start:" + c.name)
	}
	if c.panicStart {
		panic("component start panic")
	}
	if c.start != nil {
		return c.start(ctx)
	}
	return c.startErr
}
func (c *testComponent) Shutdown(ctx context.Context) error {
	if c.calls != nil {
		c.calls.add("stop:" + c.name)
	}
	if c.panicStop {
		panic("component stop panic")
	}
	if c.stop != nil {
		return c.stop(ctx)
	}
	return c.stopErr
}

type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

type blockingClock struct {
	base    *coreclock.Fake
	entered chan struct{}
	release chan struct{}
	calls   atomic.Uint64
	blockOn uint64
}

func newBlockingClock(now time.Time) *blockingClock {
	return newBlockingClockOn(now, 1)
}

func newBlockingClockOn(now time.Time, blockOn uint64) *blockingClock {
	return &blockingClock{base: coreclock.NewFake(now), entered: make(chan struct{}), release: make(chan struct{}), blockOn: blockOn}
}

func (c *blockingClock) Now() time.Time {
	if c.calls.Add(1) == c.blockOn {
		close(c.entered)
		<-c.release
	}
	return c.base.Now()
}

func (c *blockingClock) After(duration time.Duration) <-chan time.Time {
	return c.base.After(duration)
}

func (c *blockingClock) NewTimer(duration time.Duration) coreclock.Timer {
	return c.base.NewTimer(duration)
}

func (r *callRecorder) add(call string) {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
}
func (r *callRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func testRuntime(t *testing.T, capacity int, dependencies Dependencies) *Runtime {
	t.Helper()
	runtime, err := NewWithDependencies(testConfig(capacity), testGate(t, capacity), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func testGate(t *testing.T, capacity int) *commandgate.Gate {
	t.Helper()
	gate, err := commandgate.New(capacity)
	if err != nil {
		t.Fatal(err)
	}
	return gate
}

func testConfig(capacity int) Config {
	return Config{
		RuntimeConfig:  model.RuntimeConfig{QueueLimit: uint32(capacity), PayloadLimit: 1 << 20},
		CleanupTimeout: 10 * time.Second,
	}
}

func testHandler(context.Context, Services, model.Command) (model.Result, error) {
	return model.Result{Value: model.EmptyResult{}}, nil
}

func forceShutdown(t *testing.T, runtime *Runtime) {
	t.Helper()
	_ = shutdownAndDrainEvents(t, runtime)
}

func shutdownAndDrainEvents(t *testing.T, runtime *Runtime) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runtime.Shutdown(context.Background()) }()
	for {
		_, err := runtime.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			break
		}
		if err != nil {
			t.Fatalf("NextEvent() error = %v", err)
		}
	}
	return <-done
}
