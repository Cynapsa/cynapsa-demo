package runtime

import (
	"context"
	"errors"
	"fmt"
	gostdlib "runtime"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestWave2AcceptanceReadyClosingClosedEventsAreAtomicAndOrdered(t *testing.T) {
	runtime := testRuntime(t, 16, Dependencies{})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := []model.LifecycleState{
		stateAuthenticating,
		stateMeshConnected,
		stateDurableReady,
		statePeerLinkBuilding,
		stateReady,
	}
	for _, next := range path {
		if err := runtime.Transition(next); err != nil {
			t.Fatalf("Transition(%q) error = %v", next, err)
		}
		event, err := runtime.NextEvent(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		change := event.Value.(model.SessionStateChangedEvent)
		if change.Current != next {
			t.Fatalf("event current = %q, want %q", change.Current, next)
		}
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(context.Background()) }()
	want := []struct {
		previous model.LifecycleState
		current  model.LifecycleState
	}{
		{previous: stateReady, current: stateClosing},
		{previous: stateClosing, current: stateClosed},
	}
	for index, expected := range want {
		event, err := runtime.NextEvent(context.Background())
		if err != nil {
			t.Fatalf("NextEvent(%d) error = %v", index, err)
		}
		change, ok := event.Value.(model.SessionStateChangedEvent)
		if !ok || change.Previous != expected.previous || change.Current != expected.current {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if status := runtime.Status(); status.Lifecycle != stateClosed {
		t.Fatalf("Status() = %+v", status)
	}
	if metrics := runtime.Diagnostics().Metrics; metrics[diagnostics.MetricEventsPublished] != float64(len(path)+len(want)) || metrics[diagnostics.MetricEventsRejected] != 0 {
		t.Fatalf("event metrics = %+v", metrics)
	}
}

func TestWave2AcceptanceCommandCompletionAndHandlerEventOrderUnderLoad(t *testing.T) {
	const count = 256
	handler := func(_ context.Context, services Services, command model.Command) (model.Result, error) {
		if err := services.PublishEvent(context.Background(), model.Event{
			ID:        "event-" + command.ID,
			Name:      "core.error",
			CreatedAt: time.Unix(1, 0),
			Value:     model.CoreErrorEvent{Err: &model.Error{Code: "qa"}},
		}); err != nil {
			return model.Result{}, err
		}
		return model.Result{Value: model.EmptyResult{}}, nil
	}
	runtime := testRuntime(t, count, Dependencies{Handlers: map[string]Handler{"core.status": handler}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := range count {
		id := fmt.Sprintf("command-%03d", index)
		if admission, err := runtime.gate.Submit(context.Background(), model.Command{ID: id, Name: "core.status", Args: model.EmptyArgs{}}); err != nil || !admission.Accepted {
			t.Fatalf("Submit(%q) = (%+v, %v)", id, admission, err)
		}
	}
	for index := range count {
		wantID := fmt.Sprintf("command-%03d", index)
		completion, err := runtime.gate.NextCompletion(context.Background())
		if err != nil || completion.CommandID != wantID || completion.Err != nil {
			t.Fatalf("completion %d = (%+v, %v), want %q", index, completion, err, wantID)
		}
	}
	for index := range count {
		wantID := fmt.Sprintf("event-command-%03d", index)
		event, err := runtime.NextEvent(context.Background())
		if err != nil || event.ID != wantID {
			t.Fatalf("event %d = (%+v, %v), want %q", index, event, err, wantID)
		}
	}
	forceShutdown(t, runtime)
}

func TestWave2AcceptanceRepeatedStartupRollbackAndShutdownReleaseResources(t *testing.T) {
	baseline := gostdlib.NumGoroutine()
	const cycles = 100
	for cycle := range cycles {
		recorder := &callRecorder{}
		components := []Component{
			&testComponent{name: "first", calls: recorder},
			&testComponent{name: "second", calls: recorder},
		}
		runtime := testRuntime(t, 4, Dependencies{Components: components})
		if err := runtime.Start(context.Background()); err != nil {
			t.Fatalf("cycle %d Start() error = %v", cycle, err)
		}
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- runtime.Shutdown(context.Background()) }()
		for range 2 {
			if _, err := runtime.NextEvent(context.Background()); err != nil {
				t.Fatalf("cycle %d NextEvent() error = %v", cycle, err)
			}
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("cycle %d Shutdown() error = %v", cycle, err)
		}
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:first start:second stop:second stop:first]" {
			t.Fatalf("cycle %d calls = %s", cycle, got)
		}
		gateStats := runtime.gate.Stats()
		eventStats := runtime.events.Stats()
		if !gateStats.Closed || gateStats.RegistryEntries != 0 || !eventStats.Closed || eventStats.Depth != 0 || runtime.Diagnostics().WorkerCount != 0 {
			t.Fatalf("cycle %d retained resources: gate=%+v events=%+v diagnostics=%+v", cycle, gateStats, eventStats, runtime.Diagnostics())
		}
	}
	gostdlib.GC()
	gostdlib.Gosched()
	if after := gostdlib.NumGoroutine(); after > baseline+8 {
		t.Fatalf("goroutines grew from %d to %d", baseline, after)
	}
}

func TestWave2AcceptanceInjectedFailureOrPanicAtEveryComponentStage(t *testing.T) {
	const stages = 5
	for failedStage := range stages {
		for _, panicStage := range []bool{false, true} {
			name := fmt.Sprintf("stage-%d/panic-%v", failedStage, panicStage)
			t.Run(name, func(t *testing.T) {
				recorder := &callRecorder{}
				components := make([]Component, stages)
				for stage := range stages {
					component := &testComponent{name: fmt.Sprintf("component-%d", stage), calls: recorder}
					if stage == failedStage {
						if panicStage {
							component.panicStart = true
						} else {
							component.startErr = errors.New("injected")
						}
					}
					components[stage] = component
				}
				runtime := testRuntime(t, 8, Dependencies{Components: components})
				err := runtime.Start(context.Background())
				if !errors.Is(err, ErrStartupFailed) || !errors.Is(err, ErrComponentStartFailed) {
					t.Fatalf("Start() error = %v", err)
				}
				if panicStage && !errors.Is(err, ErrComponentPanic) {
					t.Fatalf("panic Start() error = %v", err)
				}
				calls := recorder.snapshot()
				wantCalls := failedStage + 1 + failedStage
				if len(calls) != wantCalls {
					t.Fatalf("calls = %v, want %d startup/rollback calls", calls, wantCalls)
				}
				for rollback := 0; rollback < failedStage; rollback++ {
					want := fmt.Sprintf("stop:component-%d", failedStage-rollback-1)
					if got := calls[failedStage+1+rollback]; got != want {
						t.Fatalf("rollback call %d = %q, want %q", rollback, got, want)
					}
				}
				forceShutdown(t, runtime)
			})
		}
	}
}

func TestWave2AcceptanceInjectedShutdownFailureOrPanicAtEveryComponentStage(t *testing.T) {
	const stages = 5
	for failedStage := range stages {
		for _, panicStage := range []bool{false, true} {
			name := fmt.Sprintf("stage-%d/panic-%v", failedStage, panicStage)
			t.Run(name, func(t *testing.T) {
				recorder := &callRecorder{}
				components := make([]Component, stages)
				for stage := range stages {
					component := &testComponent{name: fmt.Sprintf("component-%d", stage), calls: recorder}
					if stage == failedStage {
						if panicStage {
							component.panicStop = true
						} else {
							component.stopErr = errors.New("injected")
						}
					}
					components[stage] = component
				}
				runtime := testRuntime(t, 8, Dependencies{Components: components})
				if err := runtime.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				shutdownDone := make(chan error, 1)
				go func() { shutdownDone <- runtime.Shutdown(context.Background()) }()
				for range 2 {
					if _, err := runtime.NextEvent(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				err := <-shutdownDone
				if !errors.Is(err, ErrComponentStopFailed) {
					t.Fatalf("Shutdown() error = %v", err)
				}
				if panicStage && !errors.Is(err, ErrComponentPanic) {
					t.Fatalf("panic Shutdown() error = %v", err)
				}
				calls := recorder.snapshot()
				if len(calls) != stages*2 {
					t.Fatalf("calls = %v, want all starts and stops", calls)
				}
				for stop := range stages {
					want := fmt.Sprintf("stop:component-%d", stages-stop-1)
					if got := calls[stages+stop]; got != want {
						t.Fatalf("shutdown call %d = %q, want %q", stop, got, want)
					}
				}
			})
		}
	}
}

func TestWave2AcceptanceConcurrentDiagnosticsDuringCommandAndShutdownChanges(t *testing.T) {
	const capacity = 64
	runtime := testRuntime(t, capacity, Dependencies{Handlers: map[string]Handler{
		"core.status": func(context.Context, Services, model.Command) (model.Result, error) {
			return model.Result{Value: model.EmptyResult{}}, nil
		},
	}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := range capacity {
		id := fmt.Sprintf("diagnostic-%03d", index)
		if _, err := runtime.gate.Submit(context.Background(), model.Command{ID: id, Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 32 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snapshot := runtime.Diagnostics()
					if snapshot.CommandQueueDepth > capacity || snapshot.CompletionQueueDepth > capacity || snapshot.CommandRegistryEntries > capacity || snapshot.EventQueueDepth > snapshot.EventQueueCapacity || snapshot.WorkerCount > 1 {
						t.Errorf("unbounded diagnostics: %+v", snapshot)
						return
					}
				}
			}
		}()
	}
	for range capacity {
		if _, err := runtime.gate.NextCompletion(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	forceShutdown(t, runtime)
}

func TestWave2AcceptanceGateShutdownRaceDoesNotDuplicateRuntimeCompletion(t *testing.T) {
	const iterations = 200
	for iteration := range iterations {
		gate, err := commandgate.New(1)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := NewWithDependencies(testConfig(1), gate, Dependencies{Handlers: map[string]Handler{
			"core.status": func(ctx context.Context, _ Services, _ model.Command) (model.Result, error) {
				<-ctx.Done()
				return model.Result{}, ctx.Err()
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("shutdown-race-%03d", iteration)
		admission, err := gate.Submit(context.Background(), model.Command{ID: id, Name: "core.status", Args: model.EmptyArgs{}})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		go func() {
			<-start
			_ = gate.Cancel(admission.CommandHandle)
		}()
		close(start)
		runtime.BeginShutdown()
		completion, err := gate.NextCompletion(context.Background())
		if err != nil || completion.CommandID != id {
			t.Fatalf("iteration %d completion = (%+v, %v)", iteration, completion, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = runtime.Shutdown(ctx)
		if stats := gate.Stats(); stats.CompletionQueueDepth != 0 || stats.RegistryEntries != 0 {
			t.Fatalf("iteration %d duplicate/retained completion: %+v", iteration, stats)
		}
	}
}
