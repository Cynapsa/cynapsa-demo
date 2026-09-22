package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestCoreShutdownCompletionPrecedesRuntimeShutdown(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	shutdownEntered := make(chan struct{})
	shutdownRelease := make(chan struct{})
	defer func() {
		select {
		case <-shutdownRelease:
		default:
			close(shutdownRelease)
		}
	}()
	runtime, err := NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"core.shutdown": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			close(shutdownEntered)
			<-shutdownRelease
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "shutdown", Name: "core.shutdown", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	<-shutdownEntered
	if _, err = gate.Submit(context.Background(), model.Command{ID: "pending", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	close(shutdownRelease)

	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "shutdown" || completion.Err != nil {
		t.Fatalf("first completion = %+v, %v", completion, err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "late", Name: "core.status", Args: model.EmptyArgs{}}); !errors.Is(err, commandgate.ErrGateClosing) && !errors.Is(err, commandgate.ErrGateClosed) {
		t.Fatalf("late admission = %v", err)
	}
	pending, err := gate.NextCompletion(context.Background())
	if err != nil || pending.CommandID != "pending" || pending.Err == nil || !errors.Is(pending.Err.Cause, commandgate.ErrGateClosing) {
		t.Fatalf("pending completion = %+v, %v", pending, err)
	}

	drainShutdownEvents(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if runtime.Status().Lifecycle != model.LifecycleClosed {
		t.Fatalf("lifecycle = %q", runtime.Status().Lifecycle)
	}
}

func TestCoreShutdownLosingOutcomesLeaveRuntimeOpen(t *testing.T) {
	tests := []struct {
		name    string
		handler Handler
	}{
		{
			name: "typed failure",
			handler: func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
				return runtimeFailure(command.ID, "shutdown_failed", ErrHandlerUnavailable), nil
			},
		},
		{
			name: "handler error",
			handler: func(context.Context, Services, model.Command) (model.Result, error) {
				return model.Result{}, ErrHandlerUnavailable
			},
		},
		{
			name: "handler panic",
			handler: func(context.Context, Services, model.Command) (model.Result, error) {
				panic("contained")
			},
		},
		{
			name: "non empty result",
			handler: func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
				return model.Result{CommandID: command.ID, Value: model.StatusResult{Status: model.Status{Lifecycle: model.LifecycleCreated}}}, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate, err := commandgate.New(2)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{"core.shutdown": test.handler}})
			if err != nil {
				t.Fatal(err)
			}
			if err = runtime.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err = gate.Submit(context.Background(), model.Command{ID: "shutdown", Name: "core.shutdown", Args: model.EmptyArgs{}}); err != nil {
				t.Fatal(err)
			}
			completion, err := gate.NextCompletion(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if test.name == "non empty result" {
				if completion.Err != nil {
					t.Fatalf("nonempty completion = %+v", completion)
				}
			} else if completion.Err == nil {
				t.Fatalf("failure completion = %+v", completion)
			}
			if gate.Stats().Closing || runtime.Status().Lifecycle != model.LifecycleCreated {
				t.Fatalf("losing outcome terminalized: gate=%+v lifecycle=%q", gate.Stats(), runtime.Status().Lifecycle)
			}
			if _, err = gate.Submit(context.Background(), model.Command{ID: "still-open", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
				t.Fatalf("admission after losing outcome = %v", err)
			}
			runtime.BeginShutdown()
			if _, err = gate.NextCompletion(context.Background()); err != nil {
				t.Fatal(err)
			}
			drainShutdownEvents(t, runtime)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err = runtime.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCancelledCoreShutdownResultCannotTerminalize(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime, err := NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"core.shutdown": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			close(entered)
			<-release
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	admission, err := gate.Submit(context.Background(), model.Command{ID: "shutdown", Name: "core.shutdown", Args: model.EmptyArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if err = gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatal(err)
	}
	close(release)
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "shutdown" || completion.Err == nil {
		t.Fatalf("cancelled completion = %+v, %v", completion, err)
	}
	if gate.Stats().Closing || runtime.Status().Lifecycle != model.LifecycleCreated {
		t.Fatalf("late successful result terminalized: gate=%+v lifecycle=%q", gate.Stats(), runtime.Status().Lifecycle)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "still-open", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatalf("admission after cancellation = %v", err)
	}
	runtime.BeginShutdown()
	if _, err = gate.NextCompletion(context.Background()); err != nil {
		t.Fatal(err)
	}
	drainShutdownEvents(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
