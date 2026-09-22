package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestSuccessfulLogoutCompletionTerminatesRuntime(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"auth.logout": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "logout", Name: "auth.logout", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	completion, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("NextCompletion() error = %v", err)
	}
	if completion.CommandID != "logout" || completion.Err != nil {
		t.Fatalf("completion = %+v, want successful logout", completion)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "late", Name: "auth.logout", Args: model.EmptyArgs{}}); !errors.Is(err, commandgate.ErrGateClosing) && !errors.Is(err, commandgate.ErrGateClosed) {
		t.Fatalf("Submit() after logout error = %v, want closing/closed", err)
	}
	drainShutdownEvents(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := runtime.Status().Lifecycle; got != model.LifecycleClosed {
		t.Fatalf("Status().Lifecycle = %q, want closed", got)
	}
}

func TestFailedLogoutLeavesRuntimeOpen(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"auth.logout": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			return runtimeFailure(command.ID, "logout_failed", ErrHandlerUnavailable), nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "logout", Name: "auth.logout", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	completion, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if completion.Err == nil {
		t.Fatalf("completion = %+v, want failure", completion)
	}
	if got := runtime.Status().Lifecycle; got != model.LifecycleCreated {
		t.Fatalf("Status().Lifecycle = %q, want created", got)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "retry", Name: "auth.logout", Args: model.EmptyArgs{}}); err != nil {
		t.Fatalf("Submit() after failed logout error = %v", err)
	}
	runtime.BeginShutdown()
	if _, err := gate.NextCompletion(context.Background()); err != nil {
		t.Fatalf("NextCompletion(retry shutdown) error = %v", err)
	}
	drainShutdownEvents(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func drainShutdownEvents(t *testing.T, runtime *Runtime) {
	t.Helper()
	for _, want := range []model.LifecycleState{model.LifecycleClosing, model.LifecycleClosed} {
		event, err := runtime.NextEvent(context.Background())
		if err != nil {
			t.Fatalf("NextEvent(%s) error = %v", want, err)
		}
		state, ok := event.Value.(model.SessionStateChangedEvent)
		if !ok || state.Current != want {
			t.Fatalf("NextEvent(%s) = %+v", want, event)
		}
	}
}
