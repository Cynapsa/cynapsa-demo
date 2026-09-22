package commandgate

import (
	"context"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestExecuteNextTerminatingOnSuccessPublishesBeforeAdmissionCutoff(t *testing.T) {
	gate, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []model.Command{
		{ID: "logout", Name: "auth.logout", Args: model.EmptyArgs{}},
		{ID: "pending", Name: "core.status", Args: model.EmptyArgs{}},
	} {
		if _, err := gate.Submit(context.Background(), command); err != nil {
			t.Fatalf("Submit(%s) error = %v", command.ID, err)
		}
	}

	err = gate.ExecuteNextTerminatingOnSuccess(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
		return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
	})
	if err != nil {
		t.Fatalf("ExecuteNextTerminatingOnSuccess() error = %v", err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "late", Name: "core.status", Args: model.EmptyArgs{}}); !errors.Is(err, ErrGateClosing) {
		t.Fatalf("Submit() after terminal completion error = %v, want ErrGateClosing", err)
	}

	first, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("NextCompletion(first) error = %v", err)
	}
	if first.CommandID != "logout" || first.Err != nil {
		t.Fatalf("first completion = %+v, want successful logout", first)
	}
	second, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("NextCompletion(second) error = %v", err)
	}
	if second.CommandID != "pending" || second.Err == nil || !errors.Is(second.Err.Cause, ErrGateClosing) {
		t.Fatalf("second completion = %+v, want pending shutdown completion", second)
	}
}

func TestExecuteNextTerminatingOnSuccessFailureDoesNotClose(t *testing.T) {
	tests := []struct {
		name    string
		handler Handler
	}{
		{
			name: "typed failure",
			handler: func(_ context.Context, command model.Command) (model.Result, error) {
				return failureResult(command.ID, "logout_failed", ErrHandlerFailed), nil
			},
		},
		{
			name: "handler error",
			handler: func(context.Context, model.Command) (model.Result, error) {
				return model.Result{}, ErrHandlerFailed
			},
		},
		{
			name: "handler panic",
			handler: func(context.Context, model.Command) (model.Result, error) {
				panic("contained")
			},
		},
		{
			name: "non-empty success",
			handler: func(_ context.Context, command model.Command) (model.Result, error) {
				return model.Result{CommandID: command.ID, Value: model.AgentIDResult{AgentID: "agent"}}, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate, err := New(2)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := gate.Submit(context.Background(), model.Command{ID: "logout", Name: "auth.logout", Args: model.EmptyArgs{}}); err != nil {
				t.Fatal(err)
			}
			if err := gate.ExecuteNextTerminatingOnSuccess(context.Background(), test.handler); err != nil {
				t.Fatalf("ExecuteNextTerminatingOnSuccess() error = %v", err)
			}
			if _, err := gate.NextCompletion(context.Background()); err != nil {
				t.Fatalf("NextCompletion() error = %v", err)
			}
			if _, err := gate.Submit(context.Background(), model.Command{ID: "still-open", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
				t.Fatalf("Submit() after failed/non-empty logout error = %v", err)
			}
			gate.BeginShutdown()
			if _, err := gate.NextCompletion(context.Background()); err != nil {
				t.Fatalf("NextCompletion(shutdown) error = %v", err)
			}
		})
	}
}

func TestExecuteNextTerminalCommandSetContainsOnlyLogoutAndCoreShutdown(t *testing.T) {
	for _, name := range []string{"auth.logout", "core.shutdown"} {
		t.Run(name, func(t *testing.T) {
			gate, err := New(1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = gate.Submit(context.Background(), model.Command{ID: "terminal", Name: name, Args: model.EmptyArgs{}}); err != nil {
				t.Fatal(err)
			}
			if err = gate.ExecuteNextTerminatingOnSuccess(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
				return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
			}); err != nil {
				t.Fatal(err)
			}
			if !gate.Stats().Closing {
				t.Fatalf("%s did not close gate", name)
			}
			completion, err := gate.NextCompletion(context.Background())
			if err != nil || completion.CommandID != "terminal" || completion.Err != nil {
				t.Fatalf("terminal completion = %+v, %v", completion, err)
			}
		})
	}

	gate, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "ordinary", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	if err = gate.ExecuteNextTerminatingOnSuccess(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
		return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if gate.Stats().Closing {
		t.Fatal("nonterminal command closed gate")
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "still-open", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatalf("ordinary command changed admission: %v", err)
	}
	gate.BeginShutdown()
	for range 2 {
		if _, err = gate.NextCompletion(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExecuteNextDispositionReportsCancellationWinner(t *testing.T) {
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := gate.Submit(context.Background(), model.Command{ID: "delivery", Name: "delivery.next", Args: model.EmptyArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	disposition := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- gate.ExecuteNextWithDisposition(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
			close(started)
			<-release
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		}, func(_ model.Command, _ model.Result, committed bool) {
			disposition <- committed
		})
	}()
	<-started
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("ExecuteNextWithDisposition() error = %v", err)
	}
	if committed := <-disposition; committed {
		t.Fatal("disposition reported completion win after cancellation")
	}
}

func TestExecuteNextDispositionReportsShutdownWinner(t *testing.T) {
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "delivery", Name: "delivery.next", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	disposition := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- gate.ExecuteNextWithDisposition(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
			close(started)
			<-release
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		}, func(_ model.Command, _ model.Result, committed bool) {
			disposition <- committed
		})
	}()
	<-started
	gate.BeginShutdown()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("ExecuteNextWithDisposition() error = %v", err)
	}
	if committed := <-disposition; committed {
		t.Fatal("disposition reported completion win after shutdown")
	}
}
