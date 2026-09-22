package commandgate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestBeginShutdownClaimsTerminalBeforeRunningHandlerObservesCancellation(t *testing.T) {
	const iterations = 500
	for iteration := range iterations {
		gate := newTestGate(t, 1)
		commandID := fmt.Sprintf("shutdown-running-%d", iteration)
		mustSubmit(t, gate, commandID)
		entered := make(chan struct{})
		observedState := make(chan CommandState, 1)
		executeDone := make(chan error, 1)
		go func() {
			executeDone <- gate.ExecuteNext(context.Background(), func(ctx context.Context, command model.Command) (model.Result, error) {
				close(entered)
				<-ctx.Done()
				state, ok := gate.registry.State(command.ID)
				if !ok {
					observedState <- 0
				} else {
					observedState <- state
				}
				return model.Result{}, errors.New("handler woke during shutdown")
			})
		}()
		<-entered

		gate.BeginShutdown()
		if state := <-observedState; state != StateTerminal {
			t.Fatalf("iteration %d handler observed state %v, want terminal before cancellation", iteration, state)
		}
		completion := mustCompletion(t, gate)
		assertFailure(t, completion, codeShutdown, ErrGateClosing)
		if err := <-executeDone; err != nil {
			t.Fatalf("iteration %d ExecuteNext() error = %v", iteration, err)
		}
		if err := gate.Shutdown(context.Background()); err != nil {
			t.Fatalf("iteration %d Shutdown() error = %v", iteration, err)
		}
	}
}
