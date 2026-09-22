package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func FuzzLifecycleRuntimeAgainstReferenceModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 5})
	f.Add([]byte{7, 7, 7, 0, 2, 4, 6})

	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 256 {
			operations = operations[:256]
		}
		runtime := testRuntime(t, 512, Dependencies{})
		if err := runtime.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		states := []model.LifecycleState{
			stateCreated,
			stateAuthenticating,
			stateMeshConnected,
			stateDurableReady,
			statePeerLinkBuilding,
			stateReady,
			stateDegraded,
			stateFailed,
			"hostile-unknown",
		}
		expected := stateCreated
		for step, operation := range operations {
			next := states[int(operation)%len(states)]
			err := runtime.Transition(next)
			allowed := next == expected || referenceAllowsTransition(expected, next)
			if allowed {
				if err != nil {
					t.Fatalf("step %d Transition(%q -> %q) error = %v", step, expected, next, err)
				}
				expected = next
			} else if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("step %d illegal Transition(%q -> %q) error = %v", step, expected, next, err)
			}
			if got := runtime.Status().Lifecycle; got != expected {
				t.Fatalf("step %d lifecycle = %q, reference=%q", step, got, expected)
			}
		}
		forceShutdown(t, runtime)
	})
}

func referenceAllowsTransition(previous, next model.LifecycleState) bool {
	switch previous {
	case stateCreated:
		return next == stateAuthenticating || next == stateFailed
	case stateAuthenticating:
		return next == stateMeshConnected || next == stateFailed
	case stateMeshConnected:
		return next == stateDurableReady || next == stateDegraded || next == stateFailed
	case stateDurableReady:
		return next == statePeerLinkBuilding || next == stateReady || next == stateDegraded || next == stateFailed
	case statePeerLinkBuilding:
		return next == stateReady || next == stateDegraded || next == stateFailed
	case stateReady:
		return next == stateDegraded || next == stateFailed
	case stateDegraded:
		return next == stateReady || next == stateFailed
	default:
		return false
	}
}
