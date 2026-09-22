package commandgate

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{-1, 0} {
		capacity := capacity
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			t.Parallel()
			gate, err := New(capacity)
			if gate != nil || !errors.Is(err, ErrInvalidCapacity) {
				t.Fatalf("New(%d) = (%v, %v), want (nil, ErrInvalidCapacity)", capacity, gate, err)
			}
		})
	}
}

func TestRegistryRejectsNonPositiveCapacity(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{-1, 0} {
		registry := NewRegistry(capacity)
		if _, err := registry.Register("cmd"); !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("NewRegistry(%d).Register error = %v, want ErrInvalidCapacity", capacity, err)
		}
	}
}

func TestSubmitTransfersCommandOwnershipWithoutMutation(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	owned := &model.MessageIDArgs{MessageID: "original"}
	command := model.Command{ID: "cmd-owned", Name: "test", SessionID: "session", Args: owned}

	admission, err := gate.Submit(context.Background(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("Submit() = (%+v, %v), want accepted", admission, err)
	}
	dispatch := mustDispatch(t, gate, "cmd-owned")
	if dispatch.Command.ID != command.ID || dispatch.Command.Name != command.Name || dispatch.Command.SessionID != command.SessionID {
		t.Fatalf("dispatched command = %+v, want metadata preserved", dispatch.Command)
	}
	gotArgs, ok := dispatch.Command.Args.(*model.MessageIDArgs)
	if !ok || gotArgs == owned || gotArgs.MessageID != "original" || owned.MessageID != "original" {
		t.Fatalf("dispatched Args = %#v, want independent snapshot of %#v", dispatch.Command.Args, owned)
	}
}

func TestAdmissionIsSeparateFromCompletion(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	admission, err := gate.Submit(context.Background(), testCommand("cmd-admission"))
	if err != nil || !admission.Accepted {
		t.Fatalf("Submit() = (%+v, %v), want accepted", admission, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.NextCompletion(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("NextCompletion() error = %v, want context.Canceled", err)
	}
}

func TestWaitOperationsRespectCancelledAndNilContexts(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 2)
	if _, err := gate.Submit(nil, testCommand("nil-submit")); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Submit(nil) error = %v, want ErrNilContext", err)
	}
	if _, err := gate.NextDispatch(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("NextDispatch(nil) error = %v, want ErrNilContext", err)
	}
	if _, err := gate.NextCompletion(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("NextCompletion(nil) error = %v, want ErrNilContext", err)
	}
	if err := gate.ClearCompletions(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("ClearCompletions(nil) error = %v, want ErrNilContext", err)
	}
	if err := gate.Shutdown(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Shutdown(nil) error = %v, want ErrNilContext", err)
	}

	mustSubmit(t, gate, "completion-command")
	completionDispatch := mustDispatch(t, gate, "completion-command")
	if err := gate.Complete(completionDispatch.Completion, testEmptyResult("completion-command")); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, gate, "ready-command")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.NextDispatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("NextDispatch(cancelled with ready command) error = %v, want context.Canceled", err)
	}
	if _, err := gate.NextCompletion(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("NextCompletion(cancelled with ready completion) error = %v, want context.Canceled", err)
	}
}

func TestCapacityOneSaturatesUntilCompletionIsConsumed(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "cmd-1")
	if _, err := gate.Submit(context.Background(), testCommand("cmd-2")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("second Submit() error = %v, want ErrRegistryFull", err)
	}

	dispatch := mustDispatch(t, gate, "cmd-1")
	if _, err := gate.Submit(context.Background(), testCommand("cmd-2")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("Submit() after dispatch error = %v, want ErrRegistryFull", err)
	}
	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "cmd-1", Value: model.AgentIDResult{AgentID: "done"}}); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if _, err := gate.Submit(context.Background(), testCommand("cmd-2")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("Submit() before completion consumption error = %v, want ErrRegistryFull", err)
	}
	completion := mustCompletion(t, gate)
	done, ok := completion.Value.(model.AgentIDResult)
	if completion.CommandID != "cmd-1" || !ok || done.AgentID != "done" {
		t.Fatalf("completion = %+v, want cmd-1 done", completion)
	}
	mustSubmit(t, gate, "cmd-2")
}

func TestNormalCapacityReservesEveryCompletionSlot(t *testing.T) {
	t.Parallel()
	const capacity = 8
	gate := newTestGate(t, capacity)
	for index := 0; index < capacity; index++ {
		mustSubmit(t, gate, fmt.Sprintf("cmd-%d", index))
	}
	dispatches := make([]Dispatch, 0, capacity)
	for index := 0; index < capacity; index++ {
		dispatches = append(dispatches, mustDispatch(t, gate, fmt.Sprintf("cmd-%d", index)))
	}
	for index := 0; index < capacity; index++ {
		if err := gate.Complete(dispatches[index].Completion, testEmptyResult(fmt.Sprintf("cmd-%d", index))); err != nil {
			t.Fatalf("Complete(%d) error = %v", index, err)
		}
	}
	if len(gate.completions) != capacity {
		t.Fatalf("completion depth = %d, want %d", len(gate.completions), capacity)
	}
	if _, err := gate.Submit(context.Background(), testCommand("overflow")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("overflow Submit() error = %v, want ErrRegistryFull", err)
	}
	for range capacity {
		mustCompletion(t, gate)
	}
	if len(gate.completions) != 0 || gate.registry.Len() != 0 {
		t.Fatalf("after drain: completions=%d registry=%d, want zero", len(gate.completions), gate.registry.Len())
	}
}

func TestSubmitRejectsEmptyDuplicateAndCancelledAdmission(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 2)
	admission, err := gate.Submit(context.Background(), testCommand(""))
	if !errors.Is(err, ErrEmptyCommandID) || admission.Reason != ReasonInvalidCommandID {
		t.Fatalf("empty Submit() = (%+v, %v)", admission, err)
	}
	mustSubmit(t, gate, "duplicate")
	admission, err = gate.Submit(context.Background(), testCommand("duplicate"))
	if !errors.Is(err, ErrDuplicateCommandID) || admission.Reason != ReasonDuplicate {
		t.Fatalf("duplicate Submit() = (%+v, %v)", admission, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	admission, err = gate.Submit(ctx, testCommand("cancelled-before-admission"))
	if !errors.Is(err, context.Canceled) || admission.Reason != ReasonContextCancelled {
		t.Fatalf("cancelled Submit() = (%+v, %v)", admission, err)
	}
	if _, ok := gate.registry.State("cancelled-before-admission"); ok {
		t.Fatal("cancelled pre-admission command was registered")
	}
}

func TestCancelUnknownAndEmptyIdentifiers(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	if err := gate.Cancel(""); !errors.Is(err, ErrEmptyCommandHandle) {
		t.Fatalf("Cancel(empty) error = %v, want ErrEmptyCommandHandle", err)
	}
	if err := gate.Cancel("not-admitted"); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("Cancel(unknown) error = %v, want ErrCommandNotFound", err)
	}
}

func TestCancelBeforeDispatchPublishesOnlyCancellation(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	admission := mustSubmit(t, gate, "cmd-cancel")
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	completion := mustCompletion(t, gate)
	assertFailure(t, completion, codeCancelled, ErrCommandCancelled)
	if err := gate.Cancel(admission.CommandHandle); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("second Cancel() error = %v, want ErrAlreadyTerminal", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.NextDispatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("NextDispatch() error = %v, want context.Canceled", err)
	}
}

func TestCancelDuringExecutionCancelsCommandContext(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	admission := mustSubmit(t, gate, "cmd-running")
	dispatch, err := gate.NextDispatch(context.Background())
	if err != nil {
		t.Fatalf("NextDispatch() error = %v", err)
	}
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	<-dispatch.Context.Done()
	if !errors.Is(context.Cause(dispatch.Context), ErrCommandCancelled) {
		t.Fatalf("dispatch context cause = %v, want ErrCommandCancelled", context.Cause(dispatch.Context))
	}
	assertFailure(t, mustCompletion(t, gate), codeCancelled, ErrCommandCancelled)
	if err := gate.Complete(dispatch.Completion, testEmptyResult("cmd-running")); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("Complete() after cancel error = %v, want ErrAlreadyTerminal", err)
	}
}

func TestNextAlwaysReturnsContextAndCompletionCapability(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	admission := mustSubmit(t, gate, "cmd-next")
	dispatch, err := gate.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if dispatch.Command.ID != "cmd-next" || dispatch.Context == nil || dispatch.Completion.token == nil {
		t.Fatalf("Next() dispatch = %+v, want command, context, and completion capability", dispatch)
	}
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	<-dispatch.Context.Done()
	if !errors.Is(context.Cause(dispatch.Context), ErrCommandCancelled) {
		t.Fatalf("Next() context cause = %v, want ErrCommandCancelled", context.Cause(dispatch.Context))
	}
}

func TestCompleteIsExactlyOnceUnderConcurrentAttempts(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "cmd-race")
	dispatch := mustDispatch(t, gate, "cmd-race")

	const contenders = 32
	start := make(chan struct{})
	results := make(chan error, contenders)
	var workers sync.WaitGroup
	workers.Add(contenders)
	for index := 0; index < contenders; index++ {
		index := index
		go func() {
			defer workers.Done()
			<-start
			results <- gate.Complete(dispatch.Completion, model.Result{CommandID: "cmd-race", Value: model.PayloadHandleResult{Size: uint64(index)}})
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	winners := 0
	terminal := 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrAlreadyTerminal):
			terminal++
		default:
			t.Fatalf("Complete() contender error = %v", err)
		}
	}
	if winners != 1 || terminal != contenders-1 {
		t.Fatalf("winners=%d terminal=%d, want 1 and %d", winners, terminal, contenders-1)
	}
	mustCompletion(t, gate)
	if err := gate.Complete(dispatch.Completion, testEmptyResult("cmd-race")); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("post-consumption Complete() error = %v, want ErrAlreadyTerminal", err)
	}
}

func TestStaleCompletionCannotCompleteReusedCommandID(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)

	oldAdmission := mustSubmit(t, gate, "reused")
	stale := mustDispatch(t, gate, "reused")
	if err := gate.Cancel(oldAdmission.CommandHandle); err != nil {
		t.Fatalf("Cancel(reused) error = %v", err)
	}
	mustCompletion(t, gate)

	// Complete another command to evict the one-entry terminal history for
	// "reused", making that command ID eligible for a new admission.
	mustSubmit(t, gate, "evictor")
	evictor := mustDispatch(t, gate, "evictor")
	if err := gate.Complete(evictor.Completion, testEmptyResult("evictor")); err != nil {
		t.Fatalf("Complete(evictor) error = %v", err)
	}
	mustCompletion(t, gate)

	mustSubmit(t, gate, "reused")
	current := mustDispatch(t, gate, "reused")
	if err := gate.Cancel(oldAdmission.CommandHandle); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("stale Cancel() error = %v, want ErrCommandNotFound", err)
	}
	if state, ok := gate.registry.State("reused"); !ok || state != StateDispatched {
		t.Fatalf("stale cancellation changed current state = (%v, %v)", state, ok)
	}
	staleValue := &model.PayloadHandleResult{Handle: "stale"}
	staleResult := model.Result{CommandID: "reused", Value: staleValue}
	if err := gate.Complete(stale.Completion, staleResult); !errors.Is(err, ErrStaleCompletionCapability) {
		t.Fatalf("stale Complete() error = %v, want ErrStaleCompletionCapability", err)
	}
	if staleResult.Value != staleValue || staleValue.Handle != "stale" {
		t.Fatalf("rejected stale result was mutated: %+v", staleResult)
	}
	if state, ok := gate.registry.State("reused"); !ok || state != StateDispatched {
		t.Fatalf("current reused state = (%v, %v), want dispatched", state, ok)
	}

	currentValue := &model.PayloadHandleResult{Handle: "current"}
	if err := gate.Complete(current.Completion, model.Result{CommandID: "reused", Value: currentValue}); err != nil {
		t.Fatalf("current Complete() error = %v", err)
	}
	completion := mustCompletion(t, gate)
	if completion.Value != currentValue {
		t.Fatalf("completion Value = %#v, want current value %#v", completion.Value, currentValue)
	}
}

func TestCommandHandleIsHighEntropyUniqueAndCoreScoped(t *testing.T) {
	t.Parallel()
	first := newTestGate(t, 2)
	second := newTestGate(t, 1)
	firstA := mustSubmit(t, first, "a")
	firstB := mustSubmit(t, first, "b")
	secondA := mustSubmit(t, second, "a")

	handles := []CommandHandle{firstA.CommandHandle, firstB.CommandHandle, secondA.CommandHandle}
	seen := make(map[CommandHandle]struct{}, len(handles))
	for _, handle := range handles {
		if _, duplicate := seen[handle]; duplicate {
			t.Fatalf("duplicate command handle %q", handle)
		}
		seen[handle] = struct{}{}
		encoded := string(handle)
		if len(encoded) <= len(commandHandlePrefix) || encoded[:len(commandHandlePrefix)] != commandHandlePrefix {
			t.Fatalf("command handle %q lacks opaque prefix", handle)
		}
		raw, err := base64.RawURLEncoding.DecodeString(encoded[len(commandHandlePrefix):])
		if err != nil || len(raw) != commandHandleRawSize {
			t.Fatalf("command handle %q decoded to %d bytes, error=%v", handle, len(raw), err)
		}
	}

	if err := second.Cancel(firstA.CommandHandle); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("cross-core Cancel() error = %v, want ErrCommandNotFound", err)
	}
	if state, ok := second.registry.State("a"); !ok || state != StateAdmitted {
		t.Fatalf("cross-core handle changed target state = (%v, %v)", state, ok)
	}
	if err := second.Cancel(secondA.CommandHandle); err != nil {
		t.Fatalf("Cancel(valid target handle) error = %v", err)
	}
	assertFailure(t, mustCompletion(t, second), codeCancelled, ErrCommandCancelled)

	if err := first.Cancel(firstA.CommandHandle); err != nil {
		t.Fatalf("Cancel(first a) error = %v", err)
	}
	assertFailure(t, mustCompletion(t, first), codeCancelled, ErrCommandCancelled)
	if state, ok := first.registry.State("b"); !ok || state != StateAdmitted {
		t.Fatalf("handle for a changed b state = (%v, %v)", state, ok)
	}
	if err := first.Cancel(firstB.CommandHandle); err != nil {
		t.Fatalf("Cancel(first b) error = %v", err)
	}
	assertFailure(t, mustCompletion(t, first), codeCancelled, ErrCommandCancelled)
}

func TestCompletionOwnershipTransferAndRejectedRetention(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "owned-result")
	dispatch := mustDispatch(t, gate, "owned-result")
	value := &model.PayloadHandleResult{Handle: "result"}
	privateErr := &model.Error{Code: "test", Stage: "command", DiagnosticID: "owned"}
	result := model.Result{CommandID: "owned-result", Value: value, Err: privateErr}
	if err := gate.Complete(dispatch.Completion, result); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	completion := mustCompletion(t, gate)
	if completion.Value != value || completion.Err != privateErr || completion.Err.DiagnosticID != "owned" {
		t.Fatalf("completion did not preserve transferred ownership: %+v", completion)
	}

	rejectedValue := &model.PayloadHandleResult{Handle: "retained"}
	rejected := model.Result{CommandID: "owned-result", Value: rejectedValue}
	if err := gate.Complete(CompletionCapability{}, rejected); !errors.Is(err, ErrInvalidCompletionCapability) {
		t.Fatalf("Complete(invalid capability) error = %v, want ErrInvalidCompletionCapability", err)
	}
	if rejected.Value != rejectedValue || rejectedValue.Handle != "retained" {
		t.Fatalf("rejected result ownership was not retained: %+v", rejected)
	}
}

func TestCancelAndCompleteRaceProducesOneCompletion(t *testing.T) {
	t.Parallel()
	for iteration := 0; iteration < 200; iteration++ {
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		commandID := fmt.Sprintf("race-%d", iteration)
		admission := mustSubmit(t, gate, commandID)
		dispatch := mustDispatch(t, gate, commandID)
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			errs <- gate.Cancel(admission.CommandHandle)
		}()
		go func() {
			<-start
			errs <- gate.Complete(dispatch.Completion, testEmptyResult(commandID))
		}()
		close(start)
		first, second := <-errs, <-errs
		if (first == nil) == (second == nil) {
			t.Fatalf("iteration %d errors = (%v, %v), want one winner", iteration, first, second)
		}
		loser := first
		if first == nil {
			loser = second
		}
		if !errors.Is(loser, ErrAlreadyTerminal) {
			t.Fatalf("iteration %d loser error = %v, want ErrAlreadyTerminal", iteration, loser)
		}
		completion := mustCompletion(t, gate)
		if completion.CommandID != commandID {
			t.Fatalf("iteration %d completion ID = %q", iteration, completion.CommandID)
		}
		closeTestGate(gate)
	}
}

func TestExecuteNextNormalizesHandlerErrorAndPanic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		handler Handler
		code    string
		cause   error
	}{
		{
			name: "error",
			handler: func(context.Context, model.Command) (model.Result, error) {
				return model.Result{}, ErrHandlerFailed
			},
			code:  codeHandler,
			cause: ErrHandlerFailed,
		},
		{
			name: "panic",
			handler: func(context.Context, model.Command) (model.Result, error) {
				panic("secret panic detail")
			},
			code:  codeHandlerPanic,
			cause: ErrHandlerPanic,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gate := newTestGate(t, 1)
			mustSubmit(t, gate, "cmd-"+test.name)
			if err := gate.ExecuteNext(context.Background(), test.handler); err != nil {
				t.Fatalf("ExecuteNext() error = %v", err)
			}
			assertFailure(t, mustCompletion(t, gate), test.code, test.cause)
		})
	}
}

func TestCancellationWinsAgainstRunningHandler(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 1)
	admission := mustSubmit(t, gate, "cmd-handler-cancel")
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- gate.ExecuteNext(context.Background(), func(ctx context.Context, _ model.Command) (model.Result, error) {
			close(started)
			<-ctx.Done()
			return model.Result{}, ctx.Err()
		})
	}()
	<-started
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ExecuteNext() error = %v", err)
	}
	assertFailure(t, mustCompletion(t, gate), codeCancelled, ErrCommandCancelled)
}

func TestShutdownTerminalizesPendingCommandsAndDrains(t *testing.T) {
	t.Parallel()
	gate, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		mustSubmit(t, gate, fmt.Sprintf("cmd-%d", index))
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- gate.Shutdown(context.Background()) }()
	<-gate.shutdownCh

	seen := make(map[string]bool)
	for range 3 {
		completion := mustCompletion(t, gate)
		assertFailure(t, completion, codeShutdown, ErrGateClosing)
		seen[completion.CommandID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("shutdown completions = %v, want three unique IDs", seen)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	admission, err := gate.Submit(context.Background(), testCommand("after-shutdown"))
	if !errors.Is(err, ErrGateClosed) || admission.Reason != ReasonShuttingDown {
		t.Fatalf("Submit after Shutdown = (%+v, %v)", admission, err)
	}
	if _, err := gate.NextCompletion(context.Background()); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("NextCompletion after Shutdown error = %v, want ErrGateClosed", err)
	}
}

func TestShutdownDeadlineAbandonsOnlyLocalOutput(t *testing.T) {
	t.Parallel()
	gate, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, gate, "cmd-1")
	mustSubmit(t, gate, "cmd-2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v, want context.Canceled", err)
	}
	<-gate.finalizeDone
	if gate.registry.Len() != 0 || len(gate.commands) != 0 || len(gate.completions) != 0 {
		t.Fatalf("abandoned resources: registry=%d commands=%d completions=%d", gate.registry.Len(), len(gate.commands), len(gate.completions))
	}
	if _, err := gate.NextCompletion(context.Background()); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("NextCompletion() error = %v, want ErrGateClosed", err)
	}
}

func TestConcurrentShutdownCallsAreIdempotent(t *testing.T) {
	t.Parallel()
	gate, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		mustSubmit(t, gate, fmt.Sprintf("cmd-%d", index))
	}

	const callers = 16
	errs := make(chan error, callers)
	var workers sync.WaitGroup
	workers.Add(callers)
	for range callers {
		go func() {
			defer workers.Done()
			errs <- gate.Shutdown(context.Background())
		}()
	}
	<-gate.shutdownCh
	for range 4 {
		mustCompletion(t, gate)
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Shutdown() error = %v", err)
		}
	}
	if err := gate.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown() error = %v", err)
	}
}

func TestClearCompletionsRequiresShutdownAndDrains(t *testing.T) {
	t.Parallel()
	gate, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ClearCompletions(context.Background()); !errors.Is(err, ErrGateNotClosing) {
		t.Fatalf("ClearCompletions before shutdown error = %v, want ErrGateNotClosing", err)
	}
	mustSubmit(t, gate, "cmd-1")
	mustSubmit(t, gate, "cmd-2")
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- gate.Shutdown(context.Background()) }()
	<-gate.shutdownCh
	if err := gate.ClearCompletions(context.Background()); err != nil {
		t.Fatalf("ClearCompletions() error = %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestCompletionLeaseRollbackRestoresExactHeadAndCommitConsumesOnce(t *testing.T) {
	gate := newTestGate(t, 2)
	for _, id := range []string{"first", "second"} {
		mustSubmit(t, gate, id)
		dispatch := mustDispatch(t, gate, id)
		if err := gate.Complete(dispatch.Completion, testEmptyResult(id)); err != nil {
			t.Fatal(err)
		}
	}

	first, lease, err := gate.ReserveCompletion(context.Background())
	if err != nil || first.CommandID != "first" {
		t.Fatalf("ReserveCompletion() = %+v, %v", first, err)
	}
	if got := gate.Stats().CompletionQueueDepth; got != 2 {
		t.Fatalf("reserved completion depth = %d, want 2", got)
	}
	if err = gate.RollbackCompletion(lease); err != nil {
		t.Fatal(err)
	}
	if got := gate.Stats().CompletionQueueDepth; got != 2 {
		t.Fatalf("rolled-back completion depth = %d, want 2", got)
	}
	firstAgain, lease, err := gate.ReserveCompletion(context.Background())
	if err != nil || firstAgain.CommandID != "first" {
		t.Fatalf("ReserveCompletion(after rollback) = %+v, %v", firstAgain, err)
	}
	if err = gate.CommitCompletion(lease); err != nil {
		t.Fatal(err)
	}
	if got := gate.Stats().CompletionQueueDepth; got != 1 {
		t.Fatalf("committed completion depth = %d, want 1", got)
	}
	second, err := gate.NextCompletion(context.Background())
	if err != nil || second.CommandID != "second" {
		t.Fatalf("NextCompletion(second) = %+v, %v", second, err)
	}
	if stats := gate.Stats(); stats.RegistryEntries != 0 || stats.CompletionQueueDepth != 0 {
		t.Fatalf("post-commit accounting = %+v", stats)
	}
}

func TestCompletionLeaseConsumerWaitHonorsCancellation(t *testing.T) {
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "owned")
	dispatch := mustDispatch(t, gate, "owned")
	if err := gate.Complete(dispatch.Completion, testEmptyResult("owned")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := gate.ReserveCompletion(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, reserveErr := gate.ReserveCompletion(ctx)
		done <- reserveErr
	}()
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReserveCompletion() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReserveCompletion did not release its consumer wait on cancellation")
	}
	if err = gate.RollbackCompletion(lease); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionLeaseStatsStayExactAcrossReserveRollbackAndShutdown(t *testing.T) {
	const capacity = 8
	gate := newTestGate(t, capacity)
	for index := range capacity {
		id := fmt.Sprintf("cmd-%d", index)
		mustSubmit(t, gate, id)
		dispatch := mustDispatch(t, gate, id)
		if err := gate.Complete(dispatch.Completion, testEmptyResult(id)); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	statsErr := make(chan int, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if depth := gate.Stats().CompletionQueueDepth; depth != capacity {
				statsErr <- depth
				return
			}
		}
	}()
	for range 1_000 {
		_, lease, err := gate.ReserveCompletion(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err = gate.RollbackCompletion(lease); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	select {
	case depth := <-statsErr:
		t.Fatalf("completion depth during reserve/rollback = %d, want %d", depth, capacity)
	default:
	}

	_, lease, err := gate.ReserveCompletion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- gate.Shutdown(context.Background()) }()
	<-gate.shutdownCh
	if got := gate.Stats().CompletionQueueDepth; got != capacity {
		t.Fatalf("shutdown with active completion lease depth = %d, want %d", got, capacity)
	}
	if err = gate.RollbackCompletion(lease); err != nil {
		t.Fatal(err)
	}
	if err = gate.ClearCompletions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineDoesNotWaitForRetainedCompletionLease(t *testing.T) {
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "retained-completion")
	dispatch := mustDispatch(t, gate, "retained-completion")
	if err := gate.Complete(dispatch.Completion, testEmptyResult("retained-completion")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := gate.ReserveCompletion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err = gate.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Shutdown exceeded caller bound: %v", elapsed)
	}
	select {
	case <-gate.finalizeDone:
		t.Fatal("cleanup completed while the completion lease was retained")
	default:
	}
	statsDone := make(chan Stats, 1)
	go func() { statsDone <- gate.Stats() }()
	select {
	case stats := <-statsDone:
		if !stats.Closing || stats.Closed || stats.RegistryEntries != 1 {
			t.Fatalf("retained-lease stats = %+v", stats)
		}
	case <-time.After(time.Second):
		t.Fatal("Stats blocked behind retained completion lease")
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if secondErr := gate.Shutdown(secondCtx); !errors.Is(secondErr, context.DeadlineExceeded) {
		secondCancel()
		t.Fatalf("second Shutdown() = %v, want context deadline", secondErr)
	}
	secondCancel()
	if err = gate.CommitCompletion(lease); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.finalizeDone:
	case <-time.After(time.Second):
		t.Fatal("lifecycle cleanup did not finish after lease return")
	}
	if err = gate.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNextCompletionAndClearCompletionsShareOneConsumer(t *testing.T) {
	t.Parallel()
	for iteration := 0; iteration < 200; iteration++ {
		gate, err := New(4)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 4; index++ {
			mustSubmit(t, gate, fmt.Sprintf("cmd-%d", index))
		}
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- gate.Shutdown(context.Background()) }()
		<-gate.shutdownCh

		start := make(chan struct{})
		nextResults := make(chan error, 4)
		var consumers sync.WaitGroup
		consumers.Add(5)
		for range 4 {
			go func() {
				defer consumers.Done()
				<-start
				_, err := gate.NextCompletion(context.Background())
				nextResults <- err
			}()
		}
		clearResult := make(chan error, 1)
		go func() {
			defer consumers.Done()
			<-start
			clearResult <- gate.ClearCompletions(context.Background())
		}()
		close(start)
		consumers.Wait()
		close(nextResults)

		for err := range nextResults {
			if err != nil && !errors.Is(err, ErrGateClosed) && !errors.Is(err, ErrGateClosing) {
				t.Fatalf("iteration %d NextCompletion() error = %v", iteration, err)
			}
		}
		if err := <-clearResult; err != nil && !errors.Is(err, ErrGateClosed) {
			t.Fatalf("iteration %d ClearCompletions() error = %v", iteration, err)
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("iteration %d Shutdown() error = %v", iteration, err)
		}
	}
}

func TestShutdownRacesCompletionExactlyOnce(t *testing.T) {
	t.Parallel()
	for iteration := 0; iteration < 100; iteration++ {
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		commandID := fmt.Sprintf("cmd-%d", iteration)
		mustSubmit(t, gate, commandID)
		dispatch := mustDispatch(t, gate, commandID)
		start := make(chan struct{})
		completeErr := make(chan error, 1)
		shutdownDone := make(chan error, 1)
		go func() {
			<-start
			completeErr <- gate.Complete(dispatch.Completion, testEmptyResult(commandID))
		}()
		go func() {
			<-start
			shutdownDone <- gate.Shutdown(context.Background())
		}()
		close(start)
		completion := mustCompletion(t, gate)
		if completion.CommandID != commandID {
			t.Fatalf("iteration %d completion ID = %q", iteration, completion.CommandID)
		}
		if err := <-completeErr; err != nil && !errors.Is(err, ErrAlreadyTerminal) && !errors.Is(err, ErrGateClosed) {
			t.Fatalf("iteration %d Complete() error = %v", iteration, err)
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("iteration %d Shutdown() error = %v", iteration, err)
		}
	}
}

func TestRegistryStateCleanupAndBoundedTerminalHistory(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(2)
	if registry.Capacity() != 2 {
		t.Fatalf("Capacity() = %d, want 2", registry.Capacity())
	}
	if _, err := registry.Register(""); !errors.Is(err, ErrEmptyCommandID) {
		t.Fatalf("Register(empty) error = %v", err)
	}
	handleA, err := registry.Register("a")
	if err != nil {
		t.Fatal(err)
	}
	if state, ok := registry.State("a"); !ok || state != StateAdmitted {
		t.Fatalf("State(a) = (%v, %v), want admitted", state, ok)
	}
	registry.Remove("a")
	if registry.Len() != 1 {
		t.Fatal("Remove deleted a nonterminal entry")
	}
	_, completion, err := registry.markDispatched("a")
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := registry.State("a"); state != StateDispatched {
		t.Fatalf("State(a) = %v, want dispatched", state)
	}
	if err := registry.complete(completion, testEmptyResult("a")); err != nil {
		t.Fatal(err)
	}
	if !registry.consumeTerminal("a") || registry.Len() != 0 {
		t.Fatal("terminal consumption did not release live capacity")
	}
	if _, err := registry.cancelCommand(handleA, ErrCommandCancelled); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("cancel terminal a error = %v, want ErrAlreadyTerminal", err)
	}

	completeRegistryCommand(t, registry, "b")
	completeRegistryCommand(t, registry, "c") // evicts bounded tombstone a
	if _, err := registry.cancelCommand(handleA, ErrCommandCancelled); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("cancel evicted terminal a error = %v, want ErrCommandNotFound", err)
	}
	if _, err := registry.Register("a"); err != nil {
		t.Fatalf("Register(a) after bounded tombstone eviction error = %v", err)
	}
	if len(registry.terminalOrder) > registry.Capacity() || len(registry.terminalSet) > registry.Capacity() || len(registry.terminalHandles) > registry.Capacity() {
		t.Fatalf("terminal history grew beyond capacity: order=%d IDs=%d handles=%d", len(registry.terminalOrder), len(registry.terminalSet), len(registry.terminalHandles))
	}
	registry.reset()
}

func TestConcurrentStatusMethodsAndTransitions(t *testing.T) {
	t.Parallel()
	gate := newTestGate(t, 64)
	const commands = 64
	var accepted atomic.Int64
	var completed atomic.Int64
	var workers sync.WaitGroup
	workers.Add(commands)
	for index := 0; index < commands; index++ {
		index := index
		go func() {
			defer workers.Done()
			commandID := fmt.Sprintf("cmd-%d", index)
			if _, err := gate.Submit(context.Background(), testCommand(commandID)); err != nil {
				t.Errorf("Submit(%s) error = %v", commandID, err)
				return
			}
			accepted.Add(1)
			if _, ok := gate.registry.State(commandID); !ok {
				t.Errorf("State(%s) not found", commandID)
			}
		}()
	}
	workers.Wait()
	if accepted.Load() != commands {
		t.Fatalf("accepted = %d, want %d", accepted.Load(), commands)
	}
	dispatches := make(map[string]Dispatch, commands)
	for range commands {
		dispatch, err := gate.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if _, duplicate := dispatches[dispatch.Command.ID]; duplicate {
			t.Fatalf("duplicate dispatch for %q", dispatch.Command.ID)
		}
		dispatches[dispatch.Command.ID] = dispatch
	}

	workers.Add(commands)
	for index := 0; index < commands; index++ {
		index := index
		go func() {
			defer workers.Done()
			commandID := fmt.Sprintf("cmd-%d", index)
			if err := gate.Complete(dispatches[commandID].Completion, testEmptyResult(commandID)); err != nil {
				t.Errorf("Complete(%d) error = %v", index, err)
				return
			}
			completed.Add(1)
		}()
	}
	workers.Wait()
	if completed.Load() != commands {
		t.Fatalf("completed = %d, want %d", completed.Load(), commands)
	}
	for range commands {
		mustCompletion(t, gate)
	}
}

func newTestGate(t *testing.T, capacity int) *Gate {
	t.Helper()
	gate, err := New(capacity)
	if err != nil {
		t.Fatalf("New(%d) error = %v", capacity, err)
	}
	t.Cleanup(func() { closeTestGate(gate) })
	return gate
}

func closeTestGate(gate *Gate) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = gate.Shutdown(ctx)
}

func testCommand(commandID string) model.Command {
	return model.Command{ID: commandID, Name: "test.command", SessionID: "test-session", Args: model.EmptyArgs{}}
}

func testEmptyResult(commandID string) model.Result {
	return model.Result{CommandID: commandID, Value: model.EmptyResult{}}
}

func mustSubmit(t *testing.T, gate *Gate, commandID string) Admission {
	t.Helper()
	admission, err := gate.Submit(context.Background(), testCommand(commandID))
	if err != nil || !admission.Accepted || admission.CommandID != commandID || admission.CommandHandle == "" {
		t.Fatalf("Submit(%s) = (%+v, %v), want accepted", commandID, admission, err)
	}
	return admission
}

func mustDispatch(t *testing.T, gate *Gate, commandID string) Dispatch {
	t.Helper()
	dispatch, err := gate.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if dispatch.Command.ID != commandID {
		t.Fatalf("Next() command ID = %q, want %q", dispatch.Command.ID, commandID)
	}
	if dispatch.Context == nil || dispatch.Completion.token == nil {
		t.Fatalf("Next() returned incomplete dispatch: %+v", dispatch)
	}
	return dispatch
}

func mustCompletion(t *testing.T, gate *Gate) model.Result {
	t.Helper()
	result, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("NextCompletion() error = %v", err)
	}
	return result
}

func assertFailure(t *testing.T, result model.Result, code string, cause error) {
	t.Helper()
	if result.Err == nil || result.Err.Code != code || result.Err.Stage != "command" || !errors.Is(result.Err.Cause, cause) {
		t.Fatalf("failure result = %+v, want code=%q cause=%v", result, code, cause)
	}
}

func completeRegistryCommand(t *testing.T, registry *Registry, commandID string) {
	t.Helper()
	if _, err := registry.Register(commandID); err != nil {
		t.Fatalf("Register(%s) error = %v", commandID, err)
	}
	_, completion, err := registry.markDispatched(commandID)
	if err != nil {
		t.Fatalf("markDispatched(%s) error = %v", commandID, err)
	}
	if err := registry.complete(completion, testEmptyResult(commandID)); err != nil {
		t.Fatalf("complete(%s) error = %v", commandID, err)
	}
	if !registry.consumeTerminal(commandID) {
		t.Fatalf("consumeTerminal(%s) = false", commandID)
	}
}
