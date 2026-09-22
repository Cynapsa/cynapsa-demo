package commandgate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestAcceptanceConcurrentAdmissionIsBoundedUntilSlowConsumerDrains(t *testing.T) {
	const (
		capacity  = 16
		producers = 128
	)
	gate := acceptanceGate(t, capacity)

	start := make(chan struct{})
	results := make(chan struct {
		admission Admission
		err       error
	}, producers)
	var group sync.WaitGroup
	group.Add(producers)
	for index := 0; index < producers; index++ {
		index := index
		go func() {
			defer group.Done()
			<-start
			admission, err := gate.Submit(context.Background(), acceptanceCommand(fmt.Sprintf("producer-%03d", index)))
			results <- struct {
				admission Admission
				err       error
			}{admission: admission, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	accepted := make(map[string]Admission, capacity)
	rejected := 0
	for result := range results {
		if result.err == nil {
			if !result.admission.Accepted || result.admission.CommandHandle == "" {
				t.Fatalf("successful admission is incomplete: %+v", result.admission)
			}
			accepted[result.admission.CommandID] = result.admission
			continue
		}
		if !errors.Is(result.err, ErrRegistryFull) || result.admission.Accepted || result.admission.Reason != ReasonCapacityReached {
			t.Fatalf("saturated admission = (%+v, %v), want bounded rejection", result.admission, result.err)
		}
		rejected++
	}
	if len(accepted) != capacity || rejected != producers-capacity {
		t.Fatalf("accepted=%d rejected=%d, want %d and %d", len(accepted), rejected, capacity, producers-capacity)
	}
	if gate.registry.Len() != capacity || len(gate.commands) != capacity || len(gate.completions) != 0 {
		t.Fatalf("saturated resources: registry=%d commands=%d completions=%d", gate.registry.Len(), len(gate.commands), len(gate.completions))
	}

	dispatches := make([]Dispatch, 0, capacity)
	for range capacity {
		dispatch, err := gate.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if _, ok := accepted[dispatch.Command.ID]; !ok {
			t.Fatalf("dispatched unaccepted command %q", dispatch.Command.ID)
		}
		dispatches = append(dispatches, dispatch)
	}
	for _, dispatch := range dispatches {
		if err := gate.Complete(dispatch.Completion, model.Result{CommandID: dispatch.Command.ID}); err != nil {
			t.Fatalf("Complete(%q) error = %v", dispatch.Command.ID, err)
		}
	}

	// Dispatch and completion do not release capacity. This models a slow or
	// missing completion consumer without permitting unbounded growth.
	if _, err := gate.Submit(context.Background(), acceptanceCommand("blocked-by-slow-consumer")); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("Submit() while completions are undrained = %v, want ErrRegistryFull", err)
	}
	if len(gate.completions) != capacity || gate.registry.Len() != capacity {
		t.Fatalf("reserved terminal slots: completions=%d registry=%d, want %d", len(gate.completions), gate.registry.Len(), capacity)
	}

	seen := make(map[string]struct{}, capacity)
	for range capacity {
		completion, err := gate.NextCompletion(context.Background())
		if err != nil {
			t.Fatalf("NextCompletion() error = %v", err)
		}
		if _, duplicate := seen[completion.CommandID]; duplicate {
			t.Fatalf("duplicate completion for %q", completion.CommandID)
		}
		seen[completion.CommandID] = struct{}{}
	}
	if len(seen) != capacity || gate.registry.Len() != 0 {
		t.Fatalf("drained completions=%d live registry=%d", len(seen), gate.registry.Len())
	}
	if admission, err := gate.Submit(context.Background(), acceptanceCommand("capacity-recovered")); err != nil || !admission.Accepted {
		t.Fatalf("Submit() after drain = (%+v, %v), want accepted", admission, err)
	}
}

func TestAcceptanceCancelCompleteShutdownRaceHasOneTerminalResult(t *testing.T) {
	const iterations = 400
	for iteration := 0; iteration < iterations; iteration++ {
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		commandID := fmt.Sprintf("terminal-race-%03d", iteration)
		admission := acceptanceSubmit(t, gate, commandID)
		dispatch := acceptanceDispatch(t, gate)

		start := make(chan struct{})
		cancelResult := make(chan error, 1)
		completeResult := make(chan error, 1)
		shutdownResult := make(chan error, 1)
		go func() {
			<-start
			cancelResult <- gate.Cancel(admission.CommandHandle)
		}()
		go func() {
			<-start
			completeResult <- gate.Complete(dispatch.Completion, model.Result{CommandID: commandID, Value: model.EmptyResult{}})
		}()
		go func() {
			<-start
			shutdownResult <- gate.Shutdown(context.Background())
		}()
		close(start)

		completion, err := gate.NextCompletion(context.Background())
		if err != nil {
			t.Fatalf("iteration %d NextCompletion() error = %v", iteration, err)
		}
		if completion.CommandID != commandID {
			t.Fatalf("iteration %d completion ID = %q", iteration, completion.CommandID)
		}

		terminalWinners := 0
		for _, result := range []error{<-cancelResult, <-completeResult} {
			if result == nil {
				terminalWinners++
				continue
			}
			if !errors.Is(result, ErrAlreadyTerminal) && !errors.Is(result, ErrGateClosed) {
				t.Fatalf("iteration %d terminal contender error = %v", iteration, result)
			}
		}
		if terminalWinners > 1 {
			t.Fatalf("iteration %d had %d cancel/complete winners", iteration, terminalWinners)
		}
		if err := <-shutdownResult; err != nil {
			t.Fatalf("iteration %d Shutdown() error = %v", iteration, err)
		}
		if len(gate.completions) != 0 || gate.registry.Len() != 0 {
			t.Fatalf("iteration %d retained terminal output", iteration)
		}
	}
}

func TestAcceptanceReusedIDRejectsStaleHandleAndWorkerCapability(t *testing.T) {
	gate := acceptanceGate(t, 1)

	oldAdmission := acceptanceSubmit(t, gate, "reused")
	staleDispatch := acceptanceDispatch(t, gate)
	if err := gate.Cancel(oldAdmission.CommandHandle); err != nil {
		t.Fatalf("Cancel(old) error = %v", err)
	}
	acceptanceCompletion(t, gate)

	// Capacity-one terminal history must evict "reused" before its ID can be
	// admitted again. The old handle and worker capability must remain inert.
	acceptanceSubmit(t, gate, "history-evictor")
	evictor := acceptanceDispatch(t, gate)
	if err := gate.Complete(evictor.Completion, model.Result{CommandID: "history-evictor"}); err != nil {
		t.Fatalf("Complete(evictor) error = %v", err)
	}
	acceptanceCompletion(t, gate)

	currentAdmission := acceptanceSubmit(t, gate, "reused")
	currentDispatch := acceptanceDispatch(t, gate)
	if currentAdmission.CommandHandle == oldAdmission.CommandHandle {
		t.Fatal("re-admission reused a stale command handle")
	}
	if err := gate.Cancel(oldAdmission.CommandHandle); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("Cancel(stale handle) error = %v, want ErrCommandNotFound", err)
	}
	staleValue := &model.PayloadHandleResult{Handle: "stale-worker", Chunk: []byte("stale")}
	if err := gate.Complete(staleDispatch.Completion, model.Result{CommandID: "reused", Value: staleValue}); !errors.Is(err, ErrStaleCompletionCapability) {
		t.Fatalf("Complete(stale worker) error = %v, want ErrStaleCompletionCapability", err)
	}
	if staleValue.Handle != "stale-worker" || string(staleValue.Chunk) != "stale" {
		t.Fatalf("rejected stale result was mutated: %+v", staleValue)
	}
	if state, ok := gate.registry.State("reused"); !ok || state != StateDispatched {
		t.Fatalf("current admission state = (%v, %v), want dispatched", state, ok)
	}

	currentValue := &model.PayloadHandleResult{Handle: "current-worker", Chunk: []byte("current")}
	if err := gate.Complete(currentDispatch.Completion, model.Result{CommandID: "reused", Value: currentValue}); err != nil {
		t.Fatalf("Complete(current worker) error = %v", err)
	}
	completion := acceptanceCompletion(t, gate)
	if completion.Value != currentValue || currentValue.Handle != "current-worker" || string(currentValue.Chunk) != "current" {
		t.Fatalf("completion Value = %#v, want current worker", completion.Value)
	}
}

func TestAcceptanceCommandHandlesAreIsolatedAcrossCores(t *testing.T) {
	first := acceptanceGate(t, 1)
	second := acceptanceGate(t, 1)
	firstAdmission := acceptanceSubmit(t, first, "same-id")
	secondAdmission := acceptanceSubmit(t, second, "same-id")
	if firstAdmission.CommandHandle == secondAdmission.CommandHandle {
		t.Fatal("independent cores minted the same command handle")
	}

	if err := second.Cancel(firstAdmission.CommandHandle); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("cross-core Cancel() error = %v, want ErrCommandNotFound", err)
	}
	if state, ok := second.registry.State("same-id"); !ok || state != StateAdmitted {
		t.Fatalf("cross-core handle changed target state = (%v, %v)", state, ok)
	}
	if err := second.Cancel(CommandHandle("cmdh_hostile-but-opaque")); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("forged Cancel() error = %v, want ErrCommandNotFound", err)
	}
	if err := second.Cancel(secondAdmission.CommandHandle); err != nil {
		t.Fatalf("Cancel(valid handle) error = %v", err)
	}
	acceptanceCompletion(t, second)
	if err := first.Cancel(firstAdmission.CommandHandle); err != nil {
		t.Fatalf("Cancel(first handle) error = %v", err)
	}
	acceptanceCompletion(t, first)
}

func TestAcceptanceShutdownDrainsOrAbandonsAtCallerDeadline(t *testing.T) {
	t.Run("graceful drain", func(t *testing.T) {
		gate, err := New(4)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 4; index++ {
			acceptanceSubmit(t, gate, fmt.Sprintf("shutdown-%d", index))
		}
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- gate.Shutdown(context.Background()) }()
		<-gate.shutdownCh

		seen := make(map[string]struct{}, 4)
		for range 4 {
			completion := acceptanceCompletion(t, gate)
			if completion.Err == nil || completion.Err.Code != codeShutdown || !errors.Is(completion.Err.Cause, ErrGateClosing) {
				t.Fatalf("shutdown completion = %+v", completion)
			}
			seen[completion.CommandID] = struct{}{}
		}
		if len(seen) != 4 {
			t.Fatalf("shutdown delivered %d unique results, want 4", len(seen))
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
		if gate.registry.Len() != 0 || len(gate.commands) != 0 || len(gate.completions) != 0 || !gate.isClosed() {
			t.Fatalf("graceful shutdown retained resources")
		}
	})

	t.Run("deadline abandonment", func(t *testing.T) {
		gate, err := New(4)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 4; index++ {
			acceptanceSubmit(t, gate, fmt.Sprintf("abandon-%d", index))
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := gate.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown(cancelled) error = %v, want context.Canceled", err)
		}
		<-gate.finalizeDone
		if gate.registry.Len() != 0 || len(gate.commands) != 0 || len(gate.completions) != 0 || !gate.isClosed() {
			t.Fatalf("deadline shutdown retained resources")
		}
		if _, err := gate.NextCompletion(context.Background()); !errors.Is(err, ErrGateClosed) {
			t.Fatalf("NextCompletion() after deadline = %v, want ErrGateClosed", err)
		}
	})
}

func TestAcceptanceOwnershipHandoffAndPanicContainment(t *testing.T) {
	gate := acceptanceGate(t, 1)
	args := &model.PayloadWriteArgs{Handle: "command", Chunk: []byte("owned-command")}
	command := acceptanceCommand("owned")
	command.Args = args
	admission, err := gate.Submit(context.Background(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("Submit() = (%+v, %v)", admission, err)
	}
	dispatch := acceptanceDispatch(t, gate)
	gotArgs, ok := dispatch.Command.Args.(*model.PayloadWriteArgs)
	if !ok || gotArgs == args || gotArgs.Handle != "command" || string(gotArgs.Chunk) != "owned-command" || args.Handle != "command" || string(args.Chunk) != "owned-command" {
		t.Fatalf("command was not independently frozen: %#v", dispatch.Command.Args)
	}

	resultValue := &model.PayloadHandleResult{Handle: "rejected-result", Chunk: []byte("owned-result")}
	rejected := model.Result{CommandID: "owned", Value: resultValue}
	if err := gate.Complete(CompletionCapability{}, rejected); !errors.Is(err, ErrInvalidCompletionCapability) {
		t.Fatalf("Complete(invalid capability) error = %v", err)
	}
	if rejected.Value != resultValue || resultValue.Handle != "rejected-result" || string(resultValue.Chunk) != "owned-result" {
		t.Fatal("rejected completion mutated caller-owned result")
	}

	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "owned", Value: resultValue}); err != nil {
		t.Fatalf("Complete(valid capability) error = %v", err)
	}
	if completion := acceptanceCompletion(t, gate); completion.Value != resultValue || resultValue.Handle != "rejected-result" || string(resultValue.Chunk) != "owned-result" {
		t.Fatalf("completion ownership was not transferred exactly: %#v", completion.Value)
	}

	acceptanceSubmit(t, gate, "panic")
	if err := gate.ExecuteNext(context.Background(), func(context.Context, model.Command) (model.Result, error) {
		panic("sensitive panic text must not escape")
	}); err != nil {
		t.Fatalf("ExecuteNext(panic) error = %v", err)
	}
	panicCompletion := acceptanceCompletion(t, gate)
	if panicCompletion.Err == nil || panicCompletion.Err.Code != codeHandlerPanic || panicCompletion.Err.Stage != "command" || !errors.Is(panicCompletion.Err.Cause, ErrHandlerPanic) {
		t.Fatalf("panic completion = %+v", panicCompletion)
	}
	if got := panicCompletion.Err.Cause.Error(); got != ErrHandlerPanic.Error() {
		t.Fatalf("panic detail escaped through cause %q", got)
	}
}

func acceptanceGate(t *testing.T, capacity int) *Gate {
	t.Helper()
	gate, err := New(capacity)
	if err != nil {
		t.Fatalf("New(%d) error = %v", capacity, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = gate.Shutdown(ctx)
	})
	return gate
}

func acceptanceCommand(commandID string) model.Command {
	return model.Command{ID: commandID, Name: "qa.command", SessionID: "qa-session"}
}

func acceptanceSubmit(t *testing.T, gate *Gate, commandID string) Admission {
	t.Helper()
	admission, err := gate.Submit(context.Background(), acceptanceCommand(commandID))
	if err != nil || !admission.Accepted || admission.CommandHandle == "" {
		t.Fatalf("Submit(%q) = (%+v, %v), want accepted", commandID, admission, err)
	}
	return admission
}

func acceptanceDispatch(t *testing.T, gate *Gate) Dispatch {
	t.Helper()
	dispatch, err := gate.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	return dispatch
}

func acceptanceCompletion(t *testing.T, gate *Gate) model.Result {
	t.Helper()
	completion, err := gate.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("NextCompletion() error = %v", err)
	}
	return completion
}
