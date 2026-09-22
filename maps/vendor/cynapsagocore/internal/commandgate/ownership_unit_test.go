package commandgate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestSubmitIndependentlyFreezesDirectCallerMemory(t *testing.T) {
	backing := "command-id" + strings.Repeat("x", 1<<20)
	id := backing[:len("command-id")]
	chunk := []byte("caller-owned")
	command := model.Command{ID: id, Name: "payload.write", SessionID: "session", Args: model.PayloadWriteArgs{Handle: "handle", Chunk: chunk}}
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	chunk[0] = 'X'
	dispatch, err := gate.NextDispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	args := dispatch.Command.Args.(model.PayloadWriteArgs)
	if unsafe.StringData(dispatch.Command.ID) == unsafe.StringData(id) || string(args.Chunk) != "caller-owned" {
		t.Fatalf("dispatch retained caller backing: id=%q chunk=%q", dispatch.Command.ID, args.Chunk)
	}
	if string(chunk) != "Xaller-owned" {
		t.Fatalf("gate mutated rejected/accepted caller memory: %q", chunk)
	}
	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: id, Value: model.EmptyResult{}}); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedByteCapacityIsAtomicAndHeldThroughDispatch(t *testing.T) {
	command := model.Command{ID: "one", Name: "payload.write", Args: model.PayloadWriteArgs{Handle: "h", Chunk: []byte("secret")}}
	bytes, err := model.MeasureCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := NewWithByteCapacity(2, bytes)
	if err != nil {
		t.Fatal(err)
	}
	first, err := gate.Submit(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	rejectedChunk := []byte("secret")
	rejected := model.Command{ID: "two", Name: "payload.write", Args: model.PayloadWriteArgs{Handle: "h", Chunk: rejectedChunk}}
	if admission, err := gate.Submit(context.Background(), rejected); !errors.Is(err, ErrCommandBytesExceeded) || admission.Accepted {
		t.Fatalf("second Submit = (%+v, %v)", admission, err)
	}
	if string(rejectedChunk) != "secret" {
		t.Fatalf("rejection mutated caller bytes: %q", rejectedChunk)
	}
	if stats := gate.Stats(); stats.OwnedBytes != bytes || stats.ByteCapacity != bytes {
		t.Fatalf("Stats = %+v, want owned/capacity %d", stats, bytes)
	}
	dispatch, err := gate.NextDispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	retained := dispatch.Command.Args.(model.PayloadWriteArgs).Chunk
	if err := gate.Cancel(first.CommandHandle); err != nil {
		t.Fatal(err)
	}
	if gate.Stats().OwnedBytes != bytes || string(retained) != "secret" {
		t.Fatal("terminal transition released memory while dispatcher still owned it")
	}
	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "one", Value: model.EmptyResult{}}); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("losing Complete = %v", err)
	}
	if gate.Stats().OwnedBytes != 0 {
		t.Fatalf("owned bytes after terminal dispatcher release = %d", gate.Stats().OwnedBytes)
	}
	for index, value := range retained {
		if value != 0 {
			t.Fatalf("retained byte %d was not zeroed", index)
		}
	}
}

func TestMaximumCommandBytesRejectsBeforeClone(t *testing.T) {
	overhead, err := model.MeasureCommand(model.Command{ID: "large", Name: "payload.write", Args: model.PayloadWriteArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	exactBody := make([]byte, MaximumCommandBytes-overhead)
	exact := model.Command{ID: "large", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: exactBody}}
	if measured, err := model.MeasureCommand(exact); err != nil || measured != MaximumCommandBytes {
		t.Fatalf("exact measure = (%d, %v)", measured, err)
	}
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), exact); err != nil {
		t.Fatalf("exact Submit = %v", err)
	}
	gate.BeginShutdown()
	overBody := make([]byte, MaximumCommandBytes-overhead+1)
	overBody[0] = 0x7f
	over := model.Command{ID: "large", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: overBody}}
	other, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Submit(context.Background(), over); !errors.Is(err, ErrCommandBytesExceeded) {
		t.Fatalf("over Submit = %v", err)
	}
	if overBody[0] != 0x7f {
		t.Fatal("oversized rejection mutated caller body")
	}
}

func TestShutdownAndHandlerPanicReleaseSensitiveCommandBytes(t *testing.T) {
	t.Run("queued shutdown", func(t *testing.T) {
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := gate.Submit(context.Background(), model.Command{ID: "auth", Name: "auth.login", Args: model.AuthArgs{Password: []byte("password")}}); err != nil {
			t.Fatal(err)
		}
		owner := <-gate.commands
		retained := owner.command.Args.(model.AuthArgs).Password
		gate.commands <- owner
		gate.BeginShutdown()
		if gate.Stats().OwnedBytes != 0 {
			t.Fatalf("shutdown retained %d command bytes", gate.Stats().OwnedBytes)
		}
		for index, value := range retained {
			if value != 0 {
				t.Fatalf("password byte %d not zeroed", index)
			}
		}
	})

	t.Run("handler panic", func(t *testing.T) {
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := gate.Submit(context.Background(), model.Command{ID: "panic", Name: "auth.login", Args: model.AuthArgs{Password: []byte("password")}}); err != nil {
			t.Fatal(err)
		}
		var retained []byte
		err = gate.ExecuteNext(context.Background(), func(_ context.Context, command model.Command) (model.Result, error) {
			retained = command.Args.(model.AuthArgs).Password
			panic("private panic")
		})
		if err != nil {
			t.Fatalf("ExecuteNext = %v", err)
		}
		if gate.Stats().OwnedBytes != 0 {
			t.Fatalf("panic retained %d command bytes", gate.Stats().OwnedBytes)
		}
		for index, value := range retained {
			if value != 0 {
				t.Fatalf("password byte %d not zeroed", index)
			}
		}
	})
}

func TestShutdownDeadlineQuarantinesBorrowedCommandUntilDispatcherReturns(t *testing.T) {
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "borrowed", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: []byte("secret")}}); err != nil {
		t.Fatal(err)
	}
	dispatch, err := gate.NextDispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	retained := dispatch.Command.Args.(model.PayloadWriteArgs).Chunk
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := gate.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline", err)
	}
	<-gate.finalizeDone
	stats := gate.Stats()
	if !stats.Closed || stats.OwnedBytes == 0 || stats.RegistryEntries != 0 {
		t.Fatalf("closed borrowed quarantine stats = %+v", stats)
	}
	if got := string(retained); got != "secret" {
		t.Fatalf("shutdown cleared live borrowed view: %q", got)
	}
	if _, err := gate.NextDispatch(context.Background()); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("closed NextDispatch = %v", err)
	}
	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "borrowed", Value: model.EmptyResult{}}); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("late dispatcher return = %v, want closed", err)
	}
	if gate.Stats().OwnedBytes != 0 {
		t.Fatalf("late dispatcher return retained %d bytes", gate.Stats().OwnedBytes)
	}
	for index, value := range retained {
		if value != 0 {
			t.Fatalf("released byte %d = %d", index, value)
		}
	}
}

func TestSubmitOwnedRejectsEveryPostFreezeGraphMutationWithoutCharge(t *testing.T) {
	tests := []struct {
		name   string
		input  model.Command
		mutate func(*model.Command)
	}{
		{name: "id same length", input: model.Command{ID: "alpha", Name: "test", Args: model.EmptyArgs{}}, mutate: func(command *model.Command) { command.ID = "bravo" }},
		{name: "name same length", input: model.Command{ID: "id", Name: "first", Args: model.EmptyArgs{}}, mutate: func(command *model.Command) { command.Name = "other" }},
		{name: "session same length", input: model.Command{ID: "id", Name: "test", SessionID: "first", Args: model.EmptyArgs{}}, mutate: func(command *model.Command) { command.SessionID = "other" }},
		{name: "args type same charge", input: model.Command{ID: "id", Name: "test", Args: model.HandlerPathArgs{Path: "same"}}, mutate: func(command *model.Command) { command.Args = model.PayloadHandleArgs{Handle: "same"} }},
		{name: "payload bytes same length", input: model.Command{ID: "id", Name: "test", Args: model.PayloadWriteArgs{Chunk: []byte("first")}}, mutate: func(command *model.Command) { command.Args = model.PayloadWriteArgs{Chunk: []byte("first")} }},
		{name: "config pointee same size", input: model.Command{ID: "id", Name: "test", Args: model.ConfigUpdateArgs{QueueLimit: uint32Pointer(1)}}, mutate: func(command *model.Command) { command.Args = model.ConfigUpdateArgs{QueueLimit: uint32Pointer(1)} }},
		{name: "typed nil payload", input: model.Command{ID: "id", Name: "test", Args: model.MessageSendArgs{Payload: model.Payload{Value: &model.NativePayload{Body: []byte("body")}}}}, mutate: func(command *model.Command) {
			var payload *model.NativePayload
			command.Args = model.MessageSendArgs{Payload: model.Payload{Value: payload}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frozen, charged, err := model.FreezeCommand(test.input)
			if err != nil {
				t.Fatal(err)
			}
			defer model.ClearCommand(&frozen)
			test.mutate(&frozen)
			gate, err := New(1)
			if err != nil {
				t.Fatal(err)
			}
			admission, submitErr := gate.SubmitOwned(context.Background(), &frozen)
			if submitErr == nil || admission.Accepted {
				t.Fatalf("mutated token admitted: (%+v, %v)", admission, submitErr)
			}
			if gate.Stats().OwnedBytes != 0 || gate.Stats().RegistryEntries != 0 {
				t.Fatalf("mutated rejection retained resources: %+v", gate.Stats())
			}
			if bytes, ok := model.FrozenCommandBytes(frozen); !ok || bytes != charged {
				t.Fatalf("rejection consumed token: (%d, %v), want (%d, true)", bytes, ok, charged)
			}
		})
	}
}

func TestSubmitOwnedRejectsSameContentFacadeAliasesWithoutMutation(t *testing.T) {
	t.Run("byte slice with oversized capacity", func(t *testing.T) {
		frozen, _, err := model.FreezeCommand(model.Command{ID: "slice", Name: "payload.write", Args: model.PayloadWriteArgs{Handle: "handle", Chunk: []byte("secret")}})
		if err != nil {
			t.Fatal(err)
		}
		defer model.ClearCommand(&frozen)
		injected := make([]byte, len("secret"), 1<<20)
		copy(injected, "secret")
		frozen.Args = model.PayloadWriteArgs{Handle: "handle", Chunk: injected}

		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		if admission, err := gate.SubmitOwned(context.Background(), &frozen); err == nil || admission.Accepted {
			t.Fatalf("SubmitOwned substituted slice = (%+v, %v)", admission, err)
		}
		model.ClearCommand(&frozen)
		if string(injected) != "secret" {
			t.Fatalf("rejection cleanup mutated substituted caller slice: %q", injected)
		}
	})

	t.Run("substring with oversized backing", func(t *testing.T) {
		frozen, _, err := model.FreezeCommand(model.Command{ID: "string", Name: "test", Args: model.EmptyArgs{}})
		if err != nil {
			t.Fatal(err)
		}
		defer model.ClearCommand(&frozen)
		backing := "string" + strings.Repeat("x", 1<<20)
		injected := backing[:len("string")]
		frozen.ID = injected

		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		if admission, err := gate.SubmitOwned(context.Background(), &frozen); err == nil || admission.Accepted {
			t.Fatalf("SubmitOwned substituted string = (%+v, %v)", admission, err)
		}
		model.ClearCommand(&frozen)
		if backing[:len("string")] != "string" {
			t.Fatalf("rejection cleanup changed substituted string backing: %q", backing[:len("string")])
		}
	})
}

func TestClearOfCopiedConsumedFacadeCannotCorruptGateOwnership(t *testing.T) {
	frozen, _, err := model.FreezeCommand(model.Command{ID: "copied", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: []byte("secret")}})
	if err != nil {
		t.Fatal(err)
	}
	copiedFacade := frozen
	gate, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if admission, err := gate.SubmitOwned(context.Background(), &frozen); err != nil || !admission.Accepted {
		t.Fatalf("SubmitOwned = (%+v, %v)", admission, err)
	}
	model.ClearCommand(&copiedFacade)
	dispatch, err := gate.NextDispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(dispatch.Command.Args.(model.PayloadWriteArgs).Chunk); got != "secret" {
		t.Fatalf("copied facade cleanup corrupted gate-owned command: %q", got)
	}
	if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "copied", Value: model.EmptyResult{}}); err != nil {
		t.Fatal(err)
	}
}

func TestPostTransferFacadeMutationCannotReachCanonicalGraph(t *testing.T) {
	tests := []struct {
		name   string
		input  model.Command
		mutate func(*model.Command, *model.Command) []byte
		check  func(*testing.T, model.Command)
	}{
		{
			name:  "password",
			input: model.Command{ID: "password", Name: "auth.login", Args: model.AuthArgs{Username: "agent", Password: []byte("secret")}},
			mutate: func(first, second *model.Command) []byte {
				injected := []byte("caller")
				first.Args = model.AuthArgs{Password: injected}
				second.Args = &model.AuthArgs{Password: injected}
				return injected
			},
			check: func(t *testing.T, command model.Command) {
				if got := string(command.Args.(model.AuthArgs).Password); got != "secret" {
					t.Fatalf("canonical password = %q", got)
				}
			},
		},
		{
			name:  "payload",
			input: model.Command{ID: "payload", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: []byte("secret")}},
			mutate: func(first, second *model.Command) []byte {
				injected := make([]byte, len("secret"), 1<<20)
				copy(injected, "secret")
				first.Args = model.PayloadWriteArgs{Chunk: injected}
				second.Args = &model.PayloadWriteArgs{Chunk: injected}
				return injected
			},
			check: func(t *testing.T, command model.Command) {
				if got := string(command.Args.(model.PayloadWriteArgs).Chunk); got != "secret" {
					t.Fatalf("canonical payload = %q", got)
				}
			},
		},
		{
			name:  "pointee",
			input: model.Command{ID: "pointee", Name: "config.update", Args: model.ConfigUpdateArgs{QueueLimit: uint32Pointer(7)}},
			mutate: func(first, second *model.Command) []byte {
				value := uint32(99)
				first.Args = model.ConfigUpdateArgs{QueueLimit: &value}
				second.Args = &model.ConfigUpdateArgs{QueueLimit: &value}
				return []byte("untouched")
			},
			check: func(t *testing.T, command model.Command) {
				if got := *command.Args.(model.ConfigUpdateArgs).QueueLimit; got != 7 {
					t.Fatalf("canonical pointee = %d", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frozen, _, err := model.FreezeCommand(test.input)
			if err != nil {
				t.Fatal(err)
			}
			firstAlias, secondAlias := frozen, frozen
			gate, err := New(1)
			if err != nil {
				t.Fatal(err)
			}
			if admission, err := gate.SubmitOwned(context.Background(), &frozen); err != nil || !admission.Accepted {
				t.Fatalf("SubmitOwned = (%+v, %v)", admission, err)
			}
			injected := test.mutate(&firstAlias, &secondAlias)
			model.ClearCommand(&firstAlias)
			model.ClearCommand(&secondAlias)
			dispatch, err := gate.NextDispatch(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, dispatch.Command)
			if err := gate.Complete(dispatch.Completion, model.Result{CommandID: test.input.ID, Value: model.EmptyResult{}}); err != nil {
				t.Fatal(err)
			}
			if test.name != "pointee" {
				want := "secret"
				if test.name == "password" {
					want = "caller"
				}
				if string(injected) != want {
					t.Fatalf("facade cleanup mutated injected caller bytes: %q", injected)
				}
			}
		})
	}
}

func TestCopiedFacadeClearRacesOwnershipMoveWithOneWinner(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		frozen, _, err := model.FreezeCommand(model.Command{ID: "race", Name: "payload.write", Args: model.PayloadWriteArgs{Chunk: []byte("secret")}})
		if err != nil {
			t.Fatal(err)
		}
		copiedFacade := frozen
		gate, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		clearDone := make(chan struct{})
		go func() {
			<-start
			model.ClearCommand(&copiedFacade)
			close(clearDone)
		}()
		close(start)
		admission, submitErr := gate.SubmitOwned(context.Background(), &frozen)
		<-clearDone
		if admission.Accepted {
			if submitErr != nil {
				t.Fatalf("iteration %d accepted with error %v", iteration, submitErr)
			}
			dispatch, err := gate.NextDispatch(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := string(dispatch.Command.Args.(model.PayloadWriteArgs).Chunk); got != "secret" {
				t.Fatalf("iteration %d admitted corrupted canonical bytes %q", iteration, got)
			}
			if err := gate.Complete(dispatch.Completion, model.Result{CommandID: "race", Value: model.EmptyResult{}}); err != nil {
				t.Fatal(err)
			}
		} else {
			if submitErr == nil || gate.Stats().OwnedBytes != 0 || gate.Stats().RegistryEntries != 0 {
				t.Fatalf("iteration %d losing move = (%+v, %v), stats=%+v", iteration, admission, submitErr, gate.Stats())
			}
			model.ClearCommand(&frozen)
		}
	}
}

func uint32Pointer(value uint32) *uint32 { return &value }
