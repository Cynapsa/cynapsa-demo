package commandgate

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Dispatch transfers one admitted command, its gate-owned cancellation context,
// and its per-admission completion capability to the runtime dispatcher.
type Dispatch struct {
	Command    model.Command
	Context    context.Context
	Completion CompletionCapability
}

// Handler executes one private typed command. It must not retain Command or its
// reachable Args after returning unless it explicitly takes over that ownership.
type Handler func(context.Context, model.Command) (model.Result, error)

// CompletionDisposition observes whether the handler's final normalized result
// won the gate's terminal transition. For a winner it runs synchronously after
// the registry transition and before completion publication; for a loser it
// runs after the rejected completion attempt. Implementations must not panic or
// block.
type CompletionDisposition func(model.Command, model.Result, bool)

// NextDispatch waits for the next dispatchable command. Commands cancelled or
// terminalized before dispatch are discarded from the command queue because
// their sole completion has already been published.
func (g *Gate) NextDispatch(ctx context.Context) (Dispatch, error) {
	return g.nextDispatch(ctx)
}

func (g *Gate) nextDispatch(ctx context.Context) (Dispatch, error) {
	if ctx == nil {
		return Dispatch{}, ErrNilContext
	}
	for {
		if err := ctx.Err(); err != nil {
			return Dispatch{}, err
		}
		select {
		case <-ctx.Done():
			return Dispatch{}, ctx.Err()
		case <-g.shutdownCh:
			return Dispatch{}, g.stateError()
		case owner := <-g.commands:
			command, acquired := owner.acquireDispatch()
			if !acquired {
				continue
			}
			commandCtx, completion, err := g.registry.markDispatched(command.ID, owner)
			if errors.Is(err, ErrAlreadyTerminal) || errors.Is(err, ErrCommandNotFound) {
				owner.releaseDispatch()
				continue
			}
			if err != nil {
				owner.releaseDispatch()
				return Dispatch{}, err
			}
			return Dispatch{Command: command, Context: commandCtx, Completion: completion}, nil
		}
	}
}

// ExecuteNext dispatches and executes one command without creating a goroutine.
// The caller owns the worker goroutine and its lifetime. Panics are contained
// and normalized into a typed private failure completion.
func (g *Gate) ExecuteNext(ctx context.Context, handler Handler) error {
	return g.executeNext(ctx, handler, false, nil)
}

// ExecuteNextTerminatingOnSuccess dispatches and executes one command like
// ExecuteNext. When the dispatched command belongs to the closed terminal set
// (auth.logout or core.shutdown) and its handler produces a successful
// EmptyResult, the gate atomically publishes that completion and stops all
// later admission before returning. The terminal command's completion is
// always published before shutdown completions for other live commands.
//
// Runtime uses this closed seam for commands whose successful completion ends
// the process-local session. An error result, handler error, panic, or terminal
// transition won by cancellation/shutdown never initiates shutdown.
func (g *Gate) ExecuteNextTerminatingOnSuccess(ctx context.Context, handler Handler) error {
	return g.executeNext(ctx, handler, true, nil)
}

// ExecuteNextWithDisposition is ExecuteNextTerminatingOnSuccess with a closed
// completion disposition observer. Runtime uses it to commit or roll back
// private resources reserved while a command handler ran.
func (g *Gate) ExecuteNextWithDisposition(ctx context.Context, handler Handler, disposition CompletionDisposition) error {
	if disposition == nil {
		return ErrNilDisposition
	}
	return g.executeNext(ctx, handler, true, disposition)
}

func (g *Gate) executeNext(ctx context.Context, handler Handler, terminalCommands bool, disposition CompletionDisposition) error {
	if handler == nil {
		return ErrNilHandler
	}
	dispatch, err := g.NextDispatch(ctx)
	if err != nil {
		return err
	}
	terminal := &terminalDisposition{command: dispatch.Command, callback: disposition}
	if err := g.registry.attachDisposition(dispatch.Completion, terminal); err != nil {
		dispatch.Completion.token.releaseDispatch()
		terminal.resolve(false)
		if errors.Is(err, ErrAlreadyTerminal) {
			return nil
		}
		return err
	}

	execCtx, cancel := context.WithCancelCause(dispatch.Context)
	stop := context.AfterFunc(ctx, func() {
		cancel(context.Cause(ctx))
	})
	defer func() {
		stop()
		cancel(ErrCommandCompleted)
	}()

	result, handlerErr, panicErr := invokeHandler(execCtx, handler, dispatch.Command)
	result.CommandID = dispatch.Command.ID
	switch {
	case panicErr != nil:
		result = failureResult(dispatch.Command.ID, codeHandlerPanic, panicErr)
	case handlerErr != nil:
		result = failureResult(dispatch.Command.ID, codeHandler, handlerErr)
	}

	terminalSuccess := terminalCommands && isTerminalCommand(dispatch.Command.Name) && panicErr == nil && handlerErr == nil && result.Err == nil
	if terminalSuccess {
		_, terminalSuccess = result.Value.(model.EmptyResult)
	}
	completeErr := g.complete(dispatch.Completion, result, terminalSuccess)
	if completeErr != nil {
		// Cancellation or shutdown may win while the handler is returning.
		// Its completion is already queued and remains the only completion.
		if errors.Is(completeErr, ErrAlreadyTerminal) || errors.Is(completeErr, ErrGateClosed) {
			return nil
		}
		return completeErr
	}
	return nil
}

func isTerminalCommand(name string) bool {
	switch name {
	case "auth.logout", "core.shutdown":
		return true
	default:
		return false
	}
}

func invokeHandler(ctx context.Context, handler Handler, command model.Command) (
	result model.Result,
	handlerErr error,
	panicErr error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr = ErrHandlerPanic
		}
	}()
	result, handlerErr = handler(ctx, command)
	return result, handlerErr, nil
}
