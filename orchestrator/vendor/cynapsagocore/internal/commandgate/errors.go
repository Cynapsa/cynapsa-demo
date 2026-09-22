package commandgate

import (
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var (
	ErrInvalidCapacity             = errors.New("command gate: capacity must be positive")
	ErrInvalidByteCapacity         = errors.New("command gate: invalid byte capacity")
	ErrCommandBytesExceeded        = errors.New("command gate: command byte capacity reached")
	ErrInvalidCommandOwnership     = errors.New("command gate: invalid command ownership")
	ErrNilContext                  = errors.New("command gate: nil context")
	ErrEmptyCommandID              = errors.New("command gate: empty command identifier")
	ErrEmptyCommandName            = errors.New("command gate: empty command name")
	ErrEmptyCommandHandle          = errors.New("command gate: empty command handle")
	ErrCommandHandleGeneration     = errors.New("command gate: command handle generation failed")
	ErrCommandHandleExhausted      = errors.New("command gate: command handle generation exhausted")
	ErrDuplicateCommandID          = errors.New("command gate: duplicate command identifier")
	ErrRegistryFull                = errors.New("command gate: registry capacity reached")
	ErrCommandQueueFull            = errors.New("command gate: command queue capacity reached")
	ErrCompletionQueueFull         = errors.New("command gate: completion queue invariant violated")
	ErrCommandNotFound             = errors.New("command gate: command not found")
	ErrInvalidCompletionCapability = errors.New("command gate: invalid completion capability")
	ErrStaleCompletionCapability   = errors.New("command gate: stale completion capability")
	ErrAlreadyDispatched           = errors.New("command gate: command already dispatched")
	ErrAlreadyTerminal             = errors.New("command gate: command already terminal")
	ErrInvalidRegistryState        = errors.New("command gate: invalid registry state")
	ErrGateNotClosing              = errors.New("command gate: shutdown has not started")
	ErrGateClosing                 = errors.New("command gate: shutdown in progress")
	ErrGateClosed                  = errors.New("command gate: closed")
	ErrCommandCancelled            = errors.New("command gate: command cancelled locally")
	ErrCommandCompleted            = errors.New("command gate: command completed")
	ErrAdmissionRolledBack         = errors.New("command gate: admission rolled back")
	ErrNilHandler                  = errors.New("command gate: nil handler")
	ErrNilDisposition              = errors.New("command gate: nil completion disposition")
	ErrHandlerFailed               = errors.New("command gate: handler failed")
	ErrHandlerPanic                = errors.New("command gate: handler panic")
)

const (
	codeCancelled    = "command_cancelled"
	codeShutdown     = "command_shutdown"
	codeHandler      = "command_handler_failed"
	codeHandlerPanic = "command_handler_panic"
)

func failureCode(cause error) string {
	if shutdownResult(cause) {
		return codeShutdown
	}
	return codeCancelled
}

func failureResult(commandID, code string, cause error) model.Result {
	return model.Result{
		CommandID: commandID,
		Err: &model.Error{
			Code:  code,
			Stage: "command",
			Cause: cause,
		},
	}
}
