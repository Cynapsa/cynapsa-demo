package runtime

import (
	"context"
	"errors"
	"reflect"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

type deliveryCommandContextKey struct{}

func (r *Runtime) commandCompletionDisposition(command model.Command, result model.Result, committed bool) {
	if command.Name != "delivery.next" {
		return
	}
	// This function is the command-gate transaction terminator and must never
	// panic or block the sole dispatcher worker on external work.
	defer func() {
		if recover() != nil {
			_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		}
	}()
	r.controlMu.Lock()
	lease := r.deliveryLeases[command.ID]
	delete(r.deliveryLeases, command.ID)
	success := committed && result.Err == nil && lease != nil
	if value, ok := result.Value.(model.EventResult); !ok || lease == nil || value.Event.ID != lease.EventID() {
		success = false
	}
	if success {
		eventID := lease.EventID()
		admission := lease.AdmissionID()
		if err := r.events.Commit(lease); err == nil {
			r.outstandingDelivery = eventID
			r.outstandingInbound = admission
		} else if !errors.Is(err, delivery.ErrLeaseRetired) {
			_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		}
	} else if lease != nil {
		if err := r.events.Rollback(lease); err != nil && !errors.Is(err, delivery.ErrInvalidLease) && !errors.Is(err, delivery.ErrLeaseRetired) {
			_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		}
	}
	r.controlMu.Unlock()
}

// Dispatch routes one closed typed command to its injected semantic handler.
func (r *Runtime) Dispatch(ctx context.Context, command model.Command) (model.Result, error) {
	if ctx == nil {
		return model.Result{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Result{}, err
	}
	if !knownCommand(command.Name) {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		return runtimeFailure(command.ID, "unknown_command", ErrUnknownHandler), nil
	}
	if !validCommandArgs(command.Name, command.Args) {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		return runtimeFailure(command.ID, "invalid_command_arguments", ErrInvalidCommandArgs), nil
	}
	handler, ok := r.handlers[command.Name]
	if !ok {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		return runtimeFailure(command.ID, "command_unavailable", ErrHandlerUnavailable), nil
	}

	if command.Name == "delivery.next" {
		ctx = context.WithValue(ctx, deliveryCommandContextKey{}, command.ID)
	}
	result, err := handler(ctx, handlerServices{runtime: r}, command)
	result.CommandID = command.ID
	if err != nil {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		return result, err
	}
	if !validResult(result) {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
		return model.Result{}, ErrInvalidHandlerResult
	}
	if result.Err != nil {
		_ = r.metrics.Add(diagnostics.MetricCommandsFailed, 1)
	}
	return result, nil
}

func (r *Runtime) runDispatcher() {
	defer func() {
		_ = r.metrics.Record(diagnostics.MetricActiveWorkers, 0)
		r.mu.Lock()
		r.workerStarted = false
		r.mu.Unlock()
		if recover() != nil {
			_ = r.metrics.Add(diagnostics.MetricWorkerPanics, 1)
			r.reportWorkerFailure(ErrWorkerPanic)
		}
		close(r.workerDone)
	}()

	for {
		err := r.gate.ExecuteNextWithDisposition(r.rootCtx, r.Dispatch, r.commandCompletionDisposition)
		if err == nil {
			_ = r.metrics.Add(diagnostics.MetricCommandsComplete, 1)
			if r.gate.Stats().Closing {
				r.BeginShutdown()
			}
			continue
		}
		if r.rootCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, commandgate.ErrGateClosing) || errors.Is(err, commandgate.ErrGateClosed) {
			return
		}
		r.reportWorkerFailure(errors.Join(ErrWorkerFailed, err))
		return
	}
}

func (r *Runtime) reportWorkerFailure(cause error) {
	_ = r.PublishEvent(context.Background(), model.Event{
		ID:        r.nextEventID(),
		Name:      "core.error",
		CreatedAt: r.clock.Now(),
		Value: model.CoreErrorEvent{Err: &model.Error{
			Code:  "runtime_worker_failed",
			Stage: "command",
			Cause: cause,
		}},
	})
	r.mu.RLock()
	closing := r.state == stateClosing || r.state == stateClosed
	r.mu.RUnlock()
	if !closing {
		_ = r.transition(stateFailed)
	}
}

func runtimeFailure(commandID, code string, cause error) model.Result {
	return model.Result{CommandID: commandID, Err: &model.Error{Code: code, Stage: "command", Cause: cause}}
}

func validResult(result model.Result) bool {
	if result.Value != nil && result.Err != nil {
		return false
	}
	if result.Value == nil && result.Err == nil {
		return false
	}
	if result.Value != nil {
		value := reflect.ValueOf(result.Value)
		if value.Kind() == reflect.Pointer && value.IsNil() {
			return false
		}
	}
	return result.Err != nil || result.Value != nil
}
