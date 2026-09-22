package sessionkernel

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// handlers is the exhaustive command/result coupling. Every public command is
// named once and can reach only its exact typed operation field.
func (controller *SessionController) handlers() (map[string]coreruntime.Handler, error) {
	handlers := map[string]coreruntime.Handler{
		"core.init": controller.coreInitHandler(),
		"core.capabilities": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EmptyArgs, model.CapabilitiesResult] {
			return ops.CoreCapabilities
		}),
		"core.status":        localHandler(controller, "command", func(ops LocalOperations) Operation[model.EmptyArgs, model.StatusResult] { return ops.CoreStatus }),
		"core.shutdown":      localHandler(controller, "shutdown", func(ops LocalOperations) Operation[model.EmptyArgs, model.EmptyResult] { return ops.CoreShutdown }),
		"session.config.get": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EmptyArgs, model.ConfigResult] { return ops.ConfigGet }),
		"session.config.update": localHandler(controller, "command", func(ops LocalOperations) Operation[model.ConfigUpdateArgs, model.ConfigResult] {
			return ops.ConfigUpdate
		}),
		"command.channel.register": localHandler(controller, "command", func(ops LocalOperations) Operation[model.ChannelRegisterArgs, model.CompletionChannelResult] {
			return ops.CommandChannelRegister
		}),
		"command.channel.clear": localHandler(controller, "command", func(ops LocalOperations) Operation[model.ChannelIDArgs, model.EmptyResult] {
			return ops.CommandChannelClear
		}),
		"command.cancel": localHandler(controller, "command", func(ops LocalOperations) Operation[model.CommandCancelArgs, model.EmptyResult] {
			return ops.CommandCancel
		}),
		"event.sink.register": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EventSinkRegisterArgs, model.EventSinkResult] {
			return ops.EventSinkRegister
		}),
		"event.sink.clear": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EventSinkIDArgs, model.EmptyResult] {
			return ops.EventSinkClear
		}),
		"event.sink.bind": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EventSinkIDArgs, model.EmptyResult] {
			return ops.EventSinkBind
		}),

		"auth.login":                controller.authHandler(PersonalityHTTPBridge),
		"auth.connect":              controller.authHandler(PersonalityNative),
		"auth.token_login":          controller.tokenAuthHandler(PersonalityHTTPBridge),
		"auth.token_connect":        controller.tokenAuthHandler(PersonalityNative),
		"auth.installation_login":   controller.installationAuthHandler(PersonalityHTTPBridge),
		"auth.installation_connect": controller.installationAuthHandler(PersonalityNative),
		"auth.logout":               remoteHandler(controller, "auth", func(ops AuthenticatedOperations) Operation[model.EmptyArgs, model.EmptyResult] { return ops.Logout }),
		"auth.agent_id":             controller.agentIDHandler(),
		"mesh.list": remoteHandler(controller, "command", func(ops AuthenticatedOperations) Operation[model.EmptyArgs, model.MeshListResult] {
			return ops.MeshList
		}),
		"mesh.membership.refresh": remoteHandler(controller, "command", func(ops AuthenticatedOperations) Operation[model.EmptyArgs, model.EmptyResult] {
			return ops.MeshRefresh
		}),

		"address.map.put": localHandler(controller, "command", func(ops LocalOperations) Operation[model.AddressPutArgs, model.EmptyResult] { return ops.AddressPut }),
		"address.map.remove": localHandler(controller, "command", func(ops LocalOperations) Operation[model.AddressRemoveArgs, model.EmptyResult] {
			return ops.AddressRemove
		}),
		"address.map.list": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EmptyArgs, model.AddressMappingsResult] {
			return ops.AddressList
		}),
		"address.resolve": localHandler(controller, "command", func(ops LocalOperations) Operation[model.AddressResolveArgs, model.AddressResolution] {
			return ops.AddressResolve
		}),

		"message.send": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.MessageSendArgs, model.SendResult] {
			return ops.MessageSend
		}),
		"message.request": remoteHandler(controller, "rpc", func(ops AuthenticatedOperations) Operation[model.MessageRequestArgs, model.ResponseResult] {
			return ops.MessageRequest
		}),
		"message.reply": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.MessageReplyArgs, model.SendResult] {
			return ops.MessageReply
		}),

		"delivery.next": localHandler(controller, "delivery", func(ops LocalOperations) Operation[model.EmptyArgs, model.EventResult] { return ops.DeliveryNext }),
		"delivery.accept": localHandler(controller, "delivery", func(ops LocalOperations) Operation[model.DeliveryAcceptArgs, model.EmptyResult] {
			return ops.DeliveryAccept
		}),
		"delivery.queue.status": localHandler(controller, "delivery", func(ops LocalOperations) Operation[model.EmptyArgs, model.DeliveryQueueStatus] {
			return ops.DeliveryQueueStatus
		}),
		"delivery.retry": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.MessageIDArgs, model.EmptyResult] {
			return ops.DeliveryRetry
		}),
		"delivery.pause":  localHandler(controller, "delivery", func(ops LocalOperations) Operation[model.EmptyArgs, model.EmptyResult] { return ops.DeliveryPause }),
		"delivery.resume": localHandler(controller, "delivery", func(ops LocalOperations) Operation[model.EmptyArgs, model.EmptyResult] { return ops.DeliveryResume }),
		"delivery.drop": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.MessageIDArgs, model.EmptyResult] {
			return ops.DeliveryDrop
		}),

		"handler.register": localHandler(controller, "handler", func(ops LocalOperations) Operation[model.HandlerPathArgs, model.EmptyResult] {
			return ops.HandlerRegister
		}),
		"handler.unregister": localHandler(controller, "handler", func(ops LocalOperations) Operation[model.HandlerPathArgs, model.EmptyResult] {
			return ops.HandlerUnregister
		}),

		"payload.open": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.EmptyArgs, model.PayloadHandleResult] {
			return ops.PayloadOpen
		}),
		"payload.write_chunk": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadWriteArgs, model.PayloadHandleResult] {
			return ops.PayloadWrite
		}),
		"payload.finish": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadHandleArgs, model.PayloadHandleResult] {
			return ops.PayloadFinish
		}),
		"payload.cancel": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadHandleArgs, model.EmptyResult] {
			return ops.PayloadCancel
		}),
		"payload.read": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadReadArgs, model.PayloadHandleResult] {
			return ops.PayloadRead
		}),
		"payload.close": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadHandleArgs, model.EmptyResult] {
			return ops.PayloadClose
		}),
		"payload.retain": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadHandleArgs, model.PayloadHandleResult] {
			return ops.PayloadRetain
		}),
		"payload.release": localHandler(controller, "payload", func(ops LocalOperations) Operation[model.PayloadHandleArgs, model.EmptyResult] {
			return ops.PayloadRelease
		}),

		"conversation.list": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.EmptyArgs, model.ConversationListResult] {
			return ops.ConversationList
		}),
		"conversation.status": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.ConversationIDArgs, model.ConversationStatus] {
			return ops.ConversationStatus
		}),
		"conversation.close": remoteHandler(controller, "delivery", func(ops AuthenticatedOperations) Operation[model.ConversationIDArgs, model.EmptyResult] {
			return ops.ConversationClose
		}),

		"policy.set":  localHandler(controller, "policy", func(ops LocalOperations) Operation[model.PolicySetArgs, model.PolicyResult] { return ops.PolicySet }),
		"policy.get":  localHandler(controller, "policy", func(ops LocalOperations) Operation[model.EmptyArgs, model.PolicyResult] { return ops.PolicyGet }),
		"policy.test": localHandler(controller, "policy", func(ops LocalOperations) Operation[model.PolicyTestArgs, model.PolicyResult] { return ops.PolicyTest }),

		"diagnostics.peer_status": remoteHandler(controller, "command", func(ops AuthenticatedOperations) Operation[model.DiagnosticsPeerArgs, model.PeerStatus] {
			return ops.DiagnosticsPeer
		}),
		"diagnostics.connectivity_status": remoteHandler(controller, "command", func(ops AuthenticatedOperations) Operation[model.EmptyArgs, model.ConnectivityStatus] {
			return ops.DiagnosticsNetwork
		}),
		"diagnostics.snapshot": localHandler(controller, "command", func(ops LocalOperations) Operation[model.EmptyArgs, model.DiagnosticSnapshot] {
			return ops.DiagnosticsSnapshot
		}),
		"diagnostics.logs.subscribe": localHandler(controller, "command", func(ops LocalOperations) Operation[model.DiagnosticsLogsArgs, model.EmptyResult] {
			return ops.DiagnosticsLogs
		}),
	}
	catalog := coreruntime.CommandCatalog()
	if len(handlers) != len(catalog) {
		return nil, ErrInvalidConfig
	}
	for _, command := range catalog {
		if handlers[command] == nil {
			return nil, ErrInvalidConfig
		}
	}
	return handlers, nil
}

func localHandler[A any, R model.ResultValue](controller *SessionController, stage string, selectOperation func(LocalOperations) Operation[A, R]) coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (model.Result, error) {
		args, ok := operationArgs[A](command.Args)
		if !ok {
			return providerFailure(command, "command", &ProviderError{Code: ProviderRejected}), nil
		}
		operation := selectOperation(controller.local)
		return invokeOperation(ctx, services, command, args, operation, stage), nil
	}
}

func (controller *SessionController) coreInitHandler() coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (result model.Result, err error) {
		args, ok := operationArgs[model.EmptyArgs](command.Args)
		if !ok {
			return providerFailure(command, "command", &ProviderError{Code: ProviderRejected}), nil
		}
		operation := controller.local.CoreInit
		if operation == nil {
			return providerFailure(command, "command", &ProviderError{Code: ProviderUnavailable}), nil
		}
		defer func() {
			if recover() != nil {
				result = providerFailure(command, "command", &ProviderError{Code: ProviderInternal})
				err = nil
			}
		}()
		value, failure := operation(ctx, services, command.SessionID, args)
		if failure != nil {
			return providerFailure(command, "command", normalizeProviderError(ctx, failure)), nil
		}
		if value.SessionID != command.SessionID {
			return providerFailure(command, "command", &ProviderError{Code: ProviderInternal}), nil
		}
		return success(command, value), nil
	}
}

func remoteHandler[A any, R model.ResultValue](controller *SessionController, stage string, selectOperation func(AuthenticatedOperations) Operation[A, R]) coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (model.Result, error) {
		args, ok := operationArgs[A](command.Args)
		if !ok {
			return providerFailure(command, "command", &ProviderError{Code: ProviderRejected}), nil
		}
		controller.mu.RLock()
		if controller.graph == nil || controller.personality == PersonalityUnset {
			controller.mu.RUnlock()
			return providerFailure(command, stage, &ProviderError{Code: ProviderUnavailable}), nil
		}
		operation := selectOperation(controller.graph.remote)
		controller.mu.RUnlock()
		return invokeOperation(ctx, services, command, args, operation, stage), nil
	}
}

// operationArgs mirrors the runtime's closed value-or-pointer command ABI
// without reflection. Nil pointers fail closed before reaching a provider.
func operationArgs[A any](input model.CommandArgs) (A, bool) {
	if value, ok := any(input).(A); ok {
		return value, true
	}
	if pointer, ok := any(input).(*A); ok && pointer != nil {
		return *pointer, true
	}
	var zero A
	return zero, false
}

func invokeOperation[A any, R model.ResultValue](ctx context.Context, services coreruntime.Services, command model.Command, args A, operation Operation[A, R], stage string) (result model.Result) {
	if operation == nil {
		return providerFailure(command, stage, &ProviderError{Code: ProviderUnavailable})
	}
	defer func() {
		if recover() != nil {
			result = providerFailure(command, stage, &ProviderError{Code: ProviderInternal})
		}
	}()
	value, failure := operation(ctx, services, args)
	if failure != nil {
		return providerFailure(command, stage, normalizeProviderError(ctx, failure))
	}
	return success(command, value)
}

func (controller *SessionController) authHandler(personality Personality) coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (model.Result, error) {
		args, ok := operationArgs[model.AuthArgs](command.Args)
		if !ok {
			return providerFailure(command, "command", &ProviderError{Code: ProviderRejected}), nil
		}
		return controller.authenticate(ctx, services, command, args, personality), nil
	}
}

func (controller *SessionController) agentIDHandler() coreruntime.Handler {
	return func(_ context.Context, _ coreruntime.Services, command model.Command) (model.Result, error) {
		if _, ok := operationArgs[model.EmptyArgs](command.Args); !ok {
			return providerFailure(command, "command", &ProviderError{Code: ProviderRejected}), nil
		}
		controller.mu.RLock()
		agentID := controller.snapshot.AgentID
		authenticated := controller.snapshot.Authenticated
		controller.mu.RUnlock()
		if !authenticated {
			return providerFailure(command, "auth", &ProviderError{Code: ProviderUnavailable}), nil
		}
		return success(command, model.AgentIDResult{AgentID: agentID}), nil
	}
}

func success[R model.ResultValue](command model.Command, value R) model.Result {
	return model.Result{CommandID: command.ID, Value: value}
}

func providerFailure(command model.Command, stage string, failure *ProviderError) model.Result {
	failure = failureOrInternal(failure)
	code, normalizedStage, retryable, cause := "core_error", stage, false, ErrServiceUnavailable
	switch failure.Code {
	case ProviderCancelled:
		code, normalizedStage, cause = "request_cancelled", "command", context.Canceled
	case ProviderDeadline:
		if stage == "rpc" && command.Name == "message.request" {
			code, normalizedStage, cause = "rpc_timeout", "rpc", context.DeadlineExceeded
		} else {
			code, retryable, cause = "connectivity_unavailable", true, context.DeadlineExceeded
		}
	case ProviderAuthenticationRejected:
		code, normalizedStage, cause = "authentication_failed", "auth", ErrAuthenticationFailed
	case ProviderUnavailable:
		code, retryable = "connectivity_unavailable", true
	case ProviderRejected:
		code = "command_error"
	case ProviderAuthorizationRejected:
		code, normalizedStage, cause = "authorization_rejected", "policy", ErrAuthorizationRejected
	case ProviderCapacity:
		code, retryable = "queue_full", true
	case ProviderInvalidHandle:
		code, cause = "invalid_handle", ErrInvalidHandle
	case ProviderPayloadTooLarge:
		code, normalizedStage, cause = "payload_too_large", "payload", ErrPayloadTooLarge
	case ProviderPayloadIntegrity:
		code, normalizedStage, cause = "payload_integrity_failed", "payload", ErrPayloadIntegrity
	case ProviderPayloadTransfer:
		code, normalizedStage, retryable, cause = "payload_transfer_failed", "payload", true, ErrPayloadTransfer
	case ProviderInternal:
		code, cause = "core_error", ErrComponentPanic
	}
	return model.Result{CommandID: command.ID, Err: &model.Error{Code: code, Stage: normalizedStage, Cause: cause, Retryable: retryable, Location: "local"}}
}
