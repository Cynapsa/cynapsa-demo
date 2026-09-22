package cynapsagocore

import (
	"context"
	"errors"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

// localServices implements only operations whose complete semantics are owned
// by the process-local facade. The session kernel remains the sole exhaustive
// command dispatcher and couples every operation to its exact argument/result
// types at compile time.
type localServices struct {
	adapter         *sdkboundary.Adapter
	availability    localCapabilityAvailability
	addresses       *addressStore
	gate            *commandgate.Gate
	payloads        *payload.HandleStore
	handlers        *mesh.HandlerRegistry
	policies        *mesh.PolicyController
	payloadPipeline *payload.Pipeline
	runtime         *coreruntime.Runtime
	session         *sessionkernel.SessionController
	diagnosticsFeed *messagingRuntimeBridge
}

// localCapabilityAvailability is the closed set of optional application
// behavior that the constructed Core can actually execute. The SDK boundary's
// capability catalog remains the maximum versioned allowlist; core.capabilities
// filters that catalog through this construction-time availability state.
type localCapabilityAvailability struct {
	largePayloads bool
}

func (s *localServices) operations() sessionkernel.LocalOperations {
	return sessionkernel.LocalOperations{
		CoreInit:               s.coreInit,
		CoreCapabilities:       s.capabilities,
		CoreStatus:             s.status,
		CoreShutdown:           s.coreShutdown,
		ConfigGet:              s.configGet,
		ConfigUpdate:           s.configUpdate,
		CommandChannelRegister: s.commandChannelRegister,
		CommandChannelClear:    s.commandChannelClear,
		CommandCancel:          s.commandCancel,
		EventSinkRegister:      s.eventSinkRegister,
		EventSinkClear:         s.eventSinkClear,
		EventSinkBind:          s.eventSinkBind,
		AddressPut:             s.addressPut,
		AddressRemove:          s.addressRemove,
		AddressList:            s.addressList,
		AddressResolve:         s.addressResolve,
		DeliveryNext:           s.deliveryNext,
		DeliveryAccept:         s.deliveryAccept,
		DeliveryQueueStatus:    s.deliveryQueueStatus,
		DeliveryPause:          s.deliveryPause,
		DeliveryResume:         s.deliveryResume,
		HandlerRegister:        s.handlerRegister,
		HandlerUnregister:      s.handlerUnregister,
		PayloadOpen:            s.payloadOpen,
		PayloadWrite:           s.payloadWrite,
		PayloadFinish:          s.payloadFinish,
		PayloadCancel:          s.payloadCancel,
		PayloadRead:            s.payloadRead,
		PayloadClose:           s.payloadRelease,
		PayloadRetain:          s.payloadRetain,
		PayloadRelease:         s.payloadRelease,
		PolicySet:              s.policySet,
		PolicyGet:              s.policyGet,
		PolicyTest:             s.policyTest,
		DiagnosticsSnapshot:    s.diagnostics,
		DiagnosticsLogs:        s.diagnosticsLogs,
	}
}

func (s *localServices) coreShutdown(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if ctx == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := ctx.Err(); err != nil {
		return model.EmptyResult{}, providerContextError(ctx)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) commandCancel(ctx context.Context, _ coreruntime.Services, args model.CommandCancelArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.gate == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if ctx == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := ctx.Err(); err != nil {
		return model.EmptyResult{}, providerContextError(ctx)
	}
	if err := s.gate.Cancel(commandgate.CommandHandle(args.CommandHandle)); err != nil {
		switch {
		case errors.Is(err, commandgate.ErrCommandNotFound),
			errors.Is(err, commandgate.ErrEmptyCommandHandle),
			errors.Is(err, commandgate.ErrAlreadyTerminal),
			errors.Is(err, commandgate.ErrCommandCompleted):
			return model.EmptyResult{}, providerError(sessionkernel.ProviderInvalidHandle)
		case errors.Is(err, commandgate.ErrGateClosing), errors.Is(err, commandgate.ErrGateClosed):
			return model.EmptyResult{}, providerError(sessionkernel.ProviderUnavailable)
		default:
			return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
		}
	}
	return model.EmptyResult{}, nil
}

func providerContextError(ctx context.Context) *sessionkernel.ProviderError {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return providerError(sessionkernel.ProviderDeadline)
	}
	return providerError(sessionkernel.ProviderCancelled)
}

func (s *localServices) addressPut(_ context.Context, _ coreruntime.Services, args model.AddressPutArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.addresses == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.addresses.Put(args.Mapping); err != nil {
		return model.EmptyResult{}, providerAddressError(err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) addressRemove(_ context.Context, _ coreruntime.Services, args model.AddressRemoveArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.addresses == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.addresses.Remove(args.VirtualOrigin); err != nil {
		return model.EmptyResult{}, providerAddressError(err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) addressList(_ context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.AddressMappingsResult, *sessionkernel.ProviderError) {
	if s.addresses == nil {
		return model.AddressMappingsResult{}, providerError(sessionkernel.ProviderInternal)
	}
	return model.AddressMappingsResult{Mappings: s.addresses.List()}, nil
}

func (s *localServices) addressResolve(_ context.Context, _ coreruntime.Services, args model.AddressResolveArgs) (model.AddressResolution, *sessionkernel.ProviderError) {
	if s.addresses == nil {
		return model.AddressResolution{}, providerError(sessionkernel.ProviderInternal)
	}
	resolution, err := s.addresses.Resolve(args.URL)
	if err != nil {
		return model.AddressResolution{}, providerAddressError(err)
	}
	return resolution, nil
}

func providerAddressError(err error) *sessionkernel.ProviderError {
	if errors.Is(err, errAddressStoreCapacity) {
		return providerError(sessionkernel.ProviderCapacity)
	}
	if errors.Is(err, errAddressStoreInvalid) || errors.Is(err, errAddressStoreMissing) {
		return providerError(sessionkernel.ProviderRejected)
	}
	return providerError(sessionkernel.ProviderInternal)
}

func (s *localServices) coreInit(_ context.Context, _ coreruntime.Services, sessionID string, _ model.EmptyArgs) (model.CoreInitResult, *sessionkernel.ProviderError) {
	return model.CoreInitResult{SessionID: sessionID}, nil
}

func (s *localServices) capabilities(_ context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.CapabilitiesResult, *sessionkernel.ProviderError) {
	public := s.adapter.PublicCapabilities()
	commands := make([]string, len(public.Commands))
	for index, item := range public.Commands {
		commands[index] = string(item)
	}
	features := make([]string, 0, len(public.Features))
	for _, item := range public.Features {
		if item == v1.CapabilityLargePayloads && !s.availability.largePayloads {
			continue
		}
		features = append(features, string(item))
	}
	return model.CapabilitiesResult{Commands: commands, Features: features}, nil
}

func (s *localServices) status(_ context.Context, services coreruntime.Services, _ model.EmptyArgs) (model.StatusResult, *sessionkernel.ProviderError) {
	contribution := s.authenticatedDiagnostics()
	return model.StatusResult{Status: mergeSessionStatus(services.Status(), s.session, contribution.queuedMessages)}, nil
}

func (s *localServices) configGet(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.ConfigResult, *sessionkernel.ProviderError) {
	if failure := localContextFailure(ctx); failure != nil {
		return model.ConfigResult{}, failure
	}
	if s.runtime == nil {
		return model.ConfigResult{}, providerError(sessionkernel.ProviderInternal)
	}
	config := s.runtime.ConfigSnapshot()
	config.Connectivity.BootstrapData = nil
	return model.ConfigResult{Config: config}, nil
}

func (s *localServices) configUpdate(ctx context.Context, _ coreruntime.Services, args model.ConfigUpdateArgs) (model.ConfigResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.ConfigResult{}, providerError(sessionkernel.ProviderInternal)
	}
	config, err := s.runtime.UpdateConfig(ctx, args)
	if err != nil {
		return model.ConfigResult{}, providerRuntimeError(ctx, err)
	}
	config.Connectivity.BootstrapData = nil
	return model.ConfigResult{Config: config}, nil
}

func (s *localServices) commandChannelRegister(ctx context.Context, _ coreruntime.Services, args model.ChannelRegisterArgs) (model.CompletionChannelResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.CompletionChannelResult{}, providerError(sessionkernel.ProviderInternal)
	}
	result, err := s.runtime.RegisterCompletionChannel(ctx, args)
	if err != nil {
		return model.CompletionChannelResult{}, providerRuntimeError(ctx, err)
	}
	return result, nil
}

func (s *localServices) commandChannelClear(ctx context.Context, _ coreruntime.Services, args model.ChannelIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.ClearCompletionChannel(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) eventSinkRegister(ctx context.Context, _ coreruntime.Services, args model.EventSinkRegisterArgs) (model.EventSinkResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EventSinkResult{}, providerError(sessionkernel.ProviderInternal)
	}
	result, err := s.runtime.RegisterEventSink(ctx, args)
	if err != nil {
		return model.EventSinkResult{}, providerRuntimeError(ctx, err)
	}
	return result, nil
}

func (s *localServices) eventSinkClear(ctx context.Context, _ coreruntime.Services, args model.EventSinkIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.ClearEventSink(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) eventSinkBind(ctx context.Context, _ coreruntime.Services, args model.EventSinkIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.BindEventSink(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) deliveryNext(ctx context.Context, _ coreruntime.Services, args model.EmptyArgs) (model.EventResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EventResult{}, providerError(sessionkernel.ProviderInternal)
	}
	result, err := s.runtime.PollDelivery(ctx, args)
	if err != nil {
		return model.EventResult{}, providerRuntimeError(ctx, err)
	}
	return result, nil
}

func (s *localServices) deliveryAccept(ctx context.Context, _ coreruntime.Services, args model.DeliveryAcceptArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.AcceptDelivery(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) deliveryQueueStatus(ctx context.Context, _ coreruntime.Services, args model.EmptyArgs) (model.DeliveryQueueStatus, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.DeliveryQueueStatus{}, providerError(sessionkernel.ProviderInternal)
	}
	result, err := s.runtime.DeliveryQueueStatus(ctx, args)
	if err != nil {
		return model.DeliveryQueueStatus{}, providerRuntimeError(ctx, err)
	}
	return result, nil
}

func (s *localServices) deliveryPause(ctx context.Context, _ coreruntime.Services, args model.EmptyArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.PauseDelivery(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) deliveryResume(ctx context.Context, _ coreruntime.Services, args model.EmptyArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.ResumeDelivery(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) handlerRegister(ctx context.Context, _ coreruntime.Services, args model.HandlerPathArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if failure := localContextFailure(ctx); failure != nil {
		return model.EmptyResult{}, failure
	}
	if s.handlers == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.handlers.Register(args.Path); err != nil {
		return model.EmptyResult{}, providerMeshError(err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) handlerUnregister(ctx context.Context, _ coreruntime.Services, args model.HandlerPathArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if failure := localContextFailure(ctx); failure != nil {
		return model.EmptyResult{}, failure
	}
	if s.handlers == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.handlers.Unregister(args.Path); err != nil {
		return model.EmptyResult{}, providerMeshError(err)
	}
	return model.EmptyResult{}, nil
}

func (s *localServices) policySet(ctx context.Context, _ coreruntime.Services, args model.PolicySetArgs) (model.PolicyResult, *sessionkernel.ProviderError) {
	if failure := localContextFailure(ctx); failure != nil {
		return model.PolicyResult{}, failure
	}
	if s.policies == nil {
		return model.PolicyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if failure := s.policies.Replace(args.Rules); failure != nil {
		return model.PolicyResult{}, providerMeshFailure(failure)
	}
	return s.policies.Result(), nil
}

func (s *localServices) policyGet(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.PolicyResult, *sessionkernel.ProviderError) {
	if failure := localContextFailure(ctx); failure != nil {
		return model.PolicyResult{}, failure
	}
	if s.policies == nil {
		return model.PolicyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	return s.policies.Result(), nil
}

func (s *localServices) policyTest(ctx context.Context, _ coreruntime.Services, args model.PolicyTestArgs) (model.PolicyResult, *sessionkernel.ProviderError) {
	if s.policies == nil || s.payloadPipeline == nil {
		return model.PolicyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if ctx == nil {
		return model.PolicyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := ctx.Err(); err != nil {
		return model.PolicyResult{}, providerContextError(ctx)
	}
	prepared, err := s.payloadPipeline.Prepare(args.Input.Payload)
	if err != nil {
		return model.PolicyResult{}, providerPolicyPayloadError(err)
	}
	defer clearPipelineBytes(prepared.Canonical)
	if err := ctx.Err(); err != nil {
		return model.PolicyResult{}, providerContextError(ctx)
	}
	path, err := s.payloadPipeline.ApplicationPath(prepared.Profile, prepared.Canonical)
	if err != nil {
		return model.PolicyResult{}, providerPolicyPayloadError(err)
	}
	if path == "" || path != prepared.ApplicationPath {
		return model.PolicyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	result := s.policies.Result()
	result.Allowed = s.policies.Allows(args.Input.To, path)
	return result, nil
}

func (s *localServices) diagnosticsLogs(ctx context.Context, _ coreruntime.Services, args model.DiagnosticsLogsArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.runtime == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := s.runtime.SetDiagnosticLogSubscription(ctx, args); err != nil {
		return model.EmptyResult{}, providerRuntimeError(ctx, err)
	}
	return model.EmptyResult{}, nil
}

func providerRuntimeError(ctx context.Context, err error) *sessionkernel.ProviderError {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return providerError(sessionkernel.ProviderDeadline)
	case errors.Is(err, context.Canceled):
		return providerError(sessionkernel.ProviderCancelled)
	case errors.Is(err, coreruntime.ErrControlNotFound), errors.Is(err, coreruntime.ErrDeliveryNotPending),
		errors.Is(err, delivery.ErrInvalidLease), errors.Is(err, delivery.ErrLeaseRetired):
		return providerError(sessionkernel.ProviderInvalidHandle)
	case errors.Is(err, coreruntime.ErrControlCapacity), errors.Is(err, delivery.ErrQueueFull):
		return providerError(sessionkernel.ProviderCapacity)
	case errors.Is(err, delivery.ErrQueueEmpty), errors.Is(err, coreruntime.ErrClosing), errors.Is(err, coreruntime.ErrClosed),
		errors.Is(err, delivery.ErrClosing), errors.Is(err, delivery.ErrClosed):
		return providerError(sessionkernel.ProviderUnavailable)
	case errors.Is(err, coreruntime.ErrInvalidConfig), errors.Is(err, coreruntime.ErrImmutableConfig),
		errors.Is(err, coreruntime.ErrControlRegistered), errors.Is(err, coreruntime.ErrDiagnosticLogsOff),
		errors.Is(err, coreruntime.ErrDeliveryPaused), errors.Is(err, coreruntime.ErrDeliveryAcceptPending),
		errors.Is(err, delivery.ErrConsumerBusy):
		return providerError(sessionkernel.ProviderRejected)
	case errors.Is(err, delivery.ErrAdmissionExhausted):
		return providerError(sessionkernel.ProviderCapacity)
	default:
		if ctx != nil && ctx.Err() != nil {
			return providerContextError(ctx)
		}
		return providerError(sessionkernel.ProviderInternal)
	}
}

func providerPolicyPayloadError(err error) *sessionkernel.ProviderError {
	switch classifyPayloadDisposition(err) {
	case mesh.PayloadCancelled:
		return providerError(sessionkernel.ProviderCancelled)
	case mesh.PayloadDeadline:
		return providerError(sessionkernel.ProviderDeadline)
	case mesh.PayloadUnavailable:
		return providerError(sessionkernel.ProviderUnavailable)
	case mesh.PayloadAuthorization:
		return providerError(sessionkernel.ProviderAuthorizationRejected)
	case mesh.PayloadCapacity:
		return providerError(sessionkernel.ProviderCapacity)
	case mesh.PayloadTooLarge:
		return providerError(sessionkernel.ProviderPayloadTooLarge)
	case mesh.PayloadIntegrity:
		return providerError(sessionkernel.ProviderPayloadIntegrity)
	case mesh.PayloadTransferFailed:
		return providerError(sessionkernel.ProviderPayloadTransfer)
	case mesh.PayloadInvalidHandle:
		return providerError(sessionkernel.ProviderInvalidHandle)
	case mesh.PayloadInternal:
		return providerError(sessionkernel.ProviderInternal)
	default:
		return providerError(sessionkernel.ProviderRejected)
	}
}

func localContextFailure(ctx context.Context) *sessionkernel.ProviderError {
	if ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	if ctx.Err() != nil {
		return providerContextError(ctx)
	}
	return nil
}

func providerMeshError(err error) *sessionkernel.ProviderError {
	switch {
	case errors.Is(err, mesh.ErrHandlerCapacity):
		return providerError(sessionkernel.ProviderCapacity)
	case errors.Is(err, mesh.ErrHandlerInvalid):
		return providerError(sessionkernel.ProviderRejected)
	default:
		return providerError(sessionkernel.ProviderInternal)
	}
}

func providerMeshFailure(failure *mesh.Failure) *sessionkernel.ProviderError {
	if failure == nil {
		return nil
	}
	code := sessionkernel.ProviderInternal
	switch failure.Code {
	case mesh.FailureCancelled:
		code = sessionkernel.ProviderCancelled
	case mesh.FailureDeadline:
		code = sessionkernel.ProviderDeadline
	case mesh.FailureUnavailable:
		code = sessionkernel.ProviderUnavailable
	case mesh.FailureRejected:
		code = sessionkernel.ProviderRejected
	case mesh.FailureAuthorization:
		code = sessionkernel.ProviderAuthorizationRejected
	case mesh.FailureCapacity:
		code = sessionkernel.ProviderCapacity
	case mesh.FailureInvalidHandle:
		code = sessionkernel.ProviderInvalidHandle
	case mesh.FailurePayloadTooLarge:
		code = sessionkernel.ProviderPayloadTooLarge
	case mesh.FailurePayloadIntegrity:
		code = sessionkernel.ProviderPayloadIntegrity
	case mesh.FailurePayloadTransfer:
		code = sessionkernel.ProviderPayloadTransfer
	}
	return providerError(code)
}

func (s *localServices) diagnostics(_ context.Context, services coreruntime.Services, _ model.EmptyArgs) (model.DiagnosticSnapshot, *sessionkernel.ProviderError) {
	snapshot := services.Diagnostics()
	contribution := s.authenticatedDiagnostics()
	return model.DiagnosticSnapshot{
		Status:               mergeSessionStatus(services.Status(), s.session, contribution.queuedMessages),
		CommandQueueDepth:    uint64(nonnegative(snapshot.CommandQueueDepth)),
		EventQueueDepth:      uint64(nonnegative(snapshot.EventQueueDepth)),
		PeerCount:            contribution.peerCount,
		QueuedMessageCount:   contribution.queuedMessages,
		PendingRPCCount:      contribution.pendingRPC,
		PayloadTransferCount: contribution.payloadTransfers,
	}, nil
}

func (s *localServices) authenticatedDiagnostics() authenticatedDiagnosticSnapshot {
	if s == nil || s.diagnosticsFeed == nil {
		return authenticatedDiagnosticSnapshot{}
	}
	return s.diagnosticsFeed.diagnosticSnapshot()
}

func mergeSessionStatus(status model.Status, controller *sessionkernel.SessionController, queuedMessages uint64) model.Status {
	status.QueuedMessageCount = queuedMessages
	if controller == nil {
		return status
	}
	snapshot := controller.Snapshot()
	if !snapshot.Authenticated {
		return status
	}
	status.Personality = string(snapshot.Personality)
	status.AgentID = snapshot.AgentID
	status.MeshID = snapshot.MeshID
	status.MeshEndpoint = snapshot.MeshEndpoint
	status.ConnectivityDetail = "available"
	return status
}

func (s *localServices) payloadOpen(_ context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.PayloadHandleResult{}, providerPayloadError(payload.ErrInvalidLimits)
	}
	handle, err := s.payloads.Open()
	if err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	return model.PayloadHandleResult{Handle: handle}, nil
}

func (s *localServices) payloadWrite(_ context.Context, _ coreruntime.Services, args model.PayloadWriteArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.PayloadHandleResult{}, providerPayloadError(payload.ErrInvalidHandle)
	}
	if err := s.payloads.Write(args.Handle, args.Chunk); err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	return model.PayloadHandleResult{Handle: args.Handle, Size: uint64(len(args.Chunk))}, nil
}

func (s *localServices) payloadRead(_ context.Context, _ coreruntime.Services, args model.PayloadReadArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.PayloadHandleResult{}, providerPayloadError(payload.ErrInvalidHandle)
	}
	chunk, eof, err := s.payloads.Read(args.Handle, int64(args.Offset), int(args.Limit))
	if err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	return model.PayloadHandleResult{Handle: args.Handle, Size: uint64(len(chunk)), EOF: eof, Chunk: chunk}, nil
}

func (s *localServices) payloadFinish(_ context.Context, _ coreruntime.Services, args model.PayloadHandleArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.PayloadHandleResult{}, providerPayloadError(payload.ErrInvalidHandle)
	}
	if err := s.payloads.Finish(args.Handle); err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	size, err := s.payloads.Size(args.Handle)
	if err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	return model.PayloadHandleResult{Handle: args.Handle, Size: uint64(size)}, nil
}

func (s *localServices) payloadCancel(_ context.Context, _ coreruntime.Services, args model.PayloadHandleArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return s.payloadMutation(args, (*payload.HandleStore).Cancel)
}

func (s *localServices) payloadRetain(_ context.Context, _ coreruntime.Services, args model.PayloadHandleArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.PayloadHandleResult{}, providerPayloadError(payload.ErrInvalidHandle)
	}
	if err := s.payloads.Retain(args.Handle); err != nil {
		return model.PayloadHandleResult{}, providerPayloadError(err)
	}
	return model.PayloadHandleResult{Handle: args.Handle}, nil
}

func (s *localServices) payloadRelease(_ context.Context, _ coreruntime.Services, args model.PayloadHandleArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return s.payloadMutation(args, (*payload.HandleStore).Release)
}

func (s *localServices) payloadMutation(args model.PayloadHandleArgs, operation func(*payload.HandleStore, string) error) (model.EmptyResult, *sessionkernel.ProviderError) {
	if s.payloads == nil {
		return model.EmptyResult{}, providerPayloadError(payload.ErrInvalidHandle)
	}
	if err := operation(s.payloads, args.Handle); err != nil {
		return model.EmptyResult{}, providerPayloadError(err)
	}
	return model.EmptyResult{}, nil
}

func providerPayloadError(err error) *sessionkernel.ProviderError {
	code := sessionkernel.ProviderPayloadTransfer
	switch {
	case errors.Is(err, payload.ErrInvalidHandle), errors.Is(err, payload.ErrInvalidHandleState):
		code = sessionkernel.ProviderInvalidHandle
	case errors.Is(err, payload.ErrPayloadTooLarge):
		code = sessionkernel.ProviderPayloadTooLarge
	case errors.Is(err, payload.ErrIntegrity):
		code = sessionkernel.ProviderPayloadIntegrity
	case errors.Is(err, payload.ErrHandleCapacity), errors.Is(err, payload.ErrQueueFull):
		code = sessionkernel.ProviderCapacity
	}
	return &sessionkernel.ProviderError{Code: code}
}

func nonnegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}
