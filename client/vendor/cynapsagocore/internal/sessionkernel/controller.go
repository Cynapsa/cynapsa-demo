package sessionkernel

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// SessionController owns one authenticated private graph. All command routing
// is an explicit, compile-time argument/result pairing in dispatch.go.
type SessionController struct {
	mu sync.RWMutex

	profile         OperationalProfile
	clock           coreclock.Clock
	random          io.Reader
	connect         ConnectivityFactory
	enroll          enrollment.Provider
	enrollmentState enrollment.StateStore
	authenticated   AuthenticatedServiceFactory
	keys            IdentityKeyProvider
	objects         ObjectServiceFactory
	local           LocalOperations

	started     bool
	closed      bool
	lifetime    context.Context
	cancel      context.CancelFunc
	selecting   bool
	authDone    chan struct{}
	personality Personality
	graph       *serviceGraph
	snapshot    Snapshot

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  *ProviderError
	renewalWG    sync.WaitGroup
}

func (controller *SessionController) Name() string { return "session-controller" }

// Start creates only local lifetime ownership. Authentication and networking
// remain deferred to auth.login or auth.connect.
func (controller *SessionController) Start(ctx context.Context) error {
	if controller == nil || ctx == nil || controller.shutdownDone == nil || controller.clock == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.closed {
		return ErrClosed
	}
	if controller.started {
		return nil
	}
	controller.lifetime, controller.cancel = context.WithCancel(context.Background())
	controller.started = true
	return nil
}

// Shutdown has one total 30-second bound covering both authentication join and
// reverse graph cleanup. Dependency noncooperation cannot extend that bound.
func (controller *SessionController) Shutdown(ctx context.Context) error {
	if controller == nil || ctx == nil || controller.shutdownDone == nil {
		return ErrInvalidConfig
	}
	controller.shutdownOnce.Do(func() {
		controller.mu.Lock()
		controller.closed = true
		if controller.cancel != nil {
			controller.cancel()
		}
		controller.mu.Unlock()
		go controller.cleanup()
	})
	select {
	case <-controller.shutdownDone:
		controller.mu.RLock()
		failure := controller.shutdownErr
		controller.mu.RUnlock()
		if failure != nil {
			return ErrComponentStopFailed
		}
		return nil
	case <-ctx.Done():
		return ErrShutdownTimeout
	}
}

func (controller *SessionController) cleanup() {
	cleanup, cancel := context.WithTimeout(context.Background(), controller.profile.ReconnectOperationTimeout)
	defer cancel()
	controller.mu.RLock()
	authDone := controller.authDone
	controller.mu.RUnlock()
	if authDone != nil {
		select {
		case <-authDone:
		case <-cleanup.Done():
			controller.mu.Lock()
			controller.shutdownErr = providerContextError(cleanup)
			controller.mu.Unlock()
			close(controller.shutdownDone)
			return
		}
	}
	controller.renewalWG.Wait()
	// Lock acquisition is part of the total bound. Authentication never holds
	// controller.mu while waiting for a provider.
	locked := make(chan struct{}, 1)
	go func() {
		controller.mu.Lock()
		if cleanup.Err() != nil {
			controller.mu.Unlock()
			return
		}
		locked <- struct{}{}
	}()
	var graph *serviceGraph
	select {
	case <-locked:
		graph = controller.graph
		controller.graph = nil
		controller.mu.Unlock()
	case <-cleanup.Done():
		controller.mu.Lock()
		controller.shutdownErr = providerContextError(cleanup)
		controller.mu.Unlock()
		close(controller.shutdownDone)
		return
	}
	failure := graph.shutdown(cleanup)
	controller.mu.Lock()
	controller.shutdownErr = failure
	controller.mu.Unlock()
	close(controller.shutdownDone)
}

func (controller *SessionController) authenticate(ctx context.Context, services coreruntime.Services, command model.Command, args model.AuthArgs, personality Personality) model.Result {
	if validateAuthentication(args) != nil {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderAuthenticationRejected})
	}
	operation, cancel, failure := controller.beginAuthentication(ctx)
	if failure != nil {
		return providerFailure(command, "auth", failure)
	}
	defer controller.finishSelection()
	defer cancel()
	ownedPassword := append([]byte(nil), args.Password...)
	defer clear(ownedPassword)
	return controller.completeAuthentication(operation, services, command, Authentication{
		MeshEndpoint: args.MeshEndpoint, Username: args.Username, Password: ownedPassword,
		MeshID: args.MeshID, AgentInstanceID: args.AgentInstanceID,
	}, args.MeshEndpoint, personality)
}

func (controller *SessionController) completeAuthentication(operation context.Context, services coreruntime.Services, command model.Command, auth Authentication, publicMeshEndpoint string, personality Personality) model.Result {
	graph, offload, failure := controller.buildGraph(operation, auth)
	if failure != nil {
		if graph != nil {
			cleanup, cleanupCancel := context.WithTimeout(context.Background(), controller.profile.ReconnectOperationTimeout)
			_ = graph.shutdown(cleanup)
			cleanupCancel()
		}
		return providerFailure(command, "auth", failure)
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, cleanupCancel := context.WithTimeout(context.Background(), controller.profile.ReconnectOperationTimeout)
			_ = graph.shutdown(cleanup)
			cleanupCancel()
		}
	}()

	commitFailure := invokeAuthenticatedCommit(services, operation, func(commitReady func() error) error {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		if controller.closed || !controller.selecting || controller.graph != nil || controller.personality != PersonalityUnset {
			return ErrClosed
		}
		controller.graph = graph
		controller.personality = personality
		controller.snapshot = Snapshot{
			Personality: personality, AgentID: graph.identity.AgentID, MeshID: graph.identity.MeshID,
			MeshEndpoint: publicMeshEndpoint, AgentInstanceID: auth.AgentInstanceID,
			OffloadAvailable: offload, Authenticated: true,
		}
		published := false
		defer func() {
			if !published {
				controller.graph = nil
				controller.personality = PersonalityUnset
				controller.snapshot = Snapshot{}
			}
		}()
		if err := commitReady(); err != nil {
			return ErrAuthenticationFailed
		}
		committed = true
		published = true
		return nil
	})
	if !committed {
		return providerFailure(command, "auth", classifyCommitError(operation, commitFailure))
	}
	return success(command, model.AuthResult{
		AgentID: graph.identity.AgentID, MeshID: graph.identity.MeshID,
		AgentInstanceID: auth.AgentInstanceID, Personality: string(personality),
		ProfileID: auth.ProfileID, CredentialExpiresAt: auth.CredentialUsableUntil,
		OfflineStartDeadline: auth.OfflineStartDeadline, OfflineColdStartTargetSeconds: auth.OfflineColdStartTargetSeconds,
		OfflineTargetSatisfied: auth.OfflineTargetSatisfied, PolicyRevision: auth.PolicyRevision,
		SessionExpiryMode: auth.SessionExpiryMode, PreparationStatus: auth.PreparationStatus,
	})
}

func invokeAuthenticatedCommit(services coreruntime.Services, ctx context.Context, publisher coreruntime.AuthenticatedPublisher) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrComponentPanic
		}
	}()
	return services.CommitAuthenticated(ctx, publisher)
}

func (controller *SessionController) beginAuthentication(ctx context.Context) (context.Context, context.CancelFunc, *ProviderError) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.started || controller.closed {
		return nil, nil, &ProviderError{Code: ProviderCancelled}
	}
	if controller.personality != PersonalityUnset || controller.graph != nil || controller.selecting {
		return nil, nil, &ProviderError{Code: ProviderRejected}
	}
	deadline, err := controller.profile.OperationDeadline(ctx, controller.clock.Now())
	if err != nil {
		return nil, nil, &ProviderError{Code: ProviderInternal}
	}
	operation, cancel := context.WithDeadline(ctx, deadline)
	stopLifetime := context.AfterFunc(controller.lifetime, cancel)
	controller.selecting = true
	controller.authDone = make(chan struct{})
	return operation, func() { stopLifetime(); cancel() }, nil
}

func (controller *SessionController) finishSelection() {
	controller.mu.Lock()
	controller.selecting = false
	if controller.authDone != nil {
		close(controller.authDone)
	}
	controller.mu.Unlock()
}

func (controller *SessionController) buildGraph(ctx context.Context, auth Authentication) (*serviceGraph, bool, *ProviderError) {
	if auth.TokenAuthentication && (auth.CredentialUsableUntil.IsZero() || !controller.clock.Now().UTC().Before(auth.CredentialUsableUntil)) {
		return nil, false, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	if controller.connect == nil {
		return nil, false, &ProviderError{Code: ProviderUnavailable}
	}
	build := BuildContext{Auth: cloneAuthentication(auth), Profile: controller.profile, Clock: controller.clock, Random: controller.random, IdentityKeys: controller.keys}
	connectivity, failure := createConnectivity(controller.connect, ctx, build)
	if failure != nil || connectivity == nil {
		return nil, false, normalizeProviderError(ctx, failureOrInternal(failure))
	}
	graph := &serviceGraph{}
	if failure = graph.start(ctx, connectivity); failure != nil {
		return nil, false, failure
	}
	identity, failure := authenticatedIdentity(ctx, connectivity)
	if failure != nil || validateIdentity(identity, auth.Username, auth.MeshID, auth.SessionResource) != nil {
		return graph, false, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	graph.identity = identity
	graph.remote, failure = connectivityOperations(ctx, connectivity)
	if failure != nil {
		return graph, false, failure
	}
	offload := false
	var objectRuntime ObjectRuntime
	if controller.objects != nil {
		objectService, objectFailure := createObjectService(controller.objects, ctx, ObjectBuildContext{
			Identity: identity, Profile: controller.profile, Clock: controller.clock,
			Random: controller.random, IdentityKeys: controller.keys, Connectivity: connectivity,
		})
		if objectFailure != nil || objectService == nil {
			return graph, false, normalizeProviderError(ctx, failureOrInternal(objectFailure))
		}
		if failure = graph.start(ctx, objectService); failure != nil {
			return graph, false, failure
		}
		offload = true
		if provider, ok := objectService.(ObjectRuntimeService); ok {
			objectRuntime = provider.ObjectRuntime()
		}
	}
	if controller.authenticated != nil {
		authenticatedService, authenticatedOperations, authenticatedFailure := createAuthenticatedService(controller.authenticated, ctx, AuthenticatedBuildContext{
			Identity: identity, Profile: controller.profile, Clock: controller.clock,
			Random: controller.random, Connectivity: connectivity, IdentityKeys: controller.keys, Objects: objectRuntime,
		})
		if authenticatedFailure != nil || authenticatedService == nil {
			return graph, false, normalizeProviderError(ctx, failureOrInternal(authenticatedFailure))
		}
		if failure = graph.start(ctx, authenticatedService); failure != nil {
			return graph, false, failure
		}
		graph.remote, failure = mergeAuthenticatedOperations(graph.remote, authenticatedOperations)
		if failure != nil {
			return graph, false, failure
		}
	}
	return graph, offload, nil
}

func (controller *SessionController) Snapshot() Snapshot {
	if controller == nil {
		return Snapshot{}
	}
	controller.mu.RLock()
	defer controller.mu.RUnlock()
	return controller.snapshot
}

func validateAuthentication(args model.AuthArgs) error {
	if args.MeshEndpoint == "" || len(args.MeshEndpoint) > 4096 || protocol.ValidateAgentIdentity(args.Username) != nil || strings.Contains(args.Username, "/") || protocol.ValidateMeshID(args.MeshID) != nil || strings.Contains(args.MeshID, "/") || len(args.Password) == 0 || len(args.Password) > mesh.MaxCredentialBytes || len(args.AgentInstanceID) > protocol.MaxAgentIdentityBytes {
		return ErrAuthenticationFailed
	}
	return nil
}

func validateIdentity(identity AuthenticatedIdentity, requestedBare, requestedMesh string, requestedResource ...string) error {
	resource := requestedMesh
	if len(requestedResource) > 0 && requestedResource[0] != "" {
		resource = requestedResource[0]
	}
	if protocol.ValidateAgentIdentity(identity.AgentID) != nil || strings.Contains(identity.AgentID, "/") || identity.AgentID != requestedBare || protocol.ValidateMeshID(identity.MeshID) != nil || identity.MeshID != requestedMesh || identity.BoundIdentity != requestedBare+"/"+resource || protocol.ValidateAgentIdentity(identity.BoundIdentity) != nil {
		return ErrAuthenticationFailed
	}
	return nil
}

func createConnectivity(factory ConnectivityFactory, ctx context.Context, build BuildContext) (ConnectivityService, *ProviderError) {
	type result struct {
		service ConnectivityService
		failure *ProviderError
	}
	handoff := newResultHandoff[result]()
	go func() {
		var output result
		providerBuild := build
		defer func() {
			clear(providerBuild.Auth.Password)
			if recover() != nil {
				output = result{failure: &ProviderError{Code: ProviderInternal}}
			}
			if !handoff.publish(output) && output.service != nil {
				lateCleanup(output.service)
			}
		}()
		output.service, output.failure = factory.Create(ctx, providerBuild)
		output.failure = validateProviderError(output.failure)
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return nil, providerContextError(ctx)
	}
	if ctx.Err() != nil {
		if output.service != nil {
			cleanupCancelled(output.service)
		}
		return nil, providerContextError(ctx)
	}
	if output.failure != nil {
		if output.service != nil {
			cleanupUnpublished(ctx, output.service)
		}
		return nil, normalizeProviderError(ctx, output.failure)
	}
	return output.service, nil
}

func createObjectService(factory ObjectServiceFactory, ctx context.Context, build ObjectBuildContext) (Service, *ProviderError) {
	type result struct {
		service Service
		failure *ProviderError
	}
	handoff := newResultHandoff[result]()
	go func() {
		var output result
		defer func() {
			if recover() != nil {
				output = result{failure: &ProviderError{Code: ProviderInternal}}
			}
			if !handoff.publish(output) && output.service != nil {
				lateCleanup(output.service)
			}
		}()
		output.service, output.failure = factory.Create(ctx, build)
		output.failure = validateProviderError(output.failure)
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return nil, providerContextError(ctx)
	}
	if ctx.Err() != nil {
		if output.service != nil {
			cleanupCancelled(output.service)
		}
		return nil, providerContextError(ctx)
	}
	if output.failure != nil {
		if output.service != nil {
			cleanupUnpublished(ctx, output.service)
		}
		return nil, normalizeProviderError(ctx, output.failure)
	}
	return output.service, nil
}

func createAuthenticatedService(factory AuthenticatedServiceFactory, ctx context.Context, build AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError) {
	type result struct {
		service    Service
		operations AuthenticatedOperations
		failure    *ProviderError
	}
	handoff := newResultHandoff[result]()
	go func() {
		var output result
		defer func() {
			if recover() != nil {
				output = result{failure: &ProviderError{Code: ProviderInternal}}
			}
			if !handoff.publish(output) && output.service != nil {
				lateCleanup(output.service)
			}
		}()
		output.service, output.operations, output.failure = factory.Create(ctx, build)
		output.failure = validateProviderError(output.failure)
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return nil, AuthenticatedOperations{}, providerContextError(ctx)
	}
	if ctx.Err() != nil {
		if output.service != nil {
			cleanupCancelled(output.service)
		}
		return nil, AuthenticatedOperations{}, providerContextError(ctx)
	}
	if output.failure != nil {
		if output.service != nil {
			cleanupUnpublished(ctx, output.service)
		}
		return nil, AuthenticatedOperations{}, normalizeProviderError(ctx, output.failure)
	}
	return output.service, output.operations, nil
}

func authenticatedIdentity(ctx context.Context, service ConnectivityService) (identity AuthenticatedIdentity, failure *ProviderError) {
	defer func() {
		if recover() != nil {
			identity = AuthenticatedIdentity{}
			failure = &ProviderError{Code: ProviderInternal}
		}
	}()
	identity, failure = service.AuthenticatedIdentity()
	return identity, normalizeProviderError(ctx, failure)
}

func connectivityOperations(ctx context.Context, service ConnectivityService) (operations AuthenticatedOperations, failure *ProviderError) {
	defer func() {
		if recover() != nil {
			operations = AuthenticatedOperations{}
			failure = &ProviderError{Code: ProviderInternal}
		}
	}()
	if service == nil {
		return AuthenticatedOperations{}, &ProviderError{Code: ProviderInternal}
	}
	operations = service.Operations()
	if ctx != nil && ctx.Err() != nil {
		return AuthenticatedOperations{}, providerContextError(ctx)
	}
	return operations, nil
}

func mergeAuthenticatedOperations(current, added AuthenticatedOperations) (AuthenticatedOperations, *ProviderError) {
	if current.Logout != nil && added.Logout != nil ||
		current.MeshList != nil && added.MeshList != nil ||
		current.MeshRefresh != nil && added.MeshRefresh != nil ||
		current.MessageSend != nil && added.MessageSend != nil ||
		current.MessageRequest != nil && added.MessageRequest != nil ||
		current.MessageReply != nil && added.MessageReply != nil ||
		current.DeliveryRetry != nil && added.DeliveryRetry != nil ||
		current.DeliveryDrop != nil && added.DeliveryDrop != nil ||
		current.ConversationList != nil && added.ConversationList != nil ||
		current.ConversationStatus != nil && added.ConversationStatus != nil ||
		current.ConversationClose != nil && added.ConversationClose != nil ||
		current.DiagnosticsPeer != nil && added.DiagnosticsPeer != nil ||
		current.DiagnosticsNetwork != nil && added.DiagnosticsNetwork != nil {
		return AuthenticatedOperations{}, &ProviderError{Code: ProviderInternal}
	}
	if added.Logout != nil {
		current.Logout = added.Logout
	}
	if added.MeshList != nil {
		current.MeshList = added.MeshList
	}
	if added.MeshRefresh != nil {
		current.MeshRefresh = added.MeshRefresh
	}
	if added.MessageSend != nil {
		current.MessageSend = added.MessageSend
	}
	if added.MessageRequest != nil {
		current.MessageRequest = added.MessageRequest
	}
	if added.MessageReply != nil {
		current.MessageReply = added.MessageReply
	}
	if added.DeliveryRetry != nil {
		current.DeliveryRetry = added.DeliveryRetry
	}
	if added.DeliveryDrop != nil {
		current.DeliveryDrop = added.DeliveryDrop
	}
	if added.ConversationList != nil {
		current.ConversationList = added.ConversationList
	}
	if added.ConversationStatus != nil {
		current.ConversationStatus = added.ConversationStatus
	}
	if added.ConversationClose != nil {
		current.ConversationClose = added.ConversationClose
	}
	if added.DiagnosticsPeer != nil {
		current.DiagnosticsPeer = added.DiagnosticsPeer
	}
	if added.DiagnosticsNetwork != nil {
		current.DiagnosticsNetwork = added.DiagnosticsNetwork
	}
	return current, nil
}

func cloneAuthentication(input Authentication) Authentication {
	input.Password = append([]byte(nil), input.Password...)
	return input
}

func failureOrInternal(failure *ProviderError) *ProviderError {
	if failure == nil {
		return &ProviderError{Code: ProviderInternal}
	}
	return failure
}

func classifyCommitError(ctx context.Context, err error) *ProviderError {
	if ctx != nil && ctx.Err() != nil {
		return providerContextError(ctx)
	}
	if err == nil {
		return &ProviderError{Code: ProviderInternal}
	}
	if errors.Is(err, ErrComponentPanic) {
		return &ProviderError{Code: ProviderInternal}
	}
	return &ProviderError{Code: ProviderRejected}
}
