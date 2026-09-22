package sessionkernel_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

type qaServices struct {
	commit func(context.Context, coreruntime.AuthenticatedPublisher) error
}

func (qaServices) Transition(model.LifecycleState) error { return nil }
func (services qaServices) CommitAuthenticated(ctx context.Context, publisher coreruntime.AuthenticatedPublisher) error {
	if services.commit != nil {
		return services.commit(ctx, publisher)
	}
	return publisher(func() error { return ctx.Err() })
}
func (qaServices) PublishEvent(context.Context, model.Event) error { return nil }
func (qaServices) Status() model.Status                            { return model.Status{} }
func (qaServices) Diagnostics() diagnostics.Snapshot               { return diagnostics.Snapshot{} }

type qaConnectivity struct {
	mu        sync.Mutex
	starts    int
	stops     int
	identity  sessionkernel.AuthenticatedIdentity
	remote    sessionkernel.AuthenticatedOperations
	stopPanic bool
	stopBlock <-chan struct{}
	onStart   func()
	onStop    func()
}

func (*qaConnectivity) Name() string { return "qa-connectivity" }
func (service *qaConnectivity) Start(context.Context) *sessionkernel.ProviderError {
	service.mu.Lock()
	service.starts++
	onStart := service.onStart
	service.mu.Unlock()
	if onStart != nil {
		onStart()
	}
	return nil
}
func (service *qaConnectivity) Shutdown(context.Context) *sessionkernel.ProviderError {
	service.mu.Lock()
	service.stops++
	stopPanic, stopBlock, onStop := service.stopPanic, service.stopBlock, service.onStop
	service.mu.Unlock()
	if onStop != nil {
		onStop()
	}
	if stopPanic {
		panic("qa private shutdown panic")
	}
	if stopBlock != nil {
		<-stopBlock
	}
	return nil
}
func (service *qaConnectivity) AuthenticatedIdentity() (sessionkernel.AuthenticatedIdentity, *sessionkernel.ProviderError) {
	return service.identity, nil
}
func (service *qaConnectivity) Operations() sessionkernel.AuthenticatedOperations {
	return service.remote
}
func (service *qaConnectivity) stopCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.stops
}

type qaConnectivityFactory struct {
	service sessionkernel.ConnectivityService
	failure *sessionkernel.ProviderError
	create  func(context.Context, sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError)
}

func (factory qaConnectivityFactory) Create(ctx context.Context, build sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
	if factory.create != nil {
		return factory.create(ctx, build)
	}
	return factory.service, factory.failure
}

type qaObjectService struct {
	mu        sync.Mutex
	starts    int
	stops     int
	stopPanic bool
	onStart   func()
	onStop    func()
}

func (*qaObjectService) Name() string { return "qa-objects" }
func (service *qaObjectService) Start(context.Context) *sessionkernel.ProviderError {
	service.mu.Lock()
	service.starts++
	onStart := service.onStart
	service.mu.Unlock()
	if onStart != nil {
		onStart()
	}
	return nil
}
func (service *qaObjectService) Shutdown(context.Context) *sessionkernel.ProviderError {
	service.mu.Lock()
	service.stops++
	stopPanic, onStop := service.stopPanic, service.onStop
	service.mu.Unlock()
	if onStop != nil {
		onStop()
	}
	if stopPanic {
		panic("qa private object shutdown panic")
	}
	return nil
}
func (service *qaObjectService) stopCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.stops
}

type qaObjectFactory struct {
	mu      sync.Mutex
	calls   int
	service sessionkernel.Service
	failure *sessionkernel.ProviderError
}

func (factory *qaObjectFactory) Create(context.Context, sessionkernel.ObjectBuildContext) (sessionkernel.Service, *sessionkernel.ProviderError) {
	factory.mu.Lock()
	factory.calls++
	factory.mu.Unlock()
	return factory.service, factory.failure
}
func (factory *qaObjectFactory) callCount() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.calls
}

type qaIdentityKeys struct{}

func (qaIdentityKeys) SealKey(context.Context, payload.KeyContext, []byte) ([]byte, error) {
	return []byte("sealed"), nil
}
func (qaIdentityKeys) OpenKey(context.Context, payload.KeyContext, []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

type qaTrace struct {
	mu    sync.Mutex
	calls []string
}

func (trace *qaTrace) add(value string) {
	trace.mu.Lock()
	trace.calls = append(trace.calls, value)
	trace.mu.Unlock()
}
func (trace *qaTrace) String() string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return fmt.Sprint(trace.calls)
}

func qaAuthCommand(name string) model.Command {
	return model.Command{
		ID:   "qa-auth",
		Name: name,
		Args: model.AuthArgs{
			MeshEndpoint:    "mesh.example.test:5222",
			Username:        "agent@example.test",
			Password:        []byte("secret"),
			MeshID:          "mesh-one",
			AgentInstanceID: "instance-one",
		},
	}
}

func qaFactory(t *testing.T, dependencies sessionkernel.Dependencies) (*sessionkernel.SessionController, coreruntime.Dependencies) {
	t.Helper()
	factory, err := sessionkernel.NewFactory(sessionkernel.DeploymentConfig{QueueLimit: 1}, dependencies)
	if err != nil {
		t.Fatalf("NewFactory() error = %v", err)
	}
	controller, runtimeDependencies, err := factory.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = controller.Shutdown(context.Background()) })
	return controller, runtimeDependencies
}

func TestStageBProviderReturningServiceAndFailureReleasesUnpublishedService(t *testing.T) {
	service := &qaConnectivity{stopPanic: true}
	_, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{
		service: service,
		failure: &sessionkernel.ProviderError{Code: sessionkernel.ProviderUnavailable},
	}})
	result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
	if err != nil {
		t.Fatalf("auth.connect handler error = %v", err)
	}
	if result.Err == nil {
		t.Fatalf("auth.connect result = %+v, want failure", result)
	}
	if got := service.stopCount(); got != 1 {
		t.Fatalf("unpublished provider service Shutdown calls = %d, want 1", got)
	}
}

func TestStageBObjectProviderReturningServiceAndFailureReleasesBothServicesInReverse(t *testing.T) {
	trace := &qaTrace{}
	connectivity := &qaConnectivity{
		identity: sessionkernel.AuthenticatedIdentity{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"},
		onStart:  func() { trace.add("start connectivity") }, onStop: func() { trace.add("stop connectivity") },
	}
	objects := &qaObjectService{stopPanic: true, onStop: func() { trace.add("stop objects") }}
	objectFactory := &qaObjectFactory{service: objects, failure: &sessionkernel.ProviderError{Code: sessionkernel.ProviderUnavailable}}
	_, dependencies := qaFactory(t, sessionkernel.Dependencies{
		Connectivity: qaConnectivityFactory{service: connectivity}, IdentityKeys: qaIdentityKeys{}, Objects: objectFactory,
	})
	result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
	if err != nil || result.Err == nil || result.Err.Code != "connectivity_unavailable" {
		t.Fatalf("dual object provider result = (%+v, %v)", result, err)
	}
	if got := objects.stopCount(); got != 1 {
		t.Fatalf("unpublished object Shutdown calls = %d, want 1", got)
	}
	if got := connectivity.stopCount(); got != 1 {
		t.Fatalf("connectivity rollback calls = %d, want 1", got)
	}
	if got := trace.String(); got != "[start connectivity stop objects stop connectivity]" {
		t.Fatalf("rollback order = %s", got)
	}
}

func TestStageBCommitServicePanicRollsBackSelectionAndAllowsRetry(t *testing.T) {
	first := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: first}})
	panicking := qaServices{commit: func(context.Context, coreruntime.AuthenticatedPublisher) error {
		panic("private dependency text must not escape")
	}}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("auth handler leaked CommitAuthenticated panic: %v", recovered)
			}
		}()
		result, err := dependencies.Handlers["auth.connect"](context.Background(), panicking, qaAuthCommand("auth.connect"))
		if err != nil || result.Err == nil || result.Err.Code != "core_error" {
			t.Fatalf("panicking commit result = (%+v, %v), want normalized core_error", result, err)
		}
	}()
	if snapshot := controller.Snapshot(); snapshot.Authenticated || snapshot.Personality != sessionkernel.PersonalityUnset {
		t.Fatalf("snapshot after commit panic = %+v", snapshot)
	}
	result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
	if err != nil || result.Err != nil {
		t.Fatalf("retry after commit panic = (%+v, %v)", result, err)
	}
}

func TestStageBCommitReadyPanicAndCancellationRollBackAndAllowLaterAuthentication(t *testing.T) {
	for _, test := range []struct {
		name     string
		commit   func(context.Context, coreruntime.AuthenticatedPublisher) error
		wantCode string
	}{
		{
			name: "commit_ready_panic",
			commit: func(_ context.Context, publisher coreruntime.AuthenticatedPublisher) error {
				return publisher(func() error { panic("private commit-ready panic") })
			},
			wantCode: "core_error",
		},
		{
			name: "commit_error",
			commit: func(context.Context, coreruntime.AuthenticatedPublisher) error {
				return errors.New("private commit error")
			},
			wantCode: "command_error",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
				AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
			}}
			controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
			result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{commit: test.commit}, qaAuthCommand("auth.connect"))
			if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != test.wantCode {
				t.Fatalf("failed commit = (%+v, %v)", result, err)
			}
			if text := fmt.Sprintf("%+v", result.Err); bytes.Contains([]byte(text), []byte("private commit")) {
				t.Fatalf("private dependency error escaped: %q", text)
			}
			if snapshot := controller.Snapshot(); snapshot.Authenticated || snapshot.Personality != sessionkernel.PersonalityUnset {
				t.Fatalf("failed commit snapshot = %+v", snapshot)
			}
			retry, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
			if err != nil || retry.Err != nil {
				t.Fatalf("retry after failed commit = (%+v, %v)", retry, err)
			}
		})
	}

	t.Run("cancellation_during_commit_ready", func(t *testing.T) {
		service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
			AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
		}}
		controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
		ctx, cancel := context.WithCancel(context.Background())
		services := qaServices{commit: func(_ context.Context, publisher coreruntime.AuthenticatedPublisher) error {
			cancel()
			return publisher(func() error { return ctx.Err() })
		}}
		result, err := dependencies.Handlers["auth.connect"](ctx, services, qaAuthCommand("auth.connect"))
		if err != nil || result.Err == nil || result.Err.Code != "request_cancelled" || result.Value != nil {
			t.Fatalf("cancelled commit = (%+v, %v)", result, err)
		}
		if snapshot := controller.Snapshot(); snapshot.Authenticated || snapshot.Personality != sessionkernel.PersonalityUnset {
			t.Fatalf("cancelled commit snapshot = %+v", snapshot)
		}
		retry, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
		if err != nil || retry.Err != nil {
			t.Fatalf("retry after cancellation = (%+v, %v)", retry, err)
		}
	})
}

func TestStageBShutdownCancelsAuthenticationAndDoesNotWaitOnStaleSelection(t *testing.T) {
	service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
	commitEntered := make(chan struct{})
	authDone := make(chan model.Result, 1)
	go func() {
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), qaServices{commit: func(ctx context.Context, _ coreruntime.AuthenticatedPublisher) error {
			close(commitEntered)
			<-ctx.Done()
			return ctx.Err()
		}}, qaAuthCommand("auth.connect"))
		authDone <- result
	}()
	<-commitEntered
	started := time.Now()
	if err := controller.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Shutdown waited on stale selection for %v", elapsed)
	}
	result := <-authDone
	if result.Err == nil || result.Err.Code != "request_cancelled" {
		t.Fatalf("cancelled authentication = %+v", result)
	}
	if snapshot := controller.Snapshot(); snapshot.Authenticated {
		t.Fatalf("shutdown published graph: %+v", snapshot)
	}
}

func TestStageBRuntimeConfigCannotDivergeFromDeploymentQueueCapacity(t *testing.T) {
	factory, err := sessionkernel.NewFactory(sessionkernel.DeploymentConfig{QueueLimit: 1}, sessionkernel.Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 2}); err == nil {
		t.Fatal("RuntimeConfig accepted queue_limit=2 for deployment queue_limit=1")
	}
	config, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 1})
	if err != nil || config.QueueLimit != 1 || config.CleanupTimeout != 30*time.Second {
		t.Fatalf("matching RuntimeConfig = (%+v, %v)", config, err)
	}
}

func TestStageBCoreInitReceivesExactSessionIDAndRejectsProviderSubstitution(t *testing.T) {
	var received string
	local := sessionkernel.LocalOperations{CoreInit: func(_ context.Context, _ coreruntime.Services, sessionID string, _ model.EmptyArgs) (model.CoreInitResult, *sessionkernel.ProviderError) {
		received = sessionID
		return model.CoreInitResult{SessionID: sessionID}, nil
	}}
	_, dependencies := qaFactory(t, sessionkernel.Dependencies{Local: local})
	empty := model.EmptyArgs{}
	command := model.Command{ID: "core-init", Name: "core.init", SessionID: "sdk-session-exact", Args: &empty}
	result, err := dependencies.Handlers["core.init"](context.Background(), qaServices{}, command)
	value, ok := result.Value.(model.CoreInitResult)
	if err != nil || result.Err != nil || !ok || received != command.SessionID || value.SessionID != command.SessionID {
		t.Fatalf("core.init = (%+v, %v), received=%q", result, err, received)
	}

	local.CoreInit = func(context.Context, coreruntime.Services, string, model.EmptyArgs) (model.CoreInitResult, *sessionkernel.ProviderError) {
		return model.CoreInitResult{SessionID: "substituted-private-session"}, nil
	}
	_, dependencies = qaFactory(t, sessionkernel.Dependencies{Local: local})
	result, err = dependencies.Handlers["core.init"](context.Background(), qaServices{}, command)
	if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != "core_error" || result.Err.Stage != "command" || strings.Contains(fmt.Sprintf("%+v", result), "substituted-private-session") {
		t.Fatalf("substituted core.init = (%+v, %v)", result, err)
	}
}

func TestStageBProviderErrorTaxonomyIsClosedExactAndTextFree(t *testing.T) {
	tests := []struct {
		name      string
		provider  sessionkernel.ProviderErrorCode
		code      string
		stage     string
		retryable bool
		cause     error
	}{
		{"cancelled", sessionkernel.ProviderCancelled, "request_cancelled", "command", false, context.Canceled},
		{"deadline", sessionkernel.ProviderDeadline, "connectivity_unavailable", "payload", true, context.DeadlineExceeded},
		{"auth_rejected", sessionkernel.ProviderAuthenticationRejected, "authentication_failed", "auth", false, sessionkernel.ErrAuthenticationFailed},
		{"unavailable", sessionkernel.ProviderUnavailable, "connectivity_unavailable", "payload", true, sessionkernel.ErrServiceUnavailable},
		{"rejected", sessionkernel.ProviderRejected, "command_error", "payload", false, sessionkernel.ErrServiceUnavailable},
		{"authorization_rejected", sessionkernel.ProviderAuthorizationRejected, "authorization_rejected", "policy", false, sessionkernel.ErrAuthorizationRejected},
		{"capacity", sessionkernel.ProviderCapacity, "queue_full", "payload", true, sessionkernel.ErrServiceUnavailable},
		{"invalid_handle", sessionkernel.ProviderInvalidHandle, "invalid_handle", "payload", false, sessionkernel.ErrInvalidHandle},
		{"too_large", sessionkernel.ProviderPayloadTooLarge, "payload_too_large", "payload", false, sessionkernel.ErrPayloadTooLarge},
		{"integrity", sessionkernel.ProviderPayloadIntegrity, "payload_integrity_failed", "payload", false, sessionkernel.ErrPayloadIntegrity},
		{"transfer", sessionkernel.ProviderPayloadTransfer, "payload_transfer_failed", "payload", true, sessionkernel.ErrPayloadTransfer},
		{"internal", sessionkernel.ProviderInternal, "core_error", "payload", false, sessionkernel.ErrComponentPanic},
		{"unknown_zero", 0, "core_error", "payload", false, sessionkernel.ErrComponentPanic},
		{"unknown_high", sessionkernel.ProviderErrorCode(255), "core_error", "payload", false, sessionkernel.ErrComponentPanic},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := sessionkernel.LocalOperations{PayloadOpen: func(context.Context, coreruntime.Services, model.EmptyArgs) (model.PayloadHandleResult, *sessionkernel.ProviderError) {
				return model.PayloadHandleResult{}, &sessionkernel.ProviderError{Code: test.provider}
			}}
			_, dependencies := qaFactory(t, sessionkernel.Dependencies{Local: local})
			result, err := dependencies.Handlers["payload.open"](context.Background(), qaServices{}, model.Command{ID: "payload", Name: "payload.open", Args: model.EmptyArgs{}})
			if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != test.code || result.Err.Stage != test.stage || result.Err.Retryable != test.retryable || !errors.Is(result.Err.Cause, test.cause) {
				t.Fatalf("provider %d = (%+v, %v)", test.provider, result, err)
			}
			if text := fmt.Sprintf("%+v", result.Err); strings.Contains(text, "dependency") || strings.Contains(text, "private") {
				t.Fatalf("dependency text escaped: %q", text)
			}
		})
	}
}

func TestMessageRequestDeadlinePublishesLocalRPCTimeout(t *testing.T) {
	tests := []struct {
		name      string
		operation sessionkernel.Operation[model.MessageRequestArgs, model.ResponseResult]
		context   func() (context.Context, context.CancelFunc)
	}{
		{
			name: "provider_deadline",
			operation: func(context.Context, coreruntime.Services, model.MessageRequestArgs) (model.ResponseResult, *sessionkernel.ProviderError) {
				return model.ResponseResult{}, &sessionkernel.ProviderError{Code: sessionkernel.ProviderDeadline}
			},
			context: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			name: "context_deadline_normalizes_provider_failure",
			operation: func(context.Context, coreruntime.Services, model.MessageRequestArgs) (model.ResponseResult, *sessionkernel.ProviderError) {
				return model.ResponseResult{}, &sessionkernel.ProviderError{Code: sessionkernel.ProviderUnavailable}
			},
			context: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 0)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connectivity := &qaConnectivity{
				identity: sessionkernel.AuthenticatedIdentity{
					AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
				},
				remote: sessionkernel.AuthenticatedOperations{MessageRequest: test.operation},
			}
			_, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: connectivity}})
			login, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
			if err != nil || login.Err != nil {
				t.Fatalf("auth.connect = (%+v, %v)", login, err)
			}

			ctx, cancel := test.context()
			defer cancel()
			result, err := dependencies.Handlers["message.request"](ctx, qaServices{}, model.Command{
				ID: "request-deadline", Name: "message.request",
				Args: model.MessageRequestArgs{To: "peer@example.test", TTL: time.Second},
			})
			if err != nil || result.Value != nil || result.Err == nil {
				t.Fatalf("message.request = (%+v, %v)", result, err)
			}
			if result.Err.Code != "rpc_timeout" || result.Err.Stage != "rpc" || result.Err.Location != "local" || result.Err.Retryable || !errors.Is(result.Err.Cause, context.DeadlineExceeded) {
				t.Fatalf("message.request deadline = %+v", result.Err)
			}
		})
	}
}

func TestStageBQueueOneRuntimeCommitsAuthenticatedGraphAtomically(t *testing.T) {
	service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	factory, err := sessionkernel.NewFactory(sessionkernel.DeploymentConfig{QueueLimit: 1}, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
	if err != nil {
		t.Fatal(err)
	}
	controller, dependencies, err := factory.Build()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := commandgate.New(1)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := coreruntime.NewWithDependencies(runtimeConfig, gate, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Dispatch(context.Background(), qaAuthCommand("auth.connect"))
	if err != nil || result.Err != nil || runtime.Status().Lifecycle != model.LifecycleReady || !controller.Snapshot().Authenticated {
		t.Fatalf("queue-one auth = (%+v, %v), status=%+v snapshot=%+v", result, err, runtime.Status(), controller.Snapshot())
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, nextErr := runtime.NextEvent(context.Background()); nextErr != nil {
				return
			}
		}
	}()
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdown); err != nil {
		t.Fatalf("runtime shutdown = %v", err)
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("event drain remained blocked after runtime shutdown")
	}
}
