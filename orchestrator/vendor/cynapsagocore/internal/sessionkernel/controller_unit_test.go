package sessionkernel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

type testServices struct {
	mu         sync.Mutex
	commits    int
	commitFail bool
	onCommit   func()
}

type panicCommitServices struct{ testServices }

func (*panicCommitServices) CommitAuthenticated(context.Context, coreruntime.AuthenticatedPublisher) error {
	panic("private commit panic")
}

type panicCommitReadyServices struct{ testServices }

func (*panicCommitReadyServices) CommitAuthenticated(_ context.Context, publisher coreruntime.AuthenticatedPublisher) error {
	return publisher(func() error { panic("private ready panic") })
}

func (*testServices) Transition(model.LifecycleState) error { return nil }
func (services *testServices) CommitAuthenticated(ctx context.Context, publisher coreruntime.AuthenticatedPublisher) error {
	services.mu.Lock()
	services.commits++
	onCommit := services.onCommit
	services.mu.Unlock()
	if onCommit != nil {
		onCommit()
	}
	return publisher(func() error {
		if services.commitFail {
			return ErrAuthenticationFailed
		}
		return ctx.Err()
	})
}
func (*testServices) PublishEvent(context.Context, model.Event) error { return nil }
func (*testServices) Status() model.Status                            { return model.Status{} }
func (*testServices) Diagnostics() diagnostics.Snapshot               { return diagnostics.Snapshot{} }

type testService struct {
	name            string
	identity        AuthenticatedIdentity
	remote          AuthenticatedOperations
	startFail       *ProviderError
	stopFail        *ProviderError
	startBlock      <-chan struct{}
	startEnter      chan struct{}
	stopBlock       <-chan struct{}
	stopEnter       chan struct{}
	startPanic      bool
	stopPanic       bool
	operationsPanic bool
	recorder        *testRecorder
	onceStart       sync.Once
	onceStop        sync.Once
	mu              sync.Mutex
	calls           []string
}

func (service *testService) Name() string { return service.name }
func (service *testService) Start(ctx context.Context) *ProviderError {
	service.record("start:" + service.name)
	if service.startPanic {
		panic("private start panic")
	}
	if service.startEnter != nil {
		service.onceStart.Do(func() { close(service.startEnter) })
	}
	if service.startBlock != nil {
		select {
		case <-service.startBlock:
		case <-ctx.Done():
			return providerContextError(ctx)
		}
	}
	return service.startFail
}
func (service *testService) Shutdown(ctx context.Context) *ProviderError {
	service.record("stop:" + service.name)
	if service.stopPanic {
		panic("private stop panic")
	}
	if service.stopEnter != nil {
		service.onceStop.Do(func() { close(service.stopEnter) })
	}
	if service.stopBlock != nil {
		select {
		case <-service.stopBlock:
		case <-ctx.Done():
			return providerContextError(ctx)
		}
	}
	return service.stopFail
}
func (service *testService) AuthenticatedIdentity() (AuthenticatedIdentity, *ProviderError) {
	return service.identity, nil
}
func (service *testService) Operations() AuthenticatedOperations {
	if service.operationsPanic {
		panic("private operations panic")
	}
	return service.remote
}
func (service *testService) record(call string) {
	service.mu.Lock()
	service.calls = append(service.calls, call)
	service.mu.Unlock()
	if service.recorder != nil {
		service.recorder.add(call)
	}
}
func (service *testService) snapshot() []string {
	service.mu.Lock()
	defer service.mu.Unlock()
	return append([]string(nil), service.calls...)
}

type testRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (recorder *testRecorder) add(call string) {
	recorder.mu.Lock()
	recorder.calls = append(recorder.calls, call)
	recorder.mu.Unlock()
}
func (recorder *testRecorder) snapshot() []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.calls...)
}

type testConnectivityFactory struct {
	mu            sync.Mutex
	service       ConnectivityService
	failure       *ProviderError
	block         <-chan struct{}
	entered       chan struct{}
	ignoreContext bool
	once          sync.Once
	calls         int
}

func (factory *testConnectivityFactory) Create(ctx context.Context, _ BuildContext) (ConnectivityService, *ProviderError) {
	factory.mu.Lock()
	factory.calls++
	service, failure, block, entered, ignoreContext := factory.service, factory.failure, factory.block, factory.entered, factory.ignoreContext
	factory.mu.Unlock()
	if entered != nil {
		factory.once.Do(func() { close(entered) })
	}
	if block != nil {
		if ignoreContext {
			<-block
		} else {
			select {
			case <-block:
			case <-ctx.Done():
				return nil, providerContextError(ctx)
			}
		}
	}
	return service, failure
}

type testObjectFactory struct {
	service  Service
	failure  *ProviderError
	calls    int
	captured ObjectBuildContext
	mu       sync.Mutex
}

type testAuthenticatedFactory struct {
	mu          sync.Mutex
	service     Service
	operations  AuthenticatedOperations
	failure     *ProviderError
	create      func(context.Context, AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError)
	captured    AuthenticatedBuildContext
	calls       int
	panicCreate bool
}

func (factory *testAuthenticatedFactory) Create(ctx context.Context, build AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError) {
	factory.mu.Lock()
	factory.calls++
	factory.captured = build
	create := factory.create
	service, operations, failure, panicCreate := factory.service, factory.operations, factory.failure, factory.panicCreate
	factory.mu.Unlock()
	if panicCreate {
		panic("private authenticated factory panic")
	}
	if create != nil {
		return create(ctx, build)
	}
	return service, operations, failure
}

func (factory *testObjectFactory) Create(_ context.Context, build ObjectBuildContext) (Service, *ProviderError) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	factory.calls++
	factory.captured = build
	return factory.service, factory.failure
}

type testObjectRuntime struct{}

func (testObjectRuntime) Available(context.Context) (bool, error) { return false, nil }
func (testObjectRuntime) Upload(context.Context, []byte) (string, error) {
	return "", payload.ErrCarrierUnavailable
}
func (testObjectRuntime) Download(context.Context, string) ([]byte, error) {
	return nil, payload.ErrCarrierUnavailable
}
func (testObjectRuntime) PrepareUpload(context.Context, int64) (payload.PreparedObjectUpload, error) {
	return nil, payload.ErrCarrierUnavailable
}
func (testObjectRuntime) ProbeDownloadReference(context.Context, string) error {
	return payload.ErrCarrierUnavailable
}

type testObjectRuntimeService struct {
	*testService
	runtime ObjectRuntime
}

func (service *testObjectRuntimeService) ObjectRuntime() ObjectRuntime { return service.runtime }

type testKeys struct{}

func (testKeys) SealKey(context.Context, payload.KeyContext, []byte) ([]byte, error) {
	return []byte("wrapped"), nil
}
func (testKeys) OpenKey(context.Context, payload.KeyContext, []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

type cancelReturningConnectivityFactory struct {
	cancel  context.CancelFunc
	service ConnectivityService
}

func (factory cancelReturningConnectivityFactory) Create(context.Context, BuildContext) (ConnectivityService, *ProviderError) {
	factory.cancel()
	return factory.service, nil
}

type cancelReturningObjectFactory struct {
	cancel  context.CancelFunc
	service Service
}

func (factory cancelReturningObjectFactory) Create(context.Context, ObjectBuildContext) (Service, *ProviderError) {
	factory.cancel()
	return factory.service, nil
}

type cancelReturningAuthenticatedFactory struct {
	cancel  context.CancelFunc
	service Service
}

func (factory cancelReturningAuthenticatedFactory) Create(context.Context, AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError) {
	factory.cancel()
	return factory.service, AuthenticatedOperations{MessageSend: sendOperation}, nil
}

type cancelReturningStartService struct {
	testService
	cancel context.CancelFunc
}

func (service *cancelReturningStartService) Start(context.Context) *ProviderError {
	service.record("start:" + service.name)
	service.cancel()
	return nil
}

func TestFactoryBuildIsExhaustiveKernelOnlyAndFreezesCleanup(t *testing.T) {
	connectivity := &testConnectivityFactory{service: successfulConnectivity()}
	factory := newTestFactory(t, Dependencies{Connectivity: connectivity, Local: LocalOperations{CoreStatus: statusOperation}})
	controller, dependencies, err := factory.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog := coreruntime.CommandCatalog()
	if len(dependencies.Handlers) != len(catalog) {
		t.Fatalf("handler count = %d, catalog = %d", len(dependencies.Handlers), len(catalog))
	}
	for _, command := range catalog {
		if dependencies.Handlers[command] == nil {
			t.Errorf("missing command %q", command)
		}
	}
	if connectivity.calls != 0 {
		t.Fatalf("kernel start created connectivity: %d", connectivity.calls)
	}
	config, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 16})
	if err != nil || config.CleanupTimeout != 30*time.Second {
		t.Fatalf("runtime config = (%+v, %v)", config, err)
	}
	if config.QueueLimit != uint32(controller.profile.QueueCapacity) {
		t.Fatalf("runtime queue limit = %d, profile capacity = %d", config.QueueLimit, controller.profile.QueueCapacity)
	}
	_ = controller.Shutdown(context.Background())
}

func TestRuntimeConfigRejectsQueueLimitDifferentFromFactoryAuthority(t *testing.T) {
	factory := newTestFactory(t, Dependencies{})
	if config, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 15}); err != ErrInvalidConfig || config.QueueLimit != 0 || config.CleanupTimeout != 0 {
		t.Fatalf("mismatched runtime config = (%+v, %v)", config, err)
	}
	config, err := factory.RuntimeConfig(model.RuntimeConfig{QueueLimit: 16})
	if err != nil || config.QueueLimit != 16 || config.QueueLimit != uint32(factory.profile.QueueCapacity) {
		t.Fatalf("coherent runtime config = (%+v, %v), profile=%+v", config, err, factory.profile)
	}
}

func TestPureLocalWorksBeforeAuthWhileRemoteFailsClosed(t *testing.T) {
	factory := newTestFactory(t, Dependencies{Local: LocalOperations{CoreStatus: statusOperation}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{}
	local, _ := dependencies.Handlers["core.status"](context.Background(), services, model.Command{ID: "local", Name: "core.status", Args: model.EmptyArgs{}})
	if local.Err != nil {
		t.Fatalf("local = %+v", local)
	}
	remote, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "remote", Name: "message.send", Args: model.MessageSendArgs{}})
	if remote.Err == nil || remote.Err.Code != "connectivity_unavailable" || !remote.Err.Retryable {
		t.Fatalf("remote = %+v", remote)
	}
	auth, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if auth.Err == nil || auth.Err.Code != "connectivity_unavailable" {
		t.Fatalf("auth = %+v", auth)
	}
	_ = controller.Shutdown(context.Background())
}

func TestCoreInitReceivesAndReturnsExactCommandSessionID(t *testing.T) {
	received := ""
	operation := func(_ context.Context, _ coreruntime.Services, sessionID string, _ model.EmptyArgs) (model.CoreInitResult, *ProviderError) {
		received = sessionID
		return model.CoreInitResult{SessionID: sessionID}, nil
	}
	factory := newTestFactory(t, Dependencies{Local: LocalOperations{CoreInit: operation}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	command := model.Command{ID: "init", Name: "core.init", SessionID: "sdk-session-exact", Args: &model.EmptyArgs{}}
	result, _ := dependencies.Handlers["core.init"](context.Background(), &testServices{}, command)
	if received != command.SessionID {
		t.Fatalf("operation session ID = %q, want %q", received, command.SessionID)
	}
	value, ok := result.Value.(model.CoreInitResult)
	if !ok || result.Err != nil || value.SessionID != command.SessionID {
		t.Fatalf("core.init result = %+v", result)
	}
	_ = controller.Shutdown(context.Background())
}

func TestCoreInitRejectsProviderSessionIDSubstitution(t *testing.T) {
	operation := func(context.Context, coreruntime.Services, string, model.EmptyArgs) (model.CoreInitResult, *ProviderError) {
		return model.CoreInitResult{SessionID: "different-session"}, nil
	}
	factory := newTestFactory(t, Dependencies{Local: LocalOperations{CoreInit: operation}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["core.init"](context.Background(), &testServices{}, model.Command{ID: "init", Name: "core.init", SessionID: "sdk-session", Args: model.EmptyArgs{}})
	if result.Value != nil || result.Err == nil || result.Err.Code != "core_error" || result.Err.Stage != "command" {
		t.Fatalf("substituted core.init result = %+v", result)
	}
	_ = controller.Shutdown(context.Background())
}

func TestTypedHandlersAcceptRuntimeValueAndPointerCommandABI(t *testing.T) {
	connectivity := successfulConnectivity()
	connectivity.remote.MessageSend = sendOperation
	factory := newTestFactory(t, Dependencies{
		Connectivity: &testConnectivityFactory{service: connectivity},
		Local:        LocalOperations{CoreStatus: statusOperation},
	})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{}
	auth := authCommand("auth.connect")
	authArgs := auth.Args.(model.AuthArgs)
	auth.Args = &authArgs
	if result, _ := dependencies.Handlers["auth.connect"](context.Background(), services, auth); result.Err != nil {
		t.Fatalf("pointer auth = %+v", result)
	}
	empty := model.EmptyArgs{}
	if result, _ := dependencies.Handlers["core.status"](context.Background(), services, model.Command{ID: "local-pointer", Name: "core.status", Args: &empty}); result.Err != nil {
		t.Fatalf("pointer local = %+v", result)
	}
	send := model.MessageSendArgs{}
	if result, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "remote-pointer", Name: "message.send", Args: &send}); result.Err != nil {
		t.Fatalf("pointer remote = %+v", result)
	}
	var nilSend *model.MessageSendArgs
	if result, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "nil-pointer", Name: "message.send", Args: nilSend}); result.Err == nil || result.Err.Code != "command_error" {
		t.Fatalf("nil pointer = %+v", result)
	}
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticationCommitsGraphAndPersonalityAtomicallyWithoutBoundIdentityLeak(t *testing.T) {
	connectivity := successfulConnectivity()
	connectivity.remote.MessageSend = sendOperation
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{}
	result, _ := dependencies.Handlers["auth.login"](context.Background(), services, authCommand("auth.login"))
	if result.Err != nil {
		t.Fatalf("login = %+v", result)
	}
	auth := result.Value.(model.AuthResult)
	if auth.AgentID != "agent@example.test" || auth.AgentID == connectivity.identity.BoundIdentity || auth.Personality != string(PersonalityHTTPBridge) {
		t.Fatalf("public auth = %+v, private bound = %q", auth, connectivity.identity.BoundIdentity)
	}
	if snapshot := controller.Snapshot(); snapshot.AgentID != "agent@example.test" || !snapshot.Authenticated || snapshot.Personality != PersonalityHTTPBridge {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	sent, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "send", Name: "message.send", Args: model.MessageSendArgs{}})
	if _, ok := sent.Value.(model.SendResult); !ok || sent.Err != nil {
		t.Fatalf("send = %+v", sent)
	}
	second, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if second.Err == nil || services.commits != 1 {
		t.Fatalf("second = %+v, commits=%d", second, services.commits)
	}
	_ = controller.Shutdown(context.Background())
}

func TestCommitFailureLeavesGraphAndPersonalityUnpublishedAndRollsBack(t *testing.T) {
	connectivity := successfulConnectivity()
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{commitFail: true}
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if result.Err == nil || controller.Snapshot().Authenticated || controller.personality != PersonalityUnset {
		t.Fatalf("result=%+v snapshot=%+v", result, controller.Snapshot())
	}
	if got := fmt.Sprint(connectivity.snapshot()); got != "[start:connectivity stop:connectivity]" {
		t.Fatalf("rollback = %s", got)
	}
	_ = controller.Shutdown(context.Background())
}

func TestCommitPanicIsContainedRollsBackAndUnstrandsSelection(t *testing.T) {
	connectivity := successfulConnectivity()
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}})
	factory.profile.ReconnectOperationTimeout = time.Second
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())

	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &panicCommitServices{}, authCommand("auth.connect"))
	if result.Value != nil || result.Err == nil || result.Err.Code != "core_error" {
		t.Fatalf("panic result = %+v", result)
	}
	controller.mu.RLock()
	selecting, authDone := controller.selecting, controller.authDone
	controller.mu.RUnlock()
	if selecting {
		t.Fatal("selection remained active after commit panic")
	}
	select {
	case <-authDone:
	default:
		t.Fatal("authentication join remained open after commit panic")
	}
	if got := fmt.Sprint(connectivity.snapshot()); got != "[start:connectivity stop:connectivity]" {
		t.Fatalf("panic rollback = %s", got)
	}

	result, _ = dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("later authentication = %+v", result)
	}
	started := time.Now()
	if err := controller.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown waited on stale authentication: %v", elapsed)
	}
}

func TestCommitReadyPanicRollsBackStagedPublication(t *testing.T) {
	connectivity := successfulConnectivity()
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &panicCommitReadyServices{}, authCommand("auth.connect"))
	if result.Value != nil || result.Err == nil || result.Err.Code != "core_error" {
		t.Fatalf("commitReady panic result = %+v", result)
	}
	if snapshot := controller.Snapshot(); snapshot.Authenticated || snapshot.Personality != PersonalityUnset {
		t.Fatalf("staged snapshot remained published: %+v", snapshot)
	}
	if controller.graph != nil || controller.personality != PersonalityUnset || controller.selecting {
		t.Fatalf("staged graph remained published: graph=%v personality=%q selecting=%v", controller.graph != nil, controller.personality, controller.selecting)
	}
	if got := fmt.Sprint(connectivity.snapshot()); got != "[start:connectivity stop:connectivity]" {
		t.Fatalf("commitReady panic rollback = %s", got)
	}
	_ = controller.Shutdown(context.Background())
}

func TestExactBoundIdentityIsRequired(t *testing.T) {
	for _, identity := range []AuthenticatedIdentity{
		{AgentID: "agent@example.test/mesh-one", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"},
		{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/wrong", MeshID: "mesh-one"},
		{AgentID: "other@example.test", BoundIdentity: "other@example.test/mesh-one", MeshID: "mesh-one"},
	} {
		connectivity := successfulConnectivity()
		connectivity.identity = identity
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "authentication_failed" {
			t.Fatalf("identity %+v result = %+v", identity, result)
		}
		_ = controller.Shutdown(context.Background())
	}
}

func TestProviderTaxonomyPreservesCancelDeadlineOutageAndRejectWithoutText(t *testing.T) {
	tests := []struct {
		failure   *ProviderError
		wantCode  string
		retryable bool
	}{
		{&ProviderError{Code: ProviderCancelled}, "request_cancelled", false},
		{&ProviderError{Code: ProviderDeadline}, "connectivity_unavailable", true},
		{&ProviderError{Code: ProviderUnavailable}, "connectivity_unavailable", true},
		{&ProviderError{Code: ProviderAuthenticationRejected}, "authentication_failed", false},
	}
	for _, test := range tests {
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{failure: test.failure}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != test.wantCode || result.Err.Retryable != test.retryable {
			t.Fatalf("failure %d = %+v", test.failure.Code, result)
		}
		_ = controller.Shutdown(context.Background())
	}
}

func TestProviderDeadlineMapsToRPCTimeoutOnlyForMessageRequest(t *testing.T) {
	request := providerFailure(model.Command{ID: "request", Name: "message.request"}, "rpc", &ProviderError{Code: ProviderDeadline})
	if request.Err == nil || request.Err.Code != "rpc_timeout" || request.Err.Stage != "rpc" || request.Err.Retryable || !errors.Is(request.Err.Cause, context.DeadlineExceeded) {
		t.Fatalf("message.request deadline = %+v", request.Err)
	}

	other := providerFailure(model.Command{ID: "mesh", Name: "mesh.list"}, "rpc", &ProviderError{Code: ProviderDeadline})
	if other.Err == nil || other.Err.Code != "connectivity_unavailable" || other.Err.Stage != "rpc" || !other.Err.Retryable || !errors.Is(other.Err.Cause, context.DeadlineExceeded) {
		t.Fatalf("non-request rpc deadline = %+v", other.Err)
	}
}

func TestLocalPayloadTaxonomyPreservesClosedCategories(t *testing.T) {
	tests := []struct {
		provider  ProviderErrorCode
		code      string
		retryable bool
		cause     error
	}{
		{ProviderInvalidHandle, "invalid_handle", false, ErrInvalidHandle},
		{ProviderPayloadTooLarge, "payload_too_large", false, ErrPayloadTooLarge},
		{ProviderPayloadIntegrity, "payload_integrity_failed", false, ErrPayloadIntegrity},
		{ProviderPayloadTransfer, "payload_transfer_failed", true, ErrPayloadTransfer},
	}
	for _, test := range tests {
		operation := func(context.Context, coreruntime.Services, model.EmptyArgs) (model.PayloadHandleResult, *ProviderError) {
			return model.PayloadHandleResult{}, &ProviderError{Code: test.provider}
		}
		factory := newTestFactory(t, Dependencies{Local: LocalOperations{PayloadOpen: operation}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["payload.open"](context.Background(), &testServices{}, model.Command{ID: "payload", Name: "payload.open", Args: model.EmptyArgs{}})
		if result.Value != nil || result.Err == nil || result.Err.Code != test.code || result.Err.Stage != "payload" || result.Err.Retryable != test.retryable || !errors.Is(result.Err.Cause, test.cause) {
			t.Fatalf("provider %d result = %+v", test.provider, result)
		}
		_ = controller.Shutdown(context.Background())
	}
}

func TestInvalidHandlePreservesOperationStage(t *testing.T) {
	local := LocalOperations{
		CommandCancel: func(context.Context, coreruntime.Services, model.CommandCancelArgs) (model.EmptyResult, *ProviderError) {
			return model.EmptyResult{}, &ProviderError{Code: ProviderInvalidHandle}
		},
	}
	factory := newTestFactory(t, Dependencies{Local: local})
	controller, dependencies, _ := factory.Build()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := dependencies.Handlers["command.cancel"](context.Background(), &testServices{}, model.Command{
		ID: "cancel", Name: "command.cancel", Args: model.CommandCancelArgs{CommandHandle: "cmdh_invalid"},
	})
	if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != "invalid_handle" || result.Err.Stage != "command" || !errors.Is(result.Err.Cause, ErrInvalidHandle) {
		t.Fatalf("command invalid handle = (%+v, %v)", result, err)
	}
	_ = controller.Shutdown(context.Background())
}

func TestClosedTypedOperationsCannotReturnWrongResultOrInstallPreAuthRemote(t *testing.T) {
	// The following assignments compile only for their exact result variants.
	local := LocalOperations{CoreStatus: statusOperation}
	remote := AuthenticatedOperations{MessageSend: sendOperation}
	if local.CoreStatus == nil || remote.MessageSend == nil {
		t.Fatal("typed operations missing")
	}
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: successfulConnectivity()}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["message.send"](context.Background(), &testServices{}, model.Command{ID: "send", Name: "message.send", Args: model.MessageSendArgs{}})
	if result.Err == nil || result.Value != nil {
		t.Fatalf("preauth remote = %+v", result)
	}
	_ = controller.Shutdown(context.Background())
}

func TestRollbackIsReverseAndPlaintextOffloadDoesNotRequireIdentityKeys(t *testing.T) {
	recorder := &testRecorder{}
	connectivity := successfulConnectivity()
	connectivity.recorder = recorder
	object := &testService{name: "objects", recorder: recorder, startFail: &ProviderError{Code: ProviderUnavailable}}
	objects := &testObjectFactory{service: object}
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, IdentityKeys: testKeys{}, Objects: objects})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err == nil || controller.Snapshot().Personality != PersonalityUnset {
		t.Fatalf("result=%+v snapshot=%+v", result, controller.Snapshot())
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity start:objects stop:connectivity]" {
		t.Fatalf("rollback = %s", got)
	}
	_ = controller.Shutdown(context.Background())

	objects = &testObjectFactory{service: &testService{name: "unused"}}
	factory = newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: successfulConnectivity()}, Objects: objects})
	controller, dependencies, _ = factory.Build()
	_ = controller.Start(context.Background())
	result, _ = dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err != nil || objects.calls != 1 || !controller.Snapshot().OffloadAvailable {
		t.Fatalf("plaintext offload result=%+v creates=%d snapshot=%+v", result, objects.calls, controller.Snapshot())
	}
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticatedServiceFactoryBuildsAfterIdentityStartsBeforeCommitAndMergesAtomically(t *testing.T) {
	recorder := &testRecorder{}
	connectivity := successfulConnectivity()
	connectivity.recorder = recorder
	connectivity.remote.Logout = func(context.Context, coreruntime.Services, model.EmptyArgs) (model.EmptyResult, *ProviderError) {
		return model.EmptyResult{}, nil
	}
	authenticated := &testService{name: "authenticated", recorder: recorder}
	authenticatedFactory := &testAuthenticatedFactory{
		service:    authenticated,
		operations: AuthenticatedOperations{MessageSend: sendOperation},
		create: func(_ context.Context, build AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError) {
			recorder.add("create:authenticated")
			if fmt.Sprint(connectivity.snapshot()) != "[start:connectivity]" {
				t.Fatal("authenticated factory ran before connectivity Start")
			}
			return authenticated, AuthenticatedOperations{MessageSend: sendOperation}, nil
		},
	}
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: authenticatedFactory})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{onCommit: func() { recorder.add("commit") }}
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("authentication = %+v", result)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity create:authenticated start:authenticated commit]" {
		t.Fatalf("build/commit order = %s", got)
	}
	authenticatedFactory.mu.Lock()
	build := authenticatedFactory.captured
	authenticatedFactory.mu.Unlock()
	if build.Identity != connectivity.identity || build.Connectivity != connectivity || build.Clock == nil || build.Random == nil || build.Profile != controller.profile {
		t.Fatalf("authenticated build context = %+v", build)
	}
	sent, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "send", Name: "message.send", Args: model.MessageSendArgs{}})
	if sent.Err != nil || sent.Value.(model.SendResult).MessageID != "message" {
		t.Fatalf("merged operation = %+v", sent)
	}
	if err := controller.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity create:authenticated start:authenticated commit stop:authenticated stop:connectivity]" {
		t.Fatalf("shutdown order = %s", got)
	}
}

func TestAuthenticatedBuildContextCarriesApprovedKeyAndObjectCapabilities(t *testing.T) {
	connectivity := successfulConnectivity()
	keys := testKeys{}
	objectRuntime := testObjectRuntime{}
	objectService := &testObjectRuntimeService{testService: &testService{name: "objects"}, runtime: objectRuntime}
	objectFactory := &testObjectFactory{service: objectService}
	authenticatedFactory := &testAuthenticatedFactory{service: &testService{name: "authenticated"}}
	factory := newTestFactory(t, Dependencies{
		Connectivity: &testConnectivityFactory{service: connectivity}, IdentityKeys: keys,
		Objects: objectFactory, Authenticated: authenticatedFactory,
	})
	controller, dependencies, _ := factory.Build()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("authentication = %+v", result)
	}
	authenticatedFactory.mu.Lock()
	build := authenticatedFactory.captured
	authenticatedFactory.mu.Unlock()
	objectFactory.mu.Lock()
	objectBuild := objectFactory.captured
	objectFactory.mu.Unlock()
	if build.IdentityKeys == nil || build.Objects != objectRuntime {
		t.Fatalf("authenticated capabilities = keys:%v objects:%T", build.IdentityKeys != nil, build.Objects)
	}
	if objectBuild.IdentityKeys == nil || objectBuild.Connectivity != connectivity {
		t.Fatalf("object build context = keys:%v connectivity:%T", objectBuild.IdentityKeys != nil, objectBuild.Connectivity)
	}
	if err := controller.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedServiceFactoryIsNotCalledBeforeExactIdentityValidation(t *testing.T) {
	connectivity := successfulConnectivity()
	connectivity.identity.BoundIdentity = "agent@example.test/wrong"
	authenticatedFactory := &testAuthenticatedFactory{service: &testService{name: "authenticated"}}
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: authenticatedFactory})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err == nil || result.Err.Code != "authentication_failed" {
		t.Fatalf("identity result = %+v", result)
	}
	authenticatedFactory.mu.Lock()
	calls := authenticatedFactory.calls
	authenticatedFactory.mu.Unlock()
	if calls != 0 || fmt.Sprint(connectivity.snapshot()) != "[start:connectivity stop:connectivity]" {
		t.Fatalf("factory calls=%d connectivity=%v", calls, connectivity.snapshot())
	}
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticatedOperationConflictRejectsWholeGraphAndRollsBackReverse(t *testing.T) {
	recorder := &testRecorder{}
	connectivity := successfulConnectivity()
	connectivity.recorder = recorder
	connectivity.remote.MessageSend = sendOperation
	authenticated := &testService{name: "authenticated", recorder: recorder}
	authenticatedFactory := &testAuthenticatedFactory{service: authenticated, operations: AuthenticatedOperations{MessageSend: sendOperation}}
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: authenticatedFactory})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{}
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if result.Err == nil || result.Err.Code != "core_error" || services.commits != 0 || controller.Snapshot().Authenticated {
		t.Fatalf("conflict result=%+v commits=%d snapshot=%+v", result, services.commits, controller.Snapshot())
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity start:authenticated stop:authenticated stop:connectivity]" {
		t.Fatalf("conflict rollback = %s", got)
	}
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticatedServiceCommitFailureNeverPublishesMergedOperations(t *testing.T) {
	recorder := &testRecorder{}
	connectivity := successfulConnectivity()
	connectivity.recorder = recorder
	authenticated := &testService{name: "authenticated", recorder: recorder}
	factory := newTestFactory(t, Dependencies{
		Connectivity:  &testConnectivityFactory{service: connectivity},
		Authenticated: &testAuthenticatedFactory{service: authenticated, operations: AuthenticatedOperations{MessageSend: sendOperation}},
	})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{commitFail: true}, authCommand("auth.connect"))
	if result.Err == nil || controller.Snapshot().Authenticated || controller.graph != nil {
		t.Fatalf("commit failure result=%+v snapshot=%+v graph=%v", result, controller.Snapshot(), controller.graph != nil)
	}
	if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity start:authenticated stop:authenticated stop:connectivity]" {
		t.Fatalf("commit rollback = %s", got)
	}
	_ = controller.Shutdown(context.Background())
}

func TestNilAuthenticatedFactoryLeavesMissingOperationsFailClosed(t *testing.T) {
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: successfulConnectivity()}})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	services := &testServices{}
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), services, authCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("authentication=%+v", result)
	}
	sent, _ := dependencies.Handlers["message.send"](context.Background(), services, model.Command{ID: "send", Name: "message.send", Args: model.MessageSendArgs{}})
	if sent.Value != nil || sent.Err == nil || sent.Err.Code != "connectivity_unavailable" {
		t.Fatalf("missing authenticated operation=%+v", sent)
	}
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticatedServiceShutdownSharesOneTotalCleanupBound(t *testing.T) {
	never := make(chan struct{})
	stopEntered := make(chan struct{})
	authenticated := &testService{name: "authenticated", stopBlock: never, stopEnter: stopEntered}
	factory := newTestFactory(t, Dependencies{
		Connectivity:  &testConnectivityFactory{service: successfulConnectivity()},
		Authenticated: &testAuthenticatedFactory{service: authenticated},
	})
	factory.profile.ReconnectOperationTimeout = 20 * time.Millisecond
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("authentication=%+v", result)
	}
	started := time.Now()
	err := controller.Shutdown(context.Background())
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown exceeded total bound: %v", elapsed)
	}
	if !errors.Is(err, ErrComponentStopFailed) {
		t.Fatalf("shutdown=%v", err)
	}
	select {
	case <-stopEntered:
	default:
		t.Fatal("authenticated service was excluded from cleanup")
	}
	close(never)
}

func TestAuthenticatedFactoryPanicStartPanicCancellationAndShutdownPanicAreContained(t *testing.T) {
	t.Run("factory panic", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: &testAuthenticatedFactory{panicCreate: true}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "core_error" || fmt.Sprint(recorder.snapshot()) != "[start:connectivity stop:connectivity]" {
			t.Fatalf("factory panic result=%+v trace=%v", result, recorder.snapshot())
		}
		_ = controller.Shutdown(context.Background())
	})

	t.Run("start panic", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		authenticated := &testService{name: "authenticated", recorder: recorder, startPanic: true}
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: &testAuthenticatedFactory{service: authenticated}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "core_error" {
			t.Fatalf("start panic result=%+v", result)
		}
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity start:authenticated stop:authenticated stop:connectivity]" {
			t.Fatalf("start panic rollback = %s", got)
		}
		_ = controller.Shutdown(context.Background())
	})

	t.Run("provider cancellation reverse rollback", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		authenticated := &testService{name: "authenticated", recorder: recorder}
		authenticatedFactory := &testAuthenticatedFactory{service: authenticated, failure: &ProviderError{Code: ProviderCancelled}}
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: authenticatedFactory})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "request_cancelled" {
			t.Fatalf("cancel result=%+v", result)
		}
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity stop:authenticated stop:connectivity]" {
			t.Fatalf("cancel rollback = %s", got)
		}
		_ = controller.Shutdown(context.Background())
	})

	t.Run("caller cancellation late result cleanup", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		release := make(chan struct{})
		entered := make(chan struct{})
		stopEntered := make(chan struct{})
		authenticated := &testService{name: "authenticated", recorder: recorder, stopEnter: stopEntered}
		authenticatedFactory := &testAuthenticatedFactory{create: func(context.Context, AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError) {
			close(entered)
			<-release
			return authenticated, AuthenticatedOperations{MessageSend: sendOperation}, nil
		}}
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: authenticatedFactory})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan model.Result, 1)
		go func() {
			result, _ := dependencies.Handlers["auth.connect"](ctx, &testServices{}, authCommand("auth.connect"))
			done <- result
		}()
		<-entered
		cancel()
		result := <-done
		if result.Err == nil || result.Err.Code != "request_cancelled" || fmt.Sprint(connectivity.snapshot()) != "[start:connectivity stop:connectivity]" {
			t.Fatalf("caller cancellation result=%+v connectivity=%v", result, connectivity.snapshot())
		}
		close(release)
		select {
		case <-stopEntered:
		case <-time.After(time.Second):
			t.Fatal("late authenticated service was not cleaned")
		}
		if got := fmt.Sprint(authenticated.snapshot()); got != "[stop:authenticated]" {
			t.Fatalf("late cleanup=%s", got)
		}
		_ = controller.Shutdown(context.Background())
	})

	t.Run("shutdown panic continues reverse cleanup", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		authenticated := &testService{name: "authenticated", recorder: recorder, stopPanic: true}
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{service: connectivity}, Authenticated: &testAuthenticatedFactory{service: authenticated}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err != nil {
			t.Fatalf("authentication=%+v", result)
		}
		if err := controller.Shutdown(context.Background()); !errors.Is(err, ErrComponentStopFailed) {
			t.Fatalf("shutdown=%v", err)
		}
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity start:authenticated stop:authenticated stop:connectivity]" {
			t.Fatalf("shutdown panic order = %s", got)
		}
	})
}

func TestProviderCreateDualOutcomeCleansUnpublishedServicesExactlyOnce(t *testing.T) {
	t.Run("connectivity", func(t *testing.T) {
		service := successfulConnectivity()
		service.stopFail = &ProviderError{Code: ProviderInternal}
		factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{
			service: service,
			failure: &ProviderError{Code: ProviderUnavailable},
		}})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "connectivity_unavailable" {
			t.Fatalf("dual connectivity result = %+v", result)
		}
		if got := fmt.Sprint(service.snapshot()); got != "[stop:connectivity]" {
			t.Fatalf("dual connectivity ownership = %s", got)
		}
		_ = controller.Shutdown(context.Background())
		if got := fmt.Sprint(service.snapshot()); got != "[stop:connectivity]" {
			t.Fatalf("dual connectivity cleaned again = %s", got)
		}
	})

	t.Run("objects", func(t *testing.T) {
		recorder := &testRecorder{}
		connectivity := successfulConnectivity()
		connectivity.recorder = recorder
		object := &testService{name: "objects", recorder: recorder, stopFail: &ProviderError{Code: ProviderInternal}}
		factory := newTestFactory(t, Dependencies{
			Connectivity: &testConnectivityFactory{service: connectivity},
			IdentityKeys: testKeys{},
			Objects: &testObjectFactory{
				service: object,
				failure: &ProviderError{Code: ProviderUnavailable},
			},
		})
		controller, dependencies, _ := factory.Build()
		_ = controller.Start(context.Background())
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		if result.Err == nil || result.Err.Code != "connectivity_unavailable" {
			t.Fatalf("dual object result = %+v", result)
		}
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity stop:objects stop:connectivity]" {
			t.Fatalf("dual object ownership = %s", got)
		}
		_ = controller.Shutdown(context.Background())
		if got := fmt.Sprint(recorder.snapshot()); got != "[start:connectivity stop:objects stop:connectivity]" {
			t.Fatalf("dual object cleaned again = %s", got)
		}
	})
}

func TestCancellationPublicationCollisionAssignsEveryServiceExactlyOnce(t *testing.T) {
	const iterations = 1000
	waitForOneStop := func(t *testing.T, service *testService, want string) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for {
			stops := 0
			for _, call := range service.snapshot() {
				if call == "stop:owned" {
					stops++
				}
			}
			if stops != 0 || !time.Now().Before(deadline) {
				break
			}
			time.Sleep(time.Microsecond)
		}
		if got := fmt.Sprint(service.snapshot()); got != want {
			t.Fatalf("service ownership trace = %s", got)
		}
	}

	t.Run("graph start", func(t *testing.T) {
		for iteration := 0; iteration < iterations; iteration++ {
			ctx, cancel := context.WithCancel(context.Background())
			service := &cancelReturningStartService{testService: testService{name: "owned"}, cancel: cancel}
			graph := &serviceGraph{}
			failure := graph.start(ctx, service)
			if failure == nil || failure.Code != ProviderCancelled || len(graph.services) != 0 {
				t.Fatalf("iteration %d: failure=%v services=%d", iteration, failure, len(graph.services))
			}
			waitForOneStop(t, &service.testService, "[start:owned stop:owned]")
		}
	})

	t.Run("connectivity create", func(t *testing.T) {
		for iteration := 0; iteration < iterations; iteration++ {
			ctx, cancel := context.WithCancel(context.Background())
			service := &testService{name: "owned"}
			got, failure := createConnectivity(cancelReturningConnectivityFactory{cancel: cancel, service: service}, ctx, BuildContext{})
			if got != nil || failure == nil || failure.Code != ProviderCancelled {
				t.Fatalf("iteration %d: service=%v failure=%v", iteration, got != nil, failure)
			}
			waitForOneStop(t, service, "[stop:owned]")
		}
	})

	t.Run("object create", func(t *testing.T) {
		for iteration := 0; iteration < iterations; iteration++ {
			ctx, cancel := context.WithCancel(context.Background())
			service := &testService{name: "owned"}
			got, failure := createObjectService(cancelReturningObjectFactory{cancel: cancel, service: service}, ctx, ObjectBuildContext{})
			if got != nil || failure == nil || failure.Code != ProviderCancelled {
				t.Fatalf("iteration %d: service=%v failure=%v", iteration, got != nil, failure)
			}
			waitForOneStop(t, service, "[stop:owned]")
		}
	})

	t.Run("authenticated create", func(t *testing.T) {
		for iteration := 0; iteration < iterations; iteration++ {
			ctx, cancel := context.WithCancel(context.Background())
			service := &testService{name: "owned"}
			got, operations, failure := createAuthenticatedService(cancelReturningAuthenticatedFactory{cancel: cancel, service: service}, ctx, AuthenticatedBuildContext{})
			if got != nil || operations.MessageSend != nil || failure == nil || failure.Code != ProviderCancelled {
				t.Fatalf("iteration %d: service=%v operations=%+v failure=%v", iteration, got != nil, operations, failure)
			}
			waitForOneStop(t, service, "[stop:owned]")
		}
	})
}

func TestDualOutcomeCleanupSharesAuthenticationTotalDeadline(t *testing.T) {
	never := make(chan struct{})
	entered := make(chan struct{})
	service := successfulConnectivity()
	service.stopBlock = never
	service.stopEnter = entered
	factory := newTestFactory(t, Dependencies{Connectivity: &testConnectivityFactory{
		service: service,
		failure: &ProviderError{Code: ProviderUnavailable},
	}})
	factory.profile.ReconnectOperationTimeout = 20 * time.Millisecond
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	started := time.Now()
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("dual outcome cleanup exceeded authentication bound: %v", elapsed)
	}
	if result.Err == nil || result.Err.Code != "connectivity_unavailable" || !result.Err.Retryable {
		t.Fatalf("dual outcome deadline result = %+v", result)
	}
	select {
	case <-entered:
	default:
		t.Fatal("unpublished service Shutdown was not invoked")
	}
	if got := fmt.Sprint(service.snapshot()); got != "[stop:connectivity]" {
		t.Fatalf("bounded cleanup calls = %s", got)
	}
	close(never)
	_ = controller.Shutdown(context.Background())
}

func TestAuthenticationOperationAndShutdownRemainBoundedForNoncooperativeProvider(t *testing.T) {
	never := make(chan struct{})
	entered := make(chan struct{})
	connectivity := &testConnectivityFactory{block: never, entered: entered, ignoreContext: true}
	factory := newTestFactory(t, Dependencies{Connectivity: connectivity})
	factory.profile.ReconnectOperationTimeout = 20 * time.Millisecond
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	resultDone := make(chan model.Result, 1)
	go func() {
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		resultDone <- result
	}()
	<-entered
	select {
	case result := <-resultDone:
		if result.Err == nil || result.Err.Code != "connectivity_unavailable" || !result.Err.Retryable {
			t.Fatalf("timeout result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("authentication exceeded operation bound")
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- controller.Shutdown(context.Background()) }()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for noncooperative factory")
	}
	close(never)
}

func TestShutdownTotalBoundIncludesAuthenticationJoin(t *testing.T) {
	never := make(chan struct{})
	entered := make(chan struct{})
	connectivity := &testConnectivityFactory{block: never, entered: entered, ignoreContext: true}
	factory := newTestFactory(t, Dependencies{Connectivity: connectivity})
	factory.profile.ReconnectOperationTimeout = 25 * time.Millisecond
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	go func() {
		_, _ = dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
	}()
	<-entered
	started := time.Now()
	err := controller.Shutdown(context.Background())
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown exceeded total bound: %v", elapsed)
	}
	if err != nil {
		t.Fatalf("shutdown error = %v", err)
	}
	close(never)
}

func TestConcurrentAuthenticationBuildsOnce(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	connectivity := &testConnectivityFactory{service: successfulConnectivity(), block: release, entered: entered}
	factory := newTestFactory(t, Dependencies{Connectivity: connectivity})
	controller, dependencies, _ := factory.Build()
	_ = controller.Start(context.Background())
	firstDone := make(chan model.Result, 1)
	go func() {
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), &testServices{}, authCommand("auth.connect"))
		firstDone <- result
	}()
	<-entered
	second, _ := dependencies.Handlers["auth.login"](context.Background(), &testServices{}, authCommand("auth.login"))
	if second.Err == nil {
		t.Fatalf("concurrent result = %+v", second)
	}
	close(release)
	if first := <-firstDone; first.Err != nil {
		t.Fatalf("first = %+v", first)
	}
	if connectivity.calls != 1 {
		t.Fatalf("factory calls = %d", connectivity.calls)
	}
	_ = controller.Shutdown(context.Background())
}

func newTestFactory(t *testing.T, dependencies Dependencies) *Factory {
	t.Helper()
	factory, err := NewFactory(DeploymentConfig{QueueLimit: 16}, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return factory
}

func successfulConnectivity() *testService {
	return &testService{name: "connectivity", identity: AuthenticatedIdentity{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"}}
}

func authCommand(name string) model.Command {
	return model.Command{ID: "auth-command", Name: name, Args: model.AuthArgs{MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test", Password: []byte("private password"), MeshID: "mesh-one", AgentInstanceID: "instance-one"}}
}

func statusOperation(context.Context, coreruntime.Services, model.EmptyArgs) (model.StatusResult, *ProviderError) {
	return model.StatusResult{Status: model.Status{Lifecycle: model.LifecycleCreated}}, nil
}

func sendOperation(context.Context, coreruntime.Services, model.MessageSendArgs) (model.SendResult, *ProviderError) {
	return model.SendResult{MessageID: "message", ConversationID: "conversation", Accepted: true}, nil
}
