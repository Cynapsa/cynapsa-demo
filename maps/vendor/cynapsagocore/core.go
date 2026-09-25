package cynapsagocore

import (
	"context"
	"sync"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

// Core is the public, transport-opaque facade for one AZTM runtime.
type Core struct {
	boundary        *sdkboundary.Adapter
	gate            *commandgate.Gate
	runtime         *coreruntime.Runtime
	payloads        *payload.HandleStore
	handlers        *mesh.HandlerRegistry
	policies        *mesh.PolicyController
	payloadFactory  *payload.PipelineFactory
	payloadPipeline *payload.Pipeline
	session         *sessionkernel.SessionController
	diagnosticsFeed *messagingRuntimeBridge

	mu          sync.Mutex
	destroyed   bool
	destroying  bool
	closing     bool
	destroyDone chan struct{}
	active      sync.WaitGroup

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

// New validates public configuration and creates an isolated core runtime.
// It performs no authentication or network activity.
func New(config Config) (*Core, error) {
	boundary, err := sdkboundary.New()
	if err != nil {
		return nil, normalizedCreationError(boundary, err)
	}
	return newCore(config, boundary)
}

func newCore(config Config, boundary *sdkboundary.Adapter) (*Core, error) {
	return newCoreWithConnectivityEnrollmentAndState(config, boundary, newBuiltInConnectivityFactory(nil), enrollment.NewProductionClient(), enrollment.NewProductionStateStore())
}

func newCoreWithConnectivity(config Config, boundary *sdkboundary.Adapter, connectivity sessionkernel.ConnectivityFactory) (*Core, error) {
	return newCoreWithConnectivityEnrollmentAndState(config, boundary, connectivity, enrollment.NewProductionClient(), enrollment.NewMemoryStateStore())
}

func newCoreWithConnectivityAndEnrollment(config Config, boundary *sdkboundary.Adapter, connectivity sessionkernel.ConnectivityFactory, enrollmentProvider enrollment.Provider) (*Core, error) {
	return newCoreWithConnectivityEnrollmentAndState(config, boundary, connectivity, enrollmentProvider, enrollment.NewMemoryStateStore())
}

func newCoreWithConnectivityEnrollmentAndState(config Config, boundary *sdkboundary.Adapter, connectivity sessionkernel.ConnectivityFactory, enrollmentProvider enrollment.Provider, enrollmentState enrollment.StateStore) (*Core, error) {
	internalConfig, err := boundary.MapConfig(config)
	if err != nil {
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	gate, err := commandgate.New(int(internalConfig.QueueLimit))
	if err != nil {
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}

	payloads, err := payload.NewHandleStore(payload.HandleLimits{
		MaximumHandles:    int(internalConfig.QueueLimit),
		MaximumBytes:      int64(internalConfig.PayloadLimit),
		MaximumPerHandle:  int64(internalConfig.PayloadLimit),
		MaximumWriteBytes: 1 << 20,
		MaximumReadBytes:  1 << 20,
	}, nil)
	if err != nil {
		return nil, boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	addresses, err := newAddressStore(int(internalConfig.QueueLimit))
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	handlers, err := mesh.NewHandlerRegistry(int(internalConfig.QueueLimit))
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	// Mesh membership is authorized by the server handshake. The local
	// application policy is permissive until an application replaces it.
	policies, err := mesh.NewPolicyController([]model.PolicyRule{{Action: "allow"}})
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	payloadFactory, payloadPipeline, err := newInlinePayloadPipeline(internalConfig, payloads)
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailurePayload)
	}

	local := &localServices{
		adapter: boundary, addresses: addresses, gate: gate, payloads: payloads,
		handlers: handlers, policies: policies, payloadPipeline: payloadPipeline,
	}
	runtimeBridge := &messagingRuntimeBridge{}
	local.diagnosticsFeed = runtimeBridge
	providerDependencies := sessionkernel.Dependencies{Local: local.operations(), Connectivity: connectivity, Enrollment: enrollmentProvider, EnrollmentState: enrollmentState}
	// Only the built-in connectivity graph exposes the authenticated route,
	// calibrated clock, and authoritative group-query capabilities required by
	// the production messaging service. Injected test/provider connectivity
	// remains usable without pretending to satisfy that private contract.
	if _, ok := connectivity.(*builtInConnectivityFactory); ok {
		providerDependencies.Authenticated = newAuthenticatedMessagingFactory(runtimeBridge, payloadFactory, payloads, policies, handlers)
		providerDependencies.Objects = newBuiltInObjectFactory()
		local.availability.largePayloads = true
	}
	factory, err := sessionkernel.NewFactory(sessionkernel.DeploymentConfig{QueueLimit: int(internalConfig.QueueLimit)}, providerDependencies)
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	session, runtimeDependencies, err := factory.Build()
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	local.session = session
	runtimeConfig, err := factory.RuntimeConfig(internalConfig)
	if err != nil {
		payloads.Close()
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	runtime, err := coreruntime.NewWithDependencies(runtimeConfig, gate, runtimeDependencies)
	if err != nil {
		if payloads != nil {
			payloads.Close()
		}
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	runtimeBridge.bind(runtime)
	local.runtime = runtime
	return &Core{
		boundary:        boundary,
		gate:            gate,
		runtime:         runtime,
		payloads:        payloads,
		handlers:        handlers,
		policies:        policies,
		payloadFactory:  payloadFactory,
		payloadPipeline: payloadPipeline,
		session:         session,
		diagnosticsFeed: runtimeBridge,
		destroyDone:     make(chan struct{}),
		shutdownDone:    make(chan struct{}),
	}, nil
}

func normalizedCreationError(boundary *sdkboundary.Adapter, err error) error {
	if boundary == nil {
		boundary, _ = sdkboundary.New()
	}
	return boundary.MapFailure(err, sdkboundary.FailureCommand)
}

// Start initializes bounded local runtime resources. Authentication remains a
// later command and therefore successful Start leaves lifecycle state created.
func (c *Core) Start(ctx context.Context) error {
	if err := c.beginOperation(sdkboundary.FailureCommand, false); err != nil {
		return err
	}
	defer c.end()
	if ctx == nil {
		return c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureCommand)
	}
	return publicFailure(c.boundary, c.runtime.Start(ctx), sdkboundary.FailureCommand)
}

// Submit admits a versioned application command without exposing internal
// command types. A local admission rejection is returned in Admission.Error;
// the Go error is reserved for malformed input or a failed projection.
func (c *Core) Submit(ctx context.Context, command v1.Command) (v1.Admission, error) {
	if err := c.beginOperation(sdkboundary.FailureCommand, false); err != nil {
		return v1.Admission{}, err
	}
	defer c.end()
	if ctx == nil {
		return v1.Admission{}, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureCommand)
	}
	internal, err := c.boundary.DecodeCommand(ctx, command)
	if err != nil {
		return v1.Admission{}, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	defer model.ClearCommand(&internal)
	admission, admissionErr := c.gate.SubmitOwned(ctx, &internal)
	public, err := c.boundary.MapAdmission(admission, admissionErr)
	if err != nil {
		return v1.Admission{}, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return public, nil
}

// Cancel cancels the local wait identified by an admission's opaque handle.
func (c *Core) Cancel(ctx context.Context, handle v1.CommandHandle) error {
	if err := c.beginOperation(sdkboundary.FailureCommand, false); err != nil {
		return err
	}
	defer c.end()
	if ctx == nil {
		return c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureCommand)
	}
	if err := ctx.Err(); err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	if err := c.boundary.ValidateCommandHandle(handle); err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return publicFailure(c.boundary, c.gate.Cancel(commandgate.CommandHandle(handle)), sdkboundary.FailureCommand)
}

// NextCompletion waits for the next normalized command completion.
func (c *Core) NextCompletion(ctx context.Context) (v1.Completion, error) {
	if err := c.beginOperation(sdkboundary.FailureDeliveryWait, true); err != nil {
		return v1.Completion{}, err
	}
	defer c.end()
	if ctx == nil {
		return v1.Completion{}, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureDeliveryWait)
	}
	result, err := c.gate.NextCompletion(ctx)
	if err != nil {
		return v1.Completion{}, c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait)
	}
	completion, err := c.boundary.MapCompletion(result)
	if err != nil {
		return v1.Completion{}, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return completion, nil
}

// NextEvent waits for the next normalized public event.
func (c *Core) NextEvent(ctx context.Context) (v1.Event, error) {
	if err := c.beginOperation(sdkboundary.FailureDeliveryWait, true); err != nil {
		return v1.Event{}, err
	}
	defer c.end()
	if ctx == nil {
		return v1.Event{}, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureDeliveryWait)
	}
	event, err := c.runtime.NextEvent(ctx)
	if err != nil {
		return v1.Event{}, c.boundary.MapFailure(err, sdkboundary.FailureDeliveryWait)
	}
	public, err := c.boundary.MapEvent(event)
	if err != nil {
		return v1.Event{}, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return public, nil
}

// Status returns normalized application-level runtime state.
func (c *Core) Status(ctx context.Context) (Status, error) {
	if err := c.beginOperation(sdkboundary.FailureCommand, true); err != nil {
		return Status{}, err
	}
	defer c.end()
	if ctx == nil {
		return Status{}, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureCommand)
	}
	if err := ctx.Err(); err != nil {
		return Status{}, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	contribution := c.diagnosticsFeed.diagnosticSnapshot()
	return c.boundary.MapStatus(mergeSessionStatus(c.runtime.Status(), c.session, contribution.queuedMessages)), nil
}

// Shutdown stops admission immediately and starts one Core-owned cleanup. A
// caller deadline limits only that caller's wait; cleanup continues and later
// callers join the same shutdown.
func (c *Core) Shutdown(ctx context.Context) error {
	if c == nil {
		return invalidCoreError()
	}
	if ctx == nil {
		return c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureShutdown)
	}
	if err := c.beginShutdown(); err != nil {
		return err
	}
	done := c.shutdownDone
	select {
	case <-done:
		c.mu.Lock()
		err := c.shutdownErr
		c.mu.Unlock()
		return publicFailure(c.boundary, err, sdkboundary.FailureShutdown)
	case <-ctx.Done():
		return c.boundary.MapFailure(ctx.Err(), sdkboundary.FailureShutdown)
	}
}

func (c *Core) beginShutdown() error {
	if c == nil {
		return invalidCoreError()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed || c.destroying {
		return c.boundary.MapFailure(sdkboundary.ErrInvalidHandle, sdkboundary.FailureShutdown)
	}
	c.shutdownOnce.Do(func() {
		c.closing = true
		c.runtime.BeginShutdown()
		go func() {
			err := c.runtime.Shutdown(context.Background())
			c.mu.Lock()
			c.shutdownErr = err
			close(c.shutdownDone)
			c.mu.Unlock()
		}()
	})
	return nil
}

// Destroy releases local resources after the runtime has reached closed.
// Repeated destruction of the same Go Core object is idempotent.
func (c *Core) Destroy() error {
	if c == nil {
		return invalidCoreError()
	}
	for {
		c.mu.Lock()
		if c.destroyed {
			c.mu.Unlock()
			return nil
		}
		if c.destroying {
			done := c.destroyDone
			c.mu.Unlock()
			<-done
			continue
		}
		if c.runtime.Status().Lifecycle != model.LifecycleClosed {
			c.mu.Unlock()
			return c.boundary.MapFailure(coreruntime.ErrClosing, sdkboundary.FailureShutdown)
		}
		c.destroying = true
		c.mu.Unlock()
		break
	}

	c.active.Wait()
	if c.payloads != nil {
		c.payloads.Close()
	}
	c.mu.Lock()
	c.destroyed = true
	c.destroying = false
	close(c.destroyDone)
	c.mu.Unlock()
	return nil
}

func (c *Core) beginOperation(operation sdkboundary.FailureOperation, allowClosing bool) error {
	if c == nil {
		return invalidCoreError()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed || c.destroying {
		return c.boundary.MapFailure(sdkboundary.ErrInvalidHandle, operation)
	}
	// A successful terminal command closes Gate admission before Runtime begins
	// asynchronous shutdown. Observe that same cutoff here so direct facade
	// operations cannot enter during the narrow completion-to-runtime handoff.
	gateClosing := c.gate != nil && c.gate.Stats().Closing
	if !allowClosing && (c.closing || gateClosing) {
		return c.boundary.MapFailure(coreruntime.ErrClosing, operation)
	}
	c.active.Add(1)
	return nil
}

func (c *Core) end() { c.active.Done() }

func publicFailure(adapter *sdkboundary.Adapter, err error, operation sdkboundary.FailureOperation) error {
	if err == nil {
		return nil
	}
	return adapter.MapFailure(err, operation)
}

func invalidCoreError() error {
	adapter, _ := sdkboundary.New()
	return adapter.MapFailure(sdkboundary.ErrInvalidHandle, sdkboundary.FailureCommand)
}

var _ error = (*v1.Error)(nil)
