package sessionkernel

import (
	"crypto/rand"
	"io"
	"sync"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// Factory creates isolated controllers and their runtime dependency tables. It
// performs no network I/O.
type Factory struct {
	queueLimit      int
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
}

// RuntimeConfig combines boundary-mapped settings with the exact trusted
// cleanup bound required by the private runtime.
func (factory *Factory) RuntimeConfig(config model.RuntimeConfig) (coreruntime.Config, error) {
	if factory == nil || config.QueueLimit != uint32(factory.queueLimit) {
		return coreruntime.Config{}, ErrInvalidConfig
	}
	return coreruntime.Config{RuntimeConfig: config, CleanupTimeout: defaultOperationTimeout}, nil
}

func NewFactory(deployment DeploymentConfig, dependencies Dependencies) (*Factory, error) {
	profile, err := DefaultOperationalProfile(deployment)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	processClock := dependencies.Clock
	if processClock == nil {
		processClock = coreclock.Real{}
	}
	random := dependencies.Random
	if random == nil {
		random = rand.Reader
	}
	return &Factory{
		queueLimit:      deployment.QueueLimit,
		profile:         profile,
		clock:           processClock,
		random:          &lockedReader{source: random},
		connect:         dependencies.Connectivity,
		enroll:          dependencies.Enrollment,
		enrollmentState: dependencies.EnrollmentState,
		authenticated:   dependencies.Authenticated,
		keys:            dependencies.IdentityKeys,
		objects:         dependencies.Objects,
		local:           dependencies.Local,
	}, nil
}

// Build returns the controller and exact exhaustive runtime table. Runtime
// Start starts only the controller's local ownership; authentication creates
// the private service graph later.
func (factory *Factory) Build() (*SessionController, coreruntime.Dependencies, error) {
	if factory == nil {
		return nil, coreruntime.Dependencies{}, ErrInvalidConfig
	}
	controller := &SessionController{
		profile:         factory.profile,
		clock:           factory.clock,
		random:          factory.random,
		connect:         factory.connect,
		enroll:          factory.enroll,
		enrollmentState: factory.enrollmentState,
		authenticated:   factory.authenticated,
		keys:            factory.keys,
		objects:         factory.objects,
		local:           factory.local,
		shutdownDone:    make(chan struct{}),
	}
	handlers, err := controller.handlers()
	if err != nil {
		return nil, coreruntime.Dependencies{}, err
	}
	return controller, coreruntime.Dependencies{
		Clock:      factory.clock,
		Handlers:   handlers,
		Components: []coreruntime.Component{controller},
	}, nil
}

type lockedReader struct {
	mu     sync.Mutex
	source io.Reader
}

func (reader *lockedReader) Read(target []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.source.Read(target)
}
