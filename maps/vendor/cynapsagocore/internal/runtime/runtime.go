// Package runtime coordinates the private AZTM session runtime.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const (
	maxQueueLimit   = 65_536
	maxPayloadLimit = v1.MaximumPayloadBytes
)

type startPhase uint8

const (
	startPending startPhase = iota
	startRunning
	startComplete
	startFailed
)

// Runtime owns private session state, workers, and internal event streams.
type Runtime struct {
	mu         sync.RWMutex
	controlMu  sync.Mutex
	eventMu    sync.Mutex
	config     Config
	gate       *commandgate.Gate
	events     *delivery.Dispatcher
	clock      coreclock.Clock
	metrics    *diagnostics.Metrics
	handlers   map[string]Handler
	components []lifecycleComponent
	started    []lifecycleComponent
	state      model.LifecycleState
	status     model.Status

	rootCtx    context.Context
	rootCancel context.CancelCauseFunc

	startPhase startPhase
	startErr   error
	startDone  chan struct{}

	workerStarted bool
	workerDone    chan struct{}

	// componentStartInFlight is set under mu at the linearization point for one
	// Component.Start invocation. BeginShutdown uses the same lock to reject all
	// later admissions while allowing this already-owned invocation to finish
	// under startup rollback ownership.
	componentStartInFlight bool

	shutdownStarted bool
	lifecycleMu     sync.Mutex
	shutdownOnce    sync.Once
	shutdownInitErr error
	shutdownErr     error
	shutdownDone    chan struct{}

	eventSequence atomic.Uint64
	controlNext   uint64

	completionChannel   *localRegistration
	eventSink           *localRegistration
	boundEventSink      string
	diagnosticLogs      bool
	deliveryPaused      bool
	outstandingDelivery string
	outstandingInbound  delivery.AdmissionID
	deliveryLeases      map[string]*delivery.EventLease
	inboundDeliveries   map[delivery.AdmissionID]chan error
	deliveryChanged     chan struct{}
}

type localRegistration struct {
	id       string
	capacity uint32
}

// New constructs the internal runtime with no semantic handlers or lifecycle
// components. NewWithDependencies supplies the private integration seams.
func New(config Config, gate *commandgate.Gate) (*Runtime, error) {
	return NewWithDependencies(config, gate, Dependencies{})
}

// NewWithDependencies constructs the complete ownership graph without
// starting a goroutine or invoking a component. Success transfers exclusive
// lifecycle ownership of every Component instance to Runtime; callers must not
// concurrently mutate or invoke those instances afterward.
func NewWithDependencies(config Config, gate *commandgate.Gate, dependencies Dependencies) (*Runtime, error) {
	if gate == nil {
		return nil, ErrNilGate
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	gateStats := gate.Stats()
	if gateStats.Capacity != int(config.QueueLimit) || gateStats.Closing {
		return nil, ErrInvalidConfig
	}
	handlers, components, processClock, err := validateDependencies(dependencies)
	if err != nil {
		return nil, err
	}
	eventByteCapacity, err := delivery.DeriveOrdinaryByteCapacity(config.QueueLimit, config.PayloadLimit)
	if err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	events, err := delivery.NewWithByteCapacity(int(config.QueueLimit), eventByteCapacity)
	if err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	rootCtx, rootCancel := context.WithCancelCause(context.Background())
	config.Connectivity.BootstrapData = append([]byte(nil), config.Connectivity.BootstrapData...)
	return &Runtime{
		config:          config,
		gate:            gate,
		events:          events,
		clock:           processClock,
		metrics:         diagnostics.NewMetrics(),
		handlers:        handlers,
		components:      components,
		state:           stateCreated,
		status:          model.Status{Lifecycle: stateCreated},
		rootCtx:         rootCtx,
		rootCancel:      rootCancel,
		startDone:       make(chan struct{}),
		workerDone:      make(chan struct{}),
		shutdownDone:    make(chan struct{}),
		deliveryChanged: make(chan struct{}),
	}, nil
}

// Start starts dependency components and the one owned command worker. It is
// idempotent and remains in lifecycle created; authentication handlers drive
// later session transitions.
func (r *Runtime) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	if r.state == stateClosing {
		r.mu.Unlock()
		return ErrClosing
	}
	if r.state == stateClosed {
		r.mu.Unlock()
		return ErrClosed
	}
	if r.state == stateFailed && r.startPhase == startComplete {
		r.mu.Unlock()
		return ErrInvalidTransition
	}
	switch r.startPhase {
	case startComplete:
		r.mu.Unlock()
		return nil
	case startFailed:
		err := r.startErr
		r.mu.Unlock()
		return err
	case startRunning:
		done := r.startDone
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			r.mu.RLock()
			err := r.startErr
			r.mu.RUnlock()
			return err
		}
	}
	if r.state != stateCreated {
		r.mu.Unlock()
		return ErrInvalidTransition
	}
	r.startPhase = startRunning
	r.mu.Unlock()

	err := r.startup(ctx)
	r.mu.Lock()
	if err != nil {
		r.startPhase = startFailed
		r.startErr = errors.Join(ErrStartupFailed, err)
	} else {
		r.startPhase = startComplete
	}
	close(r.startDone)
	result := r.startErr
	closing := r.state == stateClosing || r.state == stateClosed
	r.mu.Unlock()
	if err != nil && !closing {
		_ = r.transition(stateFailed)
	}
	return result
}

// Status returns a race-safe private state snapshot for boundary normalization.
func (r *Runtime) Status() model.Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

// Diagnostics returns bounded private resource accounting.
func (r *Runtime) Diagnostics() diagnostics.Snapshot {
	gateStats := r.gate.Stats()
	eventStats := r.events.Stats()
	r.mu.RLock()
	workers := 0
	if r.workerStarted {
		workers = 1
	}
	observation := diagnostics.Observation{
		CommandQueueDepth:      gateStats.CommandQueueDepth,
		CompletionQueueDepth:   gateStats.CompletionQueueDepth,
		CommandRegistryEntries: gateStats.RegistryEntries,
		EventQueueDepth:        eventStats.Depth,
		EventQueueCapacity:     eventStats.Capacity,
		WorkerCount:            workers,
	}
	r.mu.RUnlock()
	return diagnostics.Capture(observation, r.metrics)
}

func validateConfig(config Config) error {
	if config.QueueLimit == 0 || config.QueueLimit > maxQueueLimit || config.PayloadLimit > maxPayloadLimit || config.CommandTimeout < 0 || config.RPCTimeout < 0 || config.Connectivity.HealthTimeout < 0 || config.Connectivity.RecoveryDelay < 0 || config.CleanupTimeout <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

func validateDependencies(dependencies Dependencies) (map[string]Handler, []lifecycleComponent, coreclock.Clock, error) {
	handlers := make(map[string]Handler, len(dependencies.Handlers))
	for name, handler := range dependencies.Handlers {
		if !knownCommand(name) {
			return nil, nil, nil, fmt.Errorf("%w: %s", ErrUnknownHandler, name)
		}
		if handler == nil {
			return nil, nil, nil, ErrInvalidDependencies
		}
		handlers[name] = handler
	}
	components := make([]lifecycleComponent, 0, len(dependencies.Components))
	names := make(map[string]struct{}, len(dependencies.Components))
	for _, component := range dependencies.Components {
		if nilInterface(component) {
			return nil, nil, nil, ErrInvalidDependencies
		}
		name, ok := componentName(component)
		if !ok || name == "" {
			return nil, nil, nil, ErrInvalidDependencies
		}
		if _, duplicate := names[name]; duplicate {
			return nil, nil, nil, ErrInvalidDependencies
		}
		names[name] = struct{}{}
		components = append(components, lifecycleComponent{name: name, component: component})
	}
	processClock := dependencies.Clock
	if nilInterface(processClock) {
		processClock = coreclock.Real{}
	}
	return handlers, components, processClock, nil
}

func componentName(component Component) (name string, ok bool) {
	defer func() {
		if recover() != nil {
			name = ""
			ok = false
		}
	}()
	return component.Name(), true
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
