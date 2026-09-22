// Package commandgate coordinates typed internal command admission and completion.
package commandgate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Gate owns bounded internal command admission.
//
// Submit independently freezes caller memory. A successful call transfers the
// frozen snapshot to the gate while caller memory remains caller-owned. The
// gate keeps that snapshot immutable through terminal dispatcher release.
type Gate struct {
	mu                 sync.Mutex
	transitionMu       sync.Mutex
	completionConsumer chan struct{}
	completionFront    *model.Result
	completionDepth    atomic.Int32
	ownedBytes         atomic.Int64
	maximumOwnedBytes  int64
	state              gateState
	commands           chan *ownedCommand
	completions        chan model.Result
	registry           *Registry
	rootCtx            context.Context
	rootCancel         context.CancelCauseFunc
	shutdownCh         chan struct{}
	closedCh           chan struct{}
	finalizeMu         sync.Mutex
	finalizeStarted    bool
	finalizeDone       chan struct{}
}

type gateState uint8

const (
	gateOpen gateState = iota
	gateClosing
	gateClosed
)

// New will construct a gate with bounded queues and exactly-once completion tracking.
//
// capacity is shared by the command queue, live registry, completion queue,
// and bounded terminal-ID history. Keeping the completion capacity equal to
// the registry capacity reserves one terminal delivery slot for every admitted
// command, so cancellation and shutdown never lose an accepted completion.
func New(capacity int) (*Gate, error) {
	byteCapacity, ok := derivedByteCapacity(capacity)
	if !ok {
		return nil, ErrInvalidCapacity
	}
	return NewWithByteCapacity(capacity, byteCapacity)
}

// NewWithByteCapacity constructs a gate with an explicit aggregate command
// snapshot budget. The budget is private composition state and may never
// exceed the hard process ceiling.
func NewWithByteCapacity(capacity int, byteCapacity int64) (*Gate, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	if byteCapacity <= 0 || byteCapacity > MaximumGateBytes {
		return nil, ErrInvalidByteCapacity
	}

	rootCtx, rootCancel := context.WithCancelCause(context.Background())
	gate := &Gate{
		state:              gateOpen,
		commands:           make(chan *ownedCommand, capacity),
		completions:        make(chan model.Result, capacity),
		maximumOwnedBytes:  byteCapacity,
		completionConsumer: make(chan struct{}, 1),
		registry:           NewRegistry(capacity),
		rootCtx:            rootCtx,
		rootCancel:         rootCancel,
		shutdownCh:         make(chan struct{}),
		closedCh:           make(chan struct{}),
		finalizeDone:       make(chan struct{}),
	}
	gate.completionConsumer <- struct{}{}
	return gate, nil
}

// Submit will admit or reject one typed internal command locally.
// It performs no network or handler work. Admission is an atomic, nonblocking
// handoff: saturation is returned to the caller instead of waiting or creating
// an unbounded goroutine.
func (g *Gate) Submit(ctx context.Context, command model.Command) (Admission, error) {
	bytes, err := model.MeasureCommand(command)
	if err != nil {
		return rejected(command.ID, ReasonCapacityReached), ErrInvalidCommandOwnership
	}
	if bytes > MaximumCommandBytes {
		return rejected(command.ID, ReasonCapacityReached), ErrCommandBytesExceeded
	}
	frozen, _, err := model.FreezeCommand(command)
	if err != nil {
		return rejected(command.ID, ReasonCapacityReached), ErrInvalidCommandOwnership
	}
	defer model.ClearCommand(&frozen)
	return g.SubmitOwned(ctx, &frozen)
}

// SubmitOwned consumes one independently frozen command only after every
// rejection condition has passed. Rejection leaves command unchanged. This is
// the boundary-to-gate move seam; ordinary internal callers use Submit.
func (g *Gate) SubmitOwned(ctx context.Context, command *model.Command) (Admission, error) {
	if command == nil {
		return rejected("", ReasonCapacityReached), ErrInvalidCommandOwnership
	}
	commandID, identified := model.FrozenCommandID(*command)
	if !identified {
		return rejected("", ReasonCapacityReached), ErrInvalidCommandOwnership
	}
	if ctx == nil {
		return rejected(commandID, ReasonContextCancelled), ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return rejected(commandID, ReasonContextCancelled), err
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return rejected(commandID, ReasonContextCancelled), err
	}
	if g.state != gateOpen {
		return rejected(commandID, ReasonShuttingDown), g.stateErrorLocked()
	}
	// Reinspect at the ownership-transfer point. This is deliberately inside
	// the gate serialization window so no queue/registry/byte reservation can
	// precede validation of the current facade against its frozen token.
	bytes, frozen := model.ValidateFrozenCommand(*command)
	if !frozen || bytes <= 0 || bytes > MaximumCommandBytes {
		return rejected(commandID, ReasonCapacityReached), ErrCommandBytesExceeded
	}
	handle, err := g.registry.register(commandID, g.rootCtx)
	if err != nil {
		return rejected(commandID, admissionReason(err)), err
	}
	if !reserveOwnedBytes(&g.ownedBytes, g.maximumOwnedBytes, bytes) {
		g.registry.removeAdmitted(commandID)
		return rejected(commandID, ReasonCapacityReached), ErrCommandBytesExceeded
	}
	ownedValue, ownedBytes, ok := model.TakeFrozenCommand(command)
	if !ok || ownedBytes != bytes {
		g.registry.removeAdmitted(commandID)
		g.ownedBytes.Add(-bytes)
		model.ClearCommand(&ownedValue)
		return rejected(commandID, ReasonCapacityReached), ErrInvalidCommandOwnership
	}
	owned := newOwnedCommand(g, ownedValue, bytes)
	if !g.registry.attachOwner(commandID, handle, owned) {
		g.registry.removeAdmitted(commandID)
		owned.terminalize()
		return rejected(commandID, ReasonCapacityReached), ErrInvalidCommandOwnership
	}

	select {
	case g.commands <- owned:
		return Admission{CommandID: commandID, CommandHandle: handle, Accepted: true}, nil
	default:
		// This cannot occur while the registry and command queue have the same
		// capacity, but retaining the rollback makes the invariant fail closed
		// if the implementation changes later.
		g.registry.removeAdmitted(commandID)
		return rejected(commandID, ReasonQueueFull), ErrCommandQueueFull
	}
}

// Next returns the next admitted command together with its gate-owned
// cancellation context and per-admission completion capability.
func (g *Gate) Next(ctx context.Context) (Dispatch, error) {
	return g.nextDispatch(ctx)
}

// Complete will publish exactly one internal result for an admitted command.
// A successful call transfers exclusive ownership of result and all memory
// reachable from Result.Value and Result.Err to the gate. A rejected call does
// not transfer ownership. The gate treats an accepted result as immutable and
// transfers it to the NextCompletion caller.
func (g *Gate) Complete(capability CompletionCapability, result model.Result) error {
	return g.complete(capability, result, false)
}

func (g *Gate) complete(capability CompletionCapability, result model.Result, beginShutdown bool) (returned error) {
	if result.CommandID == "" {
		return ErrEmptyCommandID
	}
	defer func() {
		if capability.token != nil && dispatchEnded(returned) {
			capability.token.releaseDispatch()
		}
	}()
	if beginShutdown {
		return g.completeAndBeginShutdown(capability, result)
	}
	g.transitionMu.Lock()
	err := g.registry.complete(capability, result)
	if err == nil {
		g.registry.takeDisposition(result.CommandID).resolveResult(result, true)
		g.publishCompletion(result)
	}
	g.transitionMu.Unlock()
	if err != nil {
		if errors.Is(err, ErrCommandNotFound) {
			g.mu.Lock()
			closed := g.state == gateClosed
			g.mu.Unlock()
			if closed {
				return ErrGateClosed
			}
		}
		return err
	}
	return nil
}

func dispatchEnded(err error) bool {
	return err == nil || errors.Is(err, ErrAlreadyTerminal) || errors.Is(err, ErrCommandNotFound) || errors.Is(err, ErrStaleCompletionCapability) || errors.Is(err, ErrGateClosed)
}

// completeAndBeginShutdown serializes the successful completion and the gate
// admission cutoff with Submit by holding mu across both operations. Lock
// order matches ordinary shutdown: mu precedes transitionMu.
func (g *Gate) completeAndBeginShutdown(capability CompletionCapability, result model.Result) error {
	g.mu.Lock()
	g.transitionMu.Lock()
	err := g.registry.complete(capability, result)
	if err == nil {
		g.registry.takeDisposition(result.CommandID).resolveResult(result, true)
		g.publishCompletion(result)
		g.beginShutdownTransitionLocked()
	}
	g.transitionMu.Unlock()
	closed := g.state == gateClosed
	g.mu.Unlock()
	if err != nil {
		if errors.Is(err, ErrCommandNotFound) && closed {
			return ErrGateClosed
		}
		return err
	}
	return nil
}

func (g *Gate) publishCompletion(result model.Result) {
	// Registry capacity reserves one slot for every admitted command, and all
	// terminal transition/publication pairs are serialized by transitionMu.
	// Therefore this send always has a slot and is provably bounded.
	g.completionDepth.Add(1)
	g.completions <- result
}

func (g *Gate) stateErrorLocked() error {
	if g.state == gateClosed {
		return ErrGateClosed
	}
	return ErrGateClosing
}

func (g *Gate) stateError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stateErrorLocked()
}

func (g *Gate) isClosing() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state != gateOpen
}

func (g *Gate) isClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state == gateClosed
}

func rejected(commandID, reason string) Admission {
	return Admission{CommandID: commandID, Accepted: false, Reason: reason}
}

func admissionReason(err error) string {
	switch {
	case errors.Is(err, ErrEmptyCommandID):
		return ReasonInvalidCommandID
	case errors.Is(err, ErrDuplicateCommandID), errors.Is(err, ErrAlreadyTerminal):
		return ReasonDuplicate
	case errors.Is(err, ErrRegistryFull):
		return ReasonCapacityReached
	case errors.Is(err, ErrCommandHandleGeneration), errors.Is(err, ErrCommandHandleExhausted):
		return ReasonHandleUnavailable
	default:
		return ReasonCapacityReached
	}
}
