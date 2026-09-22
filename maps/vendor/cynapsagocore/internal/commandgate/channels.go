package commandgate

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// CompletionLease keeps the exact queue head and its terminal registry entry
// owned by Gate until the boundary either commits or rolls back delivery.
// The lease deliberately carries no public identifier or mutable result.
type CompletionLease struct {
	gate   *Gate
	result model.Result
}

// ReserveCompletion waits for the next typed internal completion without
// transferring final ownership. CommitCompletion consumes the terminal
// registry entry; RollbackCompletion restores the exact head ahead of every
// later completion.
func (g *Gate) ReserveCompletion(ctx context.Context) (model.Result, *CompletionLease, error) {
	if ctx == nil {
		return model.Result{}, nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.Result{}, nil, err
	}

	if err := g.acquireCompletionConsumer(ctx); err != nil {
		return model.Result{}, nil, err
	}
	result, err := g.nextCompletionLocked(ctx)
	if err != nil {
		g.releaseCompletionConsumer()
		return model.Result{}, nil, err
	}
	return result, &CompletionLease{gate: g, result: result}, nil
}

// CommitCompletion transfers one reserved completion to its boundary owner.
func (g *Gate) CommitCompletion(lease *CompletionLease) error {
	if lease == nil || lease.gate != g {
		return ErrInvalidRegistryState
	}
	if !g.registry.consumeTerminal(lease.result.CommandID) {
		lease.gate = nil
		lease.result = model.Result{}
		g.completionDepth.Add(-1)
		g.releaseCompletionConsumer()
		return ErrInvalidRegistryState
	}
	lease.gate = nil
	lease.result = model.Result{}
	g.completionDepth.Add(-1)
	g.releaseCompletionConsumer()
	g.finalizeIfDrained()
	return nil
}

// RollbackCompletion restores one reserved completion at the authoritative
// head without consuming its terminal registry record.
func (g *Gate) RollbackCompletion(lease *CompletionLease) error {
	if lease == nil || lease.gate != g || g.completionFront != nil {
		return ErrInvalidRegistryState
	}
	result := lease.result
	g.completionFront = &result
	lease.gate = nil
	lease.result = model.Result{}
	g.releaseCompletionConsumer()
	return nil
}

func (g *Gate) acquireCompletionConsumer(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-g.closedCh:
		return ErrGateClosed
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.closedCh:
		return ErrGateClosed
	case <-g.completionConsumer:
		return nil
	}
}

func (g *Gate) releaseCompletionConsumer() {
	g.completionConsumer <- struct{}{}
}

// NextCompletion waits for the next typed internal completion and transfers
// exclusive ownership of it to the caller. Completion polling and clearing are
// serialized so dequeue and terminal-registry cleanup are one owned operation.
func (g *Gate) NextCompletion(ctx context.Context) (model.Result, error) {
	result, lease, err := g.ReserveCompletion(ctx)
	if err != nil {
		return model.Result{}, err
	}
	if err = g.CommitCompletion(lease); err != nil {
		return model.Result{}, err
	}
	return result, nil
}

func (g *Gate) nextCompletionLocked(ctx context.Context) (model.Result, error) {
	if err := ctx.Err(); err != nil {
		return model.Result{}, err
	}
	select {
	case <-g.closedCh:
		return model.Result{}, ErrGateClosed
	default:
	}
	if g.completionFront != nil {
		result := *g.completionFront
		g.completionFront = nil
		return result, nil
	}
	select {
	case result := <-g.completions:
		return result, nil
	default:
	}

	select {
	case <-ctx.Done():
		return model.Result{}, ctx.Err()
	case <-g.closedCh:
		return model.Result{}, ErrGateClosed
	case result := <-g.completions:
		return result, nil
	case <-g.shutdownCh:
		// Shutdown closes shutdownCh only after publishing every terminal
		// result it owns. Prefer already-published completions over the state
		// signal so polling can drain deterministically.
		select {
		case result := <-g.completions:
			return result, nil
		default:
		}
		return model.Result{}, ErrGateClosing
	}
}

// ClearCompletions drains completion delivery during shutdown. The completion
// channel itself is never closed; gate state owns termination signalling.
// It must be called only after shutdown has started. It shares the single
// completion-consumer ownership lock with NextCompletion.
func (g *Gate) ClearCompletions(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.isClosing() {
		return ErrGateNotClosing
	}

	if err := g.acquireCompletionConsumer(ctx); err != nil {
		return err
	}
	for {
		if g.completionFront != nil {
			result := *g.completionFront
			g.completionFront = nil
			if !g.registry.consumeTerminal(result.CommandID) {
				g.releaseCompletionConsumer()
				return ErrInvalidRegistryState
			}
			g.completionDepth.Add(-1)
			continue
		}
		select {
		case <-ctx.Done():
			g.releaseCompletionConsumer()
			return ctx.Err()
		case result := <-g.completions:
			if !g.registry.consumeTerminal(result.CommandID) {
				g.releaseCompletionConsumer()
				return ErrInvalidRegistryState
			}
			g.completionDepth.Add(-1)
		default:
			live := g.registry.Len()
			g.releaseCompletionConsumer()
			if live == 0 {
				g.finalizeIfDrained()
				return nil
			}
			// During shutdown every live entry is terminal and owns exactly one
			// queued completion, so an empty queue with live entries is an
			// invariant violation rather than a reason to wait forever.
			return ErrCompletionQueueFull
		}
	}
}

// Shutdown stops admission, terminalizes every nonterminal command, and waits
// for completion consumers to drain. At ctx expiry it returns on the caller's
// deadline while one gate-owned finalizer continues cleanup. A retained
// completion lease delays abandonment until the lease is committed or rolled
// back; admission and new dispatch remain closed throughout. A command still
// borrowed by a non-cooperative dispatcher remains bounded, quarantined, and
// byte-charged until that dispatcher returns; clearing its backing earlier
// would race the live borrowed view.
func (g *Gate) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	g.BeginShutdown()
	if err := g.registry.waitEmpty(ctx, g.closedCh); err != nil {
		g.ensureFinalizer(false)
		return err
	}
	g.ensureFinalizer(true)
	select {
	case <-g.finalizeDone:
		return nil
	default:
	}
	select {
	case <-g.finalizeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BeginShutdown idempotently stops admission and terminalizes all pending
// commands without waiting for completion consumers. Shutdown calls this and
// then waits for drain or deadline cleanup.
func (g *Gate) BeginShutdown() {
	g.beginShutdown()
}

func (g *Gate) beginShutdown() {
	g.mu.Lock()
	g.transitionMu.Lock()
	g.beginShutdownTransitionLocked()
	g.transitionMu.Unlock()
	g.mu.Unlock()
}

// beginShutdownTransitionLocked requires mu and transitionMu. It publishes
// every shutdown completion after any terminal completion already published by
// its caller.
func (g *Gate) beginShutdownTransitionLocked() {
	if g.state != gateOpen {
		return
	}
	g.state = gateClosing
	results := g.registry.terminalizeAll(ErrGateClosing)
	// Claim every command's terminal transition before publishing lifetime
	// cancellation. A running handler may wake and return immediately when its
	// context is cancelled; it must not race ahead with a handler-failure result.
	g.rootCancel(ErrGateClosing)
	for _, result := range results {
		g.registry.takeDisposition(result.CommandID).resolveResult(result, false)
		g.publishCompletion(result)
	}
	close(g.shutdownCh)
}

func (g *Gate) finalizeIfDrained() {
	if !g.isClosing() || g.registry.Len() != 0 {
		return
	}
	g.ensureFinalizer(true)
}

func (g *Gate) ensureFinalizer(preferSynchronous bool) {
	g.finalizeMu.Lock()
	if g.finalizeStarted {
		g.finalizeMu.Unlock()
		return
	}
	if preferSynchronous {
		select {
		case <-g.completionConsumer:
			g.finalizeStarted = true
			g.finalizeMu.Unlock()
			g.finalizeClosedOwned()
			return
		default:
		}
	}
	g.finalizeStarted = true
	g.finalizeMu.Unlock()
	go func() {
		<-g.completionConsumer
		g.finalizeClosedOwned()
	}()
}

// finalizeClosedOwned requires exclusive completion-consumer ownership. It
// never waits for a borrower while holding Gate locks, so diagnostics and
// repeated deadline-bounded Shutdown calls remain responsive.
func (g *Gate) finalizeClosedOwned() {
	g.mu.Lock()
	g.state = gateClosed
	g.transitionMu.Lock()
	g.registry.reset()
	drainCommands(g.commands)
	drainCompletions(g.completions)
	g.completionFront = nil
	g.completionDepth.Store(0)
	close(g.closedCh)
	g.releaseCompletionConsumer()
	g.transitionMu.Unlock()
	g.mu.Unlock()
	close(g.finalizeDone)
}

func drainCommands(queue chan *ownedCommand) {
	for {
		select {
		case owner := <-queue:
			owner.terminalize()
		default:
			return
		}
	}
}

func drainCompletions(queue chan model.Result) {
	for {
		select {
		case <-queue:
		default:
			return
		}
	}
}

func shutdownResult(err error) bool {
	return errors.Is(err, ErrGateClosing) || errors.Is(err, ErrGateClosed)
}
