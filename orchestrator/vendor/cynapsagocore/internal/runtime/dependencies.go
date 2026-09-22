package runtime

import (
	"context"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Services is the narrow runtime surface available to semantic handlers. It
// intentionally excludes Start, BeginShutdown, Shutdown, and command-gate
// ownership so a handler cannot wait on its own worker.
type Services interface {
	Transition(model.LifecycleState) error
	CommitAuthenticated(context.Context, AuthenticatedPublisher) error
	PublishEvent(context.Context, model.Event) error
	Status() model.Status
	Diagnostics() diagnostics.Snapshot
}

// Handler performs a prompt typed handoff for one allowlisted command. Long
// peer, RPC, connectivity, and payload work belongs to the injected subsystem,
// not the runtime's single dispatcher worker.
type Handler func(context.Context, Services, model.Command) (model.Result, error)

type handlerServices struct{ runtime *Runtime }

func (s handlerServices) Transition(next model.LifecycleState) error {
	return s.runtime.Transition(next)
}
func (s handlerServices) CommitAuthenticated(ctx context.Context, publisher AuthenticatedPublisher) error {
	return s.runtime.CommitAuthenticated(ctx, publisher)
}
func (s handlerServices) PublishEvent(ctx context.Context, event model.Event) error {
	return s.runtime.PublishEvent(ctx, event)
}
func (s handlerServices) Status() model.Status { return s.runtime.Status() }
func (s handlerServices) Diagnostics() diagnostics.Snapshot {
	return s.runtime.Diagnostics()
}

// Component is one injected lifecycle dependency. Start receives a startup-
// only context and must not retain it as a lifetime context. A failed Start is
// responsible for its own partial work; Runtime shuts down every previously
// started component in reverse order.
type Component interface {
	Name() string
	Start(context.Context) error
	Shutdown(context.Context) error
}

// Dependencies are private runtime integration seams. They contain no SDK
// callbacks and no concrete connectivity types.
type Dependencies struct {
	Clock      coreclock.Clock
	Handlers   map[string]Handler
	Components []Component
}

type lifecycleComponent struct {
	name      string
	component Component
}
