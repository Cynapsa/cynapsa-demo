// Package peer owns independent private state and work for one remote agent.
package peer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const MaximumPeerQueue = 65536

type Sender interface {
	Send(context.Context, transport.Kind, protocol.Envelope) error
}

type Recoverer interface {
	Recover(context.Context, string) error
}

type Inbound func(context.Context, transport.Kind, protocol.Envelope) error

type Config struct {
	PeerID         string
	QueueCapacity  int
	InboundWorkers int
	HealthPolicy   HealthPolicy
	Sender         Sender
	Recoverer      Recoverer
	Inbound        Inbound
}

type sendRequest struct {
	ctx      context.Context
	envelope protocol.Envelope
	result   chan error
}

type inboundRequest struct {
	kind     transport.Kind
	envelope protocol.Envelope
}

type healthRequest struct {
	now         time.Time
	observation transport.Observation
}

// Worker serializes peer-scoped state decisions. Network sends, handshakes,
// and inbound payload work execute outside the decision loop with hard bounds.
type Worker struct {
	config Config
	policy HealthPolicy

	stateMu     sync.RWMutex
	state       State
	sends       chan sendRequest
	inbound     chan inboundRequest
	health      chan healthRequest
	sendJobs    chan sendRequest
	inboundJobs chan inboundRequest
	done        chan struct{}
	close       sync.Once
	runMu       sync.Mutex
	started     bool
	wg          sync.WaitGroup
}

func NewWorker(config Config) (*Worker, error) {
	if protocol.ValidateAgentIdentity(config.PeerID) != nil || config.QueueCapacity <= 0 || config.QueueCapacity > MaximumPeerQueue || config.InboundWorkers <= 0 || config.InboundWorkers > 1024 || !config.HealthPolicy.valid() || config.Sender == nil || config.Inbound == nil {
		return nil, ErrInvalidConfig
	}
	return &Worker{
		config: config,
		policy: config.HealthPolicy,
		state:  State{PeerID: config.PeerID, PreferredRank: RankDurable, Health: HealthDemoted},
		sends:  make(chan sendRequest, config.QueueCapacity), inbound: make(chan inboundRequest, config.QueueCapacity),
		health: make(chan healthRequest, config.QueueCapacity), sendJobs: make(chan sendRequest, config.QueueCapacity), inboundJobs: make(chan inboundRequest, config.QueueCapacity), done: make(chan struct{}),
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if w == nil || ctx == nil {
		return ErrInvalidConfig
	}
	w.runMu.Lock()
	if w.started {
		w.runMu.Unlock()
		return ErrClosed
	}
	w.started = true
	w.runMu.Unlock()
	w.wg.Add(1)
	go w.sendLoop(ctx)
	for i := 0; i < w.config.InboundWorkers; i++ {
		w.wg.Add(1)
		go w.inboundLoop(ctx)
	}
	defer func() {
		w.close.Do(func() { close(w.done) })
		w.wg.Wait()
	}()
	for {
		select {
		case request := <-w.sends:
			w.adjustQueued(-1)
			select {
			case w.sendJobs <- request:
			case <-ctx.Done():
			}
		case request := <-w.inbound:
			select {
			case w.inboundJobs <- request:
			case <-ctx.Done():
			}
		case observation := <-w.health:
			w.observeHealth(observation.now, observation.observation)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *Worker) sendLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case request := <-w.sendJobs:
			operation, cancel := context.WithCancel(request.ctx)
			stop := context.AfterFunc(ctx, cancel)
			err := func() (result error) {
				defer func() {
					if recover() != nil {
						result = ErrUnavailable
					}
				}()
				return w.sendNow(operation, request.envelope)
			}()
			stop()
			cancel()
			request.result <- err
			close(request.result)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) inboundLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case request := <-w.inboundJobs:
			func() { defer func() { _ = recover() }(); w.handleInbound(ctx, request) }()
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) Send(ctx context.Context, envelope protocol.Envelope) error {
	if w == nil || ctx == nil || protocol.ValidateEnvelope(envelope) != nil || envelope.Recipient != w.config.PeerID {
		return ErrInvalidConfig
	}
	result := make(chan error, 1)
	request := sendRequest{ctx: ctx, envelope: envelope.Clone(), result: result}
	w.adjustQueued(1)
	select {
	case w.sends <- request:
	case <-w.done:
		w.adjustQueued(-1)
		return ErrClosed
	case <-ctx.Done():
		w.adjustQueued(-1)
		return ctx.Err()
	default:
		w.adjustQueued(-1)
		return ErrQueueFull
	}
	select {
	case err := <-result:
		return err
	case <-w.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) sendNow(ctx context.Context, envelope protocol.Envelope) error {
	if selectRank(w.Snapshot()) == RankLive {
		err := w.config.Sender.Send(ctx, transport.KindLive, envelope.Clone())
		if err == nil {
			return nil
		}
		// Every failed live send is delivery-ambiguous at this layer. Demote
		// and replay the exact original identity through durable delivery.
		w.demote()
		return w.replay(ctx, envelope)
	}
	return normalize(w.config.Sender.Send(ctx, transport.KindDurable, envelope.Clone()), ctx)
}

func (w *Worker) replay(ctx context.Context, envelope protocol.Envelope) error {
	return normalize(w.config.Sender.Send(ctx, transport.KindDurable, envelope.Clone()), ctx)
}

func (w *Worker) Receive(envelope protocol.Envelope) error {
	return w.ReceiveFrom(transport.KindDurable, envelope)
}

func (w *Worker) ReceiveFrom(kind transport.Kind, envelope protocol.Envelope) error {
	if w == nil || (kind != transport.KindLive && kind != transport.KindDurable) || protocol.ValidateEnvelope(envelope) != nil || envelope.Sender != w.config.PeerID {
		return ErrInvalidConfig
	}
	select {
	case w.inbound <- inboundRequest{kind: kind, envelope: envelope.Clone()}:
		return nil
	case <-w.done:
		return ErrClosed
	default:
		return ErrQueueFull
	}
}

func (w *Worker) handleInbound(ctx context.Context, request inboundRequest) {
	if err := w.config.Inbound(ctx, request.kind, request.envelope.Clone()); err != nil {
		return
	}
	if request.kind != transport.KindDurable || w.config.Recoverer == nil {
		return
	}
	w.stateMu.Lock()
	if w.state.PreferredRank != RankDurable || w.state.RecoveryInFlight {
		w.stateMu.Unlock()
		return
	}
	w.state.RecoveryInFlight = true
	w.stateMu.Unlock()
	// Recovery is demand-driven only after an accepted durable peer message.
	err := w.config.Recoverer.Recover(ctx, w.config.PeerID)
	w.stateMu.Lock()
	w.state.RecoveryInFlight = false
	if err == nil {
		w.state.PreferredRank = RankLive
		w.state.Health = HealthHealthy
		w.state.DisconnectedSince = time.Time{}
	}
	w.stateMu.Unlock()
}

func (w *Worker) Observe(now time.Time, observation transport.Observation) error {
	if w == nil || now.IsZero() {
		return ErrInvalidConfig
	}
	select {
	case w.health <- healthRequest{now.UTC(), observation}:
		return nil
	case <-w.done:
		return ErrClosed
	default:
		return ErrQueueFull
	}
}

func (w *Worker) demote() {
	w.stateMu.Lock()
	w.state.PreferredRank = RankDurable
	w.state.Health = HealthDemoted
	w.stateMu.Unlock()
}

func (w *Worker) adjustQueued(delta int) {
	w.stateMu.Lock()
	w.state.QueuedMessageCount += delta
	w.stateMu.Unlock()
}

func normalize(err error, ctx context.Context) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrUnavailable
}
