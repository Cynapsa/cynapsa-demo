package rank1webrtc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type RestartSignalHandler interface {
	HandleTransportReplace(context.Context, string, rank2xmpp.Jingle) error
}

type JingleSource interface {
	ReceiveJingle(context.Context) (string, rank2xmpp.Jingle, error)
}

type SignalExecutorConfig struct {
	Workers       int
	QueueCapacity int
	QueueBytes    int
}

type signalClass uint8

const (
	signalSession signalClass = iota + 1
	signalRestart
)

type signalJob struct {
	class    signalClass
	peer     string
	payload  []byte
	started  time.Time
	deadline time.Time
	token    uint64
	size     int
}

// SignalPump drains authenticated Jingle independently from bounded handshake
// execution. It retains at most one pending session initiation and the newest
// restart per peer. A retry session or restart queued behind an active session
// therefore survives, while additional sessions are rejected and an old queued
// restart is token-safely superseded by the newest authorized signal.
type SignalPump struct {
	source     JingleSource
	handshakes *handshake.Manager
	restarts   RestartSignalHandler
	timeout    time.Duration
	clock      transport.Clock
	config     SignalExecutorConfig

	runMu   sync.Mutex
	running bool

	mu             sync.Mutex
	pendingSession map[string]*signalJob
	pendingRestart map[string]*signalJob
	sessionOrder   []string
	restartOrder   []string
	active         map[string]uint64
	count          int
	bytes          int
	nextToken      uint64
	tokenExhausted bool
	preferRestart  bool
	notify         chan struct{}
	wg             sync.WaitGroup
}

func NewSignalPump(source JingleSource, handshakes *handshake.Manager, restarts RestartSignalHandler, timeout time.Duration, clock transport.Clock, executor SignalExecutorConfig) (*SignalPump, error) {
	if source == nil || handshakes == nil || restarts == nil || timeout <= 0 || clock == nil || clock.Now().IsZero() || executor.Workers <= 0 || executor.Workers > 1024 || executor.QueueCapacity < executor.Workers || executor.QueueCapacity > 65536 || executor.QueueBytes <= 0 {
		return nil, transport.ErrInvalidConfig
	}
	return &SignalPump{
		source: source, handshakes: handshakes, restarts: restarts, timeout: timeout, clock: clock, config: executor,
		pendingSession: make(map[string]*signalJob), pendingRestart: make(map[string]*signalJob), active: make(map[string]uint64), notify: make(chan struct{}, 1), preferRestart: true,
	}, nil
}

func (p *SignalPump) Run(ctx context.Context) error {
	if p == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	p.runMu.Lock()
	if p.running {
		p.runMu.Unlock()
		return transport.ErrUnavailable
	}
	p.running = true
	p.runMu.Unlock()
	defer func() {
		p.runMu.Lock()
		p.running = false
		p.runMu.Unlock()
	}()

	owned, cancel := context.WithCancel(ctx)
	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker(owned)
	}
	var result error
	for {
		from, signal, err := p.source.ReceiveJingle(owned)
		if err != nil {
			result = err
			break
		}
		job, ok := p.makeJob(from, signal)
		clearJingle(&signal)
		if ok {
			p.admit(job)
		}
	}
	cancel()
	p.wake()
	p.wg.Wait()
	p.clearPending()
	return result
}

func (p *SignalPump) makeJob(from string, signal rank2xmpp.Jingle) (*signalJob, bool) {
	class := signalSession
	switch signal.Action {
	case "transport-replace":
		class = signalRestart
	case "session-initiate":
		if signal.Initiator != from {
			return nil, false
		}
	default:
		return nil, false
	}
	started := p.clock.Now().UTC()
	if started.IsZero() {
		return nil, false
	}
	encoded, err := rank2xmpp.EncodeJingle(signal)
	if err != nil {
		return nil, false
	}
	peer := strings.Clone(from)
	jobTimeout := p.timeout
	if class == signalRestart {
		jobTimeout = max(p.timeout/2, time.Nanosecond)
	}
	return &signalJob{class: class, peer: peer, payload: encoded, started: started, deadline: started.Add(jobTimeout), size: len(peer) + len(encoded)}, true
}

func (p *SignalPump) admit(job *signalJob) {
	if job == nil || job.size <= 0 {
		clearSignalJob(job)
		return
	}
	p.mu.Lock()
	if job.class == signalRestart {
		if previous := p.pendingRestart[job.peer]; previous != nil {
			if job.size > p.config.QueueBytes || p.bytes-previous.size > p.config.QueueBytes-job.size {
				p.mu.Unlock()
				clearSignalJob(job)
				return
			}
			token, ok := p.allocateTokenLocked()
			if !ok {
				p.mu.Unlock()
				clearSignalJob(job)
				return
			}
			job.token = token
			p.pendingRestart[job.peer] = job
			p.bytes += job.size - previous.size
			p.mu.Unlock()
			clearSignalJob(previous)
			p.wake()
			return
		}
	} else if p.pendingSession[job.peer] != nil {
		p.mu.Unlock()
		clearSignalJob(job)
		return
	}
	if job.size > p.config.QueueBytes || p.count >= p.config.QueueCapacity+p.config.Workers || p.bytes > p.config.QueueBytes-job.size {
		p.mu.Unlock()
		clearSignalJob(job)
		return
	}
	token, ok := p.allocateTokenLocked()
	if !ok {
		p.mu.Unlock()
		clearSignalJob(job)
		return
	}
	job.token = token
	p.count++
	p.bytes += job.size
	if job.class == signalRestart {
		p.pendingRestart[job.peer] = job
		p.restartOrder = append(p.restartOrder, job.peer)
	} else {
		p.pendingSession[job.peer] = job
		p.sessionOrder = append(p.sessionOrder, job.peer)
	}
	p.mu.Unlock()
	p.wake()
}

func (p *SignalPump) allocateTokenLocked() (uint64, bool) {
	if p.tokenExhausted || p.nextToken == ^uint64(0) {
		p.tokenExhausted = true
		return 0, false
	}
	p.nextToken++
	return p.nextToken, true
}

func (p *SignalPump) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		job := p.next(ctx)
		if job == nil {
			return
		}
		p.execute(ctx, job)
		p.finish(job)
	}
}

func (p *SignalPump) next(ctx context.Context) *signalJob {
	for {
		p.mu.Lock()
		var job *signalJob
		if p.preferRestart {
			job = p.popLocked(signalRestart)
			if job == nil {
				job = p.popLocked(signalSession)
			}
		} else {
			job = p.popLocked(signalSession)
			if job == nil {
				job = p.popLocked(signalRestart)
			}
		}
		if job != nil {
			p.preferRestart = job.class != signalRestart
			p.active[job.peer] = job.token
			p.mu.Unlock()
			return job
		}
		p.mu.Unlock()
		select {
		case <-p.notify:
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *SignalPump) popLocked(class signalClass) *signalJob {
	var order *[]string
	var pending map[string]*signalJob
	if class == signalRestart {
		order, pending = &p.restartOrder, p.pendingRestart
	} else {
		order, pending = &p.sessionOrder, p.pendingSession
	}
	limit := len(*order)
	for i := 0; i < limit; i++ {
		peer := (*order)[0]
		*order = (*order)[1:]
		job := pending[peer]
		if job == nil {
			continue
		}
		if p.active[peer] != 0 {
			*order = append(*order, peer)
			continue
		}
		delete(pending, peer)
		return job
	}
	return nil
}

func (p *SignalPump) execute(ctx context.Context, job *signalJob) {
	defer func() { _ = recover() }()
	operation, cancel := context.WithDeadline(ctx, job.deadline)
	defer cancel()
	if job.class == signalRestart {
		signal, err := rank2xmpp.DecodeJingle(job.payload)
		if err == nil && signal.Action == "transport-replace" {
			_ = p.restarts.HandleTransportReplace(operation, job.peer, signal)
		}
		clearJingle(&signal)
		return
	}
	attempt := handshake.Attempt{ID: jobSID(job.payload), PeerID: p.localResponder(job.payload), InitiatorID: job.peer, StartedAt: job.started, Deadline: job.deadline, State: handshake.AttemptPending}
	if attempt.ID == "" || attempt.PeerID == "" {
		return
	}
	_ = p.handshakes.HandleSignal(operation, handshake.Signal{Attempt: attempt, Payload: job.payload})
}

func jobSID(payload []byte) string {
	signal, err := rank2xmpp.DecodeJingle(payload)
	if err != nil {
		return ""
	}
	defer clearJingle(&signal)
	return signal.SID
}

func (p *SignalPump) localResponder(payload []byte) string {
	signal, err := rank2xmpp.DecodeJingle(payload)
	if err != nil {
		return ""
	}
	defer clearJingle(&signal)
	return signal.Responder
}

func (p *SignalPump) finish(job *signalJob) {
	p.mu.Lock()
	if p.active[job.peer] == job.token {
		delete(p.active, job.peer)
		p.count--
		p.bytes -= job.size
	}
	p.mu.Unlock()
	clearSignalJob(job)
	p.wake()
}

func (p *SignalPump) clearPending() {
	p.mu.Lock()
	jobs := make([]*signalJob, 0, len(p.pendingSession)+len(p.pendingRestart))
	for _, job := range p.pendingSession {
		jobs = append(jobs, job)
	}
	for _, job := range p.pendingRestart {
		jobs = append(jobs, job)
	}
	p.pendingSession = make(map[string]*signalJob)
	p.pendingRestart = make(map[string]*signalJob)
	p.sessionOrder = nil
	p.restartOrder = nil
	p.active = make(map[string]uint64)
	p.count, p.bytes = 0, 0
	p.mu.Unlock()
	for _, job := range jobs {
		clearSignalJob(job)
	}
}

func (p *SignalPump) wake() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func clearSignalJob(job *signalJob) {
	if job == nil {
		return
	}
	clear(job.payload)
	*job = signalJob{}
}

func clearJingle(signal *rank2xmpp.Jingle) {
	if signal == nil {
		return
	}
	*signal = rank2xmpp.Jingle{}
}
