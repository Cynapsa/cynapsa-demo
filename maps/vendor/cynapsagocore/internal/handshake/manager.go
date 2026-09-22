// Package handshake owns private peer-link establishment and recovery.
package handshake

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const MaximumPeers = 65536

type Signal struct {
	Attempt Attempt
	Payload []byte
}

func (s Signal) clone() Signal {
	s.Payload = append([]byte(nil), s.Payload...)
	return s
}

// Negotiator is implemented by the private Jingle/WebRTC establishment owner.
// Signaling bytes never cross the SDK boundary.
type Negotiator interface {
	Establish(context.Context, Attempt) error
	Accept(context.Context, Attempt, Signal) error
	Apply(context.Context, Attempt, Signal) error
}

type Config struct {
	LocalIdentity   string
	MaximumPeers    int
	MaximumAttempts int
	AttemptTimeout  time.Duration
	CooldownInitial time.Duration
	CooldownMaximum time.Duration
	Clock           transport.Clock
	Random          io.Reader
	Jitter          func(time.Duration) time.Duration
}

type flight struct {
	attempt Attempt
	cancel  context.CancelFunc
	done    chan struct{}
}

// Manager serializes one establishment attempt per peer.
type Manager struct {
	mu        sync.Mutex
	config    Config
	engine    Negotiator
	active    map[string]*flight
	cooldowns map[string]*Cooldown
	retired   map[string]time.Time
	closed    bool
}

func NewManager(config Config, engine Negotiator) (*Manager, error) {
	if protocol.ValidateAgentIdentity(config.LocalIdentity) != nil || config.MaximumPeers <= 0 || config.MaximumPeers > MaximumPeers || config.MaximumAttempts <= 0 || config.MaximumAttempts > 1<<20 || config.AttemptTimeout <= 0 || config.CooldownInitial <= 0 || config.CooldownMaximum < config.CooldownInitial || config.Clock == nil || config.Clock.Now().IsZero() || engine == nil {
		return nil, ErrInvalidConfig
	}
	return &Manager{config: config, engine: engine, active: make(map[string]*flight), cooldowns: make(map[string]*Cooldown), retired: make(map[string]time.Time)}, nil
}

// Start runs one single-flight attempt. The caller should delegate this
// blocking operation away from a peer decision loop.
func (m *Manager) Start(ctx context.Context, peerID string) (Attempt, error) {
	if m == nil || ctx == nil || protocol.ValidateAgentIdentity(peerID) != nil || peerID == m.config.LocalIdentity {
		return Attempt{}, ErrInvalidPeer
	}
	now := m.config.Clock.Now().UTC()
	if now.IsZero() {
		return Attempt{}, ErrFailed
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return Attempt{}, ErrClosed
	}
	if m.active[peerID] != nil {
		m.mu.Unlock()
		return Attempt{}, ErrInFlight
	}
	m.pruneRetiredLocked(now)
	if len(m.active)+len(m.retired) >= m.config.MaximumAttempts {
		m.mu.Unlock()
		return Attempt{}, ErrCapacity
	}
	cooldown := m.cooldowns[peerID]
	if cooldown != nil && !cooldown.Ready(now) {
		m.mu.Unlock()
		return Attempt{}, ErrCooldown
	}
	if cooldown == nil {
		if len(m.cooldowns) >= m.config.MaximumPeers {
			m.mu.Unlock()
			return Attempt{}, ErrCapacity
		}
		cooldown, _ = NewCooldown(m.config.CooldownInitial, m.config.CooldownMaximum, m.config.Jitter)
		m.cooldowns[peerID] = cooldown
	}
	attempt, err := NewAttempt(peerID, m.config.LocalIdentity, now, m.config.AttemptTimeout, m.config.Random)
	if err != nil {
		m.mu.Unlock()
		return Attempt{}, err
	}
	owned, cancel := context.WithTimeout(ctx, m.config.AttemptTimeout)
	attempt.State = AttemptRunning
	current := &flight{attempt: attempt, cancel: cancel, done: make(chan struct{})}
	m.active[peerID] = current
	m.mu.Unlock()

	err = contain(func() error { return m.engine.Establish(owned, attempt) })
	close(current.done)
	cancel()
	return m.finish(peerID, attempt, err)
}

// Recover adapts Manager to the peer worker's demand-driven recovery seam.
func (m *Manager) Recover(ctx context.Context, peerID string) error {
	_, err := m.Start(ctx, peerID)
	return err
}

// HandleSignal validates freshness, resolves glare, and rejects retired IDs.
func (m *Manager) HandleSignal(ctx context.Context, signal Signal) error {
	if m == nil || ctx == nil || !validAttempt(signal.Attempt) || len(signal.Payload) == 0 || len(signal.Payload) > 1<<20 {
		return ErrStale
	}
	now := m.config.Clock.Now().UTC()
	if now.IsZero() {
		return ErrFailed
	}
	if !m.validRemoteAttempt(signal.Attempt, now) {
		return ErrStale
	}
	peerID := ""
	switch {
	case signal.Attempt.InitiatorID == m.config.LocalIdentity:
		peerID = signal.Attempt.PeerID
	case signal.Attempt.PeerID == m.config.LocalIdentity:
		peerID = signal.Attempt.InitiatorID
	default:
		return ErrStale
	}
	if peerID == m.config.LocalIdentity {
		return ErrStale
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	m.pruneRetiredLocked(now)
	if _, stale := m.retired[signal.Attempt.ID]; stale {
		m.mu.Unlock()
		return ErrStale
	}
	current := m.active[peerID]
	if current != nil && current.attempt.ID == signal.Attempt.ID {
		attempt := current.attempt
		m.mu.Unlock()
		return normalize(contain(func() error { return m.engine.Apply(ctx, attempt, signal.clone()) }), ctx)
	}
	cooldown := m.cooldowns[peerID]
	// A peer cannot bypass a locally imposed failure cooldown by initiating the
	// next attempt itself. An already-active attempt is still allowed to resolve
	// glare below because it was admitted while the peer was ready.
	if current == nil && cooldown != nil && !cooldown.Ready(now) {
		m.mu.Unlock()
		return ErrCooldown
	}
	if len(m.active)+len(m.retired) >= m.config.MaximumAttempts {
		m.mu.Unlock()
		return ErrCapacity
	}
	if cooldown == nil {
		if len(m.cooldowns) >= m.config.MaximumPeers {
			m.mu.Unlock()
			return ErrCapacity
		}
		cooldown, _ := NewCooldown(m.config.CooldownInitial, m.config.CooldownMaximum, m.config.Jitter)
		m.cooldowns[peerID] = cooldown
	}
	var predecessorDone <-chan struct{}
	if current != nil {
		winner := ResolveGlare(current.attempt, signal.Attempt)
		if winner.ID == current.attempt.ID {
			m.retireLocked(signal.Attempt.ID, signal.Attempt.Deadline)
			m.mu.Unlock()
			return ErrStale
		}
		current.cancel()
		m.retireLocked(current.attempt.ID, current.attempt.Deadline)
		predecessorDone = current.done
	}
	owned, cancel := context.WithTimeout(ctx, signal.Attempt.Deadline.Sub(now))
	attempt := signal.Attempt
	attempt.State = AttemptRunning
	current = &flight{attempt: attempt, cancel: cancel, done: make(chan struct{})}
	m.active[peerID] = current
	m.mu.Unlock()
	if predecessorDone != nil {
		select {
		case <-predecessorDone:
		case <-owned.Done():
			close(current.done)
			cancel()
			_, err := m.finish(peerID, attempt, owned.Err())
			return err
		}
		if err := owned.Err(); err != nil {
			close(current.done)
			cancel()
			_, err = m.finish(peerID, attempt, err)
			return err
		}
	}
	err := contain(func() error { return m.engine.Accept(owned, attempt, signal.clone()) })
	close(current.done)
	cancel()
	_, err = m.finish(peerID, attempt, err)
	return err
}

// validRemoteAttempt limits absolute timestamps supplied by the peer to one
// local attempt lifetime. This prevents a future-dated attempt from pinning a
// retired ID or an Accept context until an attacker-controlled deadline.
func (m *Manager) validRemoteAttempt(attempt Attempt, now time.Time) bool {
	if attempt.StartedAt.After(now) || !now.Before(attempt.Deadline) {
		return false
	}
	lifetime := attempt.Deadline.Sub(attempt.StartedAt)
	return lifetime > 0 && lifetime <= m.config.AttemptTimeout && !attempt.Deadline.After(now.Add(m.config.AttemptTimeout))
}

func (m *Manager) finish(peerID string, attempt Attempt, operationErr error) (Attempt, error) {
	now := m.config.Clock.Now().UTC()
	m.mu.Lock()
	current := m.active[peerID]
	if current == nil || current.attempt.ID != attempt.ID {
		m.mu.Unlock()
		attempt.State = AttemptCancelled
		return attempt, ErrStale
	}
	delete(m.active, peerID)
	m.retireLocked(attempt.ID, attempt.Deadline)
	cooldown := m.cooldowns[peerID]
	success := operationErr == nil
	if !now.IsZero() {
		cooldownBase := now
		// Cleanup is part of the failed attempt, but it must not extend that
		// attempt's backoff indefinitely. A replacement that was already
		// queued while cleanup owned the peer becomes eligible once the
		// cooldown measured from the wire attempt deadline has elapsed.
		if !success && attempt.Deadline.Before(cooldownBase) {
			cooldownBase = attempt.Deadline
		}
		cooldown.Next(cooldownBase, success)
	}
	m.mu.Unlock()
	if now.IsZero() {
		attempt.State = AttemptFailed
		return attempt, ErrFailed
	}
	if success {
		attempt.State = AttemptSucceeded
		return attempt, nil
	}
	attempt.State = AttemptFailed
	return attempt, normalize(operationErr, nil)
}

func (m *Manager) retireLocked(id string, deadline time.Time) {
	m.retired[id] = deadline
}

func (m *Manager) pruneRetiredLocked(now time.Time) {
	for id, deadline := range m.retired {
		if !now.Before(deadline) {
			delete(m.retired, id)
		}
	}
}

func (m *Manager) InFlight(peerID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[peerID] != nil
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for _, active := range m.active {
		active.cancel()
	}
	m.mu.Unlock()
}

func contain(call func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrFailed
		}
	}()
	return call()
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
	for _, known := range []error{ErrInvalidConfig, ErrInvalidPeer, ErrCapacity, ErrInFlight, ErrCooldown, ErrStale, ErrClosed, ErrFailed} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrFailed
}
