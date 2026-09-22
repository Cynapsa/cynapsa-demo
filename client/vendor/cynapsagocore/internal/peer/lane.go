package peer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// AuthorityState is the server-session membership authority lifecycle. It is
// deliberately independent from transport custody and reachability state.
type AuthorityState uint8

const (
	AuthoritySynchronizing AuthorityState = iota + 1
	AuthorityReady
	AuthorityPaused
)

// LaneState is the terminal-safe lifecycle of one lazily-created peer actor.
type LaneState uint8

const (
	LaneActive LaneState = iota + 1
	LanePaused
	LaneRemoved
	LaneClosed
)

// WorkKind is diagnostic-only lane classification. It never controls
// membership authority or transport selection.
type WorkKind uint8

const (
	WorkInbound WorkKind = iota + 1
	WorkOutbound
)

// Commit linearizes one final external ownership transfer against current
// lane authority. The effect itself runs outside the authority lock so a
// non-cooperative dependency cannot block a later removal fence. Removal that
// wins while the effect is running is reported after the effect, allowing the
// work owner to retire any newly created state. Network preparation remains
// outside Commit and must honor the Run context.
type Commit func(func() error) error

// AuthorityCheckpoint executes one intermediate ownership transfer while the
// exact peer lane is still authorized. Unlike Commit it does not terminalize
// the Work, so a composite operation may publish private prerequisite state
// and still require one final Commit before it reports success. A transient
// authority pause waits inside the checkpoint until this exact lane resumes,
// is removed, or either the supplied operation context or Work context ends;
// callers must not replay earlier effects while it waits.
type AuthorityCheckpoint func(context.Context, func() error) error

type authorityCheckpointContextKey struct{}

// AuthorityCheckpointFromContext returns the exact-lane intermediate fence
// installed for a running Work. It is process-private capability state and is
// never accepted from callers or encoded on the wire.
func AuthorityCheckpointFromContext(ctx context.Context) (AuthorityCheckpoint, bool) {
	checkpoint, ok := ctx.Value(authorityCheckpointContextKey{}).(AuthorityCheckpoint)
	return checkpoint, ok && checkpoint != nil
}

// Work transfers one private owned operation into a peer lane. Run must honor
// ctx and use Commit for SDK publication, carrier admission, or successful
// completion. Clear scrubs and releases every owned value exactly once.
type Work struct {
	Kind       WorkKind
	OwnedBytes uint64
	Run        func(context.Context, Commit) error
	Clear      func()
}

// Result is the one terminal outcome of admitted work.
type Result struct{ done <-chan error }

func (result Result) Wait(ctx context.Context) error {
	if ctx == nil || result.done == nil {
		return ErrInvalidConfig
	}
	// A terminal lane result that is already published owns the outcome. Do
	// not let a simultaneous caller cancellation hide a committed external
	// effect and invite an unsafe retry.
	select {
	case err := <-result.done:
		return err
	default:
	}
	select {
	case err := <-result.done:
		return err
	case <-ctx.Done():
		select {
		case err := <-result.done:
			return err
		default:
			return ctx.Err()
		}
	}
}

type AggregateBudget struct {
	mu                 sync.Mutex
	count, bytes       uint64
	maxCount, maxBytes uint64
}

func NewAggregateBudget(maxCount, maxBytes uint64) (*AggregateBudget, error) {
	if maxCount == 0 || maxCount > MaximumPeerQueue || maxBytes == 0 {
		return nil, ErrInvalidConfig
	}
	return &AggregateBudget{maxCount: maxCount, maxBytes: maxBytes}, nil
}

func (budget *AggregateBudget) reserve(bytes uint64) bool {
	if budget == nil {
		return false
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.count == budget.maxCount || bytes > budget.maxBytes-budget.bytes {
		return false
	}
	budget.count++
	budget.bytes += bytes
	return true
}

func (budget *AggregateBudget) release(bytes uint64) {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.count == 0 || bytes > budget.bytes {
		panic("peer: aggregate lane budget underflow")
	}
	budget.count--
	budget.bytes -= bytes
}

func (budget *AggregateBudget) Usage() (count, bytes uint64) {
	if budget == nil {
		return 0, 0
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.count, budget.bytes
}

type LaneConfig struct {
	PeerID          string
	MailboxCapacity int
	ByteCapacity    uint64
	Budget          *AggregateBudget
	InitiallyPaused bool
	IdleTimeout     time.Duration
	ClosePeer       func(string)
	OnClosed        func(*Lane)
	AuthorityReady  func() bool
}

type laneWork struct {
	work       Work
	result     chan error
	caller     context.Context
	cancelled  atomic.Bool
	stopCancel func() bool
	clearOnce  sync.Once
}

func (work *laneWork) clear(budget *AggregateBudget) {
	work.clearOnce.Do(func() {
		if work.stopCancel != nil {
			work.stopCancel()
		}
		if work.work.Clear != nil {
			work.work.Clear()
		}
		budget.release(work.work.OwnedBytes)
	})
}

// Lane is one lazy peer actor. Only run owns pending work. The atomic state is
// the external security fence consulted by admission and external callbacks.
type Lane struct {
	config LaneConfig
	state  atomic.Uint32

	admitMu         sync.Mutex
	ownedCount      uint64
	ownedBytes      uint64
	mailbox         chan *laneWork
	wake            chan struct{}
	done            chan struct{}
	closeOnce       sync.Once
	lifetime        context.Context
	cancel          context.CancelFunc
	operationMu     sync.Mutex
	operationCancel context.CancelFunc
	authorityMu     sync.RWMutex
}

func NewLane(config LaneConfig) (*Lane, error) {
	if config.PeerID == "" || config.MailboxCapacity <= 0 || config.MailboxCapacity > MaximumPeerQueue || config.ByteCapacity == 0 || config.Budget == nil || config.IdleTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	ctx, cancel := context.WithCancel(context.Background())
	lane := &Lane{config: config, mailbox: make(chan *laneWork, config.MailboxCapacity), wake: make(chan struct{}, 1), done: make(chan struct{}), lifetime: ctx, cancel: cancel}
	state := LaneActive
	if config.InitiallyPaused {
		state = LanePaused
	}
	lane.state.Store(uint32(state))
	go lane.run()
	return lane, nil
}

func (lane *Lane) State() LaneState {
	if lane == nil {
		return LaneClosed
	}
	return LaneState(lane.state.Load())
}

// Admit transfers work ownership on success. Caller cancellation stops queued
// work without weakening the lane's independent lifetime.
func (lane *Lane) Admit(ctx context.Context, work Work) (Result, error) {
	if lane == nil || ctx == nil || work.Run == nil || work.Clear == nil || (work.Kind != WorkInbound && work.Kind != WorkOutbound) {
		return Result{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	lane.admitMu.Lock()
	state := lane.State()
	if state == LaneRemoved {
		lane.admitMu.Unlock()
		return Result{}, ErrUnauthorized
	}
	if state == LaneClosed {
		lane.admitMu.Unlock()
		return Result{}, ErrClosed
	}
	if lane.ownedCount == uint64(lane.config.MailboxCapacity) || work.OwnedBytes > lane.config.ByteCapacity-lane.ownedBytes || !lane.config.Budget.reserve(work.OwnedBytes) {
		lane.admitMu.Unlock()
		return Result{}, ErrQueueFull
	}
	owned := &laneWork{work: work, result: make(chan error, 1), caller: ctx}
	lane.ownedCount++
	lane.ownedBytes += work.OwnedBytes
	owned.stopCancel = context.AfterFunc(ctx, func() {
		owned.cancelled.Store(true)
		lane.signal()
	})
	select {
	case lane.mailbox <- owned:
		lane.admitMu.Unlock()
		return Result{done: owned.result}, nil
	default:
		lane.ownedCount--
		lane.ownedBytes -= work.OwnedBytes
		lane.admitMu.Unlock()
		if owned.stopCancel != nil {
			owned.stopCancel()
		}
		lane.config.Budget.release(work.OwnedBytes)
		return Result{}, ErrQueueFull
	}
}

func (lane *Lane) releaseOwned(work *laneWork) {
	lane.admitMu.Lock()
	if lane.ownedCount == 0 || work.work.OwnedBytes > lane.ownedBytes {
		lane.admitMu.Unlock()
		panic("peer: lane budget underflow")
	}
	lane.ownedCount--
	lane.ownedBytes -= work.work.OwnedBytes
	lane.admitMu.Unlock()
	work.clear(lane.config.Budget)
}

func (lane *Lane) finish(work *laneWork, err error) {
	lane.releaseOwned(work)
	work.result <- err
	close(work.result)
}

func (lane *Lane) signal() {
	select {
	case lane.wake <- struct{}{}:
	default:
	}
}

func (lane *Lane) Pause() bool {
	if lane == nil {
		return false
	}
	lane.admitMu.Lock()
	defer lane.admitMu.Unlock()
	lane.authorityMu.Lock()
	defer lane.authorityMu.Unlock()
	if lane.state.CompareAndSwap(uint32(LaneActive), uint32(LanePaused)) {
		// Preparation already owned by this lane may continue, but checkpoints
		// and final Commit stay fenced by LanePaused. Removal still cancels the
		// operation; a present peer resumes without losing stable transfer state.
		lane.signal()
		return true
	}
	return lane.State() == LanePaused
}

func (lane *Lane) Resume() bool {
	if lane == nil {
		return false
	}
	lane.admitMu.Lock()
	defer lane.admitMu.Unlock()
	lane.authorityMu.Lock()
	defer lane.authorityMu.Unlock()
	if lane.state.CompareAndSwap(uint32(LanePaused), uint32(LaneActive)) {
		lane.signal()
		return true
	}
	active := lane.State() == LaneActive
	if active {
		// A registry-wide fence may leave the local lane Active while its actor
		// waits for authority. Exact publication must wake that actor too.
		lane.signal()
	}
	return active
}

// Remove installs the immediate terminal authority fence before ordinary FIFO
// work can run. Cleanup and exact-once transport close are owned by run.
func (lane *Lane) Remove() bool {
	if lane == nil {
		return false
	}
	lane.admitMu.Lock()
	defer lane.admitMu.Unlock()
	lane.authorityMu.Lock()
	defer lane.authorityMu.Unlock()
	for {
		state := lane.State()
		if state == LaneRemoved || state == LaneClosed {
			return false
		}
		if lane.state.CompareAndSwap(uint32(state), uint32(LaneRemoved)) {
			lane.cancelOperation()
			lane.cancel()
			lane.signal()
			return true
		}
	}
}

// retireIdle installs a terminal fence only while this actor owns no work.
// admitMu makes a racing Admit either win before this check or observe the
// terminal state; paused work is never eligible for idle retirement.
func (lane *Lane) retireIdle() bool {
	if lane == nil {
		return false
	}
	lane.admitMu.Lock()
	defer lane.admitMu.Unlock()
	if lane.ownedCount != 0 {
		return false
	}
	lane.authorityMu.Lock()
	defer lane.authorityMu.Unlock()
	if !lane.state.CompareAndSwap(uint32(LaneActive), uint32(LaneRemoved)) {
		return false
	}
	lane.cancelOperation()
	lane.cancel()
	lane.signal()
	return true
}

func (lane *Lane) Authorize() error {
	if lane == nil {
		return ErrClosed
	}
	lane.authorityMu.RLock()
	defer lane.authorityMu.RUnlock()
	return lane.authorizeLocked()
}

func (lane *Lane) authorizeLocked() error {
	switch lane.State() {
	case LaneActive:
		return nil
	case LaneRemoved, LaneClosed:
		return ErrUnauthorized
	default:
		return ErrUnavailable
	}
}

func (lane *Lane) Join(ctx context.Context) error {
	if lane == nil || ctx == nil {
		return ErrInvalidConfig
	}
	select {
	case <-lane.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (lane *Lane) Usage() (count, bytes uint64) {
	if lane == nil {
		return 0, 0
	}
	lane.admitMu.Lock()
	defer lane.admitMu.Unlock()
	return lane.ownedCount, lane.ownedBytes
}

func (lane *Lane) run() {
	defer close(lane.done)
	defer func() {
		lane.state.Store(uint32(LaneClosed))
		if lane.config.OnClosed != nil {
			lane.config.OnClosed(lane)
		}
	}()
	pending := make([]*laneWork, 0, lane.config.MailboxCapacity)
	for {
		state := lane.State()
		if state == LaneRemoved {
			for _, work := range pending {
				lane.finish(work, ErrUnauthorized)
			}
			pending = pending[:0]
			for {
				select {
				case work := <-lane.mailbox:
					lane.finish(work, ErrUnauthorized)
				default:
					lane.closeOnce.Do(func() {
						if lane.config.ClosePeer != nil {
							func() {
								defer func() { _ = recover() }()
								lane.config.ClosePeer(lane.config.PeerID)
							}()
						}
					})
					return
				}
			}
		}
		if state == LanePaused || !lane.authorityReady() {
			select {
			case work := <-lane.mailbox:
				pending = append(pending, work)
			case <-lane.wake:
			}
			pending = lane.discardCancelled(pending)
			continue
		}
		pending = lane.discardCancelled(pending)
		var work *laneWork
		if len(pending) != 0 {
			work = pending[0]
			copy(pending, pending[1:])
			pending[len(pending)-1] = nil
			pending = pending[:len(pending)-1]
		} else {
			idle := time.NewTimer(lane.config.IdleTimeout)
			select {
			case work = <-lane.mailbox:
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
			case <-lane.wake:
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
				continue
			case <-idle.C:
				lane.retireIdle()
				continue
			}
		}
		if work.cancelled.Load() {
			lane.finish(work, callerError(work.caller))
			continue
		}
		err, committed := lane.execute(work)
		// A successful Commit is the operation's membership linearization
		// point. Cancellation or a snapshot fence that wins afterwards cannot
		// retroactively change that already-admitted result.
		if committed {
			lane.finish(work, err)
			continue
		}
		state = lane.State()
		if work.cancelled.Load() {
			lane.finish(work, callerError(work.caller))
			continue
		}
		if state == LaneRemoved || state == LaneClosed {
			lane.finish(work, ErrUnauthorized)
			continue
		}
		if (state == LanePaused || !lane.authorityReady()) && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable)) {
			pending = append([]*laneWork{work}, pending...)
			continue
		}
		lane.finish(work, err)
	}
}

func (lane *Lane) execute(work *laneWork) (result error, committed bool) {
	ctx, cancel := context.WithCancel(lane.lifetime)
	stopCaller := context.AfterFunc(work.caller, cancel)
	lane.setOperationCancel(cancel)
	defer func() {
		lane.clearOperationCancel()
		stopCaller()
		cancel()
	}()
	defer func() {
		if recover() != nil {
			result = ErrUnavailable
		}
	}()
	runAuthorityEffect := func(waitCtx context.Context, effect func() error, terminal bool) error {
		if waitCtx == nil || effect == nil || committed {
			return ErrInvalidConfig
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := waitCtx.Err(); err != nil {
				return err
			}
			// Linearize membership admission while holding only the authority
			// read side. The external ownership transfer runs after releasing the
			// lock, so a non-cooperative SDK/dependency callback cannot prevent a
			// snapshot from fencing and closing the peer. If the fence wins first,
			// authorization fails and the effect is never invoked.
			lane.authorityMu.RLock()
			err := lane.authorizeLocked()
			if err == nil && !lane.authorityReady() {
				err = ErrUnavailable
			}
			if err == nil && terminal {
				// Terminal ownership moves at admission, even when the external
				// effect itself reports an error. Retrying that effect could duplicate
				// a dependency handoff whose error was ambiguous.
				committed = true
			}
			lane.authorityMu.RUnlock()
			if err == nil {
				break
			}
			if terminal || !errors.Is(err, ErrUnavailable) {
				return err
			}
			// Intermediate carrier/dependency state is already lane-owned. Hold
			// it in place during a transient snapshot rather than returning an
			// error that would either terminalize or replay the composite work.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-waitCtx.Done():
				return waitCtx.Err()
			case <-lane.lifetime.Done():
				return ErrUnauthorized
			case <-lane.wake:
			}
		}
		effectErr := effect()
		// A removal may win while a non-cooperative effect is running outside
		// the authority lock. Report that terminal fence so the work owner can
		// retire any state created after the peer cleanup scan. A mere Pause
		// does not revoke an operation that linearized while authority was
		// Ready; the next complete snapshot will either retain or remove it.
		lane.authorityMu.RLock()
		state := lane.State()
		lane.authorityMu.RUnlock()
		if state == LaneRemoved || state == LaneClosed {
			return ErrUnauthorized
		}
		return effectErr
	}
	checkpoint := func(waitCtx context.Context, effect func() error) error {
		return runAuthorityEffect(waitCtx, effect, false)
	}
	ctx = context.WithValue(ctx, authorityCheckpointContextKey{}, AuthorityCheckpoint(checkpoint))
	commit := func(effect func() error) error { return runAuthorityEffect(ctx, effect, true) }
	result = work.work.Run(ctx, commit)
	if result == nil && !committed {
		// A Work implementation may prepare outside the authority fence, but
		// it may not report success without one final current-membership
		// commit.
		result = ErrUnavailable
	}
	return result, committed
}

func (lane *Lane) authorityReady() bool {
	return lane != nil && (lane.config.AuthorityReady == nil || lane.config.AuthorityReady())
}

func (lane *Lane) setOperationCancel(cancel context.CancelFunc) {
	lane.operationMu.Lock()
	lane.operationCancel = cancel
	lane.operationMu.Unlock()
}

func (lane *Lane) clearOperationCancel() {
	lane.operationMu.Lock()
	lane.operationCancel = nil
	lane.operationMu.Unlock()
}

func (lane *Lane) cancelOperation() {
	lane.operationMu.Lock()
	cancel := lane.operationCancel
	lane.operationMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (lane *Lane) discardCancelled(pending []*laneWork) []*laneWork {
	kept := pending[:0]
	for _, work := range pending {
		if work.cancelled.Load() {
			lane.finish(work, callerError(work.caller))
			continue
		}
		kept = append(kept, work)
	}
	return kept
}

func callerError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Canceled
}
