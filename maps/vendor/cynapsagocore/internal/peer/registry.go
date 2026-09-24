package peer

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// AuthoritySession is an opaque in-process capability for one authenticated
// exact server session in a separately verified mesh. It is not a revision, timestamp,
// generation, or wire value. Retired-session callbacks cannot mutate a fresh
// session's authority.
type AuthoritySession struct {
	registry *Registry
	token    uint64
}

type RegistryConfig struct {
	MailboxCapacity     int
	LaneByteCapacity    uint64
	GlobalCountCapacity uint64
	GlobalByteCapacity  uint64
	MemberCapacity      int
	IdleTimeout         time.Duration
	FencePeer           func(string)
	AllowPeer           func(string)
	ClosePeer           func(string)
}

// Registry owns the complete current member set and lazily creates one Lane
// only when a peer has active work.
type Registry struct {
	mu           sync.Mutex
	transitionMu sync.Mutex
	config       RegistryConfig
	budget       *AggregateBudget
	state        AuthorityState
	authority    atomic.Uint64
	session      *AuthoritySession
	// snapshotInstalled distinguishes the open synchronization window, where
	// work is quarantined until membership is known, from the interval after a
	// complete snapshot has already excluded a peer. It prevents an absent lane
	// from being recreated between InstallSnapshot and Publish.
	snapshotInstalled bool
	members           map[string]struct{}
	memberEpochs      map[string]uint64
	nextMemberEpoch   uint64
	lanes             map[string]*Lane
	retiring          map[*Lane]struct{}
	dynamicRetiring   map[string]*Lane
	closed            bool
	closeDone         chan struct{}
}

func NewRegistry(config RegistryConfig) (*Registry, error) {
	if config.MailboxCapacity <= 0 || config.MailboxCapacity > MaximumPeerQueue || config.LaneByteCapacity == 0 || config.MemberCapacity <= 0 || config.MemberCapacity > MaximumPeerQueue || config.IdleTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	budget, err := NewAggregateBudget(config.GlobalCountCapacity, config.GlobalByteCapacity)
	if err != nil {
		return nil, err
	}
	registry := &Registry{config: config, budget: budget, state: AuthoritySynchronizing, members: make(map[string]struct{}, config.MemberCapacity), memberEpochs: make(map[string]uint64, config.MemberCapacity), lanes: make(map[string]*Lane), retiring: make(map[*Lane]struct{}), dynamicRetiring: make(map[string]*Lane)}
	// Even tokens are fenced; the low bit is the exact-session Ready state.
	registry.authority.Store(2)
	return registry, nil
}

// blockAuthority installs one process-local exact-session fence in a single
// atomic operation. Even tokens are fenced and odd tokens are published.
func (registry *Registry) blockAuthority() uint64 {
	if registry == nil {
		return 0
	}
	for {
		current := registry.authority.Load()
		base := current &^ uint64(1)
		if current == 0 || base > ^uint64(0)-2 {
			registry.authority.CompareAndSwap(current, 0)
			return 0
		}
		next := base + 2
		if registry.authority.CompareAndSwap(current, next) {
			return next
		}
	}
}

func (registry *Registry) retireSession(session *AuthoritySession) bool {
	if registry == nil || session == nil || session.registry != registry || session.token == 0 {
		return false
	}
	for {
		current := registry.authority.Load()
		if current&^uint64(1) != session.token || session.token > ^uint64(0)-2 {
			return false
		}
		if registry.authority.CompareAndSwap(current, session.token+2) {
			return true
		}
	}
}

func (registry *Registry) authorityReady() bool {
	return registry != nil && registry.authority.Load()&1 == 1
}

// BlockAdmission is the constant-time global membership fence. It prevents
// every existing lane Commit before any per-lane pause/cleanup iteration.
// The token advance also makes an in-flight stale Publish CAS fail closed.
func (registry *Registry) BlockAdmission() {
	if registry == nil {
		return
	}
	registry.blockAuthority()
	registry.mu.Lock()
	if !registry.closed {
		registry.session = nil
		registry.state = AuthorityPaused
	}
	registry.mu.Unlock()
}

// BeginSession retires the preceding authority capability and pauses every
// existing lane before returning a fresh synchronizing capability.
func (registry *Registry) BeginSession() (*AuthoritySession, error) {
	if registry == nil {
		return nil, ErrInvalidConfig
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, ErrClosed
	}
	token := registry.blockAuthority()
	if token == 0 {
		return nil, ErrClosed
	}
	for _, lane := range registry.lanes {
		lane.Pause()
	}
	session := &AuthoritySession{registry: registry, token: token}
	registry.session = session
	registry.state = AuthoritySynchronizing
	registry.snapshotInstalled = false
	return session, nil
}

// Pause retires one exact disconnected authority session. Work and deadlines
// remain lane-owned, but no external effect may complete while paused.
func (registry *Registry) Pause(session *AuthoritySession) error {
	if registry == nil || session == nil {
		return ErrInvalidConfig
	}
	if !registry.retireSession(session) {
		return ErrUnauthorized
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if session.registry != registry || registry.session != session {
		return ErrUnauthorized
	}
	for _, lane := range registry.lanes {
		lane.Pause()
	}
	registry.session = nil
	registry.state = AuthorityPaused
	registry.snapshotInstalled = false
	return nil
}

// Suspend closes admission and pauses every lane while retaining the exact
// published membership capability. A later Resume can reopen it only if no
// hard fence, snapshot replacement, or close superseded this session.
func (registry *Registry) Suspend(session *AuthoritySession) error {
	if registry == nil || session == nil || session.registry != registry || session.token == 0 {
		return ErrInvalidConfig
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if registry.session != session || registry.state != AuthorityReady || !registry.snapshotInstalled {
		return ErrUnauthorized
	}
	if !registry.authority.CompareAndSwap(session.token|1, session.token) {
		return ErrUnauthorized
	}
	for _, lane := range registry.lanes {
		lane.Pause()
	}
	registry.state = AuthorityPaused
	return nil
}

// Resume reopens a reversibly suspended exact membership capability. The CAS
// is the publication edge; a hard fence advances the token and makes this
// operation permanently stale.
func (registry *Registry) Resume(session *AuthoritySession) error {
	if registry == nil || session == nil || session.registry != registry || session.token == 0 {
		return ErrInvalidConfig
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if registry.session != session || registry.state != AuthorityPaused || !registry.snapshotInstalled || registry.authority.Load() != session.token {
		return ErrUnauthorized
	}
	if !registry.authority.CompareAndSwap(session.token, session.token|1) {
		return ErrUnauthorized
	}
	registry.state = AuthorityReady
	for _, lane := range registry.lanes {
		lane.Resume()
	}
	return nil
}

// InstallSnapshot atomically fences every absent lane and records the complete
// current member set. Authority remains Synchronizing and all present lanes
// remain paused until Publish validates this exact session capability.
func (registry *Registry) InstallSnapshot(session *AuthoritySession, members []string) error {
	if registry == nil || session == nil {
		return ErrInvalidConfig
	}
	canonical := append([]string(nil), members...)
	sort.Strings(canonical)
	if len(canonical) > registry.config.MemberCapacity {
		return ErrQueueFull
	}
	next := make(map[string]struct{}, len(canonical))
	for index, member := range canonical {
		if protocol.ValidateAgentIdentity(member) != nil || index != 0 && member == canonical[index-1] {
			return ErrInvalidConfig
		}
		next[member] = struct{}{}
	}

	// Serialize callback phases without holding the authority/admission lock.
	// BeginSession, Pause, and Close can therefore install their immediate
	// fail-closed fence even if an internal cleanup dependency blocks.
	registry.transitionMu.Lock()
	defer registry.transitionMu.Unlock()
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	if session.registry != registry || registry.session != session || registry.state != AuthoritySynchronizing || registry.authority.Load() != session.token {
		registry.mu.Unlock()
		return ErrUnauthorized
	}
	removedSet := make(map[string]struct{})
	for peerID := range registry.members {
		if _, present := next[peerID]; !present {
			removedSet[peerID] = struct{}{}
		}
	}
	nextEpochs := make(map[string]uint64, len(next))
	for peerID := range next {
		if epoch := registry.memberEpochs[peerID]; epoch != 0 {
			nextEpochs[peerID] = epoch
			continue
		}
		if registry.nextMemberEpoch == math.MaxUint64 {
			registry.mu.Unlock()
			return ErrEpochExhausted
		}
		registry.nextMemberEpoch++
		nextEpochs[peerID] = registry.nextMemberEpoch
	}
	allowed := make([]*Lane, 0)
	for peerID, lane := range registry.lanes {
		if _, present := next[peerID]; !present {
			// The terminal fence is installed while registry admission is locked.
			registry.retiring[lane] = struct{}{}
			lane.Remove()
			removedSet[peerID] = struct{}{}
			delete(registry.lanes, peerID)
			continue
		}
		allowed = append(allowed, lane)
	}
	removedPeers := make([]string, 0, len(removedSet))
	for peerID := range removedSet {
		removedPeers = append(removedPeers, peerID)
	}
	sort.Strings(removedPeers)
	registry.members = next
	registry.memberEpochs = nextEpochs
	registry.state = AuthoritySynchronizing
	registry.snapshotInstalled = true
	registry.mu.Unlock()
	for _, peerID := range removedPeers {
		if !callFencePeer(registry.config.FencePeer, peerID) {
			registry.mu.Lock()
			for _, lane := range allowed {
				lane.Pause()
			}
			registry.mu.Unlock()
			return ErrUnavailable
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if session.registry != registry || registry.session != session || registry.state != AuthoritySynchronizing || registry.authority.Load() != session.token {
		return ErrUnauthorized
	}
	return nil
}

// Publish resumes current lanes only after every external authority owner has
// installed the same complete snapshot. A stale session cannot reopen lanes.
func (registry *Registry) Publish(session *AuthoritySession) error {
	if registry == nil || session == nil {
		return ErrInvalidConfig
	}
	registry.transitionMu.Lock()
	defer registry.transitionMu.Unlock()
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	if session.registry != registry || registry.session != session || registry.state != AuthoritySynchronizing || registry.authority.Load() != session.token {
		registry.mu.Unlock()
		return ErrUnauthorized
	}
	canonical := make([]string, 0, len(registry.members))
	for peerID := range registry.members {
		canonical = append(canonical, peerID)
	}
	sort.Strings(canonical)
	allowed := make([]*Lane, 0, len(registry.lanes))
	for peerID, lane := range registry.lanes {
		if _, present := registry.members[peerID]; !present {
			registry.mu.Unlock()
			return ErrUnauthorized
		}
		allowed = append(allowed, lane)
	}
	registry.mu.Unlock()
	// All absent lanes were fenced by InstallSnapshot. Clear current-member
	// revocations immediately before the exact lanes resume.
	for _, peerID := range canonical {
		if !callFencePeer(registry.config.AllowPeer, peerID) {
			registry.mu.Lock()
			for _, lane := range allowed {
				lane.Pause()
			}
			registry.mu.Unlock()
			return ErrUnavailable
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if session.registry != registry || registry.session != session || registry.state != AuthoritySynchronizing || registry.authority.Load() != session.token {
		return ErrUnauthorized
	}
	// Admission remains available while AllowPeer callbacks run so work can be
	// quarantined instead of rejected during synchronization. Such admission
	// can lazily create another paused current-member lane after the first
	// snapshot above. Rebuild the resume set under the authority lock so every
	// lane that exists at the publication linearization point is opened.
	allowed = allowed[:0]
	for peerID, lane := range registry.lanes {
		if _, present := registry.members[peerID]; !present {
			return ErrUnauthorized
		}
		allowed = append(allowed, lane)
	}
	if !registry.authority.CompareAndSwap(session.token, session.token|1) {
		return ErrUnauthorized
	}
	registry.state = AuthorityReady
	for _, lane := range allowed {
		lane.Resume()
	}
	return nil
}

func callFencePeer(fence func(string), peerID string) (ok bool) {
	if fence == nil {
		return true
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	fence(peerID)
	return true
}

func (registry *Registry) AuthorityState() AuthorityState {
	if registry == nil {
		return AuthorityPaused
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.state
}

// Authorize is the immediate pre-publication/pre-success current-membership
// check supplied to operations that cross an external ownership boundary.
func (registry *Registry) Authorize(peerID string) error {
	if registry == nil {
		return ErrInvalidConfig
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	if registry.state != AuthorityReady || registry.authority.Load()&1 == 0 {
		return ErrUnavailable
	}
	if _, ok := registry.members[peerID]; !ok {
		return ErrUnauthorized
	}
	return nil
}

// ResetDynamicAuthority fences every old peer before a new authenticated
// server session is allowed to authorize peers one at a time. The caller must
// independently fence topology admission before invoking this method.
func (registry *Registry) ResetDynamicAuthority(ctx context.Context) error {
	return registry.resetDynamicAuthority(ctx, true)
}

// DropDynamicAuthority is the disconnect boundary. It destroys peer state
// without publishing a new authenticated-session capability.
func (registry *Registry) DropDynamicAuthority(ctx context.Context) error {
	return registry.resetDynamicAuthority(ctx, false)
}

func (registry *Registry) resetDynamicAuthority(ctx context.Context, reopen bool) error {
	if registry == nil || ctx == nil {
		return ErrInvalidConfig
	}
	registry.transitionMu.Lock()
	defer registry.transitionMu.Unlock()
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	if registry.blockAuthority() == 0 {
		registry.mu.Unlock()
		return ErrEpochExhausted
	}
	registry.state = AuthoritySynchronizing
	registry.session = nil
	registry.snapshotInstalled = false
	lanes := make([]*Lane, 0, len(registry.lanes))
	for _, lane := range registry.lanes {
		lane.Remove()
		lanes = append(lanes, lane)
		registry.dynamicRetiring[lane.config.PeerID] = lane
	}
	for _, lane := range registry.dynamicRetiring {
		lanes = append(lanes, lane)
	}
	registry.lanes = make(map[string]*Lane)
	registry.members = make(map[string]struct{})
	registry.memberEpochs = make(map[string]uint64)
	registry.mu.Unlock()
	for _, lane := range lanes {
		if err := lane.Join(ctx); err != nil {
			return err
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrClosed
	}
	clear(registry.dynamicRetiring)
	if !reopen {
		registry.state = AuthorityPaused
		return nil
	}
	token := registry.authority.Load()
	if token == 0 || token&1 != 0 || !registry.authority.CompareAndSwap(token, token|1) {
		return ErrUnauthorized
	}
	registry.snapshotInstalled = true
	registry.state = AuthorityReady
	return nil
}

// AddDynamicPeer installs one server-authorized exact endpoint. It does not
// infer any other member from a group roster or from a peer's address.
func (registry *Registry) AddDynamicPeer(peerID string) error {
	if registry == nil || protocol.ValidateAgentIdentity(peerID) != nil {
		return ErrInvalidConfig
	}
	registry.transitionMu.Lock()
	defer registry.transitionMu.Unlock()
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	if registry.state != AuthorityReady || !registry.authorityReady() {
		registry.mu.Unlock()
		return ErrUnavailable
	}
	if _, exists := registry.members[peerID]; exists {
		registry.mu.Unlock()
		return nil
	}
	if _, retiring := registry.dynamicRetiring[peerID]; retiring {
		registry.mu.Unlock()
		return ErrUnavailable
	}
	if len(registry.members) >= registry.config.MemberCapacity {
		registry.mu.Unlock()
		return ErrQueueFull
	}
	if registry.nextMemberEpoch == math.MaxUint64 {
		registry.mu.Unlock()
		return ErrEpochExhausted
	}
	registry.mu.Unlock()
	if !callFencePeer(registry.config.AllowPeer, peerID) {
		return ErrUnavailable
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.state != AuthorityReady || !registry.authorityReady() {
		return ErrUnavailable
	}
	registry.nextMemberEpoch++
	registry.members[peerID] = struct{}{}
	registry.memberEpochs[peerID] = registry.nextMemberEpoch
	return nil
}

// RemoveDynamicPeer installs a terminal fence, scrubs external peer state,
// and joins the exact lane before returning (the action-ACK boundary).
func (registry *Registry) RemoveDynamicPeer(ctx context.Context, peerID string) error {
	if registry == nil || ctx == nil || protocol.ValidateAgentIdentity(peerID) != nil {
		return ErrInvalidConfig
	}
	registry.transitionMu.Lock()
	defer registry.transitionMu.Unlock()
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	_, present := registry.members[peerID]
	lane := registry.lanes[peerID]
	if lane != nil {
		lane.Remove()
		delete(registry.lanes, peerID)
		registry.dynamicRetiring[peerID] = lane
	} else {
		lane = registry.dynamicRetiring[peerID]
	}
	delete(registry.members, peerID)
	delete(registry.memberEpochs, peerID)
	registry.mu.Unlock()
	if present && !callFencePeer(registry.config.FencePeer, peerID) {
		return ErrUnavailable
	}
	if lane != nil {
		if err := lane.Join(ctx); err != nil {
			return err
		}
		registry.mu.Lock()
		if registry.dynamicRetiring[peerID] == lane {
			delete(registry.dynamicRetiring, peerID)
		}
		registry.mu.Unlock()
	}
	return nil
}

// AdmitAuthenticatedInbound is called only after transport authentication has
// bound peerID. During synchronization or pause, the work is quarantined.
func (registry *Registry) AdmitAuthenticatedInbound(ctx context.Context, peerID string, work Work) (Result, error) {
	if work.Kind != WorkInbound {
		return Result{}, ErrInvalidConfig
	}
	return registry.admit(ctx, peerID, 0, work)
}

// AdmitOutbound admits local work for resolution by the next complete
// snapshot. Ready authority rejects absent destinations immediately.
func (registry *Registry) AdmitOutbound(ctx context.Context, peerID string, work Work) (Result, error) {
	if work.Kind != WorkOutbound {
		return Result{}, ErrInvalidConfig
	}
	return registry.admit(ctx, peerID, 0, work)
}

// AdmitOutboundEpoch admits outbound work only for the captured current-member
// incarnation. Removing and re-adding the same peer creates a distinct epoch,
// so stale work cannot create or poison the fresh lane.
func (registry *Registry) AdmitOutboundEpoch(ctx context.Context, peerID string, epoch uint64, work Work) (Result, error) {
	if work.Kind != WorkOutbound || epoch == 0 {
		return Result{}, ErrInvalidConfig
	}
	return registry.admit(ctx, peerID, epoch, work)
}

func (registry *Registry) admit(ctx context.Context, peerID string, expectedEpoch uint64, work Work) (Result, error) {
	if registry == nil || ctx == nil || protocol.ValidateAgentIdentity(peerID) != nil {
		return Result{}, ErrInvalidConfig
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return Result{}, ErrClosed
	}
	if registry.state == AuthorityReady || registry.snapshotInstalled && registry.session != nil {
		if _, ok := registry.members[peerID]; !ok {
			return Result{}, ErrUnauthorized
		}
	}
	if expectedEpoch != 0 && registry.memberEpochs[peerID] != expectedEpoch {
		return Result{}, ErrUnauthorized
	}
	lane := registry.lanes[peerID]
	if lane != nil && lane.State() == LaneRemoved {
		return Result{}, ErrUnavailable
	}
	if lane != nil && lane.State() == LaneClosed {
		if registry.lanes[peerID] == lane {
			delete(registry.lanes, peerID)
		}
		lane = nil
	}
	if lane == nil {
		var err error
		lane, err = NewLane(LaneConfig{
			PeerID: peerID, MailboxCapacity: registry.config.MailboxCapacity,
			ByteCapacity: registry.config.LaneByteCapacity, Budget: registry.budget,
			InitiallyPaused: registry.state != AuthorityReady, IdleTimeout: registry.config.IdleTimeout, ClosePeer: registry.config.ClosePeer,
			OnClosed: registry.laneClosed, AuthorityReady: registry.authorityReady,
		})
		if err != nil {
			return Result{}, err
		}
		registry.lanes[peerID] = lane
	}
	return lane.Admit(ctx, work)
}

func (registry *Registry) PeerEpoch(peerID string) (uint64, bool) {
	if registry == nil || peerID == "" {
		return 0, false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return 0, false
	}
	epoch := registry.memberEpochs[peerID]
	return epoch, epoch != 0
}

func (registry *Registry) ActiveLaneCount() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return len(registry.lanes)
}

func (registry *Registry) laneClosed(lane *Lane) {
	registry.mu.Lock()
	if lane != nil && registry.lanes[lane.config.PeerID] == lane {
		delete(registry.lanes, lane.config.PeerID)
	}
	delete(registry.retiring, lane)
	registry.mu.Unlock()
}

func (registry *Registry) Usage() (count, bytes uint64) {
	if registry == nil {
		return 0, 0
	}
	return registry.budget.Usage()
}

// Close fences all lanes, then joins their exact owners. A caller deadline
// bounds only the join; cleanup remains component-owned.
func (registry *Registry) Close(ctx context.Context) error {
	if registry == nil || ctx == nil {
		return ErrInvalidConfig
	}
	registry.mu.Lock()
	if registry.closeDone != nil {
		done := registry.closeDone
		registry.mu.Unlock()
		return waitRegistryClose(ctx, done)
	}
	registry.closed = true
	registry.blockAuthority()
	registry.session = nil
	registry.state = AuthorityPaused
	registry.snapshotInstalled = false
	done := make(chan struct{})
	registry.closeDone = done
	lanes := make([]*Lane, 0, len(registry.lanes))
	for _, lane := range registry.lanes {
		lane.Remove()
		lanes = append(lanes, lane)
	}
	for lane := range registry.retiring {
		lanes = append(lanes, lane)
	}
	for _, lane := range registry.dynamicRetiring {
		lanes = append(lanes, lane)
	}
	registry.lanes = make(map[string]*Lane)
	registry.retiring = make(map[*Lane]struct{})
	registry.dynamicRetiring = make(map[string]*Lane)
	registry.members = make(map[string]struct{})
	registry.memberEpochs = make(map[string]uint64)
	registry.mu.Unlock()
	go registry.finishClose(lanes, done)
	return waitRegistryClose(ctx, done)
}

func (registry *Registry) finishClose(lanes []*Lane, done chan struct{}) {
	for _, lane := range lanes {
		_ = lane.Join(context.Background())
	}
	close(done)
}

func waitRegistryClose(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
