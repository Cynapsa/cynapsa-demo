package mesh

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// AuthoritativeGroupSnapshot is one complete current-membership view returned
// by a fresh authenticated query of the server-owned group.
type AuthoritativeGroupSnapshot struct {
	MeshID     string
	ObservedAt time.Time
	Members    []Identity
}

type AuthoritativeGroupSource interface {
	// SnapshotGroup returns only a complete current-membership assembly from a
	// fresh authenticated server query. It must honor ctx and must not return a
	// partial page, roster, presence, or caller-derived membership.
	SnapshotGroup(context.Context, string) (AuthoritativeGroupSnapshot, TopologyDisposition)
}

// PeerAuthoritySnapshot is one complete current-membership authorization
// result for the authenticated local exact session and its separately verified
// mesh scope.
type PeerAuthoritySnapshot struct {
	MeshID          string
	ObservedAt      time.Time
	Local           Identity
	LocalAuthorized bool
	Peers           []PeerAuthorityResult
}

type PeerAuthorityResult struct {
	Identity   Identity
	Authorized bool
}

// PeerAuthoritySource returns complete current membership. The authority
// infers local identity and mesh from its authenticated session; meshID is a
// defensive composition binding, not wire input.
type PeerAuthoritySource interface {
	SynchronizeCurrentMembership(context.Context, string) (PeerAuthoritySnapshot, TopologyDisposition)
}

// PeerAuthorityPublisher separates authenticated synchronization from final
// local publication. Implementations keep Rank1 blocked until Topology has
// atomically installed the synchronized current membership.
type PeerAuthorityPublisher interface {
	PublishPeerAuthority(context.Context) error
	BlockPeerAuthority()
}

type TopologyDisposition uint8

const (
	TopologyReady TopologyDisposition = iota + 1
	TopologyUnavailable
	TopologyRejected
)

// Topology publishes current membership and directory data under one lock so
// policy checks and address resolution cannot observe different snapshots.
type Topology struct {
	gate             topologyGate
	authorityMu      sync.Mutex
	authorityChanged chan struct{}
	capacity         int
	meshID           string
	now              func() time.Time
	// byAgent groups the bounded exact endpoints currently authorized for one
	// logical bare agent. Each slice is sorted by Internal so request selection
	// is deterministic across installations that observe the same snapshot.
	byAgent    map[string][]Identity
	byInternal map[string]Identity
	authority  atomic.Pointer[topologyAuthorityToken]
	pending    *topologyAuthorityToken
}

// topologyAuthorityToken is a process-local install capability. Pointer
// identity prevents a stale synchronization callback from reopening
// admission after a newer fence; it is not membership history or wire state.
type topologyAuthorityToken struct{ blocked bool }

// TopologySuspension is an opaque process-local capability for one reversible
// admission fence. Pointer identity makes a hard fence or replacement win.
type TopologySuspension struct {
	topology            *Topology
	previous, suspended *topologyAuthorityToken
}

// topologyAdmission holds the topology read side across one final ownership
// transfer. A replacement cannot publish until release returns.
type topologyAdmission struct {
	topology *Topology
	once     sync.Once
}

func NewTopology(capacity int, meshID string, now func() time.Time) (*Topology, error) {
	if capacity < 1 || capacity > MaxMembershipEntries || protocol.ValidateMeshID(meshID) != nil {
		return nil, ErrInvalidConfig
	}
	if now == nil {
		now = time.Now
	}
	// QueueCapacity bounds remote peers. The authenticated local identity is
	// part of every trusted snapshot but does not consume a remote-peer slot.
	snapshotCapacity := capacity
	if snapshotCapacity < MaxMembershipEntries {
		snapshotCapacity++
	}
	topology := &Topology{
		capacity:         snapshotCapacity,
		meshID:           meshID,
		now:              now,
		byAgent:          make(map[string][]Identity, snapshotCapacity),
		byInternal:       make(map[string]Identity, snapshotCapacity),
		authorityChanged: make(chan struct{}),
	}
	topology.authority.Store(&topologyAuthorityToken{blocked: true})
	return topology, nil
}

func (topology *Topology) Replace(snapshot AuthoritativeGroupSnapshot) error {
	return topology.replace(context.Background(), snapshot)
}

func (topology *Topology) replace(ctx context.Context, snapshot AuthoritativeGroupSnapshot) error {
	if topology == nil || snapshot.MeshID != topology.meshID || snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.Location() != time.UTC || len(snapshot.Members) > topology.capacity {
		return ErrSnapshotInvalid
	}
	if ctx == nil {
		return ErrSnapshotInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	byAgent := make(map[string][]Identity, len(snapshot.Members))
	byInternal := make(map[string]Identity, len(snapshot.Members))
	previousInternal := ""
	for _, candidate := range snapshot.Members {
		identity, err := NewIdentity(candidate.AgentID, candidate.Internal)
		if err != nil || strings.Contains(identity.AgentID, "/") || !validAuthorityIdentity(identity) || (previousInternal != "" && identity.Internal <= previousInternal) {
			return ErrSnapshotInvalid
		}
		previousInternal = identity.Internal
		if _, exists := byInternal[identity.Internal]; exists {
			return ErrSnapshotInvalid
		}
		byAgent[identity.AgentID] = append(byAgent[identity.AgentID], identity)
		byInternal[identity.Internal] = identity
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	expected := topology.authority.Load()
	if err := topology.gate.lock(ctx); err != nil {
		return err
	}
	defer topology.gate.unlock()
	// A newer writer may have installed a blocked publication token while this
	// replacement was waiting for the gate. Reject that stale writer before it
	// can overwrite the newer maps or clear its pending publication.
	if topology.authority.Load() != expected {
		return ErrSnapshotStale
	}
	now := topology.now()
	if snapshot.ObservedAt.After(now) {
		return ErrSnapshotInvalid
	}
	// Cancellation that won while this writer was queued must be observed
	// before publishing the replacement membership.
	if err := ctx.Err(); err != nil {
		return err
	}
	topology.byAgent = byAgent
	topology.byInternal = byInternal
	if !topology.authority.CompareAndSwap(expected, &topologyAuthorityToken{}) {
		return ErrSnapshotStale
	}
	topology.pending = nil
	topology.notifyAuthorityChanged()
	return nil
}

func (topology *Topology) Refresh(ctx context.Context, source AuthoritativeGroupSource) error {
	if topology == nil || ctx == nil || source == nil {
		return ErrSnapshotInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, disposition := source.SnapshotGroup(ctx, topology.meshID)
	if err := ctx.Err(); err != nil {
		return err
	}
	switch disposition {
	case TopologyReady:
		return topology.replace(ctx, snapshot)
	case TopologyUnavailable:
		return ErrSnapshotStale
	case TopologyRejected:
		return ErrSnapshotInvalid
	default:
		return ErrSnapshotInvalid
	}
}

// Block removes all usable authority immediately while retaining the bounded
// working-set identities solely as input to the next authenticated sync.
func (topology *Topology) Block() {
	if topology == nil {
		return
	}
	topology.BlockAdmission()
	_ = topology.gate.lock(context.Background())
	topology.pending = nil
	topology.gate.unlock()
}

// BlockAdmission is the constant-time security fence used directly from the
// authenticated notice/disconnect callback before any asynchronous cleanup.
func (topology *Topology) BlockAdmission() {
	if topology != nil {
		topology.authority.Store(&topologyAuthorityToken{blocked: true})
		topology.notifyAuthorityChanged()
	}
}

// SuspendAdmission fences lookups without discarding the current snapshot. It
// is used only while an authenticated XMPP logical session is being resumed.
func (topology *Topology) SuspendAdmission() *TopologySuspension {
	if topology == nil {
		return nil
	}
	for {
		previous := topology.authority.Load()
		if previous == nil || previous.blocked {
			return nil
		}
		suspended := &topologyAuthorityToken{blocked: true}
		if topology.authority.CompareAndSwap(previous, suspended) {
			topology.notifyAuthorityChanged()
			return &TopologySuspension{topology: topology, previous: previous, suspended: suspended}
		}
	}
}

// ResumeAdmission restores only the exact still-current suspension.
func (topology *Topology) ResumeAdmission(ctx context.Context, suspension *TopologySuspension) error {
	if topology == nil || ctx == nil || suspension == nil || suspension.topology != topology || suspension.previous == nil || suspension.suspended == nil {
		return ErrSnapshotInvalid
	}
	if err := topology.gate.lock(ctx); err != nil {
		return err
	}
	defer topology.gate.unlock()
	if !topology.authority.CompareAndSwap(suspension.suspended, suspension.previous) {
		return ErrSnapshotStale
	}
	topology.notifyAuthorityChanged()
	return nil
}

func (topology *Topology) authorityBlocked() bool {
	if topology == nil {
		return true
	}
	state := topology.authority.Load()
	return state == nil || state.blocked
}

// waitForCurrentAuthority pauses caller-owned work while the authenticated
// server session is re-establishing current membership. The condition is
// rechecked under authorityMu so publication cannot be missed between the
// blocked observation and waiting on its transition notification.
func (topology *Topology) waitForCurrentAuthority(ctx context.Context) error {
	if topology == nil {
		return ErrSnapshotStale
	}
	if ctx == nil {
		return context.Canceled
	}
	for topology.authorityBlocked() {
		topology.authorityMu.Lock()
		changed := topology.authorityChanged
		if !topology.authorityBlocked() {
			topology.authorityMu.Unlock()
			return nil
		}
		topology.authorityMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (topology *Topology) notifyAuthorityChanged() {
	if topology == nil {
		return
	}
	topology.authorityMu.Lock()
	if topology.authorityChanged != nil {
		close(topology.authorityChanged)
	}
	topology.authorityChanged = make(chan struct{})
	topology.authorityMu.Unlock()
}

// ApplyPeerAuthority atomically replaces the current authority snapshot. An
// omitted or unauthorized peer is absent from the newly published topology.
func (topology *Topology) ApplyPeerAuthority(snapshot PeerAuthoritySnapshot) error {
	return topology.applyPeerAuthorityIf(snapshot, nil)
}

// applyPeerAuthorityIf stages a replacement only while the caller's external
// authority transaction remains current. The callback is checked under the
// topology writer gate, and the authority-token CAS makes a concurrent fence
// win even if it follows that check.
func (topology *Topology) applyPeerAuthorityIf(snapshot PeerAuthoritySnapshot, current func() bool) error {
	if topology == nil || snapshot.MeshID != topology.meshID || snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.Location() != time.UTC || !snapshot.LocalAuthorized || len(snapshot.Peers) > topology.capacity-1 || protocol.ValidateAgentIdentity(snapshot.Local.AgentID) != nil || !validAuthorityIdentity(snapshot.Local) {
		return ErrSnapshotInvalid
	}
	local, err := NewIdentity(snapshot.Local.AgentID, snapshot.Local.Internal)
	if err != nil {
		return ErrSnapshotInvalid
	}
	seenInternal := map[string]bool{local.Internal: true}
	ownedPeers := make([]PeerAuthorityResult, 0, len(snapshot.Peers))
	for _, result := range snapshot.Peers {
		identity, identityErr := NewIdentity(result.Identity.AgentID, result.Identity.Internal)
		if identityErr != nil || !validAuthorityIdentity(identity) || identity.Internal == local.Internal || seenInternal[identity.Internal] {
			return ErrSnapshotInvalid
		}
		seenInternal[identity.Internal] = true
		ownedPeers = append(ownedPeers, PeerAuthorityResult{Identity: identity, Authorized: result.Authorized})
	}
	expected := topology.authority.Load()
	_ = topology.gate.lock(context.Background())
	defer topology.gate.unlock()
	if current != nil && !current() {
		return ErrSnapshotStale
	}
	now := topology.now()
	if snapshot.ObservedAt.After(now) {
		return ErrSnapshotInvalid
	}
	install := &topologyAuthorityToken{blocked: true}
	if !topology.authority.CompareAndSwap(expected, install) {
		return ErrSnapshotStale
	}
	topology.notifyAuthorityChanged()
	byAgent := make(map[string][]Identity, len(snapshot.Peers)+1)
	byInternal := make(map[string]Identity, len(snapshot.Peers)+1)
	byAgent[local.AgentID] = []Identity{local}
	byInternal[local.Internal] = local
	for _, result := range ownedPeers {
		if !result.Authorized {
			continue
		}
		byAgent[result.Identity.AgentID] = append(byAgent[result.Identity.AgentID], result.Identity)
		byInternal[result.Identity.Internal] = result.Identity
	}
	for agentID := range byAgent {
		sort.Slice(byAgent[agentID], func(i, j int) bool { return byAgent[agentID][i].Internal < byAgent[agentID][j].Internal })
	}
	topology.byAgent, topology.byInternal = byAgent, byInternal
	topology.pending = install
	return nil
}

// validAuthorityIdentity checks only exact endpoint shape. The resource is
// opaque session addressing; the enclosing authenticated snapshot supplies
// mesh authority.
func validAuthorityIdentity(identity Identity) bool {
	return strings.HasPrefix(identity.Internal, identity.AgentID+"/") && len(identity.Internal) > len(identity.AgentID)+1 && !strings.Contains(identity.Internal[len(identity.AgentID)+1:], "/")
}

// PublishPeerAuthority opens admission only for the exact process-local
// installation most recently completed by ApplyPeerAuthority.
func (topology *Topology) PublishPeerAuthority() error {
	return topology.publishPeerAuthorityIf(nil)
}

func (topology *Topology) publishPeerAuthorityIf(current func() bool) error {
	if topology == nil {
		return ErrSnapshotInvalid
	}
	_ = topology.gate.lock(context.Background())
	defer topology.gate.unlock()
	install := topology.pending
	if install == nil || current != nil && !current() || !topology.authority.CompareAndSwap(install, &topologyAuthorityToken{}) {
		return ErrSnapshotStale
	}
	topology.notifyAuthorityChanged()
	topology.pending = nil
	return nil
}

func (topology *Topology) Lookup(agentID string) (Identity, error) {
	if protocol.ValidateAgentIdentity(agentID) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	if topology == nil {
		return Identity{}, ErrIdentityUnknown
	}
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	topology.gate.readLock()
	defer topology.gate.readUnlock()
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	identities := topology.byAgent[agentID]
	if len(identities) == 0 {
		return Identity{}, ErrIdentityUnknown
	}
	return identities[0], nil
}

// ResolveAgent returns one peer after authorizing it and the local bound
// identity under a single immutable current-membership snapshot.
func (topology *Topology) ResolveAgent(meshID, localInternal, agentID string) (Identity, error) {
	if protocol.ValidateAgentIdentity(localInternal) != nil || protocol.ValidateAgentIdentity(agentID) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	if topology == nil || meshID != topology.meshID {
		return Identity{}, ErrIdentityUnknown
	}
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	topology.gate.readLock()
	defer topology.gate.readUnlock()
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	if _, ok := topology.byInternal[localInternal]; !ok {
		return Identity{}, ErrMembershipMissing
	}
	identities := topology.byAgent[agentID]
	if len(identities) == 0 {
		return Identity{}, ErrIdentityUnknown
	}
	for _, identity := range identities {
		if identity.Internal != localInternal {
			return identity, nil
		}
	}
	return Identity{}, ErrIdentityUnknown
}

// ResolveAgentContext waits through a temporary authenticated-authority fence
// and resolves only from the next current snapshot. A peer removed while the
// transport was unavailable therefore remains rejected.
func (topology *Topology) ResolveAgentContext(ctx context.Context, meshID, localInternal, agentID string) (Identity, error) {
	if protocol.ValidateAgentIdentity(localInternal) != nil || protocol.ValidateAgentIdentity(agentID) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	if topology == nil || ctx == nil || meshID != topology.meshID {
		return Identity{}, ErrIdentityUnknown
	}
	for {
		if err := topology.waitForCurrentAuthority(ctx); err != nil {
			return Identity{}, err
		}
		if err := topology.gate.readLockContext(ctx); err != nil {
			return Identity{}, err
		}
		if topology.authorityBlocked() {
			topology.gate.readUnlock()
			continue
		}
		if _, ok := topology.byInternal[localInternal]; !ok {
			topology.gate.readUnlock()
			return Identity{}, ErrMembershipMissing
		}
		identities := topology.byAgent[agentID]
		topology.gate.readUnlock()
		for _, identity := range identities {
			if identity.Internal != localInternal {
				return identity, nil
			}
		}
		if len(identities) == 0 {
			return Identity{}, ErrIdentityUnknown
		}
		return Identity{}, ErrIdentityUnknown
	}
}

// ResolveAgentEndpointsContext returns an owned, deterministically ordered
// snapshot of every exact endpoint currently authorized for agentID in this
// mesh. The caller's exact local endpoint is omitted while sibling
// installations of the same logical agent remain eligible.
func (topology *Topology) ResolveAgentEndpointsContext(ctx context.Context, meshID, localInternal, agentID string) ([]Identity, error) {
	if protocol.ValidateAgentIdentity(localInternal) != nil || protocol.ValidateAgentIdentity(agentID) != nil {
		return nil, ErrInvalidIdentity
	}
	if topology == nil || ctx == nil || meshID != topology.meshID {
		return nil, ErrIdentityUnknown
	}
	for {
		if err := topology.waitForCurrentAuthority(ctx); err != nil {
			return nil, err
		}
		if err := topology.gate.readLockContext(ctx); err != nil {
			return nil, err
		}
		if topology.authorityBlocked() {
			topology.gate.readUnlock()
			continue
		}
		if _, ok := topology.byInternal[localInternal]; !ok {
			topology.gate.readUnlock()
			return nil, ErrMembershipMissing
		}
		current := topology.byAgent[agentID]
		endpoints := make([]Identity, 0, len(current))
		for _, identity := range current {
			if identity.Internal != localInternal {
				endpoints = append(endpoints, identity)
			}
		}
		topology.gate.readUnlock()
		if len(endpoints) == 0 {
			return nil, ErrIdentityUnknown
		}
		return endpoints, nil
	}
}

func (topology *Topology) LookupInternal(internal string) (Identity, error) {
	if protocol.ValidateAgentIdentity(internal) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	if topology == nil {
		return Identity{}, ErrIdentityUnknown
	}
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	topology.gate.readLock()
	defer topology.gate.readUnlock()
	if topology.authorityBlocked() {
		return Identity{}, ErrSnapshotStale
	}
	identity, ok := topology.byInternal[internal]
	if !ok {
		return Identity{}, ErrIdentityUnknown
	}
	return identity, nil
}

func (topology *Topology) RequireAll(meshID string, agentIDs ...string) error {
	if topology == nil || meshID != topology.meshID {
		return ErrMembershipMissing
	}
	if topology.authorityBlocked() {
		return ErrSnapshotStale
	}
	topology.gate.readLock()
	defer topology.gate.readUnlock()
	if topology.authorityBlocked() {
		return ErrSnapshotStale
	}
	for _, identity := range agentIDs {
		if _, ok := topology.byInternal[identity]; !ok {
			return ErrMembershipMissing
		}
	}
	return nil
}

// memberCountExcluding returns the number of exact identities other than the
// authenticated local full endpoint in
// current complete membership. The exclusion is applied only when that
// identity is actually present, so a removal snapshot cannot accidentally
// hide a remaining peer. Authorization stays admission-based.
func (topology *Topology) memberCountExcluding(localInternal string) int {
	if topology == nil {
		return 0
	}
	topology.gate.readLock()
	defer topology.gate.readUnlock()
	count := len(topology.byInternal)
	if _, present := topology.byInternal[localInternal]; present {
		count--
	}
	return count
}

// acquireAdmission validates current membership for all participants, then
// keeps the snapshot read-locked until ownership transfer completes.
func (topology *Topology) acquireAdmission(meshID string, internalIdentities ...string) (*topologyAdmission, error) {
	return topology.acquireAdmissionContext(context.Background(), meshID, internalIdentities...)
}

func (topology *Topology) acquireAdmissionContext(ctx context.Context, meshID string, internalIdentities ...string) (*topologyAdmission, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if topology == nil || meshID != topology.meshID {
		return nil, ErrMembershipMissing
	}
	for {
		if err := topology.waitForCurrentAuthority(ctx); err != nil {
			return nil, err
		}
		if err := topology.gate.readLockContext(ctx); err != nil {
			return nil, err
		}
		if topology.authorityBlocked() {
			topology.gate.readUnlock()
			continue
		}
		break
	}
	if err := ctx.Err(); err != nil {
		topology.gate.readUnlock()
		return nil, err
	}
	for _, identity := range internalIdentities {
		if _, ok := topology.byInternal[identity]; !ok {
			topology.gate.readUnlock()
			return nil, ErrMembershipMissing
		}
	}
	return &topologyAdmission{topology: topology}, nil
}

func (admission *topologyAdmission) release() {
	if admission == nil || admission.topology == nil {
		return
	}
	admission.once.Do(func() { admission.topology.gate.readUnlock() })
}

// identity returns an exact member from the snapshot protected by this
// admission. Callers must retain the admission until the returned value has
// been copied into their own owned state.
func (admission *topologyAdmission) identity(internal string) (Identity, error) {
	if protocol.ValidateAgentIdentity(internal) != nil {
		return Identity{}, ErrInvalidIdentity
	}
	if admission == nil || admission.topology == nil {
		return Identity{}, ErrIdentityUnknown
	}
	identity, ok := admission.topology.byInternal[internal]
	if !ok {
		return Identity{}, ErrIdentityUnknown
	}
	return identity, nil
}
