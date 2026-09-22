package transport

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const MaximumReceiveQueue = 65536

// Received preserves the private ingress carrier only until peer policy has
// observed it. The envelope still enters the common Pod 3 validation path.
type Received struct {
	Kind             Kind
	Envelope         protocol.Envelope
	Authentication   *LiveAuthentication
	authorityEpoch   uint64
	handoffEpoch     uint64
	authorityCapture liveAuthorityCapture
	liveSource       Transport
	pausedAdmission  bool
	receiptRoute     *liveReceiptRoute
}

// LiveAuthentication is immutable local evidence exported only by an
// authenticated direct transport. Membership authority is checked separately
// by the peer registry immediately before application publication.
type LiveAuthentication struct {
	Peer, MeshID   string
	ChannelBinding [sha256.Size]byte
}

type AuthenticatedReceived struct {
	Envelope       protocol.Envelope
	Authentication LiveAuthentication
	// LiveAuthorityEpoch is adapter-private provenance consumed by the
	// authenticated live adapter before it transfers ingress to Manager.
	LiveAuthorityEpoch uint64
	// InboundLease is private queue ownership used only inside a concrete
	// adapter. It is released before Manager receives the value.
	InboundLease     *InboundLease
	authorityCapture liveAuthorityCapture
}

// liveAuthorityCapture is process-private proof that an authenticated input
// was captured while one exact live-authority epoch was current, or while its
// immediately following fence was still closed. It is never serialized and
// carries no application membership generation.
type liveAuthorityCapture struct {
	gate  *LiveAuthorityGate
	epoch uint64
}

// CaptureLiveAuthority stamps adapter-owned authenticated input at the point
// it enters the adapter's bounded receive ownership. A retained adapter can
// later transfer that exact old-epoch value after publication without opening
// admission to values first captured after the fence had already reopened.
func (received *AuthenticatedReceived) CaptureLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	if received == nil {
		return false
	}
	capture, ok := gate.capture(epoch)
	if !ok {
		return false
	}
	received.authorityCapture = capture
	return true
}

type AuthenticatedTransport interface {
	Transport
	ReceiveAuthenticated(context.Context) (AuthenticatedReceived, error)
}

// LiveAuthorityCapturedIngress marks a production authenticated transport
// that stamps every bounded inbound value with CaptureLiveAuthority. Manager
// then requires that unforgeable process-local proof for an old-epoch retained
// handoff. Dependency-neutral legacy adapters retain the narrower in-flight
// receive-call handoff used by Manager tests.
type LiveAuthorityCapturedIngress interface {
	LiveAuthorityCapturedIngress()
}

// PendingLiveReceipt is one bounded exact-session receipt registration. Wait
// reports true only for an authenticated exact-match receipt. Close retires
// the registration and makes late or duplicate receipts inert.
type PendingLiveReceipt interface {
	Binding() LiveReceipt
	Done() <-chan bool
	Close()
}

// LiveReceiptTransport is implemented only by an authenticated Rank1 link.
// SendTracked registers before writing, so an immediate receipt cannot be
// missed. SendReceipt writes on this exact transport instance.
type LiveReceiptTransport interface {
	AuthenticatedTransport
	SendTracked(context.Context, protocol.Envelope) (PendingLiveReceipt, error)
	SendReceipt(context.Context, LiveReceipt) error
}

type liveReceiptRoute struct {
	source  LiveReceiptTransport
	binding LiveReceipt
}

// InboundLiveReceipt is the one-shot identity-only route for an admitted
// inbound envelope. It retains no application payload.
type InboundLiveReceipt interface {
	Binding() LiveReceipt
	Acknowledge(context.Context) error
	Close()
}

type inboundLiveReceipt struct {
	mu             sync.Mutex
	manager        *Manager
	source         LiveReceiptTransport
	binding        LiveReceipt
	authorityEpoch uint64
	rebindCurrent  bool
	closed         bool
}

func (receipt *inboundLiveReceipt) Binding() LiveReceipt {
	if receipt == nil {
		return LiveReceipt{}
	}
	receipt.mu.Lock()
	binding := receipt.binding
	receipt.mu.Unlock()
	return binding
}

func (receipt *inboundLiveReceipt) Acknowledge(ctx context.Context) error {
	if receipt == nil || ctx == nil {
		return ErrInvalidConfig
	}
	receipt.mu.Lock()
	if receipt.closed || receipt.source == nil {
		receipt.mu.Unlock()
		return ErrClosed
	}
	receipt.closed = true
	manager, source, binding, epoch, rebind := receipt.manager, receipt.source, receipt.binding, receipt.authorityEpoch, receipt.rebindCurrent
	receipt.manager = nil
	receipt.source = nil
	receipt.binding = LiveReceipt{}
	receipt.authorityEpoch = 0
	receipt.rebindCurrent = false
	receipt.mu.Unlock()
	if manager == nil {
		return ErrClosed
	}
	return manager.acknowledgeInbound(ctx, source, binding, epoch, rebind)
}

func (receipt *inboundLiveReceipt) Close() {
	if receipt == nil {
		return
	}
	receipt.mu.Lock()
	receipt.closed = true
	receipt.manager = nil
	receipt.source = nil
	receipt.binding = LiveReceipt{}
	receipt.authorityEpoch = 0
	receipt.rebindCurrent = false
	receipt.mu.Unlock()
}

// ShutdownOwnedDiscarder clears bounded receive ownership only after the
// transport has terminally joined all of its producers. Manager uses this
// private internal seam during global shutdown and after an exact detached
// receiver has finished draining already-authenticated records.
type ShutdownOwnedDiscarder interface {
	DiscardShutdownOwned()
}

// LiveAuthorityBlocker is a dependency-free final fence for a live link that
// is being terminally retired. Retained links use LiveAuthorityRebinder and
// never receive this irreversible callback.
type LiveAuthorityBlocker interface {
	BlockLiveAuthority()
}

type LiveAuthoritySignal interface {
	LiveAuthorityDone() <-chan struct{}
}

// LiveAuthorityGate is the shared O(1) authority epoch checked by Manager and
// every installed Link. Blocking increments the epoch; publishing can reopen
// only that exact epoch, so links captured before a fence stay unusable.
type LiveAuthorityGate struct {
	epoch     atomic.Uint64
	blocked   atomic.Bool
	exhausted atomic.Bool
	signal    atomic.Pointer[liveAuthoritySignal]
}

type liveAuthoritySignal struct {
	done  chan struct{}
	ready chan struct{}
}

func newLiveAuthorityGate() *LiveAuthorityGate {
	gate := &LiveAuthorityGate{}
	gate.epoch.Store(1)
	ready := make(chan struct{})
	close(ready)
	gate.signal.Store(&liveAuthoritySignal{done: make(chan struct{}), ready: ready})
	return gate
}

func (gate *LiveAuthorityGate) Block() uint64 {
	gate.blocked.Store(true)
	for {
		current := gate.epoch.Load()
		if current == ^uint64(0) {
			if gate.exhausted.CompareAndSwap(false, true) {
				previous := gate.signal.Swap(&liveAuthoritySignal{done: make(chan struct{}), ready: make(chan struct{})})
				if previous != nil {
					close(previous.done)
				}
			}
			return 0
		}
		if gate.epoch.CompareAndSwap(current, current+1) {
			previous := gate.signal.Swap(&liveAuthoritySignal{done: make(chan struct{}), ready: make(chan struct{})})
			if previous != nil {
				close(previous.done)
			}
			return current + 1
		}
	}
}

// Loss returns a dependency-free signal closed by the first authority block
// after epoch was admitted. A stale epoch receives an already-closed signal.
func (gate *LiveAuthorityGate) Loss(epoch uint64) <-chan struct{} {
	if gate != nil && epoch != 0 && gate.epoch.Load() == epoch {
		if signal := gate.signal.Load(); signal != nil && gate.epoch.Load() == epoch {
			return signal.done
		}
	}
	done := make(chan struct{})
	close(done)
	return done
}

// Ready closes when the exact blocked epoch is published. Receiver loops may
// therefore be installed before publication without observing or relabeling
// adapter-owned ingress while authority remains fenced.
func (gate *LiveAuthorityGate) Ready(epoch uint64) <-chan struct{} {
	if gate != nil && epoch != 0 && gate.epoch.Load() == epoch {
		if signal := gate.signal.Load(); signal != nil && gate.epoch.Load() == epoch {
			return signal.ready
		}
	}
	ready := make(chan struct{})
	close(ready)
	return ready
}

// Rebindable reports whether an already-started live adapter may discard its
// old-epoch ingress and bind to this exact, still-blocked recovery epoch.
func (gate *LiveAuthorityGate) Rebindable(epoch uint64) bool {
	return gate != nil && epoch != 0 && !gate.exhausted.Load() && gate.blocked.Load() && gate.epoch.Load() == epoch
}

func (gate *LiveAuthorityGate) Admit(epoch uint64) bool {
	return gate != nil && epoch != 0 && !gate.exhausted.Load() && !gate.blocked.Load() && gate.epoch.Load() == epoch
}

func (gate *LiveAuthorityGate) Publish(epoch uint64) bool {
	if gate == nil || epoch == 0 || gate.exhausted.Load() || gate.epoch.Load() != epoch {
		return false
	}
	signal := gate.signal.Load()
	if signal == nil || gate.epoch.Load() != epoch || !gate.blocked.CompareAndSwap(true, false) {
		return false
	}
	close(signal.ready)
	return true
}

func (gate *LiveAuthorityGate) capture(epoch uint64) (liveAuthorityCapture, bool) {
	if gate == nil || epoch == 0 || gate.exhausted.Load() {
		return liveAuthorityCapture{}, false
	}
	current := gate.epoch.Load()
	if current == epoch {
		return liveAuthorityCapture{gate: gate, epoch: epoch}, true
	}
	// An adapter can finish authenticating an input concurrently with Block.
	// The immediately preceding epoch remains capturable only while that exact
	// fence is closed; publication permanently ends this capture window.
	if current > 1 && epoch == current-1 && gate.blocked.Load() && gate.epoch.Load() == current {
		return liveAuthorityCapture{gate: gate, epoch: epoch}, true
	}
	return liveAuthorityCapture{}, false
}

type LiveAuthorityBinder interface {
	BindLiveAuthority(*LiveAuthorityGate, uint64) bool
}

// LiveAuthorityRebinder is implemented by a retained live adapter or by a
// Manager-owned adapter whose Start is still in progress. It binds future
// input to the new blocked epoch without relabeling already-authenticated input
// and must not close the underlying transport.
type LiveAuthorityRebinder interface {
	RebindLiveAuthority(*LiveAuthorityGate, uint64) bool
}

// Manager owns the always-active durable path and independently replaceable
// live links keyed by authenticated peer identity. The unkeyed live slot is
// retained only for small dependency-neutral adapters and tests.
type Manager struct {
	mu                         sync.Mutex
	durable                    Transport
	defaultLive                Transport
	live                       map[string]Transport
	inbound                    chan Received
	ctx                        context.Context
	cancel                     context.CancelFunc
	started                    bool
	starting                   bool
	startDone                  chan struct{}
	startErr                   error
	startTerminal              bool
	startCancel                context.CancelFunc
	generation                 uint64
	liveAuthorityEpoch         uint64
	liveAuthorityPreparedEpoch uint64
	liveAuthorityBlocked       bool
	liveInstallationEpoch      uint64
	liveInstallationBlocked    bool
	liveInstallationExhausted  bool
	liveAuthoritySetupEpoch    uint64
	liveAuthorityReconciling   bool
	liveAuthorityReconcileDone chan struct{}
	liveAuthorityChanged       chan struct{}
	quarantined                []Transport
	discardOnly                []discardOwnership
	blockedLivePeers           []string
	liveAuthority              *LiveAuthorityGate
	closed                     bool
	closeDone                  chan struct{}
	closeErr                   error
	ownedAdapters              map[transportIdentity]Transport
	adapterMetadata            map[transportIdentity]transportMetadata
	receiverStates             map[transportIdentity]receiverState
	pendingLive                map[transportIdentity]*pendingLiveInstallation
	operations                 sync.WaitGroup
	receivers                  sync.WaitGroup
	discardRetirements         sync.WaitGroup
}

type transportIdentity struct {
	typeOf  reflect.Type
	pointer uintptr
}

type transportMetadata struct {
	kind Kind
}

type adapterStartAttempt struct {
	adapter   Transport
	attempted bool
	started   bool
}

type receiverState struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type detachedAdapter struct {
	adapter      Transport
	receiverDone <-chan struct{}
}

// quarantineReadyAdapter is an adapter whose optional dependency-local live
// authority fence has already been closed. Construct it only through
// prepareAuthorityQuarantine, which deliberately runs outside Manager.mu.
type quarantineReadyAdapter struct {
	adapter Transport
}

type retainedLiveAdapter struct {
	peer         string
	adapter      Transport
	receiverDone <-chan struct{}
}

// pendingLiveInstallation is Manager-owned from the moment a recovered live
// adapter is admitted until Start either fails or the adapter is atomically
// published. Keeping unpublished candidates in the authority registry lets a
// membership fence retain or retire them without opening a stale-epoch gap.
type pendingLiveInstallation struct {
	peer              string
	adapter           Transport
	authorityEpoch    uint64
	installationEpoch uint64
}

type discardOwnership struct {
	adapter      Transport
	receiverDone <-chan struct{}
}

// managerLiveOperation is one lifecycle/authority admission shared by live
// tracked sends and receipt writes. Workstream C can replace its WaitGroup
// ownership with admitOperationLocked/finishOperation without changing the
// exact source/epoch validation or revocable context contract.
type managerLiveOperation struct {
	manager        *Manager
	source         LiveReceiptTransport
	peer           string
	authorityEpoch uint64
	lifetime       context.Context
	authorityLoss  <-chan struct{}
}

func (m *Manager) admitLiveOperationLocked(source LiveReceiptTransport, peer string, authorityEpoch uint64) *managerLiveOperation {
	if !m.liveSourceCurrentLocked(source, peer, authorityEpoch) {
		return nil
	}
	if !m.admitOperationLocked() {
		return nil
	}
	return &managerLiveOperation{
		manager: m, source: source, peer: peer,
		authorityEpoch: authorityEpoch, lifetime: m.ctx,
		authorityLoss: m.liveAuthority.Loss(authorityEpoch),
	}
}

func (operation *managerLiveOperation) revocableContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stopLifetime := context.AfterFunc(operation.lifetime, cancel)
	authorityObserved := make(chan struct{})
	go func() {
		select {
		case <-operation.authorityLoss:
			cancel()
		case <-ctx.Done():
		}
		close(authorityObserved)
	}()
	return ctx, func() {
		cancel()
		stopLifetime()
		<-authorityObserved
	}
}

func (operation *managerLiveOperation) current() bool {
	if operation == nil || operation.manager == nil {
		return false
	}
	m := operation.manager
	m.mu.Lock()
	current := m.liveSourceCurrentLocked(operation.source, operation.peer, operation.authorityEpoch)
	m.mu.Unlock()
	return current
}

func (operation *managerLiveOperation) finish() {
	if operation == nil || operation.manager == nil {
		return
	}
	m := operation.manager
	m.finishOperation()
}

func NewManager(receiveCapacity int, adapters ...Transport) (*Manager, error) {
	if receiveCapacity <= 0 || receiveCapacity > MaximumReceiveQueue || len(adapters) == 0 || len(adapters) > 2 {
		return nil, ErrInvalidConfig
	}
	m := &Manager{inbound: make(chan Received, receiveCapacity), live: make(map[string]Transport), liveAuthorityEpoch: 1, liveInstallationEpoch: 1, liveAuthority: newLiveAuthorityGate(), liveAuthorityChanged: make(chan struct{}), ownedAdapters: make(map[transportIdentity]Transport), adapterMetadata: make(map[transportIdentity]transportMetadata), receiverStates: make(map[transportIdentity]receiverState), pendingLive: make(map[transportIdentity]*pendingLiveInstallation)}
	for _, adapter := range adapters {
		if _, ok := identityOfTransport(adapter); !ok {
			return nil, ErrInvalidConfig
		}
		metadata, ok := metadataForTransport(adapter)
		if !ok {
			return nil, ErrInvalidConfig
		}
		if !m.claimAdapterLocked(adapter, metadata) {
			return nil, ErrInvalidConfig
		}
		switch metadata.kind {
		case KindDurable:
			if m.durable != nil {
				return nil, ErrInvalidConfig
			}
			m.durable = adapter
		case KindLive:
			if m.defaultLive != nil {
				return nil, ErrInvalidConfig
			}
			m.defaultLive = adapter
		default:
			return nil, ErrInvalidConfig
		}
	}
	return m, nil
}

// NewLiveManager creates a started-empty registry for peer-scoped Rank1 links.
// It owns only links installed later and never owns or competes with Rank2.
func NewLiveManager(receiveCapacity int) (*Manager, error) {
	if receiveCapacity <= 0 || receiveCapacity > MaximumReceiveQueue {
		return nil, ErrInvalidConfig
	}
	return &Manager{inbound: make(chan Received, receiveCapacity), live: make(map[string]Transport), liveAuthorityEpoch: 1, liveInstallationEpoch: 1, liveAuthority: newLiveAuthorityGate(), liveAuthorityChanged: make(chan struct{}), ownedAdapters: make(map[transportIdentity]Transport), adapterMetadata: make(map[transportIdentity]transportMetadata), receiverStates: make(map[transportIdentity]receiverState), pendingLive: make(map[transportIdentity]*pendingLiveInstallation)}, nil
}

func (m *Manager) signalLiveAuthorityChangedLocked() {
	if m.liveAuthorityChanged != nil {
		close(m.liveAuthorityChanged)
	}
	m.liveAuthorityChanged = make(chan struct{})
}

// metadataForTransport is called only before Manager ownership is published.
func metadataForTransport(adapter Transport) (metadata transportMetadata, valid bool) {
	defer func() {
		if recover() != nil {
			metadata = transportMetadata{}
			valid = false
		}
	}()
	metadata.kind = adapter.Kind()
	return metadata, true
}

func identityOfTransport(adapter Transport) (transportIdentity, bool) {
	if adapter == nil {
		return transportIdentity{}, false
	}
	value := reflect.ValueOf(adapter)
	// Stateful transports have pointer identity. Rejecting value transports is
	// conservative and avoids interface equality/hash panics for dynamic values
	// containing slices, maps, funcs, or nested non-comparable interfaces.
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return transportIdentity{}, false
	}
	return transportIdentity{typeOf: value.Type(), pointer: value.Pointer()}, true
}

func (m *Manager) claimAdapterLocked(adapter Transport, metadata transportMetadata) bool {
	identity, ok := identityOfTransport(adapter)
	if !ok {
		return false
	}
	if m.ownedAdapters == nil {
		m.ownedAdapters = make(map[transportIdentity]Transport)
	}
	if m.adapterMetadata == nil {
		m.adapterMetadata = make(map[transportIdentity]transportMetadata)
	}
	if _, exists := m.ownedAdapters[identity]; exists {
		return false
	}
	m.ownedAdapters[identity] = adapter
	m.adapterMetadata[identity] = metadata
	return true
}

func (m *Manager) metadataLocked(adapter Transport) (transportMetadata, bool) {
	identity, ok := identityOfTransport(adapter)
	if !ok {
		return transportMetadata{}, false
	}
	metadata, exists := m.adapterMetadata[identity]
	return metadata, exists
}

func (m *Manager) ownsAdapterLocked(adapter Transport) bool {
	identity, ok := identityOfTransport(adapter)
	if !ok {
		return false
	}
	_, exists := m.ownedAdapters[identity]
	return exists
}

func (m *Manager) releaseAdapterLocked(adapter Transport) {
	identity, ok := identityOfTransport(adapter)
	if ok {
		delete(m.ownedAdapters, identity)
		delete(m.adapterMetadata, identity)
	}
}

// Start establishes the durable path first. A configured live path is
// optional: its failure never tears down a successfully started durable path.
// Concurrent calls join the same attempt and dependency calls run unlocked.
func (m *Manager) Start(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.starting {
		done := m.startDone
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.Lock()
			err := m.startErr
			m.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.started {
		m.mu.Unlock()
		return nil
	}
	if m.startTerminal {
		err := m.startErr
		m.mu.Unlock()
		return err
	}
	m.starting = true
	m.generation++
	generation := m.generation
	done := make(chan struct{})
	m.startDone = done
	startCtx, startCancel := context.WithCancel(ctx)
	m.startCancel = startCancel
	durable := adapterStartAttempt{adapter: m.durable}
	live := adapterStartAttempt{adapter: m.defaultLive}
	m.mu.Unlock()

	var durableErr, liveErr error
	if durable.adapter != nil {
		durable.attempted = true
		durableErr = normalizeOperation(callStart(durable.adapter, startCtx), startCtx, ErrUnavailable)
		durable.started = durableErr == nil
	}
	if durableErr == nil && live.adapter != nil {
		live.attempted = true
		liveErr = normalizeOperation(callStart(live.adapter, startCtx), startCtx, ErrUnavailable)
		live.started = liveErr == nil
	}

	m.mu.Lock()
	terminal := m.closed
	late := terminal || m.generation != generation
	succeeded := !late && durableErr == nil && (durable.adapter != nil || liveErr == nil)
	cleanup := make([]Transport, 0, 2)
	if succeeded {
		owned, cancel := context.WithCancel(context.Background())
		m.ctx, m.cancel, m.started = owned, cancel, true
		if durable.attempted && durable.started {
			m.startReceiveLocked(durable.adapter)
		}
		if live.attempted && live.started {
			m.startReceiveLocked(live.adapter)
		} else if live.adapter != nil {
			// Optional live startup failure does not tear down a successful
			// durable path, but the unusable adapter is detached atomically.
			m.defaultLive = nil
			cleanup = append(cleanup, live.adapter)
		}
		m.startErr = nil
	} else if late {
		m.startErr = ErrClosed
	} else if durableErr != nil {
		m.startErr = durableErr
		m.startTerminal = true
	} else {
		m.startErr = liveErr
		m.startTerminal = true
	}
	if !late && !succeeded {
		// A failed initial attempt terminally detaches every configured adapter,
		// including an optional path that was never attempted. Cleanup below
		// therefore cannot race a later Manager.Close snapshot of the same owner.
		if durable.adapter != nil {
			m.durable = nil
			cleanup = append(cleanup, durable.adapter)
		}
		if live.adapter != nil {
			m.defaultLive = nil
			cleanup = append(cleanup, live.adapter)
		}
	}
	result := m.startErr
	m.mu.Unlock()

	closed := make([]Transport, 0, len(cleanup))
	failed := make([]quarantineReadyAdapter, 0, len(cleanup))
	for _, adapter := range cleanup {
		if err := normalizeOperation(callClose(adapter, context.Background()), nil, ErrClosed); err != nil {
			failed = append(failed, prepareAuthorityQuarantine(adapter))
		} else {
			closed = append(closed, adapter)
		}
	}

	m.mu.Lock()
	for _, adapter := range closed {
		m.retainDiscardOnlyLocked(adapter, nil)
	}
	for _, adapter := range failed {
		m.quarantineAdapterLocked(adapter)
	}
	m.starting = false
	m.startCancel = nil
	close(done)
	m.mu.Unlock()
	startCancel()
	return result
}

func (m *Manager) startReceiveLocked(adapter Transport) {
	owned := m.ctx
	receiver, cancel := context.WithCancel(owned)
	done := make(chan struct{})
	identity, _ := identityOfTransport(adapter)
	if m.receiverStates == nil {
		m.receiverStates = make(map[transportIdentity]receiverState)
	}
	m.receiverStates[identity] = receiverState{cancel: cancel, done: done}
	epoch := uint64(0)
	metadata, _ := m.metadataLocked(adapter)
	if metadata.kind == KindLive {
		epoch = m.liveAuthorityEpoch
	}
	m.receivers.Add(1)
	go m.runReceiveLoop(receiver, owned, adapter, metadata.kind, done, epoch)
}

func (m *Manager) receiverDoneLocked(adapter Transport) <-chan struct{} {
	identity, ok := identityOfTransport(adapter)
	if !ok {
		return nil
	}
	state, exists := m.receiverStates[identity]
	if !exists {
		return nil
	}
	return state.done
}

func (m *Manager) cancelReceiverLocked(adapter Transport) {
	identity, ok := identityOfTransport(adapter)
	if !ok {
		return
	}
	if state, exists := m.receiverStates[identity]; exists {
		state.cancel()
	}
}

func prepareAuthorityQuarantine(adapter Transport) quarantineReadyAdapter {
	if blocker, ok := adapter.(LiveAuthorityBlocker); ok {
		callBlockLiveAuthority(blocker)
	}
	return quarantineReadyAdapter{adapter: adapter}
}

// quarantineAdapterLocked publishes only adapters that have already crossed
// their irreversible dependency-local authority fence. It also defensively
// removes every peer registry alias and cancels the exact receive loop before
// the detached owner becomes visible in the quarantine cleanup set.
func (m *Manager) quarantineAdapterLocked(prepared quarantineReadyAdapter) {
	adapter := prepared.adapter
	identity, valid := identityOfTransport(adapter)
	if !valid {
		return
	}
	for peer, candidate := range m.live {
		candidateIdentity, ok := identityOfTransport(candidate)
		if ok && candidateIdentity == identity {
			delete(m.live, peer)
		}
	}
	if defaultIdentity, ok := identityOfTransport(m.defaultLive); ok && defaultIdentity == identity {
		m.defaultLive = nil
	}
	m.cancelReceiverLocked(adapter)
	m.quarantined = append(m.quarantined, adapter)
}

func (m *Manager) retainDiscardOnlyLocked(adapter Transport, done <-chan struct{}) {
	m.discardOnly = append(m.discardOnly, discardOwnership{adapter: adapter, receiverDone: done})
	if done == nil {
		return
	}
	// The caller is an admitted Manager operation. It registers this terminal
	// retirement while that operation still keeps Close from advancing past
	// operations.Wait, so discardRetirements.Add cannot race its terminal Wait.
	m.discardRetirements.Add(1)
	go m.finishDiscardRetirement(adapter, done)
}

func (m *Manager) finishDiscardRetirement(adapter Transport, receiverDone <-chan struct{}) {
	defer m.discardRetirements.Done()
	<-receiverDone
	callDiscardShutdownOwned(adapter)
	identity, ok := identityOfTransport(adapter)
	m.mu.Lock()
	if ok {
		for index := range m.discardOnly {
			candidate, valid := identityOfTransport(m.discardOnly[index].adapter)
			if valid && candidate == identity {
				m.discardOnly = append(m.discardOnly[:index], m.discardOnly[index+1:]...)
				break
			}
		}
		delete(m.receiverStates, identity)
	}
	m.releaseAdapterLocked(adapter)
	m.mu.Unlock()
}

// admitOperationLocked enrolls dependency-borrowing work in the Manager
// lifecycle. The caller must hold m.mu; Close sets closed under that same lock
// before waiting, so no WaitGroup Add can race the terminal wait.
func (m *Manager) admitOperationLocked() bool {
	if m.closed {
		return false
	}
	m.operations.Add(1)
	return true
}

func (m *Manager) finishOperation() { m.operations.Done() }

func (m *Manager) receiveLoop(ctx context.Context, adapter Transport, epochs ...uint64) {
	metadata, _ := metadataForTransport(adapter)
	m.runReceiveLoop(ctx, ctx, adapter, metadata.kind, nil, epochs...)
}

func (m *Manager) runReceiveLoop(ctx, lifetime context.Context, adapter Transport, kind Kind, done chan struct{}, epochs ...uint64) {
	defer m.receivers.Done()
	if done != nil {
		defer close(done)
	}
	authorityEpoch := uint64(0)
	if len(epochs) == 1 {
		authorityEpoch = epochs[0]
	}
	var authorityDone <-chan struct{}
	if signal, ok := adapter.(LiveAuthoritySignal); ok {
		authorityDone = signal.LiveAuthorityDone()
	}
	for {
		var item Received
		var err error
		if authenticated, ok := adapter.(AuthenticatedTransport); ok {
			// A retained receiver is deliberately restarted while the new
			// authority epoch is still fenced. Remember that this one blocking
			// dependency call began under the exact fence so a dependency-neutral
			// adapter can transfer an already-authenticated old-epoch value even
			// when the call returns just after publication. This is provenance,
			// not a queue: Manager still owns only its existing bounded handoff.
			handoffEpoch := uint64(0)
			m.mu.Lock()
			if kind == KindLive && !m.closed && m.ctx == lifetime && m.liveAuthorityBlocked && authorityEpoch != 0 && authorityEpoch == m.liveAuthorityEpoch {
				handoffEpoch = authorityEpoch
			}
			m.mu.Unlock()
			var received AuthenticatedReceived
			received, err = callReceiveAuthenticated(authenticated, ctx)
			authentication := received.Authentication
			item = Received{
				Kind: kind, Envelope: received.Envelope, Authentication: &authentication,
				authorityEpoch: received.LiveAuthorityEpoch, handoffEpoch: handoffEpoch,
				authorityCapture: received.authorityCapture, liveSource: adapter,
			}
			if receiptSource, ok := adapter.(LiveReceiptTransport); ok && kind == KindLive {
				item.receiptRoute = &liveReceiptRoute{source: receiptSource, binding: liveReceiptForEnvelope(received.Envelope, authentication.ChannelBinding)}
			}
			received = AuthenticatedReceived{}
		} else {
			item.Envelope, err = callReceive(adapter, ctx)
			item.Kind = kind
		}
		if item.authorityEpoch == 0 {
			item.authorityEpoch = authorityEpoch
		}
		if err != nil {
			clearReceived(&item)
			return
		}
		if ctx.Err() != nil {
			clearReceived(&item)
			return
		}
		m.mu.Lock()
		retired := m.closed || m.ctx != lifetime
		stale := retired || !m.admitLiveIngressLocked(&item)
		m.mu.Unlock()
		if stale {
			clearReceived(&item)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		select {
		case m.inbound <- item:
		case <-ctx.Done():
			clearReceived(&item)
			return
		case <-authorityDone:
			clearReceived(&item)
			return
		}
	}
}

// InstallLive starts and atomically installs one peer-scoped recovered link.
// A superseded link is closed only after the replacement is usable.
func (m *Manager) InstallLive(ctx context.Context, peer string, adapter Transport) error {
	if m == nil || ctx == nil || protocol.ValidateAgentIdentity(peer) != nil {
		return ErrInvalidConfig
	}
	metadata, metadataValid := metadataForTransport(adapter)
	if _, ok := identityOfTransport(adapter); !ok || !metadataValid || metadata.kind != KindLive {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if !m.started {
		m.mu.Unlock()
		return ErrNotStarted
	}
	if m.liveAuthorityBlocked || m.liveInstallationBlocked || m.liveInstallationExhausted {
		m.mu.Unlock()
		return ErrUnavailable
	}
	if m.ownsAdapterLocked(adapter) {
		m.mu.Unlock()
		return ErrInvalidConfig
	}
	generation := m.generation
	lifetime := m.ctx
	authorityEpoch := m.liveAuthorityEpoch
	installationEpoch := m.liveInstallationEpoch
	authorityGate := m.liveAuthority
	m.mu.Unlock()
	if binder, ok := adapter.(LiveAuthorityBinder); ok && !binder.BindLiveAuthority(authorityGate, authorityEpoch) {
		return ErrUnavailable
	}
	m.mu.Lock()
	if m.closed || !m.started || m.generation != generation || m.ctx != lifetime || m.liveAuthorityBlocked || m.liveAuthorityEpoch != authorityEpoch || m.liveInstallationBlocked || m.liveInstallationExhausted || m.liveInstallationEpoch != installationEpoch || m.ownsAdapterLocked(adapter) {
		m.mu.Unlock()
		return ErrUnavailable
	}
	if !m.claimAdapterLocked(adapter, metadata) {
		m.mu.Unlock()
		return ErrInvalidConfig
	}
	identity, _ := identityOfTransport(adapter)
	installation := &pendingLiveInstallation{peer: peer, adapter: adapter, authorityEpoch: authorityEpoch, installationEpoch: installationEpoch}
	if m.pendingLive == nil {
		m.pendingLive = make(map[transportIdentity]*pendingLiveInstallation)
	}
	m.pendingLive[identity] = installation
	// Close fences InstallLive under this lock before waiting on wg. The
	// unpublished adapter remains visible to membership reconciliation while
	// this operation owns Start and any final cleanup/publication.
	m.admitOperationLocked()
	m.mu.Unlock()
	defer m.finishOperation()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	err := normalizeOperation(callStart(adapter, operation), operation, ErrUnavailable)
	stop()
	cancel()
	if err != nil {
		if m.takePendingLiveInstallation(identity, installation) {
			m.closeUnpublishedLive(adapter)
		}
		return err
	}
	var old Transport
	var oldDone <-chan struct{}
	for {
		m.mu.Lock()
		current := m.pendingLive[identity]
		if current != installation {
			closed := m.closed || !m.started || generation != m.generation
			m.mu.Unlock()
			if closed {
				return ErrClosed
			}
			return ErrUnavailable
		}
		if m.liveAuthorityReconciling {
			done := m.liveAuthorityReconcileDone
			m.mu.Unlock()
			if done != nil {
				<-done
			}
			continue
		}
		lifecycleInvalid := m.closed || !m.started || generation != m.generation || m.ctx != lifetime
		if lifecycleInvalid {
			delete(m.pendingLive, identity)
			m.mu.Unlock()
			m.closeUnpublishedLive(adapter)
			return ErrClosed
		}
		if m.liveAuthorityBlocked {
			changed := m.liveAuthorityChanged
			m.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				if m.takePendingLiveInstallation(identity, installation) {
					m.closeUnpublishedLive(adapter)
				}
				return ctx.Err()
			case <-lifetime.Done():
				continue
			}
		}
		if m.liveInstallationBlocked || m.liveInstallationExhausted || m.liveInstallationEpoch != installation.installationEpoch {
			delete(m.pendingLive, identity)
			m.mu.Unlock()
			m.closeUnpublishedLive(adapter)
			return ErrUnavailable
		}
		if m.liveAuthorityEpoch != installation.authorityEpoch {
			delete(m.pendingLive, identity)
			m.mu.Unlock()
			m.closeUnpublishedLive(adapter)
			return ErrUnavailable
		}
		delete(m.pendingLive, identity)
		old = m.live[peer]
		oldDone = m.receiverDoneLocked(old)
		m.live[peer] = adapter
		m.startReceiveLocked(adapter)
		m.mu.Unlock()
		break
	}
	if old != nil {
		closeErr := normalizeOperation(callClose(old, ctx), ctx, ErrClosed)
		var quarantined quarantineReadyAdapter
		if closeErr != nil {
			quarantined = prepareAuthorityQuarantine(old)
		}
		m.mu.Lock()
		if closeErr != nil {
			m.quarantineAdapterLocked(quarantined)
		} else {
			m.retainDiscardOnlyLocked(old, oldDone)
		}
		m.mu.Unlock()
	}
	return nil
}

// takePendingLiveInstallation waits only for the exact bounded reconciliation
// that may have transferred terminal-close ownership. It returns true only
// when the caller atomically removed the still-current pending candidate.
func (m *Manager) takePendingLiveInstallation(identity transportIdentity, installation *pendingLiveInstallation) bool {
	for {
		m.mu.Lock()
		if m.liveAuthorityReconciling {
			done := m.liveAuthorityReconcileDone
			m.mu.Unlock()
			if done == nil {
				continue
			}
			<-done
			continue
		}
		if m.pendingLive[identity] != installation {
			m.mu.Unlock()
			return false
		}
		delete(m.pendingLive, identity)
		m.mu.Unlock()
		return true
	}
}

func (m *Manager) closeUnpublishedLive(adapter Transport) {
	closeErr := normalizeOperation(callClose(adapter, context.Background()), nil, ErrClosed)
	var quarantined quarantineReadyAdapter
	if closeErr != nil {
		quarantined = prepareAuthorityQuarantine(adapter)
	}
	m.mu.Lock()
	if closeErr != nil {
		m.quarantineAdapterLocked(quarantined)
	} else {
		m.releaseAdapterLocked(adapter)
	}
	m.mu.Unlock()
}

func (m *Manager) RemoveLive(ctx context.Context, peer string) error {
	if m == nil || ctx == nil || protocol.ValidateAgentIdentity(peer) != nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	adapter := m.live[peer]
	delete(m.live, peer)
	receiverDone := m.receiverDoneLocked(adapter)
	if adapter != nil {
		m.admitOperationLocked()
	}
	m.mu.Unlock()
	if adapter == nil {
		return nil
	}
	defer m.finishOperation()
	err := normalizeOperation(callClose(adapter, ctx), ctx, ErrClosed)
	var quarantined quarantineReadyAdapter
	if err != nil {
		quarantined = prepareAuthorityQuarantine(adapter)
	}
	m.mu.Lock()
	if err != nil {
		m.quarantineAdapterLocked(quarantined)
	} else {
		m.retainDiscardOnlyLocked(adapter, receiverDone)
	}
	m.mu.Unlock()
	return err
}

// RemoveLiveIf retires peer only when expected is still the installed link.
// It prevents a late expiry owner from deleting a freshly replaced link.
func (m *Manager) RemoveLiveIf(ctx context.Context, peer string, expected Transport) error {
	expectedIdentity, expectedValid := identityOfTransport(expected)
	if m == nil || ctx == nil || !expectedValid || protocol.ValidateAgentIdentity(peer) != nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	actualIdentity, actualValid := identityOfTransport(m.live[peer])
	if !actualValid || actualIdentity != expectedIdentity {
		m.mu.Unlock()
		return nil
	}
	delete(m.live, peer)
	receiverDone := m.receiverDoneLocked(expected)
	m.admitOperationLocked()
	m.mu.Unlock()
	defer m.finishOperation()
	err := normalizeOperation(callClose(expected, ctx), ctx, ErrClosed)
	var quarantined quarantineReadyAdapter
	if err != nil {
		quarantined = prepareAuthorityQuarantine(expected)
	}
	m.mu.Lock()
	if err != nil {
		m.quarantineAdapterLocked(quarantined)
	} else {
		m.retainDiscardOnlyLocked(expected, receiverDone)
	}
	m.mu.Unlock()
	return err
}

// BlockLiveAuthority is a constant-time admission fence. Quarantine performs
// the later bounded detach while the gate keeps every live path unusable.
func (m *Manager) BlockLiveAuthority() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0
	}
	// Every fence is a publication edge, including a repeated fence while
	// admission is already blocked. Advancing the shared epoch makes any
	// publisher that validated an older transaction incapable of reopening the
	// gate after a newer explicit authority loss.
	m.liveAuthorityBlocked = true
	m.liveAuthorityEpoch = m.liveAuthority.Block()
	m.liveAuthorityPreparedEpoch = 0
	m.liveAuthoritySetupEpoch = m.blockLiveInstallationLocked()
	m.signalLiveAuthorityChangedLocked()
	epoch := m.liveAuthorityEpoch
	m.mu.Unlock()
	return epoch
}

// BlockLiveInstallation fences only publication of new Rank1 links. Existing
// installed links retain their current authority epoch and remain available to
// Send, Receive, and Observe while the control plane is temporarily offline.
// Every call advances the setup epoch, so an older publisher cannot reopen a
// newer fence.
func (m *Manager) BlockLiveInstallation() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0
	}
	return m.blockLiveInstallationLocked()
}

func (m *Manager) blockLiveInstallationLocked() uint64 {
	m.liveInstallationBlocked = true
	if m.liveInstallationExhausted || m.liveInstallationEpoch == ^uint64(0) {
		m.liveInstallationExhausted = true
		return 0
	}
	m.liveInstallationEpoch++
	return m.liveInstallationEpoch
}

// PublishLiveInstallation reopens Rank1 installation only for the exact
// current setup fence. A full authority fence and every newer setup fence
// invalidate previously issued epochs.
func (m *Manager) PublishLiveInstallation(epoch uint64) error {
	if m == nil || epoch == 0 {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !m.started {
		return ErrClosed
	}
	if m.liveAuthorityBlocked || m.liveInstallationExhausted || !m.liveInstallationBlocked || m.liveInstallationEpoch != epoch {
		return ErrInvalidConfig
	}
	m.liveInstallationBlocked = false
	return nil
}

// ResumeLiveInstallation is the control-plane recovery spelling for
// PublishLiveInstallation. It retains the same exact-epoch semantics.
func (m *Manager) ResumeLiveInstallation(epoch uint64) error {
	return m.PublishLiveInstallation(epoch)
}

// LiveAuthorityPublicationCurrent reports whether epoch is the exact blocked,
// reconciled publication capability. It is intended for another authority
// owner to validate its own gate while holding that owner's publication lock;
// PublishLiveAuthority still performs the authoritative opening CAS.
func (m *Manager) LiveAuthorityPublicationCurrent(epoch uint64) bool {
	if m == nil || epoch == 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && m.started && m.liveAuthorityBlocked &&
		m.liveAuthorityEpoch == epoch && m.liveAuthorityPreparedEpoch == epoch
}

// QuarantineLiveAuthority performs the bounded O(N) detach/close preparation
// outside the synchronous XMPP authority callback. The shared gate was already
// blocked, so no detached or concurrently captured link can move ownership.
func (m *Manager) QuarantineLiveAuthority() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if !m.liveAuthorityBlocked {
		m.mu.Unlock()
		return nil
	}
	if len(m.live) == 0 && len(m.blockedLivePeers) != 0 {
		peers := append([]string(nil), m.blockedLivePeers...)
		m.mu.Unlock()
		return peers
	}
	m.blockedLivePeers = m.blockedLivePeers[:0]
	detached := make([]Transport, 0, len(m.live))
	for peer, adapter := range m.live {
		delete(m.live, peer)
		m.cancelReceiverLocked(adapter)
		detached = append(detached, adapter)
		m.blockedLivePeers = append(m.blockedLivePeers, peer)
	}
	var defaultBlocker LiveAuthorityBlocker
	if blocker, ok := m.defaultLive.(LiveAuthorityBlocker); ok {
		defaultBlocker = blocker
	}
	peers := append([]string(nil), m.blockedLivePeers...)
	admitted := (len(detached) != 0 || defaultBlocker != nil) && m.admitOperationLocked()
	m.mu.Unlock()
	if admitted {
		defer m.finishOperation()
	}
	prepared := make([]quarantineReadyAdapter, 0, len(detached))
	for _, adapter := range detached {
		prepared = append(prepared, prepareAuthorityQuarantine(adapter))
	}
	if defaultBlocker != nil {
		callBlockLiveAuthority(defaultBlocker)
	}
	m.mu.Lock()
	for _, adapter := range prepared {
		m.quarantineAdapterLocked(adapter)
	}
	m.mu.Unlock()
	sort.Strings(peers)
	return peers
}

// ReconcileLiveAuthority applies one complete current-membership snapshot to
// the peer-scoped live registry while the exact Manager authority epoch is
// fenced. Absent peers are terminally closed. Retained adapters keep their raw
// transport, but their old Manager receive loop is canceled and joined before
// the adapter binds future input to the new epoch and a fresh receiver resumes
// draining authenticated input. Already-admitted paused values keep exact
// source provenance in the root-to-peer-lane handoff. Any noncooperative
// receive or failed rebind leaves the global authority gate closed.
func (m *Manager) ReconcileLiveAuthority(ctx context.Context, epoch uint64, currentPeers []string) error {
	if m == nil || ctx == nil || epoch == 0 || len(currentPeers) > MaximumReceiveQueue {
		return ErrInvalidConfig
	}
	authorized := make(map[string]struct{}, len(currentPeers))
	for _, peer := range currentPeers {
		if protocol.ValidateAgentIdentity(peer) != nil {
			return ErrInvalidConfig
		}
		if _, duplicate := authorized[peer]; duplicate {
			return ErrInvalidConfig
		}
		authorized[peer] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	if m.closed || !m.started {
		m.mu.Unlock()
		return ErrClosed
	}
	if !m.liveAuthorityBlocked || m.liveAuthorityEpoch != epoch {
		m.mu.Unlock()
		return ErrInvalidConfig
	}
	if m.liveAuthorityReconciling {
		m.mu.Unlock()
		return ErrUnavailable
	}
	m.liveAuthorityReconciling = true
	reconcileDone := make(chan struct{})
	m.liveAuthorityReconcileDone = reconcileDone
	m.signalLiveAuthorityChangedLocked()
	prepared := false
	defer func() {
		m.mu.Lock()
		if m.liveAuthorityReconcileDone == reconcileDone {
			if prepared && !m.closed && m.started && m.liveAuthorityBlocked && m.liveAuthorityEpoch == epoch {
				m.liveAuthorityPreparedEpoch = epoch
			}
			m.liveAuthorityReconciling = false
			m.liveAuthorityReconcileDone = nil
			m.signalLiveAuthorityChangedLocked()
			close(reconcileDone)
		}
		m.mu.Unlock()
	}()
	retained := make([]retainedLiveAdapter, 0, len(m.live))
	absent := make([]detachedAdapter, 0, len(m.live))
	for peer, adapter := range m.live {
		done := m.receiverDoneLocked(adapter)
		m.cancelReceiverLocked(adapter)
		if _, keep := authorized[peer]; keep {
			retained = append(retained, retainedLiveAdapter{peer: peer, adapter: adapter, receiverDone: done})
			continue
		}
		delete(m.live, peer)
		absent = append(absent, detachedAdapter{adapter: adapter, receiverDone: done})
	}
	retainedPending := make([]*pendingLiveInstallation, 0, len(m.pendingLive))
	absentPending := make([]*pendingLiveInstallation, 0, len(m.pendingLive))
	for identity, installation := range m.pendingLive {
		if _, keep := authorized[installation.peer]; keep {
			retainedPending = append(retainedPending, installation)
			continue
		}
		delete(m.pendingLive, identity)
		absentPending = append(absentPending, installation)
	}
	if len(retained)+len(absent)+len(retainedPending)+len(absentPending) != 0 {
		m.admitOperationLocked()
	}
	m.mu.Unlock()
	if len(retained)+len(absent)+len(retainedPending)+len(absentPending) != 0 {
		defer m.finishOperation()
	}

	var result error
	closed := make([]detachedAdapter, 0, len(absent))
	failed := make([]quarantineReadyAdapter, 0, len(absent))
	for _, detached := range absent {
		quarantined := prepareAuthorityQuarantine(detached.adapter)
		if err := normalizeOperation(callClose(detached.adapter, ctx), ctx, ErrClosed); err != nil {
			result = errors.Join(result, err)
			failed = append(failed, quarantined)
		} else {
			closed = append(closed, detached)
		}
	}
	pendingClosed := make([]Transport, 0, len(absentPending))
	for _, installation := range absentPending {
		quarantined := prepareAuthorityQuarantine(installation.adapter)
		if err := normalizeOperation(callClose(installation.adapter, ctx), ctx, ErrClosed); err != nil {
			result = errors.Join(result, err)
			failed = append(failed, quarantined)
		} else {
			pendingClosed = append(pendingClosed, installation.adapter)
		}
	}
	m.mu.Lock()
	for _, detached := range closed {
		m.retainDiscardOnlyLocked(detached.adapter, detached.receiverDone)
	}
	for _, adapter := range failed {
		m.quarantineAdapterLocked(adapter)
	}
	for _, adapter := range pendingClosed {
		m.releaseAdapterLocked(adapter)
	}
	m.mu.Unlock()

	for _, kept := range retained {
		if kept.receiverDone != nil {
			select {
			case <-kept.receiverDone:
			case <-ctx.Done():
				return errors.Join(result, ctx.Err())
			}
		}
	}
	if result != nil {
		return result
	}

	for _, installation := range retainedPending {
		if rebinder, ok := installation.adapter.(LiveAuthorityRebinder); ok && !callRebindLiveAuthority(rebinder, m.liveAuthority, epoch) {
			identity, _ := identityOfTransport(installation.adapter)
			m.mu.Lock()
			if m.pendingLive[identity] == installation {
				delete(m.pendingLive, identity)
			}
			m.mu.Unlock()
			quarantined := prepareAuthorityQuarantine(installation.adapter)
			closeErr := normalizeOperation(callClose(installation.adapter, ctx), ctx, ErrClosed)
			m.mu.Lock()
			if closeErr != nil {
				m.quarantineAdapterLocked(quarantined)
			} else {
				m.releaseAdapterLocked(installation.adapter)
			}
			m.mu.Unlock()
			return errors.Join(ErrUnavailable, closeErr)
		}
		identity, _ := identityOfTransport(installation.adapter)
		m.mu.Lock()
		current := m.pendingLive[identity]
		valid := !m.closed && m.started && m.liveAuthorityBlocked && m.liveAuthorityEpoch == epoch && current == installation
		if valid {
			installation.authorityEpoch = epoch
			if !m.liveInstallationExhausted && m.liveInstallationBlocked && m.liveInstallationEpoch == m.liveAuthoritySetupEpoch {
				installation.installationEpoch = m.liveInstallationEpoch
			}
		}
		m.mu.Unlock()
		if !valid {
			return ErrUnavailable
		}
	}

	for _, kept := range retained {
		m.mu.Lock()
		actualIdentity, actualValid := identityOfTransport(m.live[kept.peer])
		expectedIdentity, expectedValid := identityOfTransport(kept.adapter)
		valid := !m.closed && m.started && m.liveAuthorityBlocked && m.liveAuthorityEpoch == epoch && actualValid && expectedValid && actualIdentity == expectedIdentity
		m.mu.Unlock()
		if !valid {
			return ErrUnavailable
		}
		if rebinder, ok := kept.adapter.(LiveAuthorityRebinder); ok && !callRebindLiveAuthority(rebinder, m.liveAuthority, epoch) {
			return ErrUnavailable
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !m.started || !m.liveAuthorityBlocked || m.liveAuthorityEpoch != epoch {
		return ErrUnavailable
	}
	for _, kept := range retained {
		actualIdentity, actualValid := identityOfTransport(m.live[kept.peer])
		expectedIdentity, expectedValid := identityOfTransport(kept.adapter)
		if !actualValid || !expectedValid || actualIdentity != expectedIdentity {
			return ErrUnavailable
		}
	}
	for _, kept := range retained {
		if identity, ok := identityOfTransport(kept.adapter); ok {
			delete(m.receiverStates, identity)
		}
		m.startReceiveLocked(kept.adapter)
	}
	prepared = true
	return nil
}

// PublishLiveAuthority opens admission only for the exact fence owner after
// reconciliation prepared that epoch. Detached quarantined adapters are
// already ingress-inert; a failed dependency Close cannot globally fence
// healthy current links or the durable path.
func (m *Manager) PublishLiveAuthority(epoch uint64) error {
	if m == nil || epoch == 0 {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !m.started {
		return ErrClosed
	}
	if !m.liveAuthorityBlocked || epoch != m.liveAuthorityEpoch || m.liveAuthorityPreparedEpoch != epoch {
		return ErrInvalidConfig
	}
	if !m.liveAuthority.Publish(epoch) {
		return ErrInvalidConfig
	}
	m.liveAuthorityBlocked = false
	m.liveAuthorityPreparedEpoch = 0
	if !m.liveInstallationExhausted && m.liveInstallationBlocked && m.liveAuthoritySetupEpoch != 0 && m.liveInstallationEpoch == m.liveAuthoritySetupEpoch {
		m.liveInstallationBlocked = false
	}
	m.liveAuthoritySetupEpoch = 0
	m.blockedLivePeers = nil
	m.signalLiveAuthorityChangedLocked()
	return nil
}

// BlockedLivePeers returns only exact peer keys captured atomically from the
// live registry at the current authority fence. It is bounded by Manager
// capacity and does not confer authority.
func (m *Manager) BlockedLivePeers() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	peers := append([]string(nil), m.blockedLivePeers...)
	m.mu.Unlock()
	return peers
}

func (m *Manager) RetireAllLive(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	retired := make([]detachedAdapter, 0, len(m.live)+len(m.quarantined))
	for _, adapter := range m.quarantined {
		retired = append(retired, detachedAdapter{adapter: adapter, receiverDone: m.receiverDoneLocked(adapter)})
	}
	m.quarantined = nil
	for peer, adapter := range m.live {
		delete(m.live, peer)
		retired = append(retired, detachedAdapter{adapter: adapter, receiverDone: m.receiverDoneLocked(adapter)})
	}
	if len(retired) != 0 {
		m.admitOperationLocked()
	}
	m.mu.Unlock()
	if len(retired) != 0 {
		defer m.finishOperation()
	}
	var result error
	failed := make([]quarantineReadyAdapter, 0)
	succeeded := make([]detachedAdapter, 0, len(retired))
	for index, retiredAdapter := range retired {
		if err := ctx.Err(); err != nil {
			for _, pending := range retired[index:] {
				failed = append(failed, prepareAuthorityQuarantine(pending.adapter))
			}
			result = errors.Join(result, err)
			break
		}
		if err := normalizeOperation(callClose(retiredAdapter.adapter, ctx), ctx, ErrClosed); err != nil {
			result = errors.Join(result, err)
			failed = append(failed, prepareAuthorityQuarantine(retiredAdapter.adapter))
		} else {
			succeeded = append(succeeded, retiredAdapter)
		}
	}
	m.mu.Lock()
	for _, retiredAdapter := range succeeded {
		m.retainDiscardOnlyLocked(retiredAdapter.adapter, retiredAdapter.receiverDone)
	}
	if len(failed) != 0 {
		for _, adapter := range failed {
			m.quarantineAdapterLocked(adapter)
		}
	} else if result == nil && m.liveAuthorityBlocked {
		m.liveAuthorityPreparedEpoch = m.liveAuthorityEpoch
	}
	m.mu.Unlock()
	return result
}

// Send uses exactly the requested path and selects live links by recipient.
func (m *Manager) Send(ctx context.Context, kind Kind, envelope protocol.Envelope) error {
	if m == nil || ctx == nil || protocol.ValidateEnvelope(envelope) != nil {
		return ErrInvalidEnvelope
	}
	m.mu.Lock()
	ready := m.started && !m.closed
	var adapter Transport
	if kind == KindDurable {
		adapter = m.durable
	} else if kind == KindLive {
		if m.liveAuthorityBlocked {
			m.mu.Unlock()
			return ErrUnavailable
		}
		adapter = m.live[envelope.Recipient]
		if adapter == nil {
			adapter = m.defaultLive
		}
	}
	if !ready {
		m.mu.Unlock()
		return ErrNotStarted
	}
	if adapter == nil {
		m.mu.Unlock()
		return ErrUnavailable
	}
	// Close fences admission under the same lock before waiting on wg, so an
	// admitted dependency Send retains adapter ownership through completion.
	m.admitOperationLocked()
	m.mu.Unlock()
	defer m.finishOperation()
	return normalizeOperation(callSend(adapter, ctx, envelope.Clone()), ctx, ErrSendAmbiguous)
}

// SendLiveTracked selects the current exact-session live link and returns
// its authenticated receipt registration. It never falls back to another
// transport and never treats a successful write as terminal delivery.
func (m *Manager) SendLiveTracked(ctx context.Context, envelope protocol.Envelope) (PendingLiveReceipt, error) {
	if m == nil || ctx == nil || protocol.ValidateEnvelope(envelope) != nil {
		return nil, ErrInvalidEnvelope
	}
	m.mu.Lock()
	ready := m.started && !m.closed && !m.liveAuthorityBlocked
	adapter := m.live[envelope.Recipient]
	if adapter == nil {
		adapter = m.defaultLive
	}
	if !ready {
		m.mu.Unlock()
		return nil, ErrNotStarted
	}
	receipts, ok := adapter.(LiveReceiptTransport)
	if !ok || receipts == nil {
		m.mu.Unlock()
		return nil, ErrUnavailable
	}
	authorityEpoch := m.liveAuthorityEpoch
	operation := m.admitLiveOperationLocked(receipts, envelope.Recipient, authorityEpoch)
	m.mu.Unlock()
	if operation == nil {
		return nil, ErrUnavailable
	}
	defer operation.finish()
	operationContext, finishContext := operation.revocableContext(ctx)
	pending, err := callSendTracked(receipts, operationContext, envelope.Clone())
	finishContext()
	if err != nil {
		if pending != nil {
			callClosePendingReceipt(pending)
		}
		if !operation.current() {
			return nil, ErrSendAmbiguous
		}
		return nil, normalizeOperation(err, ctx, ErrSendAmbiguous)
	}
	binding, valid := callPendingReceiptBinding(pending)
	if pending == nil || !valid || !liveReceiptMatchesEnvelope(binding, envelope) {
		if pending != nil {
			callClosePendingReceipt(pending)
		}
		if !operation.current() {
			return nil, ErrSendAmbiguous
		}
		return nil, ErrProtocol
	}
	if !operation.current() {
		callClosePendingReceipt(pending)
		return nil, ErrSendAmbiguous
	}
	return pending, nil
}

// AcknowledgeLive emits a receipt only through the exact authenticated link
// that supplied item. A copied envelope or a replacement session cannot forge
// the unexported route and exact binding.
func (m *Manager) AcknowledgeLive(ctx context.Context, item Received) error {
	if m == nil || ctx == nil || item.Kind != KindLive || item.Authentication == nil || item.receiptRoute == nil || item.receiptRoute.source == nil || !liveReceiptMatchesEnvelope(item.receiptRoute.binding, item.Envelope) || !liveReceiptMatchesAuthentication(item.receiptRoute.binding, *item.Authentication) {
		return ErrAuthentication
	}
	return m.acknowledgeInbound(ctx, item.receiptRoute.source, item.receiptRoute.binding, item.authorityEpoch, item.pausedAdmission)
}

// TakeLiveReceipt copies only the authenticated exact-session receipt route;
// the returned ticket never retains the received envelope or its payload.
func (m *Manager) TakeLiveReceipt(item Received) (InboundLiveReceipt, error) {
	if m == nil || item.Kind != KindLive || item.Authentication == nil || item.receiptRoute == nil || item.receiptRoute.source == nil || !liveReceiptMatchesEnvelope(item.receiptRoute.binding, item.Envelope) || !liveReceiptMatchesAuthentication(item.receiptRoute.binding, *item.Authentication) {
		return nil, ErrAuthentication
	}
	m.mu.Lock()
	current := m.receiptRouteCurrentLocked(item.receiptRoute.source, item.receiptRoute.binding, item.authorityEpoch)
	if item.pausedAdmission {
		current = m.liveReceiptSourceInstalledLocked(item.receiptRoute.source, item.receiptRoute.binding.Sender)
	}
	m.mu.Unlock()
	if !current {
		return nil, ErrAuthentication
	}
	return &inboundLiveReceipt{manager: m, source: item.receiptRoute.source, binding: item.receiptRoute.binding, authorityEpoch: item.authorityEpoch, rebindCurrent: item.pausedAdmission}, nil
}

func liveReceiptMatchesAuthentication(receipt LiveReceipt, authentication LiveAuthentication) bool {
	return receipt.Sender == authentication.Peer && receipt.MeshID == authentication.MeshID &&
		receipt.ChannelBinding == authentication.ChannelBinding
}

func (m *Manager) acknowledgeInbound(ctx context.Context, source LiveReceiptTransport, binding LiveReceipt, authorityEpoch uint64, rebindCurrent bool) error {
	if m == nil || ctx == nil || source == nil {
		return ErrAuthentication
	}
	m.mu.Lock()
	if rebindCurrent && !m.liveAuthorityBlocked && m.liveReceiptSourceInstalledLocked(source, binding.Sender) {
		authorityEpoch = m.liveAuthorityEpoch
	}
	operation := m.admitLiveOperationLocked(source, binding.Sender, authorityEpoch)
	if operation == nil {
		m.mu.Unlock()
		return ErrAuthentication
	}
	m.mu.Unlock()
	defer operation.finish()
	operationContext, finishContext := operation.revocableContext(ctx)
	err := normalizeOperation(callSendReceipt(source, operationContext, binding), operationContext, ErrSendAmbiguous)
	finishContext()
	current := operation.current()
	if !current {
		return ErrSendAmbiguous
	}
	return err
}

func (m *Manager) receiptRouteCurrentLocked(source LiveReceiptTransport, binding LiveReceipt, authorityEpoch uint64) bool {
	return m.liveSourceCurrentLocked(source, binding.Sender, authorityEpoch)
}

func (m *Manager) liveReceiptSourceInstalledLocked(source LiveReceiptTransport, peer string) bool {
	if m.closed || !m.started || m.ctx == nil || m.ctx.Err() != nil || source == nil {
		return false
	}
	sourceIdentity, sourceValid := identityOfTransport(source)
	metadata, metadataOwned := m.adapterMetadata[sourceIdentity]
	if !sourceValid || !metadataOwned || metadata.kind != KindLive {
		return false
	}
	current := m.live[peer]
	if current == nil {
		current = m.defaultLive
	}
	currentIdentity, currentValid := identityOfTransport(current)
	return currentValid && currentIdentity == sourceIdentity
}

func (m *Manager) liveSourceCurrentLocked(source LiveReceiptTransport, peer string, authorityEpoch uint64) bool {
	if m.closed || !m.started || m.ctx == nil || m.ctx.Err() != nil || m.liveAuthorityBlocked ||
		authorityEpoch == 0 || authorityEpoch != m.liveAuthorityEpoch {
		return false
	}
	sourceIdentity, sourceValid := identityOfTransport(source)
	_, metadataOwned := m.adapterMetadata[sourceIdentity]
	if !sourceValid || !metadataOwned {
		return false
	}
	current := m.live[peer]
	if current == nil {
		current = m.defaultLive
	}
	currentIdentity, currentValid := identityOfTransport(current)
	return currentValid && currentIdentity == sourceIdentity
}

func (m *Manager) ReceiveWithKind(ctx context.Context) (Received, error) {
	if m == nil || ctx == nil {
		return Received{}, ErrInvalidConfig
	}
	m.mu.Lock()
	owned, closed := m.ctx, m.closed
	m.mu.Unlock()
	if closed {
		return Received{}, ErrClosed
	}
	if owned == nil {
		return Received{}, ErrNotStarted
	}
	for {
		select {
		case item := <-m.inbound:
			if err := ctx.Err(); err != nil {
				clearReceived(&item)
				return Received{}, err
			}
			m.mu.Lock()
			retired := m.closed || m.ctx != owned || owned.Err() != nil
			stale := !retired && !m.admitLiveIngressLocked(&item)
			m.mu.Unlock()
			if retired {
				clearReceived(&item)
				return Received{}, ErrClosed
			}
			if stale {
				clearReceived(&item)
				continue
			}
			cloned := item
			cloned.Envelope = item.Envelope.Clone()
			if item.Authentication != nil {
				copy := *item.Authentication
				cloned.Authentication = &copy
			}
			clearReceived(&item)
			return cloned, nil
		case <-ctx.Done():
			return Received{}, ctx.Err()
		case <-owned.Done():
			return Received{}, ErrClosed
		}
	}
}

// admitLiveIngressLocked validates exact authenticated source/session
// provenance. During a membership fence it marks the existing bounded Manager
// handoff as paused so root can move ownership immediately into the peer lane.
// Once authority is published, only values already admitted during that fence
// may cross from the immediately preceding epoch; newly arriving stale frames
// are rejected without terminating the retained link.
func (m *Manager) admitLiveIngressLocked(item *Received) bool {
	if item == nil || item.Kind != KindLive {
		return item != nil
	}
	// Non-authenticated live transports predate Rank1's peer-bound ingress.
	// They remain usable only while current authority is published; they must
	// never cross a membership fence because there is no exact peer source to
	// quarantine into a lane.
	if item.Authentication == nil {
		return !m.liveAuthorityBlocked && item.authorityEpoch != 0 && item.authorityEpoch == m.liveAuthorityEpoch
	}
	if !m.liveItemSourceCurrentLocked(*item) || item.authorityEpoch == 0 {
		return false
	}
	current := m.liveAuthorityEpoch
	if m.liveAuthorityBlocked {
		if item.authorityEpoch != current && (current == 0 || item.authorityEpoch != current-1) {
			return false
		}
		if item.authorityEpoch != current && m.liveIngressRequiresCapture(item.liveSource) && !m.captureMatchesPreviousEpoch(*item, current) {
			return false
		}
		item.pausedAdmission = true
		return true
	}
	if item.authorityEpoch != current && current != 0 && item.authorityEpoch == current-1 && (item.pausedAdmission || m.prePublishHandoffMatches(*item, current)) {
		item.pausedAdmission = true
		item.authorityEpoch = current
	}
	return item.authorityEpoch == current
}

func (m *Manager) prePublishHandoffMatches(item Received, current uint64) bool {
	if m.liveIngressRequiresCapture(item.liveSource) {
		return m.captureMatchesPreviousEpoch(item, current)
	}
	return item.handoffEpoch != 0 && item.handoffEpoch == current
}

func (*Manager) liveIngressRequiresCapture(source Transport) bool {
	_, required := source.(LiveAuthorityCapturedIngress)
	return required
}

func (m *Manager) captureMatchesPreviousEpoch(item Received, current uint64) bool {
	return m != nil && current > 1 && item.authorityEpoch == current-1 &&
		item.authorityCapture.gate == m.liveAuthority && item.authorityCapture.epoch == item.authorityEpoch
}

func (m *Manager) liveItemSourceCurrentLocked(item Received) bool {
	if item.Authentication == nil || item.Authentication.Peer == "" || item.Authentication.Peer != item.Envelope.Sender || item.liveSource == nil {
		return false
	}
	sourceIdentity, sourceValid := identityOfTransport(item.liveSource)
	metadata, metadataOwned := m.adapterMetadata[sourceIdentity]
	if !sourceValid || !metadataOwned || metadata.kind != KindLive {
		return false
	}
	current := m.live[item.Authentication.Peer]
	if current == nil {
		current = m.defaultLive
	}
	currentIdentity, currentValid := identityOfTransport(current)
	if !currentValid || currentIdentity != sourceIdentity {
		return false
	}
	if item.receiptRoute != nil {
		receiptIdentity, receiptValid := identityOfTransport(item.receiptRoute.source)
		if !receiptValid || receiptIdentity != sourceIdentity || !liveReceiptMatchesEnvelope(item.receiptRoute.binding, item.Envelope) || !liveReceiptMatchesAuthentication(item.receiptRoute.binding, *item.Authentication) {
			return false
		}
	}
	return true
}

func clearReceived(item *Received) {
	if item == nil {
		return
	}
	clear(item.Envelope.Payload.Inline)
	clear(item.Envelope.CredentialProof)
	*item = Received{}
}

func liveReceiptForEnvelope(envelope protocol.Envelope, binding [sha256.Size]byte) LiveReceipt {
	return LiveReceipt{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: binding,
	}
}

func liveReceiptMatchesEnvelope(receipt LiveReceipt, envelope protocol.Envelope) bool {
	return receipt.ChannelBinding != [sha256.Size]byte{} && receipt.MessageID == envelope.MessageID &&
		receipt.ConversationID == envelope.ConversationID && receipt.Sender == envelope.Sender &&
		receipt.Recipient == envelope.Recipient && receipt.MeshID == envelope.MeshID
}

func (m *Manager) Receive(ctx context.Context) (protocol.Envelope, error) {
	item, err := m.ReceiveWithKind(ctx)
	return item.Envelope, err
}

func (m *Manager) Observe(kind Kind) Observation { return m.ObservePeer(kind, "") }

// LivePeer returns the currently installed peer-scoped live transport. The
// returned transport owns its own concurrent Close/Send linearization; callers
// must treat it as an ephemeral snapshot and must not infer membership from it.
func (m *Manager) LivePeer(peer string) (Transport, bool) {
	if m == nil || protocol.ValidateAgentIdentity(peer) != nil {
		return nil, false
	}
	m.mu.Lock()
	adapter := m.live[peer]
	ready := m.started && !m.closed && adapter != nil
	ready = ready && !m.liveAuthorityBlocked
	m.mu.Unlock()
	return adapter, ready
}

// IsLivePeer reports whether expected is still the exact installed live
// transport for peer under the current lifecycle and authority epoch. It is a
// linearizable identity check for token-bound operations that must never move
// from an observed link to a concurrently installed replacement.
func (m *Manager) IsLivePeer(peer string, expected Transport) bool {
	expectedIdentity, expectedValid := identityOfTransport(expected)
	if m == nil || !expectedValid || protocol.ValidateAgentIdentity(peer) != nil {
		return false
	}
	m.mu.Lock()
	actualIdentity, actualValid := identityOfTransport(m.live[peer])
	current := m.started && !m.closed && !m.liveAuthorityBlocked && actualValid && actualIdentity == expectedIdentity
	m.mu.Unlock()
	return current
}

// LivePeers returns a canonical bounded snapshot of exact peer keys. It does
// not confer authority and is used only as input to authenticated revalidation.
func (m *Manager) LivePeers() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	peers := make([]string, 0, len(m.live))
	if !m.closed {
		for peer := range m.live {
			peers = append(peers, peer)
		}
	}
	m.mu.Unlock()
	sort.Strings(peers)
	return peers
}

func (m *Manager) ObservePeer(kind Kind, peer string) Observation {
	if m == nil {
		return Observation{State: HealthClosed}
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return Observation{State: HealthClosed}
	}
	if !m.started {
		m.mu.Unlock()
		return Observation{State: HealthUnknown}
	}
	var adapter Transport
	if kind == KindDurable {
		adapter = m.durable
	} else if peer != "" {
		if !m.liveAuthorityBlocked {
			adapter = m.live[peer]
		}
	} else {
		adapter = m.defaultLive
	}
	if adapter == nil {
		m.mu.Unlock()
		return Observation{State: HealthUnknown}
	}
	if !m.admitOperationLocked() {
		m.mu.Unlock()
		return Observation{State: HealthClosed}
	}
	m.mu.Unlock()
	defer m.finishOperation()
	return callObserve(adapter)
}

func (m *Manager) Close(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalidConfig
	}
	m.mu.Lock()
	if m.closeDone != nil {
		done := m.closeDone
		m.mu.Unlock()
		return m.waitClose(ctx, done)
	}
	m.closed = true
	m.generation++
	m.signalLiveAuthorityChangedLocked()
	done := make(chan struct{})
	m.closeDone = done
	startCancel, cancel := m.startCancel, m.cancel
	var startDone <-chan struct{}
	if m.starting {
		startDone = m.startDone
	}
	adapters := make([]Transport, 0, len(m.live)+2)
	if m.defaultLive != nil {
		adapters = append(adapters, m.defaultLive)
	}
	for _, adapter := range m.live {
		adapters = append(adapters, adapter)
	}
	adapters = append(adapters, m.quarantined...)
	m.quarantined = nil
	discardOnly := append([]discardOwnership(nil), m.discardOnly...)
	m.discardOnly = nil
	if m.durable != nil {
		adapters = append(adapters, m.durable)
	}
	m.live = make(map[string]Transport)
	m.defaultLive = nil
	m.durable = nil
	m.mu.Unlock()
	if startCancel != nil {
		startCancel()
	}
	if cancel != nil {
		cancel()
	}
	// The first caller starts exactly one Manager-owned cleanup operation.
	// Caller contexts only bound joining it; dependency lifetime and retained
	// adapter ownership are independent of any one caller's deadline.
	go m.finishClose(adapters, discardOnly, startDone, done)
	return m.waitClose(ctx, done)
}

func (m *Manager) finishClose(adapters []Transport, discardOnly []discardOwnership, startDone <-chan struct{}, done chan struct{}) {
	var result error
	if startDone != nil {
		<-startDone
	}
	// Dependency operations borrow adapters and therefore must terminally join
	// before Close is invoked on any adapter. Receive workers are separate: a
	// non-cooperative Receive may require adapter.Close to unblock it.
	m.operations.Wait()
	// An admitted detached retirement may have returned a dependency Close
	// error and restored its adapter to quarantine after the first snapshot.
	// With admission fenced and operations joined, this is the final owned set.
	m.mu.Lock()
	late := append([]Transport(nil), m.quarantined...)
	m.quarantined = nil
	lateDiscard := append([]discardOwnership(nil), m.discardOnly...)
	m.discardOnly = nil
	m.mu.Unlock()
	adapters = append(adapters, late...)
	discardOnly = append(discardOnly, lateDiscard...)
	for _, adapter := range adapters {
		if err := normalizeOperation(callClose(adapter, context.Background()), nil, ErrClosed); err != nil {
			result = errors.Join(result, err)
		}
	}
	m.receivers.Wait()
	m.discardRetirements.Wait()
	m.drainInbound()
	for _, adapter := range adapters {
		callDiscardShutdownOwned(adapter)
	}
	for _, ownership := range discardOnly {
		if ownership.receiverDone != nil {
			// Exact receiver-owned detach retirements have their own joined
			// discard worker. They may still appear in this early snapshot, but
			// must never be discarded a second time here.
			continue
		}
		callDiscardShutdownOwned(ownership.adapter)
	}
	m.mu.Lock()
	m.closeErr = result
	m.ownedAdapters = make(map[transportIdentity]Transport)
	m.receiverStates = make(map[transportIdentity]receiverState)
	m.mu.Unlock()
	close(done)
}

func (m *Manager) waitClose(ctx context.Context, done <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-done:
		m.mu.Lock()
		result := m.closeErr
		m.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func callDiscardShutdownOwned(adapter Transport) {
	discarder, ok := adapter.(ShutdownOwnedDiscarder)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	discarder.DiscardShutdownOwned()
}

func callBlockLiveAuthority(blocker LiveAuthorityBlocker) {
	defer func() { _ = recover() }()
	blocker.BlockLiveAuthority()
}

func callRebindLiveAuthority(rebinder LiveAuthorityRebinder, gate *LiveAuthorityGate, epoch uint64) (rebound bool) {
	defer func() {
		if recover() != nil {
			rebound = false
		}
	}()
	return rebinder.RebindLiveAuthority(gate, epoch)
}

func (m *Manager) drainInbound() {
	for {
		select {
		case item := <-m.inbound:
			clearReceived(&item)
		default:
			return
		}
	}
}

func normalizeOperation(err error, ctx context.Context, fallback error) error {
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
	return fallback
}

func callStart(adapter Transport, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return adapter.Start(ctx)
}
func callSend(adapter Transport, ctx context.Context, envelope protocol.Envelope) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSendAmbiguous
		}
	}()
	return adapter.Send(ctx, envelope)
}
func callSendTracked(adapter LiveReceiptTransport, ctx context.Context, envelope protocol.Envelope) (pending PendingLiveReceipt, err error) {
	defer func() {
		if recover() != nil {
			if pending != nil {
				callClosePendingReceipt(pending)
			}
			pending, err = nil, ErrSendAmbiguous
		}
	}()
	return adapter.SendTracked(ctx, envelope)
}
func callPendingReceiptBinding(pending PendingLiveReceipt) (binding LiveReceipt, valid bool) {
	defer func() {
		if recover() != nil {
			binding, valid = LiveReceipt{}, false
		}
	}()
	if pending == nil {
		return LiveReceipt{}, false
	}
	return pending.Binding(), true
}
func callClosePendingReceipt(pending PendingLiveReceipt) {
	defer func() { _ = recover() }()
	if pending != nil {
		pending.Close()
	}
}
func callSendReceipt(adapter LiveReceiptTransport, ctx context.Context, receipt LiveReceipt) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSendAmbiguous
		}
	}()
	return adapter.SendReceipt(ctx, receipt)
}
func callReceive(adapter Transport, ctx context.Context) (envelope protocol.Envelope, err error) {
	defer func() {
		if recover() != nil {
			err = ErrReceive
		}
	}()
	return adapter.Receive(ctx)
}

func callReceiveAuthenticated(adapter AuthenticatedTransport, ctx context.Context) (received AuthenticatedReceived, err error) {
	defer func() {
		if recover() != nil {
			received, err = AuthenticatedReceived{}, ErrReceive
		}
	}()
	return adapter.ReceiveAuthenticated(ctx)
}
func callObserve(adapter Transport) (observation Observation) {
	observation = Observation{State: HealthFailed}
	defer func() { _ = recover() }()
	return adapter.Observe()
}
func callClose(adapter Transport, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()
	return adapter.Close(ctx)
}
