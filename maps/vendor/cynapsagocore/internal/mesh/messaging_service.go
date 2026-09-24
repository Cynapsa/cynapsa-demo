package mesh

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/policy"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

const maximumOperationTimeout = 30 * time.Second

type conversationState struct {
	id           string
	peerAgent    string
	peerInternal string
	peerEpoch    uint64
	closed       bool
	active       int
	outboundFail bool
}

type publicationState struct {
	request   PreparedSend
	admitted  bool
	published bool
	failure   Failure
	failed    bool
}

type retainedPayload struct {
	lane   conversation.LaneID
	peerID string
	value  model.Payload
	bytes  uint64
}

type retainedInboundReceipt struct {
	binding Rank1ReceiptBinding
	receipt InboundRank1Receipt
}

type peerResolutionAttempt struct {
	done    chan struct{}
	failure *Failure
}

// topologyMembershipVerifier exposes only current installed authority to the
// policy package.
type topologyMembershipVerifier struct{ topology *Topology }

func (verifier topologyMembershipVerifier) RequireAll(meshID string, agentIDs ...string) error {
	return verifier.topology.RequireAll(meshID, agentIDs...)
}

// MessagingService composes the Pod 3 correctness primitives without owning
// a concrete carrier or payload implementation.
type MessagingService struct {
	config         MessagingConfig
	identity       SessionIdentity
	topology       *Topology
	topologySrc    AuthoritativeGroupSource
	peerAuthority  PeerAuthoritySource
	peerResolver   PeerResolver
	dynamicPeers   map[string]ResolvedPeer
	outboundBound  map[string]string
	suspendedPeers map[string]ResolvedPeer
	// Protected by refreshMu. A successful authenticated rebind must explicitly
	// confirm its actual full JID before suspended exact peers may be retried.
	replayLocalFull string
	replayCursor    int
	replayRunning   atomic.Bool
	resolvingPeers  map[string]*peerResolutionAttempt // refreshMu
	// Even values are closed generations; the low bit is the post-publication
	// admission capability. A fence advances the generation before cleanup.
	dynamicPublication         atomic.Uint64
	dynamicPublicationPrepared uint64 // refreshMu
	carrier                    EnvelopeCarrier
	pipeline                   PayloadPipeline
	deliveries                 DeliverySink
	policies                   *PolicyController
	gate                       *policy.Gate
	deduper                    *conversation.Deduper
	outbox                     *outbox.Outbox
	ownsOutbox                 bool
	outboundRPC                *rpc.OutboundTable
	inboundRPC                 *rpc.InboundTable
	rpcBudget                  *rpc.ByteBudget
	handlers                   *HandlerRegistry

	mu                      sync.Mutex
	started                 bool
	closed                  bool
	shutdownDone            chan struct{}
	ctx                     context.Context
	cancel                  context.CancelFunc
	conversations           map[string]*conversationState
	responseValues          map[string]retainedPayload
	inboundValues           map[string]retainedPayload
	publications            map[string]*publicationState
	activeOperations        int
	deliverySlot            chan struct{}
	refreshMu               sync.Mutex
	operations              sync.WaitGroup
	workers                 sync.WaitGroup
	drainMu                 sync.Mutex
	drainClosed             bool
	inflight                map[string]chan struct{}
	retryAfter              map[string]bool
	attemptWake             map[string]uint64
	drainWake               chan struct{}
	drainEpoch              atomic.Uint64
	drainSlots              chan struct{}
	automaticDrain          bool
	inboundReceipts         map[string]retainedInboundReceipt
	receiptQueue            chan InboundRank1Receipt
	receiptSlots            chan struct{}
	peerLanes               *peer.Registry
	authorityFenceEpoch     atomic.Uint64
	authorityFenceExhausted atomic.Bool

	carrierBoundaryHook func(protocol.Envelope)
}

// ServiceDiagnostics is a bounded, identity-free observation of messaging
// state. It is private input to the SDK boundary's fixed public counters.
type ServiceDiagnostics struct {
	PeerCount          uint64
	QueuedMessages     uint64
	PendingRPCRequests uint64
}

// Diagnostics captures independent bounded counters without retaining locks
// across subsystem calls. PeerCount is the bounded set of locally authorized
// exact peers, excluding the authenticated local endpoint; in dynamic mode it
// includes only peers for which a server handshake has completed. It is not a
// mesh roster or a live-link/reachability count.
func (service *MessagingService) Diagnostics() ServiceDiagnostics {
	if service == nil {
		return ServiceDiagnostics{}
	}
	if !service.admitSnapshotOperation() {
		return ServiceDiagnostics{}
	}
	defer service.operations.Done()
	members := service.topology.memberCountExcluding(service.identity.BoundFull())
	queued, _ := service.outbox.Usage()
	pendingRPC := service.outboundRPC.Len() + service.inboundRPC.Len()
	return ServiceDiagnostics{
		PeerCount:          boundedDiagnosticCount(members),
		QueuedMessages:     boundedDiagnosticCount(queued),
		PendingRPCRequests: boundedDiagnosticCount(pendingRPC),
	}
}

func boundedDiagnosticCount(value int) uint64 {
	if value <= 0 {
		return 0
	}
	if value > MaxMembershipEntries {
		return MaxMembershipEntries
	}
	return uint64(value)
}

func NewMessagingService(config MessagingConfig, dependencies MessagingDependencies) (*MessagingService, error) {
	if config.QueueCapacity < 1 || config.QueueCapacity > MaxMembershipEntries || config.OutboxByteLimit < 1 || config.OutboxByteLimit > outbox.MaxByteCapacity || config.RPCTimeout == nil || config.OperationTimeout <= 0 || config.OperationTimeout > maximumOperationTimeout || config.PeerIdleTimeout <= 0 || config.PeerIdleTimeout > maximumOperationTimeout || config.OutboxPollInterval <= 0 || config.OutboxPollInterval > maximumOperationTimeout || config.Clock == nil || boolCount(dependencies.Topology != nil, dependencies.PeerAuthority != nil, dependencies.PeerResolver != nil) != 1 || dependencies.Carrier == nil || dependencies.Payloads == nil || dependencies.Deliveries == nil || dependencies.Policies == nil || dependencies.Handlers == nil {
		return nil, ErrInvalidConfig
	}
	clockReading := config.Clock.Snapshot()
	if !validCalibratedTime(clockReading) {
		return nil, ErrInvalidConfig
	}
	clockNow := calibratedNow(config.Clock)
	if config.After == nil {
		config.After = time.After
	}
	if dependencies.Identity.AgentID() == "" || dependencies.Identity.MeshID() == "" || dependencies.Identity.BoundFull() == "" {
		return nil, ErrInvalidIdentity
	}
	// Current membership is authority state, not queued application work. Its
	// fixed protocol ceiling is therefore independent from the SDK queue limit;
	// a small command queue must still install the complete server snapshot.
	topology, err := NewTopology(MaxMembershipEntries, dependencies.Identity.MeshID(), clockNow)
	if err != nil {
		return nil, err
	}
	// Terminal identities outlive the server mailbox replay window, so this
	// dedicated bounded budget must not inherit the short-lived work backlog's
	// QueueCapacity and accidentally become a 25-hour throughput limit.
	deduper, err := conversation.NewDeduper(conversation.DedupeConfig{
		GlobalCapacity:  conversation.DefaultDedupeGlobalCapacity,
		PerPeerCapacity: conversation.DefaultDedupePerPeerCapacity,
		Retention:       conversation.DefaultDedupeRetention,
		Now:             clockNow,
	})
	if err != nil {
		return nil, err
	}
	queued := dependencies.Outbox
	ownsOutbox := false
	if queued == nil {
		var err error
		queued, err = outbox.New(outbox.Config{MessageCapacity: config.QueueCapacity, ByteCapacity: config.OutboxByteLimit, Now: clockNow})
		if err != nil {
			return nil, err
		}
		ownsOutbox = true
	}
	rpcBudget, err := rpc.NewByteBudget(config.QueueCapacity)
	if err != nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, err
	}
	outbound, err := rpc.NewOutboundTableWithBudget(rpc.OutboundConfig{Capacity: config.QueueCapacity, Now: clockNow, After: config.After}, rpcBudget)
	if err != nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, err
	}
	inbound, err := rpc.NewInboundTableWithBudget(rpc.InboundConfig{Capacity: config.QueueCapacity, Now: clockNow}, rpcBudget)
	if err != nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, err
	}
	service := &MessagingService{
		config: config, identity: dependencies.Identity, topology: topology, topologySrc: dependencies.Topology, peerAuthority: dependencies.PeerAuthority, peerResolver: dependencies.PeerResolver,
		carrier: dependencies.Carrier, deliveries: dependencies.Deliveries, policies: dependencies.Policies,
		deduper: deduper, outbox: queued, ownsOutbox: ownsOutbox, outboundRPC: outbound, inboundRPC: inbound, rpcBudget: rpcBudget, handlers: dependencies.Handlers,
		conversations: make(map[string]*conversationState, config.QueueCapacity), responseValues: make(map[string]retainedPayload, config.QueueCapacity), inboundValues: make(map[string]retainedPayload, config.QueueCapacity),
		publications:    make(map[string]*publicationState, config.QueueCapacity),
		deliverySlot:    make(chan struct{}, 1),
		drainWake:       make(chan struct{}, 1),
		drainSlots:      make(chan struct{}, min(config.QueueCapacity, 8)),
		inflight:        make(map[string]chan struct{}),
		retryAfter:      make(map[string]bool),
		attemptWake:     make(map[string]uint64),
		automaticDrain:  true,
		inboundReceipts: make(map[string]retainedInboundReceipt, config.QueueCapacity),
		dynamicPeers:    make(map[string]ResolvedPeer),
		outboundBound:   make(map[string]string),
		suspendedPeers:  make(map[string]ResolvedPeer),
		resolvingPeers:  make(map[string]*peerResolutionAttempt),
		receiptQueue:    make(chan InboundRank1Receipt, config.QueueCapacity),
		receiptSlots:    make(chan struct{}, config.QueueCapacity),
	}
	lanes, err := peer.NewRegistry(peer.RegistryConfig{
		MailboxCapacity: config.QueueCapacity, LaneByteCapacity: uint64(config.OutboxByteLimit),
		GlobalCountCapacity: uint64(config.QueueCapacity), GlobalByteCapacity: uint64(config.OutboxByteLimit),
		MemberCapacity: MaxMembershipEntries, IdleTimeout: config.PeerIdleTimeout,
		FencePeer: service.fencePeerState, AllowPeer: service.allowPeerState, ClosePeer: service.closePeerTransport,
	})
	if err != nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, err
	}
	service.peerLanes = lanes
	service.deliverySlot <- struct{}{}
	pipeline, disposition := dependencies.Payloads.Create(service)
	if disposition != PayloadAccepted || pipeline == nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, ErrInvalidConfig
	}
	service.pipeline = pipeline
	gate, err := policy.NewGate(policy.GateConfig{
		MeshID: dependencies.Identity.MeshID(), LocalIdentity: dependencies.Identity.BoundFull(),
		Memberships: topologyMembershipVerifier{topology: topology},
		Paths:       pipelinePathExtractor{pipeline: pipeline}, Authorizer: serviceRuleAuthorizer{topology: topology, policies: dependencies.Policies}, Now: clockNow,
	})
	if err != nil {
		if ownsOutbox {
			queued.Destroy()
		}
		return nil, err
	}
	service.gate = gate
	return service, nil
}

func (service *MessagingService) Start(ctx context.Context) *Failure {
	if service == nil || ctx == nil {
		return &Failure{Code: FailureInternal}
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return &Failure{Code: FailureUnavailable}
	}
	if service.started {
		service.mu.Unlock()
		return nil
	}
	service.operations.Add(1)
	service.mu.Unlock()
	defer service.operations.Done()
	if _, failure := service.calibratedTime(); failure != nil {
		return failure
	}
	var authorityErr error
	if service.peerResolver != nil {
		authorityErr = service.InitializeLocalAuthority(ctx)
	} else {
		authorityErr = service.refreshTopology(ctx)
	}
	if authorityErr != nil {
		return classifyContextOr(authorityErr, ctx, FailureUnavailable)
	}
	local, err := service.topology.LookupInternal(service.identity.BoundFull())
	if err != nil || local.AgentID != service.identity.AgentID() {
		return &Failure{Code: FailureRejected}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return &Failure{Code: FailureUnavailable}
	}
	if !service.started {
		service.ctx, service.cancel = context.WithCancel(context.Background())
		service.started = true
		if service.automaticDrain {
			service.workers.Add(1)
			go service.runOutboxDrain(service.ctx)
		}
		for range min(service.config.QueueCapacity, 8) {
			service.workers.Add(1)
			go service.runInboundReceipts(service.ctx)
		}
	}
	return nil
}

func (service *MessagingService) wakeDrain() {
	if service == nil {
		return
	}
	service.drainEpoch.Add(1)
	select {
	case service.drainWake <- struct{}{}:
	default:
	}
}

func (service *MessagingService) WakeDelivery() {
	if !service.admitSnapshotOperation() {
		return
	}
	defer service.operations.Done()
	service.wakeDrain()
}

func (service *MessagingService) beginDrain(messageID string) bool {
	service.drainMu.Lock()
	defer service.drainMu.Unlock()
	if service.drainClosed {
		return false
	}
	if _, exists := service.inflight[messageID]; exists {
		return false
	}
	service.inflight[messageID] = make(chan struct{})
	service.attemptWake[messageID] = service.drainEpoch.Load()
	service.workers.Add(1)
	return true
}
func (service *MessagingService) endDrain(messageID string) {
	service.drainMu.Lock()
	done := service.inflight[messageID]
	retry := service.retryAfter[messageID]
	startWake := service.attemptWake[messageID]
	delete(service.retryAfter, messageID)
	delete(service.attemptWake, messageID)
	delete(service.inflight, messageID)
	service.drainMu.Unlock()
	if done != nil {
		close(done)
		service.workers.Done()
	}
	if retry || service.drainEpoch.Load() != startWake {
		service.wakeDrain()
	}
}

func (service *MessagingService) requestRetry(messageID string) {
	service.drainMu.Lock()
	if _, active := service.inflight[messageID]; active {
		service.retryAfter[messageID] = true
		service.drainMu.Unlock()
		return
	}
	service.drainMu.Unlock()
	service.wakeDrain()
}

func (service *MessagingService) sendCarrier(ctx context.Context, envelope protocol.Envelope) (disposition CarrierDisposition) {
	if ctx == nil {
		return CarrierUnavailable
	}
	attempt, cancel := context.WithTimeout(ctx, service.config.OperationTimeout)
	stop := context.AfterFunc(service.ctx, cancel)
	defer func() {
		stop()
		cancel()
		if recover() != nil {
			disposition = CarrierUnavailable
		}
	}()
	return service.carrier.Send(attempt, envelope)
}

type carrierResolution struct {
	disposition CarrierDisposition
	finalized   bool
}

func (service *MessagingService) resolveCarrier(ctx context.Context, reservation outbox.Reservation) carrierResolution {
	envelope := reservation.Envelope
	tracked, ok := service.carrier.(Rank1ReceiptCarrier)
	if !ok {
		return carrierResolution{disposition: service.sendCarrier(ctx, envelope)}
	}
	if reservation.FallbackOnly {
		return carrierResolution{disposition: service.sendFallback(envelope)}
	}
	// Rank1 write and receipt confirmation share one bounded pre-fallback
	// window. A stale data channel can consume its entire write deadline before
	// returning, so starting a second full receipt timer afterwards would leave
	// no operation lifetime for Rank2.
	rank1Ctx, closeWindow := context.WithTimeout(service.ctx, service.rank1ReceiptWaitTimeout(envelope))
	writeCtx, cancelWrite := context.WithCancel(rank1Ctx)
	stopCaller := context.AfterFunc(ctx, cancelWrite)
	receipt, disposition := service.sendCarrierWithReceipt(writeCtx, tracked, envelope)
	stopCaller()
	cancelWrite()
	if disposition != CarrierRank1PendingACK {
		windowExpired := rank1Ctx.Err() != nil
		closeWindow()
		if receipt != nil {
			closePendingRank1Receipt(receipt)
		}
		if disposition == CarrierUnavailable && windowExpired && ctx.Err() == nil {
			return carrierResolution{disposition: service.sendFallback(envelope)}
		}
		return carrierResolution{disposition: disposition}
	}
	if receipt == nil {
		closeWindow()
		return carrierResolution{disposition: CarrierUnavailable}
	}
	binding, validBinding := pendingRank1ReceiptBinding(receipt)
	if !validBinding {
		closeWindow()
		closePendingRank1Receipt(receipt)
		return carrierResolution{disposition: CarrierUnavailable}
	}
	if !rank1ReceiptMatchesEnvelope(binding, envelope) {
		closeWindow()
		closePendingRank1Receipt(receipt)
		return carrierResolution{disposition: CarrierUnavailable}
	}
	evidence := outbox.Rank1ReceiptEvidence{
		MessageID: binding.MessageID, ConversationID: binding.ConversationID,
		Sender: binding.Sender, Recipient: binding.Recipient, MeshID: binding.MeshID,
		ChannelBinding: binding.ChannelBinding,
	}
	pending, err := service.outbox.ClaimRank1PendingACK(reservation, evidence)
	if err != nil {
		closeWindow()
		closePendingRank1Receipt(receipt)
		return carrierResolution{disposition: CarrierUnavailable}
	}
	service.startRank1ReceiptWait(pending, evidence, receipt, rank1Ctx.Done(), closeWindow)
	// ClaimRank1PendingACK atomically ended the borrowed reservation. The
	// bounded receipt waiter owns only identity/evidence, never queued bytes or
	// a drain slot, so unrelated eligible entries keep progressing.
	return carrierResolution{disposition: CarrierAccepted, finalized: true}
}

func pendingRank1ReceiptBinding(receipt PendingRank1Receipt) (binding Rank1ReceiptBinding, valid bool) {
	defer func() {
		if recover() != nil {
			binding, valid = Rank1ReceiptBinding{}, false
		}
	}()
	if receipt == nil {
		return Rank1ReceiptBinding{}, false
	}
	return receipt.Binding(), true
}

func (service *MessagingService) startRank1ReceiptWait(pending outbox.Rank1PendingACK, evidence outbox.Rank1ReceiptEvidence, receipt PendingRank1Receipt, fallbackDeadline <-chan struct{}, closeWindow context.CancelFunc) {
	service.workers.Add(1)
	go func() {
		defer service.workers.Done()
		if closeWindow != nil {
			defer closeWindow()
		}
		defer closePendingRank1Receipt(receipt)
		done := pendingRank1ReceiptDone(receipt)
		accepted := false
		if done != nil {
			select {
			case accepted = <-done:
			case <-fallbackDeadline:
			case <-service.ctx.Done():
			}
		}
		if accepted {
			if service.outbox.MarkRank1Acknowledged(pending, evidence) == nil {
				service.wakeDrain()
			}
			return
		}
		if service.outbox.MakeRank2FallbackEligible(pending, evidence) == nil {
			service.wakeDrain()
		}
	}()
}

// rank1ReceiptWaitTimeout confines Rank1 to the first quarter of both the
// bounded transport operation and, when shorter, the current request
// lifetime. A quarter leaves bounded time for Rank2 on both the request and
// response legs of one RPC.
func (service *MessagingService) rank1ReceiptWaitTimeout(envelope protocol.Envelope) time.Duration {
	timeout := service.config.OperationTimeout / 4
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	if envelope.Mode != protocol.ModeRequest || envelope.ExpiresAt.IsZero() || service.config.Clock == nil {
		return timeout
	}
	reading := service.config.Clock.Snapshot()
	if !validCalibratedTime(reading) {
		return timeout
	}
	requestTimeout := envelope.ExpiresAt.Sub(reading.UTC) / 4
	if requestTimeout <= 0 {
		return time.Nanosecond
	}
	if requestTimeout < timeout {
		return requestTimeout
	}
	return timeout
}

func pendingRank1ReceiptDone(receipt PendingRank1Receipt) (done <-chan bool) {
	defer func() {
		if recover() != nil {
			done = nil
		}
	}()
	if receipt == nil {
		return nil
	}
	return receipt.Done()
}

func closePendingRank1Receipt(receipt PendingRank1Receipt) {
	defer func() { _ = recover() }()
	if receipt != nil {
		receipt.Close()
	}
}

func (service *MessagingService) sendCarrierWithReceipt(ctx context.Context, carrier Rank1ReceiptCarrier, envelope protocol.Envelope) (receipt PendingRank1Receipt, disposition CarrierDisposition) {
	if ctx == nil {
		return nil, CarrierUnavailable
	}
	defer func() {
		if recover() != nil {
			if receipt != nil {
				receipt.Close()
			}
			receipt, disposition = nil, CarrierUnavailable
		}
	}()
	return carrier.SendWithReceipt(ctx, envelope)
}

func (service *MessagingService) sendFallback(envelope protocol.Envelope) (disposition CarrierDisposition) {
	carrier, ok := service.carrier.(Rank1ReceiptCarrier)
	if !ok || service.ctx == nil {
		return CarrierUnavailable
	}
	attempt, cancel := context.WithTimeout(service.ctx, service.config.OperationTimeout)
	defer func() {
		cancel()
		if recover() != nil {
			disposition = CarrierUnavailable
		}
	}()
	return carrier.Fallback(attempt, envelope)
}

func rank1ReceiptMatchesEnvelope(binding Rank1ReceiptBinding, envelope protocol.Envelope) bool {
	return binding.ChannelBinding != [32]byte{} && binding.MessageID == envelope.MessageID &&
		binding.ConversationID == envelope.ConversationID && binding.Sender == envelope.Sender &&
		binding.Recipient == envelope.Recipient && binding.MeshID == envelope.MeshID
}

func (service *MessagingService) publicationReady(envelope protocol.Envelope) bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	for _, publication := range service.publications {
		request := publication.request
		if request.MessageID == envelope.MessageID && !publication.admitted {
			return false
		}
	}
	return true
}

func (service *MessagingService) waitDrain(ctx context.Context, messageID string) bool {
	service.drainMu.Lock()
	done := service.inflight[messageID]
	service.drainMu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (service *MessagingService) waitAllDrains() {
	for {
		service.drainMu.Lock()
		pending := make([]<-chan struct{}, 0, len(service.inflight))
		for _, done := range service.inflight {
			pending = append(pending, done)
		}
		service.drainMu.Unlock()
		if len(pending) == 0 {
			return
		}
		for _, done := range pending {
			<-done
		}
	}
}

func (service *MessagingService) runOutboxDrain(ctx context.Context) {
	defer service.workers.Done()
	for {
		select {
		case <-service.drainWake:
		case <-service.config.After(service.config.OutboxPollInterval):
		case <-ctx.Done():
			return
		}
		service.startSuspendedPeerRetry(ctx)
		ids := service.outbox.EligibleMessageIDs(cap(service.drainSlots))
		for _, messageID := range ids {
			if !service.beginDrain(messageID) {
				continue
			}
			reservation, err := service.outbox.ReserveEligible(messageID)
			if err != nil || reservation.MessageID == "" {
				service.endDrain(messageID)
				continue
			}
			if !service.publicationReady(reservation.Envelope) {
				service.outbox.Release(reservation)
				service.endDrain(messageID)
				continue
			}
			select {
			case service.drainSlots <- struct{}{}:
			default:
				service.outbox.Release(reservation)
				service.endDrain(messageID)
				continue
			}
			go service.runDrainAttempt(ctx, reservation)
		}
	}
}

func (service *MessagingService) runDrainAttempt(ctx context.Context, reservation outbox.Reservation) {
	progressed := false
	defer func() {
		service.outbox.Release(reservation)
		<-service.drainSlots
		service.endDrain(reservation.MessageID)
		if progressed {
			service.wakeDrain()
		}
	}()
	defer func() { _ = recover() }()
	envelope := reservation.Envelope
	now, failure := service.calibratedTime()
	if failure != nil {
		return
	}
	if envelope.Mode == protocol.ModeRequest && !envelope.ExpiresAt.IsZero() && !envelope.ExpiresAt.After(now.UTC) {
		if service.outbox.MarkReservedTerminal(reservation) == nil {
			_ = service.outboundRPC.Fail(envelope.CorrelationID, rpc.ErrCancelled)
			progressed = true
		}
		return
	}
	if !service.dynamicPublicationReady() {
		return
	}
	// Avoid occupying a drain worker and reservation while authority is already
	// paused. This is only a liveness fast path; the lane admission and Commit
	// below remain the authoritative fence for races with a fresh snapshot.
	if service.peerLanes == nil || service.peerLanes.Authorize(envelope.Recipient) != nil {
		return
	}
	var resolution carrierResolution
	result, admissionFailure := service.AdmitOutboundPeerWork(ctx, envelope.Recipient, peer.Work{
		Kind: peer.WorkOutbound,
		// The reservation borrows the outbox-owned envelope, which remains
		// charged there until this attempt releases or terminalizes it. The lane
		// owns only this bounded command/count slot.
		OwnedBytes: 0,
		Run: func(runCtx context.Context, commit peer.Commit) error {
			return commit(func() error {
				resolution = service.resolveCarrier(runCtx, reservation)
				return nil
			})
		},
		Clear: func() {},
	})
	if admissionFailure != nil {
		return
	}
	if err := result.Wait(ctx); err != nil {
		return
	}
	if resolution.finalized {
		progressed = true
		return
	}
	if resolution.disposition == CarrierAccepted || resolution.disposition == CarrierRejected {
		if service.outbox.MarkReservedTerminal(reservation) == nil {
			progressed = true
		}
	}
}

func (service *MessagingService) Shutdown(ctx context.Context) *Failure {
	if service == nil || ctx == nil {
		return &Failure{Code: FailureInternal}
	}
	service.mu.Lock()
	if service.shutdownDone != nil {
		done := service.shutdownDone
		service.mu.Unlock()
		return service.waitShutdown(ctx, done)
	}
	service.closed = true
	done := make(chan struct{})
	service.shutdownDone = done
	cancel := service.cancel
	// Close drain admission before the cleanup operation begins waiting. This
	// serializes every workers.Add with workers.Wait.
	service.drainMu.Lock()
	service.drainClosed = true
	service.drainMu.Unlock()
	service.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Cleanup is component-owned rather than caller-owned. A deadline bounds
	// only this call's join and cannot invalidate resources still borrowed by a
	// non-cooperative carrier attempt.
	go service.finishShutdown(done)
	return service.waitShutdown(ctx, done)
}

func (service *MessagingService) finishShutdown(done chan struct{}) {
	service.operations.Wait()
	service.workers.Wait()
	if service.peerLanes != nil {
		_ = service.peerLanes.Close(context.Background())
	}
	service.mu.Lock()
	for messageID, value := range service.responseValues {
		zeroModelPayload(value.value)
		service.rpcBudget.ReleaseOwned(value.bytes)
		delete(service.responseValues, messageID)
	}
	for messageID, value := range service.inboundValues {
		zeroModelPayload(value.value)
		service.rpcBudget.ReleaseOwned(value.bytes)
		delete(service.inboundValues, messageID)
	}
	for messageID, publication := range service.publications {
		for index := range publication.request.Payload.Canonical {
			publication.request.Payload.Canonical[index] = 0
		}
		delete(service.publications, messageID)
	}
	var receipts []InboundRank1Receipt
	for messageID, retained := range service.inboundReceipts {
		receipts = append(receipts, retained.receipt)
		delete(service.inboundReceipts, messageID)
	}
	service.mu.Unlock()
	for _, receipt := range receipts {
		closeInboundRank1Receipt(receipt)
		service.releaseInboundReceiptSlot()
	}
	for {
		select {
		case receipt := <-service.receiptQueue:
			closeInboundRank1Receipt(receipt)
			service.releaseInboundReceiptSlot()
		default:
			goto receiptsDrained
		}
	}
receiptsDrained:
	service.outboundRPC.FailAll(rpc.ErrCancelled)
	service.inboundRPC.CancelAll()
	service.outboundRPC.Destroy()
	service.inboundRPC.Destroy()
	if service.ownsOutbox {
		service.outbox.Destroy()
	}
	close(done)
}

// admitOperation linearizes every public dependency/state borrow with the
// shutdown fence. The returned context is canceled by either the caller or the
// service lifetime; release must be called exactly once.
func (service *MessagingService) admitOperation(ctx context.Context) (context.Context, func(), *Failure) {
	if service == nil || ctx == nil {
		return nil, nil, &Failure{Code: FailureInternal}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, contextFailure(ctx)
	}
	service.mu.Lock()
	if !service.started || service.closed || service.ctx == nil {
		service.mu.Unlock()
		return nil, nil, &Failure{Code: FailureUnavailable}
	}
	lifetime := service.ctx
	service.operations.Add(1)
	service.mu.Unlock()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	return operation, func() {
		stop()
		cancel()
		service.operations.Done()
	}, nil
}

func (service *MessagingService) admitSnapshotOperation() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	if !service.started || service.closed {
		return false
	}
	service.operations.Add(1)
	return true
}

func (service *MessagingService) waitShutdown(ctx context.Context, done <-chan struct{}) *Failure {
	if err := ctx.Err(); err != nil {
		return contextFailure(ctx)
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return contextFailure(ctx)
	}
}

func (service *MessagingService) ensureActive(ctx context.Context) *Failure {
	if failure := service.ensureLifecycleActive(ctx); failure != nil {
		return failure
	}
	if _, failure := service.calibratedTime(); failure != nil {
		return failure
	}
	return nil
}

// ensureLifecycleActive performs no authenticated-data-dependent work. Inbound
// admission uses it before the provenance gate so rejected carriers cannot cause
// clock, membership, materialization, or application-policy observations.
func (service *MessagingService) ensureLifecycleActive(ctx context.Context) *Failure {
	if service == nil || ctx == nil {
		return &Failure{Code: FailureInternal}
	}
	if err := ctx.Err(); err != nil {
		return contextFailure(ctx)
	}
	service.mu.Lock()
	active := service.started && !service.closed
	service.mu.Unlock()
	if !active {
		return &Failure{Code: FailureUnavailable}
	}
	return nil
}

func validCalibratedTime(reading CalibratedTime) bool {
	return !reading.UTC.IsZero() && reading.UTC.Location() == time.UTC && reading.Uncertainty > 0 && reading.Uncertainty <= protocol.MaxClockUncertainty
}

func canonicalClockUncertainty(uncertainty time.Duration) time.Duration {
	return ((uncertainty + time.Microsecond - 1) / time.Microsecond) * time.Microsecond
}

func calibratedNow(clock CalibratedUTCClock) func() time.Time {
	return func() time.Time {
		reading := clock.Snapshot()
		if !validCalibratedTime(reading) {
			return time.Time{}
		}
		return reading.UTC
	}
}

func (service *MessagingService) calibratedTime() (CalibratedTime, *Failure) {
	if service == nil || service.config.Clock == nil {
		return CalibratedTime{}, &Failure{Code: FailureInternal}
	}
	reading := service.config.Clock.Snapshot()
	if !validCalibratedTime(reading) {
		return CalibratedTime{}, &Failure{Code: FailureUnavailable}
	}
	reading.Uncertainty = canonicalClockUncertainty(reading.Uncertainty)
	return reading, nil
}

func (service *MessagingService) operationContext(parent context.Context) (context.Context, func()) {
	service.mu.Lock()
	lifetime := service.ctx
	service.mu.Unlock()
	operation, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	return operation, func() {
		stop()
		cancel()
	}
}

func (service *MessagingService) refreshTopology(ctx context.Context) error {
	if service == nil || ctx == nil {
		return ErrSnapshotInvalid
	}
	if service.peerResolver != nil {
		if service.topology.authorityBlocked() {
			return ErrSnapshotStale
		}
		return ctx.Err()
	}
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if service.peerAuthority != nil {
		snapshot, disposition := service.peerAuthority.SynchronizeCurrentMembership(ctx, service.identity.MeshID())
		if disposition != TopologyReady {
			service.blockPeerAuthority()
			if disposition == TopologyRejected {
				return ErrSnapshotInvalid
			}
			return ErrSnapshotStale
		}
		if err := service.topology.ApplyPeerAuthority(snapshot); err != nil {
			service.blockPeerAuthority()
			return err
		}
		if err := service.topology.PublishPeerAuthority(); err != nil {
			service.blockPeerAuthority()
			return ErrSnapshotStale
		}
		if err := service.publishPeerAuthority(ctx); err != nil {
			service.blockPeerAuthority()
			return ErrSnapshotStale
		}
	} else if err := service.refreshCompleteTopology(ctx); err != nil {
		return err
	}
	return nil
}

// refreshCompleteTopology installs one full-source query into both authority
// owners. Peer lanes remain paused while Topology replaces its maps and resume
// only after the exact same complete snapshot is installed locally.
func (service *MessagingService) refreshCompleteTopology(ctx context.Context) error {
	if service == nil || ctx == nil || service.topologySrc == nil {
		return ErrSnapshotInvalid
	}
	session, failure := service.BeginMembershipSynchronization()
	if failure != nil {
		return ErrSnapshotStale
	}
	fail := func(err error) error {
		service.topology.Block()
		_ = service.PauseMembershipAuthority(session)
		return err
	}
	snapshot, disposition := service.topologySrc.SnapshotGroup(ctx, service.identity.MeshID())
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	switch disposition {
	case TopologyReady:
	case TopologyUnavailable:
		return fail(ErrSnapshotStale)
	case TopologyRejected:
		return fail(ErrSnapshotInvalid)
	default:
		return fail(ErrSnapshotInvalid)
	}
	if err := service.topology.replace(ctx, snapshot); err != nil {
		return fail(err)
	}
	if installFailure := service.InstallCurrentMembership(session, snapshot.Members); installFailure != nil {
		return fail(ErrSnapshotInvalid)
	}
	if publishFailure := service.PublishCurrentMembership(session); publishFailure != nil {
		return fail(ErrSnapshotStale)
	}
	return nil
}

func (service *MessagingService) synchronizeCurrentMembership(ctx context.Context) error {
	if service == nil || service.peerAuthority == nil {
		return service.refreshTopology(ctx)
	}
	return service.synchronizePeerMembership(ctx, false)
}

// currentMembershipReadiness is an optional level condition supplied by an
// authenticated authority owner. Its method is exported so an implementation
// in the composing package can satisfy this private interface.
type currentMembershipReadiness interface {
	CurrentMembershipReady() bool
}

func (service *MessagingService) ensureCurrentMembership(ctx context.Context) error {
	if service == nil || service.peerAuthority == nil {
		return service.refreshTopology(ctx)
	}
	return service.synchronizePeerMembership(ctx, true)
}

func (service *MessagingService) synchronizePeerMembership(ctx context.Context, onlyIfRequired bool) error {
	service.refreshMu.Lock()
	defer service.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if onlyIfRequired {
		if readiness, ok := service.peerAuthority.(currentMembershipReadiness); ok && readiness.CurrentMembershipReady() {
			return nil
		}
	}
	snapshot, disposition := service.peerAuthority.SynchronizeCurrentMembership(ctx, service.identity.MeshID())
	if disposition != TopologyReady {
		service.blockPeerAuthority()
		if disposition == TopologyRejected {
			return ErrSnapshotInvalid
		}
		return ErrSnapshotStale
	}
	if err := service.topology.ApplyPeerAuthority(snapshot); err != nil {
		service.blockPeerAuthority()
		return err
	}
	if err := service.topology.PublishPeerAuthority(); err != nil {
		service.blockPeerAuthority()
		return ErrSnapshotStale
	}
	if err := service.publishPeerAuthority(ctx); err != nil {
		service.blockPeerAuthority()
		return ErrSnapshotStale
	}
	return nil
}

func (service *MessagingService) publishPeerAuthority(ctx context.Context) error {
	publisher, ok := service.peerAuthority.(PeerAuthorityPublisher)
	if !ok {
		return nil
	}
	return publisher.PublishPeerAuthority(ctx)
}

func (service *MessagingService) blockPeerAuthority() {
	if service == nil {
		return
	}
	service.fencePeerAuthorityAdmission()
	service.topology.Block()
}

// fencePeerAuthorityAdmission is the constant-time authority-loss boundary.
// It closes Topology and peer-lane admission before asking the concrete
// publisher to fence Rank1 and Rank2. The later Topology cleanup may wait for
// already-admitted readers, but no new ownership transfer can begin.
func (service *MessagingService) fencePeerAuthorityAdmission() {
	if service == nil {
		return
	}
	service.advanceAuthorityFenceEpoch()
	service.topology.BlockAdmission()
	if service.peerLanes != nil {
		service.peerLanes.BlockAdmission()
	}
	if publisher, ok := service.peerAuthority.(PeerAuthorityPublisher); ok {
		publisher.BlockPeerAuthority()
	}
}

func (service *MessagingService) advanceAuthorityFenceEpoch() {
	if service == nil {
		return
	}
	for {
		current := service.authorityFenceEpoch.Load()
		if current == ^uint64(0) {
			service.authorityFenceExhausted.Store(true)
			return
		}
		if service.authorityFenceEpoch.CompareAndSwap(current, current+1) {
			return
		}
	}
}

// BlockAuthorityAdmission is the constant-time half of authority loss. It is
// safe in the XMPP ingress callback and prevents final topology ownership
// transfers before asynchronous cache cleanup begins.
func (service *MessagingService) BlockAuthorityAdmission() {
	if service == nil {
		return
	}
	if service.peerResolver != nil {
		service.closeDynamicPublication()
		service.advanceAuthorityFenceEpoch()
	}
	if !service.admitSnapshotOperation() {
		return
	}
	defer service.operations.Done()
	if service.peerLanes != nil {
		service.peerLanes.BlockAdmission()
	}
	service.topology.BlockAdmission()
}

func (service *MessagingService) lookupAgent(ctx context.Context, agentID string) (Identity, error) {
	_ = ctx
	return service.topology.Lookup(agentID)
}

func (service *MessagingService) lookupInternal(ctx context.Context, internal string) (Identity, error) {
	_ = ctx
	return service.topology.LookupInternal(internal)
}

func (service *MessagingService) resolveOutboundPeer(ctx context.Context, agentID string) (Identity, error) {
	if service.peerResolver != nil {
		if failure := service.ensureDynamicPeer(ctx, agentID); failure != nil {
			return Identity{}, meshFailureError(failure)
		}
		service.refreshMu.Lock()
		full := service.outboundBound[agentID]
		service.refreshMu.Unlock()
		identity, err := service.topology.LookupInternal(full)
		if err != nil || identity.AgentID != agentID {
			return Identity{}, ErrSnapshotStale
		}
		return identity, nil
	}
	return service.topology.ResolveAgentContext(ctx, service.identity.MeshID(), service.identity.BoundFull(), agentID)
}

func (service *MessagingService) resolveOutboundEndpoints(ctx context.Context, agentID string) ([]Identity, error) {
	if service.peerResolver != nil {
		identity, err := service.resolveOutboundPeer(ctx, agentID)
		if err != nil {
			return nil, err
		}
		return []Identity{identity}, nil
	}
	return service.topology.ResolveAgentEndpointsContext(ctx, service.identity.MeshID(), service.identity.BoundFull(), agentID)
}

// CurrentPeerEndpoints returns an owned private snapshot of the exact current
// endpoints for one logical peer. It is used by the root composition for
// support-safe aggregate health and never crosses the SDK boundary.
func (service *MessagingService) CurrentPeerEndpoints(ctx context.Context, agentID string) ([]string, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return nil, operationFailure
	}
	defer release()
	if failure := service.ensureActive(operation); failure != nil {
		return nil, failure
	}
	endpoints, err := service.resolveOutboundEndpoints(operation, agentID)
	if err != nil {
		return nil, classifyMeshError(err)
	}
	result := make([]string, len(endpoints))
	for index := range endpoints {
		result[index] = strings.Clone(endpoints[index].Internal)
	}
	return result, nil
}

func (service *MessagingService) acquireTopologyAdmission(ctx context.Context, envelope protocol.Envelope) (*topologyAdmission, *Failure) {
	return service.acquireParticipantAdmission(ctx, envelope.MeshID, envelope.Sender, envelope.Recipient)
}

func (service *MessagingService) acquireParticipantAdmission(ctx context.Context, meshID string, participants ...string) (*topologyAdmission, *Failure) {
	admission, err := service.topology.acquireAdmissionContext(ctx, meshID, participants...)
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return nil, contextFailure(ctx)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, classifyContextOr(err, ctx, FailureCancelled)
		}
		return nil, classifyMeshError(err)
	}
	return admission, nil
}

func (service *MessagingService) MeshList(ctx context.Context) (model.MeshListResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.MeshListResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.MeshListResult{}, failure
	}
	local, err := service.topology.LookupInternal(service.identity.BoundFull())
	active := err == nil && local.AgentID == service.identity.AgentID()
	return model.MeshListResult{Meshes: []model.MeshSummary{{MeshID: service.identity.MeshID(), Active: active}}}, nil
}

func (service *MessagingService) MeshRefresh(ctx context.Context) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	if err := service.refreshTopology(ctx); err != nil {
		return classifyContextOr(err, ctx, FailureUnavailable)
	}
	return nil
}

// RefreshCurrentMembership installs one complete current server snapshot and
// leaves authority fenced on any failure.
func (service *MessagingService) RefreshCurrentMembership(ctx context.Context) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	if err := service.synchronizeCurrentMembership(ctx); err != nil {
		return classifyContextOr(err, ctx, FailureUnavailable)
	}
	return nil
}

// EnsureCurrentMembership refreshes current membership only when the
// authority source is still fenced after this service acquires the serialized
// refresh lane. This is for recovery monitors: an explicit synchronization
// that was already in progress may have restored authority while the
// monitor waited for the lane, in which case issuing another server query
// would be redundant.
func (service *MessagingService) EnsureCurrentMembership(ctx context.Context) *Failure {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return failure
	}
	if err := service.ensureCurrentMembership(ctx); err != nil {
		return classifyContextOr(err, ctx, FailureUnavailable)
	}
	return nil
}

func (service *MessagingService) PolicySet(ctx context.Context, args model.PolicySetArgs) (model.PolicyResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.PolicyResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.PolicyResult{}, failure
	}
	if failure := service.policies.Replace(args.Rules); failure != nil {
		return model.PolicyResult{}, failure
	}
	return service.policies.Result(), nil
}

func (service *MessagingService) PolicyGet(ctx context.Context) (model.PolicyResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.PolicyResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.PolicyResult{}, failure
	}
	return service.policies.Result(), nil
}

func (service *MessagingService) PolicyTest(ctx context.Context, args model.PolicyTestArgs) (model.PolicyResult, *Failure) {
	operation, release, operationFailure := service.admitOperation(ctx)
	if operationFailure != nil {
		return model.PolicyResult{}, operationFailure
	}
	defer release()
	ctx = operation
	if failure := service.ensureActive(ctx); failure != nil {
		return model.PolicyResult{}, failure
	}
	peer, err := service.lookupAgent(ctx, args.Input.To)
	if err != nil {
		return model.PolicyResult{}, classifyMeshError(err)
	}
	prepared, disposition := service.pipeline.Prepare(args.Input.Payload)
	if disposition != PayloadAccepted {
		zeroBytes(prepared.Canonical)
		return model.PolicyResult{}, payloadFailure(disposition)
	}
	defer zeroBytes(prepared.Canonical)
	if failure := service.validatePrepared(prepared); failure != nil {
		return model.PolicyResult{}, failure
	}
	allowed := service.policies.Allows(peer.AgentID, prepared.ApplicationPath)
	result := service.policies.Result()
	result.Allowed = allowed
	return result, nil
}

func (service *MessagingService) validatePrepared(prepared PreparedPayload) *Failure {
	snapshot := append([]byte(nil), prepared.Canonical...)
	defer zeroBytes(snapshot)
	path, disposition := service.pipeline.ApplicationPath(prepared.Profile, snapshot)
	if disposition != PayloadAccepted || path != prepared.ApplicationPath || len(prepared.Canonical) == 0 {
		return &Failure{Code: FailureRejected}
	}
	return nil
}

func contextFailure(ctx context.Context) *Failure {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Failure{Code: FailureDeadline}
	}
	return &Failure{Code: FailureCancelled}
}

func classifyContextOr(err error, ctx context.Context, fallback FailureCode) *Failure {
	if ctx != nil && ctx.Err() != nil {
		return contextFailure(ctx)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Failure{Code: FailureDeadline}
	}
	if errors.Is(err, context.Canceled) {
		return &Failure{Code: FailureCancelled}
	}
	return &Failure{Code: fallback}
}

func classifyMeshError(err error) *Failure {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Failure{Code: FailureDeadline}
	case errors.Is(err, context.Canceled):
		return &Failure{Code: FailureCancelled}
	case errors.Is(err, ErrInvalidIdentity):
		return &Failure{Code: FailureRejected}
	case errors.Is(err, ErrIdentityUnknown), errors.Is(err, ErrMembershipMissing):
		return &Failure{Code: FailureAuthorization}
	case errors.Is(err, ErrSnapshotStale):
		return &Failure{Code: FailureUnavailable}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func payloadFailure(disposition PayloadDisposition) *Failure {
	switch disposition {
	case PayloadRejected:
		return &Failure{Code: FailureRejected}
	case PayloadAuthorization:
		return &Failure{Code: FailureAuthorization}
	case PayloadUnavailable:
		return &Failure{Code: FailureUnavailable}
	case PayloadCapacity:
		return &Failure{Code: FailureCapacity}
	case PayloadTooLarge:
		return &Failure{Code: FailurePayloadTooLarge}
	case PayloadIntegrity:
		return &Failure{Code: FailurePayloadIntegrity}
	case PayloadTransferFailed:
		return &Failure{Code: FailurePayloadTransfer}
	case PayloadInvalidHandle:
		return &Failure{Code: FailureInvalidHandle}
	case PayloadCancelled:
		return &Failure{Code: FailureCancelled}
	case PayloadDeadline:
		return &Failure{Code: FailureDeadline}
	case PayloadInternal:
		return &Failure{Code: FailureInternal}
	default:
		return &Failure{Code: FailureInternal}
	}
}

func zeroModelPayload(input model.Payload) {
	switch value := input.Value.(type) {
	case model.NativePayload:
		for index := range value.Body {
			value.Body[index] = 0
		}
	case model.HTTPRequestPayload:
		for index := range value.Body {
			value.Body[index] = 0
		}
	case model.HTTPResponsePayload:
		for index := range value.Body {
			value.Body[index] = 0
		}
		if value.Error != nil {
			*value.Error = model.ApplicationError{}
		}
	}
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

type pipelinePathExtractor struct{ pipeline PayloadPipeline }

func (extractor pipelinePathExtractor) ExtractApplicationPath(profile string, canonical []byte) (string, error) {
	snapshot := append([]byte(nil), canonical...)
	defer zeroBytes(snapshot)
	path, disposition := extractor.pipeline.ApplicationPath(profile, snapshot)
	if disposition != PayloadAccepted {
		return "", policy.ErrApplicationDenied
	}
	return path, nil
}

type serviceRuleAuthorizer struct {
	topology *Topology
	policies *PolicyController
}

func (authorizer serviceRuleAuthorizer) Allows(internalIdentity, applicationPath string) bool {
	identity, err := authorizer.topology.LookupInternal(internalIdentity)
	return err == nil && authorizer.policies.Allows(identity.AgentID, applicationPath)
}
