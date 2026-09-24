package cynapsagocore

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/peer"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank1webrtc"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

// messagingRuntimeBridge breaks the construction-order cycle without a
// global or untyped registry. The runtime is bound exactly once before Core is
// published; authenticated service creation can happen only later.
type messagingRuntimeBridge struct {
	mu          sync.RWMutex
	target      *coreruntime.Runtime
	diagnostics messagingDiagnosticSource
}

type authenticatedDiagnosticSnapshot struct {
	peerCount, queuedMessages, pendingRPC, payloadTransfers uint64
}

type messagingDiagnosticSource interface {
	diagnosticSnapshot() authenticatedDiagnosticSnapshot
}

func (bridge *messagingRuntimeBridge) bind(target *coreruntime.Runtime) {
	if bridge == nil || target == nil {
		return
	}
	bridge.mu.Lock()
	if bridge.target == nil {
		bridge.target = target
	}
	bridge.mu.Unlock()
}

func (bridge *messagingRuntimeBridge) runtime() *coreruntime.Runtime {
	if bridge == nil {
		return nil
	}
	bridge.mu.RLock()
	target := bridge.target
	bridge.mu.RUnlock()
	return target
}

func (bridge *messagingRuntimeBridge) SnapshotRPCTimeout() time.Duration {
	if target := bridge.runtime(); target != nil {
		return target.ConfigSnapshot().RPCTimeout
	}
	return 0
}

func (bridge *messagingRuntimeBridge) DeliverInbound(ctx context.Context, event model.MessageReceivedEvent) error {
	target := bridge.runtime()
	if target == nil {
		return coreruntime.ErrClosing
	}
	return target.DeliverInbound(ctx, event)
}

func (bridge *messagingRuntimeBridge) registerDiagnostics(source messagingDiagnosticSource) bool {
	if bridge == nil || source == nil {
		return false
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.diagnostics != nil {
		return false
	}
	bridge.diagnostics = source
	return true
}

func (bridge *messagingRuntimeBridge) unregisterDiagnostics(source messagingDiagnosticSource) {
	if bridge == nil || source == nil {
		return
	}
	bridge.mu.Lock()
	if bridge.diagnostics == source {
		bridge.diagnostics = nil
	}
	bridge.mu.Unlock()
}

func (bridge *messagingRuntimeBridge) diagnosticSnapshot() authenticatedDiagnosticSnapshot {
	if bridge == nil {
		return authenticatedDiagnosticSnapshot{}
	}
	bridge.mu.RLock()
	source := bridge.diagnostics
	bridge.mu.RUnlock()
	if source == nil {
		return authenticatedDiagnosticSnapshot{}
	}
	return source.diagnosticSnapshot()
}

type authenticatedMessagingFactory struct {
	runtime  *messagingRuntimeBridge
	payloads *payload.PipelineFactory
	handles  *payload.HandleStore
	policies *mesh.PolicyController
	handlers *mesh.HandlerRegistry
}

func newAuthenticatedMessagingFactory(runtime *messagingRuntimeBridge, payloads *payload.PipelineFactory, handles *payload.HandleStore, policies *mesh.PolicyController, handlers *mesh.HandlerRegistry) sessionkernel.AuthenticatedServiceFactory {
	return &authenticatedMessagingFactory{runtime: runtime, payloads: payloads, handles: handles, policies: policies, handlers: handlers}
}

type messagingConnectivity interface {
	messagingCapabilities() (builtInMessagingCapabilities, bool)
}

func (factory *authenticatedMessagingFactory) Create(ctx context.Context, build sessionkernel.AuthenticatedBuildContext) (sessionkernel.Service, sessionkernel.AuthenticatedOperations, *sessionkernel.ProviderError) {
	if factory == nil || factory.runtime == nil || factory.runtime.runtime() == nil || factory.payloads == nil || factory.handles == nil || factory.policies == nil || factory.handlers == nil || ctx == nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, failure
	}
	iceTransportPolicy, err := rank1webrtc.DeploymentICETransportPolicy()
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	connectivity, ok := build.Connectivity.(messagingConnectivity)
	if !ok {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderUnavailable)
	}
	capabilities, ok := connectivity.messagingCapabilities()
	if !ok || capabilities.client == nil || capabilities.clock == nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderUnavailable)
	}
	identity, err := mesh.NewSessionIdentity(build.Identity.MeshID, build.Identity.AgentID, build.Identity.BoundIdentity, capabilities.client)
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderAuthenticationRejected)
	}
	if build.Profile.QueueCapacity <= 0 || build.Profile.StanzaBudgetBytes <= 0 || build.Profile.ReconnectOperationTimeout <= 0 || build.Profile.ReconnectOperationTimeout > 30*time.Second || int64(build.Profile.QueueCapacity) > math.MaxInt64/int64(build.Profile.StanzaBudgetBytes) {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	live, err := transport.NewLiveManager(build.Profile.QueueCapacity)
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	payloadFactory := factory.payloads
	var payloadRuntime *authenticatedPayloadRuntime
	payloadRuntimeOwned := false
	defer func() {
		if payloadRuntime != nil && !payloadRuntimeOwned {
			payloadRuntime.Close()
		}
	}()
	limits, limitErr := authenticatedPayloadLimits(factory.runtime.runtime().ConfigSnapshot(), build.Profile)
	if limitErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	var cipher payload.PayloadCipher
	if build.IdentityKeys != nil {
		var cipherErr error
		cipher, cipherErr = payload.NewAEADCipher(build.IdentityKeys, build.IdentityKeys, build.Random)
		if cipherErr != nil {
			return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
		}
	}
	transfers, transferErr := payload.NewReassembler(limits, cipher, build.Clock.Now)
	if transferErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	payloadRuntime, transferErr = newAuthenticatedPayloadRuntime(transfers, build.Profile.QueueCapacity, build.Profile.ReconnectOperationTimeout)
	if transferErr != nil {
		transfers.Close()
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	directMaximum := limits.ChunkBytes + 4096
	direct, directErr := rank1webrtc.NewManagedPayloadTransfer(live, rank1webrtc.PayloadTransferConfig{MaximumChunkBytes: directMaximum, InFlightChunks: limits.InFlightChunks, MaximumTransfers: limits.MaximumTransfers})
	if directErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	textMaximum := base64.RawStdEncoding.EncodedLen(directMaximum)
	messageChunks, messageErr := rank2xmpp.NewPayloadChunks(capabilities.client, rank2xmpp.PayloadChunksConfig{
		MaximumFrameBytes: limits.MaximumFrameBytes, MaximumChunkBytes: textMaximum,
		StanzaBudgetBytes: build.Profile.StanzaBudgetBytes, XMLOverheadBytes: 16 << 10,
		InFlightChunks: limits.InFlightChunks, MaximumTransfers: limits.MaximumTransfers,
	})
	if messageErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	var objects payload.ObjectStore
	var objectEvidence payload.MaterializationAcknowledger
	var readinessProbe rank2xmpp.ObjectReadinessProbe
	if build.Objects != nil {
		objectTransfer, objectErr := rank2xmpp.NewObjectTransfer(capabilities.client, limits.MaximumTransfers)
		if objectErr != nil {
			return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
		}
		objects, objectEvidence, readinessProbe = build.Objects, objectTransfer, build.Objects
	}
	payloadFactory, transferErr = payload.NewPipelineFactory(limits, payload.PipelineDependencies{
		Handles: factory.handles, Direct: direct, Objects: objects, ObjectEvidence: objectEvidence,
		Messages: messageChunks, Cipher: cipher, Transfers: transfers,
	})
	if transferErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if bindErr := capabilities.client.BindPayloadRuntime(payloadRuntime, payloadRuntime, readinessProbe); bindErr != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	pipelineFactory, err := newMeshPayloadPipelineFactory(payloadFactory, payloadRuntime)
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	rank1Clock := transport.ClockFunc(func() time.Time { return capabilities.clock.Snapshot().UTC })
	authority := &rank1MembershipAuthority{client: capabilities.client, live: live, timeout: build.Profile.ReconnectOperationTimeout, meshID: build.Identity.MeshID, localBare: build.Identity.AgentID, localFull: build.Identity.BoundIdentity, blocked: true, dynamic: true}
	authority.managerEpoch = live.BlockLiveAuthority()
	if err := capabilities.client.SetAuthorityFence(authority.Fence); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := capabilities.client.SetAuthoritySuspension(authority.Suspend, authority.Resume); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	iceAuthority, err := rank1webrtc.NewExternalServiceConfigurationSource(capabilities.client, rank1Clock, iceTransportPolicy)
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	assembly, err := rank1webrtc.NewHandshakeNegotiator(rank1webrtc.HandshakeAssemblyConfig{
		MeshID: build.Identity.MeshID, LocalIdentity: build.Identity.BoundIdentity, Manager: live, Authority: authority, Exchange: capabilities.client,
		ICETransportPolicy: iceTransportPolicy, ICEConfiguration: iceAuthority, ExpiryCleanupTimeout: build.Profile.ReconnectOperationTimeout,
		MaximumMessageBytes: transport.MaximumControlFrameBytes, ReceiveCapacity: build.Profile.QueueCapacity,
		TransferWorkers: min(build.Profile.TransferWorkers, build.Profile.QueueCapacity), TransferQueue: build.Profile.QueueCapacity,
		SCTPStreams: 16, Clock: rank1Clock,
		TransferReceiver: payloadRuntime, TransferRouteResolver: payloadRuntime,
	})
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	handshakes, err := handshake.NewManager(handshake.Config{
		LocalIdentity: build.Identity.BoundIdentity, MaximumPeers: build.Profile.QueueCapacity, MaximumAttempts: build.Profile.QueueCapacity,
		AttemptTimeout: build.Profile.ReconnectOperationTimeout, CooldownInitial: build.Profile.ReconnectInitial, CooldownMaximum: build.Profile.ReconnectMaximum,
		Clock: rank1Clock, Random: build.Random, Jitter: func(value time.Duration) time.Duration {
			jitter, jitterErr := sessionkernel.CryptoJitter(build.Random, value)
			if jitterErr != nil {
				return value
			}
			return jitter
		},
	}, assembly)
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	signalQueueBytes := 64 << 20
	if build.Profile.StanzaBudgetBytes <= signalQueueBytes/build.Profile.QueueCapacity {
		signalQueueBytes = max(rank2xmpp.MaximumSignalBytes, build.Profile.StanzaBudgetBytes*build.Profile.QueueCapacity)
	}
	signals, err := rank1webrtc.NewSignalPump(capabilities.client, handshakes, assembly, build.Profile.ReconnectOperationTimeout, rank1Clock, rank1webrtc.SignalExecutorConfig{
		Workers: min(build.Profile.TransferWorkers, build.Profile.QueueCapacity), QueueCapacity: build.Profile.QueueCapacity, QueueBytes: signalQueueBytes,
	})
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	recovery := &rank1RecoveryController{live: live, refresh: assembly, replace: handshakes, refreshTimeout: max(build.Profile.ReconnectOperationTimeout/2, time.Nanosecond)}
	carrier := newRankedEnvelopeCarrier(live, recovery, newRank2EnvelopeCarrier(capabilities.client), build.Profile.ReconnectOperationTimeout, build.Profile.ReconnectInitial, rank1Clock)
	carrier.authority = authority
	outboxBytes := min(int64(build.Profile.QueueCapacity)*int64(build.Profile.StanzaBudgetBytes), outbox.MaxByteCapacity)
	messaging, err := mesh.NewMessagingService(mesh.MessagingConfig{
		QueueCapacity: build.Profile.QueueCapacity, OutboxByteLimit: outboxBytes,
		RPCTimeout: factory.runtime, OperationTimeout: build.Profile.ReconnectOperationTimeout,
		PeerIdleTimeout: build.Profile.ReconnectOperationTimeout, OutboxPollInterval: build.Profile.ReconnectOperationTimeout,
		Clock: capabilities.clock,
	}, mesh.MessagingDependencies{
		Identity: identity, PeerResolver: rank2PeerResolver{client: capabilities.client}, Carrier: carrier,
		Payloads: pipelineFactory, Deliveries: newRuntimeDeliverySink(factory.runtime), Policies: factory.policies, Handlers: factory.handlers,
		Outbox: capabilities.client.DeliveryOutbox(),
	})
	if err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := capabilities.client.SetRevocationHandler(func(operation context.Context, notice rank2xmpp.Revocation) error {
		if operation == nil {
			return rank2xmpp.ErrInvalidConfig
		}
		var failure *mesh.Failure
		switch notice.Type {
		case rank2xmpp.RevokeLogical:
			failure = messaging.RevokeLogical(operation, notice.Peer)
		case rank2xmpp.RevokeInstallation:
			failure = messaging.RevokeInstallation(operation, notice.Peer, notice.InstallationID, notice.SessionGeneration)
		default:
			return rank2xmpp.ErrProtocol
		}
		if failure != nil {
			return rank2xmpp.ErrUnavailable
		}
		return nil
	}); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := capabilities.client.SetRoutingFailureHandler(messaging.ReportServerRoutingFailure); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	if err := payloadRuntime.bindMessaging(messaging); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	carrier.mu.Lock()
	carrier.wake = messaging.WakeDelivery
	carrier.mu.Unlock()
	if err := capabilities.client.SetDeliveryWake(messaging.WakeDelivery); err != nil {
		return nil, sessionkernel.AuthenticatedOperations{}, providerError(sessionkernel.ProviderInternal)
	}
	authority.mu.Lock()
	authority.messaging = messaging
	authority.mu.Unlock()
	rank1 := &rank1MessagingRuntime{live: live, handshakes: handshakes, assembly: assembly, carrier: carrier, signals: signals, inbound: &rank1InboundPump{source: live, receiver: messaging, authority: authority, localRecipient: build.Identity.BoundIdentity, meshID: build.Identity.MeshID, receiptTimeout: build.Profile.ReconnectOperationTimeout}, client: capabilities.client, authority: authority, timeout: build.Profile.ReconnectOperationTimeout, retryDelay: build.Profile.ReconnectInitial}
	service := newAuthenticatedMessagingService(messaging, newRank2InboundPump(capabilities.client, messaging, build.Identity.BoundIdentity, build.Identity.MeshID), rank1, factory.runtime, payloadRuntime)
	payloadRuntimeOwned = true
	if failure := connectivityContextError(ctx); failure != nil {
		return service, sessionkernel.AuthenticatedOperations{}, failure
	}
	return service, service.operations(), nil
}

func authenticatedPayloadLimits(config model.RuntimeConfig, profile sessionkernel.OperationalProfile) (payload.Limits, error) {
	maximum := int64(config.PayloadLimit)
	if maximum <= 0 || maximum > payload.MaximumCanonicalBytes || profile.QueueCapacity <= 0 || profile.TransferWorkers <= 0 || profile.StanzaBudgetBytes <= 0 {
		return payload.Limits{}, payload.ErrInvalidLimits
	}
	const chunkBytes = 64 << 10
	if maximum > math.MaxInt64-payload.MaximumTransferredOverheadBytes {
		return payload.Limits{}, payload.ErrInvalidLimits
	}
	transferred := maximum + payload.MaximumTransferredOverheadBytes
	if maximum > math.MaxInt64-transferred {
		return payload.Limits{}, payload.ErrInvalidLimits
	}
	reassembly := transferred + maximum
	if reassembly > payload.MaximumReassemblyBytes {
		return payload.Limits{}, payload.ErrInvalidLimits
	}
	maximumChunks := uint32((transferred + chunkBytes - 1) / chunkBytes)
	if maximumChunks == 0 || maximumChunks > payload.MaximumTransferChunks {
		return payload.Limits{}, payload.ErrInvalidLimits
	}
	maximumTransfers := min(profile.QueueCapacity, 65_536)
	limits := payload.Limits{
		InlineBytes: min(maximum, int64(protocol.MaxInlinePayloadBytes)), MaximumPayloadBytes: maximum,
		ChunkBytes: chunkBytes, MaximumFrameBytes: transport.MaximumControlFrameBytes, MaximumChunks: maximumChunks,
		InFlightChunks: min(4, int(maximumChunks)), TransfersPerPeer: maximumTransfers, MaximumTransfers: maximumTransfers,
		ReassemblyBytesPerPeer: reassembly, ReassemblyBytes: reassembly,
		TransferLifetime: 5 * time.Minute, CleanupTimeout: min(profile.ReconnectOperationTimeout, 30*time.Second),
		WorkerCount: min(profile.TransferWorkers, maximumTransfers), WorkerQueue: maximumTransfers,
	}
	if err := limits.Validate(); err != nil {
		return payload.Limits{}, err
	}
	return limits, nil
}

type rank2GroupTopologySource struct {
	client            *rank2xmpp.Client
	clock             mesh.CalibratedUTCClock
	meshID, localBare string
	localFull         string
	rank1             *rank1MembershipAuthority
	// synchronizeMembership is a deterministic unit-test seam for proving
	// observation-time ordering around local reconciliation. Production always
	// uses rank1.synchronize.
	synchronizeMembership func(context.Context) ([]mesh.Identity, error)
}

type rank1MembershipAuthority struct {
	client                       *rank2xmpp.Client
	live                         *transport.Manager
	timeout                      time.Duration
	meshID, localBare, localFull string
	dynamic                      bool
	mu                           sync.Mutex
	// fenceMu serializes Manager epoch ownership across authority fences. It is
	// never held while calling into MessagingService.
	fenceMu             sync.Mutex
	syncMu              sync.Mutex
	blocked             bool
	ready               bool
	suspended           bool
	managerEpoch        uint64
	installationEpoch   uint64
	messaging           *mesh.MessagingService
	membershipSession   *mesh.MembershipAuthoritySession
	pendingSnapshot     rank2xmpp.AuthoritySnapshot
	pendingPeers        []string
	currentPeers        []string
	stateChanged        chan struct{}
	stateVersion        uint64
	generation          uint64
	generationExhausted bool
	beforeAcknowledge   func()
}

func (source *rank2GroupTopologySource) SynchronizeCurrentMembership(ctx context.Context, meshID string) (mesh.PeerAuthoritySnapshot, mesh.TopologyDisposition) {
	if source == nil || source.clock == nil || ctx == nil || meshID != source.meshID || source.rank1 == nil && source.synchronizeMembership == nil {
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyRejected
	}
	// Capture the observation before authenticated fetch and reconciliation so
	// it remains diagnostic evidence of when this complete snapshot began.
	reading := source.clock.Snapshot()
	if reading.UTC.IsZero() || reading.UTC.Location() != time.UTC || reading.Uncertainty <= 0 || reading.Uncertainty > protocol.MaxClockUncertainty {
		if source.rank1 != nil {
			source.rank1.Fence()
		}
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
	}
	var members []mesh.Identity
	var err error
	if source.synchronizeMembership != nil {
		members, err = source.synchronizeMembership(ctx)
	} else {
		members, err = source.rank1.synchronize(ctx)
	}
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, rank2xmpp.ErrUnavailable) || errors.Is(err, rank2xmpp.ErrClosed) {
			return mesh.PeerAuthoritySnapshot{}, mesh.TopologyUnavailable
		}
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyRejected
	}
	local, err := mesh.NewIdentity(source.localBare, source.localFull)
	if err != nil {
		return mesh.PeerAuthoritySnapshot{}, mesh.TopologyRejected
	}
	results := make([]mesh.PeerAuthorityResult, 0, len(members))
	for _, identity := range members {
		if identity.Internal == source.localFull {
			continue
		}
		results = append(results, mesh.PeerAuthorityResult{Identity: identity, Authorized: true})
	}
	return mesh.PeerAuthoritySnapshot{MeshID: meshID, ObservedAt: reading.UTC, Local: local, LocalAuthorized: true, Peers: results}, mesh.TopologyReady
}

func (source *rank2GroupTopologySource) PublishPeerAuthority(ctx context.Context) error {
	if source == nil || source.rank1 == nil || ctx == nil {
		return rank2xmpp.ErrInvalidConfig
	}
	return source.rank1.publishCurrentMembership(ctx)
}

func (source *rank2GroupTopologySource) BlockPeerAuthority() {
	if source != nil && source.rank1 != nil {
		source.rank1.Fence()
	}
}

// CurrentMembershipReady lets recovery monitors coalesce behind an explicit
// fresh synchronization that has already restored this logical session.
func (source *rank2GroupTopologySource) CurrentMembershipReady() bool {
	return source != nil && source.rank1 != nil && source.rank1.MembershipReady()
}

func (authority *rank1MembershipAuthority) AuthorizeRank1(ctx context.Context, peerFull string) error {
	if authority == nil || authority.client == nil || authority.live == nil || ctx == nil || peerFull == authority.localFull {
		return rank2xmpp.ErrUnavailable
	}
	slash := strings.LastIndexByte(peerFull, '/')
	if slash <= 0 || slash == len(peerFull)-1 {
		return rank2xmpp.ErrProtocol
	}
	peerBare := peerFull[:slash]
	if peerBare == "" || strings.Contains(peerBare, "/") || protocol.ValidateAgentIdentity(peerBare) != nil {
		return rank2xmpp.ErrProtocol
	}
	authority.mu.Lock()
	ready, messaging := authority.ready && !authority.blocked && !authority.suspended && !authority.generationExhausted, authority.messaging
	authority.mu.Unlock()
	if !ready || messaging == nil {
		return rank2xmpp.ErrUnavailable
	}
	if authority.dynamic {
		// Rank1 must authorize the exact installation it will connect to.
		// A bare lookup could select another installation while this stale
		// full peer address still happens to be present in the local cache.
		if failure := messaging.AuthorizeExactPeer(ctx, peerFull); failure != nil {
			if failure.Code == mesh.FailureRejected || failure.Code == mesh.FailureAuthorization {
				return rank2xmpp.ErrAuthentication
			}
			return rank2xmpp.ErrUnavailable
		}
	}
	if failure := messaging.AuthorizeCurrentPeer(peerFull); failure != nil {
		if failure.Code == mesh.FailureRejected || failure.Code == mesh.FailureAuthorization {
			return rank2xmpp.ErrAuthentication
		}
		return rank2xmpp.ErrUnavailable
	}
	return nil
}

// Fence is the synchronous hard-invalidation boundary. The Manager gate
// closes first; then the exact membership session is retired so queued work
// cannot publish or report success until a fresh complete snapshot.
func (authority *rank1MembershipAuthority) Fence() {
	authority.fence()
}

// Suspend is the reversible transport-outage fence. It blocks new Rank1 setup
// while retaining the exact current membership and already published links.
// Transport loss alone is not authenticated revocation evidence.
func (authority *rank1MembershipAuthority) Suspend() bool {
	if authority == nil || authority.live == nil {
		return false
	}
	// A transport interruption now destroys peer authorization. The client
	// follows a false suspension with the hard fence before it publishes the
	// disconnected state; resumption will require fresh peer handshakes.
	if authority.dynamic {
		return false
	}
	authority.fenceMu.Lock()
	defer authority.fenceMu.Unlock()
	authority.mu.Lock()
	if authority.generationExhausted || authority.blocked || !authority.ready {
		authority.mu.Unlock()
		return false
	}
	if authority.suspended {
		authority.mu.Unlock()
		return true
	}
	generation := authority.generation
	session := authority.membershipSession
	authority.mu.Unlock()
	if session == nil {
		return false
	}
	installationEpoch := authority.live.BlockLiveInstallation()
	if installationEpoch == 0 {
		return false
	}
	authority.mu.Lock()
	if authority.generation != generation || authority.blocked || !authority.ready || authority.membershipSession != session {
		authority.mu.Unlock()
		return false
	}
	authority.suspended = true
	authority.installationEpoch = installationEpoch
	authority.signalStateChangedLocked()
	authority.mu.Unlock()
	emitRank1Evidence(rank1EvidenceRecord{Event: "control_suspended", DataReady: true, Suspended: true})
	return true
}

// Resume restores only an exact still-current suspension. A hard membership
// invalidation makes the retained capability stale and returns restored=false
// so the control worker can perform the ordinary full synchronization.
func (authority *rank1MembershipAuthority) Resume(ctx context.Context) (restored bool, err error) {
	if authority == nil || authority.live == nil || ctx == nil {
		return false, rank2xmpp.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	authority.fenceMu.Lock()
	defer authority.fenceMu.Unlock()
	authority.mu.Lock()
	if authority.blocked || authority.generationExhausted {
		authority.mu.Unlock()
		return false, nil
	}
	if !authority.suspended {
		ready := authority.ready
		authority.mu.Unlock()
		return ready, nil
	}
	generation, installationEpoch := authority.generation, authority.installationEpoch
	authority.mu.Unlock()
	if installationEpoch == 0 {
		return false, rank2xmpp.ErrUnavailable
	}
	if err = authority.live.ResumeLiveInstallation(installationEpoch); err != nil {
		return false, rank2xmpp.ErrUnavailable
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.generation != generation || authority.blocked || !authority.suspended || authority.installationEpoch != installationEpoch {
		authority.live.BlockLiveInstallation()
		return false, rank2xmpp.ErrUnavailable
	}
	authority.suspended = false
	authority.installationEpoch = 0
	authority.signalStateChangedLocked()
	return true, nil
}

func (authority *rank1MembershipAuthority) fence() uint64 {
	if authority == nil {
		return 0
	}
	authority.fenceMu.Lock()
	managerEpoch := authority.live.BlockLiveAuthority()
	authority.mu.Lock()
	if authority.generationExhausted || authority.generation == math.MaxUint64 {
		authority.generationExhausted = true
	} else {
		authority.generation++
	}
	generation := authority.generation
	authority.blocked = true
	authority.ready = false
	authority.suspended = false
	authority.installationEpoch = 0
	authority.managerEpoch = managerEpoch
	session, messaging := authority.membershipSession, authority.messaging
	authority.membershipSession = nil
	authority.pendingSnapshot = rank2xmpp.AuthoritySnapshot{}
	authority.pendingPeers = nil
	authority.currentPeers = nil
	// Every fence is an edge. A normal refresh emits one edge here and a
	// second on publication; a failed refresh emits its second edge through
	// the failure fence. Monitors can therefore distinguish an in-progress
	// transaction from a settled blocked state without timer heuristics.
	authority.signalStateChangedLocked()
	authority.mu.Unlock()
	authority.fenceMu.Unlock()
	emitRank1Evidence(rank1EvidenceRecord{Event: "hard_fence", Blocked: true})
	if messaging != nil {
		messaging.BlockAuthorityAdmission()
	}
	if session != nil && messaging != nil {
		_ = messaging.PauseMembershipAuthority(session)
	}
	return generation
}

// StateChanged returns a level-safe authority edge. Consumers must always
// capture the channel before reading MembershipReady and re-check the level
// after every wake; transitions may coalesce while no consumer is scheduled.
func (authority *rank1MembershipAuthority) StateChanged() <-chan struct{} {
	changed, _, _ := authority.observeState()
	return changed
}

func (authority *rank1MembershipAuthority) observeState() (<-chan struct{}, uint64, bool) {
	if authority == nil {
		closed := make(chan struct{})
		close(closed)
		return closed, 0, false
	}
	authority.mu.Lock()
	if authority.stateChanged == nil {
		authority.stateChanged = make(chan struct{})
	}
	changed, version := authority.stateChanged, authority.stateVersion
	ready := authority.ready && !authority.blocked && !authority.suspended && !authority.generationExhausted
	authority.mu.Unlock()
	return changed, version, ready
}

func (authority *rank1MembershipAuthority) signalStateChangedLocked() {
	authority.stateVersion++
	if authority.stateChanged != nil {
		close(authority.stateChanged)
	}
	authority.stateChanged = make(chan struct{})
}

func (authority *rank1MembershipAuthority) AdmitExistingRank1(peer string) bool {
	if authority == nil || protocol.ValidateAgentIdentity(peer) != nil {
		return false
	}
	authority.mu.Lock()
	ready := authority.ready && !authority.blocked && !authority.generationExhausted
	messaging, live := authority.messaging, authority.live
	authority.mu.Unlock()
	if !ready || messaging == nil || live == nil {
		emitRank1Evidence(rank1EvidenceRecord{Event: "retained_admission_rejected", Reason: "data_not_ready", DataReady: ready})
		return false
	}
	if messaging.AuthorizeCurrentPeer(peer) != nil {
		emitRank1Evidence(rank1EvidenceRecord{Event: "retained_admission_rejected", Reason: "membership"})
		return false
	}
	_, current := live.LivePeer(peer)
	if !current {
		emitRank1Evidence(rank1EvidenceRecord{Event: "retained_admission_rejected", Reason: "link_not_current", DataReady: true})
	}
	return current
}

func (authority *rank1MembershipAuthority) MembershipReady() bool {
	if authority == nil {
		return false
	}
	authority.mu.Lock()
	ready := authority.ready && !authority.blocked && !authority.suspended && !authority.generationExhausted
	authority.mu.Unlock()
	return ready
}

func (authority *rank1MembershipAuthority) DataReady() bool {
	if authority == nil {
		return false
	}
	authority.mu.Lock()
	// A transient control-session outage blocks new Rank1 establishment but
	// does not revoke the already authenticated membership and link capability.
	// Hard fences still clear ready and block retained data immediately.
	ready := authority.ready && !authority.blocked && !authority.generationExhausted
	authority.mu.Unlock()
	return ready
}

// openDynamic publishes only the authenticated local session. Peers are
// admitted individually after a fresh server-authorized Cynapsa handshake;
// no complete mesh membership vector is queried or installed.
func (authority *rank1MembershipAuthority) openDynamic(ctx context.Context) error {
	if authority == nil || !authority.dynamic || authority.client == nil || authority.live == nil || ctx == nil {
		return rank2xmpp.ErrInvalidConfig
	}
	authority.syncMu.Lock()
	defer authority.syncMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if authority.client.DurableState() != rank2xmpp.DurableLive {
		return rank2xmpp.ErrUnavailable
	}
	authority.mu.Lock()
	if authority.ready && !authority.blocked && !authority.suspended {
		authority.mu.Unlock()
		return nil
	}
	messaging, epoch, generation := authority.messaging, authority.managerEpoch, authority.generation
	authority.mu.Unlock()
	if messaging == nil || epoch == 0 {
		return rank2xmpp.ErrUnavailable
	}
	if err := messaging.InitializeLocalAuthority(ctx); err != nil {
		return rank2xmpp.ErrUnavailable
	}
	if err := authority.live.ReconcileLiveAuthority(ctx, epoch, nil); err != nil {
		return rank2xmpp.ErrUnavailable
	}
	if err := authority.live.PublishLiveAuthority(epoch); err != nil {
		return rank2xmpp.ErrUnavailable
	}
	if err := messaging.ReauthorizeSuspendedPeers(ctx, authority.client.CurrentBoundIdentity()); err != nil {
		authority.Fence()
		return rank2xmpp.ErrUnavailable
	}
	liveSession := authority.client.DurableState() == rank2xmpp.DurableLive
	authority.mu.Lock()
	current := authority.generation == generation && authority.managerEpoch == epoch && !authority.generationExhausted && liveSession
	if current {
		authority.blocked = false
		authority.ready = true
		authority.suspended = false
		authority.signalStateChangedLocked()
	}
	authority.mu.Unlock()
	if !current {
		authority.Fence()
		return rank2xmpp.ErrUnavailable
	}
	return nil
}

func (authority *rank1MembershipAuthority) synchronize(ctx context.Context) ([]mesh.Identity, error) {
	if authority == nil || ctx == nil {
		return nil, rank2xmpp.ErrInvalidConfig
	}
	authority.syncMu.Lock()
	defer authority.syncMu.Unlock()
	var (
		snapshot     rank2xmpp.AuthoritySnapshot
		members      []mesh.Identity
		managerEpoch uint64
		transaction  uint64
		err          error
	)
	authority.mu.Lock()
	messaging := authority.messaging
	exhausted := authority.generationExhausted
	authority.mu.Unlock()
	if exhausted {
		return nil, rank2xmpp.ErrUnavailable
	}
	transaction = authority.fence()
	authority.mu.Lock()
	managerEpoch = authority.managerEpoch
	authority.mu.Unlock()
	if messaging == nil || managerEpoch == 0 {
		return nil, rank2xmpp.ErrUnavailable
	}
	session, failure := messaging.BeginMembershipSynchronization()
	if failure != nil {
		return nil, rank2xmpp.ErrUnavailable
	}
	authority.mu.Lock()
	if authority.generationExhausted || !authority.blocked || authority.generation != transaction || authority.managerEpoch != managerEpoch {
		authority.mu.Unlock()
		_ = messaging.PauseMembershipAuthority(session)
		return nil, rank2xmpp.ErrUnavailable
	}
	authority.membershipSession = session
	authority.mu.Unlock()
	snapshot, err = authority.client.SyncAuthority(ctx)
	if err != nil {
		authority.Fence()
		return nil, err
	}
	members, err = authority.snapshotMembers(snapshot.Members)
	if err != nil {
		authority.Fence()
		return nil, err
	}
	currentPeers := make([]string, 0, len(members))
	for _, member := range members {
		if member.Internal != authority.localFull {
			currentPeers = append(currentPeers, member.Internal)
		}
	}
	operation, cancel := context.WithTimeout(ctx, authority.timeout)
	err = authority.live.ReconcileLiveAuthority(operation, managerEpoch, currentPeers)
	cancel()
	if err != nil {
		authority.Fence()
		return nil, rank2xmpp.ErrUnavailable
	}
	if failure := messaging.InstallCurrentMembership(session, members); failure != nil {
		authority.Fence()
		return nil, rank2xmpp.ErrUnavailable
	}
	authority.mu.Lock()
	if authority.generationExhausted || !authority.blocked || authority.generation != transaction || authority.membershipSession != session || authority.managerEpoch != managerEpoch {
		authority.mu.Unlock()
		authority.Fence()
		return nil, rank2xmpp.ErrUnavailable
	}
	authority.pendingSnapshot = snapshot
	authority.pendingPeers = append([]string{}, currentPeers...)
	authority.mu.Unlock()
	return members, nil
}

// publishCurrentMembership is the second half of the local transaction. The
// topology has already published the complete snapshot while peer transports
// and lanes remain fenced; this exact process-local session capability opens
// Rank2, Rank1, and finally the peer lanes only if no newer authority fence has
// superseded it.
func (authority *rank1MembershipAuthority) publishCurrentMembership(ctx context.Context) error {
	if authority == nil || ctx == nil {
		return rank2xmpp.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	authority.mu.Lock()
	session, managerEpoch, snapshot, messaging, beforeAcknowledge, generation := authority.membershipSession, authority.managerEpoch, authority.pendingSnapshot, authority.messaging, authority.beforeAcknowledge, authority.generation
	valid := !authority.generationExhausted && authority.blocked && !authority.suspended && !authority.ready && session != nil && messaging != nil && managerEpoch != 0 && len(snapshot.Members) != 0
	authority.mu.Unlock()
	if !valid {
		return rank2xmpp.ErrUnavailable
	}
	if beforeAcknowledge != nil {
		beforeAcknowledge()
	}
	if err := authority.client.AcknowledgeAuthoritySnapshotIf(snapshot, func() bool {
		authority.mu.Lock()
		current := !authority.generationExhausted && authority.blocked && !authority.ready && authority.generation == generation && authority.membershipSession == session && authority.managerEpoch == managerEpoch
		authority.mu.Unlock()
		return current && authority.live.LiveAuthorityPublicationCurrent(managerEpoch)
	}); err != nil {
		authority.Fence()
		return err
	}
	if err := authority.live.PublishLiveAuthority(managerEpoch); err != nil {
		authority.Fence()
		return rank2xmpp.ErrUnavailable
	}
	if failure := messaging.PublishCurrentMembership(session); failure != nil {
		authority.Fence()
		return rank2xmpp.ErrUnavailable
	}
	authority.mu.Lock()
	if authority.generationExhausted || !authority.blocked || authority.ready || authority.generation != generation || authority.membershipSession != session || authority.managerEpoch != managerEpoch || len(authority.pendingSnapshot.Members) == 0 {
		authority.mu.Unlock()
		authority.Fence()
		return rank2xmpp.ErrUnavailable
	}
	authority.pendingSnapshot = rank2xmpp.AuthoritySnapshot{}
	authority.currentPeers = append(authority.currentPeers[:0], authority.pendingPeers...)
	authority.pendingPeers = nil
	authority.blocked = false
	authority.ready = true
	authority.signalStateChangedLocked()
	authority.mu.Unlock()
	return nil
}

func (authority *rank1MembershipAuthority) snapshotMembers(current []string) ([]mesh.Identity, error) {
	if len(current) == 0 || len(current) > mesh.MaxMembershipEntries {
		return nil, rank2xmpp.ErrProtocol
	}
	members := make([]mesh.Identity, 0, len(current))
	localFound := false
	for index, full := range current {
		if index > 0 && current[index-1] >= full {
			return nil, rank2xmpp.ErrProtocol
		}
		slash := strings.LastIndexByte(full, '/')
		if slash <= 0 || slash == len(full)-1 {
			return nil, rank2xmpp.ErrProtocol
		}
		bare, resource := full[:slash], full[slash+1:]
		if strings.Contains(bare, "/") || resource == "" {
			return nil, rank2xmpp.ErrProtocol
		}
		identity, err := mesh.NewIdentity(bare, full)
		if err != nil {
			return nil, rank2xmpp.ErrProtocol
		}
		localFound = localFound || full == authority.localFull
		members = append(members, identity)
	}
	if !localFound {
		return nil, rank2xmpp.ErrAuthentication
	}
	return members, nil
}

type rankedEnvelopeCarrier struct {
	live       *transport.Manager
	recovery   rank1Recoverer
	durable    mesh.EnvelopeCarrier
	timeout    time.Duration
	retryDelay time.Duration
	clock      transport.Clock
	authority  *rank1MembershipAuthority

	mu       sync.Mutex
	pending  map[string]*rank1RecoveryDemand
	closed   bool
	wake     func()
	wg       sync.WaitGroup
	lifetime context.Context
	cancel   context.CancelFunc
}

type rank1RecoveryDemand struct {
	again              bool
	automaticRetryUsed bool
}

const rank1RecoveryHealthPollInterval = 25 * time.Millisecond

type rank1Recoverer interface {
	Recover(context.Context, string) error
}

type rank1PathRefresher interface {
	RecoverInPlace(context.Context, string, transport.Transport) error
}

type rank1LivePeerObserver interface {
	LivePeer(string) (transport.Transport, bool)
	IsLivePeer(string, transport.Transport) bool
}

type rank1ExactLivePeerRemover interface {
	RemoveLiveIf(context.Context, string, transport.Transport) error
}

type rank1RestartEligible interface {
	RestartEligible() bool
}

type rank1RecoveryController struct {
	live           rank1LivePeerObserver
	refresh        rank1PathRefresher
	replace        rank1Recoverer
	refreshTimeout time.Duration
}

func (controller *rank1RecoveryController) Recover(ctx context.Context, peerID string) error {
	if controller == nil || controller.live == nil || controller.refresh == nil || controller.replace == nil || ctx == nil || controller.refreshTimeout <= 0 {
		return transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	adapter, eligible := controller.inPlaceCandidate(peerID)
	if !eligible || !controller.isCurrent(peerID, adapter) {
		return controller.replace.Recover(ctx, peerID)
	}
	previous := adapter.Observe().LastProgress
	refresh, cancel := context.WithTimeout(ctx, controller.refreshTimeout)
	err := controller.refresh.RecoverInPlace(refresh, peerID, adapter)
	if err == nil {
		err = controller.waitForFreshHealth(refresh, peerID, adapter, previous)
	}
	cancel()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if remover, ok := controller.live.(rank1ExactLivePeerRemover); ok && controller.isCurrent(peerID, adapter) {
		cleanupTimeout := min(controller.refreshTimeout/3, 5*time.Second)
		if cleanupTimeout <= 0 {
			cleanupTimeout = time.Nanosecond
		}
		cleanup, cancelCleanup := context.WithTimeout(ctx, cleanupTimeout)
		removeErr := remover.RemoveLiveIf(cleanup, peerID, adapter)
		cancelCleanup()
		if removeErr != nil && controller.isCurrent(peerID, adapter) {
			return removeErr
		}
	}
	return controller.replace.Recover(ctx, peerID)
}

func (controller *rank1RecoveryController) waitForFreshHealth(ctx context.Context, peerID string, expected transport.Transport, previous time.Time) error {
	if ctx == nil || expected == nil || previous.IsZero() {
		return transport.ErrUnavailable
	}
	observe := func() (bool, error) {
		if !controller.isCurrent(peerID, expected) {
			return false, transport.ErrUnavailable
		}
		observation := expected.Observe()
		if observation.State == transport.HealthHealthy && observation.LastProgress.After(previous) {
			return true, nil
		}
		switch observation.State {
		case transport.HealthUnknown, transport.HealthFailed, transport.HealthClosed:
			return false, transport.ErrUnavailable
		default:
			return false, nil
		}
	}
	if healthy, err := observe(); healthy || err != nil {
		return err
	}
	ticker := time.NewTicker(rank1RecoveryHealthPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if healthy, err := observe(); healthy || err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (controller *rank1RecoveryController) inPlaceCandidate(peerID string) (adapter transport.Transport, eligible bool) {
	// Live transports are private dependencies, but a faulty observation must
	// still fail closed into bounded replacement rather than panic through the
	// recovery worker.
	defer func() {
		if recover() != nil {
			adapter = nil
			eligible = false
		}
	}()
	adapter, ok := controller.live.LivePeer(peerID)
	if !ok || adapter == nil {
		return nil, false
	}
	observation := adapter.Observe()
	if observation.LastProgress.IsZero() {
		return nil, false
	}
	switch observation.State {
	case transport.HealthConnecting, transport.HealthHealthy, transport.HealthDisconnected:
		return adapter, true
	case transport.HealthFailed:
		candidate, ok := adapter.(rank1RestartEligible)
		return adapter, ok && candidate.RestartEligible()
	default:
		return nil, false
	}
}

func (controller *rank1RecoveryController) isCurrent(peerID string, expected transport.Transport) (current bool) {
	defer func() {
		if recover() != nil {
			current = false
		}
	}()
	return controller.live.IsLivePeer(peerID, expected)
}

func newRankedEnvelopeCarrier(live *transport.Manager, recovery rank1Recoverer, durable mesh.EnvelopeCarrier, timeout, retryDelay time.Duration, clock transport.Clock) *rankedEnvelopeCarrier {
	lifetime, cancel := context.WithCancel(context.Background())
	return &rankedEnvelopeCarrier{
		live: live, recovery: recovery, durable: durable, timeout: timeout, retryDelay: retryDelay,
		clock: clock, pending: make(map[string]*rank1RecoveryDemand), lifetime: lifetime, cancel: cancel,
	}
}

func (carrier *rankedEnvelopeCarrier) Send(ctx context.Context, envelope protocol.Envelope) mesh.CarrierDisposition {
	if carrier == nil || carrier.live == nil || carrier.recovery == nil || carrier.durable == nil || carrier.clock == nil || ctx == nil {
		return mesh.CarrierUnavailable
	}
	if owned, ok := carrier.durable.(interface{ OwnsEnvelope(string) bool }); ok && owned.OwnsEnvelope(envelope.MessageID) {
		return carrier.durable.Send(ctx, envelope)
	}
	observation := carrier.live.ObservePeer(transport.KindLive, envelope.Recipient)
	if (carrier.authority == nil || carrier.authority.AdmitExistingRank1(envelope.Recipient)) && carrier.liveEligible(observation) {
		if err := carrier.live.Send(ctx, transport.KindLive, envelope); err == nil {
			return mesh.CarrierAccepted
		}
	}
	return carrier.sendDurableThenRecover(ctx, envelope)
}

type rankedPendingReceipt struct{ pending transport.PendingLiveReceipt }

func (receipt *rankedPendingReceipt) Binding() mesh.Rank1ReceiptBinding {
	if receipt == nil || receipt.pending == nil {
		return mesh.Rank1ReceiptBinding{}
	}
	binding := receipt.pending.Binding()
	return mesh.Rank1ReceiptBinding{
		MessageID: binding.MessageID, ConversationID: binding.ConversationID,
		Sender: binding.Sender, Recipient: binding.Recipient, MeshID: binding.MeshID,
		ChannelBinding: binding.ChannelBinding,
	}
}

func (receipt *rankedPendingReceipt) Done() <-chan bool {
	if receipt == nil || receipt.pending == nil {
		return nil
	}
	return receipt.pending.Done()
}

func (receipt *rankedPendingReceipt) Close() {
	if receipt != nil && receipt.pending != nil {
		receipt.pending.Close()
	}
}

func (carrier *rankedEnvelopeCarrier) SendWithReceipt(ctx context.Context, envelope protocol.Envelope) (mesh.PendingRank1Receipt, mesh.CarrierDisposition) {
	if carrier == nil || carrier.live == nil || carrier.recovery == nil || carrier.durable == nil || carrier.clock == nil || ctx == nil {
		return nil, mesh.CarrierUnavailable
	}
	if owned, ok := carrier.durable.(interface{ OwnsEnvelope(string) bool }); ok && owned.OwnsEnvelope(envelope.MessageID) {
		return nil, carrier.durable.Send(ctx, envelope)
	}
	observation := carrier.live.ObservePeer(transport.KindLive, envelope.Recipient)
	if (carrier.authority == nil || carrier.authority.AdmitExistingRank1(envelope.Recipient)) && carrier.liveEligible(observation) {
		pending, err := carrier.live.SendLiveTracked(ctx, envelope)
		if err == nil && pending != nil {
			return &rankedPendingReceipt{pending: pending}, mesh.CarrierRank1PendingACK
		}
	}
	return nil, carrier.sendDurableThenRecover(ctx, envelope)
}

func (carrier *rankedEnvelopeCarrier) Fallback(ctx context.Context, envelope protocol.Envelope) mesh.CarrierDisposition {
	if carrier == nil || carrier.durable == nil || ctx == nil {
		return mesh.CarrierUnavailable
	}
	return carrier.sendDurableThenRecover(ctx, envelope)
}

// sendDurableThenRecover preserves the ownership ordering promised by the
// ranked carrier. Direct-path establishment sends control over the same
// serialized durable writer used by the fallback path. Recovery may begin only
// after the durable carrier has completed its handoff attempt; it remains
// advisory and never delays that ownership boundary.
func (carrier *rankedEnvelopeCarrier) sendDurableThenRecover(ctx context.Context, envelope protocol.Envelope) mesh.CarrierDisposition {
	disposition := carrier.durable.Send(ctx, envelope)
	if ctx.Err() == nil {
		carrier.trigger(envelope.Recipient)
	}
	return disposition
}

func (carrier *rankedEnvelopeCarrier) OwnsEnvelope(messageID string) bool {
	if carrier == nil || carrier.durable == nil {
		return false
	}
	owned, ok := carrier.durable.(interface{ OwnsEnvelope(string) bool })
	return ok && owned.OwnsEnvelope(messageID)
}

// liveEligible applies the frozen V1 health policy before ownership can move
// into a private live link. A hard failure is demoted immediately. A temporary
// disconnect may continue using reliable retransmission until the
// thirty-second no-progress boundary; after that boundary the durable path is
// used and a fresh current-membership-authorized private link is demanded.
func (carrier *rankedEnvelopeCarrier) liveEligible(observation transport.Observation) bool {
	if observation.State == transport.HealthHealthy {
		return true
	}
	if observation.State != transport.HealthDisconnected || observation.LastProgress.IsZero() {
		return false
	}
	now := carrier.clock.Now().UTC()
	return !now.IsZero() && !now.Before(observation.LastProgress) && now.Sub(observation.LastProgress) < peer.DemotionThreshold
}

// trigger coalesces establishment per peer. The durable send never waits for
// private peer negotiation, while the service owns and joins every attempt.
func (carrier *rankedEnvelopeCarrier) trigger(peer string) {
	if carrier == nil || carrier.timeout <= 0 || carrier.retryDelay <= 0 || carrier.lifetime == nil || protocol.ValidateAgentIdentity(peer) != nil {
		return
	}
	carrier.mu.Lock()
	if carrier.closed {
		carrier.mu.Unlock()
		return
	}
	if demand := carrier.pending[peer]; demand != nil {
		demand.again = true
		carrier.mu.Unlock()
		return
	}
	demand := &rank1RecoveryDemand{}
	carrier.pending[peer] = demand
	carrier.wg.Add(1)
	carrier.mu.Unlock()
	go func() {
		defer carrier.wg.Done()
		for {
			carrier.mu.Lock()
			if carrier.closed {
				delete(carrier.pending, peer)
				carrier.mu.Unlock()
				return
			}
			carrier.mu.Unlock()
			recovery, cancel := context.WithTimeout(carrier.lifetime, carrier.timeout)
			err := carrier.recovery.Recover(recovery, peer)
			if err == nil {
				// Establishment success proves only that the local link was installed.
				// Keep the demand alive until the link has received authenticated
				// peer traffic (normally the first health reply). A one-sided SCTP or
				// DataChannel open must consume the same bounded retry as any other
				// unavailable recovery attempt.
				err = carrier.waitForHealthyRank1(recovery, peer)
			}
			cancel()
			carrier.mu.Lock()
			retry := demand.again
			if err != nil && !retry && !demand.automaticRetryUsed {
				demand.automaticRetryUsed = true
				retry = true
			}
			if carrier.closed || err == nil || !retry {
				delete(carrier.pending, peer)
				wake := carrier.wake
				carrier.mu.Unlock()
				if err == nil && wake != nil {
					wake()
				}
				return
			}
			demand.again = false
			carrier.mu.Unlock()
			timer := time.NewTimer(carrier.retryDelay)
			select {
			case <-timer.C:
			case <-carrier.lifetime.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				carrier.mu.Lock()
				delete(carrier.pending, peer)
				carrier.mu.Unlock()
				return
			}
		}
	}()
}

func (carrier *rankedEnvelopeCarrier) waitForHealthyRank1(ctx context.Context, peerID string) error {
	if carrier == nil || carrier.live == nil || ctx == nil || protocol.ValidateAgentIdentity(peerID) != nil {
		return transport.ErrUnavailable
	}
	observe := func() (bool, error) {
		observation := carrier.live.ObservePeer(transport.KindLive, peerID)
		switch observation.State {
		case transport.HealthHealthy:
			return true, nil
		case transport.HealthUnknown, transport.HealthFailed, transport.HealthClosed:
			return false, transport.ErrUnavailable
		default:
			return false, nil
		}
	}
	if healthy, err := observe(); healthy || err != nil {
		return err
	}
	ticker := time.NewTicker(rank1RecoveryHealthPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if healthy, err := observe(); healthy || err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (carrier *rankedEnvelopeCarrier) stop() {
	if carrier == nil {
		return
	}
	carrier.mu.Lock()
	if carrier.closed {
		carrier.mu.Unlock()
		return
	}
	carrier.closed = true
	cancel := carrier.cancel
	if cancel != nil {
		cancel()
	}
	carrier.mu.Unlock()
}

func (carrier *rankedEnvelopeCarrier) wait() {
	if carrier == nil {
		return
	}
	carrier.wg.Wait()
}

func (carrier *rankedEnvelopeCarrier) recoveryInProgress(peer string) bool {
	if carrier == nil {
		return false
	}
	carrier.mu.Lock()
	inProgress := carrier.pending[peer] != nil
	carrier.mu.Unlock()
	return inProgress
}

// ClosePeer is invoked only by the terminal owner of that peer's lane. It
// removes the live path without touching the mesh-wide durable session.
func (carrier *rankedEnvelopeCarrier) ClosePeer(ctx context.Context, peerID string) error {
	if carrier == nil || carrier.live == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	return carrier.live.RemoveLive(ctx, peerID)
}

type rank1InboundPump struct {
	source         *transport.Manager
	receiver       any
	authority      *rank1MembershipAuthority
	localRecipient string
	meshID         string
	receiptTimeout time.Duration
}

type rank1EnvelopeReceiver interface {
	ReceiveRank1WithReceipt(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope, mesh.InboundRank1Receipt) *mesh.Failure
}

type quarantinedRank1EnvelopeReceiver interface {
	QuarantineRank1WithReceipt(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope, mesh.InboundRank1Receipt) *mesh.Failure
}

type legacyRank1EnvelopeReceiver interface {
	ReceiveRank1(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) (bool, *mesh.Failure)
}

type rank1InboundReceipt struct{ receipt transport.InboundLiveReceipt }

func (receipt *rank1InboundReceipt) Binding() mesh.Rank1ReceiptBinding {
	if receipt == nil || receipt.receipt == nil {
		return mesh.Rank1ReceiptBinding{}
	}
	binding := receipt.receipt.Binding()
	return mesh.Rank1ReceiptBinding{
		MessageID: binding.MessageID, ConversationID: binding.ConversationID,
		Sender: binding.Sender, Recipient: binding.Recipient, MeshID: binding.MeshID,
		ChannelBinding: binding.ChannelBinding,
	}
}

func (receipt *rank1InboundReceipt) Acknowledge(ctx context.Context) error {
	if receipt == nil || receipt.receipt == nil {
		return transport.ErrAuthentication
	}
	return receipt.receipt.Acknowledge(ctx)
}

func (receipt *rank1InboundReceipt) Close() {
	if receipt != nil && receipt.receipt != nil {
		receipt.receipt.Close()
		receipt.receipt = nil
	}
}

func (pump *rank1InboundPump) Run(ctx context.Context) *mesh.Failure {
	if pump == nil || pump.source == nil || pump.receiver == nil || ctx == nil || pump.receiptTimeout <= 0 || pump.receiptTimeout > 30*time.Second {
		return &mesh.Failure{Code: mesh.FailureInternal}
	}
	for {
		received, err := pump.source.ReceiveWithKind(ctx)
		if err != nil {
			return classifyRank1PumpError(ctx, err)
		}
		authentication := received.Authentication
		if received.Kind != transport.KindLive || authentication == nil || authentication.Peer != received.Envelope.Sender || authentication.MeshID != pump.meshID {
			clearRank2Envelope(received.Envelope)
			continue
		}
		membershipReady := pump.authority == nil || pump.authority.DataReady()
		if membershipReady && pump.authority != nil && !pump.authority.AdmitExistingRank1(authentication.Peer) {
			clearRank2Envelope(received.Envelope)
			continue
		}
		provenance, provenanceErr := mesh.NewBoundRank1Provenance(authentication.Peer, pump.localRecipient, pump.meshID, authentication.ChannelBinding)
		if provenanceErr != nil {
			clearRank2Envelope(received.Envelope)
			continue
		}
		envelope := received.Envelope.Clone()
		var failure *mesh.Failure
		if receiver, ok := pump.receiver.(rank1EnvelopeReceiver); ok {
			liveReceipt, receiptErr := pump.source.TakeLiveReceipt(received)
			if receiptErr != nil {
				clearRank2Envelope(envelope)
				clearRank2Envelope(received.Envelope)
				continue
			}
			receipt := &rank1InboundReceipt{receipt: liveReceipt}
			if !membershipReady {
				quarantine, supported := pump.receiver.(quarantinedRank1EnvelopeReceiver)
				if !supported {
					receipt.Close()
					clearRank2Envelope(envelope)
					clearRank2Envelope(received.Envelope)
					continue
				}
				failure = quarantine.QuarantineRank1WithReceipt(ctx, provenance, envelope, receipt)
			} else {
				failure = receiver.ReceiveRank1WithReceipt(ctx, provenance, envelope, receipt)
			}
		} else if receiver, ok := pump.receiver.(legacyRank1EnvelopeReceiver); ok && membershipReady {
			var eligible bool
			eligible, failure = receiver.ReceiveRank1(ctx, provenance, envelope)
			if failure == nil && eligible {
				receiptContext, cancelReceipt := context.WithTimeout(ctx, pump.receiptTimeout)
				_ = pump.source.AcknowledgeLive(receiptContext, received)
				cancelReceipt()
			}
		} else if membershipReady {
			failure = &mesh.Failure{Code: mesh.FailureInternal}
		}
		clearRank2Envelope(envelope)
		clearRank2Envelope(received.Envelope)
		if failure != nil && (ctx.Err() != nil || failure.Code == mesh.FailureInternal) {
			return failure
		}
	}
}

func classifyRank1PumpError(ctx context.Context, err error) *mesh.Failure {
	if ctx != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return &mesh.Failure{Code: mesh.FailureCancelled}
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &mesh.Failure{Code: mesh.FailureDeadline}
		}
	}
	if errors.Is(err, transport.ErrClosed) || errors.Is(err, transport.ErrUnavailable) {
		return &mesh.Failure{Code: mesh.FailureUnavailable}
	}
	return &mesh.Failure{Code: mesh.FailureInternal}
}

type rank1MessagingRuntime struct {
	live       *transport.Manager
	handshakes *handshake.Manager
	assembly   *rank1webrtc.HandshakeNegotiator
	carrier    *rankedEnvelopeCarrier
	signals    *rank1webrtc.SignalPump
	inbound    *rank1InboundPump
	client     *rank2xmpp.Client
	authority  *rank1MembershipAuthority
	timeout    time.Duration
	retryDelay time.Duration
}

type authenticatedMessagingService struct {
	messaging *mesh.MessagingService
	pump      *rank2InboundPump
	rank1     *rank1MessagingRuntime
	payloads  *authenticatedPayloadRuntime
	runtime   *messagingRuntimeBridge

	mu                 sync.Mutex
	started            bool
	pumpStop           context.CancelFunc
	pumpDone           chan struct{}
	workerFailureName  string
	workerFailureCount uint64

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  *sessionkernel.ProviderError
}

type namedMessagingWorker struct {
	name string
	run  func(context.Context) error
}

func newAuthenticatedMessagingService(messaging *mesh.MessagingService, pump *rank2InboundPump, rank1 *rank1MessagingRuntime, runtime *messagingRuntimeBridge, payloads ...*authenticatedPayloadRuntime) *authenticatedMessagingService {
	var payloadRuntime *authenticatedPayloadRuntime
	if len(payloads) == 1 {
		payloadRuntime = payloads[0]
	}
	return &authenticatedMessagingService{messaging: messaging, pump: pump, rank1: rank1, payloads: payloadRuntime, runtime: runtime, shutdownDone: make(chan struct{})}
}

func (*authenticatedMessagingService) Name() string { return "authenticated-messaging" }

func (service *authenticatedMessagingService) Start(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || service.messaging == nil || service.pump == nil || service.rank1 == nil || service.rank1.live == nil || service.rank1.handshakes == nil || service.rank1.carrier == nil || service.rank1.signals == nil || service.rank1.inbound == nil || service.rank1.client == nil || service.rank1.timeout <= 0 || service.rank1.retryDelay <= 0 || ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	if err := service.rank1.live.Start(ctx); err != nil {
		service.shutdownOnce.Do(service.cleanup)
		return classifyConnectivityError(ctx, err)
	}
	if failure := service.messaging.Start(ctx); failure != nil {
		// A failed Service.Start is self-cleaning by sessionkernel contract; the
		// graph will not acquire or later stop this unpublished service.
		service.shutdownOnce.Do(service.cleanup)
		return providerMeshFailure(failure)
	}
	if service.rank1.authority != nil && service.rank1.authority.dynamic {
		if err := service.rank1.authority.openDynamic(ctx); err != nil {
			service.shutdownOnce.Do(service.cleanup)
			return classifyConnectivityError(ctx, err)
		}
	}
	if failure := connectivityContextError(ctx); failure != nil {
		service.shutdownOnce.Do(service.cleanup)
		return failure
	}
	service.mu.Lock()
	if service.started {
		service.mu.Unlock()
		return nil
	}
	pumpContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	service.started = true
	service.pumpStop = cancel
	service.pumpDone = done
	service.mu.Unlock()
	workers := []namedMessagingWorker{
		{name: "rank2-inbound", run: func(runCtx context.Context) error {
			failure := service.pump.Run(runCtx)
			if failure == nil || failure.Code == mesh.FailureCancelled && runCtx.Err() != nil {
				return nil
			}
			return errors.New("rank2 inbound stopped")
		}},
		{name: "rank1-signals", run: func(runCtx context.Context) error { return service.rank1.signals.Run(runCtx) }},
		{name: "rank1-inbound", run: func(runCtx context.Context) error {
			failure := service.rank1.inbound.Run(runCtx)
			if failure == nil || failure.Code == mesh.FailureCancelled && runCtx.Err() != nil {
				return nil
			}
			return errors.New("rank1 inbound stopped")
		}},
		{name: "rank2-state", run: service.monitorRank2ForRank1},
	}
	if service.rank1.authority == nil || !service.rank1.authority.dynamic {
		workers = append(workers, namedMessagingWorker{name: "authority", run: service.monitorAuthorityChanges})
	}
	go func() {
		defer close(done)
		runMessagingWorkerSupervisor(pumpContext, service.rank1.retryDelay, workers, service.recordWorkerFailure)
	}()
	if service.runtime == nil || !service.runtime.registerDiagnostics(service) {
		service.shutdownOnce.Do(service.cleanup)
		return providerError(sessionkernel.ProviderInternal)
	}
	return nil
}

// runMessagingWorkerSupervisor gives every long-lived consumer an independent
// sequential restart loop. One worker's failure cannot cancel or join healthy
// siblings, and a replacement generation begins only after that exact worker
// returned. The top-level owner still joins every cooperative worker when the
// shared service context is cancelled.
func runMessagingWorkerSupervisor(ctx context.Context, retryDelay time.Duration, workers []namedMessagingWorker, onFailure func(string, error)) {
	if ctx == nil || retryDelay <= 0 || len(workers) == 0 {
		return
	}
	var supervised sync.WaitGroup
	supervised.Add(len(workers))
	for _, worker := range workers {
		worker := worker
		go func() {
			defer supervised.Done()
			runMessagingWorker(ctx, retryDelay, worker, onFailure)
		}()
	}
	supervised.Wait()
}

func runMessagingWorker(ctx context.Context, retryDelay time.Duration, worker namedMessagingWorker, onFailure func(string, error)) {
	for ctx.Err() == nil {
		failure := callMessagingWorker(ctx, worker.run)
		if ctx.Err() != nil {
			return
		}
		if failure == nil {
			failure = errors.New("messaging worker stopped")
		}
		if onFailure != nil {
			onFailure(worker.name, failure)
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func callMessagingWorker(ctx context.Context, operation func(context.Context) error) (err error) {
	if ctx == nil || operation == nil {
		return errors.New("invalid messaging worker")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("messaging worker panic")
		}
	}()
	return operation(ctx)
}

func (service *authenticatedMessagingService) recordWorkerFailure(name string, _ error) {
	if service == nil {
		return
	}
	service.mu.Lock()
	service.workerFailureName = name
	service.workerFailureCount++
	service.mu.Unlock()
}

func (service *authenticatedMessagingService) monitorRank2ForRank1(ctx context.Context) error {
	for {
		changed := service.rank1.client.StateChanged()
		authorityChanged, authorityVersion, authorityReady := service.rank1.authority.observeState()
		state := service.rank1.client.DurableState()
		if state == rank2xmpp.DurablePending || state == rank2xmpp.DurableUnknown {
			if service.rank1.client.TerminalFailure() != nil {
				service.messaging.FailPendingRequestsOnAuthenticationRejection()
			}
			// The transport callback fences admission synchronously. Cleanup is
			// the slower phase and must fully retire peers before any reconnect
			// publication can restore application traffic.
			if service.rank1.authority.dynamic {
				operation, cancel := context.WithTimeout(ctx, service.rank1.timeout)
				err := service.messaging.DropDynamicAuthority(operation)
				cancel()
				if err != nil {
					if !waitRank2AuthorityRetry(ctx, changed, service.rank1.retryDelay) {
						return nil
					}
					continue
				}
			}
		} else if state == rank2xmpp.DurableLive {
			// StateChanged is lossless as an edge but multiple transitions may
			// coalesce before this goroutine reads them. A fresh replacement can
			// therefore already be Live while its staged authority transaction
			// still needs publication. Composite authority readiness is the level
			// condition: Client readiness alone becomes true before Rank1 and peer
			// lanes are published.
			if authorityReady {
				service.messaging.WakeDelivery()
				select {
				case <-changed:
					continue
				case <-authorityChanged:
					if !waitRank2AuthoritySettlement(ctx, changed, service.rank1.authority, authorityVersion, service.rank1.timeout) {
						return nil
					}
					continue
				case <-ctx.Done():
					return nil
				}
			}
			operation, cancel := context.WithTimeout(ctx, service.rank1.timeout)
			var failure *mesh.Failure
			if service.rank1.authority.dynamic {
				if err := service.rank1.authority.openDynamic(operation); err != nil {
					failure = &mesh.Failure{Code: mesh.FailureUnavailable}
				}
			} else {
				failure = service.messaging.EnsureCurrentMembership(operation)
			}
			cancel()
			if failure == nil {
				// A wake before PublishCurrentMembership is consumed while peer
				// admission remains fenced. Wake only after the whole authority
				// transaction has opened Rank2, Rank1, and peer lanes.
				service.messaging.WakeDelivery()
			} else {
				if !waitRank2AuthorityRetry(ctx, changed, service.rank1.retryDelay) {
					return nil
				}
				continue
			}
		}
		select {
		case <-changed:
		case <-authorityChanged:
			if !waitRank2AuthoritySettlement(ctx, changed, service.rank1.authority, authorityVersion, service.rank1.timeout) {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// waitRank2AuthoritySettlement coalesces the paired start/settlement edges of
// one authority transaction. The second edge is either successful publication
// or a failure fence; the timeout is only a guard against a non-cooperative
// dependency that emits neither terminal edge.
func waitRank2AuthoritySettlement(ctx context.Context, durableChanged <-chan struct{}, authority *rank1MembershipAuthority, observedVersion uint64, delay time.Duration) bool {
	if ctx == nil || authority == nil {
		return false
	}
	if delay <= 0 {
		delay = time.Nanosecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		authorityChanged, version, ready := authority.observeState()
		if ready || version-observedVersion >= 2 {
			return true
		}
		select {
		case <-durableChanged:
			return true
		case <-authorityChanged:
			continue
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

type rank2MembershipReadiness interface {
	MembershipReady() bool
}

func rank2MembershipRefreshRequired(readiness rank2MembershipReadiness, observedPending bool) bool {
	return readiness == nil || !readiness.MembershipReady()
}

func waitRank2AuthorityRetry(ctx context.Context, changed <-chan struct{}, delay time.Duration) bool {
	if ctx == nil {
		return false
	}
	if delay <= 0 {
		delay = time.Nanosecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-changed:
		return true
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (service *authenticatedMessagingService) monitorAuthorityChanges(ctx context.Context) error {
	for {
		err := service.rank1.client.ReceiveAuthorityChange(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, rank2xmpp.ErrUnavailable) {
				// A wake from a retired ingress was superseded by reconnect. The
				// state monitor owns replacement-session authority publication.
				continue
			}
			if errors.Is(err, rank2xmpp.ErrClosed) {
				return nil
			}
			service.rank1.authority.Fence()
			return err
		}
		// The parser fenced this exact ingress synchronously before publishing
		// the change event. RefreshCurrentMembership serializes the replacement
		// transaction; an extra fence here could split its publication.
		for service.rank1.client.DurableState() == rank2xmpp.DurableLive && !service.rank1.authority.MembershipReady() {
			changed := service.rank1.client.StateChanged()
			operation, cancel := context.WithTimeout(ctx, service.rank1.timeout)
			syncFailure := service.messaging.EnsureCurrentMembership(operation)
			cancel()
			if syncFailure == nil {
				service.messaging.WakeDelivery()
				break
			}
			// A failed publication does not necessarily change the durable-link state.
			// Retry with bounded backoff while the same durable session is live.
			if !waitRank2AuthorityRetry(ctx, changed, service.rank1.retryDelay) {
				return nil
			}
		}
	}
}

func (service *authenticatedMessagingService) Shutdown(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	service.shutdownOnce.Do(func() { go service.cleanup() })
	select {
	case <-service.shutdownDone:
		return service.shutdownResult()
	case <-ctx.Done():
		return connectivityContextError(ctx)
	}
}

func (service *authenticatedMessagingService) cleanup() {
	var result *sessionkernel.ProviderError
	record := func(failure *sessionkernel.ProviderError) {
		if result == nil && failure != nil {
			result = &sessionkernel.ProviderError{Code: failure.Code}
		}
	}
	if service.runtime != nil {
		service.runtime.unregisterDiagnostics(service)
	}
	service.mu.Lock()
	cancel, done := service.pumpStop, service.pumpDone
	service.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	// Fence new recovery demand before canceling Messaging operations. The
	// Messaging service must terminally join those operations before any live
	// dependency they may borrow is closed; otherwise Manager.Close can wait on
	// a Send whose cancellation source is still waiting behind Manager.Close.
	if service.rank1 != nil && service.rank1.carrier != nil {
		service.rank1.carrier.stop()
	}
	if service.messaging != nil {
		record(providerMeshFailure(service.messaging.Shutdown(context.Background())))
	}
	if service.rank1 != nil {
		if service.rank1.handshakes != nil {
			service.rank1.handshakes.Close()
		}
		if service.rank1.assembly != nil {
			record(classifyConnectivityError(nil, service.rank1.assembly.Close(context.Background())))
		}
		if service.rank1.carrier != nil {
			service.rank1.carrier.wait()
		}
		if service.rank1.live != nil {
			record(classifyConnectivityError(nil, service.rank1.live.Close(context.Background())))
		}
	}
	if service.payloads != nil {
		service.payloads.Close()
	}
	service.mu.Lock()
	service.shutdownErr = result
	service.mu.Unlock()
	close(service.shutdownDone)
}

func (service *authenticatedMessagingService) shutdownResult() *sessionkernel.ProviderError {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.shutdownErr == nil {
		return nil
	}
	return &sessionkernel.ProviderError{Code: service.shutdownErr.Code}
}

func (service *authenticatedMessagingService) diagnosticSnapshot() authenticatedDiagnosticSnapshot {
	if service == nil || service.messaging == nil {
		return authenticatedDiagnosticSnapshot{}
	}
	messaging := service.messaging.Diagnostics()
	queuedMessages := messaging.QueuedMessages
	payloadTransfers := uint64(0)
	if service.payloads != nil {
		payloadTransfers = service.payloads.activeTransfers()
	}
	return authenticatedDiagnosticSnapshot{
		peerCount: messaging.PeerCount, queuedMessages: queuedMessages,
		pendingRPC: messaging.PendingRPCRequests, payloadTransfers: payloadTransfers,
	}
}

func saturatingDiagnosticSum(left, right uint64) uint64 {
	const maximum = uint64(65_536)
	if left >= maximum || right >= maximum-left {
		return maximum
	}
	return left + right
}

func (service *authenticatedMessagingService) operations() sessionkernel.AuthenticatedOperations {
	return sessionkernel.AuthenticatedOperations{
		MeshList: service.meshList, MeshRefresh: service.meshRefresh,
		MessageSend: service.messageSend, MessageRequest: service.messageRequest, MessageReply: service.messageReply,
		DeliveryRetry: service.deliveryRetry, DeliveryDrop: service.deliveryDrop,
		ConversationList: service.conversationList, ConversationStatus: service.conversationStatus, ConversationClose: service.conversationClose,
		DiagnosticsPeer: service.diagnosticsPeer,
	}
}

func (service *authenticatedMessagingService) diagnosticsPeer(ctx context.Context, _ coreruntime.Services, args model.DiagnosticsPeerArgs) (model.PeerStatus, *sessionkernel.ProviderError) {
	if service == nil || service.rank1 == nil || service.rank1.live == nil || ctx == nil || args.Peer == "" || strings.Contains(args.Peer, "/") || protocol.ValidateAgentIdentity(args.Peer) != nil {
		return model.PeerStatus{}, providerError(sessionkernel.ProviderRejected)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return model.PeerStatus{}, failure
	}
	endpoints, endpointFailure := service.messaging.CurrentPeerEndpoints(ctx, args.Peer)
	if endpointFailure != nil {
		emitRank1Evidence(rank1EvidenceRecord{Event: "peer_diagnostic", Reason: "endpoint_resolution"})
		if contextFailure := connectivityContextError(ctx); contextFailure != nil {
			return model.PeerStatus{}, contextFailure
		}
	}
	result := aggregatePeerStatus(args.Peer, endpoints, service.rank1.live, service.rank1.carrier)
	emitRank1Evidence(rank1EvidenceRecord{Event: "peer_diagnostic", EndpointCount: len(endpoints), Connectivity: result.Connectivity, Reachable: result.Reachable, RecoveryPending: result.RecoveryInProgress})
	return result, nil
}

func aggregatePeerStatus(peer string, endpoints []string, live *transport.Manager, carrier *rankedEnvelopeCarrier) model.PeerStatus {
	result := model.PeerStatus{Peer: peer, Connectivity: "unavailable"}
	if live == nil {
		return result
	}
	healthy, connecting, recovering := false, false, false
	for _, endpoint := range endpoints {
		observation := live.ObservePeer(transport.KindLive, endpoint)
		healthy = healthy || observation.State == transport.HealthHealthy
		connecting = connecting || observation.State == transport.HealthConnecting
		recovering = recovering || observation.State == transport.HealthConnecting || carrier != nil && carrier.recoveryInProgress(endpoint)
	}
	result.RecoveryInProgress = recovering
	switch {
	case healthy:
		result.Connectivity, result.Reachable = "available", true
	case connecting:
		result.Connectivity = "degraded"
	case recovering:
		result.Connectivity = "degraded"
	}
	return result
}

func (service *authenticatedMessagingService) meshList(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.MeshListResult, *sessionkernel.ProviderError) {
	result, failure := service.messaging.MeshList(ctx)
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) meshRefresh(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return model.EmptyResult{}, providerMeshFailure(service.messaging.MeshRefresh(ctx))
}

func (service *authenticatedMessagingService) messageSend(ctx context.Context, _ coreruntime.Services, args model.MessageSendArgs) (model.SendResult, *sessionkernel.ProviderError) {
	if failure := service.terminalConnectivityFailure(); failure != nil {
		return model.SendResult{}, failure
	}
	result, failure := service.messaging.MessageSend(ctx, args)
	if failure != nil {
		if terminal := service.terminalConnectivityFailure(); terminal != nil {
			return model.SendResult{}, terminal
		}
	}
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) messageRequest(ctx context.Context, _ coreruntime.Services, args model.MessageRequestArgs) (model.ResponseResult, *sessionkernel.ProviderError) {
	if failure := service.terminalConnectivityFailure(); failure != nil {
		return model.ResponseResult{}, failure
	}
	result, failure := service.messaging.MessageRequest(ctx, args)
	if failure != nil {
		if terminal := service.terminalConnectivityFailure(); terminal != nil {
			return model.ResponseResult{}, terminal
		}
	}
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) messageReply(ctx context.Context, _ coreruntime.Services, args model.MessageReplyArgs) (model.SendResult, *sessionkernel.ProviderError) {
	if failure := service.terminalConnectivityFailure(); failure != nil {
		return model.SendResult{}, failure
	}
	result, failure := service.messaging.MessageReply(ctx, args)
	if failure != nil {
		if terminal := service.terminalConnectivityFailure(); terminal != nil {
			return model.SendResult{}, terminal
		}
	}
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) terminalConnectivityFailure() *sessionkernel.ProviderError {
	if service == nil || service.rank1 == nil || service.rank1.client == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	if err := service.rank1.client.TerminalFailure(); err != nil {
		return classifyConnectivityError(nil, err)
	}
	return nil
}

func (service *authenticatedMessagingService) deliveryRetry(ctx context.Context, _ coreruntime.Services, args model.MessageIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return model.EmptyResult{}, providerMeshFailure(service.messaging.DeliveryRetry(ctx, args.MessageID))
}

func (service *authenticatedMessagingService) deliveryDrop(ctx context.Context, _ coreruntime.Services, args model.MessageIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return model.EmptyResult{}, providerMeshFailure(service.messaging.DeliveryDrop(ctx, args.MessageID))
}

func (service *authenticatedMessagingService) conversationList(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.ConversationListResult, *sessionkernel.ProviderError) {
	result, failure := service.messaging.ConversationList(ctx)
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) conversationStatus(ctx context.Context, _ coreruntime.Services, args model.ConversationIDArgs) (model.ConversationStatus, *sessionkernel.ProviderError) {
	result, failure := service.messaging.ConversationStatus(ctx, args.ConversationID)
	return result, providerMeshFailure(failure)
}

func (service *authenticatedMessagingService) conversationClose(ctx context.Context, _ coreruntime.Services, args model.ConversationIDArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	return model.EmptyResult{}, providerMeshFailure(service.messaging.ConversationClose(ctx, args.ConversationID))
}
