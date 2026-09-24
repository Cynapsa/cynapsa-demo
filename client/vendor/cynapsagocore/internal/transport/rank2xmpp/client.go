// Package rank2xmpp implements the private durable delivery adapter.
package rank2xmpp

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type StanzaKind uint8

const (
	StanzaEnvelope StanzaKind = iota + 1
	StanzaSignal
	StanzaSignalResult
	StanzaTransferManifest
	StanzaTransferChunk
	StanzaTransferFinish
	StanzaTransferCompletion
	StanzaTransferAbort
	// StanzaTimeCalibration accounts for the authenticated XEP-0202 IQ in
	// the XEP-0198 stream ledger. It is never exposed as application data or
	// replayed onto a replacement clean session.
	StanzaTimeCalibration
	// StanzaAuthoritySync is a private, session-local peer revalidation IQ.
	// It is correlated and handled but never replayed as application traffic.
	StanzaAuthoritySync
	// StanzaAuthorityDiscovery accounts for the mandatory fresh-session
	// XEP-0030 authority capability query. It is session-local control state.
	StanzaAuthorityDiscovery
	// StanzaUploadSlotQuery accounts for one authenticated XEP-0363 slot IQ.
	// A slot is session-local private control state: it is never exposed as
	// application data and is never replayed onto a replacement clean session.
	StanzaUploadSlotQuery
	// StanzaExternalServiceQuery accounts for one authenticated XEP-0215 IQ.
	// Discovery and credential results are session-local control state and are
	// never replayed onto a clean replacement session.
	StanzaExternalServiceQuery
	// StanzaSessionPingResult accounts for an automatic XEP-0199 response to
	// the authenticated server domain. Unlike request/response control state,
	// this exact IQ result is replay-safe on a continued XEP-0198 stream. It is
	// still discarded rather than replayed on a clean replacement session.
	StanzaSessionPingResult
	StanzaObjectReadinessRequest
	StanzaObjectReadinessResult
	StanzaObjectTransfer
	StanzaObjectTransferFailure
	StanzaObjectTransferAbort
	// Private server-authorized peer lookup; never replayed on a new session.
	StanzaPeerAuthorization
	// Server revoke action acknowledgement after owner cleanup.
	StanzaPeerRevocationAck
	// Immediate IQ result for a server revoke packet. This is not proof that
	// owner cleanup finished; that proof is StanzaPeerRevocationAck.
	StanzaPeerRevocationPacketAck
	// Outbound idle liveness IQ to the authenticated server domain.
	StanzaSessionPingQuery
)

type Stanza struct {
	Kind          StanzaKind
	From          string
	To            string
	MeshID        string
	Ordinal       uint64
	AttemptID     string
	TransferID    string
	MessageID     string
	Data          []byte
	Evidence      payload.CompletionEvidence
	inboundLease  *transport.InboundLease
	inboundAccept *inboundAcceptance
}

func (s Stanza) clone() Stanza {
	s.inboundLease = nil
	s.inboundAccept = nil
	s.Data = append([]byte(nil), s.Data...)
	return s
}

func (s Stanza) streamManagementRecord() Stanza {
	s.inboundLease = nil
	s.inboundAccept = nil
	if s.Kind == StanzaEnvelope {
		s.Data = nil
		return s
	}
	s.Data = append([]byte(nil), s.Data...)
	return s
}

type EventKind uint8

const (
	EventStanza EventKind = iota + 1
	EventHandled
	EventCustodyAccepted
	EventMailboxComplete
	EventMembershipChanged
	EventJingleFailure
	// EventResumeAuthorityResult is the strict server-originated continuity
	// result sent after ejabberd's post-resume <r/> phase separator. It is
	// completed on the dedicated control worker after every preceding replayed
	// authority item has been processed.
	EventResumeAuthorityResult
	EventPeerRevoked
	EventRoutingFailure
)

type Event struct {
	Kind              EventKind
	Stanza            Stanza
	HandledThrough    uint64
	HandledCount      uint32
	MessageID         string
	AttemptID         string
	sessionGeneration uint64
	inboundLease      *transport.InboundLease
	resumeBarrier     *resumeAuthorityBarrier
	Revocations       []Revocation
	ControlID         string
	ErrorCondition    string
}

// resumeAuthorityBarrier is the production resume-continuity proof. The
// server appends its result after the complete pre-resume outbound FIFO and
// after its exact-session readiness handoff. Closing done therefore releases
// retained peer traffic only after the dedicated control worker has processed
// every preceding authority event.
type resumeAuthorityBarrier struct {
	done     chan struct{}
	ready    atomic.Bool
	rejected atomic.Bool
}

func isControlEvent(kind EventKind) bool {
	switch kind {
	case EventHandled, EventCustodyAccepted, EventMailboxComplete, EventMembershipChanged, EventJingleFailure, EventResumeAuthorityResult, EventPeerRevoked, EventRoutingFailure:
		return true
	default:
		return false
	}
}

type Authentication struct {
	Username        string
	Password        []byte
	MeshID          string
	SessionResource string
}

type Credential struct {
	Password    []byte
	UsableUntil time.Time
}

type CredentialSource func(context.Context) (Credential, error)

type SessionExpiryMode uint8

const (
	SessionExpiryContinue SessionExpiryMode = iota
	SessionExpiryDisconnect
)

type Authenticated struct {
	BareIdentity  string
	BoundIdentity string
	Proof         []byte
}

// AuthenticatedInbound preserves the sender authenticated by the XMPP stanza
// separately from the untrusted envelope bytes. Downstream policy must use
// AuthenticatedSender as transport provenance and must never derive provenance
// from Envelope.Sender alone.
type AuthenticatedInbound struct {
	AuthenticatedSender string
	Envelope            protocol.Envelope
	inboundLease        *transport.InboundLease
	inboundAccept       *inboundAcceptance
}

func (inbound AuthenticatedInbound) clone() AuthenticatedInbound {
	inbound.inboundLease = nil
	inbound.inboundAccept = nil
	inbound.Envelope = inbound.Envelope.Clone()
	return inbound
}

// Accept confirms that the receiving SDK/runtime has safely assumed ownership
// of this exact envelope. Production uses it to advance the recipient's
// XEP-0198 handled counter; injected sessions without a receipt are a no-op.
func (inbound *AuthenticatedInbound) Accept(ctx context.Context) error {
	if inbound == nil {
		return ErrInvalidConfig
	}
	acceptance := inbound.inboundAccept
	inbound.inboundAccept = nil
	return acceptance.accept(ctx)
}

// Reject leaves the envelope unhandled so the exact-resource server mailbox
// can replay it after transport recovery.
func (inbound *AuthenticatedInbound) Reject() {
	if inbound == nil {
		return
	}
	acceptance := inbound.inboundAccept
	inbound.inboundAccept = nil
	acceptance.reject()
}

// Session is the injected XMPP/XEP implementation. The explicit phases let
// Client prove TLS -> SASL -> resource bind -> XEP-0198 ordering in tests.
type Session interface {
	ConnectTLS(context.Context, string) error
	Authenticate(context.Context, string, []byte) (bareIdentity string, proof []byte, err error)
	BindResource(context.Context, string) (string, error)
	EnableStreamManagement(context.Context, bool) error
	// Send borrows the stanza only until it returns. Implementations must clone
	// it before retaining it for replay or asynchronous processing.
	Send(context.Context, Stanza) error
	Receive(context.Context) (Event, error)
	// Resume completes the injected session's resume contract. Production
	// sessions implement preparedResumeSession so Client can authorize the
	// exact resumed stream before any application replay.
	Resume(context.Context) (bool, error)
	CatchUp(context.Context, int) ([]Stanza, error)
	Close(context.Context) error
}

// sessionInterrupter is the private fail-stop seam for a published session
// whose writer outlived its operation context. It must interrupt raw I/O
// without graceful protocol writes and without clearing stream-management or
// outbox replay metadata. The caller joins the interruption callback before
// releasing send ownership, so a stale callback cannot target a replacement
// connection installed into the same Session object.
type sessionInterrupter interface {
	interruptIO()
}

type Dialer interface {
	Dial(context.Context) (Session, error)
}

type authorityIngressFenceSession interface {
	setAuthorityIngressFence(func(uint64) bool)
}

type dynamicPeerAuthoritySession interface {
	setDynamicPeerAuthority(bool)
}

type controlEventSession interface {
	ReceiveControl(context.Context) (Event, error)
}

type resumeAuthorityBarrierSession interface {
	WaitResumeAuthorityResult(context.Context) error
}

type authorityDiscoverySession interface {
	DiscoverAuthority(context.Context) error
}

type pendingSession interface {
	PendingForReplay(context.Context) ([]Stanza, error)
}

// preparedResumeSession separates XEP-0198 negotiation from retransmission.
// A membership-gated client must authenticate the resumed session's complete
// current snapshot before allowing retained application stanzas back on wire.
type preparedResumeSession interface {
	PrepareResume(context.Context) (bool, error)
	ReplayPrepared(context.Context, func(Stanza) bool, func(Stanza) (preparedReplayStanza, bool)) error
}

// preparedReplayPeersSession exposes only recipient metadata, never payload
// bytes, so dynamic resume can refresh server authority before retransmission.
type preparedReplayPeersSession interface {
	PreparedReplayPeers(context.Context) ([]string, error)
}

type preparedReplayStanza struct {
	Stanza  Stanza
	Release func()
}

type TransferReceiver interface {
	// HandleStanza borrows route and stanza only through the callback. Receivers
	// must clone their values before retaining them or using them asynchronously.
	HandleStanza(context.Context, payload.CarrierRoute, Stanza) (*payload.CompletionEvidence, error)
}

// AuthenticatedPayloadWorkAdmitter moves authenticated file/control work into
// the common current-membership peer lane before any receiver or readiness
// dependency is invoked.
type AuthenticatedPayloadWorkAdmitter interface {
	AdmitAuthenticatedPayloadWork(context.Context, string, uint64, func(context.Context) error) error
}

type RouteResolver interface {
	ResolveTransferRoute(string) (payload.CarrierRoute, bool)
}

type waitingRouteResolver interface {
	WaitTransferRoute(context.Context, string) (payload.CarrierRoute, bool)
}

// ObjectReadinessProbe is the receiver-side exact-reference reachability
// capability. The concrete XEP-0363 client implements this without exposing
// transport details outside the private core.
type ObjectReadinessProbe interface {
	ProbeDownloadReference(context.Context, string) error
}

type DurableState uint8

const (
	DurableUnknown DurableState = iota
	DurableLive
	DurableMailbox
	DurablePending
)

type Config struct {
	Endpoint string
	Auth     Authentication
	// DynamicPeerAuthority uses server-authorized peer IQs instead of a full
	// membership snapshot. Production composition enables this explicitly.
	DynamicPeerAuthority           bool
	ReceiveCapacity                int
	TransferWorkers                int
	TransferQueue                  int
	TransferByteCapacity           int64
	UnresolvedTransferCapacity     int
	UnresolvedTransferByteCapacity int64
	UnresolvedTransferLifetime     time.Duration
	MailboxLimit                   int
	ReconnectAttempts              int
	ReconnectInitial               time.Duration
	ReconnectMaximum               time.Duration
	ReconnectOperationTimeout      time.Duration
	CredentialUsableUntil          time.Time
	CredentialNow                  func() time.Time
	CredentialSource               CredentialSource
	SessionExpiryMode              SessionExpiryMode
	Jitter                         func(time.Duration) time.Duration
	// ProofSink receives the one absolute setup context and must return when it
	// is canceled. The proof slice is borrowed and cleared when the call ends.
	ProofSink func(context.Context, string, []byte) error
	// Clock is an optional ownership injection for composition. When nil,
	// Client creates an unready calibrated clock and still requires XEP-0202
	// calibration before Start can publish graph readiness.
	Clock *transport.CalibratedClock
}

// clientStateMutex gives exact-session authority callbacks an exclusive
// publication lease while leaving the ordinary state mutex released during
// the dependency callback. Session replacement and Close take the shared
// lease through Lock and therefore cannot publish across an admitted old
// session callback.
type clientStateMutex struct {
	state       sync.Mutex
	publication sync.RWMutex
}

func (mu *clientStateMutex) Lock() {
	mu.publication.RLock()
	mu.state.Lock()
}

func (mu *clientStateMutex) Unlock() {
	mu.state.Unlock()
	mu.publication.RUnlock()
}

func (mu *clientStateMutex) withExclusivePublication(callback func() bool) bool {
	mu.publication.Lock()
	defer mu.publication.Unlock()
	return callback()
}

// Client owns the private durable server session and resume state.
type Client struct {
	config         Config
	dialer         Dialer
	outbox         *outbox.Outbox
	receiver       TransferReceiver
	payloadWork    AuthenticatedPayloadWorkAdmitter
	routes         RouteResolver
	readinessProbe ObjectReadinessProbe
	payloadBound   bool
	clock          *transport.CalibratedClock

	mu                      clientStateMutex
	session                 Session
	identity                Authenticated
	state                   DurableState
	stateChanged            chan struct{}
	progress                time.Time
	ctx                     context.Context
	cancel                  context.CancelFunc
	started                 bool
	starting                bool
	startDone               chan struct{}
	startErr                error
	startCancel             context.CancelFunc
	generation              uint64
	stateEpoch              uint64
	sessionEpoch            uint64
	ingress                 *ingressGeneration
	workersStarted          bool
	closed                  bool
	closeDone               chan struct{}
	closeErr                error
	terminalFailure         error
	receiveWG               sync.WaitGroup
	inboundBudget           *transport.InboundBudget
	inbound                 chan AuthenticatedInbound
	signals                 chan Stanza
	authorityChanges        chan struct{}
	authorityPendingIngress uint64
	authorityWakeQueued     bool
	authorityFence          func()
	authoritySuspend        func() bool
	authorityResume         func(context.Context) (bool, error)
	authoritySuspended      bool
	retainedDataAuthority   bool
	authorityGeneration     uint64
	authorityExhausted      bool
	authorityPendingNonce   string
	authoritySnapshot       AuthoritySnapshot
	authorityHandledPending uint64
	membershipReady         bool
	membershipReadySignal   chan struct{}
	jobs                    []chan transferWork
	transferAdmission       transferAdmission
	complete                map[string]completionWaiter
	jingle                  map[string]*jingleWaiter
	readiness               map[string]objectReadinessWaiter
	readinessSeen           map[string]objectReadinessInbound
	wg                      sync.WaitGroup
	// sendMu serializes the one session write owner. Unlike a sync.Mutex, its
	// acquisition is context-cancellable so a caller that already transferred
	// durable ownership never wedges behind reconnect or another wire attempt.
	sendMu *cancellableMutex
	// ownershipMu preserves application-envelope ordinal and wire-attempt order
	// independently of control writes. A queued envelope may transfer durable
	// ownership before it can acquire sendMu; later application sends must not
	// overtake that envelope while the exact session is fenced for replay.
	ownershipMu *cancellableMutex
	// recoveryGate serializes the compound resumeSession -> calibration
	// transaction across public Resume and the background reconnect owner.
	// A channel token makes ownership acquisition context-cancellable; no Go
	// mutex is held while a bounded dependency operation runs.
	recoveryGate          chan struct{}
	reconnectMu           sync.Mutex
	reconnectDemand       chan reconnectRequest
	replay                []Stanza
	rank2NextOrdinal      uint64
	rank2Handled          uint64
	deliveryWake          func()
	revocationHandler     func(context.Context, Revocation) error
	routingFailureHandler func(string, string)
	revocationJobs        chan revocationWork
	externalQuery         *cancellableMutex
	external              externalServiceCache
}

// ingressGeneration is one immutable authenticated session handoff. Exactly
// one receive worker owns its mailbox and live Receive calls. Replacement
// cancels this generation before the worker can observe the next one.
type ingressGeneration struct {
	id                   uint64
	session              Session
	boundIdentity        string
	mailbox              []Stanza
	ctx                  context.Context
	cancel               context.CancelFunc
	ready                chan error
	readyOnce            sync.Once
	activationGeneration uint64
	activationStateEpoch uint64
}

// cancellableMutex is a one-token ownership gate. Lock and Unlock retain the
// sync.Locker shape for deterministic package tests; production acquisition
// always uses LockContext.
type cancellableMutex struct {
	token chan struct{}
}

func newCancellableMutex() *cancellableMutex {
	mutex := &cancellableMutex{token: make(chan struct{}, 1)}
	mutex.token <- struct{}{}
	return mutex
}

func (mutex *cancellableMutex) Lock() {
	<-mutex.token
}

func (mutex *cancellableMutex) LockContext(ctx context.Context) error {
	if mutex == nil || ctx == nil {
		return ErrInvalidConfig
	}
	select {
	case <-mutex.token:
		if err := ctx.Err(); err != nil {
			mutex.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mutex *cancellableMutex) Unlock() {
	if mutex == nil || mutex.token == nil {
		panic("rank2xmpp: send ownership released without initialization")
	}
	select {
	case mutex.token <- struct{}{}:
	default:
		panic("rank2xmpp: send ownership released twice")
	}
}

func callAuthorityFence(fence func()) (ok bool) {
	if fence == nil {
		return false
	}
	ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	fence()
	return ok
}

func callAuthoritySuspend(suspend func() bool) (ok bool) {
	if suspend == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return suspend()
}

func callAuthorityResume(resume func(context.Context) (bool, error), ctx context.Context) (restored bool, err error) {
	if resume == nil || ctx == nil {
		return false, ErrUnavailable
	}
	defer func() {
		if recover() != nil {
			restored, err = false, ErrUnavailable
		}
	}()
	return resume(ctx)
}

func newIngressGeneration(id uint64, session Session, boundIdentity string, mailbox []Stanza, lifetime context.Context) *ingressGeneration {
	ctx, cancel := context.WithCancel(lifetime)
	return &ingressGeneration{
		id:            id,
		session:       session,
		boundIdentity: boundIdentity,
		mailbox:       mailbox,
		ctx:           ctx,
		cancel:        cancel,
		ready:         make(chan error, 1),
	}
}

func (ingress *ingressGeneration) finish(err error) {
	if ingress == nil {
		return
	}
	ingress.readyOnce.Do(func() {
		ingress.ready <- err
		close(ingress.ready)
	})
}

type completion struct {
	evidence payload.CompletionEvidence
	err      error
}
type completionWaiter struct {
	peer      string
	messageID string
	result    chan completion
}

func NewClient(config Config, dialer Dialer, pending *outbox.Outbox, receiver TransferReceiver, routes RouteResolver) (*Client, error) {
	endpoint, endpointErr := ParseEndpoint(config.Endpoint)
	if endpointErr != nil || protocol.ValidateMeshID(config.Auth.MeshID) != nil || protocol.ValidateAgentIdentity(config.Auth.Username) != nil || strings.Contains(config.Auth.Username, "/") || strings.Contains(config.Auth.MeshID, "/") || !validSessionResourceConfig(config.Auth.SessionResource) || len(config.Auth.Password) == 0 || len(config.Auth.Password) > 4<<10 || config.ReceiveCapacity <= 0 || config.ReceiveCapacity > transport.MaximumReceiveQueue || config.TransferWorkers <= 0 || config.TransferWorkers > 1024 || config.TransferQueue < config.TransferWorkers || config.TransferQueue > 65536 || config.TransferByteCapacity < minimumRetainedStanzaBytes || config.TransferByteCapacity > MaximumTransferWorkBytes || config.UnresolvedTransferCapacity <= 0 || config.UnresolvedTransferCapacity > MaximumUnresolvedTransferEntries || config.UnresolvedTransferByteCapacity < minimumRetainedStanzaBytes || config.UnresolvedTransferByteCapacity > MaximumUnresolvedTransferBytes || config.UnresolvedTransferLifetime <= 0 || config.UnresolvedTransferLifetime > config.ReconnectOperationTimeout || config.MailboxLimit <= 0 || config.MailboxLimit > 65536 || config.ReconnectAttempts < 0 || config.ReconnectAttempts > 1024 || config.ReconnectInitial <= 0 || config.ReconnectMaximum < config.ReconnectInitial || config.ReconnectOperationTimeout <= 0 || config.ReconnectOperationTimeout > 10*time.Minute || config.CredentialUsableUntil.IsZero() != (config.CredentialNow == nil) || !config.CredentialUsableUntil.IsZero() && (!config.CredentialUsableUntil.After(config.CredentialNow().UTC()) || config.CredentialUsableUntil.Location() != time.UTC) || config.SessionExpiryMode > SessionExpiryDisconnect || dialer == nil || pending == nil {
		return nil, ErrInvalidConfig
	}
	config.Endpoint = endpoint.DialAddress()
	config.Auth.Username = strings.Clone(config.Auth.Username)
	config.Auth.MeshID = strings.Clone(config.Auth.MeshID)
	config.Auth.SessionResource = strings.Clone(config.Auth.SessionResource)
	config.Auth.Password = append([]byte(nil), config.Auth.Password...)
	if config.Clock == nil {
		config.Clock = transport.NewCalibratedClock(nil)
	}
	jobs := make([]chan transferWork, config.TransferWorkers)
	for i := range jobs {
		capacity := config.TransferQueue / config.TransferWorkers
		if i < config.TransferQueue%config.TransferWorkers {
			capacity++
		}
		jobs[i] = make(chan transferWork, capacity)
	}
	recoveryGate := make(chan struct{}, 1)
	recoveryGate <- struct{}{}
	inboundBudget, err := transport.NewInboundBudget(config.ReceiveCapacity, transport.MaximumControlFrameBytes)
	if err != nil {
		clear(config.Auth.Password)
		return nil, ErrInvalidConfig
	}
	return &Client{config: config, dialer: dialer, outbox: pending, receiver: receiver, routes: routes, clock: config.Clock, stateChanged: make(chan struct{}), inbound: make(chan AuthenticatedInbound, config.ReceiveCapacity), signals: make(chan Stanza, config.ReceiveCapacity), inboundBudget: inboundBudget, authorityChanges: make(chan struct{}, 1), membershipReadySignal: make(chan struct{}), jobs: jobs, revocationJobs: make(chan revocationWork, config.ReceiveCapacity), transferAdmission: newTransferAdmission(len(jobs)), complete: make(map[string]completionWaiter), jingle: make(map[string]*jingleWaiter), readiness: make(map[string]objectReadinessWaiter), readinessSeen: make(map[string]objectReadinessInbound), sendMu: newCancellableMutex(), ownershipMu: newCancellableMutex(), recoveryGate: recoveryGate, reconnectDemand: make(chan reconnectRequest, 1), rank2NextOrdinal: 1, externalQuery: newCancellableMutex()}, nil
}

// Clock returns the one monotonic-backed calibrated time source owned by this
// authenticated Rank 2 client. Root composition injects this exact pointer
// into Rank 1 and authenticated messaging services.
func (c *Client) Clock() *transport.CalibratedClock {
	if c == nil {
		return nil
	}
	return c.clock
}

// DeliveryOutbox exposes the one process-private carrier-neutral ownership
// queue to root composition. SDK layers never receive or retain this pointer.
func (c *Client) DeliveryOutbox() *outbox.Outbox {
	if c == nil {
		return nil
	}
	return c.outbox
}

func (c *Client) SetDeliveryWake(wake func()) error {
	if c == nil || wake == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deliveryWake != nil {
		return ErrInvalidConfig
	}
	c.deliveryWake = wake
	return nil
}

// SetAuthorityFence installs the private synchronous Rank1 authority gate.
// It may be installed once; replacement would allow a late callback to target
// a retired graph.
func (c *Client) SetAuthorityFence(fence func()) error {
	if c == nil || fence == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.authorityFence != nil {
		return ErrUnavailable
	}
	c.authorityFence = fence
	return nil
}

// SetAuthoritySuspension installs the reversible transport-outage boundary.
// suspend must synchronously block new live-link publication while retaining
// the exact current-membership capability for already published authenticated
// links. resume may reopen setup only after the ordered XEP-0198 replay barrier
// has completed. Hard authority changes use SetAuthorityFence instead.
func (c *Client) SetAuthoritySuspension(suspend func() bool, resume func(context.Context) (bool, error)) error {
	if c == nil || suspend == nil || resume == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.authoritySuspend != nil || c.authorityResume != nil {
		return ErrUnavailable
	}
	c.authoritySuspend = suspend
	c.authorityResume = resume
	return nil
}

func (c *Client) pauseMembershipLocked(pendingNonce string) bool {
	c.authoritySuspended = false
	c.membershipReady = false
	if c.membershipReadySignal == nil {
		c.membershipReadySignal = make(chan struct{})
	}
	if c.authorityExhausted || c.authorityGeneration == math.MaxUint64 {
		c.authorityExhausted = true
		c.authorityPendingNonce = ""
		c.authoritySnapshot = AuthoritySnapshot{}
		return false
	}
	c.authorityGeneration++
	c.authorityPendingNonce = pendingNonce
	return true
}

// activateDynamicAuthorityLocked opens only the authenticated transport.
// It does not authorize any peer: every peer handshake still needs a fresh
// server IQ and exact-session result.
func (c *Client) activateDynamicAuthorityLocked() {
	c.authoritySnapshot = AuthoritySnapshot{}
	c.authorityPendingNonce = ""
	c.authoritySuspended = false
	c.membershipReady = true
	if c.membershipReadySignal != nil {
		close(c.membershipReadySignal)
		c.membershipReadySignal = nil
	}
}

func (c *Client) TimeCalibration() (transport.CalibrationSnapshot, bool) {
	if c == nil || c.clock == nil {
		return transport.CalibrationSnapshot{}, false
	}
	return c.clock.Snapshot()
}

func (c *Client) Kind() transport.Kind { return transport.KindDurable }

func (c *Client) Start(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	if c.closed {
		failure := c.closedFailureLocked()
		c.mu.Unlock()
		return failure
	}
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if c.starting {
		done := c.startDone
		c.mu.Unlock()
		select {
		case <-done:
			c.mu.Lock()
			err := c.startErr
			c.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.starting = true
	c.generation++
	generation := c.generation
	done := make(chan struct{})
	c.startDone = done
	startCtx, startCancel := context.WithCancel(ctx)
	c.startCancel = startCancel
	c.mu.Unlock()
	attempt, err := c.connect(startCtx, generation)
	if err != nil {
		c.finishStart(generation, nil, Authenticated{}, nil, err)
		startCancel()
		c.mu.Lock()
		result := c.startErr
		c.mu.Unlock()
		return result
	}
	// The terminal page makes the exact server session routable, but local
	// application delivery remains paused until the membership owner installs
	// this complete vector and acknowledges its exact-session capability.
	var authoritySnapshot AuthoritySnapshot
	var syncErr error
	if !c.config.DynamicPeerAuthority {
		authoritySnapshot, syncErr = synchronizeAuthoritySession(attempt.ctx, attempt.session, c.config.Auth.MeshID, attempt.identity.BoundIdentity)
	}
	if syncErr != nil {
		err = syncErr
		// Classify against the still-live attempt context. Abort owns and
		// cancels that context, so normalizing afterward would incorrectly
		// turn an authority/protocol failure into caller cancellation.
		if contextErr := attempt.ctx.Err(); contextErr != nil {
			err = contextErr
		} else {
			// Authority synchronization is part of authenticated
			// connectivity readiness. Invalid, revoked, or temporarily
			// unavailable authority is retryable connectivity loss at the
			// public boundary; private protocol/authentication detail must
			// not be confused with the user's SASL credentials.
			err = ErrUnavailable
		}
		_ = attempt.finish(false)
		c.finishStart(generation, nil, Authenticated{}, nil, err)
		startCancel()
		c.mu.Lock()
		result := c.startErr
		c.mu.Unlock()
		return result
	}
	mailbox, err := catchUpSession(attempt.session, attempt.ctx, c.config.MailboxLimit)
	if err != nil {
		err = normalize(err, attempt.ctx, ErrUnavailable)
		_ = attempt.finish(false)
		c.finishStart(generation, nil, Authenticated{}, nil, err)
		startCancel()
		c.mu.Lock()
		result := c.startErr
		c.mu.Unlock()
		return result
	}
	if err = leaseMailbox(attempt.ctx, mailbox, c.inboundBudget); err != nil {
		clearStanzas(mailbox)
		_ = attempt.finish(false)
		c.finishStart(generation, nil, Authenticated{}, nil, err)
		startCancel()
		c.mu.Lock()
		result := c.startErr
		c.mu.Unlock()
		return result
	}
	// This scope owns every returned mailbox byte until the exact assignment to
	// ingress below. Setting mailbox=nil is the sole ownership transfer.
	defer func() { clearStanzas(mailbox) }()
	if err = c.consumeAttemptProof(attempt, generation); err != nil {
		_ = attempt.finish(false)
		c.finishStart(generation, nil, Authenticated{}, nil, err)
		startCancel()
		c.mu.Lock()
		result := c.startErr
		c.mu.Unlock()
		return result
	}
	owned, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	if c.closed || c.generation != generation {
		c.startErr, c.starting, c.startCancel = ErrClosed, false, nil
		close(done)
		c.mu.Unlock()
		cancel()
		startCancel()
		_ = attempt.finish(false)
		return ErrClosed
	}
	if publicationErr := attempt.finish(true); publicationErr != nil {
		c.startErr, c.starting, c.startCancel = publicationErr, false, nil
		close(done)
		c.mu.Unlock()
		cancel()
		startCancel()
		return publicationErr
	}
	clear(c.identity.Proof)
	initialState := DurableLive
	if len(mailbox) != 0 {
		initialState = DurableMailbox
	}
	c.sessionEpoch++
	ingress := newIngressGeneration(c.sessionEpoch, attempt.session, attempt.identity.BoundIdentity, mailbox, owned)
	mailbox = nil
	c.session, c.identity, c.ingress, c.ctx, c.cancel, c.started, c.progress = attempt.session, attempt.identity, ingress, owned, cancel, true, c.clock.Now()
	if authoritySnapshot.nonce != "" {
		authoritySnapshot.session = attempt.session
		authoritySnapshot.sessionEpoch = ingress.id
		if c.pauseMembershipLocked(authoritySnapshot.nonce) {
			authoritySnapshot.authorityGeneration = c.authorityGeneration
			c.authoritySnapshot = authoritySnapshot.clone()
		}
	}
	if c.config.DynamicPeerAuthority {
		c.activateDynamicAuthorityLocked()
	}
	c.bindAuthorityIngressLocked(attempt.session)
	retiredExternal := c.clearExternalServicesLocked()
	_ = c.setStateLocked(initialState)
	c.bindIngressActivationLocked(ingress)
	if !c.workersStarted {
		c.workersStarted = true
		for i := 0; i < c.config.TransferWorkers; i++ {
			c.wg.Add(1)
			go c.transferLoop(c.jobs[i])
		}
		c.wg.Add(7)
		go c.receiveLoop()
		go c.controlLoop()
		go c.revocationLoop()
		go c.revocationLoop()
		go c.keepaliveLoop()
		go c.reconnectLoop()
		go c.clockLoop()
		if !c.config.CredentialUsableUntil.IsZero() && c.config.SessionExpiryMode == SessionExpiryDisconnect {
			c.wg.Add(1)
			go c.credentialExpiryLoop()
		}
	}
	c.startErr, c.starting, c.startCancel = nil, false, nil
	close(done)
	c.mu.Unlock()
	retireExternalServiceExpiry(retiredExternal)
	startCancel()
	return nil
}

func (c *Client) finishStart(generation uint64, _ Session, _ Authenticated, _ []Stanza, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != generation || c.closed {
		err = ErrClosed
	}
	c.startErr, c.starting, c.startCancel = err, false, nil
	if c.startDone != nil {
		close(c.startDone)
	}
}

func (c *Client) connect(ctx context.Context, generation uint64) (*setupAttempt, error) {
	credential, credentialErr := c.currentCredential(ctx)
	if credentialErr != nil {
		return nil, credentialErr
	}
	defer clear(credential.Password)
	setup, cancelSetup := credentialBoundOperation(ctx, c.config.ReconnectOperationTimeout, credential.UsableUntil)
	phaseTimeout := boundedSetupPhaseTimeout(c.config.ReconnectOperationTimeout)
	session, err := dialSession(c.dialer, setup)
	if err != nil {
		result := normalize(err, setup, ErrUnavailable)
		cancelSetup()
		return nil, result
	}
	if dynamic, ok := session.(dynamicPeerAuthoritySession); ok {
		dynamic.setDynamicPeerAuthority(c.config.DynamicPeerAuthority)
	}
	if bounded, ok := session.(inboundBudgetSession); ok {
		if err = bounded.bindInboundBudget(c.inboundBudget); err != nil {
			_ = closeSession(session, setup)
			cancelSetup()
			return nil, normalize(err, setup, ErrUnavailable)
		}
	}
	attempt := newSetupAttempt(setup, cancelSetup, c.config.ReconnectOperationTimeout, session)
	fail := func(reason error) (*setupAttempt, error) {
		_ = attempt.finish(false)
		return nil, reason
	}
	if err := runSetupPhase(setup, ctx, phaseTimeout, attempt.candidate, func(operation context.Context) error {
		return connectSession(session, operation, c.config.Endpoint)
	}); err != nil {
		return fail(normalizeEstablishment(err, setup, establishmentNetworkTLS))
	}
	password := append([]byte(nil), credential.Password...)
	var bare string
	var proof []byte
	err = runSetupPhase(setup, ctx, phaseTimeout, attempt.candidate, func(operation context.Context) error {
		var authenticateErr error
		bare, proof, authenticateErr = authenticateSession(session, operation, c.config.Auth.Username, password)
		return authenticateErr
	})
	clear(password)
	if err != nil {
		clear(proof)
		return fail(normalizeEstablishment(err, setup, establishmentAuthentication))
	}
	if c.config.ProofSink == nil {
		clear(proof)
		proof = nil
	}
	var full string
	err = runSetupPhase(setup, ctx, phaseTimeout, attempt.candidate, func(operation context.Context) error {
		var bindErr error
		resource := c.config.Auth.SessionResource
		if resource == "" {
			resource = c.config.Auth.MeshID
		}
		full, bindErr = bindSession(session, operation, resource)
		return bindErr
	})
	if err != nil {
		clear(proof)
		return fail(normalizeEstablishment(err, setup, establishmentIdentityBinding))
	}
	if c.VerifyBoundIdentity(bare, c.config.Auth.MeshID, full) != nil {
		clear(proof)
		return fail(ErrIdentityBinding)
	}
	if err := runSetupPhase(setup, ctx, phaseTimeout, attempt.candidate, func(operation context.Context) error {
		return enableSMSession(session, operation)
	}); err != nil {
		clear(proof)
		return fail(normalizeEstablishment(err, setup, establishmentStreamManagement))
	}
	if !c.config.DynamicPeerAuthority {
		discoveryTimeout := boundedSetupPhaseTimeout(authorityDiscoveryTimeout)
		if discoveryTimeout > phaseTimeout {
			discoveryTimeout = phaseTimeout
		}
		if err := runSetupPhase(setup, ctx, discoveryTimeout, attempt.candidate, func(operation context.Context) error {
			return discoverAuthoritySession(session, operation)
		}); err != nil {
			clear(proof)
			return fail(normalize(err, setup, ErrUnavailable))
		}
	}
	calibrationTimeout := boundedSetupPhaseTimeout(timeCalibrationTimeout)
	if err := runSetupPhase(setup, ctx, calibrationTimeout, attempt.candidate, func(operation context.Context) error {
		return c.calibrate(operation, session)
	}); err != nil {
		clear(proof)
		return fail(err)
	}
	if err := setupContextError(ctx, setup, nil); err != nil {
		clear(proof)
		return fail(err)
	}
	if !c.setupGenerationCurrent(generation) {
		clear(proof)
		return fail(ErrClosed)
	}
	attempt.setIdentity(Authenticated{BareIdentity: bare, BoundIdentity: full}, proof)
	return attempt, nil
}

func (c *Client) consumeAttemptProof(attempt *setupAttempt, generation uint64) error {
	if attempt == nil {
		return ErrUnavailable
	}
	if err := setupContextError(attempt.ctx, attempt.ctx, nil); err != nil {
		return err
	}
	if c.config.ProofSink == nil {
		attempt.clearProof()
		return nil
	}
	if len(attempt.proof) == 0 {
		return ErrAuthentication
	}
	sinkProof := append([]byte(nil), attempt.proof...)
	attempt.clearProof()
	if err := c.consumeSetupProof(attempt.ctx, generation, sinkProof); err != nil {
		if contextErr := exactContextError(attempt.ctx); contextErr != nil {
			return contextErr
		}
		if !c.setupGenerationCurrent(generation) {
			return ErrClosed
		}
		return ErrAuthentication
	}
	if err := exactContextError(attempt.ctx); err != nil {
		return err
	}
	return nil
}

func (c *Client) setupGenerationCurrent(generation uint64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.generation == generation
}

func (c *Client) consumeSetupProof(ctx context.Context, generation uint64, proof []byte) error {
	c.mu.Lock()
	if c.closed || c.generation != generation || exactContextError(ctx) != nil {
		c.mu.Unlock()
		clear(proof)
		if err := exactContextError(ctx); err != nil {
			return err
		}
		return ErrClosed
	}
	// Admission and terminal Close are ordered by c.mu. Once admitted, Close
	// joins this callback through wg before it can return.
	c.wg.Add(1)
	sink := c.config.ProofSink
	meshID := c.config.Auth.MeshID
	c.mu.Unlock()
	err := consumeProof(sink, ctx, meshID, proof)
	c.wg.Done()
	c.mu.Lock()
	current := !c.closed && c.generation == generation
	c.mu.Unlock()
	if !current {
		return ErrClosed
	}
	return err
}

func (c *Client) calibrate(parent context.Context, session Session) error {
	if c == nil || c.clock == nil || parent == nil || session == nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(parent, timeCalibrationTimeout)
	defer cancel()
	err := c.clock.Calibrate(ctx, func(queryCtx context.Context) (time.Time, error) {
		return queryServerTimeSession(session, queryCtx)
	})
	if err == nil {
		if _, ok := c.clock.Snapshot(); ok {
			return nil
		}
		return ErrUnavailable
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	return ErrUnavailable
}

func (c *Client) Send(ctx context.Context, envelope protocol.Envelope) error {
	_, err := c.sendOwned(ctx, envelope, false)
	return err
}

// SendOwnership is the closed result of transferring one envelope to the
// durable client's bounded replay ownership. It is independent from the
// immediate session/wire attempt after ownership has transferred.
type SendOwnership uint8

const (
	AcceptedOwned SendOwnership = iota + 1
	UnavailableNoHandoff
	Rejected
	Capacity
)

// SendOwned transfers one validated envelope to the durable client's bounded
// private outbox. AcceptedOwned linearizes exactly when that enqueue succeeds;
// later session, wire, or caller-context failure cannot revoke ownership and
// instead requests normal recovery/replay.
func (c *Client) SendOwned(ctx context.Context, envelope protocol.Envelope) SendOwnership {
	ownership, _ := c.sendOwned(ctx, envelope, false)
	return ownership
}

func (c *Client) SendQueuedOwned(ctx context.Context, envelope protocol.Envelope) SendOwnership {
	ownership, _ := c.sendOwned(ctx, envelope, true)
	return ownership
}

// PendingEnvelopeCount returns the bounded number of application envelopes
// currently owned by the durable replay outbox. It excludes control and
// transfer stanzas and exposes no identifiers or carrier state.
func (c *Client) PendingEnvelopeCount() uint64 {
	if c == nil || c.outbox == nil {
		return 0
	}
	messages, _ := c.outbox.Usage()
	if messages <= 0 {
		return 0
	}
	if messages > 65_536 {
		return 65_536
	}
	return uint64(messages)
}

func (c *Client) OwnsEnvelope(messageID string) bool {
	return c != nil && c.outbox != nil && c.outbox.OwnsRank2(messageID)
}

// sendOwned is shared with Send so the compatibility API retains its existing
// normalized attempt error while root composition consumes only exact durable
// ownership. Wire bytes are hydrated from the outbox after exact Rank2 ownership
// is established.
func (c *Client) sendOwned(ctx context.Context, envelope protocol.Envelope, queued bool) (SendOwnership, error) {
	if c == nil || ctx == nil || c.ownershipMu == nil || protocol.ValidateEnvelope(envelope) != nil {
		return Rejected, transport.ErrInvalidEnvelope
	}
	if err := c.ownershipMu.LockContext(ctx); err != nil {
		return UnavailableNoHandoff, err
	}
	defer c.ownershipMu.Unlock()

	var (
		local           string
		session         Session
		ingress         *ingressGeneration
		ordinal         uint64
		queuedCommitted bool
	)
	if queued {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return UnavailableNoHandoff, err
		}
		local, session, ingress = c.identity.BoundIdentity, c.session, c.ingress
		ready := c.started && !c.closed && c.state == DurableLive && c.membershipReady
		if !ready {
			failure := c.terminalFailure
			c.mu.Unlock()
			if failure != nil {
				return UnavailableNoHandoff, failure
			}
			return UnavailableNoHandoff, ErrUnavailable
		}
		if envelope.Sender != local || envelope.MeshID != c.config.Auth.MeshID {
			c.mu.Unlock()
			return Rejected, ErrUnavailable
		}
		if c.outbox.OwnsRank2(envelope.MessageID) {
			c.mu.Unlock()
			return AcceptedOwned, nil
		}
		if c.rank2NextOrdinal == 0 {
			c.mu.Unlock()
			return Capacity, ErrQueueFull
		}
		ordinal = c.rank2NextOrdinal
		err := c.outbox.ClaimRank2(envelope.MessageID, ordinal)
		if err == nil {
			if ordinal == math.MaxUint64 {
				c.rank2NextOrdinal = 0
			} else {
				c.rank2NextOrdinal++
			}
			queuedCommitted = true
		}
		c.mu.Unlock()
		if err != nil {
			if errors.Is(err, outbox.ErrCapacity) {
				return Capacity, ErrQueueFull
			}
			return Rejected, ErrQueueFull
		}
	}

	if err := c.sendMu.LockContext(ctx); err != nil {
		if queuedCommitted {
			// Ownership is already durable, so writer starvation cannot revoke the
			// handoff. Fence the exact session and let recovery replay by ordinal;
			// never invoke Session.Send concurrently with the stuck writer.
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			c.interruptFailedIngress(session, ingress)
			return AcceptedOwned, err
		}
		return UnavailableNoHandoff, err
	}
	defer c.sendMu.Unlock()
	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		if queuedCommitted {
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			return AcceptedOwned, err
		}
		return UnavailableNoHandoff, err
	}
	currentLocal, currentSession, currentIngress := c.identity.BoundIdentity, c.session, c.ingress
	ready := c.started && !c.closed && c.state == DurableLive && c.membershipReady
	if queuedCommitted && (currentSession != session || currentIngress != ingress || currentLocal != local) {
		// Recovery replaced the session while this caller waited for writer
		// admission. In dynamic mode the claim may have raced the recovery
		// owner's metadata snapshot. Return it to the general queue so the
		// exact-peer revalidation drain cannot miss this accepted payload.
		c.mu.Unlock()
		if c.config.DynamicPeerAuthority {
			_ = c.outbox.ReleaseRank2Owned(envelope.MessageID, ordinal)
			c.mu.Lock()
			wake := c.deliveryWake
			c.mu.Unlock()
			if wake != nil {
				wake()
			}
		}
		return AcceptedOwned, nil
	}
	if !ready {
		failure := c.terminalFailure
		c.mu.Unlock()
		if queuedCommitted {
			// ClaimRank2 already transferred durable ownership, but this exact
			// session lost authority admission before the wire attempt. Fence it and
			// appoint reconnect/replay as the future owner; returning AcceptedOwned
			// without that transition would strand the ordinal outside normal drains.
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			if failure != nil {
				return AcceptedOwned, failure
			}
			return AcceptedOwned, ErrUnavailable
		}
		if failure != nil {
			return UnavailableNoHandoff, failure
		}
		return UnavailableNoHandoff, ErrUnavailable
	}
	if envelope.Sender != currentLocal || envelope.MeshID != c.config.Auth.MeshID {
		c.mu.Unlock()
		if queuedCommitted {
			// The claim was validated against this exact session immediately before
			// writer admission, so any identity drift is a failed publication. Route
			// the owned ordinal through the same ordered recovery path.
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			return AcceptedOwned, ErrUnavailable
		}
		return Rejected, ErrUnavailable
	}
	local, session, ingress = currentLocal, currentSession, currentIngress
	var err error
	if !queued {
		reservation, enqueueErr := c.outbox.EnqueueReserved(envelope.Clone())
		err = enqueueErr
		if err == nil && reservation.MessageID == "" {
			err = outbox.ErrOwnedEntry
		} else if err == nil {
			defer c.outbox.Release(reservation)
		}
	}
	if !queuedCommitted && err == nil && c.outbox.OwnsRank2(envelope.MessageID) {
		c.mu.Unlock()
		return AcceptedOwned, nil
	}
	if !queuedCommitted && err == nil && c.rank2NextOrdinal == 0 {
		c.mu.Unlock()
		return Capacity, ErrQueueFull
	}
	if !queuedCommitted {
		ordinal = c.rank2NextOrdinal
	}
	if !queuedCommitted && err == nil {
		err = c.outbox.ClaimRank2(envelope.MessageID, ordinal)
	}
	if !queuedCommitted && err == nil {
		if ordinal == math.MaxUint64 {
			c.rank2NextOrdinal = 0
		} else {
			c.rank2NextOrdinal++
		}
	}
	c.mu.Unlock()
	if err != nil {
		if errors.Is(err, outbox.ErrCapacity) {
			return Capacity, ErrQueueFull
		}
		return Rejected, ErrQueueFull
	}
	stanza, ok := c.pendingEnvelopeStanza(local, outbox.Rank2Metadata{MessageID: envelope.MessageID, MeshID: envelope.MeshID, TransportOrdinal: ordinal})
	if !ok {
		emitRank2Evidence(c.evidenceRecord("xmpp_send_failed", "application", "prepare_envelope", ingress), transport.ErrSendAmbiguous)
		_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
		return AcceptedOwned, transport.ErrSendAmbiguous
	}
	sendErr := sendSession(session, ctx, stanza)
	clear(stanza.Data)
	if sendErr != nil {
		emitRank2Evidence(c.evidenceRecord("xmpp_send_failed", "application", "send_envelope", ingress), sendErr)
		_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
		return AcceptedOwned, normalize(sendErr, ctx, transport.ErrSendAmbiguous)
	}
	c.mu.Lock()
	if !c.closed && c.session == session {
		c.progress = c.clock.Now()
	}
	c.mu.Unlock()
	return AcceptedOwned, nil
}

func (c *Client) sendStanza(ctx context.Context, stanza Stanza) error {
	return c.sendStanzaWithFailure(ctx, stanza, false)
}

func (c *Client) sendStanzaWithFailure(ctx context.Context, stanza Stanza, durableStateLoss bool) error {
	if c == nil || ctx == nil || c.sendMu == nil {
		return ErrInvalidConfig
	}
	if err := c.sendMu.LockContext(ctx); err != nil {
		return err
	}
	defer c.sendMu.Unlock()
	c.mu.Lock()
	session, ingress, ready := c.session, c.ingress, c.started && !c.closed && c.membershipReady
	if !ready || session == nil {
		failure := c.terminalFailure
		c.mu.Unlock()
		if failure != nil {
			return failure
		}
		return ErrUnavailable
	}
	c.mu.Unlock()
	err := sendSession(session, ctx, stanza)
	if err == nil {
		c.mu.Lock()
		if !c.closed && c.started && c.session == session && (ingress == nil || c.ingress == ingress) {
			c.progress = c.clock.Now()
		}
		c.mu.Unlock()
	} else {
		var staged *wireError
		if errors.As(err, &staged) && staged.stage == wireNotStarted {
			// Nothing reached the stream, so a cancelled peer handshake
			// cannot take down the shared XMPP session.
			return err
		}
		record := c.evidenceRecord("xmpp_send_failed", "client", "send_stanza", ingress)
		record.Reconnect = durableStateLoss || ctx.Err() != nil
		emitRank2Evidence(record, err)
		if durableStateLoss || ctx.Err() != nil {
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
		} else {
			c.requestIngressReconnect(session, ingress)
		}
	}
	return err
}

// interruptFailedIngress hard-stops only the exact current session after its
// durable failure state has already been published. In particular, a stale
// post-claim waiter must never interrupt a replacement that reused the same
// client while it was waiting for sendMu.
func (c *Client) interruptFailedIngress(session Session, ingress *ingressGeneration) {
	if c == nil || session == nil {
		return
	}
	c.mu.Lock()
	current := !c.closed && c.started && c.session == session &&
		(c.state == DurablePending || c.state == DurableUnknown)
	if ingress != nil {
		current = current && c.ingress == ingress && c.sessionEpoch == ingress.id
	}
	c.mu.Unlock()
	if current {
		interruptSession(session)
	}
}

func (c *Client) requestIngressReconnect(session Session, ingress *ingressGeneration) {
	if c == nil || session == nil {
		return
	}
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		current := !c.closed && c.started && c.session == session
		if ingress != nil {
			current = current && c.ingress == ingress && c.sessionEpoch == ingress.id
		}
		owner := c.stateTransitionOwnerLocked()
		c.mu.state.Unlock()
		if current {
			c.enqueueReconnectOwned(owner)
		}
		return current
	})
}

func (c *Client) Receive(ctx context.Context) (protocol.Envelope, error) {
	inbound, err := c.ReceiveAuthenticated(ctx)
	return inbound.Envelope, err
}

// ReceiveAuthenticated returns one ownership-safe application envelope paired
// with the sender identity authenticated by its stanza/session context.
//
// The bounded queue is shared with Receive: each record has exactly one
// consumer. A started client continues waiting across resume and clean
// reconnect because those transitions retain the client lifetime and identity.
// Before Start it returns ErrUnavailable. After Close linearizes it returns
// ErrClosed and never releases a buffered record. Caller cancellation is
// returned without being normalized.
func (c *Client) ReceiveAuthenticated(ctx context.Context) (AuthenticatedInbound, error) {
	return c.receiveAuthenticated(ctx, true)
}

// ReceiveAuthenticatedForQuarantine transfers transport-authenticated input
// to the composed per-peer lane even while current membership is paused. It
// does not authorize SDK publication; the peer lane owns that final fence.
func (c *Client) ReceiveAuthenticatedForQuarantine(ctx context.Context) (AuthenticatedInbound, error) {
	return c.receiveAuthenticated(ctx, false)
}

func (c *Client) receiveAuthenticated(ctx context.Context, waitForMembership bool) (AuthenticatedInbound, error) {
	if c == nil || ctx == nil {
		return AuthenticatedInbound{}, ErrInvalidConfig
	}
	// Cancellation observed before the call begins cannot compete with a
	// ready record. Once the receive below wins, ownership has linearized to
	// this caller and later caller cancellation must not discard the record.
	if err := ctx.Err(); err != nil {
		return AuthenticatedInbound{}, err
	}
	c.mu.Lock()
	if c.closed {
		failure := c.closedFailureLocked()
		c.mu.Unlock()
		return AuthenticatedInbound{}, failure
	}
	if !c.started || c.ctx == nil {
		c.mu.Unlock()
		return AuthenticatedInbound{}, ErrUnavailable
	}
	lifetime, generation := c.ctx, c.generation
	c.receiveWG.Add(1)
	c.mu.Unlock()
	defer c.receiveWG.Done()
	// Recheck immediately after the required client lock. Cancellation or
	// lifetime retirement that became observable while waiting for that lock
	// wins without competing for a ready queue record. After these checks, a
	// receive selected concurrently with cancellation owns the record.
	if err := ctx.Err(); err != nil {
		return AuthenticatedInbound{}, err
	}
	if err := lifetime.Err(); err != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return AuthenticatedInbound{}, callerErr
		}
		return AuthenticatedInbound{}, c.closedFailure()
	}

	for {
		c.mu.Lock()
		ready, readySignal := c.membershipReady, c.membershipReadySignal
		c.mu.Unlock()
		if waitForMembership && !ready {
			select {
			case <-readySignal:
			case <-ctx.Done():
				return AuthenticatedInbound{}, ctx.Err()
			case <-lifetime.Done():
				return AuthenticatedInbound{}, c.closedFailure()
			}
			continue
		}
		select {
		case inbound := <-c.inbound:
			c.mu.Lock()
			closed := c.closed || c.generation != generation
			active, stillReady := c.started, c.membershipReady
			c.mu.Unlock()
			if closed {
				clearAuthenticatedInbound(&inbound)
				return AuthenticatedInbound{}, c.closedFailure()
			}
			if !active {
				clearAuthenticatedInbound(&inbound)
				return AuthenticatedInbound{}, ErrUnavailable
			}
			if waitForMembership && !stillReady {
				for {
					c.mu.Lock()
					closed = c.closed || c.generation != generation
					stillReady, readySignal = c.membershipReady, c.membershipReadySignal
					c.mu.Unlock()
					if closed {
						clearAuthenticatedInbound(&inbound)
						return AuthenticatedInbound{}, c.closedFailure()
					}
					if stillReady {
						break
					}
					select {
					case <-readySignal:
					case <-ctx.Done():
						clearAuthenticatedInbound(&inbound)
						return AuthenticatedInbound{}, ctx.Err()
					case <-lifetime.Done():
						clearAuthenticatedInbound(&inbound)
						return AuthenticatedInbound{}, c.closedFailure()
					}
				}
			}
			if inbound.inboundLease != nil {
				inbound.inboundLease.Release()
				inbound.inboundLease = nil
			}
			return inbound, nil
		case <-ctx.Done():
			return AuthenticatedInbound{}, ctx.Err()
		case <-lifetime.Done():
			if err := ctx.Err(); err != nil {
				return AuthenticatedInbound{}, err
			}
			return AuthenticatedInbound{}, c.closedFailure()
		}
	}
}

// ReceiveAuthorityChange returns one authenticated live-only wakeup. It
// carries no membership evidence; the consumer must begin a fresh full sync.
func (c *Client) ReceiveAuthorityChange(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	lifetime, generation, started, closed := c.ctx, c.generation, c.started, c.closed
	closedFailure := c.closedFailureLocked()
	if !closed && started && lifetime != nil {
		c.receiveWG.Add(1)
	}
	c.mu.Unlock()
	if closed {
		return closedFailure
	}
	if !started || lifetime == nil {
		return ErrUnavailable
	}
	defer c.receiveWG.Done()
	select {
	case <-c.authorityChanges:
		c.mu.Lock()
		closed := c.closed
		replaced := c.generation != generation
		pendingIngress := c.authorityPendingIngress
		currentIngress := c.sessionEpoch
		c.authorityPendingIngress = 0
		c.authorityWakeQueued = false
		closedFailure = c.closedFailureLocked()
		c.mu.Unlock()
		if closed {
			return closedFailure
		}
		// A queued wake belongs to one exact ingress generation. Reconnect may
		// replace that ingress before the monitor consumes the wake; this is
		// superseded work, not terminal Client closure and not authority evidence
		// against the replacement session.
		if replaced || pendingIngress != currentIngress {
			return ErrUnavailable
		}
		if pendingIngress == 0 {
			return ErrProtocol
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-lifetime.Done():
		return c.closedFailure()
	}
}

func (c *Client) receiveLoop() {
	defer c.wg.Done()
	c.receiveEvents()
}

// controlLoop owns the server/session control lane independently from peer
// delivery. A blocked SDK receiver, transfer worker, or peer lane cannot delay
// an authority invalidation or the ordered resume barrier.
func (c *Client) controlLoop() {
	defer c.wg.Done()
	for {
		ingress := c.currentIngress()
		if ingress == nil {
			return
		}
		source, ok := ingress.session.(controlEventSession)
		if !ok {
			select {
			case <-ingress.ctx.Done():
				if c.isClosed() {
					return
				}
				continue
			case <-c.ctx.Done():
				return
			}
		}
		event, err := source.ReceiveControl(ingress.ctx)
		if !c.ingressCurrent(ingress) {
			clearEventOwned(&event)
			if c.isClosed() {
				return
			}
			continue
		}
		if err != nil {
			clearEventOwned(&event)
			if ingress.ctx.Err() != nil {
				continue
			}
			emitRank2Evidence(c.evidenceRecord("xmpp_receive_failed", "control", "receive_control", ingress), err)
			_ = c.transitionIngressFailure(ingress.session, ingress, DurablePending, true)
			continue
		}
		if !isControlEvent(event.Kind) {
			clearEventOwned(&event)
			c.fenceIngressOverload(ingress)
			continue
		}
		c.handleIngressEvent(ingress, event)
	}
}

func (c *Client) receiveEvents() {
	for {
		ingress := c.currentIngress()
		if ingress == nil {
			return
		}
		if !c.runIngressGeneration(ingress) {
			return
		}
	}
}

// runIngressGeneration stages one generation's bounded mailbox before it
// begins any live Receive. Every late result is checked against the exact
// generation before it can affect state or application delivery.
func (c *Client) runIngressGeneration(ingress *ingressGeneration) bool {
	owner, owned := c.ingressActivationOwner(ingress)
	if !owned {
		ingress.finish(ErrUnavailable)
		return !c.isClosed()
	}
	for i := range ingress.mailbox {
		if !c.ingressCurrent(ingress) {
			clearStanzas(ingress.mailbox)
			ingress.mailbox = nil
			ingress.finish(ErrClosed)
			return !c.isClosed()
		}
		stanza := ingress.mailbox[i]
		ingress.mailbox[i] = Stanza{}
		c.handleIngressEvent(ingress, Event{Kind: EventStanza, Stanza: stanza})
	}
	clearStanzas(ingress.mailbox)
	ingress.mailbox = nil
	if err := c.setIngressStateOwned(owner, ingress, DurableLive); err != nil {
		ingress.finish(err)
		// Activation is one-shot. A newer state edge on the same current
		// ingress (for example a send failure or same-session recovery) makes
		// this Live publication stale, but must not make receiveEvents select
		// the same ingress and retry activation forever. A replacement still
		// returns to the outer loop for exact-ingress handoff.
		if !c.ingressCurrent(ingress) {
			return !c.isClosed()
		}
	} else {
		ingress.finish(nil)
	}

	for {
		event, err := receiveSession(ingress.session, ingress.ctx)
		if !c.ingressCurrent(ingress) {
			clearEventOwned(&event)
			return !c.isClosed()
		}
		if err != nil {
			clearEventOwned(&event)
			if ingress.ctx.Err() != nil {
				return !c.isClosed()
			}
			emitRank2Evidence(c.evidenceRecord("xmpp_receive_failed", "application", "receive", ingress), err)
			_ = c.transitionIngressFailure(ingress.session, ingress, DurablePending, true)
			timer := time.NewTimer(c.config.ReconnectInitial)
			select {
			case <-timer.C:
			case <-ingress.ctx.Done():
				timer.Stop()
				return !c.isClosed()
			}
			continue
		}
		// Optional production sessions expose a physically separate control
		// queue. Injected compatibility sessions may still return controls here;
		// they retain the same fail-closed handling but no peer frame is ever
		// accepted by the production control worker.
		c.handleIngressEvent(ingress, event)
	}
}

func (c *Client) currentIngress() *ingressGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started {
		return nil
	}
	return c.ingress
}

func (c *Client) ingressCurrent(ingress *ingressGeneration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ingress != nil && !c.closed && c.started && c.ingress == ingress && c.sessionEpoch == ingress.id
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) setIngressState(ingress *ingressGeneration, state DurableState) error {
	if state == DurablePending || state == DurableUnknown {
		if ingress == nil {
			return ErrUnavailable
		}
		return c.transitionIngressFailure(ingress.session, ingress, state, false)
	}
	owner, ok := c.captureIngressStateOwner(ingress)
	if !ok {
		return ErrUnavailable
	}
	return c.setIngressStateOwned(owner, ingress, state)
}

func (c *Client) captureIngressStateOwner(ingress *ingressGeneration) (stateTransitionOwner, bool) {
	if c == nil || ingress == nil {
		return stateTransitionOwner{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started || c.ingress != ingress || c.sessionEpoch != ingress.id {
		return stateTransitionOwner{}, false
	}
	return c.stateTransitionOwnerLocked(), true
}

func (c *Client) bindIngressActivationLocked(ingress *ingressGeneration) {
	if ingress == nil {
		return
	}
	ingress.activationGeneration = c.generation
	ingress.activationStateEpoch = c.stateEpoch
}

func (c *Client) ingressActivationOwner(ingress *ingressGeneration) (stateTransitionOwner, bool) {
	if c == nil || ingress == nil {
		return stateTransitionOwner{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started || c.ingress != ingress || c.sessionEpoch != ingress.id {
		return stateTransitionOwner{}, false
	}
	// Package-level dependency tests may construct an ingress directly. Real
	// Start/reconnect publications always bind these fields before unlocking.
	if ingress.activationGeneration == 0 || ingress.activationStateEpoch == 0 {
		return c.stateTransitionOwnerLocked(), true
	}
	return stateTransitionOwner{
		generation:   ingress.activationGeneration,
		stateEpoch:   ingress.activationStateEpoch,
		session:      ingress.session,
		ingress:      ingress,
		sessionEpoch: ingress.id,
	}, true
}

func (c *Client) setIngressStateOwned(owner stateTransitionOwner, ingress *ingressGeneration, state DurableState) error {
	_, err := c.publishIngressStateOwned(owner, ingress, state)
	return err
}

func (c *Client) publishIngressStateOwned(owner stateTransitionOwner, ingress *ingressGeneration, state DurableState) (stateTransitionOwner, error) {
	if c == nil || ingress == nil {
		return stateTransitionOwner{}, ErrUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return stateTransitionOwner{}, ErrClosed
	}
	if !c.stateTransitionOwnerCurrentLocked(owner) || c.ingress != ingress || c.sessionEpoch != ingress.id {
		c.mu.Unlock()
		return stateTransitionOwner{}, ErrUnavailable
	}
	// Mailbox/handled completion is not recovery evidence for an already
	// failed durable session. Only an existing Live edge or the exact Mailbox
	// edge may publish Live; Pending/Unknown require authenticated recovery.
	if state == DurableLive && c.state != DurableLive && c.state != DurableMailbox {
		c.mu.Unlock()
		return stateTransitionOwner{}, ErrUnavailable
	}
	retiredExternal := c.setStateLocked(state)
	nextOwner := c.stateTransitionOwnerLocked()
	c.mu.Unlock()
	retireExternalServiceExpiry(retiredExternal)
	return nextOwner, nil
}

// transitionIngressFailure is the single exact-session loss linearization.
// Its exclusive publication lease prevents Start/reconnect/Close from
// publishing a replacement between the current-ingress check, synchronous
// authority fence, state transition, and bounded reconnect demand.
func (c *Client) transitionIngressFailure(session Session, ingress *ingressGeneration, state DurableState, reconnect bool) error {
	if c == nil || session == nil || state != DurablePending && state != DurableUnknown {
		return ErrInvalidConfig
	}
	var retiredExternal *externalServiceExpiryOwner
	var admitted, fenceFailed bool
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		current := !c.closed && c.started && c.session == session
		if ingress != nil {
			current = current && c.ingress == ingress && c.sessionEpoch == ingress.id
		}
		alreadyFailed := c.state == DurablePending || c.state == DurableUnknown
		fence, suspend := c.authorityFence, c.authoritySuspend
		alreadySuspended := c.authoritySuspended
		c.mu.state.Unlock()
		if !current {
			return false
		}
		admitted = true
		// Transport loss suspends control and new live-link publication rather
		// than treating network failure as revocation. Already published Rank1
		// links retain their exact data capability; a successful ordered resume
		// reopens setup. A missing or failed suspension seam falls back to the
		// irreversible authority fence.
		suspended := alreadySuspended
		if !alreadyFailed && !suspended {
			suspended = callAuthoritySuspend(suspend)
			if !suspended && fence != nil && !callAuthorityFence(fence) {
				fenceFailed = true
			}
		}
		c.mu.state.Lock()
		if suspended {
			c.authoritySuspended = true
			c.retainedDataAuthority = true
			retiredExternal = c.setStatePreservingAuthorityLocked(state)
		} else {
			retiredExternal = c.setStateLocked(state)
		}
		owner := c.stateTransitionOwnerLocked()
		c.mu.state.Unlock()
		if reconnect {
			c.enqueueReconnectOwned(owner)
		}
		return true
	})
	retireExternalServiceExpiry(retiredExternal)
	record := c.evidenceRecord("durable_state_transition", "client", "ingress_failure", ingress)
	record.State = uint8(state)
	record.Admitted = admitted
	record.Reconnect = reconnect
	emitRank2Evidence(record, nil)
	if !admitted || fenceFailed {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) requestReconnect() {
	owner, ok := c.captureStateTransitionOwner()
	if ok {
		c.requestReconnectOwned(owner)
	}
}

// requestReconnectOwned coalesces recovery demand only within one exact state
// publication. A fresh demand replaces a queued request for a retired state;
// the reconnect loop revalidates again after dequeue and after waiting for its
// serialization gates, before it invokes any reconnect dependency.
func (c *Client) requestReconnectOwned(owner stateTransitionOwner) {
	if c == nil || c.reconnectDemand == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stateTransitionOwnerCurrentLocked(owner) {
		return
	}
	c.enqueueReconnectOwned(owner)
}

// Queue a retry only for the exact pending publication observed while holding
// the state lock. A concurrent public Resume cannot turn this into a fresh
// reconnect request against a healthy session.
func (c *Client) requestReconnectIfPending() {
	if c == nil || c.reconnectDemand == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started || c.state != DurablePending {
		return
	}
	c.enqueueReconnectOwned(c.stateTransitionOwnerLocked())
}

func (c *Client) enqueueReconnectOwned(owner stateTransitionOwner) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	request := reconnectRequest{owner: owner}
	select {
	case queued := <-c.reconnectDemand:
		if queued.owner == owner {
			request = queued
		}
	default:
	}
	select {
	case c.reconnectDemand <- request:
	default:
	}
}

func (c *Client) reconnectLoop() {
	defer c.wg.Done()
	for {
		select {
		case request := <-c.reconnectDemand:
			if !c.reconnectOwned(request.owner) {
				_ = c.transitionOwnedState(request.owner, DurablePending, false, false)
				if c.config.ReconnectAttempts == 0 {
					continue
				}
				// A credential-store read may recover after this attempt. Keep a
				// bounded retry demand alive while the same client is pending;
				// terminal server denial cancels c.ctx and stops this loop.
				timer := time.NewTimer(max(c.config.ReconnectInitial, 100*time.Millisecond))
				select {
				case <-timer.C:
					c.requestReconnectIfPending()
				case <-c.ctx.Done():
					timer.Stop()
					return
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// terminateRejectedRelogin is reserved for an authenticated denial during a
// fresh dynamic-mode bind. A failed XEP-0198 resume alone is not such proof:
// the following clean login can still succeed. Close retires the old peer
// authority fence, replay and outbox, and stops further recovery demand.
func (c *Client) terminateRejectedRelogin(owner stateTransitionOwner) {
	c.mu.Lock()
	current := c.stateTransitionOwnerCurrentLocked(owner)
	if current {
		c.terminalFailure = ErrAuthentication
	}
	c.mu.Unlock()
	if !current {
		return
	}
	// Close owns component teardown, but its join includes this reconnect
	// worker. An already-cancelled caller context lets it publish closure and
	// launch cleanup without waiting for itself.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Close(ctx)
}

func (c *Client) closedFailureLocked() error {
	if c.terminalFailure != nil {
		return c.terminalFailure
	}
	return ErrClosed
}

func (c *Client) closedFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closedFailureLocked()
}

// TerminalFailure distinguishes a server-rejected re-login from a normal
// application Close without exposing dependency-specific authentication data.
func (c *Client) TerminalFailure() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminalFailure
}

func (c *Client) clockLoop() {
	defer c.wg.Done()
	for {
		invalidated := c.clock.Invalidation()
		select {
		case <-invalidated:
			owner, owned := c.captureStateTransitionOwner()
			if owned {
				_ = c.transitionOwnedState(owner, DurablePending, true, true)
			}
			ready := c.clock.Ready()
			select {
			case <-ready:
				continue
			case <-c.ctx.Done():
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) handleEvent(event Event) {
	ingress := c.currentIngress()
	if ingress == nil {
		return
	}
	c.handleIngressEvent(ingress, event)
}

func (c *Client) handleIngressEvent(ingress *ingressGeneration, event Event, handoffContexts ...context.Context) {
	defer clearEventOwned(&event)
	handoff := ingress.ctx
	if len(handoffContexts) == 1 && handoffContexts[0] != nil {
		handoff = handoffContexts[0]
	}
	c.mu.Lock()
	if c.closed || c.ingress != ingress || c.sessionEpoch != ingress.id {
		c.mu.Unlock()
		return
	}
	if c.inboundBudget == nil {
		capacity := cap(c.inbound)
		if capacity <= 0 {
			capacity = 1
		}
		c.inboundBudget, _ = transport.NewInboundBudget(capacity, transport.MaximumControlFrameBytes)
	}
	if event.inboundLease == nil && event.Stanza.inboundLease == nil {
		c.mu.Unlock()
		size := 0
		if event.Kind == EventStanza {
			// Injected Session implementations retain their own value. Establish
			// Core ownership once at that dependency boundary; production
			// Mellium events already arrive with a movable lease and need no copy.
			event.Stanza = event.Stanza.clone()
			var ok bool
			size, ok = inboundStanzaBytes(event.Stanza)
			if !ok {
				c.fenceIngressOverload(ingress)
				return
			}
		}
		lease, err := c.inboundBudget.Acquire(handoff, size)
		if err != nil {
			if ingress.ctx.Err() == nil && handoff.Err() == nil {
				c.fenceIngressOverload(ingress)
			}
			return
		}
		if event.Kind == EventStanza {
			event.Stanza.inboundLease = lease
		} else {
			event.inboundLease = lease
		}
		c.mu.Lock()
		if c.closed || c.ingress != ingress || c.sessionEpoch != ingress.id {
			c.mu.Unlock()
			return
		}
	}
	owner := c.stateTransitionOwnerLocked()
	c.progress = c.clock.Now()
	c.mu.Unlock()
	switch event.Kind {
	case EventHandled:
		c.mu.Lock()
		ready := c.membershipReady
		if !ready && event.HandledThrough > c.authorityHandledPending {
			c.authorityHandledPending = event.HandledThrough
		}
		c.mu.Unlock()
		if ready && c.markRank2Handled(event.HandledThrough) == nil {
			_ = c.setIngressStateOwned(owner, ingress, DurableLive)
		}
	case EventCustodyAccepted:
		_ = c.markRank2Handled(event.HandledThrough)
		if c.retireRank2Custody(event.MessageID) == nil {
			_ = c.setIngressStateOwned(owner, ingress, DurableLive)
		}
	case EventMailboxComplete:
		_ = c.setIngressStateOwned(owner, ingress, DurableLive)
	case EventMembershipChanged:
		c.handleIngressAuthorityChange(ingress)
	case EventPeerRevoked:
		c.enqueuePeerRevocation(ingress, event)
	case EventRoutingFailure:
		if !validReadinessTyped(event.MessageID, "msg_", 16) || !validJingleIQErrorCondition(event.ErrorCondition) {
			return
		}
		c.mu.Lock()
		handler := c.routingFailureHandler
		c.mu.Unlock()
		if handler != nil {
			handler(event.MessageID, event.ErrorCondition)
		}
	case EventResumeAuthorityResult:
		if event.resumeBarrier != nil {
			close(event.resumeBarrier.done)
			event.resumeBarrier = nil
		}
	case EventJingleFailure:
		// The adapter already authenticated this ID against its bounded issued
		// ledger. A delayed server error may legitimately outlive its waiter.
		_ = c.failJingleExchange(event.AttemptID)
	case EventStanza:
		stanza := moveEventStanza(&event)
		defer clearStanzaOwned(&stanza)
		if stanza.MeshID != c.config.Auth.MeshID || stanza.To != ingress.boundIdentity || protocol.ValidateAgentIdentity(stanza.From) != nil {
			return
		}
		switch stanza.Kind {
		case StanzaEnvelope:
			inbound, ok := decodeAuthenticatedInboundOwned(&stanza)
			if !ok {
				return
			}
			select {
			case c.inbound <- inbound:
				inbound = AuthenticatedInbound{}
			case <-handoff.Done():
				clearAuthenticatedInbound(&inbound)
			}
		case StanzaSignal:
			if !c.handleSignal(handoff, &stanza) {
				if ingress.ctx.Err() == nil && handoff.Err() == nil {
					c.fenceIngressOverload(ingress)
				}
			}
		case StanzaTransferCompletion:
			c.deliverCompletion(stanza)
		case StanzaObjectReadinessResult:
			c.handleObjectReadinessResult(stanza)
		case StanzaObjectTransferFailure:
			c.failObjectCompletion(stanza)
		case StanzaTransferManifest, StanzaTransferChunk, StanzaTransferFinish, StanzaTransferAbort, StanzaObjectReadinessRequest, StanzaObjectTransfer, StanzaObjectTransferAbort:
			if stanza.inboundLease != nil {
				stanza.inboundLease.Release()
				stanza.inboundLease = nil
			}
			c.admitTransfer(stanza, handoff)
			stanza = Stanza{}
		}
	}
}

func clearAuthenticatedInbound(inbound *AuthenticatedInbound) {
	if inbound == nil {
		return
	}
	clear(inbound.Envelope.Payload.Inline)
	clear(inbound.Envelope.CredentialProof)
	inbound.Reject()
	if inbound.inboundLease != nil {
		inbound.inboundLease.Release()
	}
	*inbound = AuthenticatedInbound{}
}

func (c *Client) fenceIngressOverload(ingress *ingressGeneration) {
	if c == nil || ingress == nil {
		return
	}
	_ = c.transitionIngressFailure(ingress.session, ingress, DurablePending, true)
}

func (c *Client) handleIngressAuthorityChange(ingress *ingressGeneration) {
	if c == nil || ingress == nil {
		return
	}
	var retiredExternal *externalServiceExpiryOwner
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		current := !c.closed && c.started && c.session == ingress.session && c.ingress == ingress && c.sessionEpoch == ingress.id
		alreadyFailed := c.state == DurablePending || c.state == DurableUnknown
		alreadyPaused := !c.membershipReady
		retainedData := c.retainedDataAuthority
		fence := c.authorityFence
		c.mu.state.Unlock()
		if !current {
			return false
		}
		// Pending/unknown already records an admitted authority loss. Repeated
		// control callbacks cannot make progress by invoking the same failing
		// dependency again; retain the fail-closed state and one bounded demand.
		needsFence := retainedData || !alreadyFailed && !alreadyPaused
		fenced := !needsFence || fence != nil && callAuthorityFence(fence)
		fenceFailed := needsFence && !fenced
		if alreadyFailed || fenceFailed {
			c.mu.state.Lock()
			if fenced {
				c.retainedDataAuthority = false
			}
			retiredExternal = c.setStateLocked(DurablePending)
			owner := c.stateTransitionOwnerLocked()
			c.mu.state.Unlock()
			c.enqueueReconnectOwned(owner)
			return true
		}
		c.mu.state.Lock()
		if fenced {
			c.retainedDataAuthority = false
		}
		if !alreadyPaused {
			c.authoritySnapshot = AuthoritySnapshot{}
			c.pauseMembershipLocked("")
		}
		c.authorityPendingIngress = ingress.id
		queueWake := !c.authorityWakeQueued
		if queueWake {
			c.authorityWakeQueued = true
		}
		c.mu.state.Unlock()
		if queueWake {
			select {
			case c.authorityChanges <- struct{}{}:
			default:
				c.mu.state.Lock()
				c.authorityWakeQueued = false
				c.mu.state.Unlock()
			}
		}
		return true
	})
	retireExternalServiceExpiry(retiredExternal)
}

func decodeAuthenticatedInbound(stanza Stanza) (AuthenticatedInbound, bool) {
	owned := stanza.clone()
	return decodeAuthenticatedInboundOwned(&owned)
}

func decodeAuthenticatedInboundOwned(stanza *Stanza) (AuthenticatedInbound, bool) {
	if stanza == nil {
		return AuthenticatedInbound{}, false
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		return AuthenticatedInbound{}, false
	}
	envelope, err := codec.Decode(stanza.Data)
	if err != nil || envelope.Sender != stanza.From || envelope.Recipient != stanza.To || envelope.MeshID != stanza.MeshID {
		clear(envelope.Payload.Inline)
		clear(envelope.CredentialProof)
		return AuthenticatedInbound{}, false
	}
	inbound := AuthenticatedInbound{AuthenticatedSender: stanza.From, Envelope: envelope, inboundLease: stanza.inboundLease, inboundAccept: stanza.inboundAccept}
	clear(stanza.Data)
	stanza.Data = nil
	stanza.inboundLease = nil
	stanza.inboundAccept = nil
	return inbound, true
}

func (c *Client) reconnect() bool {
	return c.reconnectWithOwner(nil)
}

func (c *Client) reconnectOwned(owner stateTransitionOwner) bool {
	return c.reconnectWithOwner(&owner)
}

func (c *Client) reconnectWithOwner(required *stateTransitionOwner) bool {
	if !c.credentialUsable() {
		return false
	}
	if err := c.acquireRecovery(c.ctx); err != nil {
		return false
	}
	defer c.releaseRecovery()
	if err := c.sendMu.LockContext(c.ctx); err != nil {
		return false
	}
	sendLocked := true
	defer func() {
		if sendLocked {
			c.sendMu.Unlock()
		}
	}()
	c.mu.Lock()
	if c.closed || !c.started {
		c.mu.Unlock()
		return false
	}
	owner := c.stateTransitionOwnerLocked()
	if required != nil {
		if !c.stateTransitionOwnerCurrentLocked(*required) {
			c.mu.Unlock()
			return false
		}
		owner = *required
	}
	generation := c.generation
	retained := c.replay
	c.replay = nil
	c.mu.Unlock()
	emitRank2Evidence(c.evidenceRecord("recovery_started", "client", "reconnect", owner.ingress), nil)
	// reconnect owns retained exclusively. On every unsuccessful exit it either
	// restores that exact storage to the client or clears it if Close won.
	defer func() {
		if len(retained) == 0 {
			return
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			clearStanzas(retained)
			retained = nil
			return
		}
		filtered, filterErr := filterReplayMesh(retained, c.config.Auth.MeshID)
		retained = filtered
		displaced := c.replay
		clearStanzas(displaced)
		if filterErr == nil {
			c.replay = retained
		} else {
			c.replay = nil
		}
		retained = nil
		c.mu.Unlock()
	}()
	delay := c.config.ReconnectInitial
	for attempt := 0; attempt < c.config.ReconnectAttempts; attempt++ {
		if attempt > 0 {
			wait := delay
			if c.config.Jitter != nil {
				if j := c.config.Jitter(delay); j >= 0 && j <= c.config.ReconnectMaximum {
					wait = j
				}
			}
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-c.ctx.Done():
				timer.Stop()
				return false
			}
			if delay >= c.config.ReconnectMaximum/2 {
				delay = c.config.ReconnectMaximum
			} else {
				delay *= 2
			}
		}
		c.mu.Lock()
		session := c.session
		c.mu.Unlock()
		record := c.evidenceRecord("recovery_attempt_started", "client", "resume", owner.ingress)
		record.Attempt = attempt + 1
		emitRank2Evidence(record, nil)
		operation, operationCancel := c.reconnectOperation()
		resumed, prepared, err := c.prepareResume(session, operation)
		record = c.evidenceRecord("recovery_resume_result", "client", "prepare_resume", owner.ingress)
		record.Attempt = attempt + 1
		record.Resumed = resumed
		emitRank2Evidence(record, err)
		if err == nil && resumed {
			// TLS, SASL, and <resume/> consume the pre-commit attempt budget. Once
			// the server accepts <resume/>, the replacement socket owns the logical
			// stream and its authority barrier/replay/calibration phase needs one
			// fresh bounded budget; an expiring dial budget must not force a clean bind.
			operationCancel()
			operation, operationCancel = c.reconnectOperation()
		}
		if err == nil && resumed {
			err = c.completeCommittedResume(operation, session, prepared, owner, generation)
			record = c.evidenceRecord("recovery_resume_post_commit_result", "client", "complete_resume", owner.ingress)
			record.Attempt = attempt + 1
			record.Resumed = true
			emitRank2Evidence(record, err)
		}
		operationCancel()
		if err == nil && resumed {
			if c.setRecoveryStateOwned(owner, generation, DurableLive) != nil {
				return false
			}
			clearStanzas(retained)
			retained = nil
			return true
		}
		if !c.hardInvalidateAuthorityOwned(owner, resumeFailureRetainsData(resumed, err)) {
			return false
		}
		var sessionPending []Stanza
		if source, ok := session.(pendingSession); ok {
			operation, operationCancel = c.reconnectOperation()
			sessionPending, err = pendingForReplaySession(source, operation)
			operationCancel()
			if err != nil {
				continue
			}
			if err = validateReplayGraph(retained, sessionPending); err != nil {
				clearStanzas(retained)
				retained = nil
				clearStanzas(sessionPending)
				return false
			}
			sessionPending = applicationReplayValidated(sessionPending)
		}
		retained, err = mergeReplay(retained, sessionPending)
		if err != nil {
			return false
		}
		operation, operationCancel = c.reconnectOperation()
		record = c.evidenceRecord("clean_bind_started", "client", "connect", owner.ingress)
		record.Attempt = attempt + 1
		emitRank2Evidence(record, nil)
		attempt, err := c.connect(operation, generation)
		if err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "connect", owner.ingress), err)
			operationCancel()
			if c.config.DynamicPeerAuthority &&
				(errors.Is(err, ErrAuthentication) || errors.Is(err, ErrIdentityBinding) || errors.Is(err, ErrAuthorityRejected)) {
				c.terminateRejectedRelogin(owner)
				return false
			}
			continue
		}
		emitRank2Evidence(c.evidenceRecord("clean_bind_connected", "client", "authority_sync", owner.ingress), nil)
		newSession, identity := attempt.session, attempt.identity
		var authoritySnapshot AuthoritySnapshot
		if !c.config.DynamicPeerAuthority {
			authoritySnapshot, err = synchronizeAuthoritySession(
				attempt.ctx, newSession, c.config.Auth.MeshID, identity.BoundIdentity,
			)
		}
		if err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "authority_sync", owner.ingress), err)
			_ = attempt.finish(false)
			operationCancel()
			continue
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		old := c.session
		// The old session's XEP-0198 queue is authoritative for cross-kind wire
		// order. The outbox only supplements envelopes absent from that ledger.
		retained, err = filterReplayMesh(retained, c.config.Auth.MeshID)
		if err != nil {
			c.mu.Unlock()
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		owned := c.outbox.Rank2Metadata()
		handled := c.rank2Handled
		c.mu.Unlock()
		retained, err = reconcileRank2Replay(retained, owned, identity.BoundIdentity,
			c.config.Auth.MeshID, handled)
		if err != nil {
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		mailbox, err := catchUpSession(newSession, attempt.ctx, c.config.MailboxLimit)
		if err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "mailbox_catchup", owner.ingress), err)
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		if err = leaseMailbox(attempt.ctx, mailbox, c.inboundBudget); err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "mailbox_lease", owner.ingress), err)
			clearStanzas(mailbox)
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		if err = c.consumeAttemptProof(attempt, generation); err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "consume_proof", owner.ingress), err)
			clearStanzas(mailbox)
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		c.mu.Lock()
		if c.closed || c.generation != generation {
			c.mu.Unlock()
			clearStanzas(mailbox)
			_ = attempt.finish(false)
			operationCancel()
			return false
		}
		if err = attempt.finish(true); err != nil {
			emitRank2Evidence(c.evidenceRecord("clean_bind_failed", "client", "publish", owner.ingress), err)
			c.mu.Unlock()
			clearStanzas(mailbox)
			operationCancel()
			return false
		}
		oldIngress := c.ingress
		lifetime := c.ctx
		c.sessionEpoch++
		newIngress := newIngressGeneration(c.sessionEpoch, newSession, identity.BoundIdentity, mailbox, lifetime)
		newState := DurableMailbox
		if len(mailbox) == 0 {
			newState = DurableLive
			newIngress.finish(nil)
		}
		mailbox = nil
		clear(c.identity.Proof)
		c.session, c.identity, c.ingress = newSession, identity, newIngress
		if c.config.DynamicPeerAuthority {
			c.activateDynamicAuthorityLocked()
		} else {
			authoritySnapshot.session = newSession
			authoritySnapshot.sessionEpoch = newIngress.id
			if c.pauseMembershipLocked(authoritySnapshot.nonce) {
				authoritySnapshot.authorityGeneration = c.authorityGeneration
				c.authoritySnapshot = authoritySnapshot.clone()
			}
		}
		c.bindAuthorityIngressLocked(newSession)
		retiredExternal := c.clearExternalServicesLocked()
		_ = c.setStateLocked(newState)
		c.bindIngressActivationLocked(newIngress)
		// A clean replacement has authenticated, negotiated the authority
		// extension, and downloaded a complete snapshot, but the composed
		// membership owner has not installed that snapshot yet. Keep every old
		// application replay record private until the exact snapshot capability
		// is acknowledged. AcknowledgeAuthoritySnapshot filters the records
		// against the installed membership and only then writes them to this
		// replacement session.
		clearStanzas(c.replay)
		if c.config.DynamicPeerAuthority {
			// XEP-0198 resumption failed: no server custody proof exists for
			// old exact-resource stanzas. This best-effort version neither
			// reroutes nor retries them on a new session.
			clearStanzas(retained)
			retained = nil
			c.replay = nil
		} else {
			c.replay = retained
		}
		retained = nil
		c.mu.Unlock()
		if c.config.DynamicPeerAuthority {
			// A clean bind cannot prove custody of an attempt addressed to the
			// retired exact resource. Keep its immutable payload in the general
			// outbox so the mesh drain can reauthorize the same exact peer before
			// retrying. Old-session custody evidence cannot resurrect a retired
			// entry or release a claim with a newer transport ordinal.
			for _, pending := range owned {
				_ = c.outbox.ReleaseRank2Owned(pending.MessageID, pending.TransportOrdinal)
			}
			if c.deliveryWake != nil {
				c.deliveryWake()
			}
		}
		emitRank2Evidence(c.evidenceRecord("clean_bind_published", "client", "ready", newIngress), nil)
		operationCancel()
		retireExternalServiceExpiry(retiredExternal)
		if oldIngress != nil {
			oldIngress.cancel()
		}
		// The replacement session and its staged authority snapshot are now the
		// sole published send target. Retiring the disconnected session remains a
		// bounded part of recovery, but it must not hold the send-ownership lock:
		// socket shutdown can consume the whole reconnect-operation timeout while
		// replay and authority publication have already made the replacement
		// usable. Fresh sends may therefore serialize on the replacement while the
		// recovery owner finishes best-effort cleanup of the retired session.
		c.sendMu.Unlock()
		sendLocked = false
		c.closeReconnectSession(old)
		select {
		case readyErr := <-newIngress.ready:
			return readyErr == nil && c.recoveryGenerationError(generation) == nil
		case <-lifetime.Done():
			return false
		}
	}
	return false
}

func (c *Client) sendReplayStanza(session Session, ctx context.Context, local string, stanza Stanza) error {
	prepared, ok := c.prepareReplayStanza(local, stanza)
	if !ok {
		return ErrStreamManagement
	}
	if prepared.Release != nil {
		defer prepared.Release()
	}
	return sendSession(session, ctx, prepared.Stanza)
}

// bindAuthorityIngressLocked ties a parser-level control fence to the exact
// currently published Session. A late handler from a replaced session cannot
// target the new authority epoch.
func (c *Client) bindAuthorityIngressLocked(session Session) {
	target, ok := session.(authorityIngressFenceSession)
	if !ok {
		return
	}
	target.setAuthorityIngressFence(func(_ uint64) bool {
		return c.mu.withExclusivePublication(func() bool {
			// The publication lease is exclusive, but the state mutex is held
			// only for admission. The external fence runs without c.mu.state.
			c.mu.state.Lock()
			current := !c.closed && c.started && c.session == session
			fence := c.authorityFence
			c.mu.state.Unlock()
			fenced := current && fence != nil && callAuthorityFence(fence)
			if fenced {
				c.mu.state.Lock()
				if !c.closed && c.started && c.session == session {
					c.retainedDataAuthority = false
					c.authoritySnapshot = AuthoritySnapshot{}
					c.pauseMembershipLocked("")
				}
				c.mu.state.Unlock()
			}
			return fenced
		})
	})
}

func (c *Client) prepareResume(session Session, ctx context.Context) (bool, preparedResumeSession, error) {
	if !c.credentialUsable() {
		return false, nil, ErrAuthentication
	}
	if prepared, ok := session.(preparedResumeSession); ok {
		resumed, err := prepared.PrepareResume(ctx)
		return resumed, prepared, err
	}
	resumed, err := resumeSession(session, ctx)
	return resumed, nil, err
}

func (c *Client) credentialUsable() bool {
	if c == nil {
		return false
	}
	if c.config.CredentialSource == nil && c.config.CredentialUsableUntil.IsZero() {
		return c != nil
	}
	credential, err := c.currentCredential(context.Background())
	clear(credential.Password)
	return err == nil
}

func (c *Client) credentialBoundOperation(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	credential, err := c.currentCredential(parent)
	clear(credential.Password)
	if err != nil {
		operation, cancel := context.WithCancel(parent)
		cancel()
		return operation, func() {}
	}
	return credentialBoundOperation(parent, timeout, credential.UsableUntil)
}

func credentialBoundOperation(parent context.Context, timeout time.Duration, usableUntil time.Time) (context.Context, context.CancelFunc) {
	if usableUntil.IsZero() {
		return context.WithTimeout(parent, timeout)
	}
	credentialContext, credentialCancel := context.WithDeadline(parent, usableUntil)
	operation, operationCancel := context.WithTimeout(credentialContext, timeout)
	return operation, func() {
		operationCancel()
		credentialCancel()
	}
}

func (c *Client) currentCredential(ctx context.Context) (Credential, error) {
	if c == nil {
		return Credential{}, ErrAuthentication
	}
	if c.config.CredentialSource != nil {
		credential, err := c.config.CredentialSource(ctx)
		if err != nil {
			clear(credential.Password)
			if errors.Is(err, ErrUnavailable) {
				return Credential{}, ErrUnavailable
			}
			return Credential{}, ErrAuthentication
		}
		if len(credential.Password) == 0 || credential.UsableUntil.IsZero() || credential.UsableUntil.Location() != time.UTC || c.config.CredentialNow == nil || !c.config.CredentialNow().UTC().Before(credential.UsableUntil) {
			clear(credential.Password)
			return Credential{}, ErrUnavailable
		}
		return credential, nil
	}
	if !c.config.CredentialUsableUntil.IsZero() && (c.config.CredentialNow == nil || !c.config.CredentialNow().UTC().Before(c.config.CredentialUsableUntil)) {
		return Credential{}, ErrAuthentication
	}
	return Credential{Password: append([]byte(nil), c.config.Auth.Password...), UsableUntil: c.config.CredentialUsableUntil}, nil
}

func validSessionResourceConfig(resource string) bool {
	if resource == "" {
		return true
	}
	if len(resource) > 512 || strings.ContainsAny(resource, "/@") {
		return false
	}
	for _, value := range resource {
		if value <= 0x20 || value == 0x7f {
			return false
		}
	}
	return true
}

func (c *Client) credentialExpiryLoop() {
	defer c.wg.Done()
	for {
		remaining := c.config.CredentialUsableUntil.Sub(c.config.CredentialNow().UTC())
		if remaining <= 0 {
			go func() { _ = c.Close(context.Background()) }()
			return
		}
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-c.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (c *Client) waitResumeAuthorityResult(ctx context.Context, session Session) error {
	barrier, ok := session.(resumeAuthorityBarrierSession)
	if !ok {
		// Injected compatibility sessions have no wire-level parser. Production
		// Mellium sessions always implement the ordered server marker.
		return nil
	}
	return barrier.WaitResumeAuthorityResult(ctx)
}

// completeCommittedResume is the shared post-<resumed/> transaction for both
// public and background recovery. The caller owns one fresh bounded context
// for this whole phase; no step is allowed to silently acquire an unbounded
// lifetime after the server has committed the replacement stream.
func (c *Client) completeCommittedResume(ctx context.Context, session Session, prepared preparedResumeSession, owner stateTransitionOwner, generation uint64) error {
	if c == nil || ctx == nil || session == nil {
		return ErrInvalidConfig
	}
	if err := c.waitResumeAuthorityResult(ctx, session); err != nil {
		emitRank2Evidence(c.evidenceRecord("resume_stage_failed", "client", "authority_result", owner.ingress), err)
		return err
	}
	replay := func() error {
		if prepared == nil {
			return nil
		}
		if err := c.replayPrepared(ctx, session, prepared); err != nil {
			emitRank2Evidence(c.evidenceRecord("resume_stage_failed", "client", "replay", owner.ingress), err)
			return err
		}
		return nil
	}
	if !c.config.DynamicPeerAuthority {
		// Legacy replay is still gated by the server's continuity marker.
		if err := replay(); err != nil {
			return err
		}
	}
	if err := c.calibrate(ctx, session); err != nil {
		emitRank2Evidence(c.evidenceRecord("resume_stage_failed", "client", "clock_calibration", owner.ingress), err)
		return err
	}
	if err := c.restoreSuspendedAuthority(ctx, owner); err != nil {
		emitRank2Evidence(c.evidenceRecord("resume_stage_failed", "client", "authority_restore", owner.ingress), err)
		return err
	}
	if c.config.DynamicPeerAuthority {
		c.mu.Lock()
		if c.session != session || c.closed || c.generation != generation {
			c.mu.Unlock()
			return ErrUnavailable
		}
		c.activateDynamicAuthorityLocked()
		c.mu.Unlock()
		if prepared != nil {
			if err := c.reauthorizePreparedReplay(ctx, session); err != nil {
				emitRank2Evidence(c.evidenceRecord("resume_stage_failed", "client", "peer_reauthorization", owner.ingress), err)
				return err
			}
			if err := replay(); err != nil {
				return err
			}
		}
	}
	if err := c.recoveryGenerationError(generation); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// reauthorizePreparedReplay is a fresh server handshake for every exact peer
// that could receive old-stream application data. No old peer cache or mesh
// snapshot is trusted after resume. If any peer cannot be reauthorized, the
// entire prepared replay fails closed and normal clean-bind recovery takes
// over instead of emitting an application stanza to that peer.
func (c *Client) reauthorizePreparedReplay(ctx context.Context, session Session) error {
	peers := make(map[string]struct{})
	for _, pending := range c.outbox.Rank2Metadata() {
		peers[pending.Recipient] = struct{}{}
	}
	source, ok := session.(preparedReplayPeersSession)
	if !ok {
		return ErrUnavailable
	}
	prepared, err := source.PreparedReplayPeers(ctx)
	if err != nil {
		return err
	}
	for _, peer := range prepared {
		peers[peer] = struct{}{}
	}
	if len(peers) == 0 {
		return nil
	}
	authorizer, ok := session.(peerExactAuthorizationSession)
	if !ok {
		return ErrUnavailable
	}
	for peer := range peers {
		if !canonicalFullPeer(peer) {
			return ErrProtocol
		}
		result, err := authorizer.ResolveAuthorizedExactPeer(ctx, peer)
		if err != nil {
			return err
		}
		if !validAuthorizedPeer(result, peer) {
			return ErrProtocol
		}
	}
	return nil
}

func (c *Client) evidenceRecord(event, source, stage string, ingress *ingressGeneration) rank2EvidenceRecord {
	record := rank2EvidenceRecord{Event: event, Source: source, Stage: stage}
	if c == nil {
		return record
	}
	c.mu.Lock()
	record.ClientGeneration = c.generation
	record.StateEpoch = c.stateEpoch
	record.SessionEpoch = c.sessionEpoch
	record.State = uint8(c.state)
	record.Current = !c.closed && c.started
	if ingress != nil {
		record.Current = record.Current && c.ingress == ingress && c.sessionEpoch == ingress.id
	}
	c.mu.Unlock()
	return record
}

func (c *Client) restoreSuspendedAuthority(ctx context.Context, owner stateTransitionOwner) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	var result error
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		if !c.stateTransitionOwnerCurrentLocked(owner) {
			c.mu.state.Unlock()
			result = ErrUnavailable
			return false
		}
		if !c.authoritySuspended {
			c.mu.state.Unlock()
			return true
		}
		resume := c.authorityResume
		c.mu.state.Unlock()
		restored, err := callAuthorityResume(resume, ctx)
		if err != nil {
			result = err
			return false
		}
		c.mu.state.Lock()
		defer c.mu.state.Unlock()
		if !c.stateTransitionOwnerCurrentLocked(owner) {
			result = ErrUnavailable
			return false
		}
		if !restored {
			// A replayed hard authority invalidation may have superseded the
			// suspension. In that case the control lane owns the full resync and
			// the resumed transport itself remains usable but fenced.
			if c.authoritySuspended {
				result = ErrUnavailable
				return false
			}
			return true
		}
		c.authoritySuspended = false
		c.retainedDataAuthority = false
		return true
	})
	return result
}

// hardInvalidateAuthorityOwned retires Rank2 replay membership for the exact
// failed recovery owner. An unobserved network failure may retain the separate
// established Rank1 data capability; authenticated negative/protocol evidence
// invokes the hard root fence before clean fallback.
func resumeFailureRetainsData(resumed bool, err error) bool {
	if !resumed && err == nil {
		return false
	}
	if errors.Is(err, ErrAuthorityRejected) || errors.Is(err, ErrAuthentication) || errors.Is(err, ErrIdentityBinding) || errors.Is(err, ErrProtocol) || errors.Is(err, ErrStreamManagement) {
		return false
	}
	return err != nil && (errors.Is(err, ErrUnavailable) || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

func (c *Client) hardInvalidateAuthorityOwned(owner stateTransitionOwner, retainData bool) bool {
	if c == nil {
		return false
	}
	valid := false
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		if !c.stateTransitionOwnerCurrentLocked(owner) {
			c.mu.state.Unlock()
			return false
		}
		hadRetainedData := c.retainedDataAuthority
		alreadyInvalid := !c.membershipReady && !c.authoritySuspended && !hadRetainedData
		retainEstablishedData := hadRetainedData && retainData
		fence := c.authorityFence
		c.mu.state.Unlock()
		// Failure to resume invalidates Rank2 replay state. It does not itself
		// prove revocation of an already published Rank1 link. Preserve that
		// data capability until fresh authenticated reconciliation, an explicit
		// authority change, logout, shutdown, or expiry performs the hard fence.
		fenced := alreadyInvalid || retainEstablishedData
		if !fenced {
			if hadRetainedData {
				fenced = fence != nil && callAuthorityFence(fence)
			} else {
				fenced = fence == nil || callAuthorityFence(fence)
			}
		}
		c.mu.state.Lock()
		defer c.mu.state.Unlock()
		if !c.stateTransitionOwnerCurrentLocked(owner) {
			return false
		}
		if !alreadyInvalid {
			c.authoritySnapshot = AuthoritySnapshot{}
			c.pauseMembershipLocked("")
		}
		if !retainEstablishedData && fenced {
			c.retainedDataAuthority = false
		}
		valid = fenced
		return true
	})
	return valid
}

func (c *Client) replayPrepared(ctx context.Context, wire Session, session preparedResumeSession) error {
	c.mu.Lock()
	meshID, local := c.config.Auth.MeshID, c.identity.BoundIdentity
	c.mu.Unlock()
	pending := c.outbox.Rank2Metadata()
	seen := make(map[uint64]struct{}, len(pending))
	nextOwned, lastEnvelope := 0, uint64(0)
	err := session.ReplayPrepared(ctx, func(stanza Stanza) bool {
		if !replayMeshAllowed(stanza, meshID) {
			return false
		}
		if stanza.Kind == StanzaEnvelope {
			if len(pending) == 0 {
				return true
			}
			if stanza.Ordinal == 0 || stanza.Ordinal <= lastEnvelope {
				return false
			}
			for nextOwned < len(pending) && pending[nextOwned].TransportOrdinal < stanza.Ordinal {
				if _, ok := seen[pending[nextOwned].TransportOrdinal]; !ok {
					return false
				}
				nextOwned++
			}
			if nextOwned < len(pending) && pending[nextOwned].TransportOrdinal == stanza.Ordinal {
				seen[stanza.Ordinal] = struct{}{}
				nextOwned++
			}
			lastEnvelope = stanza.Ordinal
		}
		return true
	}, func(stanza Stanza) (preparedReplayStanza, bool) {
		return c.prepareReplayStanza(local, stanza)
	})
	if err != nil {
		return err
	}
	for _, metadata := range pending {
		if _, ok := seen[metadata.TransportOrdinal]; ok {
			continue
		}
		if metadata.TransportOrdinal <= lastEnvelope || metadata.MeshID != meshID {
			return ErrStreamManagement
		}
		stanza, ok := c.pendingEnvelopeStanza(local, metadata)
		if !ok {
			return ErrStreamManagement
		}
		if err := sendSession(wire, ctx, stanza); err != nil {
			clear(stanza.Data)
			return err
		}
		clear(stanza.Data)
		lastEnvelope = metadata.TransportOrdinal
	}
	return nil
}

// reconcileRank2Replay consumes retained ledger metadata and inserts outbox
// envelopes that are absent because their raw SM acknowledgements arrived
// before custody acceptance. Acknowledged omissions are a prefix of the live
// ledger; never-admitted owned attempts are a suffix. Anything in the middle
// would violate the serialized wire/outbox ordering contract and fails closed.
func reconcileRank2Replay(retained []Stanza, owned []outbox.Rank2Metadata,
	local, meshID string, handled uint64) (result []Stanza, err error) {
	defer func() {
		if err != nil {
			clearStanzas(retained)
			clearStanzas(result)
			result = nil
		}
	}()
	if protocol.ValidateAgentIdentity(local) != nil || protocol.ValidateMeshID(meshID) != nil {
		return nil, ErrStreamManagement
	}
	byOrdinal := make(map[uint64]outbox.Rank2Metadata, len(owned))
	previous := uint64(0)
	for _, metadata := range owned {
		if metadata.TransportOrdinal == 0 || metadata.TransportOrdinal <= previous ||
			metadata.Sender != local || metadata.MeshID != meshID ||
			protocol.ValidateAgentIdentity(metadata.Recipient) != nil ||
			!validReadinessTyped(metadata.MessageID, "msg_", 16) {
			return nil, ErrStreamManagement
		}
		previous = metadata.TransportOrdinal
		byOrdinal[metadata.TransportOrdinal] = metadata
	}

	matched := make(map[uint64]struct{}, len(owned))
	firstEnvelope, lastEnvelope := uint64(0), uint64(0)
	write := 0
	for read := range retained {
		stanza := &retained[read]
		if stanza.Kind == StanzaEnvelope {
			if !validEnvelopeReplayMetadata(*stanza, meshID) {
				return nil, ErrStreamManagement
			}
			metadata, exists := byOrdinal[stanza.Ordinal]
			if !exists {
				// Exact custody may have retired this outbox entry while a
				// reconnect snapshot was being assembled. It no longer needs
				// hydration or replay.
				clearStanzaOwned(stanza)
				continue
			}
			if metadata.MessageID != stanza.MessageID || metadata.Sender != stanza.From ||
				metadata.Recipient != stanza.To || metadata.MeshID != stanza.MeshID {
				return nil, ErrStreamManagement
			}
			if _, duplicate := matched[stanza.Ordinal]; duplicate ||
				(lastEnvelope != 0 && stanza.Ordinal <= lastEnvelope) {
				return nil, ErrStreamManagement
			}
			matched[stanza.Ordinal] = struct{}{}
			if firstEnvelope == 0 {
				firstEnvelope = stanza.Ordinal
			}
			lastEnvelope = stanza.Ordinal
		}
		if write != read {
			retained[write] = *stanza
			*stanza = Stanza{}
		}
		write++
	}
	retained = retained[:write]

	prefix := make([]Stanza, 0, len(owned))
	suffix := make([]Stanza, 0, len(owned))
	for _, metadata := range owned {
		if _, exists := matched[metadata.TransportOrdinal]; exists {
			continue
		}
		stanza := Stanza{
			Kind: StanzaEnvelope, From: metadata.Sender, To: metadata.Recipient,
			MeshID: metadata.MeshID, Ordinal: metadata.TransportOrdinal,
			MessageID: metadata.MessageID,
		}
		if metadata.TransportOrdinal <= handled {
			if firstEnvelope != 0 && metadata.TransportOrdinal >= firstEnvelope {
				return nil, ErrStreamManagement
			}
			prefix = append(prefix, stanza)
			continue
		}
		if lastEnvelope != 0 && metadata.TransportOrdinal <= lastEnvelope {
			return nil, ErrStreamManagement
		}
		suffix = append(suffix, stanza)
	}

	result = make([]Stanza, 0, len(prefix)+len(retained)+len(suffix))
	result = append(result, prefix...)
	for i := range retained {
		result = append(result, retained[i])
		retained[i] = Stanza{}
	}
	result = append(result, suffix...)
	if validateErr := validateReplayGraph(result, nil); validateErr != nil {
		return result, validateErr
	}
	return result, nil
}

func (c *Client) prepareReplayStanza(local string, stanza Stanza) (preparedReplayStanza, bool) {
	if stanza.Kind != StanzaEnvelope {
		return preparedReplayStanza{Stanza: stanza}, true
	}
	if !validEnvelopeReplayMetadata(stanza, c.config.Auth.MeshID) || stanza.From != local {
		return preparedReplayStanza{}, false
	}
	outbound, ok := c.pendingEnvelopeStanza(local, outbox.Rank2Metadata{
		MessageID:        stanza.MessageID,
		MeshID:           stanza.MeshID,
		TransportOrdinal: stanza.Ordinal,
	})
	if !ok {
		return preparedReplayStanza{}, false
	}
	if outbound.To != stanza.To || outbound.MeshID != stanza.MeshID || outbound.MessageID != stanza.MessageID || outbound.Ordinal != stanza.Ordinal {
		clearStanzaOwned(&outbound)
		return preparedReplayStanza{}, false
	}
	return preparedReplayStanza{
		Stanza: outbound,
		Release: func() {
			clearStanzaOwned(&outbound)
		},
	}, true
}

func clearStanzas(stanzas []Stanza) {
	for i := range stanzas {
		clearStanzaOwned(&stanzas[i])
	}
}

func replayMeshAllowed(stanza Stanza, meshID string) bool {
	if stanza.MeshID != meshID {
		return false
	}
	if stanza.Kind == StanzaObjectReadinessRequest {
		_, err := decodeObjectReadinessRequest(stanza.Data)
		return err == nil
	}
	if stanza.Kind == StanzaObjectReadinessResult {
		_, err := decodeObjectReadinessResult(stanza.Data)
		return err == nil
	}
	if stanza.Kind == StanzaObjectTransfer || stanza.Kind == StanzaObjectTransferFailure || stanza.Kind == StanzaObjectTransferAbort {
		publication, err := decodeObjectPublication(stanza.Data)
		if err != nil {
			return false
		}
		defer clearObjectPublicationOwned(&publication)
		return true
	}
	if stanza.Kind != StanzaEnvelope {
		return true
	}
	return validEnvelopeReplayMetadata(stanza, meshID)
}

func validEnvelopeReplayMetadata(stanza Stanza, meshID string) bool {
	return stanza.Kind == StanzaEnvelope &&
		stanza.MeshID == meshID &&
		stanza.Ordinal != 0 &&
		len(stanza.Data) == 0 &&
		validReadinessTyped(stanza.MessageID, "msg_", 16) &&
		protocol.ValidateMeshID(stanza.MeshID) == nil &&
		protocol.ValidateAgentIdentity(stanza.From) == nil &&
		protocol.ValidateAgentIdentity(stanza.To) == nil
}

func isSessionControlStanza(kind StanzaKind) bool {
	return kind == StanzaTimeCalibration || kind == StanzaAuthoritySync || kind == StanzaAuthorityDiscovery || kind == StanzaUploadSlotQuery || kind == StanzaExternalServiceQuery || kind == StanzaSessionPingResult || kind == StanzaPeerAuthorization || kind == StanzaPeerRevocationAck || kind == StanzaPeerRevocationPacketAck || kind == StanzaSessionPingQuery
}

func (c *Client) reconnectOperation() (context.Context, context.CancelFunc) {
	return c.credentialBoundOperation(c.ctx, c.config.ReconnectOperationTimeout)
}

func (c *Client) acquireRecovery(ctx context.Context) error {
	if c == nil || ctx == nil || c.recoveryGate == nil {
		return ErrInvalidConfig
	}
	select {
	case <-c.recoveryGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) releaseRecovery() {
	if c == nil || c.recoveryGate == nil {
		panic("rank2xmpp: recovery ownership released without initialization")
	}
	select {
	case c.recoveryGate <- struct{}{}:
	default:
		panic("rank2xmpp: recovery ownership released twice")
	}
}

func (c *Client) recoveryGenerationError(generation uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation != generation {
		return ErrClosed
	}
	if !c.started {
		return ErrUnavailable
	}
	return nil
}

// setRecoveryState is the recovery transaction's final linearization point.
// Close changes generation before canceling the client lifetime, so even a
// non-cooperative dependency that returns after cancellation cannot publish a
// live state into a retired client generation.
func (c *Client) setRecoveryState(generation uint64, state DurableState) error {
	owner, ok := c.captureStateTransitionOwner()
	if !ok {
		return ErrClosed
	}
	return c.setRecoveryStateOwned(owner, generation, state)
}

func (c *Client) setRecoveryStateOwned(owner stateTransitionOwner, generation uint64, state DurableState) error {
	if state == DurablePending || state == DurableUnknown {
		if owner.generation != generation {
			return ErrClosed
		}
		return c.transitionOwnedState(owner, state, false, false)
	}
	c.mu.Lock()
	if c.closed || c.generation != generation {
		c.mu.Unlock()
		return ErrClosed
	}
	if !c.started || !c.stateTransitionOwnerCurrentLocked(owner) {
		c.mu.Unlock()
		return ErrUnavailable
	}
	retiredExternal := c.setStateLocked(state)
	c.mu.Unlock()
	retireExternalServiceExpiry(retiredExternal)
	return nil
}

func (c *Client) closeReconnectSession(session Session) {
	if session == nil {
		return
	}
	ctx, cancel := c.reconnectOperation()
	_ = closeSession(session, ctx)
	cancel()
}

func (c *Client) pendingEnvelopeStanza(local string, pending outbox.Rank2Metadata) (Stanza, bool) {
	codec, err := protocol.NewCodec()
	if err != nil {
		return Stanza{}, false
	}
	envelope, err := c.outbox.GetRank2(pending.MessageID, pending.TransportOrdinal)
	if err != nil {
		return Stanza{}, false
	}
	encoded, err := codec.Encode(envelope)
	clear(envelope.Payload.Inline)
	clear(envelope.CredentialProof)
	if err != nil {
		return Stanza{}, false
	}
	return Stanza{Kind: StanzaEnvelope, From: local, To: envelope.Recipient, MeshID: envelope.MeshID, Ordinal: pending.TransportOrdinal, MessageID: pending.MessageID, Data: encoded}, true
}

func (c *Client) markRank2Handled(handled uint64) error {
	if handled == 0 {
		return nil
	}
	c.mu.Lock()
	if handled < c.rank2Handled || handled >= c.rank2NextOrdinal && c.rank2NextOrdinal != 0 {
		c.mu.Unlock()
		return outbox.ErrInvalidEvidence
	}
	c.rank2Handled = handled
	c.mu.Unlock()
	return nil
}

// retireRank2Custody is the only Rank-2 server handoff retirement boundary.
// Raw XEP-0198 handled evidence advances the transient wire ledger but does
// not prove that the exact-resource mailbox transaction committed.
func (c *Client) retireRank2Custody(messageID string) error {
	if c == nil || !validReadinessTyped(messageID, "msg_", 16) {
		return outbox.ErrInvalidEvidence
	}
	var ordinal uint64
	for _, metadata := range c.outbox.Rank2Metadata() {
		if metadata.MessageID == messageID {
			ordinal = metadata.TransportOrdinal
			break
		}
	}
	if ordinal == 0 {
		// A duplicate or stale acceptance is deliberately inert.
		return outbox.ErrInvalidEvidence
	}
	if err := c.outbox.RetireRank2Owned(messageID, ordinal); err != nil {
		return err
	}
	c.mu.Lock()
	wake := c.deliveryWake
	c.mu.Unlock()
	if wake != nil {
		wake()
	}
	return nil
}

func (c *Client) transferLoop(jobs <-chan transferWork) {
	defer c.wg.Done()
	for {
		select {
		case work := <-jobs:
			stanza := work.stanza
			func() {
				defer c.releaseTransferWork(work)
				if stanza.Kind == StanzaObjectReadinessRequest {
					c.mu.Lock()
					admitter := c.payloadWork
					c.mu.Unlock()
					if admitter == nil {
						c.handleObjectReadinessRequest(c.ctx, stanza)
						return
					}
					charge, ok := retainedStanzaBytes(stanza)
					if !ok {
						return
					}
					_ = admitter.AdmitAuthenticatedPayloadWork(c.ctx, stanza.From, uint64(charge), func(runCtx context.Context) error {
						c.handleObjectReadinessRequest(runCtx, stanza)
						return runCtx.Err()
					})
					return
				}
				if stanza.Kind == StanzaObjectTransfer {
					publication, decodeErr := decodeObjectPublication(stanza.Data)
					if decodeErr != nil {
						_ = c.sendStanza(c.ctx, Stanza{Kind: StanzaObjectTransferFailure, From: stanza.To, To: stanza.From, MeshID: stanza.MeshID, TransferID: stanza.TransferID, MessageID: stanza.MessageID, Data: stanza.Data})
						return
					}
					authorized := func() bool {
						defer clearObjectPublicationOwned(&publication)
						return c.authorizeObjectPublication(stanza, publication)
					}()
					if !authorized {
						_ = c.sendStanza(c.ctx, Stanza{Kind: StanzaObjectTransferFailure, From: stanza.To, To: stanza.From, MeshID: stanza.MeshID, TransferID: stanza.TransferID, MessageID: stanza.MessageID, Data: stanza.Data})
						return
					}
				}
				c.mu.Lock()
				receiver := c.receiver
				c.mu.Unlock()
				route := work.route
				if receiver == nil || !transferRouteAuthorizes(route, stanza) {
					return
				}
				evidence, err := callTransferReceiver(receiver, c.ctx, route, stanza.clone())
				if (stanza.Kind == StanzaTransferFinish || stanza.Kind == StanzaObjectTransfer) && err == nil && evidence != nil && evidence.TransferID == stanza.TransferID && evidence.MessageID == route.MessageID {
					if !c.admitMembershipPublication() {
						return
					}
					_ = c.sendStanza(c.ctx, Stanza{Kind: StanzaTransferCompletion, From: stanza.To, To: stanza.From, MeshID: stanza.MeshID, TransferID: stanza.TransferID, MessageID: route.MessageID, Evidence: *evidence})
				} else if stanza.Kind == StanzaObjectTransfer && err != nil {
					if !c.admitMembershipPublication() {
						return
					}
					_ = c.sendStanza(c.ctx, Stanza{Kind: StanzaObjectTransferFailure, From: stanza.To, To: stanza.From, MeshID: stanza.MeshID, TransferID: stanza.TransferID, MessageID: route.MessageID, Data: stanza.Data})
				}
			}()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) admitMembershipPublication() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.started && c.membershipReady
}

func transferLane(id string, count int) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		hash ^= uint32(id[i])
		hash *= 16777619
	}
	return int(hash % uint32(count))
}

func (c *Client) Observe() transport.Observation {
	if c == nil {
		return transport.Observation{State: transport.HealthClosed}
	}
	c.mu.Lock()
	started, closed, state, progress := c.started, c.closed, c.state, c.progress
	owner := c.stateTransitionOwnerLocked()
	c.mu.Unlock()
	if closed {
		return transport.Observation{State: transport.HealthClosed}
	}
	if !started {
		return transport.Observation{State: transport.HealthConnecting}
	}
	if _, ok := c.clock.Snapshot(); !ok {
		if owner, owned := c.captureStateTransitionOwner(); owned {
			_ = c.transitionOwnedState(owner, DurablePending, true, true)
		}
		return transport.Observation{State: transport.HealthDisconnected}
	}
	if state == DurablePending {
		// Recovery is demand-driven after each bounded reconnect sequence.
		// A health observation supplies a new bounded demand, allowing the same
		// Core to establish a fresh authorized bind after administrative re-add
		// without an unbounded autonomous authentication loop while absent.
		c.requestReconnectOwned(owner)
		return transport.Observation{State: transport.HealthDisconnected}
	}
	return transport.Observation{State: transport.HealthHealthy, LastProgress: progress}
}

func (c *Client) DurableState() DurableState { c.mu.Lock(); defer c.mu.Unlock(); return c.state }

// MembershipReady reports whether the current durable session's exact
// authority snapshot has been installed by the membership owner. State edges
// are coalescible notifications, so recovery monitors must inspect this level
// condition instead of relying on having observed an earlier Pending edge.
func (c *Client) MembershipReady() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.started && c.state == DurableLive && c.membershipReady
}

// stateTransitionOwner binds a delayed lifecycle transition to the exact
// authenticated session publication that observed the failure. The client
// lifetime generation alone is insufficient because a clean reconnect can
// replace session and ingress without changing that generation.
type stateTransitionOwner struct {
	generation   uint64
	stateEpoch   uint64
	session      Session
	ingress      *ingressGeneration
	sessionEpoch uint64
}

type reconnectRequest struct {
	owner stateTransitionOwner
}

func (c *Client) stateTransitionOwnerLocked() stateTransitionOwner {
	return stateTransitionOwner{
		generation:   c.generation,
		stateEpoch:   c.stateEpoch,
		session:      c.session,
		ingress:      c.ingress,
		sessionEpoch: c.sessionEpoch,
	}
}

func (c *Client) captureStateTransitionOwner() (stateTransitionOwner, bool) {
	if c == nil {
		return stateTransitionOwner{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.started {
		return stateTransitionOwner{}, false
	}
	return c.stateTransitionOwnerLocked(), true
}

func (c *Client) stateTransitionOwnerCurrentLocked(owner stateTransitionOwner) bool {
	return !c.closed && c.started && c.generation == owner.generation && c.stateEpoch == owner.stateEpoch && c.session == owner.session && c.ingress == owner.ingress && c.sessionEpoch == owner.sessionEpoch
}

// transitionOwnedState is the delayed state publication linearization used by
// background reconnect, clock invalidation, and health observation. Its
// exact publication edge is checked both before and after the external
// callback, whose panic is contained by callAuthorityFence. The exclusive
// publication lease makes an admitted authority fence atomic with Close,
// recovery, and session replacement while leaving the ordinary state mutex
// released during the callback.
// A clock-triggered transition additionally proves that the triggering
// calibration is still invalid, so an old closed invalidation channel cannot
// demote a newly calibrated current session.
func (c *Client) transitionOwnedState(owner stateTransitionOwner, state DurableState, reconnect, requireClockInvalid bool) error {
	if c == nil {
		return ErrInvalidConfig
	}
	var retiredExternal *externalServiceExpiryOwner
	var admitted, fenceFailed bool
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		current := c.stateTransitionOwnerCurrentLocked(owner)
		alreadyFailed := c.state == DurablePending || c.state == DurableUnknown
		retainedData := c.retainedDataAuthority
		retainData := retainedData && !requireClockInvalid
		needsFence := (state == DurablePending || state == DurableUnknown) && !retainData && (retainedData || !alreadyFailed)
		fence := c.authorityFence
		c.mu.state.Unlock()
		if !current {
			return false
		}
		if requireClockInvalid {
			if c.clock == nil {
				return false
			}
			if _, ok := c.clock.Snapshot(); ok {
				return false
			}
		}
		admitted = true
		if needsFence {
			if fence == nil {
				fenceFailed = retainedData
			} else if !callAuthorityFence(fence) {
				fenceFailed = true
			}
		}
		// Calibration is independent of Client publication. Recheck it after
		// the potentially non-cooperative fence so a recovery that refreshed
		// the clock while this goroutine waited makes the old edge inert.
		if requireClockInvalid {
			if _, ok := c.clock.Snapshot(); ok {
				admitted = false
				return false
			}
		}
		c.mu.state.Lock()
		if !c.stateTransitionOwnerCurrentLocked(owner) {
			c.mu.state.Unlock()
			admitted = false
			return false
		}
		if needsFence && !fenceFailed {
			c.retainedDataAuthority = false
		}
		retiredExternal = c.setStateLocked(state)
		// setStateLocked advances stateEpoch, so the reconnect request must
		// own the resulting publication rather than the retired input owner.
		nextOwner := c.stateTransitionOwnerLocked()
		c.mu.state.Unlock()
		if reconnect {
			c.enqueueReconnectOwned(nextOwner)
		}
		return true
	})
	retireExternalServiceExpiry(retiredExternal)
	if !admitted || fenceFailed {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) setState(state DurableState) {
	owner, ok := c.captureStateTransitionOwner()
	if !ok {
		return
	}
	_ = c.transitionOwnedState(owner, state, false, false)
}
func (c *Client) setStateLocked(state DurableState) *externalServiceExpiryOwner {
	var retiredExternal *externalServiceExpiryOwner
	if state == DurablePending || state == DurableUnknown {
		retiredExternal = c.clearExternalServicesLocked()
		c.authoritySnapshot = AuthoritySnapshot{}
		c.pauseMembershipLocked("")
	}
	if c.state == state {
		return retiredExternal
	}
	c.state = state
	c.stateEpoch++
	if c.stateChanged != nil {
		close(c.stateChanged)
	}
	c.stateChanged = make(chan struct{})
	return retiredExternal
}

func (c *Client) setStatePreservingAuthorityLocked(state DurableState) *externalServiceExpiryOwner {
	var retiredExternal *externalServiceExpiryOwner
	if state == DurablePending || state == DurableUnknown {
		retiredExternal = c.clearExternalServicesLocked()
	}
	if c.state == state {
		return retiredExternal
	}
	c.state = state
	c.stateEpoch++
	if c.stateChanged != nil {
		close(c.stateChanged)
	}
	c.stateChanged = make(chan struct{})
	return retiredExternal
}

// StateChanged is a lossless edge notification: callers capture the channel,
// wait for closure, then read DurableState and capture the next channel.
func (c *Client) StateChanged() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	c.mu.Lock()
	changed := c.stateChanged
	c.mu.Unlock()
	return changed
}
func (c *Client) closedChan() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx == nil {
		return nil
	}
	return c.ctx.Done()
}

func (c *Client) Close(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	var (
		retiredExternal     *externalServiceExpiryOwner
		cancel, startCancel context.CancelFunc
		startDone           chan struct{}
		session             Session
		ingress             *ingressGeneration
		closeDone           chan struct{}
		closeClaimed        bool
	)
	c.mu.withExclusivePublication(func() bool {
		c.mu.state.Lock()
		if c.closed {
			closeDone = c.closeDone
			c.mu.state.Unlock()
			return false
		}
		fence := c.authorityFence
		c.retainedDataAuthority = false
		c.mu.state.Unlock()
		if fence != nil {
			_ = callAuthorityFence(fence)
		}
		c.mu.state.Lock()
		c.closed = true
		c.generation++
		c.closeDone = make(chan struct{})
		closeDone = c.closeDone
		retiredExternal = c.setStateLocked(DurableUnknown)
		closedFailure := c.closedFailureLocked()
		cancel, startCancel, startDone, session, ingress = c.cancel, c.startCancel, c.startDone, c.session, c.ingress
		clear(c.identity.Proof)
		c.identity = Authenticated{}
		for _, waiter := range c.complete {
			select {
			case waiter.result <- completion{err: closedFailure}:
			default:
			}
		}
		c.complete = make(map[string]completionWaiter)
		for i := range c.replay {
			clear(c.replay[i].Data)
		}
		c.replay = nil
		for _, waiter := range c.jingle {
			select {
			case waiter.result <- jingleResult{err: closedFailure}:
			default:
			}
		}
		c.jingle = make(map[string]*jingleWaiter)
		c.retireObjectReadinessLocked(closedFailure)
		c.closeTransferAdmissionLocked()
		closeClaimed = true
		c.mu.state.Unlock()
		return true
	})
	if closeClaimed {
		if ingress != nil {
			ingress.cancel()
		}
		if startCancel != nil {
			startCancel()
		}
		if cancel != nil {
			cancel()
		}
		go c.finishClose(retiredExternal, session, startDone)
	}
	if closeDone == nil {
		return ErrClosed
	}
	select {
	case <-closeDone:
		c.mu.Lock()
		result := c.closeErr
		c.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) finishClose(retiredExternal *externalServiceExpiryOwner, session Session, startDone chan struct{}) {
	if retiredExternal != nil {
		retireExternalServiceExpiry(retiredExternal)
	}
	var result error
	if session != nil {
		result = normalize(closeSession(session, context.Background()), context.Background(), ErrClosed)
	}
	c.wg.Wait()
	if startDone != nil {
		<-startDone
	}
	c.receiveWG.Wait()
	for {
		select {
		case inbound := <-c.inbound:
			clearAuthenticatedInbound(&inbound)
		default:
			goto signals
		}
	}

signals:
	for {
		select {
		case signal := <-c.signals:
			clearStanzaOwned(&signal)
		default:
			goto drained
		}
	}

drained:
	c.mu.Lock()
	if c.ingress != nil {
		clearStanzas(c.ingress.mailbox)
		c.ingress.mailbox = nil
	}
	clear(c.config.Auth.Password)
	c.config.Auth.Password = nil
	c.closeErr = result
	done := c.closeDone
	c.mu.Unlock()
	if c.outbox != nil {
		c.outbox.Destroy()
	}
	close(done)
}

func normalize(err error, ctx context.Context, fallback error) error {
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
