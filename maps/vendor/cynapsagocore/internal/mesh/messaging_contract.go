package mesh

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/policy"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// FailureCode is the complete application-service failure vocabulary. It is
// deliberately text-free so dependency details cannot escape through root
// composition.
type FailureCode uint8

const (
	FailureCancelled FailureCode = iota + 1
	FailureDeadline
	FailureUnavailable
	FailureRejected
	FailureAuthorization
	FailureCapacity
	FailureInvalidHandle
	FailurePayloadTooLarge
	FailurePayloadIntegrity
	FailurePayloadTransfer
	FailureInternal
)

type Failure struct{ Code FailureCode }

type CarrierDisposition uint8

const (
	// CarrierAccepted is trusted terminal transport acceptance. It permits the
	// process outbox entry to be released; it is not remote application work.
	CarrierAccepted CarrierDisposition = iota + 1
	// CarrierAmbiguous and CarrierUnavailable retain the exact outbox entry for
	// identity-preserving retry.
	CarrierAmbiguous
	CarrierUnavailable
	CarrierRejected
	CarrierCapacity
	// CarrierRank1PendingACK means the exact envelope was written on one
	// authenticated Rank1 session but remains owned by the Core outbox until
	// the receiving Core's exact receipt is verified.
	CarrierRank1PendingACK
)

type EnvelopeCarrier interface {
	// Send borrows an immutable envelope only for the duration of the call. A
	// carrier that retains it must make and account for its own bounded copy.
	Send(context.Context, protocol.Envelope) CarrierDisposition
}

// PeerClosingCarrier is the exact-once Rank1 teardown seam owned by a removed
// peer lane. Durable server-session ownership is mesh-wide and is not closed.
type PeerClosingCarrier interface {
	ClosePeer(context.Context, string) error
}

type Rank1ReceiptBinding struct {
	MessageID, ConversationID, Sender, Recipient, MeshID string
	ChannelBinding                                       [sha256.Size]byte
}

type PendingRank1Receipt interface {
	Binding() Rank1ReceiptBinding
	Done() <-chan bool
	Close()
}

// InboundRank1Receipt is an identity-only route back to the exact
// authenticated Rank1 session that supplied one envelope. MessagingService
// owns it after admission and closes it without acknowledgement on rejection.
type InboundRank1Receipt interface {
	Binding() Rank1ReceiptBinding
	Acknowledge(context.Context) error
	Close()
}

// Rank1ReceiptCarrier is an optional private stronger seam. SendWithReceipt
// may choose Rank2 directly; a non-nil receipt is valid only with
// CarrierRank1PendingACK. Fallback sends directly through Rank2 after the
// exact Rank1 pending state is closed.
type Rank1ReceiptCarrier interface {
	SendWithReceipt(context.Context, protocol.Envelope) (PendingRank1Receipt, CarrierDisposition)
	Fallback(context.Context, protocol.Envelope) CarrierDisposition
}

type CarrierAuthentication = policy.CarrierAuthentication
type AuthenticatedProvenance = policy.AuthenticatedProvenance

const (
	CarrierGroupRank2 = policy.CarrierGroupRank2
	CarrierBoundRank1 = policy.CarrierBoundRank1
)

func NewGroupRank2Provenance(sender, recipient, meshID string) (AuthenticatedProvenance, error) {
	return policy.NewGroupRank2Provenance(sender, recipient, meshID)
}

func NewBoundRank1Provenance(sender, recipient, meshID string, binding [sha256.Size]byte) (AuthenticatedProvenance, error) {
	return policy.NewBoundRank1Provenance(sender, recipient, meshID, binding)
}

type PayloadDisposition uint8

const (
	PayloadAccepted PayloadDisposition = iota + 1
	PayloadRejected
	PayloadAuthorization
	PayloadUnavailable
	PayloadCapacity
	PayloadTooLarge
	PayloadIntegrity
	PayloadTransferFailed
	PayloadInvalidHandle
	PayloadCancelled
	PayloadDeadline
	PayloadInternal
)

type PreparedPayload struct {
	Profile         string
	Canonical       []byte
	ApplicationPath string
}

func (prepared PreparedPayload) clone() PreparedPayload {
	prepared.Canonical = append([]byte(nil), prepared.Canonical...)
	return prepared
}

func (request PreparedSend) clone() PreparedSend {
	request.Payload = request.Payload.clone()
	return request
}

type PreparedSend struct {
	PeerID, MessageID, MeshID, SenderID, RecipientID, ConversationID string
	Mode                                                             protocol.Mode
	CorrelationID, ReplyTo                                           string
	CreatedAt                                                        time.Time
	ExpiresAt                                                        time.Time
	ClockUncertainty                                                 time.Duration
	Payload                                                          PreparedPayload
	conversation                                                     *conversationState
	peerEpoch                                                        uint64
}

type EnvelopePublication struct {
	PeerID, MessageID, MeshID, SenderID, RecipientID, ConversationID string
	Mode                                                             protocol.Mode
	CorrelationID, ReplyTo                                           string
	CreatedAt                                                        time.Time
	ExpiresAt                                                        time.Time
	ClockUncertainty                                                 time.Duration
	Descriptor                                                       protocol.PayloadDescriptor
}

type EnvelopePublisher interface {
	// PublishEnvelope borrows publication only through the callback. An
	// implementation must clone its descriptor before retaining it or using it
	// asynchronously.
	PublishEnvelope(context.Context, EnvelopePublication) *Failure
}

// PayloadPipelineFactory solves the publisher construction cycle without an
// untyped registry. A successful pipeline owns payload serialization,
// transfer selection, materialization, and strict reconstruction.
type PayloadPipelineFactory interface {
	Create(EnvelopePublisher) (PayloadPipeline, PayloadDisposition)
}

type PayloadPipeline interface {
	// Prepare transfers ownership of PreparedPayload.Canonical to the caller.
	Prepare(model.Payload) (PreparedPayload, PayloadDisposition)
	// Send borrows request and every reachable byte only for the call.
	Send(context.Context, PreparedSend) PayloadDisposition
	// Materialize transfers ownership of the returned canonical bytes.
	Materialize(context.Context, protocol.Envelope) ([]byte, PayloadDisposition)
	Decode([]byte) (model.Payload, PayloadDisposition)
	ApplicationPath(string, []byte) (string, PayloadDisposition)
}

// PeerPayloadRetirer is the optional stronger cleanup seam implemented by the
// production pipeline. Snapshot removal uses it to cancel and scrub retained
// transfer state before any surviving peer lane resumes.
type PeerPayloadAuthority interface {
	RetirePeer(meshID, peerID string)
	AllowPeer(meshID, peerID string)
}

type DeliveryDisposition uint8

const (
	// DeliveryAccepted means the SDK has safely assumed local responsibility
	// through the mandatory delivery.accept handshake.
	DeliveryAccepted DeliveryDisposition = iota + 1
	DeliveryUnavailable
	DeliveryCapacity
	DeliveryRejected
)

type InboundDelivery struct {
	MessageID, ConversationID, FromAgentID, MeshID string
	Mode                                           protocol.Mode
	RequestHandle                                  string
	Payload                                        model.Payload
}

type DeliverySink interface {
	// Deliver must honor ctx. Implementations serialize the event through the
	// single Pod 2 lease and return Accepted only after delivery.accept commits.
	Deliver(context.Context, InboundDelivery) DeliveryDisposition
}

// RPCTimeoutSource returns one atomically published current configuration
// snapshot. MessageRequest reads it at most once, and only when its TTL is 0.
type RPCTimeoutSource interface {
	SnapshotRPCTimeout() time.Duration
}

// CalibratedTime is one atomic reading from the sole Pod 3 time authority.
// Uncertainty is a conservative bound supplied by the clock owner; Pod 3 does
// not estimate drift. It must be positive and no greater than the protocol cap.
type CalibratedTime struct {
	UTC         time.Time
	Uncertainty time.Duration
}

// CalibratedUTCClock is calibrated from the authenticated Mesh Server through
// an XEP-0202 midpoint measurement before the authenticated graph becomes
// ready. Snapshot advances from a monotonic local base plus that offset,
// returns UTC and its current bounded uncertainty atomically, and never mutates
// the operating-system clock. Rank 1 does not independently calibrate it.
type CalibratedUTCClock interface {
	Snapshot() CalibratedTime
}

// MessagingConfig bounds every service-owned table. QueueCapacity is the one
// authoritative deployment capacity used for conversations, RPC, topology,
// outbox messages, and handler interests.
type MessagingConfig struct {
	QueueCapacity      int
	OutboxByteLimit    int64
	RPCTimeout         RPCTimeoutSource
	OperationTimeout   time.Duration
	PeerIdleTimeout    time.Duration
	OutboxPollInterval time.Duration
	Clock              CalibratedUTCClock
	After              func(time.Duration) <-chan time.Time
}

type MessagingDependencies struct {
	Identity      SessionIdentity
	Topology      AuthoritativeGroupSource
	PeerAuthority PeerAuthoritySource
	// PeerResolver performs one authenticated server handshake for a bare peer.
	// It is mutually exclusive with the legacy complete-membership sources.
	PeerResolver PeerResolver
	Carrier      EnvelopeCarrier
	Payloads     PayloadPipelineFactory
	Deliveries   DeliverySink
	Policies     *PolicyController
	Handlers     *HandlerRegistry
	Outbox       *outbox.Outbox
}
