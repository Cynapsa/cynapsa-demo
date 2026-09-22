package payload

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	MaximumTransferChunks uint32 = 1 << 20
	// MaximumReassemblyBytes is the V1 hard ceiling for all ciphertext,
	// plaintext, and retained canonical buffers owned by one reassembler.
	MaximumReassemblyBytes int64 = 256 << 20
)

// Limits bounds every payload allocation, fragment set, queue, and lifetime.
// Callers may choose smaller policy values; the hard maxima are V1 safety caps.
type Limits struct {
	InlineBytes            int64
	MaximumPayloadBytes    int64
	ChunkBytes             int
	MaximumFrameBytes      int
	MaximumChunks          uint32
	InFlightChunks         int
	TransfersPerPeer       int
	MaximumTransfers       int
	ReassemblyBytesPerPeer int64
	ReassemblyBytes        int64
	TransferLifetime       time.Duration
	CleanupTimeout         time.Duration
	WorkerCount            int
	WorkerQueue            int
}

func (l Limits) Validate() error {
	if l.InlineBytes < 0 || l.InlineBytes > protocol.MaxInlinePayloadBytes || l.MaximumPayloadBytes <= 0 || l.MaximumPayloadBytes > MaximumCanonicalBytes || l.InlineBytes > l.MaximumPayloadBytes || l.ChunkBytes <= 0 || l.ChunkBytes > 1<<20 || l.MaximumFrameBytes < l.ChunkBytes || l.MaximumFrameBytes > 2<<20 || l.MaximumChunks == 0 || l.MaximumChunks > MaximumTransferChunks || l.InFlightChunks <= 0 || uint32(l.InFlightChunks) > l.MaximumChunks || l.TransfersPerPeer <= 0 || l.MaximumTransfers <= 0 || l.MaximumTransfers > 65536 || l.TransfersPerPeer > l.MaximumTransfers || l.ReassemblyBytesPerPeer <= 0 || l.ReassemblyBytes <= 0 || l.ReassemblyBytesPerPeer > l.ReassemblyBytes || l.ReassemblyBytes > MaximumReassemblyBytes || l.TransferLifetime <= 0 || l.CleanupTimeout <= 0 || l.CleanupTimeout > 30*time.Second || l.WorkerCount <= 0 || l.WorkerCount > 1024 || l.WorkerQueue <= 0 || l.WorkerQueue > 65536 {
		return ErrInvalidLimits
	}
	return nil
}

// CarrierKind identifies a private large-payload mechanism inside the core.
type CarrierKind uint8

const (
	CarrierDirectChunks CarrierKind = iota + 1
	CarrierObjectUpload
	CarrierMessageChunks
)

// CompletionEvidence is authenticated private evidence that the receiver
// materialized the logical payload. Upload or local write completion is not it.
type CompletionEvidence struct {
	TransferID string
	MessageID  string
	Digest     [32]byte
}

type FrameEncoding uint8

const (
	FrameBinary FrameEncoding = iota + 1
	FrameText
)

// CarrierFrame is an already versioned and bounded opaque payload-system
// frame. The carrier may use Data only for the duration of the call.
type CarrierFrame struct {
	TransferID string
	Index      uint32
	Encoding   FrameEncoding
	Data       []byte
}

// CarrierRoute is trusted routing context supplied out-of-band to a carrier.
// A carrier must never derive its peer or recipient from frame bytes.
type CarrierRoute struct {
	PeerID      string
	MeshID      string
	SenderID    string
	RecipientID string
	MessageID   string
}

// ChunkCarrier is the small carrier seam Pod 4 implements for direct binary
// and durable text-safe chunks. Route and frame arguments are borrowed only
// through each callback; implementations must clone them before retaining or
// using them asynchronously. Every method must return when ctx is done; Finish
// must wait for authenticated remote materialization evidence.
type ChunkCarrier interface {
	Available(context.Context, CarrierRoute) (bool, error)
	Begin(context.Context, CarrierRoute, CarrierFrame) error
	SendChunk(context.Context, CarrierRoute, CarrierFrame) error
	Finish(context.Context, CarrierRoute, string) (CompletionEvidence, error)
	Abort(context.Context, CarrierRoute, string) error
}

// ObjectStore performs private object I/O. V1 may transfer the canonical
// payload bytes directly and rely on HTTPS for transport confidentiality. A
// future end-to-end cipher can wrap the same bytes without changing this
// carrier contract. Every method must return when ctx is done.
type ObjectStore interface {
	Available(context.Context) (bool, error)
	Upload(context.Context, []byte) (string, error)
	Download(context.Context, string) ([]byte, error)
}

// PreparedObjectUpload is one bounded upload slot whose exact receiver-facing
// reference is known before any payload bytes are written. Commit is single-use;
// Abort is idempotent and retires local ownership of an uncommitted slot. Every
// non-nil handle returned by PrepareUpload transfers to its caller even when
// accompanied by an error and must be Abort'ed exactly once.
type PreparedObjectUpload interface {
	DownloadReference() string
	Commit(context.Context, []byte) error
	Abort()
}

// PreparedObjectStore is required for object offload. The split preparation
// step lets the authenticated peer prove that it can reach the exact download
// authority before the sender uploads any payload bytes.
type PreparedObjectStore interface {
	ObjectStore
	PrepareUpload(context.Context, int64) (PreparedObjectUpload, error)
}

// MaterializationAcknowledger bridges carrier-private object references to
// authenticated receiver completion/failure evidence.
type MaterializationAcknowledger interface {
	PublishObject(context.Context, CarrierRoute, TransferManifest, string) error
	AwaitMaterialization(context.Context, CarrierRoute, string) (CompletionEvidence, error)
	AbortMaterialization(context.Context, CarrierRoute, string) error
}

// ObjectReadinessAcknowledger performs the authenticated, short-lived
// receiver reachability exchange for the exact prepared download reference.
// A successful return proves network reachability only; final materialization
// evidence remains the sole successful-delivery linearization.
type ObjectReadinessAcknowledger interface {
	MaterializationAcknowledger
	ConfirmObjectReadiness(context.Context, CarrierRoute, TransferManifest, string) error
}

// TransferBinding is trusted authenticated context supplied by the mesh and
// conversation layers. Inbound manifest strings cannot establish this binding.
type TransferBinding struct {
	TransferID      string
	MessageID       string
	MeshID          string
	SenderID        string
	RecipientID     string
	Profile         string
	CanonicalSize   int64
	CanonicalDigest [32]byte
}

// TransferRequest contains one private canonical snapshot and stable identity.
type TransferRequest struct {
	PeerID           string
	MessageID        string
	MeshID           string
	SenderID         string
	RecipientID      string
	ConversationID   string
	Mode             protocol.Mode
	CorrelationID    string
	ReplyTo          string
	CreatedAt        time.Time
	ExpiresAt        time.Time
	ClockUncertainty time.Duration
	Profile          string
	Canonical        []byte
}

// EnvelopePublication is carrier-neutral logical context. The injected
// publisher owns envelope construction, proof, and network publication, and
// network publication. It must return only after the pending logical message
// is registered for the supplied descriptor.
type EnvelopePublication struct {
	Route            CarrierRoute
	ConversationID   string
	Mode             protocol.Mode
	CorrelationID    string
	ReplyTo          string
	CreatedAt        time.Time
	ExpiresAt        time.Time
	ClockUncertainty time.Duration
	Descriptor       protocol.PayloadDescriptor
}

type EnvelopePublisher interface {
	// PublishPayloadEnvelope borrows publication only through the callback.
	// Implementations must clone the descriptor and any other retained values
	// before returning or using them asynchronously.
	PublishPayloadEnvelope(context.Context, EnvelopePublication) error
}

// AuthorityCheckpoint is a process-private exact-peer fence injected by the
// mesh composition. Coordinator calls it immediately around every external
// carrier/dependency invocation so a membership fence that wins first prevents
// the call, independent of whether that dependency observes ctx promptly.
type AuthorityCheckpoint func(context.Context, func() error) error

type authorityCheckpointContextKey struct{}

func WithAuthorityCheckpoint(ctx context.Context, checkpoint AuthorityCheckpoint) context.Context {
	if ctx == nil || checkpoint == nil {
		return ctx
	}
	return context.WithValue(ctx, authorityCheckpointContextKey{}, checkpoint)
}

func authorityCheckpointFromContext(ctx context.Context) (AuthorityCheckpoint, bool) {
	if ctx == nil {
		return nil, false
	}
	checkpoint, ok := ctx.Value(authorityCheckpointContextKey{}).(AuthorityCheckpoint)
	return checkpoint, ok && checkpoint != nil
}

// TransferReceipt records private terminal evidence without becoming an SDK value.
type TransferReceipt struct {
	MessageID  string
	TransferID string
	Carrier    CarrierKind
	Descriptor protocol.PayloadDescriptor
}

// PayloadCipher is the optional private end-to-end encryption seam. V1 may
// leave it nil and rely on DTLS, HTTPS, or XMPP TLS for transport security.
// Returned ciphertext/plaintext is newly owned by the caller and must not
// alias an input. Implementations retain no payload-sized workspace after a
// call returns. Login passwords are never an input.
type PayloadCipher interface {
	Encrypt(context.Context, TransferBinding, []byte) (ciphertext []byte, encryptionRef string, err error)
	Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error)
}

// Coordinator owns ADR 0005 selection, fallback, and one logical outcome.
type Coordinator struct {
	limits         Limits
	direct         ChunkCarrier
	objects        ObjectStore
	objectEvidence MaterializationAcknowledger
	messages       ChunkCarrier
	cipher         PayloadCipher
	publisher      EnvelopePublisher
	now            func() time.Time
	activeLarge    atomic.Int64
}

// ActiveTransfers returns the number of large logical payload sends that are
// currently inside the bounded carrier ladder. Inline publication is excluded
// because it does not create private transfer state.
func (c *Coordinator) ActiveTransfers() uint64 {
	if c == nil {
		return 0
	}
	active := c.activeLarge.Load()
	if active <= 0 {
		return 0
	}
	return uint64(active)
}

// NewCoordinator validates dependencies without importing concrete transports.
func NewCoordinator(limits Limits, direct ChunkCarrier, objects ObjectStore, objectEvidence MaterializationAcknowledger, messages ChunkCarrier, cipher PayloadCipher, publisher EnvelopePublisher) (*Coordinator, error) {
	if err := limits.Validate(); err != nil || publisher == nil || (objects == nil) != (objectEvidence == nil) {
		return nil, ErrInvalidLimits
	}
	return &Coordinator{limits: limits, direct: direct, objects: objects, objectEvidence: objectEvidence, messages: messages, cipher: cipher, publisher: publisher, now: time.Now}, nil
}
