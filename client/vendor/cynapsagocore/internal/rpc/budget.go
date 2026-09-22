package rpc

import (
	"math"
	"strings"
	"sync"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	maxRetainedRPCBytes uint64 = 256 << 20
	// retainedBytesPerRPC is the closed maximum materialized response plus its
	// validated wire envelope. Tests prove a legal maximum response and issued
	// identity fit in one unit.
	retainedBytesPerRPC uint64 = v1.MaximumPayloadBytes + protocol.MaxEnvelopeBytes
)

// ByteBudget is the shared, service-owned admission budget for dynamically
// retained RPC request, result, and identity bytes. It is intentionally not
// part of the public SDK configuration surface.
type ByteBudget struct {
	mu      sync.Mutex
	maximum uint64
	used    uint64
}

// Stats is a bounded, identity-free snapshot of shared RPC ownership.
type Stats struct {
	OwnedBytes   uint64
	ByteCapacity uint64
}

// NewByteBudget derives the hard service ceiling from the configured queue
// capacity. The checked product is capped at 256 MiB.
func NewByteBudget(capacity int) (*ByteBudget, error) {
	if capacity < 1 || capacity > MaxTableCapacity {
		return nil, ErrInvalidConfig
	}
	perEnvelope := retainedBytesPerRPC
	count := uint64(capacity)
	maximum := maxRetainedRPCBytes
	if count <= math.MaxUint64/perEnvelope {
		if product := count * perEnvelope; product < maximum {
			maximum = product
		}
	}
	return &ByteBudget{maximum: maximum}, nil
}

func newByteBudget(maximum uint64) (*ByteBudget, error) {
	if maximum == 0 || maximum > maxRetainedRPCBytes {
		return nil, ErrInvalidConfig
	}
	return &ByteBudget{maximum: maximum}, nil
}

func (b *ByteBudget) replace(oldSize, newSize uint64) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if oldSize > b.used {
		panic("rpc: byte budget ownership underflow")
	}
	remaining := b.maximum - (b.used - oldSize)
	if newSize > remaining {
		return false
	}
	b.used = b.used - oldSize + newSize
	return true
}

func (b *ByteBudget) reserve(size uint64) bool { return b.replace(0, size) }

func (b *ByteBudget) release(size uint64) {
	if !b.replace(size, 0) {
		panic("rpc: unreachable byte budget release")
	}
}

// ReserveOwned admits a composed RPC owner outside the correlation tables.
func (b *ByteBudget) ReserveOwned(size uint64) bool { return b.reserve(size) }

// ReleaseOwned releases a composed RPC owner after its graph is cleared or
// moved to an unretained caller.
func (b *ByteBudget) ReleaseOwned(size uint64) { b.release(size) }

// ReplaceOwned atomically transitions one composed owner between exact sizes.
func (b *ByteBudget) ReplaceOwned(oldSize, newSize uint64) bool { return b.replace(oldSize, newSize) }

func (b *ByteBudget) ownedBytes() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (b *ByteBudget) stats() Stats {
	if b == nil {
		return Stats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{OwnedBytes: b.used, ByteCapacity: b.maximum}
}

func envelopeDynamicBytes(envelope protocol.Envelope) uint64 {
	return uint64(len(envelope.MessageID) + len(envelope.ConversationID) +
		len(envelope.CorrelationID) + len(envelope.ReplyTo) + len(envelope.Sender) +
		len(envelope.Recipient) + len(envelope.MeshID) + len(envelope.Payload.Profile) +
		len(envelope.Payload.Reference) + len(envelope.Payload.EncryptionRef) +
		len(envelope.Payload.Inline) + len(envelope.CredentialProof))
}

// cloneEnvelopeExact freezes every caller-backed string and byte slice. Byte
// slice capacity equals length so the non-allocating pre-admission measurement
// is also the exact retained charge.
func cloneEnvelopeExact(envelope protocol.Envelope) protocol.Envelope {
	envelope.MessageID = strings.Clone(envelope.MessageID)
	envelope.ConversationID = strings.Clone(envelope.ConversationID)
	envelope.Sender = strings.Clone(envelope.Sender)
	envelope.Recipient = strings.Clone(envelope.Recipient)
	envelope.MeshID = strings.Clone(envelope.MeshID)
	envelope.CorrelationID = strings.Clone(envelope.CorrelationID)
	envelope.ReplyTo = strings.Clone(envelope.ReplyTo)
	envelope.Payload.Profile = strings.Clone(envelope.Payload.Profile)
	envelope.Payload.Reference = strings.Clone(envelope.Payload.Reference)
	envelope.Payload.EncryptionRef = strings.Clone(envelope.Payload.EncryptionRef)
	envelope.Payload.Inline = cloneBytesExact(envelope.Payload.Inline)
	envelope.CredentialProof = cloneBytesExact(envelope.CredentialProof)
	return envelope
}

func cloneBytesExact(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
