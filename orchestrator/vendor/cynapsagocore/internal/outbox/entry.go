package outbox

import (
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// Entry contains private replay and retention metadata.
type Entry struct {
	Envelope protocol.Envelope
	QueuedAt time.Time
	Bytes    int64
	Ordinal  uint64
}

func (entry Entry) clone() Entry {
	entry.Envelope = entry.Envelope.Clone()
	return entry
}
