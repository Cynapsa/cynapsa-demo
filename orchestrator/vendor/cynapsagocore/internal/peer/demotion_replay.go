package peer

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// ReplayAfterUncertainSend resends the exact envelope through durable delivery.
func (w *Worker) ReplayAfterUncertainSend(ctx context.Context, envelope protocol.Envelope) error {
	if w == nil || ctx == nil || protocol.ValidateEnvelope(envelope) != nil {
		return ErrInvalidConfig
	}
	w.demote()
	return w.replay(ctx, envelope.Clone())
}
