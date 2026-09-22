package conversation

import "github.com/Cynapsa/cynapsagocore/internal/protocol"

// Delivery is one independently admitted, materialized envelope. The Core
// deliberately makes no relative-order promise between deliveries.
type Delivery struct {
	Envelope         protocol.Envelope
	CanonicalPayload []byte
}
