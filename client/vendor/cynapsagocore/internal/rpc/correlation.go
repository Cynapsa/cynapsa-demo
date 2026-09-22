package rpc

import "github.com/Cynapsa/cynapsagocore/internal/protocol"

// NewCorrelationID will create a private unpredictable request correlation value.
func NewCorrelationID() (string, error) {
	return protocol.NewCorrelationID()
}
