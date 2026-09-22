package rank1webrtc

import (
	"context"
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func healthEvidenceOutcome(err error) string {
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, transport.ErrUnavailable):
		return "unavailable"
	case errors.Is(err, transport.ErrClosed):
		return "closed"
	case errors.Is(err, transport.ErrProtocol):
		return "protocol"
	case errors.Is(err, transport.ErrSendAmbiguous):
		return "ambiguous"
	default:
		return "error"
	}
}

func healthStateEvidence(state transport.HealthState) string {
	switch state {
	case transport.HealthUnknown:
		return "unknown"
	case transport.HealthConnecting:
		return "connecting"
	case transport.HealthHealthy:
		return "healthy"
	case transport.HealthDisconnected:
		return "disconnected"
	case transport.HealthFailed:
		return "failed"
	case transport.HealthClosed:
		return "closed"
	default:
		return "invalid"
	}
}
