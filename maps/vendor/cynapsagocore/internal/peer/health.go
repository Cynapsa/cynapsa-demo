package peer

import (
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const (
	SuspiciousThreshold = 15 * time.Second
	DemotionThreshold   = 30 * time.Second
)

// HealthPolicy freezes the approved V1 thresholds. The live-link adapter owns
// the fixed five-second probe cadence and 16-byte nonce framing.
type HealthPolicy struct {
	SuspiciousAfter time.Duration
	DemoteAfter     time.Duration
}

func DefaultHealthPolicy() HealthPolicy {
	return HealthPolicy{SuspiciousAfter: SuspiciousThreshold, DemoteAfter: DemotionThreshold}
}

func (p HealthPolicy) valid() bool {
	return p.SuspiciousAfter == SuspiciousThreshold && p.DemoteAfter == DemotionThreshold
}

func (w *Worker) observeHealth(now time.Time, observation transport.Observation) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	if !observation.LastProgress.IsZero() && observation.LastProgress.After(w.state.LastProgress) {
		w.state.LastProgress = observation.LastProgress
	}
	switch observation.State {
	case transport.HealthHealthy:
		w.state.DisconnectedSince = time.Time{}
		w.state.Health = HealthHealthy
	case transport.HealthFailed, transport.HealthClosed:
		w.state.Health = HealthDemoted
		w.state.PreferredRank = RankDurable
		w.state.DisconnectedSince = now
	case transport.HealthDisconnected:
		// Trusted progress while the dependency reports disconnected starts a
		// fresh continuous no-progress interval.
		if !observation.LastProgress.IsZero() && (w.state.DisconnectedSince.IsZero() || observation.LastProgress.After(w.state.DisconnectedSince)) {
			w.state.DisconnectedSince = observation.LastProgress
		}
		if w.state.DisconnectedSince.IsZero() {
			w.state.DisconnectedSince = now
		}
		elapsed := now.Sub(w.state.DisconnectedSince)
		if elapsed >= w.policy.DemoteAfter {
			w.state.Health = HealthDemoted
			w.state.PreferredRank = RankDurable
		} else if elapsed >= w.policy.SuspiciousAfter {
			w.state.Health = HealthSuspicious
		}
	}
}
