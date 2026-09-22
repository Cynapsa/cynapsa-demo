package rank1webrtc

import "github.com/Cynapsa/cynapsagocore/internal/transport"

func (l *Link) Observe() transport.Observation {
	if l == nil {
		return transport.Observation{State: transport.HealthClosed}
	}
	l.mu.Lock()
	channel, started, closed := l.channel, l.started, l.closed
	l.mu.Unlock()
	if closed {
		return transport.Observation{State: transport.HealthClosed}
	}
	if !started || channel == nil {
		return transport.Observation{State: transport.HealthConnecting}
	}
	observation := observeChannel(channel)
	progress := l.healthProgress()
	if progress.After(observation.LastProgress) {
		observation.LastProgress = progress
	}
	now := l.config.Clock.Now()
	if now.IsZero() {
		observation.State = transport.HealthDisconnected
	} else if observation.State == transport.HealthHealthy && observation.LastProgress.IsZero() {
		// A locally opened SCTP channel is not yet proof that the peer's read
		// path is usable. Rank1 becomes eligible only after one authenticated
		// inbound frame (normally the first health-probe reply).
		observation.State = transport.HealthConnecting
	} else if observation.State == transport.HealthHealthy && !observation.LastProgress.IsZero() && now.Sub(observation.LastProgress) > 2*HealthProbeInterval {
		// A probe is emitted at each HealthProbeInterval. Allow two complete
		// intervals so scheduler delay and reply RTT cannot create a false
		// disconnected window at the exact send boundary. Underlying channel
		// Failed and Closed states still take effect immediately above.
		observation.State = transport.HealthDisconnected
	}
	return observation
}

// RestartEligible reports whether this exact live link still owns a
// nonterminal restart-capable connection. Peer progress is deliberately not
// considered here: the recovery tier combines this capability with the exact
// link observation so a merely local DataChannel open can never justify an
// in-place ICE restart.
func (l *Link) RestartEligible() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	connection, channel, started, closed := l.connection, l.channel, l.started, l.closed
	l.mu.Unlock()
	if !started || closed || channel == nil {
		return false
	}
	if _, ok := connection.(RestartablePeerConnection); !ok {
		return false
	}
	switch observeChannel(channel).State {
	case transport.HealthConnecting, transport.HealthHealthy, transport.HealthDisconnected:
		return true
	case transport.HealthFailed:
		restartable, ok := channel.(interface{ restartable() bool })
		return ok && restartable.restartable()
	default:
		return false
	}
}
