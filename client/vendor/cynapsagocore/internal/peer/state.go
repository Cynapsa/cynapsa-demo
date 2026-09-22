package peer

import "time"

type Health uint8

const (
	HealthHealthy Health = iota + 1
	HealthSuspicious
	HealthDemoted
)

// State contains a private, race-free peer snapshot.
type State struct {
	PeerID             string
	PreferredRank      Rank
	Health             Health
	RecoveryInFlight   bool
	QueuedMessageCount int
	DisconnectedSince  time.Time
	LastProgress       time.Time
}

func (w *Worker) Snapshot() State {
	if w == nil {
		return State{}
	}
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	return w.state
}
