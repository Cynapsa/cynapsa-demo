package handshake

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const attemptEntropyBytes = 16

type AttemptState uint8

const (
	AttemptPending AttemptState = iota + 1
	AttemptRunning
	AttemptSucceeded
	AttemptFailed
	AttemptCancelled
)

// Attempt contains one private establishment identity and immutable binding.
type Attempt struct {
	ID          string
	PeerID      string
	InitiatorID string
	StartedAt   time.Time
	Deadline    time.Time
	State       AttemptState
}

func NewAttempt(peerID, initiatorID string, startedAt time.Time, timeout time.Duration, random io.Reader) (Attempt, error) {
	if protocol.ValidateAgentIdentity(peerID) != nil || protocol.ValidateAgentIdentity(initiatorID) != nil || startedAt.IsZero() || timeout <= 0 {
		return Attempt{}, ErrInvalidConfig
	}
	if random == nil {
		random = rand.Reader
	}
	raw := make([]byte, attemptEntropyBytes)
	if _, err := io.ReadFull(random, raw); err != nil {
		clear(raw)
		return Attempt{}, ErrFailed
	}
	id := "hsk_" + base64.RawURLEncoding.EncodeToString(raw)
	clear(raw)
	startedAt = startedAt.UTC()
	return Attempt{ID: id, PeerID: peerID, InitiatorID: initiatorID, StartedAt: startedAt, Deadline: startedAt.Add(timeout), State: AttemptPending}, nil
}

func validAttempt(attempt Attempt) bool {
	if protocol.ValidateAgentIdentity(attempt.PeerID) != nil || protocol.ValidateAgentIdentity(attempt.InitiatorID) != nil || attempt.StartedAt.IsZero() || !attempt.Deadline.After(attempt.StartedAt) {
		return false
	}
	if len(attempt.ID) != len("hsk_")+22 || attempt.ID[:4] != "hsk_" {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(attempt.ID[4:])
	valid := err == nil && len(raw) == attemptEntropyBytes && base64.RawURLEncoding.EncodeToString(raw) == attempt.ID[4:]
	clear(raw)
	return valid
}
