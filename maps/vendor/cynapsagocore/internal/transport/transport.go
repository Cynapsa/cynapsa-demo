// Package transport defines private delivery interfaces used by peer workers.
package transport

import (
	"context"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// Kind identifies a private carrier. It must never cross the SDK boundary.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindLive
	KindDurable
)

// HealthState is normalized dependency health used by private peer policy.
type HealthState uint8

const (
	HealthUnknown HealthState = iota
	HealthConnecting
	HealthHealthy
	HealthDisconnected
	HealthFailed
	HealthClosed
)

// Observation is immutable private health evidence. LastProgress is set only
// from trusted byte, heartbeat, or inbound-message progress.
type Observation struct {
	State        HealthState
	LastProgress time.Time
}

// Transport is the deliberately small cancellable carrier seam. Implementors
// must copy accepted envelopes, transfer an ownership-safe envelope from
// Receive, and return from every method when ctx is done.
type Transport interface {
	Kind() Kind
	Start(context.Context) error
	Send(context.Context, protocol.Envelope) error
	Receive(context.Context) (protocol.Envelope, error)
	Observe() Observation
	Close(context.Context) error
}

// CloneEnvelope makes carrier ownership explicit.
func CloneEnvelope(envelope protocol.Envelope) protocol.Envelope { return envelope.Clone() }
