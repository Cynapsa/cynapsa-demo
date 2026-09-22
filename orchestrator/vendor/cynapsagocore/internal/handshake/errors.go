package handshake

import "errors"

var (
	ErrInvalidConfig = errors.New("handshake: invalid configuration")
	ErrInvalidPeer   = errors.New("handshake: invalid peer")
	ErrCapacity      = errors.New("handshake: capacity exhausted")
	ErrInFlight      = errors.New("handshake: attempt already in flight")
	ErrCooldown      = errors.New("handshake: cooldown active")
	ErrStale         = errors.New("handshake: stale attempt")
	ErrClosed        = errors.New("handshake: manager closed")
	ErrFailed        = errors.New("handshake: establishment failed")
)
