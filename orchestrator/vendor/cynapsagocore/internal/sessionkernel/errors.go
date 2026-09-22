// Package sessionkernel composes one authenticated private service graph.
package sessionkernel

import "errors"

var (
	ErrInvalidConfig         = errors.New("session kernel: invalid configuration")
	ErrClosed                = errors.New("session kernel: closed")
	ErrAuthenticationFailed  = errors.New("session kernel: authentication failed")
	ErrAuthorizationRejected = errors.New("session kernel: authorization rejected")
	ErrServiceUnavailable    = errors.New("session kernel: service unavailable")
	ErrInvalidHandle         = errors.New("session kernel: invalid handle")
	ErrPayloadTooLarge       = errors.New("session kernel: payload too large")
	ErrPayloadIntegrity      = errors.New("session kernel: payload integrity failure")
	ErrPayloadTransfer       = errors.New("session kernel: payload transfer failure")
	ErrComponentStopFailed   = errors.New("session kernel: component shutdown failed")
	ErrComponentPanic        = errors.New("session kernel: component panic contained")
	ErrShutdownTimeout       = errors.New("session kernel: shutdown deadline exceeded")
)
