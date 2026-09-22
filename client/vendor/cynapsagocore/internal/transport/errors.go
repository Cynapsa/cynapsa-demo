package transport

import (
	"context"
	"errors"
)

var (
	ErrInvalidConfig   = errors.New("transport: invalid configuration")
	ErrInvalidEnvelope = errors.New("transport: invalid envelope")
	ErrNotStarted      = errors.New("transport: not started")
	ErrUnavailable     = errors.New("transport: path unavailable")
	ErrQueueFull       = errors.New("transport: queue full")
	ErrClosed          = errors.New("transport: closed")
	ErrSendRejected    = errors.New("transport: send rejected")
	ErrSendAmbiguous   = errors.New("transport: send completion ambiguous")
	ErrReceive         = errors.New("transport: receive failed")
	ErrAuthentication  = errors.New("transport: authentication failed")
	ErrProtocol        = errors.New("transport: protocol violation")
)

// classify strips dependency text while preserving cancellation class and
// already-normalized private transport errors.
func classify(err error, ctx context.Context) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	for _, known := range []error{ErrInvalidConfig, ErrInvalidEnvelope, ErrNotStarted, ErrUnavailable, ErrQueueFull, ErrClosed, ErrSendRejected, ErrSendAmbiguous, ErrReceive, ErrAuthentication, ErrProtocol} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrReceive
}
