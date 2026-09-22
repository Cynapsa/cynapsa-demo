package rank2xmpp

import (
	"context"
	"errors"
	"io"
	"net"
)

// establishmentPhase identifies the operation that was actually in progress.
// Mellium performs TLS, SASL, and resource binding while constructing its
// session, so the nominal Session method being called is not sufficient to
// classify a failure.
type establishmentPhase uint8

const (
	establishmentNetworkTLS establishmentPhase = iota + 1
	establishmentAuthentication
	establishmentIdentityBinding
	establishmentStreamManagement
)

type establishmentError struct {
	phase establishmentPhase
	cause error
}

func (e *establishmentError) Error() string { return "durable transport: establishment failed" }
func (e *establishmentError) Unwrap() error { return e.cause }

func phaseError(phase establishmentPhase, err error) error {
	if err == nil {
		return nil
	}
	var existing *establishmentError
	if errors.As(err, &existing) {
		return err
	}
	return &establishmentError{phase: phase, cause: err}
}

// phaseForDependencyError preserves connectivity errors as connectivity
// failures even when the socket disappears during SASL or binding. This uses
// error identity and interfaces only; dependency error text is never parsed.
func phaseForDependencyError(phase establishmentPhase, err error) establishmentPhase {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return establishmentNetworkTLS
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return establishmentNetworkTLS
	}
	return phase
}

func normalizeEstablishment(err error, ctx context.Context, fallback establishmentPhase) error {
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
	phase := fallback
	var phased *establishmentError
	if errors.As(err, &phased) {
		phase = phased.phase
	}
	switch phase {
	case establishmentNetworkTLS:
		return ErrUnavailable
	case establishmentAuthentication:
		return ErrAuthentication
	case establishmentIdentityBinding:
		return ErrIdentityBinding
	case establishmentStreamManagement:
		return ErrStreamManagement
	default:
		return ErrUnavailable
	}
}
