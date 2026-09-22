package sessionkernel

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"math"
	"time"

	"mellium.im/sasl"
)

const (
	MaximumQueueCapacity = 65_536
	MaximumStanzaBytes   = 4 << 20
)

const (
	defaultReconnectAttempts = 8
	defaultReconnectInitial  = 250 * time.Millisecond
	defaultReconnectMaximum  = 30 * time.Second
	defaultOperationTimeout  = 30 * time.Second
	defaultTransferWorkers   = 4
)

// DeploymentConfig contains only process-local deployment bounds. Application
// authentication values arrive later through auth.login or auth.connect.
type DeploymentConfig struct {
	QueueLimit int
}

func (config DeploymentConfig) validate() error {
	if config.QueueLimit < 1 || config.QueueLimit > MaximumQueueCapacity {
		return ErrInvalidConfig
	}
	return nil
}

// OperationalProfile freezes the approved private production defaults. It is
// passed by value to dependency factories so they cannot mutate shared state.
type OperationalProfile struct {
	ReconnectAttempts         int
	ReconnectInitial          time.Duration
	ReconnectMaximum          time.Duration
	ReconnectOperationTimeout time.Duration
	StanzaBudgetBytes         int
	TransferWorkers           int
	QueueCapacity             int
	UseSystemTLSRoots         bool
	VerifyEndpointHostname    bool
	// TokenAuthentication is process-private and selected by the authenticated
	// command path. Ordinary password sessions retain the approved SCRAM profile.
	TokenAuthentication   bool
	CredentialUsableUntil time.Time
	AuthMeshID            string
}

// DefaultOperationalProfile derives every queue-based private capacity from
// the one deployment queue limit and its hard ceiling.
func DefaultOperationalProfile(config DeploymentConfig) (OperationalProfile, error) {
	if err := config.validate(); err != nil {
		return OperationalProfile{}, err
	}
	return OperationalProfile{
		ReconnectAttempts:         defaultReconnectAttempts,
		ReconnectInitial:          defaultReconnectInitial,
		ReconnectMaximum:          defaultReconnectMaximum,
		ReconnectOperationTimeout: defaultOperationTimeout,
		StanzaBudgetBytes:         MaximumStanzaBytes,
		TransferWorkers:           defaultTransferWorkers,
		QueueCapacity:             min(config.QueueLimit, MaximumQueueCapacity),
		UseSystemTLSRoots:         true,
		VerifyEndpointHostname:    true,
	}, nil
}

// TLSConfig returns a fresh configuration using the operating-system trust
// store and exact endpoint-hostname verification. No insecure fallback exists.
func (profile OperationalProfile) TLSConfig(endpointHostname string) (*tls.Config, error) {
	if !profile.UseSystemTLSRoots || !profile.VerifyEndpointHostname || endpointHostname == "" {
		return nil, ErrInvalidConfig
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		return nil, ErrInvalidConfig
	}
	return &tls.Config{
		RootCAs:            roots,
		ServerName:         endpointHostname,
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS13,
	}, nil
}

// SASLMechanisms separates token presentation from password challenge-response.
// Token presentation is permitted only inside the same verified TLS connection.
func (profile OperationalProfile) SASLMechanisms() []sasl.Mechanism {
	if profile.TokenAuthentication {
		return []sasl.Mechanism{sasl.Plain}
	}
	return []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256}
}

// OperationDeadline returns the earlier of the caller deadline and the
// approved 30-second per-operation bound.
func (profile OperationalProfile) OperationDeadline(ctx context.Context, now time.Time) (time.Time, error) {
	if ctx == nil || now.IsZero() || profile.ReconnectOperationTimeout <= 0 {
		return time.Time{}, ErrInvalidConfig
	}
	deadline := now.Add(profile.ReconnectOperationTimeout)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	return deadline, nil
}

// BackoffCeiling returns the bounded exponential ceiling for a one-based
// reconnect attempt.
func (profile OperationalProfile) BackoffCeiling(attempt int) (time.Duration, error) {
	if attempt < 1 || attempt > profile.ReconnectAttempts || profile.ReconnectInitial <= 0 || profile.ReconnectMaximum < profile.ReconnectInitial {
		return 0, ErrInvalidConfig
	}
	delay := profile.ReconnectInitial
	for index := 1; index < attempt && delay < profile.ReconnectMaximum; index++ {
		if delay >= profile.ReconnectMaximum/2 {
			delay = profile.ReconnectMaximum
			break
		}
		delay *= 2
	}
	return min(delay, profile.ReconnectMaximum), nil
}

// CryptoJitter chooses a full-jitter delay in [0, ceiling] using unbiased
// rejection sampling from the injected cryptographic entropy source.
func CryptoJitter(random io.Reader, ceiling time.Duration) (time.Duration, error) {
	if random == nil {
		random = rand.Reader
	}
	if ceiling < 0 {
		return 0, ErrInvalidConfig
	}
	bound := uint64(ceiling)
	if bound == math.MaxUint64 {
		return 0, ErrInvalidConfig
	}
	span := bound + 1
	limit := uint64(math.MaxUint64) - uint64(math.MaxUint64)%span
	var encoded [8]byte
	for {
		if _, err := io.ReadFull(random, encoded[:]); err != nil {
			return 0, ErrInvalidConfig
		}
		value := binary.BigEndian.Uint64(encoded[:])
		if value < limit {
			return time.Duration(value % span), nil
		}
	}
}
