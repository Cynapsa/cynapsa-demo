package cynapsagocore

import (
	"context"
	"errors"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type durableDialerBuilder func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error)

// builtInConnectivityFactory is the process-private production construction
// seam. It owns no mutable global state and performs no network I/O until the
// returned service is started by the authenticated session graph.
type builtInConnectivityFactory struct {
	buildDialer durableDialerBuilder
}

func newBuiltInConnectivityFactory(builder durableDialerBuilder) *builtInConnectivityFactory {
	if builder == nil {
		builder = buildMelliumDialer
	}
	return &builtInConnectivityFactory{buildDialer: builder}
}

func buildMelliumDialer(meshEndpoint string, profile sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
	endpoint, err := rank2xmpp.ParseEndpoint(meshEndpoint)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := profile.TLSConfig(endpoint.TLSServerName())
	if err != nil {
		return nil, err
	}
	var credentialNow func() time.Time
	if !profile.CredentialUsableUntil.IsZero() {
		credentialNow = time.Now
	}
	return rank2xmpp.NewMelliumDialer(meshEndpoint, rank2xmpp.MelliumConfig{
		TLSConfig:                    tlsConfig,
		SASLMechanisms:               profile.SASLMechanisms(),
		ReceiveCapacity:              profile.QueueCapacity,
		StreamManagementCapacity:     profile.QueueCapacity,
		StreamManagementByteCapacity: boundedPrivateBytes(profile.QueueCapacity, profile.StanzaBudgetBytes, rank2xmpp.MaximumStreamManagementBytes),
		MaximumFrameBytes:            transport.MaximumControlFrameBytes,
		StanzaBudgetBytes:            profile.StanzaBudgetBytes,
		CredentialUsableUntil:        profile.CredentialUsableUntil,
		CredentialNow:                credentialNow,
		MeshID:                       profile.AuthMeshID,
	})
}

func (factory *builtInConnectivityFactory) Create(ctx context.Context, build sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
	if factory == nil || factory.buildDialer == nil || ctx == nil || build.Clock == nil || build.Random == nil {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return nil, failure
	}
	if _, err := rank2xmpp.ParseEndpoint(build.Auth.MeshEndpoint); err != nil {
		return nil, providerError(sessionkernel.ProviderAuthenticationRejected)
	}
	profile := build.Profile
	profile.TokenAuthentication = build.Auth.TokenAuthentication
	profile.AuthMeshID = build.Auth.MeshID
	if build.Auth.SessionExpiryMode == "disconnect" {
		profile.CredentialUsableUntil = build.Auth.CredentialUsableUntil
	}
	dialer, err := factory.buildDialer(build.Auth.MeshEndpoint, profile)
	if err != nil || dialer == nil {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return nil, failure
	}
	queueCapacity := build.Profile.QueueCapacity
	workerCount := min(build.Profile.TransferWorkers, queueCapacity)
	if queueCapacity <= 0 || workerCount <= 0 || build.Profile.StanzaBudgetBytes <= 0 {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	byteCapacity := int64(queueCapacity) * int64(build.Profile.StanzaBudgetBytes)
	if byteCapacity > outbox.MaxByteCapacity {
		byteCapacity = outbox.MaxByteCapacity
	}
	transferBytes := boundedPrivateBytes(queueCapacity, build.Profile.StanzaBudgetBytes, rank2xmpp.MaximumTransferWorkBytes)
	unresolvedBytes := boundedPrivateBytes(queueCapacity, build.Profile.StanzaBudgetBytes, rank2xmpp.MaximumUnresolvedTransferBytes)
	unresolvedCount := min(queueCapacity, rank2xmpp.MaximumUnresolvedTransferEntries)
	if transferBytes < int64(build.Profile.StanzaBudgetBytes) || unresolvedBytes < int64(build.Profile.StanzaBudgetBytes) || unresolvedCount <= 0 {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	pending, err := outbox.New(outbox.Config{MessageCapacity: queueCapacity, ByteCapacity: byteCapacity, Now: build.Clock.Now})
	if err != nil {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	password := append([]byte(nil), build.Auth.Password...)
	defer clearSecret(password)
	random := build.Random
	var credentialNow func() time.Time
	if !build.Auth.CredentialUsableUntil.IsZero() {
		credentialNow = build.Clock.Now
	}
	calibratedClock := transport.NewCalibratedClock(nil)
	var credentialSource rank2xmpp.CredentialSource
	if build.Auth.CredentialSource != nil {
		credentialSource = func(ctx context.Context) (rank2xmpp.Credential, error) {
			snapshot, failure := build.Auth.CredentialSource.Snapshot(ctx)
			if failure != nil {
				clearSecret(snapshot.Password)
				// A local credential-source failure, including expiry while a
				// renewal is in progress, is not a server login denial.
				// Retain the sender outbox for a later credential retry.
				return rank2xmpp.Credential{}, rank2xmpp.ErrUnavailable
			}
			return rank2xmpp.Credential{Password: snapshot.Password, UsableUntil: snapshot.UsableUntil}, nil
		}
	}
	expiryMode := rank2xmpp.SessionExpiryContinue
	if build.Auth.SessionExpiryMode == "disconnect" {
		expiryMode = rank2xmpp.SessionExpiryDisconnect
	}
	client, err := rank2xmpp.NewClient(rank2xmpp.Config{
		Endpoint:                       build.Auth.MeshEndpoint,
		DynamicPeerAuthority:           true,
		Auth:                           rank2xmpp.Authentication{Username: build.Auth.Username, Password: password, MeshID: build.Auth.MeshID, SessionResource: build.Auth.SessionResource},
		ReceiveCapacity:                queueCapacity,
		TransferWorkers:                workerCount,
		TransferQueue:                  queueCapacity,
		TransferByteCapacity:           transferBytes,
		UnresolvedTransferCapacity:     unresolvedCount,
		UnresolvedTransferByteCapacity: unresolvedBytes,
		UnresolvedTransferLifetime:     build.Profile.ReconnectOperationTimeout,
		MailboxLimit:                   queueCapacity,
		ReconnectAttempts:              build.Profile.ReconnectAttempts,
		ReconnectInitial:               build.Profile.ReconnectInitial,
		ReconnectMaximum:               build.Profile.ReconnectMaximum,
		ReconnectOperationTimeout:      build.Profile.ReconnectOperationTimeout,
		CredentialUsableUntil:          build.Auth.CredentialUsableUntil,
		CredentialNow:                  credentialNow,
		CredentialSource:               credentialSource,
		SessionExpiryMode:              expiryMode,
		Clock:                          calibratedClock,
		Jitter: func(ceilingDuration time.Duration) time.Duration {
			jitter, jitterErr := sessionkernel.CryptoJitter(random, ceilingDuration)
			if jitterErr != nil {
				return ceilingDuration
			}
			return jitter
		},
	}, dialer, pending, nil, nil)
	if err != nil {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	service := &builtInConnectivity{
		client: client, cleanupTimeout: build.Profile.ReconnectOperationTimeout,
		clock: meshCalibratedClockAdapter{source: calibratedClock},
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return service, failure
	}
	return service, nil
}

func boundedPrivateBytes(count, perItem int, maximum int64) int64 {
	if count <= 0 || perItem <= 0 || maximum <= 0 || int64(count) > (1<<63-1)/int64(perItem) {
		return 0
	}
	value := int64(count) * int64(perItem)
	if value > maximum {
		return maximum
	}
	return value
}

type builtInConnectivity struct {
	client         *rank2xmpp.Client
	cleanupTimeout time.Duration
	clock          meshCalibratedClockAdapter
}

type builtInMessagingCapabilities struct {
	client *rank2xmpp.Client
	clock  mesh.CalibratedUTCClock
}

func (service *builtInConnectivity) messagingCapabilities() (builtInMessagingCapabilities, bool) {
	if service == nil || service.client == nil || service.clock.source == nil {
		return builtInMessagingCapabilities{}, false
	}
	return builtInMessagingCapabilities{client: service.client, clock: service.clock}, true
}

// meshCalibratedClockAdapter is the sole typed conversion between Pod 4's
// calibrated clock evidence and Pod 3's messaging-clock contract. It holds the
// same underlying clock used by the durable client; it never duplicates state
// or performs a second calibration.
type meshCalibratedClockAdapter struct {
	source *transport.CalibratedClock
}

func (adapter meshCalibratedClockAdapter) Snapshot() mesh.CalibratedTime {
	if adapter.source == nil {
		return mesh.CalibratedTime{}
	}
	snapshot, ok := adapter.source.Snapshot()
	if !ok {
		return mesh.CalibratedTime{}
	}
	return mesh.CalibratedTime{UTC: snapshot.UTC, Uncertainty: snapshot.Uncertainty}
}

func (*builtInConnectivity) Name() string { return "durable-connectivity" }

func (service *builtInConnectivity) Start(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || service.client == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	err := service.client.Start(ctx)
	failure := classifyConnectivityError(ctx, err)
	if failure == nil {
		return nil
	}
	// The session graph does not acquire a service whose Start failed. Retire
	// the unpublished client here so its credential copy is cleared promptly.
	timeout := service.cleanupTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cleanup, cancel := context.WithTimeout(context.Background(), timeout)
	_ = service.client.Close(cleanup)
	cancel()
	return failure
}

func (service *builtInConnectivity) Shutdown(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || service.client == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	return classifyConnectivityError(ctx, service.client.Close(ctx))
}

func (service *builtInConnectivity) AuthenticatedIdentity() (sessionkernel.AuthenticatedIdentity, *sessionkernel.ProviderError) {
	if service == nil || service.client == nil {
		return sessionkernel.AuthenticatedIdentity{}, providerError(sessionkernel.ProviderInternal)
	}
	identity, err := service.client.AuthenticatedIdentity()
	if err != nil {
		return sessionkernel.AuthenticatedIdentity{}, classifyConnectivityError(nil, err)
	}
	return sessionkernel.AuthenticatedIdentity{AgentID: identity.BareIdentity, BoundIdentity: identity.BoundIdentity, MeshID: identity.MeshID}, nil
}

// Operations exposes only behavior owned concretely by the authenticated
// durable client. Messaging, peer diagnostics, and other application services
// remain nil until their complete typed dependency graphs are composed.
func (service *builtInConnectivity) Operations() sessionkernel.AuthenticatedOperations {
	if service == nil || service.client == nil {
		return sessionkernel.AuthenticatedOperations{}
	}
	return sessionkernel.AuthenticatedOperations{
		Logout:             service.logout,
		DiagnosticsNetwork: service.diagnosticsNetwork,
	}
}

func (service *builtInConnectivity) logout(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.EmptyResult, *sessionkernel.ProviderError) {
	if service == nil || service.client == nil || ctx == nil {
		return model.EmptyResult{}, providerError(sessionkernel.ProviderInternal)
	}
	if failure := classifyConnectivityError(ctx, service.client.Close(ctx)); failure != nil {
		return model.EmptyResult{}, failure
	}
	return model.EmptyResult{}, nil
}

func (service *builtInConnectivity) diagnosticsNetwork(ctx context.Context, _ coreruntime.Services, _ model.EmptyArgs) (model.ConnectivityStatus, *sessionkernel.ProviderError) {
	if service == nil || service.client == nil || ctx == nil {
		return model.ConnectivityStatus{}, providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return model.ConnectivityStatus{}, failure
	}
	return model.ConnectivityStatus{State: applicationConnectivityState(service.client.Observe().State)}, nil
}

func applicationConnectivityState(state transport.HealthState) string {
	switch state {
	case transport.HealthHealthy:
		return "available"
	case transport.HealthConnecting, transport.HealthDisconnected:
		return "degraded"
	case transport.HealthFailed, transport.HealthClosed:
		return "unavailable"
	default:
		return "unknown"
	}
}

func classifyConnectivityError(ctx context.Context, err error) *sessionkernel.ProviderError {
	if err == nil {
		return nil
	}
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return providerError(sessionkernel.ProviderCancelled)
		case context.DeadlineExceeded:
			return providerError(sessionkernel.ProviderDeadline)
		}
	}
	switch {
	case errors.Is(err, context.Canceled):
		return providerError(sessionkernel.ProviderCancelled)
	case errors.Is(err, context.DeadlineExceeded):
		return providerError(sessionkernel.ProviderDeadline)
	case errors.Is(err, rank2xmpp.ErrAuthentication), errors.Is(err, rank2xmpp.ErrIdentityBinding):
		return providerError(sessionkernel.ProviderAuthenticationRejected)
	case errors.Is(err, rank2xmpp.ErrQueueFull):
		return providerError(sessionkernel.ProviderCapacity)
	case errors.Is(err, rank2xmpp.ErrUnavailable), errors.Is(err, rank2xmpp.ErrStreamManagement), errors.Is(err, rank2xmpp.ErrClosed):
		return providerError(sessionkernel.ProviderUnavailable)
	default:
		return providerError(sessionkernel.ProviderInternal)
	}
}

func providerError(code sessionkernel.ProviderErrorCode) *sessionkernel.ProviderError {
	return &sessionkernel.ProviderError{Code: code}
}

func connectivityContextError(ctx context.Context) *sessionkernel.ProviderError {
	if ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	switch ctx.Err() {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return providerError(sessionkernel.ProviderDeadline)
	default:
		return providerError(sessionkernel.ProviderCancelled)
	}
}

func clearSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
