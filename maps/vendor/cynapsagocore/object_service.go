package cynapsagocore

import (
	"context"
	"strings"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	objecthttp "github.com/Cynapsa/cynapsagocore/internal/xep0363"
)

// builtInObjectFactory composes the strict private HTTP object client after the
// authenticated durable session is live. V1 intentionally permits plaintext
// payload bytes inside HTTPS; the authenticated recipient-readiness exchange
// still completes before PUT and all downloaded bytes remain digest-bound.
type builtInObjectFactory struct{}

func newBuiltInObjectFactory() sessionkernel.ObjectServiceFactory { return builtInObjectFactory{} }

func (builtInObjectFactory) Create(ctx context.Context, build sessionkernel.ObjectBuildContext) (sessionkernel.Service, *sessionkernel.ProviderError) {
	if ctx == nil || build.Connectivity == nil || build.Profile.ReconnectOperationTimeout <= 0 {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return nil, failure
	}
	connectivity, ok := build.Connectivity.(messagingConnectivity)
	if !ok {
		return nil, providerError(sessionkernel.ProviderUnavailable)
	}
	capabilities, ok := connectivity.messagingCapabilities()
	if !ok || capabilities.client == nil {
		return nil, providerError(sessionkernel.ProviderUnavailable)
	}
	domain, ok := identityDomain(build.Identity.AgentID)
	if !ok {
		return nil, providerError(sessionkernel.ProviderAuthenticationRejected)
	}
	timeout := build.Profile.ReconnectOperationTimeout
	client, err := objecthttp.NewClient(objecthttp.Config{
		Policy: objecthttp.Policy{
			MaximumBytes: payload.MaximumTransferredBytes, MaximumURLBytes: 16 << 10,
			MaximumResponseHeaderBytes: 64 << 10, RequestTimeout: timeout,
			DialTimeout: min(timeout, 10*time.Second), TLSHandshakeTimeout: min(timeout, 10*time.Second),
			ResponseHeaderTimeout: min(timeout, 10*time.Second), IdleConnTimeout: timeout, MaximumRedirects: 2,
			AllowedHosts: map[string]bool{domain: true, "upload." + domain: true},
			AllowedPorts: map[uint16]bool{443: true},
		},
		Slots: capabilities.client,
	})
	if err != nil {
		return nil, providerError(sessionkernel.ProviderInternal)
	}
	return &builtInObjectService{client: client}, nil
}

func identityDomain(identity string) (string, bool) {
	index := strings.LastIndexByte(identity, '@')
	if index <= 0 || index == len(identity)-1 || strings.Contains(identity[index+1:], "/") {
		return "", false
	}
	return strings.ToLower(identity[index+1:]), true
}

type builtInObjectService struct{ client *objecthttp.Client }

func (*builtInObjectService) Name() string { return "private-objects" }

func (service *builtInObjectService) Start(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || service.client == nil || ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	if failure := connectivityContextError(ctx); failure != nil {
		return failure
	}
	return nil
}

func (service *builtInObjectService) Shutdown(ctx context.Context) *sessionkernel.ProviderError {
	if service == nil || service.client == nil || ctx == nil {
		return providerError(sessionkernel.ProviderInternal)
	}
	service.client.Close()
	return connectivityContextError(ctx)
}

func (service *builtInObjectService) ObjectRuntime() sessionkernel.ObjectRuntime {
	if service == nil {
		return nil
	}
	return service.client
}

var _ sessionkernel.ObjectRuntimeService = (*builtInObjectService)(nil)
