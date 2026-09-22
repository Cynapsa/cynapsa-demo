package sessionkernel

import (
	"context"
	"sync"
)

type serviceGraph struct {
	services []Service
	identity AuthenticatedIdentity
	remote   AuthenticatedOperations
}

// shutdown uses one caller-owned total bound. A noncooperative provider may
// retain only its own goroutine; it cannot delay later Core cleanup.
func (graph *serviceGraph) shutdown(ctx context.Context) *ProviderError {
	if graph == nil {
		return nil
	}
	var first *ProviderError
	for index := len(graph.services) - 1; index >= 0; index-- {
		if ctx.Err() != nil {
			if first == nil {
				first = providerContextError(ctx)
			}
			break
		}
		if failure := stopService(ctx, graph.services[index]); failure != nil && first == nil {
			first = failure
		}
	}
	return first
}

func (graph *serviceGraph) start(ctx context.Context, service Service) *ProviderError {
	if graph == nil || service == nil {
		return &ProviderError{Code: ProviderInternal}
	}
	name := safeServiceName(service)
	if name == "" {
		cleanupUnpublished(ctx, service)
		return &ProviderError{Code: ProviderInternal}
	}
	for _, installed := range graph.services {
		if safeServiceName(installed) == name {
			cleanupUnpublished(ctx, service)
			return &ProviderError{Code: ProviderInternal}
		}
	}
	type startResult struct {
		failure  *ProviderError
		panicked bool
	}
	handoff := newResultHandoff[startResult]()
	go func() {
		failure, panicked := callServiceStart(ctx, service)
		if !handoff.publish(startResult{failure: failure, panicked: panicked}) {
			if panicked || failure == nil {
				lateCleanup(service)
			}
		}
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return providerContextError(ctx)
	}
	if ctx.Err() != nil {
		if output.panicked || output.failure == nil {
			cleanupCancelled(service)
		}
		return providerContextError(ctx)
	}
	if output.failure != nil {
		if output.panicked {
			cleanupUnpublished(ctx, service)
		}
		return normalizeProviderError(ctx, output.failure)
	}
	graph.services = append(graph.services, service)
	return nil
}

type resultHandoffState uint8

const (
	resultPending resultHandoffState = iota
	resultPublished
	resultClaimed
	resultAbandoned
)

// resultHandoff assigns exactly one owner to an asynchronously produced
// result. Cancellation atomically abandons a pending result to its producer;
// a result that was already published is claimed by the caller even when the
// cancellation and publication notifications become ready together.
type resultHandoff[T any] struct {
	mu    sync.Mutex
	ready chan struct{}
	state resultHandoffState
	value T
}

func newResultHandoff[T any]() *resultHandoff[T] {
	return &resultHandoff[T]{ready: make(chan struct{})}
}

// publish reports whether the caller now owns value. A false result transfers
// ownership back to the producer, which must dispose of any contained service.
func (handoff *resultHandoff[T]) publish(value T) bool {
	handoff.mu.Lock()
	defer handoff.mu.Unlock()
	if handoff.state == resultAbandoned {
		return false
	}
	if handoff.state != resultPending {
		return false
	}
	handoff.value = value
	handoff.state = resultPublished
	close(handoff.ready)
	return true
}

// await does not return on cancellation until the mutex assigns ownership:
// either this caller claims an already-published value or the producer owns a
// future late result through the abandoned state.
func (handoff *resultHandoff[T]) await(ctx context.Context) (T, bool) {
	select {
	case <-handoff.ready:
	case <-ctx.Done():
	}
	handoff.mu.Lock()
	defer handoff.mu.Unlock()
	if handoff.state == resultPublished {
		handoff.state = resultClaimed
		return handoff.value, true
	}
	if handoff.state == resultPending {
		handoff.state = resultAbandoned
	}
	var zero T
	return zero, false
}

func stopService(ctx context.Context, service Service) *ProviderError {
	if service == nil {
		return &ProviderError{Code: ProviderInternal}
	}
	result := make(chan *ProviderError, 1)
	go func() { result <- callServiceShutdown(ctx, service) }()
	select {
	case failure := <-result:
		return normalizeProviderError(ctx, failure)
	case <-ctx.Done():
		return providerContextError(ctx)
	}
}

func lateCleanup(service Service) {
	cleanup, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()
	_ = stopService(cleanup, service)
}

// cleanupCancelled transfers ownership to an independent bounded cleanup so
// cancellation does not make the caller wait for a noncooperative provider.
func cleanupCancelled(service Service) {
	go lateCleanup(service)
}

// cleanupUnpublished transfers no ownership: it invokes Shutdown exactly once
// and contains its normalized failure. Active authentication shares its total
// deadline; a service arriving after cancellation gets an independent bounded
// late-cleanup attempt without delaying the completed caller.
func cleanupUnpublished(ctx context.Context, service Service) {
	if ctx != nil && ctx.Err() == nil {
		_ = stopService(ctx, service)
		return
	}
	cleanupCancelled(service)
}

func safeServiceName(service Service) (name string) {
	defer func() {
		if recover() != nil {
			name = ""
		}
	}()
	return service.Name()
}

func callServiceStart(ctx context.Context, service Service) (failure *ProviderError, panicked bool) {
	defer func() {
		if recover() != nil {
			failure = &ProviderError{Code: ProviderInternal}
			panicked = true
		}
	}()
	return validateProviderError(service.Start(ctx)), false
}

func callServiceShutdown(ctx context.Context, service Service) (failure *ProviderError) {
	defer func() {
		if recover() != nil {
			failure = &ProviderError{Code: ProviderInternal}
		}
	}()
	return validateProviderError(service.Shutdown(ctx))
}

func validateProviderError(failure *ProviderError) *ProviderError {
	if failure == nil || failure.valid() {
		return failure
	}
	return &ProviderError{Code: ProviderInternal}
}

func normalizeProviderError(ctx context.Context, failure *ProviderError) *ProviderError {
	if ctx != nil && ctx.Err() != nil {
		return providerContextError(ctx)
	}
	return validateProviderError(failure)
}

func providerContextError(ctx context.Context) *ProviderError {
	if ctx != nil && ctx.Err() == context.DeadlineExceeded {
		return &ProviderError{Code: ProviderDeadline}
	}
	return &ProviderError{Code: ProviderCancelled}
}
