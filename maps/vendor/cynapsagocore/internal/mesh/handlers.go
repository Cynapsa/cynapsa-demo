package mesh

import (
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/policy"
)

// HandlerRegistry is a bounded SDK-local interest registry. It never changes
// membership or authorization and is not consulted as remote routing proof.
type HandlerRegistry struct {
	mu       sync.RWMutex
	capacity int
	paths    map[string]struct{}
}

func NewHandlerRegistry(capacity int) (*HandlerRegistry, error) {
	if capacity < 1 || capacity > MaxMembershipEntries {
		return nil, ErrInvalidConfig
	}
	return &HandlerRegistry{capacity: capacity, paths: make(map[string]struct{}, capacity)}, nil
}

// Register is idempotent for an existing exact path.
func (registry *HandlerRegistry) Register(path string) error {
	if registry == nil || policy.ValidateRule(policy.Rule{Action: policy.ActionAllow, Path: path}) != nil || path == "" {
		return ErrHandlerInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.paths[path]; exists {
		return nil
	}
	if len(registry.paths) >= registry.capacity {
		return ErrHandlerCapacity
	}
	registry.paths[path] = struct{}{}
	return nil
}

// Unregister is idempotent; disappearance of a local interest is not a remote
// authorization operation.
func (registry *HandlerRegistry) Unregister(path string) error {
	if registry == nil || policy.ValidateRule(policy.Rule{Action: policy.ActionAllow, Path: path}) != nil || path == "" {
		return ErrHandlerInvalid
	}
	registry.mu.Lock()
	delete(registry.paths, path)
	registry.mu.Unlock()
	return nil
}

func (registry *HandlerRegistry) Contains(path string) bool {
	if registry == nil {
		return false
	}
	registry.mu.RLock()
	_, ok := registry.paths[path]
	registry.mu.RUnlock()
	return ok
}

func (registry *HandlerRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.paths)
}
