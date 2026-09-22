package policy

import "sync"

// Store atomically publishes one immutable compiled policy. Readers always
// observe a complete ruleset; a failed replacement leaves the prior ruleset
// untouched.
type Store struct {
	mu       sync.RWMutex
	compiled *Compiled
}

func NewStore(rules []Rule) (*Store, error) {
	compiled, err := Compile(rules)
	if err != nil {
		return nil, err
	}
	return &Store{compiled: compiled}, nil
}

func (store *Store) Replace(rules []Rule) error {
	if store == nil {
		return ErrInvalidRule
	}
	compiled, err := Compile(rules)
	if err != nil {
		return err
	}
	store.mu.Lock()
	store.compiled = compiled
	store.mu.Unlock()
	return nil
}

func (store *Store) Rules() []Rule {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	compiled := store.compiled
	store.mu.RUnlock()
	return compiled.Rules()
}

func (store *Store) Allows(agentID, applicationPath string) bool {
	if store == nil {
		return false
	}
	store.mu.RLock()
	compiled := store.compiled
	store.mu.RUnlock()
	return compiled.Allows(agentID, applicationPath)
}
