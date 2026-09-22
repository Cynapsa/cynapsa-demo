package commandgate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// CommandState is one explicit command-registry state.
type CommandState uint8

const (
	StateAdmitted CommandState = iota + 1
	StateDispatched
	StateTerminal
)

type registryEntry struct {
	commandID   string
	state       CommandState
	ctx         context.Context
	cancel      context.CancelCauseFunc
	handle      CommandHandle
	completion  *completionToken
	disposition *terminalDisposition
	owner       *ownedCommand
}

func (r *Registry) attachDisposition(capability CompletionCapability, disposition *terminalDisposition) error {
	if capability.token == nil || disposition == nil {
		return ErrInvalidCompletionCapability
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[capability.token.commandKey]
	if !ok || entry.completion != capability.token || entry.state == StateTerminal {
		return ErrAlreadyTerminal
	}
	entry.disposition = disposition
	return nil
}

func (r *Registry) takeDisposition(commandID string) *terminalDisposition {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[r.commandKeyLocked(commandID)]
	if entry == nil {
		return nil
	}
	disposition := entry.disposition
	entry.disposition = nil
	return disposition
}

// CompletionCapability identifies one specific admission of a command ID. Its
// fields are private so only the command gate can mint a valid capability.
type CompletionCapability struct {
	token *completionToken
}

type completionToken struct {
	commandKey commandIDKey
	owner      *ownedCommand
	released   sync.Once
}

func (token *completionToken) releaseDispatch() {
	if token == nil {
		return
	}
	token.released.Do(func() {
		if token.owner != nil {
			token.owner.releaseDispatch()
		}
	})
}

type terminalRecord struct {
	commandKey commandIDKey
	handle     CommandHandle
}

// commandIDKey is a fixed-size, process-keyed digest. Registry metadata never
// retains an arbitrarily large command-ID backing after its charged owner is
// released.
type commandIDKey [sha256.Size]byte

// Registry tracks admitted command lifecycle inside the core.
//
// Terminal entries remain live until their completion is consumed. Consumption
// releases live capacity and moves a fixed-size keyed identifier digest into a
// bounded terminal history, allowing post-terminal operations to fail
// deterministically without retaining caller-sized ID backing.
type Registry struct {
	mu               sync.Mutex
	capacity         int
	entries          map[commandIDKey]*registryEntry
	activeHandles    map[CommandHandle]commandIDKey
	terminalSet      map[commandIDKey]struct{}
	terminalHandles  map[CommandHandle]struct{}
	terminalOrder    []terminalRecord
	handleScope      [commandHandleScopeSize]byte
	terminalKey      [sha256.Size]byte
	handleScopeReady bool
	nextGeneration   uint64
	changed          chan struct{}
}

// NewRegistry will construct a bounded command registry.
func NewRegistry(capacity int) *Registry {
	return &Registry{
		capacity:        capacity,
		entries:         make(map[commandIDKey]*registryEntry),
		activeHandles:   make(map[CommandHandle]commandIDKey),
		terminalSet:     make(map[commandIDKey]struct{}),
		terminalHandles: make(map[CommandHandle]struct{}),
		changed:         make(chan struct{}),
	}
}

// Register reserves one command identifier using a background-owned context.
// Gate uses register to attach entries to its lifetime context.
func (r *Registry) Register(commandID string) (CommandHandle, error) {
	return r.register(commandID, context.Background())
}

func (r *Registry) register(commandID string, parent context.Context) (CommandHandle, error) {
	if commandID == "" {
		return "", ErrEmptyCommandID
	}
	if parent == nil {
		return "", ErrNilContext
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capacity <= 0 {
		return "", ErrInvalidCapacity
	}
	if err := r.ensureHandleScopeLocked(); err != nil {
		return "", err
	}
	commandKey := r.commandKeyLocked(commandID)
	if _, ok := r.entries[commandKey]; ok {
		return "", ErrDuplicateCommandID
	}
	if _, ok := r.terminalSet[commandKey]; ok {
		return "", ErrAlreadyTerminal
	}
	if len(r.entries) >= r.capacity {
		return "", ErrRegistryFull
	}
	handle, err := r.newCommandHandleLocked()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancelCause(parent)
	r.entries[commandKey] = &registryEntry{
		commandID:  commandID,
		state:      StateAdmitted,
		ctx:        ctx,
		cancel:     cancel,
		handle:     handle,
		completion: &completionToken{commandKey: commandKey},
	}
	r.activeHandles[handle] = commandKey
	r.notifyLocked()
	return handle, nil
}

func (r *Registry) attachOwner(commandID string, handle CommandHandle, owner *ownedCommand) bool {
	if owner == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[r.commandKeyLocked(commandID)]
	if entry == nil || entry.handle != handle || entry.state != StateAdmitted || entry.owner != nil {
		return false
	}
	entry.owner = owner
	entry.completion.owner = owner
	return true
}

func (r *Registry) markDispatched(commandID string, owners ...*ownedCommand) (context.Context, CompletionCapability, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey := r.commandKeyLocked(commandID)
	entry, ok := r.entries[commandKey]
	if !ok {
		if _, terminal := r.terminalSet[commandKey]; terminal {
			return nil, CompletionCapability{}, ErrAlreadyTerminal
		}
		return nil, CompletionCapability{}, ErrCommandNotFound
	}
	if entry.owner != nil {
		if len(owners) != 1 || owners[0] != entry.owner {
			return nil, CompletionCapability{}, ErrInvalidRegistryState
		}
	}
	switch entry.state {
	case StateAdmitted:
		entry.state = StateDispatched
		r.notifyLocked()
		return entry.ctx, CompletionCapability{token: entry.completion}, nil
	case StateDispatched:
		return nil, CompletionCapability{}, ErrAlreadyDispatched
	case StateTerminal:
		return nil, CompletionCapability{}, ErrAlreadyTerminal
	default:
		return nil, CompletionCapability{}, ErrInvalidRegistryState
	}
}

func (r *Registry) complete(capability CompletionCapability, result model.Result) error {
	if capability.token == nil {
		return ErrInvalidCompletionCapability
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey := r.commandKeyLocked(result.CommandID)
	if capability.token.commandKey != commandKey {
		return ErrInvalidCompletionCapability
	}
	entry, ok := r.entries[commandKey]
	if !ok {
		if _, terminal := r.terminalSet[commandKey]; terminal {
			return ErrAlreadyTerminal
		}
		return ErrCommandNotFound
	}
	if entry.completion != capability.token {
		return ErrStaleCompletionCapability
	}
	if entry.state == StateTerminal {
		return ErrAlreadyTerminal
	}
	if entry.state != StateAdmitted && entry.state != StateDispatched {
		return ErrInvalidRegistryState
	}
	entry.state = StateTerminal
	entry.commandID = ""
	entry.cancel(ErrCommandCompleted)
	if entry.owner != nil {
		entry.owner.terminalize()
	}
	r.notifyLocked()
	return nil
}

func (r *Registry) cancelCommand(handle CommandHandle, cause error) (model.Result, error) {
	if handle == "" {
		return model.Result{}, ErrEmptyCommandHandle
	}
	if cause == nil {
		cause = ErrCommandCancelled
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey, ok := r.activeHandles[handle]
	if !ok {
		if _, terminal := r.terminalHandles[handle]; terminal {
			return model.Result{}, ErrAlreadyTerminal
		}
		return model.Result{}, ErrCommandNotFound
	}
	entry, ok := r.entries[commandKey]
	if !ok {
		return model.Result{}, ErrInvalidRegistryState
	}
	if entry.handle != handle {
		return model.Result{}, ErrInvalidRegistryState
	}
	if entry.state == StateTerminal {
		return model.Result{}, ErrAlreadyTerminal
	}
	if entry.state != StateAdmitted && entry.state != StateDispatched {
		return model.Result{}, ErrInvalidRegistryState
	}
	commandID := entry.commandID
	entry.state = StateTerminal
	entry.commandID = ""
	entry.cancel(cause)
	if entry.owner != nil {
		entry.owner.terminalize()
	}
	r.notifyLocked()
	return failureResult(commandID, failureCode(cause), cause), nil
}

func (r *Registry) terminalizeAll(cause error) []model.Result {
	if cause == nil {
		cause = ErrGateClosing
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	results := make([]model.Result, 0, len(r.entries))
	for _, entry := range r.entries {
		if entry.state == StateTerminal {
			continue
		}
		commandID := entry.commandID
		entry.state = StateTerminal
		entry.commandID = ""
		entry.cancel(cause)
		if entry.owner != nil {
			entry.owner.terminalize()
		}
		results = append(results, failureResult(commandID, failureCode(cause), cause))
	}
	if len(results) > 0 {
		r.notifyLocked()
	}
	return results
}

func (r *Registry) commandContext(commandID string) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey := r.commandKeyLocked(commandID)
	entry, ok := r.entries[commandKey]
	if !ok {
		if _, terminal := r.terminalSet[commandKey]; terminal {
			return nil, ErrAlreadyTerminal
		}
		return nil, ErrCommandNotFound
	}
	return entry.ctx, nil
}

// Remove deletes terminal command state and releases its live capacity. A
// nonterminal command is intentionally retained.
func (r *Registry) Remove(commandID string) {
	r.consumeTerminal(commandID)
}

func (r *Registry) consumeTerminal(commandID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey := r.commandKeyLocked(commandID)
	entry, ok := r.entries[commandKey]
	if !ok || entry.state != StateTerminal {
		return false
	}
	delete(r.entries, commandKey)
	delete(r.activeHandles, entry.handle)
	r.addTerminalLocked(commandKey, entry.handle)
	r.notifyLocked()
	return true
}

func (r *Registry) removeAdmitted(commandID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	commandKey := r.commandKeyLocked(commandID)
	entry, ok := r.entries[commandKey]
	if !ok || entry.state != StateAdmitted {
		return
	}
	entry.cancel(ErrAdmissionRolledBack)
	entry.commandID = ""
	if entry.owner != nil {
		entry.owner.terminalize()
	}
	delete(r.entries, commandKey)
	delete(r.activeHandles, entry.handle)
	r.notifyLocked()
}

func (r *Registry) addTerminalLocked(commandKey commandIDKey, handle CommandHandle) {
	if r.capacity <= 0 {
		return
	}
	if _, exists := r.terminalSet[commandKey]; exists {
		return
	}
	if len(r.terminalOrder) == r.capacity {
		oldest := r.terminalOrder[0]
		copy(r.terminalOrder, r.terminalOrder[1:])
		r.terminalOrder = r.terminalOrder[:len(r.terminalOrder)-1]
		delete(r.terminalSet, oldest.commandKey)
		delete(r.terminalHandles, oldest.handle)
	}
	r.terminalOrder = append(r.terminalOrder, terminalRecord{commandKey: commandKey, handle: handle})
	r.terminalSet[commandKey] = struct{}{}
	r.terminalHandles[handle] = struct{}{}
}

func (r *Registry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.entries {
		entry.cancel(ErrGateClosed)
		entry.commandID = ""
		if entry.owner != nil {
			entry.owner.terminalize()
		}
	}
	r.entries = make(map[commandIDKey]*registryEntry)
	r.activeHandles = make(map[CommandHandle]commandIDKey)
	r.terminalSet = make(map[commandIDKey]struct{})
	r.terminalHandles = make(map[CommandHandle]struct{})
	r.terminalOrder = nil
	r.notifyLocked()
}

func (r *Registry) waitEmpty(ctx context.Context, closed <-chan struct{}) error {
	for {
		r.mu.Lock()
		if len(r.entries) == 0 {
			r.mu.Unlock()
			return nil
		}
		changed := r.changed
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return nil
		case <-changed:
		}
	}
}

// State returns the live state for commandID. Consumed terminal tombstones are
// reported as StateTerminal.
func (r *Registry) State(commandID string) (CommandState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.handleScopeReady {
		return 0, false
	}
	commandKey := r.commandKeyLocked(commandID)
	if entry, ok := r.entries[commandKey]; ok {
		return entry.state, true
	}
	_, ok := r.terminalSet[commandKey]
	return StateTerminal, ok
}

// Len returns the number of live registry entries.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Capacity returns the configured live-entry bound.
func (r *Registry) Capacity() int { return r.capacity }

func (r *Registry) commandKeyLocked(commandID string) commandIDKey {
	mac := hmac.New(sha256.New, r.terminalKey[:])
	// hash.Hash does not implement io.StringWriter; io.WriteString would copy
	// the complete ID into a temporary []byte. Stream through one fixed scratch
	// buffer so temporary ownership is independent of caller-controlled length.
	var scratch [4096]byte
	defer clear(scratch[:])
	for offset := 0; offset < len(commandID); {
		count := copy(scratch[:], commandID[offset:])
		_, _ = mac.Write(scratch[:count])
		clear(scratch[:count])
		offset += count
	}
	var key commandIDKey
	copy(key[:], mac.Sum(nil))
	return key
}

func (r *Registry) notifyLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}
