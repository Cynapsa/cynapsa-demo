package main

import (
	"context"
	"sync"

	core "github.com/Cynapsa/cynapsagocore"
)

// abiCoreRecord coordinates the numeric handle lifetime with every active ABI
// call. It contains a Go pointer privately, but only its random numeric registry
// key crosses the native boundary.
type abiCoreRecord struct {
	mu               sync.Mutex
	core             *core.Core
	accepting        bool
	active           uint64
	quiet            chan struct{}
	quietOnce        sync.Once
	callbacks        *callbackManager
	callbacksAllowed bool
	callbacksClosing bool

	// callbackRegistrationCheckpoint is a package-private deterministic test
	// seam. Production records leave it nil.
	callbackRegistrationCheckpoint func()

	destroyOnce sync.Once
	destroyDone chan struct{}
	destroyErr  error
}

func newABICoreRecord(value *core.Core) *abiCoreRecord {
	return &abiCoreRecord{core: value, accepting: true, quiet: make(chan struct{}), callbacksAllowed: true, destroyDone: make(chan struct{})}
}

func (r *abiCoreRecord) acquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting || r.core == nil {
		return false
	}
	r.active++
	return true
}

func (r *abiCoreRecord) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == 0 {
		panic("abiCoreRecord.release without acquire")
	}
	r.active--
	if !r.accepting && r.active == 0 {
		r.quietOnce.Do(func() { close(r.quiet) })
	}
}

func (r *abiCoreRecord) beginDestroy() {
	r.mu.Lock()
	r.accepting = false
	r.callbacksAllowed = false
	r.callbacksClosing = true
	if r.active == 0 {
		r.quietOnce.Do(func() { close(r.quiet) })
	}
	r.mu.Unlock()
}

func (r *abiCoreRecord) waitQuiet() { <-r.quiet }

func (r *abiCoreRecord) value() *core.Core {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.core
}

func (r *abiCoreRecord) clearValue() {
	r.mu.Lock()
	r.core = nil
	r.mu.Unlock()
}

func (r *abiCoreRecord) installCallbacks(manager *callbackManager) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting || !r.callbacksAllowed || r.core == nil || r.callbacks != nil || manager == nil {
		return errInvalidABIHandle
	}
	r.callbacks = manager
	manager.start()
	return nil
}

func (r *abiCoreRecord) allowCallbacks() (bool, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	allowed := r.accepting && r.callbacksAllowed && !r.callbacksClosing && r.core != nil && r.callbacks == nil
	return allowed, r.callbackRegistrationCheckpoint
}

func (r *abiCoreRecord) stopCallbacks(ctx context.Context, permanent bool) error {
	r.mu.Lock()
	if permanent {
		r.callbacksClosing = true
	}
	// Blocks replacement registration until the current manager has fully
	// quiesced; a timed-out clear can be joined safely by a later clear.
	r.callbacksAllowed = false
	manager := r.callbacks
	r.mu.Unlock()
	if err := manager.stop(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if r.callbacks == manager {
		r.callbacks = nil
	}
	if !permanent && !r.callbacksClosing && r.accepting && r.core != nil {
		r.callbacksAllowed = true
	}
	r.mu.Unlock()
	return nil
}

func (r *abiCoreRecord) destroy() error {
	r.destroyOnce.Do(func() {
		r.waitQuiet()
		if err := r.stopCallbacks(context.Background(), true); err != nil {
			r.destroyErr = err
			close(r.destroyDone)
			return
		}
		value := r.value()
		if value != nil {
			r.destroyErr = value.Destroy()
		}
		r.clearValue()
		close(r.destroyDone)
	})
	<-r.destroyDone
	return r.destroyErr
}
