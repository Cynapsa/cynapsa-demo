package main

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

const (
	callbackKindCompletion uint32 = iota + 1
	callbackKindEvent
	callbackKindDiagnostic
)

type callbackManagerLifecycle uint8

const (
	callbackManagerNew callbackManagerLifecycle = iota
	callbackManagerRunning
	callbackManagerStopped
)

type callbackJob struct {
	token      uint64
	kind       uint32
	descriptor BufferDescriptor
	commit     func() error
	rollback   func() error
}

type callbackReservation struct {
	encoded   []byte
	name      string
	commit    func() error
	rollback  func() error
	normalize func(error) error
}

type callbackManager struct {
	coreHandle uint64
	core       *core.Core
	callback   unsafe.Pointer
	completion uint64
	event      uint64
	diagnostic uint64

	ctx       context.Context
	cancel    context.CancelFunc
	jobs      chan callbackJob
	wg        sync.WaitGroup
	pumps     atomic.Int32
	pumpsDone chan struct{}
	clear     sync.Once
	done      chan struct{}
	doneOnce  sync.Once
	state     *callbackState
	// Production construction fixes these dependencies. Package tests replace
	// them only to prove descriptor cleanup when a host callback unwinds.
	buffers           *bufferStore
	invoke            func(unsafe.Pointer, uint64, uint64, uint32, BufferDescriptor)
	reserveCompletion func(context.Context) (callbackReservation, error)
	reserveEvent      func(context.Context) (callbackReservation, error)
	beforeDispatch    func(callbackJob)

	lifecycleMu sync.Mutex
	lifecycle   callbackManagerLifecycle
}

func newCallbackManager(coreHandle uint64, value *core.Core, callback unsafe.Pointer, completion, event, diagnostic uint64, capacity uint32) (*callbackManager, error) {
	if value == nil || callback == nil || capacity == 0 || capacity > 65_536 || completion == 0 && event == 0 || diagnostic != 0 && event == 0 {
		return nil, errInvalidABIInput
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &callbackManager{
		coreHandle: coreHandle,
		core:       value,
		callback:   callback,
		completion: completion,
		event:      event,
		diagnostic: diagnostic,
		ctx:        ctx,
		cancel:     cancel,
		jobs:       make(chan callbackJob, capacity),
		done:       make(chan struct{}),
		state:      newCallbackState(),
		buffers:    processBuffers,
		invoke:     invokeNativeCallback,
	}
	m.reserveCompletion = func(ctx context.Context) (callbackReservation, error) {
		encoded, lease, err := value.ReserveNativeCompletionABI(ctx)
		if err != nil {
			return callbackReservation{}, err
		}
		return callbackReservation{encoded: encoded, commit: lease.Commit, rollback: lease.Rollback, normalize: lease.NormalizeError}, nil
	}
	m.reserveEvent = func(ctx context.Context) (callbackReservation, error) {
		encoded, name, lease, err := value.ReserveNativeEventABI(ctx, true)
		if err != nil {
			return callbackReservation{}, err
		}
		return callbackReservation{encoded: encoded, name: name, commit: lease.Commit, rollback: lease.Rollback, normalize: lease.NormalizeError}, nil
	}
	return m, nil
}

// start is called exactly once while the owning record lock still makes this
// manager visible. Construction itself starts no consumer, so a rejected
// registration can never steal a completion or event.
func (m *callbackManager) start() {
	m.lifecycleMu.Lock()
	if m.lifecycle != callbackManagerNew {
		m.lifecycleMu.Unlock()
		return
	}
	m.lifecycle = callbackManagerRunning

	workers := 1
	pumpWorkers := 0
	if m.completion != 0 {
		workers++
		pumpWorkers++
	}
	if m.event != 0 {
		workers++
		pumpWorkers++
	}
	m.pumps.Store(int32(pumpWorkers))
	m.pumpsDone = make(chan struct{})
	if pumpWorkers == 0 {
		close(m.pumpsDone)
	}
	// Every Add must happen before any worker or waiter can call Wait.
	m.wg.Add(workers)
	if m.completion != 0 {
		go m.completionPump()
	}
	if m.event != 0 {
		go m.eventPump()
	}
	go m.dispatch()
	go func() {
		m.wg.Wait()
		m.lifecycleMu.Lock()
		m.lifecycle = callbackManagerStopped
		m.closeDone()
		m.lifecycleMu.Unlock()
	}()
	m.lifecycleMu.Unlock()
}

func (m *callbackManager) completionPump() {
	defer m.wg.Done()
	defer m.pumpDone()
	for {
		reservation, err := m.reserveCompletion(m.ctx)
		if err != nil {
			return
		}
		if !m.prepareAndEnqueue(reservation, m.completion, callbackKindCompletion) {
			return
		}
	}
}

func (m *callbackManager) eventPump() {
	defer m.wg.Done()
	defer m.pumpDone()
	for {
		reservation, err := m.reserveEvent(m.ctx)
		if err != nil {
			return
		}
		token, kind := m.event, callbackKindEvent
		if reservation.name == string(v1.EventDiagnosticLog) && m.diagnostic != 0 {
			token, kind = m.diagnostic, callbackKindDiagnostic
		}
		if !m.prepareAndEnqueue(reservation, token, kind) {
			return
		}
	}
}

func (m *callbackManager) prepareAndEnqueue(reservation callbackReservation, token uint64, kind uint32) bool {
	descriptor, changed, err := safeAllocateOrWait(m.buffers, reservation.encoded)
	clear(reservation.encoded)
	if err != nil {
		_ = reservation.rollback()
		if changed == nil {
			return false
		}
		select {
		case <-m.ctx.Done():
			return false
		case <-changed:
			return true
		}
	}
	job := callbackJob{
		token: token, kind: kind, descriptor: descriptor,
		commit: reservation.commit, rollback: reservation.rollback,
	}
	select {
	case m.jobs <- job:
		return true
	case <-m.ctx.Done():
		m.abortJob(job)
		return false
	}
}

func (m *callbackManager) dispatch() {
	defer m.wg.Done()
	for {
		select {
		case job := <-m.jobs:
			if m.beforeDispatch != nil {
				m.beforeDispatch(job)
			}
			if !m.state.begin() {
				m.abortJob(job)
				continue
			}
			err, panicked := m.commitJob(job)
			if panicked {
				m.state.endAndCloseAdmission()
				m.clear.Do(m.cancel)
				m.drainPreparedJobsAfterPumps()
				return
			}
			if err != nil {
				_ = m.buffers.free(job.descriptor.BufferHandle)
				m.state.end()
				continue
			}
			completed := false
			func() {
				defer func() {
					if recover() != nil {
						_ = m.buffers.free(job.descriptor.BufferHandle)
						return
					}
					completed = true
				}()
				m.invoke(m.callback, m.coreHandle, job.token, job.kind, job.descriptor)
			}()
			m.state.end()
			if !completed {
				continue
			}
		case <-m.ctx.Done():
			m.drainPreparedJobsAfterPumps()
			return
		}
	}
}

func (m *callbackManager) pumpDone() {
	if m.pumps.Add(-1) == 0 {
		close(m.pumpsDone)
	}
}

func (m *callbackManager) drainPreparedJobsAfterPumps() {
	if m.pumpsDone == nil {
		for {
			select {
			case job := <-m.jobs:
				m.abortJob(job)
			default:
				return
			}
		}
	}
	for {
		select {
		case job := <-m.jobs:
			m.abortJob(job)
		case <-m.pumpsDone:
			for {
				select {
				case job := <-m.jobs:
					m.abortJob(job)
				default:
					return
				}
			}
		}
	}
}

func (m *callbackManager) commitJob(job callbackJob) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			m.abortJob(job)
			panicked = true
		}
	}()
	return job.commit(), false
}

func (m *callbackManager) abortJob(job callbackJob) {
	rollbackReservationNoPanic(job.rollback)
	if job.descriptor.BufferHandle != 0 {
		_ = m.buffers.free(job.descriptor.BufferHandle)
	}
}

func rollbackReservationNoPanic(rollback func() error) {
	if rollback == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = rollback()
}

func (m *callbackManager) stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.lifecycleMu.Lock()
	if m.lifecycle == callbackManagerNew {
		// No worker can have admitted a callback before start. Retire this
		// unpublished candidate independently of the caller's wait budget.
		m.lifecycle = callbackManagerStopped
		_ = m.state.clear(context.Background())
		m.clear.Do(m.cancel)
		m.closeDone()
		m.lifecycleMu.Unlock()
		return nil
	}
	done := m.done
	m.lifecycleMu.Unlock()

	// Close callback admission before cancelling the pumps. This establishes
	// the clear call as the exact no-new-dispatch boundary; already in-flight
	// host code is joined below.
	err := m.state.clear(ctx)
	m.clear.Do(m.cancel)
	if err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *callbackManager) closeDone() {
	m.doneOnce.Do(func() { close(m.done) })
}
