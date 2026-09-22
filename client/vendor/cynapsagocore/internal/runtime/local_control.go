package runtime

import (
	"context"
	"fmt"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// ConfigSnapshot returns an owned copy of the current private runtime
// configuration. Callers must still project only allowlisted public fields.
func (r *Runtime) ConfigSnapshot() model.RuntimeConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneRuntimeConfig(r.config.RuntimeConfig)
}

// UpdateConfig atomically applies the two V1 mutable application timeouts.
// Queue and payload limits define allocated resource ownership and are always
// rejected when present, even when the requested value equals the current
// value. This lets the boundary distinguish an immutable-field request from an
// empty update without rebuilding live queues or stores.
func (r *Runtime) UpdateConfig(ctx context.Context, update model.ConfigUpdateArgs) (model.RuntimeConfig, error) {
	if ctx == nil {
		return model.RuntimeConfig{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return model.RuntimeConfig{}, err
	}
	if update.QueueLimit != nil || update.PayloadLimit != nil {
		return model.RuntimeConfig{}, ErrImmutableConfig
	}
	if update.CommandTimeout != nil && *update.CommandTimeout <= 0 || update.RPCTimeout != nil && *update.RPCTimeout <= 0 {
		return model.RuntimeConfig{}, ErrInvalidConfig
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.RuntimeConfig{}, err
	}
	if r.state == stateClosing {
		return model.RuntimeConfig{}, ErrClosing
	}
	if r.state == stateClosed {
		return model.RuntimeConfig{}, ErrClosed
	}
	if update.CommandTimeout != nil {
		r.config.CommandTimeout = *update.CommandTimeout
	}
	if update.RPCTimeout != nil {
		r.config.RPCTimeout = *update.RPCTimeout
	}
	return cloneRuntimeConfig(r.config.RuntimeConfig), nil
}

// RegisterCompletionChannel registers the sole logical consumer of the
// authoritative Gate completion queue. It does not create or mirror a second
// queue and never invokes an SDK callback.
func (r *Runtime) RegisterCompletionChannel(ctx context.Context, args model.ChannelRegisterArgs) (model.CompletionChannelResult, error) {
	if err := validateControlContext(ctx); err != nil {
		return model.CompletionChannelResult{}, err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlRequestLocked(ctx, args.Capacity); err != nil {
		return model.CompletionChannelResult{}, err
	}
	if r.completionChannel != nil {
		return model.CompletionChannelResult{}, ErrControlRegistered
	}
	registration := &localRegistration{id: r.nextControlIDLocked("completion"), capacity: args.Capacity}
	r.completionChannel = registration
	return model.CompletionChannelResult{ChannelID: registration.id, MaxInFlight: registration.capacity}, nil
}

// ClearCompletionChannel removes only the logical consumer registration. The
// Gate retains ownership of admitted completions; abandoning queued completion
// output remains restricted to Gate shutdown.
func (r *Runtime) ClearCompletionChannel(ctx context.Context, args model.ChannelIDArgs) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if r.completionChannel == nil || args.ChannelID == "" || r.completionChannel.id != args.ChannelID {
		return ErrControlNotFound
	}
	r.completionChannel = nil
	return nil
}

// RegisterEventSink registers the sole logical consumer of the authoritative
// Runtime event queue. Callback installation and invocation remain outside
// Runtime ownership.
func (r *Runtime) RegisterEventSink(ctx context.Context, args model.EventSinkRegisterArgs) (model.EventSinkResult, error) {
	if err := validateControlContext(ctx); err != nil {
		return model.EventSinkResult{}, err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlRequestLocked(ctx, args.Capacity); err != nil {
		return model.EventSinkResult{}, err
	}
	if r.eventSink != nil {
		return model.EventSinkResult{}, ErrControlRegistered
	}
	registration := &localRegistration{id: r.nextControlIDLocked("sink"), capacity: args.Capacity}
	r.eventSink = registration
	return model.EventSinkResult{SinkID: registration.id}, nil
}

// BindEventSink marks the registered logical sink as the consumer for this
// Runtime. A Runtime represents exactly one SDK/Core session, so no separate
// session identifier is accepted here.
func (r *Runtime) BindEventSink(ctx context.Context, args model.EventSinkIDArgs) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		return err
	}
	if r.eventSink == nil || args.SinkID == "" || r.eventSink.id != args.SinkID {
		return ErrControlNotFound
	}
	r.boundEventSink = args.SinkID
	return nil
}

// ClearEventSink removes and unbinds the logical sink. It does not drain the
// shared event queue; Runtime shutdown owns bounded drain or abandonment.
func (r *Runtime) ClearEventSink(ctx context.Context, args model.EventSinkIDArgs) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if r.eventSink == nil || args.SinkID == "" || r.eventSink.id != args.SinkID {
		return ErrControlNotFound
	}
	r.eventSink = nil
	r.boundEventSink = ""
	return nil
}

// SetDiagnosticLogSubscription controls admission of the closed, support-safe
// diagnostics.log event variant. Disabling the subscription does not reveal or
// retain raw dependency logs.
func (r *Runtime) SetDiagnosticLogSubscription(ctx context.Context, args model.DiagnosticsLogsArgs) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		r.controlMu.Unlock()
		return err
	}
	r.diagnosticLogs = args.Enabled
	r.controlMu.Unlock()
	return nil
}

// PollDelivery consumes one event from the authoritative Runtime event queue
// without waiting. This prompt form is intended for delivery.next handlers;
// public blocking polling continues to use NextEvent directly.
func (r *Runtime) PollDelivery(ctx context.Context, _ model.EmptyArgs) (model.EventResult, error) {
	if err := validateControlContext(ctx); err != nil {
		return model.EventResult{}, err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		return model.EventResult{}, err
	}
	if r.deliveryPaused {
		return model.EventResult{}, ErrDeliveryPaused
	}
	if r.outstandingDelivery != "" {
		return model.EventResult{}, ErrDeliveryAcceptPending
	}
	commandID, transactional := ctx.Value(deliveryCommandContextKey{}).(string)
	if transactional && commandID == "" {
		return model.EventResult{}, ErrDeliveryNotPending
	}
	event, lease, err := r.events.ReserveNext(ctx)
	if err != nil {
		return model.EventResult{}, err
	}
	if transactional {
		if r.deliveryLeases == nil {
			r.deliveryLeases = make(map[string]*delivery.EventLease)
		}
		if _, exists := r.deliveryLeases[commandID]; exists {
			_ = r.events.Rollback(lease)
			return model.EventResult{}, ErrDeliveryAcceptPending
		}
		r.deliveryLeases[commandID] = lease
	} else {
		admission := lease.AdmissionID()
		if err := r.events.Commit(lease); err != nil {
			return model.EventResult{}, err
		}
		r.outstandingDelivery = event.ID
		r.outstandingInbound = admission
	}
	return model.EventResult{Event: event}, nil
}

// AcceptDelivery confirms that the SDK safely took ownership of, or enqueued,
// the exact event returned by the preceding command-side PollDelivery. It does
// not assert that application processing completed and is not a network ACK.
// The acknowledgement is exact and single-use; wrong, stale, and repeated IDs
// fail closed without changing the outstanding event.
func (r *Runtime) AcceptDelivery(ctx context.Context, args model.DeliveryAcceptArgs) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		return err
	}
	if args.EventID == "" || r.outstandingDelivery == "" || args.EventID != r.outstandingDelivery {
		return ErrDeliveryNotPending
	}
	r.outstandingDelivery = ""
	admission := r.outstandingInbound
	r.outstandingInbound = 0
	r.notifyDeliveryChangedLocked()
	if receipt, ok := r.inboundDeliveries[admission]; ok {
		delete(r.inboundDeliveries, admission)
		receipt <- nil
	}
	return nil
}

// DeliveryQueueStatus returns bounded local event-queue accounting. It does
// not expose private transport or durable-outbox state.
func (r *Runtime) DeliveryQueueStatus(ctx context.Context, _ model.EmptyArgs) (model.DeliveryQueueStatus, error) {
	if err := validateControlContext(ctx); err != nil {
		return model.DeliveryQueueStatus{}, err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		return model.DeliveryQueueStatus{}, err
	}
	stats := r.events.Stats()
	return model.DeliveryQueueStatus{Queued: uint64(stats.Depth), Paused: r.deliveryPaused}, nil
}

// PauseDelivery promptly pauses command-side local delivery consumption while
// retaining the bounded queue. Event producers remain subject to its existing
// fail-closed capacity.
func (r *Runtime) PauseDelivery(ctx context.Context, _ model.EmptyArgs) error {
	return r.setDeliveryPaused(ctx, true)
}

// ResumeDelivery promptly resumes command-side local delivery consumption.
func (r *Runtime) ResumeDelivery(ctx context.Context, _ model.EmptyArgs) error {
	return r.setDeliveryPaused(ctx, false)
}

func (r *Runtime) setDeliveryPaused(ctx context.Context, paused bool) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := r.validateControlStateLocked(ctx); err != nil {
		return err
	}
	r.deliveryPaused = paused
	return nil
}

func (r *Runtime) validateControlRequestLocked(ctx context.Context, capacity uint32) error {
	if err := r.validateControlStateLocked(ctx); err != nil {
		return err
	}
	r.mu.RLock()
	limit := r.config.QueueLimit
	r.mu.RUnlock()
	if capacity == 0 || capacity > limit {
		return ErrControlCapacity
	}
	return nil
}

func (r *Runtime) validateControlStateLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	state := r.state
	r.mu.RUnlock()
	if state == stateClosing {
		return ErrClosing
	}
	if state == stateClosed {
		return ErrClosed
	}
	return nil
}

func validateControlContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

func (r *Runtime) nextControlIDLocked(prefix string) string {
	r.controlNext++
	return fmt.Sprintf("%s-%d", prefix, r.controlNext)
}

func (r *Runtime) clearLocalControls() {
	r.controlMu.Lock()
	r.completionChannel = nil
	r.eventSink = nil
	r.boundEventSink = ""
	r.diagnosticLogs = false
	r.deliveryPaused = false
	r.outstandingDelivery = ""
	r.outstandingInbound = 0
	r.notifyDeliveryChangedLocked()
	for commandID, lease := range r.deliveryLeases {
		_ = r.events.Rollback(lease)
		delete(r.deliveryLeases, commandID)
	}
	r.controlMu.Unlock()
}

func cloneRuntimeConfig(config model.RuntimeConfig) model.RuntimeConfig {
	config.Connectivity.BootstrapData = append([]byte(nil), config.Connectivity.BootstrapData...)
	return config
}
