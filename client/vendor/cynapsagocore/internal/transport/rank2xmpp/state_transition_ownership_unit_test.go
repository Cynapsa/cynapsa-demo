package rank2xmpp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func stateOwnershipTestClient(session Session, clock *transport.CalibratedClock, fence func()) *Client {
	return &Client{
		clock:             clock,
		session:           session,
		state:             DurableLive,
		stateChanged:      make(chan struct{}),
		started:           true,
		generation:        7,
		authorityFence:    fence,
		complete:          make(map[string]completionWaiter),
		jingle:            make(map[string]*jingleWaiter),
		readiness:         make(map[string]objectReadinessWaiter),
		readinessSeen:     make(map[string]objectReadinessInbound),
		transferAdmission: newTransferAdmission(0),
		reconnectDemand:   make(chan reconnectRequest, 1),
	}
}

func TestOwnedStateTransitionCannotOverwriteClose(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint32
	client := stateOwnershipTestClient(session, nil, func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	})

	transitionDone := make(chan struct{})
	go func() {
		client.setState(DurablePending)
		close(transitionDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pending transition did not enter the authority fence")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- client.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		t.Fatalf("Close crossed the admitted old transition: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case <-transitionDone:
	case <-time.After(time.Second):
		t.Fatal("pending transition did not finish")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the transition released publication")
	}
	if !client.closed || client.DurableState() != DurableUnknown {
		t.Fatalf("terminal close was overwritten: closed=%t state=%v", client.closed, client.DurableState())
	}
}

func TestOwnedStateTransitionRejectsRetiredSessionAndIngress(t *testing.T) {
	oldSession := &fakeSession{events: make(chan Event)}
	newSession := &fakeSession{events: make(chan Event)}
	oldIngress := newIngressGeneration(11, oldSession, "a@example.test/mesh", nil, context.Background())
	newIngress := newIngressGeneration(12, newSession, "a@example.test/mesh", nil, context.Background())
	t.Cleanup(oldIngress.cancel)
	t.Cleanup(newIngress.cancel)
	var fences atomic.Uint32
	client := stateOwnershipTestClient(oldSession, nil, func() { fences.Add(1) })
	client.ingress, client.sessionEpoch = oldIngress, oldIngress.id
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture current session owner")
	}

	client.mu.Lock()
	client.session, client.ingress, client.sessionEpoch = newSession, newIngress, newIngress.id
	client.mu.Unlock()
	if err := client.transitionOwnedState(owner, DurablePending, true, false); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("retired owner transition=%v", err)
	}
	if client.DurableState() != DurableLive || fences.Load() != 0 {
		t.Fatalf("retired owner demoted replacement: state=%v fences=%d", client.DurableState(), fences.Load())
	}
	select {
	case <-client.reconnectDemand:
		t.Fatal("retired owner requested reconnect for the replacement")
	default:
	}
}

func TestClockInvalidationTransitionIsExactToCurrentCalibration(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	local := base
	clock := transport.NewCalibratedClock(func() time.Time { return local })
	calibrate := func(server time.Time) {
		t.Helper()
		if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) { return server, nil }); err != nil {
			t.Fatal(err)
		}
	}
	calibrate(base)
	session := &fakeSession{events: make(chan Event)}
	var fences atomic.Uint32
	client := stateOwnershipTestClient(session, clock, func() { fences.Add(1) })
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture clock transition owner")
	}
	invalidated := clock.Invalidation()
	local = base.Add(-2 * time.Second)
	if _, valid := clock.Snapshot(); valid {
		t.Fatal("backward local clock did not invalidate calibration")
	}
	select {
	case <-invalidated:
	default:
		t.Fatal("invalidation edge was not published")
	}

	// Model a recovery that recalibrates before the clock loop consumes the old
	// invalidation edge. That closed edge must not demote the recovered session.
	local = base.Add(time.Second)
	calibrate(base.Add(time.Second))
	if err := client.transitionOwnedState(owner, DurablePending, true, true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale clock transition=%v", err)
	}
	if client.DurableState() != DurableLive || fences.Load() != 0 {
		t.Fatalf("stale invalidation demoted recovery: state=%v fences=%d", client.DurableState(), fences.Load())
	}
	select {
	case <-client.reconnectDemand:
		t.Fatal("stale invalidation requested reconnect")
	default:
	}

	// A genuinely current invalid calibration still fails closed and requests
	// the normal bounded recovery path.
	local = base.Add(-3 * time.Second)
	if _, valid := clock.Snapshot(); valid {
		t.Fatal("second backward clock did not invalidate calibration")
	}
	current, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture current invalidation owner")
	}
	if err := client.transitionOwnedState(current, DurablePending, true, true); err != nil {
		t.Fatalf("current clock transition=%v", err)
	}
	if client.DurableState() != DurablePending || fences.Load() != 1 {
		t.Fatalf("current invalidation did not fail closed: state=%v fences=%d", client.DurableState(), fences.Load())
	}
	select {
	case <-client.reconnectDemand:
	default:
		t.Fatal("current invalidation did not request reconnect")
	}
}

func TestOwnedStateTransitionContainsFencePanicAndFailsClosed(t *testing.T) {
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, nil, func() { panic("fence") })
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture current owner")
	}
	if err := client.transitionOwnedState(owner, DurablePending, false, false); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("panicked fence result=%v", err)
	}
	if state := client.DurableState(); state != DurablePending {
		t.Fatalf("panicked fence left transport live: %v", state)
	}
}

func TestClockInvalidationHardFencesNetworkRetainedData(t *testing.T) {
	var fences atomic.Int32
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, transport.NewCalibratedClock(nil), func() { fences.Add(1) })
	client.state = DurablePending
	client.membershipReady = false
	client.authoritySuspended = false
	client.retainedDataAuthority = true
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture retained-data owner")
	}
	if err := client.transitionOwnedState(owner, DurablePending, false, true); err != nil {
		t.Fatal(err)
	}
	if fences.Load() != 1 || client.retainedDataAuthority {
		t.Fatalf("clock invalidation fences=%d retained=%t", fences.Load(), client.retainedDataAuthority)
	}
}

func TestRetainedDataMarkerSurvivesPanickingHardFence(t *testing.T) {
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, transport.NewCalibratedClock(nil), func() { panic("fence") })
	client.state = DurablePending
	client.membershipReady = false
	client.authoritySuspended = false
	client.retainedDataAuthority = true
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture retained-data owner")
	}
	if err := client.transitionOwnedState(owner, DurablePending, false, true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("panicking hard fence=%v", err)
	}
	if !client.retainedDataAuthority {
		t.Fatal("failed hard fence cleared the retained-data retry marker")
	}
}

func TestAuthorityIngressFencePanicPreservesRetainedDataMarker(t *testing.T) {
	session := newMelliumSession(MelliumConfig{}, Endpoint{})
	session.generation = 1
	client := stateOwnershipTestClient(session, nil, func() { panic("fence") })
	client.retainedDataAuthority = true
	client.bindAuthorityIngressLocked(session)
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(1))
	if session.fenceAuthorityIngress(ctx) {
		t.Fatal("panicking ingress fence reported success")
	}
	if !client.retainedDataAuthority {
		t.Fatal("panicking ingress fence cleared retained-data retry marker")
	}
}

func TestRecoveryLiveCannotOverwriteNewerSameSessionLoss(t *testing.T) {
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, nil, nil)
	recoveryOwner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture recovery owner")
	}
	lossOwner := recoveryOwner
	if err := client.transitionOwnedState(lossOwner, DurablePending, false, false); err != nil {
		t.Fatal(err)
	}
	if err := client.setRecoveryStateOwned(recoveryOwner, 7, DurableLive); err == nil {
		t.Fatal("old recovery overwrote a newer same-session loss")
	}
	if state := client.DurableState(); state != DurablePending {
		t.Fatalf("newer loss state=%v", state)
	}
}

type reconnectCountingSession struct {
	*fakeSession
	resumes atomic.Uint32
}

func (session *reconnectCountingSession) Resume(context.Context) (bool, error) {
	session.resumes.Add(1)
	return true, nil
}

func TestQueuedReconnectDemandCannotCrossNewerRecovery(t *testing.T) {
	session := &reconnectCountingSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := stateOwnershipTestClient(session, nil, nil)
	client.state = DurablePending
	client.stateEpoch = 1
	client.ctx = context.Background()
	client.sendMu = newCancellableMutex()
	client.recoveryGate = make(chan struct{}, 1)
	client.recoveryGate <- struct{}{}
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture pending owner")
	}
	client.requestReconnectOwned(owner)
	request := <-client.reconnectDemand

	if err := client.setRecoveryStateOwned(owner, 7, DurableLive); err != nil {
		t.Fatal(err)
	}
	if client.reconnectOwned(request.owner) {
		t.Fatal("stale queued demand recovered after a newer Live publication")
	}
	if calls := session.resumes.Load(); calls != 0 {
		t.Fatalf("stale queued demand invoked Resume %d times", calls)
	}
}

func TestPendingRetryCannotQueueHealthySessionOwner(t *testing.T) {
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, nil, nil)
	client.state = DurablePending
	client.stateEpoch = 1
	client.requestReconnectIfPending()
	queued := <-client.reconnectDemand
	if queued.owner.stateEpoch != 1 {
		t.Fatalf("queued owner epoch=%d", queued.owner.stateEpoch)
	}
	if err := client.setRecoveryStateOwned(queued.owner, client.generation, DurableLive); err != nil {
		t.Fatal(err)
	}
	client.requestReconnectIfPending()
	select {
	case request := <-client.reconnectDemand:
		t.Fatalf("healthy session received pending retry: %#v", request)
	default:
	}
}

func TestRecoveryFailureFencePanicStillPublishesPending(t *testing.T) {
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, nil, func() { panic("fence") })
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture recovery owner")
	}
	if err := client.setRecoveryStateOwned(owner, client.generation, DurablePending); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("panicked recovery fence result=%v", err)
	}
	if state := client.DurableState(); state != DurablePending {
		t.Fatalf("panicked recovery fence left state=%v", state)
	}
}

func TestStalePendingObservationCannotDemandAfterLiveRecovery(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var sourceMu sync.Mutex
	block := false
	entered, release := make(chan struct{}), make(chan struct{})
	clock := transport.NewCalibratedClock(func() time.Time {
		sourceMu.Lock()
		wait := block
		if block {
			block = false
			close(entered)
		}
		sourceMu.Unlock()
		if wait {
			<-release
		}
		return base
	})
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) { return base, nil }); err != nil {
		t.Fatal(err)
	}
	client := stateOwnershipTestClient(&fakeSession{events: make(chan Event)}, clock, nil)
	client.state, client.stateEpoch = DurablePending, 1
	owner, ok := client.captureStateTransitionOwner()
	if !ok {
		t.Fatal("failed to capture pending owner")
	}
	sourceMu.Lock()
	block = true
	sourceMu.Unlock()
	observed := make(chan transport.Observation, 1)
	go func() { observed <- client.Observe() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Observe did not pause after reading Pending")
	}
	if err := client.setRecoveryStateOwned(owner, client.generation, DurableLive); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("Observe did not return")
	}
	select {
	case demand := <-client.reconnectDemand:
		t.Fatalf("stale Pending observation queued demand for state edge %d", demand.owner.stateEpoch)
	default:
	}
}

func TestIngressLiveCompletionCannotOverwriteNewerSendFailure(t *testing.T) {
	session := &fakeSession{events: make(chan Event, 1)}
	ingress := newIngressGeneration(4, session, "a@example.test/mesh", nil, context.Background())
	t.Cleanup(ingress.cancel)
	client := stateOwnershipTestClient(session, nil, nil)
	client.stateEpoch = 1
	client.ingress, client.sessionEpoch = ingress, ingress.id
	client.mu.Lock()
	client.bindIngressActivationLocked(ingress)
	client.mu.Unlock()
	if err := client.transitionIngressFailure(session, ingress, DurablePending, true); err != nil {
		t.Fatal(err)
	}
	session.events <- Event{Kind: EventMailboxComplete}
	done := make(chan bool, 1)
	go func() { done <- client.runIngressGeneration(ingress) }()
	deadline := time.Now().Add(time.Second)
	for len(session.events) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(session.events) != 0 {
		t.Fatal("current ingress never entered live Receive after stale activation")
	}
	// Exercise the post-activation control synchronously as well: consuming a
	// live mailbox-complete event is not authenticated recovery evidence.
	client.handleIngressEvent(ingress, Event{Kind: EventMailboxComplete})
	if state := client.DurableState(); state != DurablePending {
		t.Fatalf("post-activation mailbox control overwrote newer send failure: %v", state)
	}
	select {
	case err := <-ingress.ready:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("stale ingress completion=%v", err)
		}
	default:
		t.Fatal("stale ingress completion did not retire readiness")
	}
	ingress.cancel()
	select {
	case keepRunning := <-done:
		if !keepRunning {
			t.Fatal("current ingress completion unexpectedly closed client")
		}
	case <-time.After(time.Second):
		t.Fatal("live receive did not stop after cancellation")
	}
}

func TestStaleActivationHandsOffReplacedIngress(t *testing.T) {
	oldSession := &fakeSession{events: make(chan Event, 1)}
	oldIngress := newIngressGeneration(4, oldSession, "a@example.test/mesh", nil, context.Background())
	t.Cleanup(oldIngress.cancel)
	client := stateOwnershipTestClient(oldSession, nil, nil)
	client.stateEpoch = 1
	client.ingress, client.sessionEpoch = oldIngress, oldIngress.id
	client.mu.Lock()
	client.bindIngressActivationLocked(oldIngress)
	client.mu.Unlock()

	newSession := &fakeSession{events: make(chan Event, 1)}
	newIngress := newIngressGeneration(5, newSession, "a@example.test/mesh", nil, context.Background())
	t.Cleanup(newIngress.cancel)
	client.mu.Lock()
	client.session, client.ingress, client.sessionEpoch = newSession, newIngress, newIngress.id
	client.bindIngressActivationLocked(newIngress)
	client.mu.Unlock()
	oldSession.events <- Event{Kind: EventMailboxComplete}
	if keepRunning := client.runIngressGeneration(oldIngress); !keepRunning {
		t.Fatal("replaced ingress incorrectly stopped outer receive loop")
	}
	if len(oldSession.events) != 1 {
		t.Fatal("replaced ingress entered old live Receive")
	}
	select {
	case err := <-oldIngress.ready:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("replaced activation result=%v", err)
		}
	default:
		t.Fatal("replaced activation did not finish")
	}
}
