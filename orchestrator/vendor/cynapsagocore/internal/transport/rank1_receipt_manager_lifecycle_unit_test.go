package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type lifecycleReceiptAdapter struct {
	*fakeBoundAdapter
	mu       sync.Mutex
	receipts int
}

type rebindableLifecycleReceiptAdapter struct {
	*lifecycleReceiptAdapter
}

func (*rebindableLifecycleReceiptAdapter) BindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	return gate != nil && gate.Admit(epoch)
}

func (*rebindableLifecycleReceiptAdapter) RebindLiveAuthority(gate *LiveAuthorityGate, epoch uint64) bool {
	return gate != nil && gate.Rebindable(epoch)
}

func (*rebindableLifecycleReceiptAdapter) BlockLiveAuthority() {}

type noncooperativeReceiptAdapter struct {
	*lifecycleReceiptAdapter
	entered chan struct{}
	release chan struct{}
}

type trackedLifecyclePending struct {
	binding        LiveReceipt
	done           chan bool
	bindingEntered chan struct{}
	bindingRelease chan struct{}
	bindingPanic   bool
	closeEntered   chan struct{}
	closeRelease   chan struct{}
	closePanic     bool
	mu             sync.Mutex
	closes         int
}

func (pending *trackedLifecyclePending) Binding() LiveReceipt {
	if pending.bindingEntered != nil {
		close(pending.bindingEntered)
	}
	if pending.bindingRelease != nil {
		<-pending.bindingRelease
	}
	if pending.bindingPanic {
		panic("pending binding")
	}
	return pending.binding
}
func (pending *trackedLifecyclePending) Done() <-chan bool { return pending.done }
func (pending *trackedLifecyclePending) Close() {
	pending.mu.Lock()
	pending.closes++
	pending.mu.Unlock()
	if pending.closeEntered != nil {
		close(pending.closeEntered)
	}
	if pending.closeRelease != nil {
		<-pending.closeRelease
	}
	if pending.closePanic {
		panic("pending close")
	}
}
func (pending *trackedLifecyclePending) closeCount() int {
	pending.mu.Lock()
	defer pending.mu.Unlock()
	return pending.closes
}

type noncooperativeTrackedAdapter struct {
	*lifecycleReceiptAdapter
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	pending *trackedLifecyclePending
}

type trackedSendResult struct {
	pending PendingLiveReceipt
	err     error
}

type configuredTrackedAdapter struct {
	*lifecycleReceiptAdapter
	pending func(protocol.Envelope) PendingLiveReceipt
	err     error
}

func (adapter *configuredTrackedAdapter) SendTracked(_ context.Context, envelope protocol.Envelope) (PendingLiveReceipt, error) {
	return adapter.pending(envelope), adapter.err
}

func (adapter *noncooperativeTrackedAdapter) SendTracked(_ context.Context, envelope protocol.Envelope) (PendingLiveReceipt, error) {
	close(adapter.entered)
	<-adapter.release
	pending := &trackedLifecyclePending{binding: liveReceiptForEnvelope(envelope, [32]byte{1}), done: make(chan bool)}
	adapter.mu.Lock()
	adapter.pending = pending
	adapter.mu.Unlock()
	return pending, nil
}

func (adapter *noncooperativeTrackedAdapter) returnedPending() *trackedLifecyclePending {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.pending
}

func (adapter *noncooperativeReceiptAdapter) SendReceipt(context.Context, LiveReceipt) error {
	close(adapter.entered)
	<-adapter.release
	return nil
}

func (*lifecycleReceiptAdapter) SendTracked(context.Context, protocol.Envelope) (PendingLiveReceipt, error) {
	return nil, ErrUnavailable
}

func (adapter *lifecycleReceiptAdapter) SendReceipt(context.Context, LiveReceipt) error {
	adapter.mu.Lock()
	adapter.receipts++
	adapter.mu.Unlock()
	return nil
}

func (adapter *lifecycleReceiptAdapter) receiptCount() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.receipts
}

func lifecycleReceiptItem(manager *Manager, source LiveReceiptTransport, envelope protocol.Envelope) Received {
	binding := liveReceiptForEnvelope(envelope, [32]byte{1})
	return Received{
		Kind: KindLive, Envelope: envelope,
		Authentication: &LiveAuthentication{Peer: envelope.Sender, MeshID: envelope.MeshID, ChannelBinding: binding.ChannelBinding},
		authorityEpoch: manager.liveAuthorityEpoch,
		receiptRoute:   &liveReceiptRoute{source: source, binding: binding},
	}
}

func TestDelayedAndLegacyReceiptFailAfterManagerAuthorityFence(t *testing.T) {
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	source := &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: base, auth: make(chan AuthenticatedReceived)}}
	manager, err := NewManager(2, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	item := lifecycleReceiptItem(manager, source, envelope)
	ticket, err := manager.TakeLiveReceipt(item)
	if err != nil {
		t.Fatal(err)
	}
	manager.BlockLiveAuthority()
	if err = ticket.Acknowledge(context.Background()); err == nil {
		t.Fatal("delayed ticket acknowledged after authority fence")
	}
	if err = manager.AcknowledgeLive(context.Background(), item); err == nil {
		t.Fatal("legacy receipt acknowledged after authority fence")
	}
	if got := source.receiptCount(); got != 0 {
		t.Fatalf("fenced manager emitted %d stale receipts", got)
	}
}

func TestDelayedReceiptFailsAfterExactSourceReplacement(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	newAdapter := func() *lifecycleReceiptAdapter {
		return &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}}
	}
	first, second := newAdapter(), newAdapter()
	if err = manager.InstallLive(context.Background(), envelope.Sender, first); err != nil {
		t.Fatal(err)
	}
	item := lifecycleReceiptItem(manager, first, envelope)
	ticket, err := manager.TakeLiveReceipt(item)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.InstallLive(context.Background(), envelope.Sender, second); err != nil {
		t.Fatal(err)
	}
	if err = ticket.Acknowledge(context.Background()); err == nil {
		t.Fatal("old-source ticket acknowledged after replacement")
	}
	if got := first.receiptCount(); got != 0 {
		t.Fatalf("replacement emitted %d receipts on old source", got)
	}
}

func TestDelayedReceiptFailsAfterAuthorityRetirement(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	source := &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}}
	if err = manager.InstallLive(context.Background(), envelope.Sender, source); err != nil {
		t.Fatal(err)
	}
	ticket, err := manager.TakeLiveReceipt(lifecycleReceiptItem(manager, source, envelope))
	if err != nil {
		t.Fatal(err)
	}
	manager.BlockLiveAuthority()
	manager.QuarantineLiveAuthority()
	if err = manager.RetireAllLive(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = ticket.Acknowledge(context.Background()); err == nil {
		t.Fatal("pre-fence ticket acknowledged after retirement")
	}
	if got := source.receiptCount(); got != 0 {
		t.Fatalf("authority retirement emitted %d stale receipts", got)
	}
}

func TestPausedInboundReceiptRebindsOnlyToRetainedExactSource(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	envelope := managerEnvelope(t)
	source := &rebindableLifecycleReceiptAdapter{lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{
		fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived),
	}}}
	if err = manager.InstallLive(context.Background(), envelope.Sender, source); err != nil {
		t.Fatal(err)
	}
	epoch := manager.BlockLiveAuthority()
	item := lifecycleReceiptItem(manager, source, envelope)
	item.authorityEpoch = epoch - 1
	item.liveSource = source
	item.pausedAdmission = true
	ticket, err := manager.TakeLiveReceipt(item)
	if err != nil {
		t.Fatalf("take paused receipt=%v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, []string{envelope.Sender}); err != nil {
		t.Fatalf("reconcile retained=%v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish retained=%v", err)
	}
	if err = ticket.Acknowledge(context.Background()); err != nil {
		t.Fatalf("acknowledge rebound receipt=%v", err)
	}
	if got := source.receiptCount(); got != 1 {
		t.Fatalf("retained source receipts=%d", got)
	}
}

func TestPausedInboundReceiptRejectsAbsentExactSource(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	envelope := managerEnvelope(t)
	source := &rebindableLifecycleReceiptAdapter{lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{
		fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived),
	}}}
	if err = manager.InstallLive(context.Background(), envelope.Sender, source); err != nil {
		t.Fatal(err)
	}
	epoch := manager.BlockLiveAuthority()
	item := lifecycleReceiptItem(manager, source, envelope)
	item.authorityEpoch = epoch - 1
	item.liveSource = source
	item.pausedAdmission = true
	ticket, err := manager.TakeLiveReceipt(item)
	if err != nil {
		t.Fatalf("take paused receipt=%v", err)
	}
	if err = manager.ReconcileLiveAuthority(context.Background(), epoch, nil); err != nil {
		t.Fatalf("reconcile absent=%v", err)
	}
	if err = manager.PublishLiveAuthority(epoch); err != nil {
		t.Fatalf("publish absent=%v", err)
	}
	if err = ticket.Acknowledge(context.Background()); err == nil {
		t.Fatal("absent source receipt acknowledged")
	}
	if got := source.receiptCount(); got != 0 {
		t.Fatalf("absent source receipts=%d", got)
	}
}

func TestAuthorityFenceDoesNotWaitForNoncooperativeReceiptWriter(t *testing.T) {
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	source := &noncooperativeReceiptAdapter{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: base, auth: make(chan AuthenticatedReceived)}},
		entered:                 make(chan struct{}), release: make(chan struct{}),
	}
	manager, err := NewManager(2, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ticket, err := manager.TakeLiveReceipt(lifecycleReceiptItem(manager, source, managerEnvelope(t)))
	if err != nil {
		t.Fatal(err)
	}
	ackDone := make(chan error, 1)
	go func() { ackDone <- ticket.Acknowledge(context.Background()) }()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("receipt writer was not entered")
	}
	fenceDone := make(chan struct{})
	go func() { manager.BlockLiveAuthority(); close(fenceDone) }()
	select {
	case <-fenceDone:
	case <-time.After(100 * time.Millisecond):
		close(source.release)
		t.Fatal("authority fence waited for noncooperative receipt writer")
	}
	close(source.release)
	if err = <-ackDone; !errors.Is(err, ErrSendAmbiguous) {
		t.Fatalf("fenced in-flight receipt disposition=%v", err)
	}
}

func TestTrackedSendClosesOldEpochPendingExactlyOnce(t *testing.T) {
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	source := &noncooperativeTrackedAdapter{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: base, auth: make(chan AuthenticatedReceived)}},
		entered:                 make(chan struct{}), release: make(chan struct{}),
	}
	manager, err := NewManager(2, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan trackedSendResult, 1)
	go func() {
		pending, sendErr := manager.SendLiveTracked(context.Background(), managerEnvelope(t))
		result <- trackedSendResult{pending: pending, err: sendErr}
	}()
	<-source.entered
	manager.BlockLiveAuthority()
	close(source.release)
	sendResult := <-result
	if sendResult.pending != nil {
		t.Fatal("stale tracked send returned pending ownership")
	}
	if !errors.Is(sendResult.err, ErrSendAmbiguous) {
		t.Fatalf("stale tracked send=%v", sendResult.err)
	}
	pending := source.returnedPending()
	closes := 0
	if pending != nil {
		closes = pending.closeCount()
	}
	if pending == nil || closes != 1 {
		t.Fatalf("stale pending=%#v closes=%d", pending, closes)
	}
}

func TestTrackedSendClosesReplacedAndRetiredPendingExactlyOnce(t *testing.T) {
	for _, transition := range []string{"replacement", "authority"} {
		t.Run(transition, func(t *testing.T) {
			manager, err := NewLiveManager(2)
			if err != nil {
				t.Fatal(err)
			}
			if err = manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			envelope := managerEnvelope(t)
			source := &noncooperativeTrackedAdapter{
				lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}},
				entered:                 make(chan struct{}), release: make(chan struct{}),
			}
			if err = manager.InstallLive(context.Background(), envelope.Recipient, source); err != nil {
				t.Fatal(err)
			}
			result := make(chan trackedSendResult, 1)
			go func() {
				pending, sendErr := manager.SendLiveTracked(context.Background(), envelope)
				result <- trackedSendResult{pending: pending, err: sendErr}
			}()
			<-source.entered
			if transition == "replacement" {
				replacement := &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}, auth: make(chan AuthenticatedReceived)}}
				if err = manager.InstallLive(context.Background(), envelope.Recipient, replacement); err != nil {
					t.Fatal(err)
				}
			} else {
				manager.BlockLiveAuthority()
			}
			close(source.release)
			sendResult := <-result
			if sendResult.pending != nil {
				t.Fatal("stale tracked send returned pending ownership")
			}
			if !errors.Is(sendResult.err, ErrSendAmbiguous) {
				t.Fatalf("stale tracked send=%v", sendResult.err)
			}
			pending := source.returnedPending()
			if pending == nil || pending.closeCount() != 1 {
				t.Fatalf("stale pending=%#v", pending)
			}
		})
	}
}

func TestManagerCloseJoinsNoncooperativeTrackedSend(t *testing.T) {
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	source := &noncooperativeTrackedAdapter{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: base, auth: make(chan AuthenticatedReceived)}},
		entered:                 make(chan struct{}), release: make(chan struct{}),
	}
	manager, err := NewManager(2, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	sendDone := make(chan error, 1)
	go func() {
		_, sendErr := manager.SendLiveTracked(context.Background(), managerEnvelope(t))
		sendDone <- sendErr
	}()
	<-source.entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close(context.Background()) }()
	select {
	case err = <-closeDone:
		t.Fatalf("Manager.Close returned before admitted send joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(source.release)
	if err = <-sendDone; !errors.Is(err, ErrSendAmbiguous) {
		t.Fatalf("closed tracked send=%v", err)
	}
	if err = <-closeDone; err != nil {
		t.Fatalf("close=%v", err)
	}
	if pending := source.returnedPending(); pending == nil || pending.closeCount() != 1 {
		t.Fatalf("closed pending=%#v", pending)
	}
}

func newConfiguredTrackedManager(t *testing.T, pending func(protocol.Envelope) PendingLiveReceipt, sendErr error) *Manager {
	t.Helper()
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	source := &configuredTrackedAdapter{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{fakeAdapter: base, auth: make(chan AuthenticatedReceived)}},
		pending:                 pending,
		err:                     sendErr,
	}
	manager, err := NewManager(2, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return manager
}

func waitManagerClosed(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		closed := manager.closed
		manager.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Manager.Close did not enter terminal state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManagerCloseJoinsTrackedPendingBinding(t *testing.T) {
	bindingEntered, bindingRelease := make(chan struct{}), make(chan struct{})
	var returned *trackedLifecyclePending
	manager := newConfiguredTrackedManager(t, func(envelope protocol.Envelope) PendingLiveReceipt {
		returned = &trackedLifecyclePending{
			binding: liveReceiptForEnvelope(envelope, [32]byte{1}), done: make(chan bool),
			bindingEntered: bindingEntered, bindingRelease: bindingRelease,
		}
		return returned
	}, nil)
	sendDone := make(chan trackedSendResult, 1)
	go func() {
		pending, err := manager.SendLiveTracked(context.Background(), managerEnvelope(t))
		sendDone <- trackedSendResult{pending: pending, err: err}
	}()
	<-bindingEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close(context.Background()) }()
	waitManagerClosed(t, manager)
	select {
	case err := <-closeDone:
		close(bindingRelease)
		t.Fatalf("Manager.Close returned before pending Binding joined: %v", err)
	default:
	}
	close(bindingRelease)
	result := <-sendDone
	if result.pending != nil || !errors.Is(result.err, ErrSendAmbiguous) {
		t.Fatalf("closed pending Binding result=%#v err=%v", result.pending, result.err)
	}
	if returned == nil || returned.closeCount() != 1 {
		t.Fatalf("closed pending Binding cleanup=%#v", returned)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseJoinsTrackedPendingErrorCleanup(t *testing.T) {
	closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
	var returned *trackedLifecyclePending
	manager := newConfiguredTrackedManager(t, func(envelope protocol.Envelope) PendingLiveReceipt {
		returned = &trackedLifecyclePending{
			binding: liveReceiptForEnvelope(envelope, [32]byte{1}), done: make(chan bool),
			closeEntered: closeEntered, closeRelease: closeRelease,
		}
		return returned
	}, ErrUnavailable)
	sendDone := make(chan trackedSendResult, 1)
	go func() {
		pending, err := manager.SendLiveTracked(context.Background(), managerEnvelope(t))
		sendDone <- trackedSendResult{pending: pending, err: err}
	}()
	<-closeEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close(context.Background()) }()
	waitManagerClosed(t, manager)
	select {
	case err := <-closeDone:
		close(closeRelease)
		t.Fatalf("Manager.Close returned before pending error cleanup joined: %v", err)
	default:
	}
	close(closeRelease)
	result := <-sendDone
	if result.pending != nil || !errors.Is(result.err, ErrSendAmbiguous) {
		t.Fatalf("closed pending error cleanup result=%#v err=%v", result.pending, result.err)
	}
	if returned == nil || returned.closeCount() != 1 {
		t.Fatalf("closed pending error cleanup=%#v", returned)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestTrackedPendingPanicsAreContainedAndClosedOnce(t *testing.T) {
	for _, test := range []struct {
		name         string
		bindingPanic bool
		closePanic   bool
		sendErr      error
		want         error
	}{
		{name: "binding", bindingPanic: true, want: ErrProtocol},
		{name: "binding-and-close", bindingPanic: true, closePanic: true, want: ErrProtocol},
		{name: "send-error-and-close", closePanic: true, sendErr: ErrUnavailable, want: ErrSendAmbiguous},
	} {
		t.Run(test.name, func(t *testing.T) {
			var returned *trackedLifecyclePending
			manager := newConfiguredTrackedManager(t, func(envelope protocol.Envelope) PendingLiveReceipt {
				returned = &trackedLifecyclePending{
					binding: liveReceiptForEnvelope(envelope, [32]byte{1}), done: make(chan bool),
					bindingPanic: test.bindingPanic, closePanic: test.closePanic,
				}
				return returned
			}, test.sendErr)
			pending, err := manager.SendLiveTracked(context.Background(), managerEnvelope(t))
			if pending != nil || !errors.Is(err, test.want) {
				t.Fatalf("pending=%#v err=%v want=%v", pending, err, test.want)
			}
			if returned == nil || returned.closeCount() != 1 {
				t.Fatalf("pending cleanup=%#v", returned)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
