package cynapsagocore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type cancellationBoundRecoverer struct {
	started  chan struct{}
	returned chan struct{}
	once     sync.Once

	mu    sync.Mutex
	calls int
}

func (recoverer *cancellationBoundRecoverer) Recover(ctx context.Context, _ string) error {
	recoverer.mu.Lock()
	recoverer.calls++
	recoverer.mu.Unlock()
	recoverer.once.Do(func() { close(recoverer.started) })
	<-ctx.Done()
	close(recoverer.returned)
	return ctx.Err()
}

func (recoverer *cancellationBoundRecoverer) callCount() int {
	recoverer.mu.Lock()
	defer recoverer.mu.Unlock()
	return recoverer.calls
}

func TestRankedCarrierStopCancelsRecoveryAndWaitsPromptly(t *testing.T) {
	recoverer := &cancellationBoundRecoverer{
		started:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	carrier := newRankedEnvelopeCarrier(
		nil,
		recoverer,
		nil,
		time.Hour,
		time.Hour,
		transport.ClockFunc(time.Now),
	)
	peerID := "peer@example.test/mesh"
	carrier.trigger(peerID)
	select {
	case <-recoverer.started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}

	// A coalesced demand must not become another attempt after shutdown.
	carrier.trigger(peerID)
	carrier.stop()
	waited := make(chan struct{})
	go func() {
		carrier.wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("carrier wait consumed the recovery timeout after stop")
	}
	select {
	case <-recoverer.returned:
	default:
		t.Fatal("carrier wait returned before the cancelled recovery exited")
	}
	if calls := recoverer.callCount(); calls != 1 {
		t.Fatalf("Recover() calls=%d, want 1", calls)
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("cancelled recovery remained pending after wait")
	}

	carrier.trigger(peerID)
	if calls := recoverer.callCount(); calls != 1 {
		t.Fatalf("Recover() calls after stop=%d, want 1", calls)
	}
	carrier.stop()
}

type controlledFailureRecoverer struct {
	started  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

type failOnceRecoverer struct {
	mu      sync.Mutex
	calls   int
	succeed chan struct{}
}

func (recoverer *failOnceRecoverer) Recover(context.Context, string) error {
	recoverer.mu.Lock()
	recoverer.calls++
	call := recoverer.calls
	recoverer.mu.Unlock()
	if call == 1 {
		return transport.ErrUnavailable
	}
	close(recoverer.succeed)
	return nil
}

func (recoverer *failOnceRecoverer) callCount() int {
	recoverer.mu.Lock()
	defer recoverer.mu.Unlock()
	return recoverer.calls
}

func TestRankedCarrierRetriesSingleDemandOnceAfterTransientFailure(t *testing.T) {
	recoverer := &failOnceRecoverer{succeed: make(chan struct{})}
	carrier := newRankedEnvelopeCarrier(nil, recoverer, nil, time.Second, time.Millisecond, transport.ClockFunc(time.Now))
	peerID := "peer@example.test/mesh"
	carrier.trigger(peerID)
	select {
	case <-recoverer.succeed:
	case <-time.After(time.Second):
		t.Fatal("single recovery demand was not retried")
	}
	carrier.wait()
	if got := recoverer.callCount(); got != 2 {
		t.Fatalf("recovery calls=%d, want 2", got)
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("successful automatic retry remained pending")
	}
}

type countingFailureRecoverer struct {
	mu    sync.Mutex
	calls int
}

func (recoverer *countingFailureRecoverer) Recover(context.Context, string) error {
	recoverer.mu.Lock()
	recoverer.calls++
	recoverer.mu.Unlock()
	return transport.ErrUnavailable
}

func (recoverer *countingFailureRecoverer) callCount() int {
	recoverer.mu.Lock()
	defer recoverer.mu.Unlock()
	return recoverer.calls
}

func TestRankedCarrierBoundsAutomaticRetryForSingleDemand(t *testing.T) {
	recoverer := &countingFailureRecoverer{}
	carrier := newRankedEnvelopeCarrier(nil, recoverer, nil, time.Second, time.Millisecond, transport.ClockFunc(time.Now))
	peerID := "peer@example.test/mesh"
	carrier.trigger(peerID)
	carrier.wait()
	if got := recoverer.callCount(); got != 2 {
		t.Fatalf("recovery calls=%d, want one attempt and one automatic retry", got)
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("exhausted automatic retry remained pending")
	}
}

type oneSidedOpenRecoverer struct {
	mu      sync.Mutex
	calls   int
	live    *rootLiveCarrier
	healthy time.Time
}

func (recoverer *oneSidedOpenRecoverer) Recover(context.Context, string) error {
	recoverer.mu.Lock()
	recoverer.calls++
	call := recoverer.calls
	recoverer.mu.Unlock()
	if call == 2 {
		recoverer.live.mu.Lock()
		recoverer.live.state = transport.HealthHealthy
		recoverer.live.progress = recoverer.healthy
		recoverer.live.mu.Unlock()
	}
	return nil
}

func (recoverer *oneSidedOpenRecoverer) callCount() int {
	recoverer.mu.Lock()
	defer recoverer.mu.Unlock()
	return recoverer.calls
}

func TestRankedCarrierRetriesLocallyInstalledLinkWithoutPeerHealthEvidence(t *testing.T) {
	live, err := transport.NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := live.Close(context.Background()); closeErr != nil {
			t.Errorf("close live manager: %v", closeErr)
		}
	}()
	peerID := "peer@example.test/mesh"
	adapter := &rootLiveCarrier{state: transport.HealthConnecting}
	if err = live.InstallLive(context.Background(), peerID, adapter); err != nil {
		t.Fatal(err)
	}
	recoverer := &oneSidedOpenRecoverer{live: adapter, healthy: time.Now().UTC()}
	carrier := newRankedEnvelopeCarrier(live, recoverer, nil, 40*time.Millisecond, time.Millisecond, transport.ClockFunc(time.Now))
	carrier.trigger(peerID)
	carrier.wait()
	if got := recoverer.callCount(); got != 2 {
		t.Fatalf("recovery calls=%d, want one locally-open attempt and one automatic retry", got)
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("peer-confirmed retry remained pending")
	}
	if observation := live.ObservePeer(transport.KindLive, peerID); observation.State != transport.HealthHealthy || observation.LastProgress.IsZero() {
		t.Fatalf("final health=%#v, want authenticated peer progress", observation)
	}
	carrier.stop()
}

func (recoverer *controlledFailureRecoverer) Recover(context.Context, string) error {
	close(recoverer.started)
	<-recoverer.release
	close(recoverer.returned)
	return errors.New("recovery failed")
}

func TestRankedCarrierStopCancelsRecoveryBackoff(t *testing.T) {
	recoverer := &controlledFailureRecoverer{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	carrier := newRankedEnvelopeCarrier(
		nil,
		recoverer,
		nil,
		time.Hour,
		time.Hour,
		transport.ClockFunc(time.Now),
	)
	peerID := "peer@example.test/mesh"
	carrier.trigger(peerID)
	select {
	case <-recoverer.started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	carrier.trigger(peerID)
	close(recoverer.release)
	select {
	case <-recoverer.returned:
	case <-time.After(time.Second):
		t.Fatal("failed recovery did not return")
	}
	deadline := time.Now().Add(time.Second)
	for {
		carrier.mu.Lock()
		demand := carrier.pending[peerID]
		inBackoff := demand != nil && !demand.again
		carrier.mu.Unlock()
		if inBackoff {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("recovery did not enter retry backoff")
		}
		time.Sleep(time.Millisecond)
	}

	carrier.stop()
	waited := make(chan struct{})
	go func() {
		carrier.wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("carrier wait consumed the retry delay after stop")
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("cancelled recovery backoff remained pending after wait")
	}
}
