package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type qaBlockingSender struct {
	entered chan struct{}
	once    sync.Once
}

func (s *qaBlockingSender) Send(ctx context.Context, _ transport.Kind, _ protocol.Envelope) error {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func TestQAPeerWorkersIsolateBlockedNetworkOperations(t *testing.T) {
	blocked := &qaBlockingSender{entered: make(chan struct{})}
	fast := &testSender{}
	accept := func(context.Context, transport.Kind, protocol.Envelope) error { return nil }
	left := newTestWorker(t, blocked, nil, accept)
	right := newTestWorker(t, fast, nil, accept)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leftDone := make(chan error, 1)
	rightDone := make(chan error, 1)
	go func() { leftDone <- left.Run(ctx) }()
	go func() { rightDone <- right.Run(ctx) }()

	leftSend := make(chan error, 1)
	envelope := peerEnvelope(t, "a", "b")
	go func() { leftSend <- left.Send(ctx, envelope) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("blocked peer did not enter sender")
	}
	if err := right.Send(ctx, envelope); err != nil {
		t.Fatalf("independent peer was blocked: %v", err)
	}
	cancel()
	if err := <-leftSend; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked send error = %v", err)
	}
	<-leftDone
	<-rightDone
}

func TestQARejectedDurableInboundDoesNotTriggerRecovery(t *testing.T) {
	recoverer := &testRecoverer{}
	worker := newTestWorker(t, &testSender{}, recoverer, func(context.Context, transport.Kind, protocol.Envelope) error {
		return errors.New("rejected before trusted acceptance")
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if err := worker.ReceiveFrom(transport.KindDurable, peerEnvelope(t, "b", "a")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	recoverer.mu.Lock()
	calls := recoverer.calls
	recoverer.mu.Unlock()
	if calls != 0 {
		t.Fatalf("recovery calls after rejected inbound = %d", calls)
	}
	cancel()
	<-done
}

func TestQAContinuousDisconnectRequiresFullDemotionIntervalAfterProgress(t *testing.T) {
	worker := newTestWorker(t, &testSender{}, nil, func(context.Context, transport.Kind, protocol.Envelope) error { return nil })
	start := time.Unix(10_000, 0).UTC()
	worker.stateMu.Lock()
	worker.state.PreferredRank = RankLive
	worker.state.Health = HealthHealthy
	worker.stateMu.Unlock()

	worker.observeHealth(start, transport.Observation{State: transport.HealthDisconnected})
	worker.observeHealth(start.Add(20*time.Second), transport.Observation{State: transport.HealthDisconnected, LastProgress: start.Add(20 * time.Second)})
	worker.observeHealth(start.Add(49*time.Second), transport.Observation{State: transport.HealthDisconnected})
	if state := worker.Snapshot(); state.PreferredRank != RankLive || state.Health != HealthSuspicious {
		t.Fatalf("demoted before 30 seconds after latest progress: %#v", state)
	}
	worker.observeHealth(start.Add(50*time.Second), transport.Observation{State: transport.HealthDisconnected})
	if state := worker.Snapshot(); state.PreferredRank != RankDurable || state.Health != HealthDemoted {
		t.Fatalf("did not demote at continuous threshold: %#v", state)
	}
}

type qaGateRecoverer struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *qaGateRecoverer) Recover(ctx context.Context, _ string) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestQADurableRecoveryIsSingleFlightWhileInboundContinues(t *testing.T) {
	recoverer := &qaGateRecoverer{entered: make(chan struct{}), release: make(chan struct{})}
	accepted := make(chan struct{}, 2)
	worker := newTestWorker(t, &testSender{}, recoverer, func(context.Context, transport.Kind, protocol.Envelope) error {
		accepted <- struct{}{}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	envelope := peerEnvelope(t, "b", "a")
	if err := worker.ReceiveFrom(transport.KindDurable, envelope); err != nil {
		t.Fatal(err)
	}
	<-recoverer.entered
	if err := worker.ReceiveFrom(transport.KindDurable, envelope); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("inbound processing stalled behind recovery")
		}
	}
	recoverer.mu.Lock()
	calls := recoverer.calls
	recoverer.mu.Unlock()
	if calls != 1 {
		t.Fatalf("concurrent recovery calls = %d", calls)
	}
	close(recoverer.release)
	deadline := time.Now().Add(time.Second)
	for worker.Snapshot().RecoveryInFlight && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if state := worker.Snapshot(); state.RecoveryInFlight || state.PreferredRank != RankLive {
		t.Fatalf("recovery completion state = %#v", state)
	}
	cancel()
	<-done
}
