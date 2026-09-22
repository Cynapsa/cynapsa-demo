package cynapsagocore

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestMessagingWorkerSupervisorRestartsOnlyFailedWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var primaryStarts atomic.Uint32
	var companionStarts atomic.Uint32
	var companionTicks atomic.Uint32
	restarted := make(chan struct{})
	companionStarted := make(chan struct{})
	tick := make(chan struct{})
	workers := []namedMessagingWorker{
		{name: "primary", run: func(runCtx context.Context) error {
			if primaryStarts.Add(1) == 1 {
				return errors.New("injected worker failure")
			}
			close(restarted)
			<-runCtx.Done()
			return runCtx.Err()
		}},
		{name: "companion", run: func(runCtx context.Context) error {
			companionStarts.Add(1)
			close(companionStarted)
			for {
				select {
				case <-tick:
					companionTicks.Add(1)
				case <-runCtx.Done():
					return runCtx.Err()
				}
			}
		}},
	}
	failures := make(chan string, 2)
	done := make(chan struct{})
	go func() {
		runMessagingWorkerSupervisor(ctx, time.Millisecond, workers, func(name string, err error) {
			if err == nil {
				name = "nil-error"
			}
			failures <- name
		})
		close(done)
	}()
	select {
	case <-companionStarted:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("companion worker did not start")
	}
	select {
	case <-restarted:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("worker supervisor did not replace the failed generation")
	}
	select {
	case name := <-failures:
		if name != "primary" {
			cancel()
			<-done
			t.Fatalf("worker failure = %q", name)
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("failed worker did not publish diagnostics")
	}
	tick <- struct{}{}
	waitMessagingLifecycleCondition(t, func() bool { return companionTicks.Load() == 1 })
	if primaryStarts.Load() != 2 || companionStarts.Load() != 1 {
		cancel()
		<-done
		t.Fatalf("worker starts primary=%d companion=%d", primaryStarts.Load(), companionStarts.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker supervisor did not join cancelled workers")
	}
}

func TestMessagingWorkerSupervisorCompanionTicksThroughRepeatedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var failingStarts atomic.Uint32
	var failingActive atomic.Int32
	var overlap atomic.Bool
	var diagnostics atomic.Uint32
	var companionStarts atomic.Uint32
	var companionTicks atomic.Uint32
	probe := make(chan chan struct{})
	workers := []namedMessagingWorker{
		{name: "flapping", run: func(context.Context) error {
			if failingActive.Add(1) != 1 {
				overlap.Store(true)
			}
			defer failingActive.Add(-1)
			failingStarts.Add(1)
			return errors.New("repeated injected failure")
		}},
		{name: "companion", run: func(runCtx context.Context) error {
			companionStarts.Add(1)
			for {
				select {
				case acknowledged := <-probe:
					companionTicks.Add(1)
					close(acknowledged)
				case <-runCtx.Done():
					return runCtx.Err()
				}
			}
		}},
	}
	done := make(chan struct{})
	go func() {
		runMessagingWorkerSupervisor(ctx, time.Millisecond, workers, func(name string, err error) {
			if name == "flapping" && err != nil {
				diagnostics.Add(1)
			}
		})
		close(done)
	}()
	waitMessagingLifecycleCondition(t, func() bool { return diagnostics.Load() >= 3 && companionStarts.Load() == 1 })
	probeMessagingCompanion(t, probe)
	waitMessagingLifecycleCondition(t, func() bool { return diagnostics.Load() >= 12 })
	probeMessagingCompanion(t, probe)
	if failingStarts.Load() < 12 || companionStarts.Load() != 1 || companionTicks.Load() != 2 || overlap.Load() {
		cancel()
		<-done
		t.Fatalf("failing/companion starts/ticks/overlap = %d/%d/%d/%t", failingStarts.Load(), companionStarts.Load(), companionTicks.Load(), overlap.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("repeated-failure supervisor did not join")
	}
}

func TestMessagingWorkerSupervisorGlobalCancellationJoinsAllWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const workerCount = 5
	started := make(chan struct{}, workerCount)
	joined := make(chan struct{}, workerCount)
	workers := make([]namedMessagingWorker, 0, workerCount)
	for index := 0; index < workerCount; index++ {
		workers = append(workers, namedMessagingWorker{name: fmt.Sprintf("cooperative-%d", index), run: func(runCtx context.Context) error {
			started <- struct{}{}
			defer func() { joined <- struct{}{} }()
			<-runCtx.Done()
			return runCtx.Err()
		}})
	}
	done := make(chan struct{})
	go func() {
		runMessagingWorkerSupervisor(ctx, time.Millisecond, workers, nil)
		close(done)
	}()
	for index := 0; index < workerCount; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatalf("worker %d did not start", index)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("top-level supervisor did not join cooperative workers")
	}
	if len(joined) != workerCount {
		t.Fatalf("joined workers=%d want=%d", len(joined), workerCount)
	}
}

func TestMessagingWorkerSupervisorRaceStressNeverOverlapsGenerations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const (
		flappingWorkers   = 8
		failedGenerations = 20
	)
	starts := make([]atomic.Uint32, flappingWorkers)
	active := make([]atomic.Int32, flappingWorkers)
	var overlap atomic.Bool
	var diagnostics atomic.Uint32
	workers := make([]namedMessagingWorker, 0, flappingWorkers+1)
	for index := 0; index < flappingWorkers; index++ {
		index := index
		workers = append(workers, namedMessagingWorker{name: fmt.Sprintf("stress-flapping-%d", index), run: func(runCtx context.Context) error {
			if active[index].Add(1) != 1 {
				overlap.Store(true)
			}
			defer active[index].Add(-1)
			generation := starts[index].Add(1)
			runtime.Gosched()
			if generation <= failedGenerations {
				return errors.New("stress failure")
			}
			<-runCtx.Done()
			return runCtx.Err()
		}})
	}
	var companionStarts atomic.Uint32
	var companionTicks atomic.Uint32
	workers = append(workers, namedMessagingWorker{name: "stress-companion", run: func(runCtx context.Context) error {
		companionStarts.Add(1)
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				companionTicks.Add(1)
			case <-runCtx.Done():
				return runCtx.Err()
			}
		}
	}})
	done := make(chan struct{})
	go func() {
		runMessagingWorkerSupervisor(ctx, time.Microsecond, workers, func(string, error) { diagnostics.Add(1) })
		close(done)
	}()
	waitMessagingLifecycleCondition(t, func() bool {
		if companionStarts.Load() != 1 || companionTicks.Load() < 5 {
			return false
		}
		for index := range starts {
			if starts[index].Load() < failedGenerations+1 {
				return false
			}
		}
		return true
	})
	if overlap.Load() || diagnostics.Load() < flappingWorkers*failedGenerations {
		cancel()
		<-done
		t.Fatalf("overlap=%t diagnostics=%d", overlap.Load(), diagnostics.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("race-stress supervisor did not join")
	}
	for index := range active {
		if active[index].Load() != 0 {
			t.Fatalf("worker %d remained active", index)
		}
	}
}

func probeMessagingCompanion(t *testing.T, probe chan<- chan struct{}) {
	t.Helper()
	acknowledged := make(chan struct{})
	select {
	case probe <- acknowledged:
	case <-time.After(time.Second):
		t.Fatal("companion did not accept tick probe")
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("companion did not process tick probe")
	}
}

func waitMessagingLifecycleCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for messaging worker lifecycle condition")
		}
		time.Sleep(time.Millisecond)
	}
}

type lifecycleErrorManagerTransport struct {
	mu     sync.Mutex
	closes int
	err    error
}

func (*lifecycleErrorManagerTransport) Kind() transport.Kind                          { return transport.KindDurable }
func (*lifecycleErrorManagerTransport) Start(context.Context) error                   { return nil }
func (*lifecycleErrorManagerTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*lifecycleErrorManagerTransport) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (*lifecycleErrorManagerTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (adapter *lifecycleErrorManagerTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	adapter.mu.Unlock()
	if adapter.err != nil {
		return adapter.err
	}
	return errors.New("private cleanup canary")
}
func (adapter *lifecycleErrorManagerTransport) closeCount() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.closes
}

type lifecycleBlockedManagerTransport struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu     sync.Mutex
	closes int
}

func (*lifecycleBlockedManagerTransport) Kind() transport.Kind                          { return transport.KindDurable }
func (*lifecycleBlockedManagerTransport) Start(context.Context) error                   { return nil }
func (*lifecycleBlockedManagerTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*lifecycleBlockedManagerTransport) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (*lifecycleBlockedManagerTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (adapter *lifecycleBlockedManagerTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	adapter.mu.Unlock()
	adapter.once.Do(func() { close(adapter.entered) })
	<-adapter.release
	return nil
}
func (adapter *lifecycleBlockedManagerTransport) closeCount() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.closes
}

func TestAuthenticatedMessagingShutdownJoinsManagerLifecycleCleanup(t *testing.T) {
	adapter := &lifecycleBlockedManagerTransport{entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := transport.NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := newAuthenticatedMessagingService(nil, nil, &rank1MessagingRuntime{live: manager}, nil)
	released := false
	defer func() {
		if !released {
			close(adapter.release)
		}
	}()

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	firstResult := make(chan *sessionkernel.ProviderError, 1)
	go func() { firstResult <- service.Shutdown(first) }()
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("Manager adapter Close was not entered")
	}
	if failure := <-firstResult; failure == nil || failure.Code != sessionkernel.ProviderDeadline {
		t.Fatalf("deadline Shutdown = %#v", failure)
	}
	joined := make(chan *sessionkernel.ProviderError, 1)
	go func() { joined <- service.Shutdown(context.Background()) }()
	select {
	case failure := <-joined:
		t.Fatalf("terminal Shutdown returned before Manager join: %#v", failure)
	case <-time.After(50 * time.Millisecond):
	}

	close(adapter.release)
	released = true
	if failure := <-joined; failure != nil {
		t.Fatalf("joined Shutdown = %#v", failure)
	}
	if closes := adapter.closeCount(); closes != 1 {
		t.Fatalf("Manager adapter Close calls = %d", closes)
	}
}

func TestAuthenticatedMessagingShutdownReturnsSharedCleanupFailure(t *testing.T) {
	adapter := &lifecycleErrorManagerTransport{}
	manager, err := transport.NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := newAuthenticatedMessagingService(nil, nil, &rank1MessagingRuntime{live: manager}, nil)
	first := service.Shutdown(context.Background())
	second := service.Shutdown(context.Background())
	if first == nil || first.Code != sessionkernel.ProviderInternal || second == nil || second.Code != first.Code {
		t.Fatalf("cleanup results: first=%#v second=%#v", first, second)
	}
	if closes := adapter.closeCount(); closes != 1 {
		t.Fatalf("adapter Close calls = %d", closes)
	}
}

func TestAuthenticatedMessagingShutdownKeepsOperationalClosedAsFailure(t *testing.T) {
	adapter := &lifecycleErrorManagerTransport{err: transport.ErrClosed}
	manager, err := transport.NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := newAuthenticatedMessagingService(nil, nil, &rank1MessagingRuntime{live: manager}, nil)
	first := service.Shutdown(context.Background())
	second := service.Shutdown(context.Background())
	if first == nil || first.Code != sessionkernel.ProviderInternal || second == nil || second.Code != first.Code {
		t.Fatalf("operational ErrClosed was globally weakened: first=%#v second=%#v", first, second)
	}
	if closes := adapter.closeCount(); closes != 1 {
		t.Fatalf("adapter Close calls=%d", closes)
	}
}
