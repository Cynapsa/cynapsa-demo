package transport

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type qaDependencyTransport struct {
	kind     Kind
	startErr error
	sendErr  error
	closeErr error
}

func (t *qaDependencyTransport) Kind() Kind { return t.kind }
func (t *qaDependencyTransport) Start(context.Context) error {
	return t.startErr
}
func (t *qaDependencyTransport) Send(context.Context, protocol.Envelope) error {
	return t.sendErr
}
func (t *qaDependencyTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (t *qaDependencyTransport) Observe() Observation { return Observation{State: HealthHealthy} }
func (t *qaDependencyTransport) Close(context.Context) error {
	return t.closeErr
}

func TestQAManagerClassifiesDependencyErrorsByOperationWithoutCanary(t *testing.T) {
	const canary = "credential-canary-should-not-escape"
	t.Run("start", func(t *testing.T) {
		adapter := &qaDependencyTransport{kind: KindLive, startErr: errors.New(canary)}
		manager, err := NewManager(1, adapter)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.Start(context.Background())
		if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), canary) {
			t.Fatalf("start classification = %v", err)
		}
	})
	t.Run("send", func(t *testing.T) {
		adapter := &qaDependencyTransport{kind: KindLive, sendErr: errors.New(canary)}
		manager, err := NewManager(1, adapter)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		err = manager.Send(context.Background(), KindLive, managerEnvelope(t))
		if !errors.Is(err, ErrSendAmbiguous) || strings.Contains(err.Error(), canary) {
			t.Fatalf("send classification = %v", err)
		}
		_ = manager.Close(context.Background())
	})
	t.Run("close", func(t *testing.T) {
		adapter := &qaDependencyTransport{kind: KindLive, closeErr: errors.New(canary)}
		manager, err := NewManager(1, adapter)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		err = manager.Close(context.Background())
		if !errors.Is(err, ErrClosed) || strings.Contains(err.Error(), canary) {
			t.Fatalf("close classification = %v", err)
		}
	})
}

func TestQALiveStartupFailureLeavesAlwaysOnDurablePathRunning(t *testing.T) {
	durable := &fakeAdapter{kind: KindDurable, recv: make(chan protocol.Envelope)}
	live := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope), startErr: ErrUnavailable}
	manager, err := NewManager(2, durable, live)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("optional live path prevented durable startup: %v", err)
	}
	durable.mu.Lock()
	started, closed := durable.started, durable.closed
	durable.mu.Unlock()
	if !started || closed {
		t.Fatalf("durable path after live failure: started=%v closed=%v", started, closed)
	}
	_ = manager.Close(context.Background())
}

type qaBlockingStartTransport struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (t *qaBlockingStartTransport) Kind() Kind { return KindDurable }
func (t *qaBlockingStartTransport) Start(ctx context.Context) error {
	t.once.Do(func() { close(t.entered) })
	select {
	case <-t.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (t *qaBlockingStartTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (t *qaBlockingStartTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (t *qaBlockingStartTransport) Observe() Observation        { return Observation{State: HealthConnecting} }
func (t *qaBlockingStartTransport) Close(context.Context) error { return nil }

func TestQAManagerCloseDeadlineIsHonoredDuringInFlightStart(t *testing.T) {
	adapter := &qaBlockingStartTransport{entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- manager.Start(context.Background()) }()
	<-adapter.entered
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.Close(closeCtx) }()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) && err != nil {
			t.Fatalf("close error = %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		close(adapter.release)
		<-startResult
		<-closeResult
		t.Fatal("manager Close blocked on Start dependency beyond its deadline")
	}
	close(adapter.release)
	<-startResult
	_ = manager.Close(context.Background())
}
