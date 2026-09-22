package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type lifecycleCloseTransport struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	closes   int
	discards int
}

type lifecycleSendTransport struct {
	entered chan struct{}
	release chan struct{}

	mu       sync.Mutex
	discards int
}

type lifecycleInstallTransport struct {
	entered chan struct{}
	release chan struct{}

	mu     sync.Mutex
	closes int
}

type lifecycleDetachedTransport struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type ownershipTransport struct {
	mu         sync.Mutex
	closes     int
	discards   int
	panicFirst bool
}

type lifecycleBorrowTransport struct {
	sendEntered    chan struct{}
	sendRelease    chan struct{}
	observeEntered chan struct{}
	observeRelease chan struct{}
	closeEntered   chan struct{}
	sendOnce       sync.Once
	observeOnce    sync.Once
	closeOnce      sync.Once
}

type lifecycleCloseUnblocksReceiveTransport struct {
	receiveEntered chan struct{}
	receiveRelease chan struct{}
	receiveOnce    sync.Once
	closeOnce      sync.Once
}

type partialStartOwnershipTransport struct {
	mu        sync.Mutex
	kind      Kind
	startMode string
	closeMode string
	starts    int
	closes    int
	discards  int
}

type preStartObserveTransport struct {
	startEntered chan struct{}
	startRelease chan struct{}
	startOnce    sync.Once
	mu           sync.Mutex
	observes     int
	closes       int
}

type installFailureOwnershipTransport struct {
	mu           sync.Mutex
	startMode    string
	closeMode    string
	startEntered chan struct{}
	startOnce    sync.Once
	starts       int
	closes       int
	discards     int
	running      bool
}

func (*installFailureOwnershipTransport) Kind() Kind { return KindLive }
func (adapter *installFailureOwnershipTransport) Start(ctx context.Context) error {
	adapter.mu.Lock()
	adapter.starts++
	adapter.running = true
	mode := adapter.startMode
	adapter.mu.Unlock()
	switch mode {
	case "failure":
		return errors.New("private install start failure")
	case "panic":
		panic("private install start panic")
	case "cancel":
		adapter.startOnce.Do(func() { close(adapter.startEntered) })
		<-ctx.Done()
		return ctx.Err()
	default:
		return nil
	}
}
func (*installFailureOwnershipTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*installFailureOwnershipTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (*installFailureOwnershipTransport) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (adapter *installFailureOwnershipTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	call, mode := adapter.closes, adapter.closeMode
	if mode == "" || call > 1 {
		adapter.running = false
	}
	adapter.mu.Unlock()
	if call == 1 {
		switch mode {
		case "failure":
			return errors.New("private install close failure")
		case "panic":
			panic("private install close panic")
		}
	}
	return nil
}
func (adapter *installFailureOwnershipTransport) DiscardShutdownOwned() {
	adapter.mu.Lock()
	adapter.discards++
	adapter.running = false
	adapter.mu.Unlock()
}
func (adapter *installFailureOwnershipTransport) setStartMode(mode string) {
	adapter.mu.Lock()
	adapter.startMode = mode
	adapter.mu.Unlock()
}
func (adapter *installFailureOwnershipTransport) counts() (int, int, int, bool) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.starts, adapter.closes, adapter.discards, adapter.running
}

type retainedInboundOwnershipTransport struct {
	mu       sync.Mutex
	envelope protocol.Envelope
	retained []byte
	received chan struct{}
	drain    chan struct{}
	once     sync.Once
	closes   int
	discards int
}

type boundOwnershipTransport struct {
	*ownershipTransport
}

func (*retainedInboundOwnershipTransport) Kind() Kind                  { return KindLive }
func (*retainedInboundOwnershipTransport) Start(context.Context) error { return nil }
func (*retainedInboundOwnershipTransport) Send(context.Context, protocol.Envelope) error {
	return nil
}
func (adapter *retainedInboundOwnershipTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	delivered := false
	adapter.once.Do(func() {
		delivered = true
		close(adapter.received)
	})
	if delivered {
		return adapter.envelope.Clone(), nil
	}
	select {
	case <-adapter.drain:
		return protocol.Envelope{}, ErrClosed
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	}
}
func (adapter *retainedInboundOwnershipTransport) ReceiveAuthenticated(ctx context.Context) (AuthenticatedReceived, error) {
	envelope, err := adapter.Receive(ctx)
	return AuthenticatedReceived{
		Envelope:       envelope,
		Authentication: LiveAuthentication{Peer: envelope.Sender, MeshID: envelope.MeshID},
	}, err
}
func (*retainedInboundOwnershipTransport) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (adapter *retainedInboundOwnershipTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	adapter.mu.Unlock()
	return nil
}
func (adapter *retainedInboundOwnershipTransport) DiscardShutdownOwned() {
	adapter.mu.Lock()
	clear(adapter.retained)
	adapter.discards++
	adapter.mu.Unlock()
}
func (adapter *retainedInboundOwnershipTransport) state() (int, int, bool) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	nonzero := false
	for _, value := range adapter.retained {
		nonzero = nonzero || value != 0
	}
	return adapter.closes, adapter.discards, nonzero
}

func (*preStartObserveTransport) Kind() Kind { return KindDurable }
func (adapter *preStartObserveTransport) Start(context.Context) error {
	adapter.startOnce.Do(func() { close(adapter.startEntered) })
	<-adapter.startRelease
	return errors.New("private start failure")
}
func (*preStartObserveTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*preStartObserveTransport) Receive(context.Context) (protocol.Envelope, error) {
	return protocol.Envelope{}, ErrClosed
}
func (adapter *preStartObserveTransport) Observe() Observation {
	adapter.mu.Lock()
	adapter.observes++
	adapter.mu.Unlock()
	return Observation{State: HealthHealthy}
}
func (adapter *preStartObserveTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	adapter.mu.Unlock()
	return nil
}
func (adapter *preStartObserveTransport) counts() (int, int) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.observes, adapter.closes
}

func (adapter *partialStartOwnershipTransport) Kind() Kind { return adapter.kind }
func (adapter *partialStartOwnershipTransport) Start(context.Context) error {
	adapter.mu.Lock()
	adapter.starts++
	mode := adapter.startMode
	adapter.mu.Unlock()
	switch mode {
	case "failure":
		return errors.New("private start failure")
	case "panic":
		panic("private start panic")
	case "cancel":
		return context.Canceled
	default:
		return nil
	}
}
func (*partialStartOwnershipTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*partialStartOwnershipTransport) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (*partialStartOwnershipTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (adapter *partialStartOwnershipTransport) Close(context.Context) error {
	adapter.mu.Lock()
	adapter.closes++
	call, mode := adapter.closes, adapter.closeMode
	adapter.mu.Unlock()
	if call == 1 {
		switch mode {
		case "failure":
			return errors.New("private close failure")
		case "panic":
			panic("private close panic")
		}
	}
	return nil
}
func (adapter *partialStartOwnershipTransport) DiscardShutdownOwned() {
	adapter.mu.Lock()
	adapter.discards++
	adapter.mu.Unlock()
}
func (adapter *partialStartOwnershipTransport) counts() (int, int, int) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.starts, adapter.closes, adapter.discards
}

func (*lifecycleCloseUnblocksReceiveTransport) Kind() Kind                  { return KindDurable }
func (*lifecycleCloseUnblocksReceiveTransport) Start(context.Context) error { return nil }
func (*lifecycleCloseUnblocksReceiveTransport) Send(context.Context, protocol.Envelope) error {
	return nil
}
func (*lifecycleCloseUnblocksReceiveTransport) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (adapter *lifecycleCloseUnblocksReceiveTransport) Receive(context.Context) (protocol.Envelope, error) {
	adapter.receiveOnce.Do(func() { close(adapter.receiveEntered) })
	<-adapter.receiveRelease
	return protocol.Envelope{}, ErrClosed
}
func (adapter *lifecycleCloseUnblocksReceiveTransport) Close(context.Context) error {
	adapter.closeOnce.Do(func() { close(adapter.receiveRelease) })
	return nil
}

func (*lifecycleBorrowTransport) Kind() Kind                  { return KindDurable }
func (*lifecycleBorrowTransport) Start(context.Context) error { return nil }
func (adapter *lifecycleBorrowTransport) Send(context.Context, protocol.Envelope) error {
	adapter.sendOnce.Do(func() { close(adapter.sendEntered) })
	<-adapter.sendRelease
	return nil
}
func (adapter *lifecycleBorrowTransport) Observe() Observation {
	adapter.observeOnce.Do(func() { close(adapter.observeEntered) })
	<-adapter.observeRelease
	return Observation{State: HealthHealthy}
}
func (*lifecycleBorrowTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (adapter *lifecycleBorrowTransport) Close(context.Context) error {
	adapter.closeOnce.Do(func() { close(adapter.closeEntered) })
	return nil
}

func (*ownershipTransport) Kind() Kind                                    { return KindLive }
func (*ownershipTransport) Start(context.Context) error                   { return nil }
func (*ownershipTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*ownershipTransport) Observe() Observation                          { return Observation{State: HealthHealthy} }
func (*ownershipTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (transport *ownershipTransport) Close(context.Context) error {
	transport.mu.Lock()
	transport.closes++
	call := transport.closes
	panicFirst := transport.panicFirst
	transport.mu.Unlock()
	if panicFirst && call == 1 {
		panic("first close")
	}
	return nil
}
func (transport *ownershipTransport) DiscardShutdownOwned() {
	transport.mu.Lock()
	transport.discards++
	transport.mu.Unlock()
}
func (transport *ownershipTransport) counts() (int, int) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.closes, transport.discards
}

type nonComparableTransport struct{ retained []byte }

func (nonComparableTransport) Kind() Kind                                    { return KindLive }
func (nonComparableTransport) Start(context.Context) error                   { return nil }
func (nonComparableTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (nonComparableTransport) Receive(context.Context) (protocol.Envelope, error) {
	return protocol.Envelope{}, ErrClosed
}
func (nonComparableTransport) Observe() Observation        { return Observation{State: HealthHealthy} }
func (nonComparableTransport) Close(context.Context) error { return nil }

func (*lifecycleDetachedTransport) Kind() Kind                                    { return KindLive }
func (*lifecycleDetachedTransport) Start(context.Context) error                   { return nil }
func (*lifecycleDetachedTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*lifecycleDetachedTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (*lifecycleDetachedTransport) Observe() Observation { return Observation{State: HealthHealthy} }
func (transport *lifecycleDetachedTransport) Close(context.Context) error {
	transport.once.Do(func() { close(transport.entered) })
	<-transport.release
	return nil
}

func (*lifecycleInstallTransport) Kind() Kind { return KindLive }
func (transport *lifecycleInstallTransport) Start(context.Context) error {
	close(transport.entered)
	<-transport.release
	return nil
}
func (*lifecycleInstallTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*lifecycleInstallTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (*lifecycleInstallTransport) Observe() Observation { return Observation{State: HealthHealthy} }
func (transport *lifecycleInstallTransport) Close(context.Context) error {
	transport.mu.Lock()
	transport.closes++
	transport.mu.Unlock()
	return nil
}
func (transport *lifecycleInstallTransport) closeCount() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.closes
}

func (*lifecycleSendTransport) Kind() Kind                  { return KindDurable }
func (*lifecycleSendTransport) Start(context.Context) error { return nil }
func (transport *lifecycleSendTransport) Send(context.Context, protocol.Envelope) error {
	close(transport.entered)
	<-transport.release
	return nil
}
func (*lifecycleSendTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (*lifecycleSendTransport) Observe() Observation        { return Observation{State: HealthHealthy} }
func (*lifecycleSendTransport) Close(context.Context) error { return nil }
func (transport *lifecycleSendTransport) DiscardShutdownOwned() {
	transport.mu.Lock()
	transport.discards++
	transport.mu.Unlock()
}
func (transport *lifecycleSendTransport) discardCount() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.discards
}

func (*lifecycleCloseTransport) Kind() Kind                                    { return KindDurable }
func (*lifecycleCloseTransport) Start(context.Context) error                   { return nil }
func (*lifecycleCloseTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*lifecycleCloseTransport) Observe() Observation {
	return Observation{State: HealthHealthy}
}
func (*lifecycleCloseTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (transport *lifecycleCloseTransport) Close(context.Context) error {
	transport.mu.Lock()
	transport.closes++
	transport.mu.Unlock()
	transport.once.Do(func() { close(transport.entered) })
	<-transport.release
	return nil
}
func (transport *lifecycleCloseTransport) DiscardShutdownOwned() {
	transport.mu.Lock()
	transport.discards++
	transport.mu.Unlock()
}
func (transport *lifecycleCloseTransport) counts() (int, int) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.closes, transport.discards
}

func TestManagerCloseDeadlineJoinsOneLifecycleCleanup(t *testing.T) {
	adapter := &lifecycleCloseTransport{entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() { firstResult <- manager.Close(first) }()
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("dependency Close was not entered")
	}
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close = %v", err)
	}

	second, cancelSecond := context.WithCancel(context.Background())
	cancelSecond()
	if err := manager.Close(second); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Close = %v", err)
	}
	if closes, discards := adapter.counts(); closes != 1 || discards != 0 {
		t.Fatalf("cleanup before release: closes=%d discards=%d", closes, discards)
	}

	close(adapter.release)
	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("joined Close = %v", err)
	}
	if closes, discards := adapter.counts(); closes != 1 || discards != 1 {
		t.Fatalf("final cleanup: closes=%d discards=%d", closes, discards)
	}
}

func TestManagerCloseRetainsAdapterThroughNonCooperativeSend(t *testing.T) {
	adapter := &lifecycleSendTransport{entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	sendResult := make(chan error, 1)
	go func() { sendResult <- manager.Send(context.Background(), KindDurable, envelope) }()
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("dependency Send was not entered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := manager.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close during Send = %v", err)
	}
	if discards := adapter.discardCount(); discards != 0 {
		t.Fatalf("adapter discarded while Send borrowed it: %d", discards)
	}
	close(adapter.release)
	if err := <-sendResult; err != nil {
		t.Fatalf("Send = %v", err)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("joined Close = %v", err)
	}
	if discards := adapter.discardCount(); discards != 1 {
		t.Fatalf("final discard count = %d", discards)
	}
}

func TestManagerCloseJoinsUnpublishedInstallLiveStart(t *testing.T) {
	manager, err := NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter := &lifecycleInstallTransport{entered: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(adapter.release)
		}
	}()
	installResult := make(chan error, 1)
	go func() { installResult <- manager.InstallLive(context.Background(), "peer/mesh", adapter) }()
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("InstallLive adapter Start was not entered")
	}

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	if err := manager.Close(first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close during InstallLive = %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- manager.Close(context.Background()) }()
	select {
	case err := <-joined:
		t.Fatalf("Close completed before unpublished Start joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(adapter.release)
	released = true
	if err := <-installResult; !errors.Is(err, ErrClosed) {
		t.Fatalf("InstallLive after Close = %v", err)
	}
	if err := <-joined; err != nil {
		t.Fatalf("joined Close = %v", err)
	}
	if closes := adapter.closeCount(); closes != 1 {
		t.Fatalf("unpublished adapter Close calls = %d", closes)
	}
}

func TestManagerCloseJoinsDetachedLifecycleOperations(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Manager, Transport) error
	}{
		{name: "remove", run: func(manager *Manager, _ Transport) error {
			return manager.RemoveLive(context.Background(), "peer/mesh")
		}},
		{name: "remove-if", run: func(manager *Manager, adapter Transport) error {
			return manager.RemoveLiveIf(context.Background(), "peer/mesh", adapter)
		}},
		{name: "retire-all", run: func(manager *Manager, _ Transport) error {
			return manager.RetireAllLive(context.Background())
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, err := NewLiveManager(1)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			adapter := &lifecycleDetachedTransport{entered: make(chan struct{}), release: make(chan struct{})}
			if err := manager.InstallLive(context.Background(), "peer/mesh", adapter); err != nil {
				t.Fatal(err)
			}
			released := false
			defer func() {
				if !released {
					close(adapter.release)
				}
			}()
			operation := make(chan error, 1)
			go func() { operation <- test.run(manager, adapter) }()
			select {
			case <-adapter.entered:
			case <-time.After(time.Second):
				t.Fatal("detached dependency Close was not entered")
			}
			closed := make(chan error, 1)
			go func() { closed <- manager.Close(context.Background()) }()
			select {
			case err := <-closed:
				t.Fatalf("Manager Close returned before detached operation: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(adapter.release)
			released = true
			if err := <-operation; err != nil {
				t.Fatalf("detached operation = %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("Manager Close = %v", err)
			}
			if err := test.run(manager, adapter); !errors.Is(err, ErrClosed) {
				t.Fatalf("operation after Close = %v", err)
			}
		})
	}
}

func TestManagerRejectsDuplicateAndNonPointerAdapterOwnership(t *testing.T) {
	if _, err := NewManager(1, nonComparableTransport{retained: []byte("owned")}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-pointer constructor adapter = %v", err)
	}
	constructorDuplicate := &ownershipTransport{}
	if _, err := NewManager(1, constructorDuplicate, constructorDuplicate); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate constructor adapter = %v", err)
	}
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallLive(context.Background(), "value/mesh", nonComparableTransport{retained: []byte("owned")}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-pointer InstallLive = %v", err)
	}
	adapter := &ownershipTransport{}
	if err := manager.InstallLive(context.Background(), "first/mesh", adapter); err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallLive(context.Background(), "second/mesh", adapter); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate InstallLive = %v", err)
	}
	if err := manager.RemoveLiveIf(context.Background(), "first/mesh", nonComparableTransport{retained: []byte("untrusted")}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-comparable expected RemoveLiveIf = %v", err)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closes, discards := adapter.counts(); closes != 1 || discards != 1 {
		t.Fatalf("duplicate ownership cleanup: closes=%d discards=%d", closes, discards)
	}
}

func TestManagerReplacementRetainsFailedOldCloseForFinalCleanup(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := &ownershipTransport{panicFirst: true}
	fresh := &ownershipTransport{}
	if err := manager.InstallLive(context.Background(), "peer/mesh", old); err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallLive(context.Background(), "peer/mesh", fresh); err != nil {
		t.Fatalf("replacement = %v", err)
	}
	if closes, discards := old.counts(); closes != 1 || discards != 0 {
		t.Fatalf("old after replacement: closes=%d discards=%d", closes, discards)
	}
	if err := manager.InstallLive(context.Background(), "other/mesh", old); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("quarantined duplicate InstallLive = %v", err)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Manager Close = %v", err)
	}
	if closes, discards := old.counts(); closes != 2 || discards != 1 {
		t.Fatalf("old final cleanup: closes=%d discards=%d", closes, discards)
	}
	if closes, discards := fresh.counts(); closes != 1 || discards != 1 {
		t.Fatalf("fresh final cleanup: closes=%d discards=%d", closes, discards)
	}
}

func TestManagerCloseJoinsDependencyBorrowersBeforeAdapterClose(t *testing.T) {
	for _, operation := range []string{"send", "observe"} {
		t.Run(operation, func(t *testing.T) {
			adapter := &lifecycleBorrowTransport{
				sendEntered: make(chan struct{}), sendRelease: make(chan struct{}),
				observeEntered: make(chan struct{}), observeRelease: make(chan struct{}),
				closeEntered: make(chan struct{}),
			}
			manager, err := NewManager(1, adapter)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			operationDone := make(chan error, 1)
			var entered, release chan struct{}
			switch operation {
			case "send":
				entered, release = adapter.sendEntered, adapter.sendRelease
				envelope := managerEnvelope(t)
				go func() { operationDone <- manager.Send(context.Background(), KindDurable, envelope) }()
			case "observe":
				entered, release = adapter.observeEntered, adapter.observeRelease
				go func() {
					observation := manager.Observe(KindDurable)
					if observation.State != HealthHealthy {
						operationDone <- errors.New("unexpected observation")
						return
					}
					operationDone <- nil
				}()
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("dependency operation was not entered")
			}
			closed := make(chan error, 1)
			go func() { closed <- manager.Close(context.Background()) }()
			select {
			case <-adapter.closeEntered:
				close(release)
				t.Fatal("adapter closed while dependency operation still borrowed it")
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-operationDone; err != nil {
				t.Fatalf("dependency operation = %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("Close = %v", err)
			}
		})
	}
}

func TestManagerLifecycleAdmissionSeamProtectsFutureReceiptBorrow(t *testing.T) {
	adapter := &lifecycleBorrowTransport{
		sendEntered: make(chan struct{}), sendRelease: make(chan struct{}),
		observeEntered: make(chan struct{}), observeRelease: make(chan struct{}),
		closeEntered: make(chan struct{}),
	}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	admitted := manager.admitOperationLocked()
	manager.mu.Unlock()
	if !admitted {
		t.Fatal("future receipt dependency operation was not admitted")
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close(context.Background()) }()
	select {
	case <-adapter.closeEntered:
		manager.finishOperation()
		t.Fatal("adapter closed while future receipt operation still borrowed it")
	case <-time.After(50 * time.Millisecond):
	}
	manager.finishOperation()
	if err := <-closed; err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestManagerAdapterCloseUnblocksReceiveBeforeWorkerJoin(t *testing.T) {
	adapter := &lifecycleCloseUnblocksReceiveTransport{receiveEntered: make(chan struct{}), receiveRelease: make(chan struct{})}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.receiveEntered:
	case <-time.After(time.Second):
		t.Fatal("Receive was not entered")
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock and join Receive")
	}
}

func TestManagerPartialStartOwnershipIsTerminalizedExactlyOnce(t *testing.T) {
	tests := []struct {
		name              string
		durableMode       string
		liveMode          string
		withoutDurable    bool
		wantStartError    bool
		wantDurableStarts int
		wantLiveStarts    int
		wantDurableEarly  int
		wantLiveEarly     int
	}{
		{name: "both-success", wantDurableStarts: 1, wantLiveStarts: 1},
		{name: "durable-failure-live-unattempted", durableMode: "failure", wantStartError: true, wantDurableStarts: 1, wantDurableEarly: 1, wantLiveEarly: 1},
		{name: "durable-panic-live-unattempted", durableMode: "panic", wantStartError: true, wantDurableStarts: 1, wantDurableEarly: 1, wantLiveEarly: 1},
		{name: "durable-cancel-live-unattempted", durableMode: "cancel", wantStartError: true, wantDurableStarts: 1, wantDurableEarly: 1, wantLiveEarly: 1},
		{name: "optional-live-failure", liveMode: "failure", wantDurableStarts: 1, wantLiveStarts: 1, wantLiveEarly: 1},
		{name: "optional-live-panic", liveMode: "panic", wantDurableStarts: 1, wantLiveStarts: 1, wantLiveEarly: 1},
		{name: "optional-live-cancel", liveMode: "cancel", wantDurableStarts: 1, wantLiveStarts: 1, wantLiveEarly: 1},
		{name: "only-live-failure", liveMode: "failure", withoutDurable: true, wantStartError: true, wantLiveStarts: 1, wantLiveEarly: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durable := &partialStartOwnershipTransport{kind: KindDurable, startMode: test.durableMode}
			live := &partialStartOwnershipTransport{kind: KindLive, startMode: test.liveMode}
			adapters := []Transport{durable, live}
			if test.withoutDurable {
				adapters = []Transport{live}
			}
			manager, err := NewManager(1, adapters...)
			if err != nil {
				t.Fatal(err)
			}
			first := manager.Start(context.Background())
			if (first != nil) != test.wantStartError {
				t.Fatalf("Start = %v, want error=%v", first, test.wantStartError)
			}
			durableStarts, durableCloses, _ := durable.counts()
			liveStarts, liveCloses, _ := live.counts()
			if test.withoutDurable {
				durableStarts, durableCloses = 0, 0
			}
			if durableStarts != test.wantDurableStarts || liveStarts != test.wantLiveStarts || durableCloses != test.wantDurableEarly || liveCloses != test.wantLiveEarly {
				t.Fatalf("after Start: durable=(%d,%d) live=(%d,%d)", durableStarts, durableCloses, liveStarts, liveCloses)
			}
			second := manager.Start(context.Background())
			if (second != nil) != test.wantStartError {
				t.Fatalf("second Start = %v, want error=%v", second, test.wantStartError)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("Close = %v", err)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("repeated Close = %v", err)
			}
			durableStarts, durableCloses, durableDiscards := durable.counts()
			liveStarts, liveCloses, liveDiscards := live.counts()
			if test.withoutDurable {
				durableStarts, durableCloses, durableDiscards = 0, 0, 0
			}
			if durableStarts != test.wantDurableStarts || liveStarts != test.wantLiveStarts {
				t.Fatalf("Start calls changed: durable=%d live=%d", durableStarts, liveStarts)
			}
			wantDurableCloses := 1
			wantDurableDiscards := 1
			if test.withoutDurable {
				wantDurableCloses, wantDurableDiscards = 0, 0
			}
			if durableCloses != wantDurableCloses || durableDiscards != wantDurableDiscards || liveCloses != 1 || liveDiscards != 1 {
				t.Fatalf("terminal ownership: durable=(%d,%d) live=(%d,%d)", durableCloses, durableDiscards, liveCloses, liveDiscards)
			}
		})
	}
}

func TestManagerPartialStartCloseFailureIsRetriedOnlyByTerminalCleanup(t *testing.T) {
	for _, closeMode := range []string{"failure", "panic"} {
		t.Run(closeMode, func(t *testing.T) {
			durable := &partialStartOwnershipTransport{kind: KindDurable, startMode: "failure"}
			live := &partialStartOwnershipTransport{kind: KindLive, closeMode: closeMode}
			manager, err := NewManager(1, durable, live)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err == nil {
				t.Fatal("Start unexpectedly succeeded")
			}
			if _, closes, discards := live.counts(); closes != 1 || discards != 0 {
				t.Fatalf("partial cleanup: closes=%d discards=%d", closes, discards)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("Close retry = %v", err)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("repeated Close = %v", err)
			}
			if _, closes, discards := live.counts(); closes != 2 || discards != 1 {
				t.Fatalf("terminal retry: closes=%d discards=%d", closes, discards)
			}
		})
	}
}

func TestManagerObserveRequiresPublishedStartBeforeDependencyBorrow(t *testing.T) {
	for _, closeWinner := range []bool{false, true} {
		name := "partial-start-cleanup"
		if closeWinner {
			name = "close-winner"
		}
		t.Run(name, func(t *testing.T) {
			adapter := &preStartObserveTransport{startEntered: make(chan struct{}), startRelease: make(chan struct{})}
			manager, err := NewManager(1, adapter)
			if err != nil {
				t.Fatal(err)
			}
			if observation := manager.Observe(KindDurable); observation.State != HealthUnknown {
				t.Fatalf("pre-Start Observe = %#v", observation)
			}
			started := make(chan error, 1)
			go func() { started <- manager.Start(context.Background()) }()
			select {
			case <-adapter.startEntered:
			case <-time.After(time.Second):
				t.Fatal("Start was not entered")
			}
			if observation := manager.Observe(KindDurable); observation.State != HealthUnknown {
				t.Fatalf("in-progress Start Observe = %#v", observation)
			}
			var closed chan error
			if closeWinner {
				closed = make(chan error, 1)
				go func() { closed <- manager.Close(context.Background()) }()
				for deadline := time.Now().Add(time.Second); ; {
					if observation := manager.Observe(KindDurable); observation.State == HealthClosed {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("Close did not fence Observe")
					}
					time.Sleep(time.Millisecond)
				}
			}
			close(adapter.startRelease)
			if err := <-started; err == nil {
				t.Fatal("Start unexpectedly succeeded")
			}
			if closeWinner {
				if err := <-closed; err != nil {
					t.Fatalf("Close = %v", err)
				}
			} else if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("Close = %v", err)
			}
			if observes, closes := adapter.counts(); observes != 0 || closes != 1 {
				t.Fatalf("dependency calls: observes=%d closes=%d", observes, closes)
			}
		})
	}
}

func TestManagerInstallLiveFailureTerminallyClosesUnpublishedAdapter(t *testing.T) {
	for _, startMode := range []string{"failure", "panic"} {
		t.Run(startMode, func(t *testing.T) {
			manager, err := NewLiveManager(1)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			adapter := &installFailureOwnershipTransport{startMode: startMode}
			if err := manager.InstallLive(context.Background(), "peer/mesh", adapter); err == nil {
				t.Fatal("InstallLive unexpectedly succeeded")
			}
			if starts, closes, discards, running := adapter.counts(); starts != 1 || closes != 1 || discards != 0 || running {
				t.Fatalf("failed install ownership: starts=%d closes=%d discards=%d running=%v", starts, closes, discards, running)
			}
			adapter.setStartMode("")
			if err := manager.InstallLive(context.Background(), "peer/mesh", adapter); err != nil {
				t.Fatalf("released adapter reinstall = %v", err)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if starts, closes, discards, running := adapter.counts(); starts != 2 || closes != 2 || discards != 1 || running {
				t.Fatalf("terminal ownership: starts=%d closes=%d discards=%d running=%v", starts, closes, discards, running)
			}
		})
	}
}

func TestManagerInstallLiveCloseFailureQuarantinesUnpublishedAdapter(t *testing.T) {
	for _, closeMode := range []string{"failure", "panic"} {
		t.Run(closeMode, func(t *testing.T) {
			manager, err := NewLiveManager(1)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			adapter := &installFailureOwnershipTransport{startMode: "failure", closeMode: closeMode}
			if err := manager.InstallLive(context.Background(), "peer/mesh", adapter); err == nil {
				t.Fatal("InstallLive unexpectedly succeeded")
			}
			if err := manager.InstallLive(context.Background(), "other/mesh", adapter); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("quarantined reinstall = %v", err)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatalf("Close retry = %v", err)
			}
			if starts, closes, discards, running := adapter.counts(); starts != 1 || closes != 2 || discards != 1 || running {
				t.Fatalf("quarantine cleanup: starts=%d closes=%d discards=%d running=%v", starts, closes, discards, running)
			}
		})
	}
}

func TestManagerCloseCancelsAndJoinsUnpublishedInstallFailureCleanup(t *testing.T) {
	manager, err := NewLiveManager(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter := &installFailureOwnershipTransport{startMode: "cancel", startEntered: make(chan struct{})}
	installed := make(chan error, 1)
	go func() { installed <- manager.InstallLive(context.Background(), "peer/mesh", adapter) }()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("InstallLive Start was not entered")
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-installed; err == nil {
		t.Fatal("InstallLive unexpectedly succeeded")
	}
	if starts, closes, discards, running := adapter.counts(); starts != 1 || closes != 1 || discards != 0 || running {
		t.Fatalf("canceled install cleanup: starts=%d closes=%d discards=%d running=%v", starts, closes, discards, running)
	}
}

func TestManagerDetachedReceiveOwnershipRetainedThroughTerminalDrain(t *testing.T) {
	for _, operation := range []string{"remove", "replace", "retire-all"} {
		t.Run(operation, func(t *testing.T) {
			manager, err := NewLiveManager(2)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			envelope := managerEnvelope(t)
			secret := []byte("retained-lower-layer-secret")
			envelope.Payload.Inline = []byte("retained-inbound-record")
			adapter := &retainedInboundOwnershipTransport{envelope: envelope, retained: secret, received: make(chan struct{}), drain: make(chan struct{})}
			if err := manager.InstallLive(context.Background(), envelope.Sender, adapter); err != nil {
				t.Fatal(err)
			}
			select {
			case <-adapter.received:
			case <-time.After(time.Second):
				t.Fatal("inbound secret was not retained")
			}
			for deadline := time.Now().Add(time.Second); len(manager.inbound) != 1; {
				if time.Now().After(deadline) {
					t.Fatal("inbound record was not transferred to Manager ownership")
				}
				time.Sleep(time.Millisecond)
			}
			switch operation {
			case "remove":
				err = manager.RemoveLive(context.Background(), envelope.Sender)
			case "replace":
				err = manager.InstallLive(context.Background(), envelope.Sender, &boundOwnershipTransport{ownershipTransport: &ownershipTransport{}})
			case "retire-all":
				err = manager.RetireAllLive(context.Background())
			}
			if err != nil {
				t.Fatalf("detach = %v", err)
			}
			if closes, discards, nonzero := adapter.state(); closes != 1 || discards != 0 || !nonzero {
				t.Fatalf("pre-terminal ownership: closes=%d discards=%d nonzero=%v", closes, discards, nonzero)
			}
			if err := manager.InstallLive(context.Background(), "other/mesh", adapter); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("detached adapter reinstall = %v", err)
			}
			close(adapter.drain)
			for deadline := time.Now().Add(time.Second); ; {
				_, discards, nonzero := adapter.state()
				if discards == 1 && !nonzero {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("receiver retirement did not discard: discards=%d nonzero=%v", discards, nonzero)
				}
				time.Sleep(time.Millisecond)
			}
			manager.mu.Lock()
			owned := manager.ownsAdapterLocked(adapter)
			manager.mu.Unlock()
			if owned {
				t.Fatal("receiver retirement did not release adapter identity")
			}
			if operation == "remove" || operation == "replace" {
				receiveContext, cancelReceive := context.WithTimeout(context.Background(), 25*time.Millisecond)
				received, receiveErr := manager.ReceiveWithKind(receiveContext)
				cancelReceive()
				if !errors.Is(receiveErr, context.DeadlineExceeded) {
					t.Fatalf("detached exact-source record = %#v, %v", received, receiveErr)
				}
				clearReceived(&received)
			}
			if err := manager.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if closes, discards, nonzero := adapter.state(); closes != 1 || discards != 1 || nonzero {
				t.Fatalf("terminal drain: closes=%d discards=%d nonzero=%v", closes, discards, nonzero)
			}
		})
	}
}
