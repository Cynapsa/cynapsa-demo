package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestManagerCloseDrainsBufferedInboundAndFencesReceive(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	canary := envelope.Payload.Inline
	manager.inbound <- Received{Kind: KindLive, Envelope: envelope}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(manager.inbound) != 0 {
		t.Fatal("manager retained buffered inbound after joined close")
	}
	for _, value := range canary {
		if value != 0 {
			t.Fatal("manager did not clear dropped inbound payload")
		}
	}
	if _, err = manager.ReceiveWithKind(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close receive=%v", err)
	}
}

type partialErrorAdapter struct {
	kind          Kind
	envelope      protocol.Envelope
	authenticated bool
}

func (adapter *partialErrorAdapter) Kind() Kind                            { return adapter.kind }
func (*partialErrorAdapter) Start(context.Context) error                   { return nil }
func (*partialErrorAdapter) Send(context.Context, protocol.Envelope) error { return nil }
func (adapter *partialErrorAdapter) Receive(context.Context) (protocol.Envelope, error) {
	return adapter.envelope, errors.New("partial receive")
}
func (*partialErrorAdapter) Observe() Observation        { return Observation{State: HealthHealthy} }
func (*partialErrorAdapter) Close(context.Context) error { return nil }
func (adapter *partialErrorAdapter) ReceiveAuthenticated(context.Context) (AuthenticatedReceived, error) {
	return AuthenticatedReceived{Envelope: adapter.envelope, Authentication: LiveAuthentication{}}, errors.New("partial authenticated receive")
}

func TestReceiveLoopClearsPartialEnvelopeReturnedWithError(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(map[bool]string{false: "generic", true: "authenticated"}[authenticated], func(t *testing.T) {
			envelope := managerEnvelope(t)
			canary := envelope.Payload.Inline
			adapter := &partialErrorAdapter{kind: KindDurable, envelope: envelope, authenticated: authenticated}
			var transportAdapter Transport = adapter
			if !authenticated {
				transportAdapter = genericPartialAdapter{partialErrorAdapter: adapter}
			}
			manager := &Manager{inbound: make(chan Received, 1), live: make(map[string]Transport), ctx: context.Background()}
			manager.receivers.Add(1)
			go manager.receiveLoop(manager.ctx, transportAdapter)
			manager.receivers.Wait()
			for _, value := range canary {
				if value != 0 {
					t.Fatal("partial error payload was not cleared")
				}
			}
		})
	}
}

type genericPartialAdapter struct{ partialErrorAdapter *partialErrorAdapter }

func (adapter genericPartialAdapter) Kind() Kind { return adapter.partialErrorAdapter.Kind() }
func (adapter genericPartialAdapter) Start(ctx context.Context) error {
	return adapter.partialErrorAdapter.Start(ctx)
}
func (adapter genericPartialAdapter) Send(ctx context.Context, envelope protocol.Envelope) error {
	return adapter.partialErrorAdapter.Send(ctx, envelope)
}
func (adapter genericPartialAdapter) Receive(ctx context.Context) (protocol.Envelope, error) {
	return adapter.partialErrorAdapter.Receive(ctx)
}
func (adapter genericPartialAdapter) Observe() Observation {
	return adapter.partialErrorAdapter.Observe()
}
func (adapter genericPartialAdapter) Close(ctx context.Context) error {
	return adapter.partialErrorAdapter.Close(ctx)
}

func TestManagerAuthorityFenceDropsPrebufferedAuthentication(t *testing.T) {
	manager, err := NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	envelope := managerEnvelope(t)
	authentication := LiveAuthentication{Peer: envelope.Sender, MeshID: envelope.MeshID}
	manager.mu.Lock()
	epoch := manager.liveAuthorityEpoch
	manager.mu.Unlock()
	manager.inbound <- Received{Kind: KindLive, Envelope: envelope, Authentication: &authentication, authorityEpoch: epoch}
	manager.BlockLiveAuthority()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	_, err = manager.ReceiveWithKind(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-fence authentication escaped fence: %v", err)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseDropsTerminalAdapterReferences(t *testing.T) {
	adapter := &partialErrorAdapter{kind: KindDurable, envelope: managerEnvelope(t)}
	manager, err := NewManager(1, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	durable, defaultLive, liveCount := manager.durable, manager.defaultLive, len(manager.live)
	manager.mu.Unlock()
	if durable != nil || defaultLive != nil || liveCount != 0 {
		t.Fatalf("terminal adapters durable=%T default=%T live=%d", durable, defaultLive, liveCount)
	}
}
