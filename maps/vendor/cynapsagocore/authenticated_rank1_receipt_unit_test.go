package cynapsagocore

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type rootReceiptPending struct{ binding transport.LiveReceipt }

func (pending rootReceiptPending) Binding() transport.LiveReceipt { return pending.binding }
func (rootReceiptPending) Done() <-chan bool {
	done := make(chan bool, 1)
	done <- false
	close(done)
	return done
}
func (rootReceiptPending) Close() {}

type rootReceiptLive struct {
	inbound  chan transport.AuthenticatedReceived
	receipts chan transport.LiveReceipt
	closed   chan struct{}
	once     sync.Once
	binding  [sha256.Size]byte
}

func (*rootReceiptLive) Kind() transport.Kind                          { return transport.KindLive }
func (*rootReceiptLive) Start(context.Context) error                   { return nil }
func (*rootReceiptLive) Send(context.Context, protocol.Envelope) error { return nil }
func (*rootReceiptLive) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (live *rootReceiptLive) Close(context.Context) error {
	live.once.Do(func() { close(live.closed) })
	return nil
}
func (live *rootReceiptLive) Receive(ctx context.Context) (protocol.Envelope, error) {
	received, err := live.ReceiveAuthenticated(ctx)
	return received.Envelope, err
}
func (live *rootReceiptLive) ReceiveAuthenticated(ctx context.Context) (transport.AuthenticatedReceived, error) {
	select {
	case received := <-live.inbound:
		return received, nil
	case <-live.closed:
		return transport.AuthenticatedReceived{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.AuthenticatedReceived{}, ctx.Err()
	}
}
func (live *rootReceiptLive) SendTracked(_ context.Context, envelope protocol.Envelope) (transport.PendingLiveReceipt, error) {
	return rootReceiptPending{binding: transport.LiveReceipt{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: live.binding,
	}}, nil
}
func (live *rootReceiptLive) SendReceipt(ctx context.Context, receipt transport.LiveReceipt) error {
	select {
	case live.receipts <- receipt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type rootRank1Admission struct {
	mu       sync.Mutex
	outcomes []rootRank1AdmissionOutcome
	calls    chan struct{}
}

type rootRank1AdmissionOutcome struct {
	eligible bool
	failure  *mesh.Failure
}

func (receiver *rootRank1Admission) ReceiveRank1(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) (bool, *mesh.Failure) {
	receiver.mu.Lock()
	outcome := receiver.outcomes[0]
	receiver.outcomes = receiver.outcomes[1:]
	receiver.mu.Unlock()
	receiver.calls <- struct{}{}
	return outcome.eligible, outcome.failure
}

func (receiver *rootRank1Admission) ReceiveRank1WithReceipt(ctx context.Context, _ mesh.AuthenticatedProvenance, _ protocol.Envelope, receipt mesh.InboundRank1Receipt) *mesh.Failure {
	receiver.mu.Lock()
	outcome := receiver.outcomes[0]
	receiver.outcomes = receiver.outcomes[1:]
	receiver.mu.Unlock()
	receiver.calls <- struct{}{}
	if outcome.failure == nil && outcome.eligible {
		_ = receipt.Acknowledge(ctx)
	}
	receipt.Close()
	return outcome.failure
}

func TestRank1PumpReceiptsOnlyAfterReceiverAdmission(t *testing.T) {
	manager, err := transport.NewLiveManager(4)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	binding := sha256.Sum256([]byte("root-exact-rank1-session"))
	live := &rootReceiptLive{inbound: make(chan transport.AuthenticatedReceived, 3), receipts: make(chan transport.LiveReceipt, 1), closed: make(chan struct{}), binding: binding}
	peer := "claimed@example.test/mesh"
	if err = manager.InstallLive(context.Background(), peer, live); err != nil {
		t.Fatal(err)
	}
	receiver := &rootRank1Admission{calls: make(chan struct{}, 3), outcomes: []rootRank1AdmissionOutcome{
		{},
		{failure: &mesh.Failure{Code: mesh.FailureCapacity}},
		{eligible: true},
	}}
	pump := &rank1InboundPump{source: manager, receiver: receiver, localRecipient: "local@example.test/mesh", meshID: "mesh", receiptTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *mesh.Failure, 1)
	go func() { done <- pump.Run(ctx) }()
	for sequence := uint64(1); sequence <= 3; sequence++ {
		envelope := rootRank2Envelope(t, sequence, "admission")
		live.inbound <- transport.AuthenticatedReceived{Envelope: envelope, Authentication: transport.LiveAuthentication{Peer: peer, MeshID: "mesh", ChannelBinding: binding}}
		select {
		case <-receiver.calls:
		case <-time.After(time.Second):
			t.Fatal("receiver was not called")
		}
		if sequence < 3 {
			select {
			case receipt := <-live.receipts:
				t.Fatalf("unadmitted sequence %d receipted: %#v", sequence, receipt)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	select {
	case receipt := <-live.receipts:
		if receipt.MessageID == "" || receipt.MeshID != "mesh" || receipt.ChannelBinding != binding {
			t.Fatalf("receipt=%#v", receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted envelope was not receipted")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pump did not stop")
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
