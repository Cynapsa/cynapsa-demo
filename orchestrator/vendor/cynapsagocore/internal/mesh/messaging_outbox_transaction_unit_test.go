package mesh

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type retryBarrierCarrier struct {
	mu                     sync.Mutex
	calls                  int
	secondEntered, release chan struct{}
}

func (carrier *retryBarrierCarrier) Send(ctx context.Context, _ protocol.Envelope) CarrierDisposition {
	carrier.mu.Lock()
	carrier.calls++
	call := carrier.calls
	carrier.mu.Unlock()
	if call == 1 {
		return CarrierUnavailable
	}
	if call == 2 {
		close(carrier.secondEntered)
		select {
		case <-carrier.release:
			return CarrierAccepted
		case <-ctx.Done():
			return CarrierUnavailable
		}
	}
	return CarrierRejected
}

func (carrier *retryBarrierCarrier) callCount() int {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.calls
}

func TestDeliveryRetryCoalescesWithAutomaticAttempt(t *testing.T) {
	carrier := &retryBarrierCarrier{secondEntered: make(chan struct{}), release: make(chan struct{})}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	source := &mutableGroupSource{}
	source.replace(now, Identity{AgentID: testLocalBare, Internal: testLocalFull}, Identity{AgentID: testPeerBare, Internal: testPeerFull})
	service, _ := newCurrentMembershipService(t, source, carrier, &testDeliverySink{})
	result, failure := service.MessageSend(context.Background(), model.MessageSendArgs{To: testPeerBare, Payload: nativePayload("/epoch", "payload")})
	if failure != nil || !result.Accepted {
		t.Fatalf("send=%#v failure=%#v", result, failure)
	}
	select {
	case <-carrier.secondEntered:
	case <-time.After(time.Second):
		t.Fatal("automatic retry did not enter carrier")
	}
	retryDone := make(chan *Failure, 1)
	go func() { retryDone <- service.DeliveryRetry(context.Background(), result.MessageID) }()
	select {
	case retryFailure := <-retryDone:
		if retryFailure != nil {
			t.Fatalf("coalesced retry=%#v", retryFailure)
		}
	case <-time.After(time.Second):
		t.Fatal("retry blocked behind active automatic attempt")
	}
	if calls := carrier.callCount(); calls != 2 {
		t.Fatalf("coalesced retry started another carrier attempt: %d", calls)
	}
	close(carrier.release)
	deadline := time.Now().Add(time.Second)
	for {
		status, statusFailure := service.QueueStatus(context.Background())
		if statusFailure != nil {
			t.Fatalf("queue status=%#v", statusFailure)
		}
		if status.Queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("accepted automatic attempt did not retire entry")
		}
		time.Sleep(time.Millisecond)
	}
}
