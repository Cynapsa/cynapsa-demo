package outbox

import (
	"errors"
	"testing"
)

func TestServerRoutingFailureRetiresRank2OwnedEntryWithoutTouchingOtherMessages(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	rejected, _ := acceptedRPCPair(t)
	other, _ := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.ClaimRank2(rejected.MessageID, 19); err != nil {
		t.Fatal(err)
	}
	queue.Release(reservation)
	if _, err := queue.Enqueue(other); err != nil {
		t.Fatal(err)
	}
	if err := queue.RetireServerRejected(rejected.MessageID); err != nil {
		t.Fatal(err)
	}
	if queue.OwnsRank2(rejected.MessageID) {
		t.Fatal("rejected message retained Rank2 ownership")
	}
	if ids := queue.EligibleMessageIDs(2); len(ids) != 1 || ids[0] != other.MessageID {
		t.Fatalf("unrelated queue changed: %v", ids)
	}
	if err := queue.RetireServerRejected(rejected.MessageID); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("duplicate rejection = %v", err)
	}
}

func TestServerRoutingFailureDefersZeroizingActiveBorrowUntilRelease(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 1, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RetireServerRejected(request.MessageID); err != nil {
		t.Fatal(err)
	}
	if reservation.Envelope.MessageID != request.MessageID {
		t.Fatal("retirement mutated an active borrower")
	}
	queue.Release(reservation)
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("retired entry remained after release: %d messages, %d bytes", messages, bytes)
	}
}
