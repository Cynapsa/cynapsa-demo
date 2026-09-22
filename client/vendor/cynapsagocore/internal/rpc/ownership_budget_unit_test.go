package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestRPCByteBudgetClosedMaximumAndAggregateCap(t *testing.T) {
	one, err := NewByteBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	wantOne := uint64(v1.MaximumPayloadBytes + protocol.MaxEnvelopeBytes)
	if stats := one.stats(); stats.ByteCapacity != wantOne {
		t.Fatalf("capacity=%d want %d", stats.ByteCapacity, wantOne)
	}
	if !one.ReserveOwned(wantOne) || one.ReserveOwned(1) {
		t.Fatal("maximum materialized response exact/+1 admission is not closed")
	}
	one.ReleaseOwned(wantOne)

	shared, err := NewByteBudget(MaxTableCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if stats := shared.stats(); stats.ByteCapacity != maxRetainedRPCBytes {
		t.Fatalf("aggregate capacity=%d want %d", stats.ByteCapacity, maxRetainedRPCBytes)
	}
}

type borrowedTerminalError struct {
	text  string
	cause error
}

func (value *borrowedTerminalError) Error() string { return value.text }
func (value *borrowedTerminalError) Unwrap() error { return value.cause }

func TestOutboundCanonicalizesTerminalErrorsBeforeRetention(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	request := rpcRequest(t, "error ownership")
	if err := table.Register(request, request.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	unknown := errors.New(strings.Repeat("unknown", 1<<18))
	if err := table.Fail(request.CorrelationID, unknown); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown cause=%v", err)
	}
	source := strings.Repeat("x", 2<<20) + "wrapped cancellation" + strings.Repeat("y", 2<<20)
	start := 2 << 20
	wrapped := &borrowedTerminalError{text: source[start : start+len("wrapped cancellation")], cause: fmt.Errorf("outer: %w", ErrCancelled)}
	if err := table.Fail(request.CorrelationID, wrapped); err != nil {
		t.Fatal(err)
	}
	table.mu.Lock()
	result := table.entries[request.CorrelationID].terminalResult
	retired := table.retired[request.CorrelationID]
	table.mu.Unlock()
	if result.err != ErrCancelled || retired.cause != ErrCancelled {
		t.Fatalf("retained causes=%v %v", result.err, retired.cause)
	}
	if _, err := table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	table.Destroy()
}

func TestInboundReplyBorrowIsChargedScrubbedAndUsesOwnedLeaseHandle(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	budget, _ := newByteBudget(maxRetainedRPCBytes)
	table, _ := NewInboundTableWithBudget(InboundConfig{Capacity: 1, Now: func() time.Time { return now }}, budget)
	request := rpcRequest(t, strings.Repeat("borrow canary", 1024))
	handle, err := table.CreateHandle(request, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	before := budget.ownedBytes()
	backing := strings.Repeat("h", 1<<20) + handle
	callerHandle := backing[len(backing)-len(handle):]
	borrow, lease, err := table.BeginReply(callerHandle)
	if err != nil {
		t.Fatal(err)
	}
	if delta := budget.ownedBytes() - before; delta != envelopeDynamicBytes(borrow) {
		t.Fatalf("borrow charge=%d", delta)
	}
	leaseStart := uintptr(unsafe.Pointer(unsafe.StringData(lease.handle)))
	backingStart := uintptr(unsafe.Pointer(unsafe.StringData(backing)))
	if leaseStart >= backingStart && leaseStart < backingStart+uintptr(len(backing)) {
		t.Fatal("lease handle aliases caller backing")
	}
	borrowInline := borrow.Payload.Inline
	borrowProof := borrow.CredentialProof
	if err := table.ReleaseReply(lease); err != nil {
		t.Fatal(err)
	}
	clearEnvelope(&borrow)
	if err := table.FinalizeReply(lease); err != nil {
		t.Fatal(err)
	}
	if budget.ownedBytes() != before || !bytes.Equal(borrowInline, make([]byte, len(borrowInline))) || !bytes.Equal(borrowProof, make([]byte, len(borrowProof))) {
		t.Fatal("released borrow was not scrubbed before charge release")
	}
	table.Destroy()
}

func TestCompleteCapacityFailureWakesWaiter(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	request := rpcRequest(t, "request fills budget")
	budget, _ := newByteBudget(envelopeDynamicBytes(request) + lateResponseDigestBytes)
	table, _ := NewOutboundTableWithBudget(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }}, budget)
	if err := table.Register(request, request.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Complete(rpcResponse(t, request)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("complete=%v", err)
	}
	if _, err := table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("wait=%v", err)
	}
	table.Destroy()
}

func TestReplyBorrowSurvivesConcurrentAuthorityRevocation(t *testing.T) {
	actions := []struct {
		name   string
		revoke func(*InboundTable, string, time.Time) error
	}{
		{name: "cancel", revoke: func(table *InboundTable, handle string, _ time.Time) error {
			return table.Cancel(handle)
		}},
		{name: "cancel peer", revoke: func(table *InboundTable, _ string, _ time.Time) error {
			if cancelled := table.CancelPeer("mesh", "agent-a"); cancelled != 1 {
				return fmt.Errorf("cancelled=%d", cancelled)
			}
			return nil
		}},
		{name: "prune", revoke: func(table *InboundTable, _ string, now time.Time) error {
			if pruned := table.Prune(now.Add(2 * time.Minute)); pruned != 1 {
				return fmt.Errorf("pruned=%d", pruned)
			}
			return nil
		}},
		{name: "destroy", revoke: func(table *InboundTable, _ string, _ time.Time) error {
			table.Destroy()
			return nil
		}},
	}
	for _, action := range actions {
		t.Run(action.name, func(t *testing.T) {
			now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
			budget, err := newByteBudget(maxRetainedRPCBytes)
			if err != nil {
				t.Fatal(err)
			}
			table, err := NewInboundTableWithBudget(InboundConfig{Capacity: 1, Now: func() time.Time { return now }}, budget)
			if err != nil {
				t.Fatal(err)
			}
			request := rpcRequest(t, strings.Repeat("concurrent borrow", 1024))
			handle, err := table.CreateHandle(request, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			borrow, lease, err := table.BeginReply(handle)
			if err != nil {
				t.Fatal(err)
			}
			inlineWant := bytes.Clone(borrow.Payload.Inline)
			proofWant := bytes.Clone(borrow.CredentialProof)
			borrowCharge := envelopeDynamicBytes(borrow)
			started := make(chan struct{})
			stop := make(chan struct{})
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				close(started)
				for {
					select {
					case <-stop:
						return
					default:
						_ = borrow.Payload.Inline[0]
						_ = borrow.CredentialProof[0]
					}
				}
			}()
			<-started
			if err := action.revoke(table, handle, now); err != nil {
				t.Fatal(err)
			}
			close(stop)
			<-readDone
			if !bytes.Equal(borrow.Payload.Inline, inlineWant) || !bytes.Equal(borrow.CredentialProof, proofWant) {
				t.Fatal("authority revocation mutated active reply borrow")
			}
			if budget.ownedBytes() < borrowCharge {
				t.Fatalf("borrow charge released before holder finalization: owned=%d borrow=%d", budget.ownedBytes(), borrowCharge)
			}
			commitErr := table.CommitReply(lease)
			if action.name == "cancel peer" {
				if !errors.Is(commitErr, ErrAuthorizationRejected) {
					t.Fatalf("peer-revoked commit=%v", commitErr)
				}
			} else if !errors.Is(commitErr, ErrInvalidLease) {
				t.Fatalf("revoked commit=%v", commitErr)
			}
			clearEnvelope(&borrow)
			if err := table.FinalizeReply(lease); err != nil {
				t.Fatal(err)
			}
			if action.name != "destroy" {
				table.Destroy()
			}
			if owned := budget.ownedBytes(); owned != 0 {
				t.Fatalf("owned bytes after scrub/finalize/destroy=%d", owned)
			}
			if err := table.FinalizeReply(lease); err != nil || budget.ownedBytes() != 0 {
				t.Fatalf("idempotent finalize=%v owned=%d", err, budget.ownedBytes())
			}
		})
	}
}

func TestOutboundExactAdmissionCancellationScrubsAndDestroyReleasesIdentity(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	request := rpcRequest(t, "request secret")
	exact := envelopeDynamicBytes(request) + lateResponseDigestBytes
	shortBudget, _ := newByteBudget(exact - 1)
	short, _ := NewOutboundTableWithBudget(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }}, shortBudget)
	if err := short.Register(request, "not-retained", now.Add(time.Minute)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("one-short register=%v", err)
	}
	budget, _ := newByteBudget(exact)
	table, err := NewOutboundTableWithBudget(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Register(request, "not-retained", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(request.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	if budget.ownedBytes() != uint64(len(request.CorrelationID))+lateResponseDigestBytes {
		t.Fatalf("issued identity charge=%d", budget.ownedBytes())
	}
	table.Destroy()
	if budget.ownedBytes() != 0 {
		t.Fatalf("destroy retained %d bytes", budget.ownedBytes())
	}

	table, _ = NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err := table.Register(request, "not-retained", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	response := rpcResponse(t, request)
	if err := table.Complete(response); err != nil {
		t.Fatal(err)
	}
	table.mu.Lock()
	result := <-table.entries[request.CorrelationID].done
	ownedInline := result.envelope.Payload.Inline
	ownedProof := result.envelope.CredentialProof
	table.entries[request.CorrelationID].done <- result
	table.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := table.Wait(ctx, request.CorrelationID); !errors.Is(err, ErrCancelled) || got.MessageID != "" {
		t.Fatalf("cancelled ready wait=%#v %v", got, err)
	}
	if !bytes.Equal(ownedInline, make([]byte, len(ownedInline))) || !bytes.Equal(ownedProof, make([]byte, len(ownedProof))) {
		t.Fatal("discarded terminal result was not scrubbed")
	}
	table.Destroy()
	if table.OwnedBytes() != 0 {
		t.Fatalf("destroy retained %d bytes", table.OwnedBytes())
	}
}

func TestOutboundAdmissionFreezesCallerSubstringKeys(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	request := rpcRequest(t, "freeze")
	correlationSource := strings.Repeat("x", 1<<20) + request.CorrelationID
	conversationSource := strings.Repeat("y", 1<<20) + request.ConversationID
	request.CorrelationID = correlationSource[len(correlationSource)-len(request.CorrelationID):]
	request.ConversationID = conversationSource[len(conversationSource)-len(request.ConversationID):]
	table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err := table.Register(request, "command", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	aliases := func(value, source string) bool {
		start := uintptr(unsafe.Pointer(unsafe.StringData(value)))
		sourceStart := uintptr(unsafe.Pointer(unsafe.StringData(source)))
		return start >= sourceStart && start < sourceStart+uintptr(len(source))
	}
	table.mu.Lock()
	entry := table.entries[request.CorrelationID]
	if aliases(entry.correlationID, correlationSource) || aliases(entry.conversationID, conversationSource) || aliases(entry.request.CorrelationID, correlationSource) || aliases(entry.request.ConversationID, conversationSource) {
		table.mu.Unlock()
		t.Fatal("retained key aliases huge caller backing")
	}
	table.mu.Unlock()
	if !table.ActiveConversation(request.ConversationID) {
		t.Fatal("owned index changed with caller backing lifecycle")
	}
	table.Destroy()
}
