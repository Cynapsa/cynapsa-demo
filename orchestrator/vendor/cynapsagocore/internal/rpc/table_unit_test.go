package rpc

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func rpcRequest(t *testing.T, seed string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", []byte(seed))
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh", "agent-a", "agent-b")
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := protocol.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		Version:          protocol.Version2,
		MessageID:        messageID,
		ConversationID:   conversationID,
		Sender:           "agent-a",
		Recipient:        "agent-b",
		MeshID:           "mesh",
		Mode:             protocol.ModeRequest,
		ExpiresAt:        time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC),
		CorrelationID:    correlationID,
		CreatedAt:        time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	}
}

func rpcResponse(t *testing.T, request protocol.Envelope) protocol.Envelope {
	t.Helper()
	response := rpcRequest(t, "response")
	response.Sender, response.Recipient = request.Recipient, request.Sender
	response.ConversationID = request.ConversationID
	response.MeshID = request.MeshID
	response.Mode = protocol.ModeResponse
	response.ExpiresAt = time.Time{}
	response.CorrelationID = request.CorrelationID
	response.ReplyTo = request.MessageID
	return response
}

func TestOutboundSuccessDuplicateAndPeerBinding(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := rpcRequest(t, "request")
	deadline := now.Add(time.Minute)
	if err := table.Register(request, "command", deadline); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(request, "other", deadline); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate register: %v", err)
	}
	wrong := rpcResponse(t, request)
	wrong.Sender = "attacker"
	if err := table.Complete(wrong); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("peer binding: %v", err)
	}
	response := rpcResponse(t, request)
	if err := table.Complete(response); err != nil {
		t.Fatal(err)
	}
	if err := table.Complete(response); !errors.Is(err, ErrCompleted) {
		t.Fatalf("duplicate response: %v", err)
	}
	got, err := table.Wait(context.Background(), request.CorrelationID)
	if err != nil || got.MessageID != response.MessageID || table.Len() != 0 {
		t.Fatalf("got=%#v len=%d err=%v", got, table.Len(), err)
	}
	if err := table.Complete(response); !errors.Is(err, ErrCompleted) {
		t.Fatalf("late response: %v", err)
	}
}

func TestCompleteAcceptedRunsEvidenceOnlyForCurrentExactResponse(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := rpcRequest(t, "request")
	if err = table.Register(request, "command", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	accept := func(gotRequest, gotResponse protocol.Envelope) error {
		calls++
		if gotRequest.MessageID != request.MessageID || gotResponse.ReplyTo != request.MessageID {
			t.Fatal("accept callback received mismatched ownership")
		}
		return nil
	}
	forged := rpcResponse(t, request)
	forged.Sender = "attacker"
	if err = table.CompleteAccepted(forged, accept); !errors.Is(err, ErrInvalidResponse) || calls != 0 {
		t.Fatalf("forged complete=%v calls=%d", err, calls)
	}
	response := rpcResponse(t, request)
	callbackFailure := errors.New("terminal evidence unavailable")
	if err = table.CompleteAccepted(response, func(protocol.Envelope, protocol.Envelope) error {
		calls++
		return callbackFailure
	}); !errors.Is(err, callbackFailure) || calls != 1 {
		t.Fatalf("callback failure=%v calls=%d", err, calls)
	}
	if err = table.CompleteAccepted(response, accept); err != nil || calls != 2 {
		t.Fatalf("accepted complete=%v calls=%d", err, calls)
	}
	if err = table.CompleteAccepted(response, accept); !errors.Is(err, ErrCompleted) || calls != 2 {
		t.Fatalf("late complete=%v calls=%d", err, calls)
	}
}

func TestOutboundCancellationTimeoutAndCapacity(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	timer := make(chan time.Time, 1)
	table, err := NewOutboundTable(OutboundConfig{
		Capacity: 2,
		Now:      func() time.Time { return now },
		After:    func(time.Duration) <-chan time.Time { return timer },
	})
	if err != nil {
		t.Fatal(err)
	}
	first := rpcRequest(t, "first")
	second := rpcRequest(t, "second")
	if err := table.Register(first, "first", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(second, "second", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(rpcRequest(t, "third"), "third", now.Add(time.Minute)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if err := table.Cancel(first.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Wait(context.Background(), first.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancel wait: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := table.Wait(context.Background(), second.CorrelationID)
		result <- err
	}()
	timer <- now.Add(time.Minute)
	if err := <-result; !errors.Is(err, ErrExpired) {
		t.Fatalf("timeout: %v", err)
	}
	if err := table.Complete(rpcResponse(t, second)); err == nil {
		t.Fatal("late response was accepted")
	}
}

func TestOutboundTerminalCountsOnceAndLateCompletionDoesNotBlock(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first := rpcRequest(t, "first")
	if err := table.Register(first, "first", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Complete(rpcResponse(t, first)); err != nil {
		t.Fatal(err)
	}
	second := rpcRequest(t, "second")
	if err := table.Register(second, "second", now.Add(time.Minute)); err != nil {
		t.Fatalf("terminal request consumed two capacity slots: %v", err)
	}
	now = now.Add(time.Minute)
	if err := table.Complete(rpcResponse(t, first)); err == nil {
		t.Fatal("late duplicate response accepted")
	}
}

func TestInboundSingleUseExpiryCapacityAndConcurrency(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewInboundTable(InboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := rpcRequest(t, "inbound")
	handle, err := table.CreateHandle(request, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second, err := table.CreateHandle(rpcRequest(t, "second"), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.CreateHandle(rpcRequest(t, "third"), now.Add(time.Minute)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}

	var successes int
	var mu sync.Mutex
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := table.ConsumeHandle(handle); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wait.Wait()
	if successes != 1 {
		t.Fatalf("successful consumes=%d", successes)
	}
	if _, err := table.ConsumeHandle(handle); !errors.Is(err, ErrCompleted) {
		t.Fatalf("reuse: %v", err)
	}

	now = now.Add(time.Minute)
	if _, err := table.ConsumeHandle(second); err == nil {
		t.Fatal("expired handle accepted")
	}
	if _, err := table.CreateHandle(rpcRequest(t, "replacement"), now.Add(time.Minute)); err != nil {
		t.Fatalf("expired entry did not release capacity: %v", err)
	}
}

func TestInboundReplyLeaseCommitReleaseCancel(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewInboundTable(InboundConfig{Capacity: 3, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := table.CreateHandle(rpcRequest(t, "lease"), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.BeginReply(handle); !errors.Is(err, ErrReplyInProgress) {
		t.Fatalf("concurrent reply: %v", err)
	}
	if err := table.ReleaseReply(lease); err != nil {
		t.Fatal(err)
	}
	_, lease, err = table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(lease); err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(lease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("lease reuse: %v", err)
	}

	cancelled, err := table.CreateHandle(rpcRequest(t, "cancelled"), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, cancelledLease, err := table.BeginReply(cancelled)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(cancelled); err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(cancelledLease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("cancelled lease commit: %v", err)
	}

	expiring, err := table.CreateHandle(rpcRequest(t, "expiring"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, expiredLease, err := table.BeginReply(expiring)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := table.CommitReply(expiredLease); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired reserved lease commit: %v", err)
	}
}

func TestInboundReleasedLeaseCannotCommitLater(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewInboundTable(InboundConfig{
		Capacity:     1,
		Now:          func() time.Time { return now },
		LeaseEntropy: bytes.NewReader(make([]byte, 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := table.CreateHandle(rpcRequest(t, "stale-lease"), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, stale, err := table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.ReleaseReply(stale); err != nil {
		t.Fatal(err)
	}
	_, current, err := table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(stale); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale released lease committed: %v", err)
	}
	if err := table.CommitReply(current); err != nil {
		t.Fatal(err)
	}
}

func TestRPCConfigurationBounds(t *testing.T) {
	if _, err := NewOutboundTable(OutboundConfig{Capacity: MaxTableCapacity}); err != nil {
		t.Fatalf("maximum outbound capacity rejected: %v", err)
	}
	if _, err := NewInboundTable(InboundConfig{Capacity: MaxTableCapacity}); err != nil {
		t.Fatalf("maximum inbound capacity rejected: %v", err)
	}
	for _, capacity := range []int{0, -1, MaxTableCapacity + 1} {
		if _, err := NewOutboundTable(OutboundConfig{Capacity: capacity}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("outbound capacity %d: %v", capacity, err)
		}
		if _, err := NewInboundTable(InboundConfig{Capacity: capacity}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("inbound capacity %d: %v", capacity, err)
		}
	}
}

func TestRPCIdentityNeverReusedAfterPruneAndIssuanceExhaustion(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	outbound, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first := rpcRequest(t, "first")
	deadline := now.Add(time.Second)
	if err := outbound.Register(first, "first", deadline); err != nil {
		t.Fatal(err)
	}
	if err := outbound.Cancel(first.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err := outbound.Wait(context.Background(), first.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled wait: %v", err)
	}
	now = now.Add(2 * time.Second)
	outbound.Prune(now)
	if err := outbound.Register(first, "reused", now.Add(time.Minute)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("pruned correlation reused: %v", err)
	}
	next := rpcRequest(t, "next")
	if err := outbound.Register(next, "next", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := outbound.Complete(rpcResponse(t, first)); err == nil {
		t.Fatal("stale response completed a later table state")
	}
	if err := outbound.Complete(rpcResponse(t, next)); err != nil {
		t.Fatalf("current response rejected: %v", err)
	}

	entropy := bytes.Repeat(make([]byte, requestHandleEntropyBytes), requestHandleAttempts+1)
	inbound, err := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }, Entropy: bytes.NewReader(entropy)})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := inbound.CreateHandle(rpcRequest(t, "inbound-first"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inbound.ConsumeHandle(handle); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	inbound.Prune(now)
	if _, err := inbound.CreateHandle(rpcRequest(t, "inbound-next"), now.Add(time.Minute)); !errors.Is(err, ErrHandleCollision) {
		t.Fatalf("pruned request handle reused: %v", err)
	}

	exhaustedOutbound, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	exhaustedOutbound.issuedCount = MaxCorrelationIssuance
	if err := exhaustedOutbound.Register(rpcRequest(t, "exhausted-correlation"), "command", now.Add(time.Minute)); !errors.Is(err, ErrIssuanceExhausted) {
		t.Fatalf("correlation issuance exhaustion: %v", err)
	}
	exhaustedInbound, _ := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	exhaustedInbound.issuedCount = MaxRequestHandleIssuance
	if _, err := exhaustedInbound.CreateHandle(rpcRequest(t, "exhausted-handle"), now.Add(time.Minute)); !errors.Is(err, ErrIssuanceExhausted) {
		t.Fatalf("request handle issuance exhaustion: %v", err)
	}
	if MaxCorrelationIssuance != 1_048_576 || MaxRequestHandleIssuance != 1_048_576 {
		t.Fatal("process-lifetime issuance ceilings changed")
	}
}

func TestRPCDeadlineClockIsReadAfterStateLockAcquisition(t *testing.T) {
	initial := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

	t.Run("outbound register", func(t *testing.T) {
		now := initial
		table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
		request := rpcRequest(t, "delayed-register")
		table.mu.Lock()
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			close(started)
			result <- table.Register(request, "command", initial.Add(time.Second))
		}()
		<-started
		now = initial.Add(2 * time.Second)
		table.mu.Unlock()
		if err := <-result; !errors.Is(err, ErrExpired) {
			t.Fatalf("delayed register used stale clock: %v", err)
		}
	})

	t.Run("outbound complete", func(t *testing.T) {
		now := initial
		table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
		request := rpcRequest(t, "delayed-complete")
		if err := table.Register(request, "command", initial.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		response := rpcResponse(t, request)
		table.mu.Lock()
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			close(started)
			result <- table.Complete(response)
		}()
		<-started
		now = initial.Add(2 * time.Second)
		table.mu.Unlock()
		if err := <-result; err == nil {
			t.Fatal("delayed completion crossed its deadline")
		}
	})

	t.Run("inbound create", func(t *testing.T) {
		now := initial
		table, _ := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }})
		request := rpcRequest(t, "delayed-create")
		table.mu.Lock()
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			close(started)
			_, err := table.CreateHandle(request, initial.Add(time.Second))
			result <- err
		}()
		<-started
		now = initial.Add(2 * time.Second)
		table.mu.Unlock()
		if err := <-result; !errors.Is(err, ErrExpired) {
			t.Fatalf("delayed create used stale clock: %v", err)
		}
	})

	t.Run("inbound reserve", func(t *testing.T) {
		now := initial
		table, _ := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }})
		handle, err := table.CreateHandle(rpcRequest(t, "delayed-reserve"), initial.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		table.mu.Lock()
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			close(started)
			_, _, err := table.BeginReply(handle)
			result <- err
		}()
		<-started
		now = initial.Add(2 * time.Second)
		table.mu.Unlock()
		if err := <-result; err == nil {
			t.Fatal("delayed reply reservation crossed its deadline")
		}
	})
}
