package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

// TestRequestResponseLifecycle will verify success, timeout, cancellation, and late-response handling.
func TestRequestResponseLifecycle(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	table, err := rpc.NewOutboundTable(rpc.OutboundConfig{Capacity: 4, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := integrationEnvelope(t, 1, protocol.ModeRequest)
	deadline := now.Add(time.Minute)
	if err := table.Register(request, "command-success", deadline); err != nil {
		t.Fatal(err)
	}
	response := responseFor(t, request)
	if err := table.Complete(response); err != nil {
		t.Fatal(err)
	}
	got, err := table.Wait(context.Background(), request.CorrelationID)
	if err != nil || got.MessageID != response.MessageID {
		t.Fatalf("response=%#v err=%v", got, err)
	}
	if err := table.Complete(response); !errors.Is(err, rpc.ErrCompleted) {
		t.Fatalf("late response=%v", err)
	}
	if err := table.Register(request, "reused", deadline); !errors.Is(err, rpc.ErrDuplicate) {
		t.Fatalf("correlation reuse=%v", err)
	}

	cancelled := integrationEnvelope(t, 2, protocol.ModeRequest)
	if err := table.Register(cancelled, "command-cancel", deadline); err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(cancelled.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Wait(context.Background(), cancelled.CorrelationID); !errors.Is(err, rpc.ErrCancelled) {
		t.Fatalf("cancel=%v", err)
	}

	expired := integrationEnvelope(t, 3, protocol.ModeRequest)
	expiredDeadline := now.Add(time.Second)
	if err := table.Register(expired, "command-timeout", expiredDeadline); err != nil {
		t.Fatal(err)
	}
	now = expiredDeadline
	if _, err := table.Wait(context.Background(), expired.CorrelationID); !errors.Is(err, rpc.ErrExpired) {
		t.Fatalf("timeout=%v", err)
	}

	inbound, err := rpc.NewInboundTable(rpc.InboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := inbound.CreateHandle(request, deadline)
	if err != nil {
		t.Fatal(err)
	}
	trusted, lease, err := inbound.BeginReply(handle)
	if err != nil || trusted.MessageID != request.MessageID {
		t.Fatalf("begin reply=%#v err=%v", trusted, err)
	}
	if _, _, err := inbound.BeginReply(handle); !errors.Is(err, rpc.ErrReplyInProgress) {
		t.Fatalf("double reply=%v", err)
	}
	if err := inbound.CommitReply(lease); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inbound.BeginReply(handle); !errors.Is(err, rpc.ErrCompleted) {
		t.Fatalf("late reply=%v", err)
	}

	inboundExpired := integrationEnvelope(t, 4, protocol.ModeRequest)
	now = deadline.Add(time.Second)
	if _, err := inbound.CreateHandle(inboundExpired, deadline); !errors.Is(err, rpc.ErrExpired) {
		t.Fatalf("expired inbound request=%v", err)
	}
}
