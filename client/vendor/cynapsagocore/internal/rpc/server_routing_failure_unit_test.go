package rpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestFailMessageIDOnlyCompletesTheRejectedRPC(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	rejected := peerRevocationRequest(t, "rejected")
	other := peerRevocationRequest(t, "other")
	if err := table.Register(rejected, rejected.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(other, other.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := table.FailMessageID("msg_unknown", ErrAuthorizationRejected); got != "" {
		t.Fatalf("unknown message changed correlation %q", got)
	}
	if got := table.FailMessageID(rejected.MessageID, ErrAuthorizationRejected); got != rejected.CorrelationID {
		t.Fatalf("failed correlation = %q", got)
	}
	if _, err := table.Wait(context.Background(), rejected.CorrelationID); !errors.Is(err, ErrAuthorizationRejected) {
		t.Fatalf("rejected wait = %v", err)
	}
	if err := table.Complete(peerRevocationResponse(t, other)); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Wait(context.Background(), other.CorrelationID); err != nil {
		t.Fatalf("unrelated wait = %v", err)
	}
}

func TestFailMessageIDReplacesOnlyQueuedUnconsumedSuccess(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	request := peerRevocationRequest(t, "request")
	if err := table.Register(request, request.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Complete(peerRevocationResponse(t, request)); err != nil {
		t.Fatal(err)
	}
	table.mu.Lock()
	owned := table.entries[request.CorrelationID].terminalResult.envelope.Payload.Inline
	table.mu.Unlock()
	if got := table.FailMessageID(request.MessageID, ErrServerUnavailable); got != request.CorrelationID {
		t.Fatalf("failed correlation = %q", got)
	}
	if !bytes.Equal(owned, make([]byte, len(owned))) {
		t.Fatal("replaced response bytes were not scrubbed")
	}
	if _, err := table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrServerUnavailable) {
		t.Fatalf("wait = %v", err)
	}
	if got := table.FailMessageID(request.MessageID, ErrAuthorizationRejected); got != "" {
		t.Fatalf("consumed result changed by duplicate denial: %q", got)
	}
}
