package rpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func peerRevocationRequest(t *testing.T, seed string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", []byte(seed))
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh", "agent-a", "agent-b")
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   conversationID,
		Sender:           "agent-a",
		Recipient:        "agent-b",
		MeshID:           "mesh",
		Mode:             protocol.ModeRequest,
		CreatedAt:        time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2026, 8, 29, 12, 1, 0, 0, time.UTC),
		ClockUncertainty: time.Millisecond,
		Payload:          payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func peerRevocationResponse(t *testing.T, request protocol.Envelope) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", []byte("response"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   request.ConversationID,
		Sender:           request.Recipient,
		Recipient:        request.Sender,
		MeshID:           request.MeshID,
		Mode:             protocol.ModeResponse,
		CorrelationID:    request.CorrelationID,
		ReplyTo:          request.MessageID,
		CreatedAt:        request.CreatedAt,
		ClockUncertainty: request.ClockUncertainty,
		Payload:          payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestOutboundFailPeerRejectsOnlyRemovedDestination(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 3, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	removed := peerRevocationRequest(t, "removed")
	allowed := peerRevocationRequest(t, "allowed")
	allowed.Recipient = "agent-c"
	allowed.ConversationID, _ = conversation.DeriveID(allowed.MeshID, allowed.Sender, allowed.Recipient)
	if err := table.Register(removed, removed.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(allowed, allowed.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if count := table.FailPeer("mesh", "agent-b"); count != 1 {
		t.Fatalf("failed=%d", count)
	}
	if _, err := table.Wait(context.Background(), removed.CorrelationID); !errors.Is(err, ErrAuthorizationRejected) {
		t.Fatalf("removed wait=%v", err)
	}
	response := peerRevocationResponse(t, allowed)
	if err := table.Complete(response); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Wait(context.Background(), allowed.CorrelationID); err != nil {
		t.Fatal(err)
	}
}

func TestOutboundFailPeerScrubsQueuedSuccessUnlessWaiterConsumedIt(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	table, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	request := peerRevocationRequest(t, "request")
	response := peerRevocationResponse(t, request)
	if err := table.Register(request, request.MessageID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := table.Complete(response); err != nil {
		t.Fatal(err)
	}
	table.mu.Lock()
	owned := table.entries[request.CorrelationID].terminalResult.envelope.Payload.Inline
	table.mu.Unlock()
	if count := table.FailPeer(request.MeshID, request.Recipient); count != 1 {
		t.Fatalf("failed=%d", count)
	}
	if !bytes.Equal(owned, make([]byte, len(owned))) {
		t.Fatal("queued successful response was not scrubbed")
	}
	if _, err := table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrAuthorizationRejected) {
		t.Fatalf("wait=%v", err)
	}
}

func TestInboundCancelPeerInvalidatesReplyLeaseAndRetainsAuthorizationCause(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	table, err := NewInboundTable(InboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := peerRevocationRequest(t, "inbound removed")
	handle, err := table.CreateHandle(request, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	borrow, lease, err := table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	borrowInline := borrow.Payload.Inline
	borrowProof := borrow.CredentialProof
	wantInline := bytes.Clone(borrowInline)
	wantProof := bytes.Clone(borrowProof)
	if count := table.CancelPeer(request.MeshID, request.Sender); count != 1 {
		t.Fatalf("cancelled=%d", count)
	}
	if !bytes.Equal(borrowInline, wantInline) || !bytes.Equal(borrowProof, wantProof) {
		t.Fatal("peer revocation mutated the active SDK reply borrow")
	}
	if err := table.CommitReply(lease); !errors.Is(err, ErrAuthorizationRejected) {
		t.Fatalf("stale lease=%v", err)
	}
	if _, _, err := table.BeginReply(handle); !errors.Is(err, ErrAuthorizationRejected) {
		t.Fatalf("later reply=%v", err)
	}
	clearEnvelope(&borrow)
	if err := table.FinalizeReply(lease); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(borrowInline, make([]byte, len(borrowInline))) || !bytes.Equal(borrowProof, make([]byte, len(borrowProof))) {
		t.Fatal("reply borrow was not scrubbed by its holder before finalization")
	}
}
