package rpc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestValidateLateResponseAcceptsExactExpiredAndCancelledRequests(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Destroy()

	expired := rpcRequest(t, "expired")
	expiredDeadline := now.Add(time.Minute)
	if err = table.Register(expired, "command-expired", expiredDeadline); err != nil {
		t.Fatal(err)
	}
	now = expiredDeadline
	if removed := table.Prune(now); removed != 1 {
		t.Fatalf("expired prune removed=%d want 1", removed)
	}
	if _, err = table.Wait(context.Background(), expired.CorrelationID); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired terminal cause=%v", err)
	}
	if !table.ValidateLateResponse(rpcResponse(t, expired)) {
		t.Fatal("exact expired response rejected")
	}
	if _, err = table.Wait(context.Background(), expired.CorrelationID); !errors.Is(err, ErrExpired) {
		t.Fatalf("validation changed expired terminal cause=%v", err)
	}

	cancelled := rpcRequest(t, "cancelled")
	if err = table.Register(cancelled, "command-cancelled", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = table.Cancel(cancelled.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err = table.Wait(context.Background(), cancelled.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled terminal cause=%v", err)
	}
	if !table.ValidateLateResponse(rpcResponse(t, cancelled)) {
		t.Fatal("exact cancelled response rejected")
	}
	if _, err = table.Wait(context.Background(), cancelled.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("validation changed cancelled terminal cause=%v", err)
	}
}

func TestValidateLateResponseRejectsEveryForgedBindingFieldAndUnknown(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Destroy()
	request := rpcRequest(t, "binding")
	if err = table.Register(request, "command", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = table.Cancel(request.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err = table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	exact := rpcResponse(t, request)
	if !table.ValidateLateResponse(exact) {
		t.Fatal("exact response rejected")
	}

	other := rpcRequest(t, "other-binding")
	otherConversation, err := conversation.DeriveID("mesh", "agent-a", "agent-c")
	if err != nil {
		t.Fatal(err)
	}
	forgeries := map[string]func(*protocol.Envelope){
		"reply-to":     func(response *protocol.Envelope) { response.ReplyTo = other.MessageID },
		"correlation":  func(response *protocol.Envelope) { response.CorrelationID = other.CorrelationID },
		"conversation": func(response *protocol.Envelope) { response.ConversationID = otherConversation },
		"mesh":         func(response *protocol.Envelope) { response.MeshID = "other-mesh" },
		"sender":       func(response *protocol.Envelope) { response.Sender = "agent-c" },
		"recipient":    func(response *protocol.Envelope) { response.Recipient = "agent-c" },
	}
	for name, forge := range forgeries {
		t.Run(name, func(t *testing.T) {
			response := exact
			forge(&response)
			if table.ValidateLateResponse(response) {
				t.Fatal("forged response accepted")
			}
		})
	}

	unknown := rpcResponse(t, other)
	if table.ValidateLateResponse(unknown) {
		t.Fatal("unknown correlation accepted")
	}
	invalid := exact
	invalid.Payload.Inline[0] ^= 0xff
	if table.ValidateLateResponse(invalid) {
		t.Fatal("protocol-invalid response accepted")
	}
}

func TestLateResponseDigestRetainsNoPayloadOrPathAndUsesSharedBudget(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	request := rpcRequest(t, strings.Repeat("payload-secret", 1024))
	responsePath := strings.Repeat("/private/path/", 128)
	admissionBytes := envelopeDynamicBytes(request) + uint64(len(responsePath)) + lateResponseDigestBytes

	shortBudget, err := newByteBudget(admissionBytes - 1)
	if err != nil {
		t.Fatal(err)
	}
	short, err := NewOutboundTableWithBudget(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }}, shortBudget)
	if err != nil {
		t.Fatal(err)
	}
	if err = short.RegisterWithResponsePath(request, "command", responsePath, now.Add(time.Minute)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("one-byte-short digest admission=%v", err)
	}
	short.Destroy()

	budget, err := newByteBudget(admissionBytes)
	if err != nil {
		t.Fatal(err)
	}
	table, err := NewOutboundTableWithBudget(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = table.RegisterWithResponsePath(request, "command", responsePath, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if owned := budget.ownedBytes(); owned != admissionBytes {
		t.Fatalf("registered bytes=%d want %d", owned, admissionBytes)
	}
	if err = table.Cancel(request.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err = table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	issuedBytes := uint64(len(request.CorrelationID)) + lateResponseDigestBytes
	if owned := budget.ownedBytes(); owned != issuedBytes {
		t.Fatalf("terminal issued bytes=%d want %d", owned, issuedBytes)
	}
	if len(table.entries) != 0 || len(table.issued) != 1 {
		t.Fatalf("terminal retention entries=%d issued=%d", len(table.entries), len(table.issued))
	}
	issued := table.issued[request.CorrelationID]
	if issued.lateResponseDigest == ([lateResponseDigestBytes]byte{}) {
		t.Fatal("issued response binding was not retained")
	}
	if !table.ValidateLateResponse(rpcResponse(t, request)) {
		t.Fatal("payload-free issued binding did not validate")
	}

	table.Destroy()
	if owned := budget.ownedBytes(); owned != 0 {
		t.Fatalf("destroy retained %d bytes", owned)
	}
	if len(table.issued) != 0 || table.ValidateLateResponse(rpcResponse(t, request)) {
		t.Fatal("destroy retained late-response authority")
	}
}

func TestValidateLateResponseIsRaceSafeAcrossTerminalTransition(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := rpcRequest(t, "race")
	if err = table.Register(request, "command", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	response := rpcResponse(t, request)
	forged := response
	forged.Sender = "agent-c"

	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 1_000 {
				if !table.ValidateLateResponse(response) {
					t.Error("exact response rejected during terminal transition")
					return
				}
				if table.ValidateLateResponse(forged) {
					t.Error("forged response accepted during terminal transition")
					return
				}
			}
		}()
	}
	close(start)
	if err = table.Cancel(request.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if _, err = table.Wait(context.Background(), request.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	workers.Wait()
	if !table.ValidateLateResponse(response) {
		t.Fatal("terminal transition lost issued binding")
	}
	table.Destroy()
}
