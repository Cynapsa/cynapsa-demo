package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestOutboundConversationAccountingTracksRetainedOwnershipExactly(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	table, err := NewOutboundTable(OutboundConfig{Capacity: 8, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first := rpcRequest(t, "conversation-a")
	second := rpcRequest(t, "conversation-b")
	second.Recipient = "agent-c"
	second.ConversationID, _ = conversation.DeriveID(second.MeshID, second.Sender, second.Recipient)
	deadline := now.Add(time.Minute)
	if err := table.Register(first, "first", deadline); err != nil {
		t.Fatal(err)
	}
	if err := table.Register(second, "second", deadline); err != nil {
		t.Fatal(err)
	}
	if !table.ActiveConversation(first.ConversationID) || !table.ActiveConversation(second.ConversationID) {
		t.Fatal("registered conversations were not indexed")
	}
	if err := table.Complete(rpcResponse(t, first)); err != nil {
		t.Fatal(err)
	}
	if !table.ActiveConversation(first.ConversationID) {
		t.Fatal("terminal response stopped blocking before its waiter consumed it")
	}
	if _, err := table.Wait(context.Background(), first.CorrelationID); err != nil {
		t.Fatal(err)
	}
	if table.ActiveConversation(first.ConversationID) || !table.ActiveConversation(second.ConversationID) {
		t.Fatal("consuming one conversation changed unrelated ownership")
	}
	if err := table.Fail(second.CorrelationID, ErrCancelled); err != nil {
		t.Fatal(err)
	}
	if !table.ActiveConversation(second.ConversationID) {
		t.Fatal("terminal error stopped blocking before its waiter consumed it")
	}
	if _, err := table.Wait(context.Background(), second.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("failed wait=%v", err)
	}
	if table.ActiveConversation(second.ConversationID) || len(table.active) != 0 {
		t.Fatalf("outbound conversation index leaked: %#v", table.active)
	}
}

func TestOutboundConversationAccountingExpiryAndShutdown(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	table, _ := NewOutboundTable(OutboundConfig{Capacity: 8, Now: func() time.Time { return now }})
	expired := rpcRequest(t, "expired")
	if err := table.Register(expired, "expired", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if table.ActiveConversation(expired.ConversationID) || table.Len() != 0 {
		t.Fatal("non-waiting expired request retained conversation ownership")
	}

	current := rpcRequest(t, "current")
	deadline := now.Add(time.Minute)
	if err := table.Register(current, "current", deadline); err != nil {
		t.Fatal(err)
	}
	if failed := table.FailAll(ErrCancelled); failed != 1 || !table.ActiveConversation(current.ConversationID) {
		t.Fatalf("fail all=%d active=%t", failed, table.ActiveConversation(current.ConversationID))
	}
	if _, err := table.Wait(context.Background(), current.CorrelationID); !errors.Is(err, ErrCancelled) {
		t.Fatalf("shutdown wait=%v", err)
	}
	if len(table.active) != 0 {
		t.Fatalf("shutdown index leaked: %#v", table.active)
	}
}

func TestInboundConversationAccountingLifecycle(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	table, err := NewInboundTable(InboundConfig{Capacity: 8, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first := rpcRequest(t, "inbound-a")
	second := rpcRequest(t, "inbound-b")
	second.Recipient = "agent-c"
	second.ConversationID, _ = conversation.DeriveID(second.MeshID, second.Sender, second.Recipient)
	deadline := now.Add(time.Minute)
	firstHandle, err := table.CreateHandle(first, deadline)
	if err != nil {
		t.Fatal(err)
	}
	secondHandle, err := table.CreateHandle(second, deadline)
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := table.BeginReply(firstHandle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.ReleaseReply(lease); err != nil || !table.ActiveConversation(first.ConversationID) {
		t.Fatalf("released reply active=%t err=%v", table.ActiveConversation(first.ConversationID), err)
	}
	_, lease, err = table.BeginReply(firstHandle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(lease); err != nil || table.ActiveConversation(first.ConversationID) {
		t.Fatalf("committed reply active=%t err=%v", table.ActiveConversation(first.ConversationID), err)
	}
	if err := table.Cancel(secondHandle); err != nil || table.ActiveConversation(second.ConversationID) {
		t.Fatalf("cancelled reply active=%t err=%v", table.ActiveConversation(second.ConversationID), err)
	}

	expiring := rpcRequest(t, "inbound-expiry")
	handle, err := table.CreateHandle(expiring, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if table.ActiveConversation(expiring.ConversationID) || table.Len() != 0 {
		t.Fatalf("expired handle %q retained ownership", handle)
	}

	current := rpcRequest(t, "inbound-current")
	_, _ = table.CreateHandle(current, now.Add(time.Minute))
	if cancelled := table.CancelAll(); cancelled != 1 || len(table.active) != 0 {
		t.Fatalf("cancel all=%d index=%#v", cancelled, table.active)
	}
}

func TestConversationRetainedOwnershipQueriesDoNotInvokeClock(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	request := rpcRequest(t, "retained-query")
	outbound, _ := NewOutboundTable(OutboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err := outbound.Register(request, "request", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	inbound, _ := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if _, err := inbound.CreateHandle(request, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	outbound.now = func() time.Time { panic("retained ownership consulted outbound clock") }
	inbound.now = func() time.Time { panic("retained ownership consulted inbound clock") }
	if !outbound.RetainsConversation(request.ConversationID) || !inbound.RetainsConversation(request.ConversationID) {
		t.Fatal("retained ownership query lost indexed entry")
	}
}

func TestConversationAccountingConcurrentTerminalTransitionsDoNotLeak(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	const requests = 128
	outbound, _ := NewOutboundTable(OutboundConfig{Capacity: requests, Now: func() time.Time { return now }})
	inbound, _ := NewInboundTable(InboundConfig{Capacity: requests, Now: func() time.Time { return now }})
	type ownership struct {
		request protocol.Envelope
		handle  string
	}
	owned := make([]ownership, 0, requests)
	for index := 0; index < requests; index++ {
		request := rpcRequest(t, "concurrent")
		if index%2 != 0 {
			request.Recipient = "agent-c"
			request.ConversationID, _ = conversation.DeriveID(request.MeshID, request.Sender, request.Recipient)
		}
		if err := outbound.Register(request, request.MessageID, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		handle, err := inbound.CreateHandle(request, now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		owned = append(owned, ownership{request: request, handle: handle})
	}
	var workers sync.WaitGroup
	for index := range owned {
		entry := owned[index]
		workers.Add(2)
		go func() {
			defer workers.Done()
			if err := outbound.Fail(entry.request.CorrelationID, ErrCancelled); err != nil {
				t.Errorf("outbound fail: %v", err)
				return
			}
			if _, err := outbound.Wait(context.Background(), entry.request.CorrelationID); !errors.Is(err, ErrCancelled) {
				t.Errorf("outbound wait: %v", err)
			}
		}()
		go func() {
			defer workers.Done()
			if err := inbound.Cancel(entry.handle); err != nil {
				t.Errorf("inbound cancel: %v", err)
			}
		}()
	}
	workers.Wait()
	if outbound.Len() != 0 || inbound.Len() != 0 || len(outbound.active) != 0 || len(inbound.active) != 0 {
		t.Fatalf("concurrent cleanup leaked: outbound=%d/%#v inbound=%d/%#v", outbound.Len(), outbound.active, inbound.Len(), inbound.active)
	}
}
