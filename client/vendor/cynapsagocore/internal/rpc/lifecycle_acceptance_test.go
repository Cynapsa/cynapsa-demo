package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestAcceptanceRandomizedOutboundLifecycleRejectsEveryLateResponse(t *testing.T) {
	base := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	for seed := uint64(0); seed < 256; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%03d", seed), func(t *testing.T) {
			now := base
			table, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			request := acceptanceRPCRequest(t, seed)
			deadline := base.Add(time.Minute)
			if err := table.Register(request, "command", deadline); err != nil {
				t.Fatal(err)
			}

			response := acceptanceRPCResponse(t, request, seed)
			var want error
			switch seed % 4 {
			case 0:
				if err := table.Complete(response); err != nil {
					t.Fatal(err)
				}
				want = nil
			case 1:
				if err := table.Cancel(request.CorrelationID); err != nil {
					t.Fatal(err)
				}
				want = ErrCancelled
			case 2:
				now = deadline
				want = ErrExpired
			case 3:
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, got := table.Wait(ctx, request.CorrelationID)
				if !errors.Is(got, ErrCancelled) {
					t.Fatalf("context cancellation = %v", got)
				}
				want = ErrCancelled
			}

			if seed%4 != 3 {
				got, gotErr := table.Wait(context.Background(), request.CorrelationID)
				if !errors.Is(gotErr, want) {
					t.Fatalf("wait error = %v, want %v", gotErr, want)
				}
				if want == nil && got.MessageID != response.MessageID {
					t.Fatalf("response identity changed: got %q want %q", got.MessageID, response.MessageID)
				}
			}
			if err := table.Complete(response); err == nil {
				t.Fatal("late or duplicate response was accepted")
			}

			now = deadline.Add(time.Second)
			table.Prune(now)
			if err := table.Register(request, "reused", now.Add(time.Minute)); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("correlation reused after retirement: %v", err)
			}
		})
	}
}

func TestAcceptanceReplyLeaseHasOneOwnerAcrossReleaseCancelAndTimeout(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	leaseEntropy := bytes.NewReader(bytes.Repeat([]byte{0x5a}, 16*4))
	table, err := NewInboundTable(InboundConfig{
		Capacity:     4,
		Now:          func() time.Time { return now },
		LeaseEntropy: leaseEntropy,
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := table.CreateHandle(acceptanceRPCRequest(t, 1), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	var successes atomic.Int64
	leases := make(chan ReplyLease, 64)
	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, lease, err := table.BeginReply(handle)
			if err == nil {
				successes.Add(1)
				leases <- lease
				return
			}
			if !errors.Is(err, ErrReplyInProgress) {
				t.Errorf("BeginReply = %v", err)
			}
		}()
	}
	wait.Wait()
	close(leases)
	if successes.Load() != 1 {
		t.Fatalf("lease owners = %d, want 1", successes.Load())
	}
	first := <-leases
	if err := table.ReleaseReply(first); err != nil {
		t.Fatal(err)
	}
	_, second, err := table.BeginReply(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.CommitReply(first); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("released lease committed after reacquisition: %v", err)
	}
	if err := table.CommitReply(second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.BeginReply(handle); !errors.Is(err, ErrCompleted) {
		t.Fatalf("committed handle reuse = %v", err)
	}

	cancelled, err := table.CreateHandle(acceptanceRPCRequest(t, 2), now.Add(time.Minute))
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
	if err := table.ReleaseReply(cancelledLease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("cancelled lease released: %v", err)
	}

	expiring, err := table.CreateHandle(acceptanceRPCRequest(t, 3), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, expiringLease, err := table.BeginReply(expiring)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := table.CommitReply(expiringLease); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired lease commit = %v", err)
	}
}

func TestAcceptanceRPCIssuanceBoundaryAndBoundedRetirementMemory(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	outbound, err := NewOutboundTable(OutboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	outbound.issuedCount = MaxCorrelationIssuance - 1
	last := acceptanceRPCRequest(t, 90)
	if err := outbound.Register(last, "last", now.Add(time.Minute)); err != nil {
		t.Fatalf("last correlation issuance rejected: %v", err)
	}
	if outbound.issuedCount != MaxCorrelationIssuance {
		t.Fatalf("issued count = %d", outbound.issuedCount)
	}
	if err := outbound.Register(acceptanceRPCRequest(t, 91), "overflow", now.Add(time.Minute)); !errors.Is(err, ErrIssuanceExhausted) {
		t.Fatalf("correlation overflow = %v", err)
	}

	inbound, err := NewInboundTable(InboundConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	inbound.issuedCount = MaxRequestHandleIssuance - 1
	if _, err := inbound.CreateHandle(acceptanceRPCRequest(t, 92), now.Add(time.Minute)); err != nil {
		t.Fatalf("last handle issuance rejected: %v", err)
	}
	if _, err := inbound.CreateHandle(acceptanceRPCRequest(t, 93), now.Add(time.Minute)); !errors.Is(err, ErrIssuanceExhausted) {
		t.Fatalf("handle overflow = %v", err)
	}

	// The never-reuse registries grow only to the explicit process ceiling;
	// active and retired request state remains constrained by table capacity.
	bounded, err := NewInboundTable(InboundConfig{Capacity: 1, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index < 1_024; index++ {
		handle, err := bounded.CreateHandle(acceptanceRPCRequest(t, 1_000+index), now.Add(time.Nanosecond))
		if err != nil {
			t.Fatalf("create %d: %v", index, err)
		}
		if _, err := bounded.ConsumeHandle(handle); err != nil {
			t.Fatalf("consume %d: %v", index, err)
		}
		now = now.Add(time.Nanosecond)
		bounded.Prune(now)
		if bounded.Len() > 1 || len(bounded.retired) > 1 {
			t.Fatalf("live state exceeded capacity: entries=%d retired=%d", bounded.Len(), len(bounded.retired))
		}
	}
	if len(bounded.issued) != 1_024 || bounded.issuedCount != 1_024 {
		t.Fatalf("never-reuse registry = %d count=%d", len(bounded.issued), bounded.issuedCount)
	}
}

func acceptanceRPCRequest(t *testing.T, seed uint64) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", []byte(fmt.Sprintf("request-%d", seed)))
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
		Version: protocol.Version2, MessageID: messageID, ConversationID: conversationID,
		Sender: "agent-a", Recipient: "agent-b", MeshID: "mesh",
		Mode: protocol.ModeRequest, CorrelationID: correlationID,
		CreatedAt:        time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload, CredentialProof: []byte("proof"),
	}
}

func acceptanceRPCResponse(t *testing.T, request protocol.Envelope, seed uint64) protocol.Envelope {
	t.Helper()
	response := acceptanceRPCRequest(t, seed+10_000)
	response.Sender, response.Recipient = request.Recipient, request.Sender
	response.MeshID = request.MeshID
	response.ConversationID = request.ConversationID
	response.Mode = protocol.ModeResponse
	response.ExpiresAt = time.Time{}
	response.CorrelationID = request.CorrelationID
	response.ReplyTo = request.MessageID
	return response
}
