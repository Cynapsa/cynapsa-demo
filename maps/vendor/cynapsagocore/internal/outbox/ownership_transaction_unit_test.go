package outbox

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func rank1EvidenceFor(envelope protocol.Envelope, session string) Rank1ReceiptEvidence {
	return Rank1ReceiptEvidence{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: sha256.Sum256([]byte(session)),
	}
}

func acceptedRPCPair(t *testing.T) (protocol.Envelope, protocol.Envelope) {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", []byte("request"))
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh", "agent-a", "agent-b")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	request, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: conversationID, Sender: "agent-a", Recipient: "agent-b", MeshID: "mesh",
		Mode: protocol.ModeRequest, CreatedAt: now, ExpiresAt: now.Add(time.Minute),
		ClockUncertainty: time.Millisecond, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	responsePayload, err := protocol.NewInlinePayload("aztm.native", []byte("response"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: request.ConversationID, Sender: request.Recipient, Recipient: request.Sender, MeshID: request.MeshID,
		Mode: protocol.ModeResponse, CorrelationID: request.CorrelationID, ReplyTo: request.MessageID,
		CreatedAt: now, ClockUncertainty: time.Millisecond, Payload: responsePayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return request, response
}

func TestReleaseRank2OwnedPreservesPayloadWithoutResurrectingRetiredClaims(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.ClaimRank2(request.MessageID, 19); err != nil {
		t.Fatal(err)
	}
	queue.Release(reservation)
	if err = queue.ReleaseRank2Owned(request.MessageID, 18); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("wrong ordinal release = %v", err)
	}
	if !queue.OwnsRank2(request.MessageID) {
		t.Fatal("wrong ordinal disturbed carrier claim")
	}
	if err = queue.ReleaseRank2Owned(request.MessageID, 19); err != nil {
		t.Fatal(err)
	}
	if ids := queue.EligibleMessageIDs(1); len(ids) != 1 || ids[0] != request.MessageID {
		t.Fatalf("released payload not schedulable: %v", ids)
	}
	if err = queue.ReleaseRank2Owned(request.MessageID, 19); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("duplicate release = %v", err)
	}
	reservation, err = queue.ReserveEligible(request.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.ClaimRank2(request.MessageID, 20); err != nil {
		t.Fatal(err)
	}
	queue.Release(reservation)
	if err = queue.ReleaseRank2Owned(request.MessageID, 19); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("stale release disturbed new claim: %v", err)
	}
	if err = queue.RetireRank2Owned(request.MessageID, 20); err != nil {
		t.Fatal(err)
	}
	if err = queue.ReleaseRank2Owned(request.MessageID, 20); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("retired payload resurrected: %v", err)
	}
	if count, _ := queue.Usage(); count != 0 {
		t.Fatalf("retired payload remains: %d", count)
	}
}

func TestAcceptedRPCResponseCrossesRank1AndRank2Ownership(t *testing.T) {
	for _, state := range []string{"rank1-pending", "rank2-owned"} {
		t.Run(state, func(t *testing.T) {
			queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			request, response := acceptedRPCPair(t)
			reservation, err := queue.EnqueueReserved(request)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "rank1-pending":
				evidence := rank1EvidenceFor(request, "lost-receipt-session")
				if _, err = queue.ClaimRank1PendingACK(reservation, evidence); err != nil {
					t.Fatal(err)
				}
			case "rank2-owned":
				if err = queue.ClaimRank2(request.MessageID, 19); err != nil || !queue.Release(reservation) {
					t.Fatalf("Rank2 setup err=%v", err)
				}
			}
			if err = queue.RetireAcceptedRPCResponse(request, response); err != nil {
				t.Fatal(err)
			}
			if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
				t.Fatalf("usage=%d/%d", messages, bytes)
			}
		})
	}
}

func TestAcceptedRPCResponsePreservesActiveRank2BorrowUntilRelease(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, response := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.ClaimRank2(request.MessageID, 29); err != nil {
		t.Fatal(err)
	}
	if err = queue.RetireAcceptedRPCResponse(request, response); err != nil {
		t.Fatal(err)
	}
	if _, err = queue.Get(request.MessageID); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("terminal request remained visible: %v", err)
	}
	if messages, bytes := queue.Usage(); messages != 1 || bytes == 0 {
		t.Fatalf("active borrow was cleared early: %d/%d", messages, bytes)
	}
	if !queue.Release(reservation) {
		t.Fatal("terminal active borrower did not release retained backing")
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("released usage=%d/%d", messages, bytes)
	}
}

func TestAcceptedRPCResponseRejectsForgedEvidenceAndLateDuplicateIsInert(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 3, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, response := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(request)
	if err != nil {
		t.Fatal(err)
	}
	evidence := rank1EvidenceFor(request, "lost-receipt-session")
	if _, err = queue.ClaimRank1PendingACK(reservation, evidence); err != nil {
		t.Fatal(err)
	}
	forged := response.Clone()
	forged.Sender = "attacker"
	if err = queue.RetireAcceptedRPCResponse(request, forged); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("forged response=%v", err)
	}
	if messages, _ := queue.Usage(); messages != 1 {
		t.Fatalf("forged response retired %d entries", messages)
	}
	if err = queue.RetireAcceptedRPCResponse(request, response); err != nil {
		t.Fatal(err)
	}
	successor := outboxEnvelope(t, "agent-a", "agent-b", []byte("successor"))
	if _, err = queue.Enqueue(successor); err != nil {
		t.Fatal(err)
	}
	if err = queue.RetireAcceptedRPCResponse(request, response); err != nil {
		t.Fatalf("late duplicate=%v", err)
	}
	if _, err = queue.Get(successor.MessageID); err != nil {
		t.Fatalf("late response touched successor: %v", err)
	}
}

func TestLateRPCResponseRequiresExactQueuedRequestBeforeRetirement(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, response := acceptedRPCPair(t)
	reservation, err := queue.EnqueueReserved(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.ClaimRank2(request.MessageID, 31); err != nil || !queue.Release(reservation) {
		t.Fatalf("Rank2 setup err=%v", err)
	}
	forged := response.Clone()
	forged.CorrelationID = "corr_AAAAAAAAAAAAAAAAAAAAAA"
	if queue.ValidateLateRPCResponse(forged) {
		t.Fatal("correlation-confused late response validated")
	}
	if err = queue.RetireLateRPCResponse(forged); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("correlation-confused late response=%v", err)
	}
	if !queue.ValidateLateRPCResponse(response) {
		t.Fatal("exact late response did not validate")
	}
	if err = queue.RetireLateRPCResponse(response); err != nil {
		t.Fatal(err)
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("late response usage=%d/%d", messages, bytes)
	}
	if err = queue.RetireLateRPCResponse(response); err != nil {
		t.Fatalf("idempotent late response=%v", err)
	}
}

func TestAcceptedRPCResponseRacesCarrierTerminalEvidence(t *testing.T) {
	for _, state := range []string{"rank1-pending", "rank2-owned"} {
		t.Run(state, func(t *testing.T) {
			for iteration := 0; iteration < 100; iteration++ {
				queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				request, response := acceptedRPCPair(t)
				reservation, err := queue.EnqueueReserved(request)
				if err != nil {
					t.Fatal(err)
				}
				var carrierTerminal func() error
				if state == "rank1-pending" {
					evidence := rank1EvidenceFor(request, "race-session")
					pending, claimErr := queue.ClaimRank1PendingACK(reservation, evidence)
					if claimErr != nil {
						t.Fatal(claimErr)
					}
					carrierTerminal = func() error { return queue.MarkRank1Acknowledged(pending, evidence) }
				} else {
					if err = queue.ClaimRank2(request.MessageID, 23); err != nil || !queue.Release(reservation) {
						t.Fatalf("Rank2 setup err=%v", err)
					}
					carrierTerminal = func() error { return queue.RetireRank2Owned(request.MessageID, 23) }
				}
				start := make(chan struct{})
				errs := make(chan error, 2)
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); <-start; errs <- carrierTerminal() }()
				go func() { defer wg.Done(); <-start; errs <- queue.RetireAcceptedRPCResponse(request, response) }()
				close(start)
				wg.Wait()
				close(errs)
				for result := range errs {
					if result != nil && !errors.Is(result, ErrUnknownEntry) {
						t.Fatalf("terminal race=%v", result)
					}
				}
				if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
					t.Fatalf("iteration %d usage=%d/%d", iteration, messages, bytes)
				}
			}
		})
	}
}

func TestRank1PendingACKExactEvidenceAndRank2FallbackOwnership(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("immutable"))
	if _, err = queue.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	reservation, err := queue.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" || reservation.FallbackOnly {
		t.Fatalf("reserve=%#v err=%v", reservation, err)
	}
	evidence := rank1EvidenceFor(envelope, "session-a")
	beforeMessages, beforeBytes := queue.Usage()
	pending, err := queue.ClaimRank1PendingACK(reservation, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if queue.Release(reservation) {
		t.Fatal("Rank1 transition retained a borrowed reservation")
	}
	if messages, bytes := queue.Usage(); messages != beforeMessages || bytes != beforeBytes {
		t.Fatalf("Rank1 transition changed accounting: before=%d/%d after=%d/%d", beforeMessages, beforeBytes, messages, bytes)
	}
	if err = queue.MarkTerminal(envelope.MessageID); !errors.Is(err, ErrOwnedEntry) {
		t.Fatalf("generic terminal crossed Rank1 ownership: %v", err)
	}
	if again, _ := queue.ReserveEligible(envelope.MessageID); again.MessageID != "" {
		t.Fatal("Rank1-pending entry was reservable")
	}
	forged := evidence
	forged.ChannelBinding = sha256.Sum256([]byte("session-b"))
	if err = queue.MarkRank1Acknowledged(pending, forged); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("forged receipt=%v", err)
	}
	if err = queue.MakeRank2FallbackEligible(pending, evidence); err != nil {
		t.Fatal(err)
	}
	if err = queue.MarkRank1Acknowledged(pending, evidence); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("late receipt after fallback=%v", err)
	}
	reservation, err = queue.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" || !reservation.FallbackOnly {
		t.Fatalf("fallback reserve=%#v err=%v", reservation, err)
	}
	if err = queue.ClaimRank2(envelope.MessageID, 1); err != nil {
		t.Fatalf("same reservation Rank2 claim=%v", err)
	}
	if !queue.Release(reservation) || !queue.OwnsRank2(envelope.MessageID) {
		t.Fatal("Rank2 ownership did not survive reservation release")
	}
	if _, err = queue.ClaimRank1PendingACK(reservation, evidence); err == nil {
		t.Fatal("Rank2-owned entry migrated back to Rank1")
	}
	if removed := queue.MarkRank2Handled(1); len(removed) != 1 || removed[0] != envelope.MessageID {
		t.Fatalf("handled=%v", removed)
	}
}

func TestRank1ReceiptTerminalAndLateDuplicateFailClosed(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("terminal"))
	reservation, err := queue.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := rank1EvidenceFor(envelope, "session-a")
	pending, err := queue.ClaimRank1PendingACK(reservation, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.MarkRank1Acknowledged(pending, evidence); err != nil {
		t.Fatal(err)
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("usage=%d/%d", messages, bytes)
	}
	if err = queue.MarkRank1Acknowledged(pending, evidence); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("duplicate receipt=%v", err)
	}
}

func TestRank2OwnerRetirementRequiresExactTransportOrdinal(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("rank2-owned"))
	reservation, err := queue.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.ClaimRank2(envelope.MessageID, 17); err != nil {
		t.Fatal(err)
	}
	if !queue.Release(reservation) {
		t.Fatal("Rank2 claim did not release exact borrow")
	}
	if err = queue.MarkTerminal(envelope.MessageID); !errors.Is(err, ErrOwnedEntry) {
		t.Fatalf("generic terminal crossed Rank2 ownership: %v", err)
	}
	if err = queue.RetireRank2Owned(envelope.MessageID, 18); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("wrong owner ordinal retired entry: %v", err)
	}
	if err = queue.RetireRank2Owned(envelope.MessageID, 17); err != nil {
		t.Fatalf("exact Rank2 owner retirement: %v", err)
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("retired usage=%d/%d", messages, bytes)
	}
}

func TestRank1PendingACKCannotSurvivePeerRemoval(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("stale"))
	reservation, err := queue.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := rank1EvidenceFor(envelope, "old-session")
	pending, err := queue.ClaimRank1PendingACK(reservation, evidence)
	if err != nil {
		t.Fatal(err)
	}
	retired := queue.RetirePeerOwned(envelope.MeshID, envelope.Recipient)
	if len(retired) != 1 || retired[0].MessageID != envelope.MessageID {
		t.Fatalf("retired=%#v", retired)
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("retired usage=%d/%d", messages, bytes)
	}
	if err = queue.MarkRank1Acknowledged(pending, evidence); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("removed peer receipt=%v", err)
	}
	if err = queue.MakeRank2FallbackEligible(pending, evidence); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("removed peer fallback=%v", err)
	}
}

func TestReservationSerializesDropRank2ClaimAndHandledEvidence(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("secret"))
	if _, err = queue.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	reservation, err := queue.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" {
		t.Fatalf("reservation=%#v err=%v", reservation, err)
	}
	if err = queue.MarkUnownedTerminal(envelope.MessageID); !errors.Is(err, ErrOwnedEntry) {
		t.Fatalf("drop crossed reservation: %v", err)
	}
	if err = queue.ClaimRank2(envelope.MessageID, 1); err != nil {
		t.Fatal(err)
	}
	if !queue.Release(reservation) || !queue.OwnsRank2(envelope.MessageID) {
		t.Fatal("borrow release revoked Rank2 owner")
	}
	if err = queue.MarkUnownedTerminal(envelope.MessageID); !errors.Is(err, ErrOwnedEntry) {
		t.Fatalf("drop crossed Rank2 ownership: %v", err)
	}
	if removed := queue.MarkRank2Handled(0); len(removed) != 0 {
		t.Fatalf("zero ack removed=%v", removed)
	}
	if removed := queue.MarkRank2Handled(1); len(removed) != 1 || removed[0] != envelope.MessageID {
		t.Fatalf("handled removed=%v", removed)
	}
	if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("usage=%d,%d", messages, bytes)
	}
}

func TestDropTransactionWaitsForExactBorrowerOutcome(t *testing.T) {
	prepare := func(t *testing.T) (*Outbox, protocol.Envelope, Reservation, <-chan error) {
		t.Helper()
		queue, err := New(Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte("borrowed secret"))
		if _, err = queue.Enqueue(envelope); err != nil {
			t.Fatal(err)
		}
		reservation, err := queue.ReserveEligible(envelope.MessageID)
		if err != nil || reservation.MessageID == "" {
			t.Fatalf("reservation=%#v err=%v", reservation, err)
		}
		wait, err := queue.DropUnowned(envelope.MessageID)
		if err != nil || wait == nil {
			t.Fatalf("drop wait=%v err=%v", wait, err)
		}
		if duplicate, duplicateErr := queue.DropUnowned(envelope.MessageID); duplicate != nil || !errors.Is(duplicateErr, ErrOwnedEntry) {
			t.Fatalf("duplicate drop wait=%v err=%v", duplicate, duplicateErr)
		}
		return queue, envelope, reservation, wait
	}

	t.Run("no handoff release completes drop", func(t *testing.T) {
		queue, _, reservation, wait := prepare(t)
		if !queue.Release(reservation) {
			t.Fatal("borrow release failed")
		}
		if result := <-wait; result != nil {
			t.Fatalf("drop result=%v", result)
		}
		if messages, bytes := queue.Usage(); messages != 0 || bytes != 0 {
			t.Fatalf("usage=%d,%d", messages, bytes)
		}
	})

	t.Run("cancel revokes pending destructive intent", func(t *testing.T) {
		queue, envelope, reservation, wait := prepare(t)
		if !queue.CancelDrop(envelope.MessageID, wait) {
			t.Fatal("exact drop cancellation failed")
		}
		if queue.CancelDrop(envelope.MessageID, wait) {
			t.Fatal("stale drop cancellation succeeded")
		}
		if !queue.Release(reservation) {
			t.Fatal("borrow release failed")
		}
		if messages, _ := queue.Usage(); messages != 1 {
			t.Fatalf("cancelled drop later deleted queue entry: %d", messages)
		}
	})

	t.Run("Rank2 claim rejects drop", func(t *testing.T) {
		queue, envelope, reservation, wait := prepare(t)
		if err := queue.ClaimRank2(envelope.MessageID, 1); err != nil {
			t.Fatal(err)
		}
		if result := <-wait; !errors.Is(result, ErrOwnedEntry) {
			t.Fatalf("drop result=%v", result)
		}
		if !queue.Release(reservation) || !queue.OwnsRank2(envelope.MessageID) {
			t.Fatal("Rank2 ownership did not survive borrower release")
		}
		queue.MarkRank2Handled(1)
	})

	t.Run("Rank1 terminal rejects drop", func(t *testing.T) {
		queue, _, reservation, wait := prepare(t)
		if err := queue.MarkReservedTerminal(reservation); err != nil {
			t.Fatal(err)
		}
		if result := <-wait; !errors.Is(result, ErrOwnedEntry) {
			t.Fatalf("drop result=%v", result)
		}
	})

	t.Run("peer retirement rejects drop", func(t *testing.T) {
		queue, _, reservation, wait := prepare(t)
		if retired := queue.RetirePeerOwned("mesh", "agent-b"); len(retired) != 1 {
			t.Fatalf("retired=%v", retired)
		}
		if result := <-wait; !errors.Is(result, ErrOwnedEntry) {
			t.Fatalf("drop result=%v", result)
		}
		if !queue.Release(reservation) {
			t.Fatal("retired borrower release failed")
		}
	})
}

func TestMessageIndexAtMaximumCapacityIsExact(t *testing.T) {
	store, err := NewMemoryStore(MaxMessageCapacity, MaxByteCapacity)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	for ordinal := uint64(1); ordinal <= uint64(MaxMessageCapacity); ordinal++ {
		messageID := fmt.Sprintf("msg-index-%05d", ordinal)
		envelope := firstIndexedEnvelope(messageID)
		key := messageKey(envelope)
		store.byOrdinal[ordinal] = storedEntry{entry: Entry{Envelope: envelope, Ordinal: ordinal}, messageKey: key}
		store.byMessage[key] = ordinal
		store.byID[messageID] = ordinal
	}
	store.mu.Unlock()
	last := fmt.Sprintf("msg-index-%05d", MaxMessageCapacity)
	if err := store.Delete(last); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(last); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("deleted index lookup=%v", err)
	}
}

func TestBoundedHeadSchedulingRoundRobinsPastDrainSlotCount(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 16, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var ninth string
	for index := 0; index < 9; index++ {
		envelope := outboxEnvelope(t, "agent-a", fmt.Sprintf("agent-%d", index), []byte("payload"))
		if _, err := queue.Enqueue(envelope); err != nil {
			t.Fatal(err)
		}
		if index == 8 {
			ninth = envelope.MessageID
		}
	}
	first := queue.EligibleMessageIDs(8)
	if len(first) != 8 {
		t.Fatalf("first round heads=%d", len(first))
	}
	for _, id := range first {
		reservation, reserveErr := queue.ReserveEligible(id)
		if reserveErr != nil || reservation.MessageID == "" || !queue.Release(reservation) {
			t.Fatalf("reserve/release %q = %#v, %v", id, reservation, reserveErr)
		}
	}
	second := queue.EligibleMessageIDs(8)
	if len(second) != 8 || second[0] != ninth {
		t.Fatalf("second round did not rotate to ninth conversation: %v", second)
	}
}

func firstIndexedEnvelope(messageID string) protocol.Envelope {
	return protocol.Envelope{MessageID: messageID, MeshID: "mesh", Sender: "agent-a", Recipient: "agent-b", ConversationID: "conversation"}
}

func TestProvisionalAbortLeavesIndependentSuccessorAndCannotOwnDuplicate(t *testing.T) {
	queue, err := New(Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first := outboxEnvelope(t, "agent-a", "agent-b", []byte("first"))
	second := outboxEnvelope(t, "agent-a", "agent-b", []byte("second"))
	second.ConversationID = first.ConversationID
	provisional, err := queue.EnqueueProvisional(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = queue.Enqueue(second); err != nil {
		t.Fatal(err)
	}
	if ids := queue.EligibleMessageIDs(2); len(ids) != 1 || ids[0] != second.MessageID {
		t.Fatalf("independent successor heads=%v", ids)
	}
	if err = queue.AbortProvisional(provisional); err != nil {
		t.Fatal(err)
	}
	if ids := queue.EligibleMessageIDs(2); len(ids) != 1 || ids[0] != second.MessageID {
		t.Fatalf("successor heads=%v", ids)
	}
	if _, err = queue.EnqueueProvisional(second); !errors.Is(err, ErrDuplicateConflict) {
		t.Fatalf("duplicate provisional=%v", err)
	}
	if _, err = queue.Get(second.MessageID); err != nil {
		t.Fatalf("duplicate altered accepted entry: %v", err)
	}
}
