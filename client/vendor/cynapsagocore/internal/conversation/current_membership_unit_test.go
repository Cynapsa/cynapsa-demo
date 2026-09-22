package conversation

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func dedupeTestEnvelope(t *testing.T, now time.Time, lane LaneID, messageNumber int) protocol.Envelope {
	t.Helper()
	payload := []byte("payload")
	var entropy [16]byte
	binary.BigEndian.PutUint64(entropy[8:], uint64(messageNumber))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: "msg_" + base64.RawURLEncoding.EncodeToString(entropy[:]), ConversationID: lane.ConversationID(),
		Sender: lane.Sender(), Recipient: lane.Recipient(), MeshID: lane.MeshID(),
		Mode: protocol.ModeMessage, CreatedAt: now, ClockUncertainty: time.Millisecond,
		Payload: protocol.PayloadDescriptor{Kind: protocol.PayloadInline, Profile: "opaque.binary", Inline: payload, Size: int64(len(payload)), Digest: sha256.Sum256(payload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestConversationIdentityHasNoGenerationOrDirection(t *testing.T) {
	forward, err := DeriveID("mesh", "a@example/mesh", "b@example/mesh")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := DeriveID("mesh", "b@example/mesh", "a@example/mesh")
	if err != nil || reverse != forward {
		t.Fatalf("reverse=%q forward=%q err=%v", reverse, forward, err)
	}
	if other, err := DeriveID("other", "a@example/mesh", "b@example/mesh"); err != nil || other == forward {
		t.Fatalf("other=%q err=%v", other, err)
	}
}

func TestDeduperUsesStableMessageIdentity(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	lane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "a@example/mesh", Recipient: "b@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("payload")
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", ConversationID: lane.ConversationID(),
		Sender: lane.Sender(), Recipient: lane.Recipient(), MeshID: lane.MeshID(),
		Mode: protocol.ModeMessage, CreatedAt: now, ClockUncertainty: time.Millisecond,
		Payload: protocol.PayloadDescriptor{Kind: protocol.PayloadInline, Profile: "opaque.binary", Inline: payload, Size: int64(len(payload)), Digest: sha256.Sum256(payload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	deduper, err := NewDeduper(DedupeConfig{GlobalCapacity: 2, PerPeerCapacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeNew {
		t.Fatalf("first=%q err=%v", got, err)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeDuplicateInFlight {
		t.Fatalf("duplicate=%q err=%v", got, err)
	}
	if err := deduper.MarkTerminal(lane, envelope); err != nil {
		t.Fatal(err)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeDuplicateTerminal {
		t.Fatalf("terminal=%q err=%v", got, err)
	}
}

func TestDeduperTreatsScopedMessageIDAsDuplicateRegardlessOfContent(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	lane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "a@example/mesh", Recipient: "b@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	originalPayload := []byte("original")
	original, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: "msg_AQEBAQEBAQEBAQEBAQEBAQ", ConversationID: lane.ConversationID(),
		Sender: lane.Sender(), Recipient: lane.Recipient(), MeshID: lane.MeshID(),
		Mode: protocol.ModeMessage, CreatedAt: now, ClockUncertainty: time.Millisecond,
		Payload: protocol.PayloadDescriptor{Kind: protocol.PayloadInline, Profile: "opaque.binary", Inline: originalPayload, Size: int64(len(originalPayload)), Digest: sha256.Sum256(originalPayload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	changedPayload := []byte("changed")
	changed, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: original.MessageID, ConversationID: original.ConversationID,
		Sender: original.Sender, Recipient: original.Recipient, MeshID: original.MeshID,
		Mode: original.Mode, CreatedAt: original.CreatedAt, ClockUncertainty: original.ClockUncertainty,
		Payload: protocol.PayloadDescriptor{Kind: protocol.PayloadInline, Profile: "opaque.binary", Inline: changedPayload, Size: int64(len(changedPayload)), Digest: sha256.Sum256(changedPayload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	deduper, err := NewDeduper(DedupeConfig{GlobalCapacity: 2, PerPeerCapacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := deduper.Check(lane, original); err != nil || got != DedupeNew {
		t.Fatalf("first=%q err=%v", got, err)
	}
	if got, err := deduper.Check(lane, changed); err != nil || got != DedupeDuplicateInFlight {
		t.Fatalf("changed in-flight=%q err=%v", got, err)
	}
	if err := deduper.MarkTerminal(lane, changed); err != nil {
		t.Fatalf("mark changed terminal: %v", err)
	}
	if got, err := deduper.Check(lane, changed); err != nil || got != DedupeDuplicateTerminal {
		t.Fatalf("changed terminal=%q err=%v", got, err)
	}
}

func TestDefaultDedupeRetentionCoversMailboxReplayBoundary(t *testing.T) {
	if DefaultDedupeRetention <= MaximumMailboxReplayRetention {
		t.Fatalf("dedupe retention %s must outlive mailbox replay %s", DefaultDedupeRetention, MaximumMailboxReplayRetention)
	}

	now := time.Unix(30, 0).UTC()
	lane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "a@example/mesh", Recipient: "b@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("mailbox replay boundary")
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID: "msg_AgICAgICAgICAgICAgICAg", ConversationID: lane.ConversationID(),
		Sender: lane.Sender(), Recipient: lane.Recipient(), MeshID: lane.MeshID(),
		Mode: protocol.ModeMessage, CreatedAt: now, ClockUncertainty: time.Millisecond,
		Payload: protocol.PayloadDescriptor{Kind: protocol.PayloadInline, Profile: "opaque.binary", Inline: payload, Size: int64(len(payload)), Digest: sha256.Sum256(payload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	deduper, err := NewDeduper(DedupeConfig{GlobalCapacity: 2, PerPeerCapacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeNew {
		t.Fatalf("first=%q err=%v", got, err)
	}
	if err := deduper.MarkTerminal(lane, envelope); err != nil {
		t.Fatal(err)
	}

	now = now.Add(MaximumMailboxReplayRetention)
	if removed := deduper.Prune(now); removed != 0 {
		t.Fatalf("pruned %d terminal IDs at mailbox retention boundary", removed)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeDuplicateTerminal {
		t.Fatalf("mailbox-boundary replay=%q err=%v", got, err)
	}

	now = now.Add(DedupeReplaySafetyMargin - time.Nanosecond)
	if removed := deduper.Prune(now); removed != 0 {
		t.Fatalf("pruned %d terminal IDs before dedupe expiry", removed)
	}
	now = now.Add(time.Nanosecond)
	if removed := deduper.Prune(now); removed != 1 {
		t.Fatalf("pruned %d terminal IDs at dedupe expiry, want 1", removed)
	}
	if got, err := deduper.Check(lane, envelope); err != nil || got != DedupeNew {
		t.Fatalf("post-expiry replay=%q err=%v", got, err)
	}
}

func TestDeduperEnforcesIndependentGlobalAndPerPeerBoundsUntilExpiry(t *testing.T) {
	now := time.Unix(40, 0).UTC()
	firstLane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "a@example/mesh", Recipient: "local@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	secondLane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "b@example/mesh", Recipient: "local@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	deduper, err := NewDeduper(DedupeConfig{
		GlobalCapacity: 3, PerPeerCapacity: 2, Retention: time.Hour,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	for index := 1; index <= 2; index++ {
		envelope := dedupeTestEnvelope(t, now, firstLane, index)
		if got, err := deduper.Check(firstLane, envelope); err != nil || got != DedupeNew {
			t.Fatalf("first peer message %d = %q, %v", index, got, err)
		}
		if err := deduper.MarkTerminal(firstLane, envelope); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := deduper.Check(firstLane, dedupeTestEnvelope(t, now, firstLane, 3)); !errors.Is(err, ErrDedupeCapacity) {
		t.Fatalf("per-peer boundary error = %v", err)
	}

	secondEnvelope := dedupeTestEnvelope(t, now, secondLane, 4)
	if got, err := deduper.Check(secondLane, secondEnvelope); err != nil || got != DedupeNew {
		t.Fatalf("second peer message = %q, %v", got, err)
	}
	if err := deduper.MarkTerminal(secondLane, secondEnvelope); err != nil {
		t.Fatal(err)
	}
	thirdLane, err := NewLaneID(AuthenticatedBinding{MeshID: "mesh", AuthenticatedSender: "c@example/mesh", Recipient: "local@example/mesh"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deduper.Check(thirdLane, dedupeTestEnvelope(t, now, thirdLane, 5)); !errors.Is(err, ErrDedupeCapacity) {
		t.Fatalf("global boundary error = %v", err)
	}

	now = now.Add(time.Hour)
	if removed := deduper.Prune(now); removed != 3 {
		t.Fatalf("expired terminal identities removed = %d, want 3", removed)
	}
	if got, err := deduper.Check(firstLane, dedupeTestEnvelope(t, now, firstLane, 3)); err != nil || got != DedupeNew {
		t.Fatalf("post-expiry admission = %q, %v", got, err)
	}
}
