package outbox

import (
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestOutboxStableOrdinalAndCarrierConvergence(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	outbox, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	canonical := []byte("canonical application payload")
	inline := outboxEnvelope(t, "agent-a", "agent-b", canonical)
	ordinal, err := outbox.Enqueue(inline)
	if err != nil || ordinal != 1 {
		t.Fatalf("first enqueue ordinal=%d err=%v", ordinal, err)
	}
	if repeated, err := outbox.Enqueue(inline.Clone()); err != nil || repeated != ordinal {
		t.Fatalf("idempotent enqueue ordinal=%d err=%v", repeated, err)
	}
	refreshed := inline.Clone()
	refreshed.CredentialProof = []byte("refreshed-proof")
	if repeated, err := outbox.Enqueue(refreshed); err != nil || repeated != ordinal {
		t.Fatalf("proof refresh ordinal=%d err=%v", repeated, err)
	}
	refreshedPending, err := outbox.Pending()
	if err != nil || len(refreshedPending) != 1 || string(refreshedPending[0].CredentialProof) != "refreshed-proof" {
		t.Fatalf("proof refresh pending=%#v err=%v", refreshedPending, err)
	}

	digest := sha256.Sum256(canonical)
	objectPayload, err := protocol.NewReferencedPayload(protocol.PayloadObjectReference, inline.Payload.Profile, "object://private/one", int64(len(canonical)), digest, "enc-1")
	if err != nil {
		t.Fatal(err)
	}
	object := inline.Clone()
	object.Payload = objectPayload
	if replaced, err := outbox.Enqueue(object); err != nil || replaced != ordinal {
		t.Fatalf("object transition ordinal=%d err=%v", replaced, err)
	}

	transferPayload, err := protocol.NewReferencedPayload(protocol.PayloadTransferReference, inline.Payload.Profile, "transfer://private/one", int64(len(canonical)), digest, "enc-2")
	if err != nil {
		t.Fatal(err)
	}
	transfer := inline.Clone()
	transfer.Payload = transferPayload
	if replaced, err := outbox.Enqueue(transfer); err != nil || replaced != ordinal {
		t.Fatalf("transfer transition ordinal=%d err=%v", replaced, err)
	}
	pending, err := outbox.Pending()
	if err != nil || len(pending) != 1 || pending[0].Payload.Reference != transferPayload.Reference {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if messages, _ := outbox.Usage(); messages != 1 {
		t.Fatalf("carrier transition allocated %d entries", messages)
	}

	conflict := transfer.Clone()
	conflict.Payload, err = protocol.NewInlinePayload(transfer.Payload.Profile, []byte("different logical payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Enqueue(conflict); !errors.Is(err, ErrDuplicateConflict) {
		t.Fatalf("logical conflict: %v", err)
	}
}

func TestOutboxCanonicalByteAndMessageCapacity(t *testing.T) {
	first := outboxEnvelope(t, "agent-a", "agent-b", []byte("one"))
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(first)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := New(Config{MessageCapacity: 2, ByteCapacity: int64(len(encoded))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exact.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if messages, bytes := exact.Usage(); messages != 1 || bytes != int64(len(encoded)) {
		t.Fatalf("usage messages=%d bytes=%d want=%d", messages, bytes, len(encoded))
	}
	if _, err := exact.Enqueue(outboxEnvelope(t, "agent-a", "agent-b", []byte("two"))); !errors.Is(err, ErrCapacity) {
		t.Fatalf("byte capacity: %v", err)
	}

	bounded, err := New(Config{MessageCapacity: 1, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bounded.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if _, err := bounded.Enqueue(outboxEnvelope(t, "agent-a", "agent-b", []byte("second"))); !errors.Is(err, ErrCapacity) {
		t.Fatalf("message capacity: %v", err)
	}

	store, err := NewMemoryStore(1, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(Entry{Envelope: first, QueuedAt: time.Now(), Bytes: 1, Ordinal: 1}); err != nil {
		t.Fatal(err)
	}
	if _, bytes := store.usage(); bytes != int64(len(encoded)) {
		t.Fatalf("caller-controlled byte accounting: %d", bytes)
	}
}

func TestOutboxHandledThroughAndIdentityPreservingReplay(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var originals []protocol.Envelope
	for _, body := range []string{"one", "two", "three"} {
		envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte(body))
		originals = append(originals, envelope)
		if _, err := outbox.Enqueue(envelope); err != nil {
			t.Fatal(err)
		}
	}
	if err := outbox.MarkHandled(2); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkHandled(1); !errors.Is(err, ErrHandledRegression) {
		t.Fatalf("regression: %v", err)
	}
	if err := outbox.MarkHandled(4); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("future evidence: %v", err)
	}
	pending, err := outbox.Pending()
	if err != nil || len(pending) != 1 || pending[0].MessageID != originals[2].MessageID {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	pending[0].CredentialProof[0] ^= 0xff
	again, _ := outbox.Pending()
	if again[0].CredentialProof[0] != originals[2].CredentialProof[0] {
		t.Fatal("pending snapshot aliased retained envelope")
	}
	if err := outbox.MarkTerminal(originals[2].MessageID); err != nil {
		t.Fatal(err)
	}
	if messages, bytes := outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("terminal usage messages=%d bytes=%d", messages, bytes)
	}
}

func TestOutboxRejectsGlobalMessageIDCollision(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first := outboxEnvelope(t, "agent-a", "agent-b", []byte("one"))
	second := outboxEnvelope(t, "agent-c", "agent-b", []byte("two"))
	second.MessageID = first.MessageID
	if _, err := outbox.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Enqueue(second); !errors.Is(err, ErrDuplicateConflict) {
		t.Fatalf("message ID collision=%v", err)
	}
	if err := outbox.MarkTerminal(first.MessageID); err != nil {
		t.Fatalf("unique terminal evidence: %v", err)
	}
	if messages, _ := outbox.Usage(); messages != 0 {
		t.Fatalf("terminal evidence retained messages: %d", messages)
	}
}

func TestOutboxConcurrentCapacityIsFailClosed(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 32, ByteCapacity: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for index := range 128 {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			envelope := outboxEnvelope(t, "agent-a", "agent-b", []byte{byte(index), byte(index >> 8)})
			if _, err := outbox.Enqueue(envelope); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.Is(err, ErrCapacity) {
				t.Errorf("enqueue: %v", err)
			}
		}(index)
	}
	wait.Wait()
	if successes != 32 {
		t.Fatalf("successes=%d", successes)
	}
	if messages, _ := outbox.Usage(); messages != 32 {
		t.Fatalf("stored=%d", messages)
	}
}

func TestOutboxConfigurationBounds(t *testing.T) {
	if _, err := New(Config{MessageCapacity: MaxMessageCapacity, ByteCapacity: 1}); err != nil {
		t.Fatalf("maximum message capacity rejected: %v", err)
	}
	for _, config := range []Config{
		{},
		{MessageCapacity: -1, ByteCapacity: 1},
		{MessageCapacity: MaxMessageCapacity + 1, ByteCapacity: 1},
		{MessageCapacity: 1, ByteCapacity: 0},
	} {
		if _, err := New(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %#v: %v", config, err)
		}
	}
}

func outboxEnvelope(t *testing.T, sender, recipient string, canonical []byte) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("aztm.native", canonical)
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh", sender, recipient)
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		Version:          protocol.Version2,
		MessageID:        messageID,
		ConversationID:   conversationID,
		Sender:           sender,
		Recipient:        recipient,
		MeshID:           "mesh",
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	}
}
