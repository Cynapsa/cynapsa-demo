package outbox

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestAcceptanceOutboxExactByteArithmeticAndCarrierGrowthFailureIsAtomic(t *testing.T) {
	first := mustAcceptanceOutboxEnvelope(t, 1, []byte("first"))
	second := mustAcceptanceOutboxEnvelope(t, 2, []byte("second"))
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	firstWire, _ := codec.Encode(first)
	secondWire, _ := codec.Encode(second)
	exactBytes := int64(len(firstWire) + len(secondWire))
	outbox, err := New(Config{MessageCapacity: 3, ByteCapacity: exactBytes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Enqueue(second); err != nil {
		t.Fatal(err)
	}
	if messages, bytes := outbox.Usage(); messages != 2 || bytes != exactBytes {
		t.Fatalf("exact usage = (%d,%d), want (2,%d)", messages, bytes, exactBytes)
	}
	if _, err := outbox.Enqueue(mustAcceptanceOutboxEnvelope(t, 3, nil)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("byte overflow = %v", err)
	}

	canonical := make([]byte, 4<<10)
	for index := range canonical {
		canonical[index] = byte(index)
	}
	inline := mustAcceptanceOutboxEnvelope(t, 50, canonical)
	referencePayload, err := protocol.NewReferencedPayload(
		protocol.PayloadObjectReference, inline.Payload.Profile, "object://private/compact",
		int64(len(canonical)), sha256.Sum256(canonical), "enc-private",
	)
	if err != nil {
		t.Fatal(err)
	}
	reference := inline.Clone()
	reference.Payload = referencePayload
	referenceWire, err := codec.Encode(reference)
	if err != nil {
		t.Fatal(err)
	}
	growthBounded, err := New(Config{MessageCapacity: 1, ByteCapacity: int64(len(referenceWire))})
	if err != nil {
		t.Fatal(err)
	}
	ordinal, err := growthBounded.Enqueue(reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := growthBounded.Enqueue(inline); !errors.Is(err, ErrCapacity) {
		t.Fatalf("carrier growth overflow = %v", err)
	}
	pending, err := growthBounded.Pending()
	if err != nil || len(pending) != 1 || pending[0].Payload.Kind != protocol.PayloadObjectReference {
		t.Fatalf("failed replacement mutated entry: %#v err=%v", pending, err)
	}
	if entry, ok := growthBounded.store.lookup(reference); !ok || entry.Ordinal != ordinal || entry.Bytes != int64(len(referenceWire)) {
		t.Fatalf("failed replacement changed accounting: %#v ok=%v", entry, ok)
	}

	if _, err := New(Config{MessageCapacity: 1, ByteCapacity: MaxByteCapacity + 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("oversized aggregate RAM cap = %v", err)
	}
	maxBytes, err := New(Config{MessageCapacity: 1, ByteCapacity: MaxByteCapacity})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maxBytes.Enqueue(first); err != nil {
		t.Fatalf("maximum supported capacity rejected an entry: %v", err)
	}
}

func TestAcceptanceOutboxOrdinalHandledEvidenceAndProcessClearBoundaries(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkHandled(1); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("future evidence on empty outbox = %v", err)
	}
	for index := uint64(1); index <= 3; index++ {
		ordinal, err := outbox.Enqueue(mustAcceptanceOutboxEnvelope(t, index, []byte{byte(index)}))
		if err != nil || ordinal != index {
			t.Fatalf("enqueue %d ordinal=%d err=%v", index, ordinal, err)
		}
	}
	if err := outbox.MarkHandled(2); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkHandled(2); err != nil {
		t.Fatalf("idempotent handled evidence = %v", err)
	}
	if err := outbox.MarkHandled(1); !errors.Is(err, ErrHandledRegression) {
		t.Fatalf("handled regression = %v", err)
	}
	if err := outbox.MarkHandled(4); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("future handled evidence = %v", err)
	}
	pending, _ := outbox.Pending()
	if len(pending) != 1 {
		t.Fatalf("pending after handled-through = %d", len(pending))
	}
	originalProof := append([]byte(nil), pending[0].CredentialProof...)
	pending[0].CredentialProof[0] ^= 0xff
	again, _ := outbox.Pending()
	if string(again[0].CredentialProof) != string(originalProof) {
		t.Fatal("replay snapshot mutation reached retained entry")
	}

	fresh, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if messages, bytes := fresh.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("new process inherited state: messages=%d bytes=%d", messages, bytes)
	}
	if ordinal, err := fresh.Enqueue(mustAcceptanceOutboxEnvelope(t, 99, []byte("fresh"))); err != nil || ordinal != 1 {
		t.Fatalf("new process ordinal=%d err=%v", ordinal, err)
	}
	if oldPending, _ := outbox.Pending(); len(oldPending) != 1 {
		t.Fatal("new process mutated old process state")
	}
}

func TestAcceptanceOutboxConflictCarrierConvergenceAndAmbiguousEvidence(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	canonical := []byte("one logical canonical snapshot")
	inline := mustAcceptanceOutboxEnvelope(t, 10, canonical)
	ordinal, err := outbox.Enqueue(inline)
	if err != nil {
		t.Fatal(err)
	}

	referencePayload, err := protocol.NewReferencedPayload(
		protocol.PayloadTransferReference, inline.Payload.Profile, "transfer://private/replay",
		int64(len(canonical)), sha256.Sum256(canonical), "enc-replay",
	)
	if err != nil {
		t.Fatal(err)
	}
	replay := inline.Clone()
	replay.Payload = referencePayload
	replay.CredentialProof = []byte("refreshed-proof")
	if replayOrdinal, err := outbox.Enqueue(replay); err != nil || replayOrdinal != ordinal {
		t.Fatalf("carrier replay ordinal=%d want=%d err=%v", replayOrdinal, ordinal, err)
	}
	if messages, _ := outbox.Usage(); messages != 1 {
		t.Fatalf("carrier replay allocated %d logical entries", messages)
	}
	pending, _ := outbox.Pending()
	if len(pending) != 1 || pending[0].Payload.Kind != protocol.PayloadTransferReference || string(pending[0].CredentialProof) != "refreshed-proof" {
		t.Fatalf("carrier convergence snapshot = %#v", pending)
	}

	conflict := replay.Clone()
	conflict.Payload, err = protocol.NewInlinePayload(replay.Payload.Profile, []byte("different canonical snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Enqueue(conflict); !errors.Is(err, ErrDuplicateConflict) {
		t.Fatalf("logical identity conflict = %v", err)
	}
	pending, _ = outbox.Pending()
	if len(pending) != 1 || pending[0].Payload.Reference != replay.Payload.Reference {
		t.Fatal("conflict mutated converged replay entry")
	}

	otherSender := mustAcceptanceOutboxEnvelope(t, 11, []byte("other"))
	otherSender.Sender = "agent-c"
	otherSender.ConversationID, err = conversation.DeriveID(otherSender.MeshID, otherSender.Sender, otherSender.Recipient)
	if err != nil {
		t.Fatal(err)
	}
	otherSender.MessageID = replay.MessageID
	if _, err := outbox.Enqueue(otherSender); !errors.Is(err, ErrDuplicateConflict) {
		t.Fatalf("global message ID collision = %v", err)
	}
	if err := outbox.MarkTerminal(replay.MessageID); err != nil {
		t.Fatalf("unique message terminal evidence = %v", err)
	}
	if messages, _ := outbox.Usage(); messages != 0 {
		t.Fatalf("terminal evidence retained state: %d", messages)
	}
}

func TestAcceptanceOutboxOrdinalExhaustionNeverWrapsOrReuses(t *testing.T) {
	outbox, err := New(Config{MessageCapacity: 2, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	outbox.nextOrdinal = math.MaxUint64
	last := mustAcceptanceOutboxEnvelope(t, 1, []byte("last"))
	ordinal, err := outbox.Enqueue(last)
	if err != nil || ordinal != math.MaxUint64 {
		t.Fatalf("last ordinal=%d err=%v", ordinal, err)
	}
	if _, err := outbox.Enqueue(mustAcceptanceOutboxEnvelope(t, 2, []byte("overflow"))); !errors.Is(err, ErrOrdinalExhausted) {
		t.Fatalf("ordinal wrap = %v", err)
	}
	if err := outbox.MarkHandled(math.MaxUint64); err != nil {
		t.Fatalf("terminal cumulative evidence at max = %v", err)
	}
	if messages, bytes := outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("max handled usage=(%d,%d)", messages, bytes)
	}
	if _, err := outbox.Enqueue(last); !errors.Is(err, ErrOrdinalExhausted) {
		t.Fatalf("ordinal reused after clearing max = %v", err)
	}
}

func TestQACurrentMembershipRetiresOnlyRemovedPeerReplay(t *testing.T) {
	queued, err := New(Config{MessageCapacity: 4, ByteCapacity: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	removed := mustAcceptanceOutboxEnvelope(t, 501, []byte("removed-peer"))
	retained := mustAcceptanceOutboxEnvelope(t, 502, []byte("retained-peer"))
	retained.Recipient = "agent-c"
	retained.ConversationID, err = conversation.DeriveID(retained.MeshID, retained.Sender, retained.Recipient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queued.Enqueue(removed); err != nil {
		t.Fatal(err)
	}
	if _, err := queued.Enqueue(retained); err != nil {
		t.Fatal(err)
	}
	if evidence := queued.RetirePeerOwned("mesh", removed.Recipient); len(evidence) != 1 {
		t.Fatalf("retired entries = %d", len(evidence))
	}
	if _, err := queued.Get(removed.MessageID); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("old queued replay = %v", err)
	}
	current, err := queued.Get(retained.MessageID)
	if err != nil || current.ConversationID != retained.ConversationID {
		t.Fatalf("current queued entry = %#v, %v", current, err)
	}
	if messages, _ := queued.Usage(); messages != 1 {
		t.Fatalf("queued usage after retirement = %d", messages)
	}
	if evidence := queued.RetirePeerOwned("other-mesh", removed.Recipient); len(evidence) != 0 {
		t.Fatalf("cross-mesh retirement = %d", len(evidence))
	}
}

func TestRetirePeerOwnedCopiesIdentifiersBeforeZeroizingExactlyOnce(t *testing.T) {
	queued, err := New(Config{MessageCapacity: 2, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	old := mustAcceptanceOutboxEnvelope(t, 503, []byte("retirement-secret"))
	if _, err := queued.Enqueue(old); err != nil {
		t.Fatal(err)
	}
	queued.store.mu.Lock()
	var retainedPayload, retainedProof []byte
	for _, stored := range queued.store.byOrdinal {
		retainedPayload = stored.entry.Envelope.Payload.Inline
		retainedProof = stored.entry.Envelope.CredentialProof
	}
	queued.store.mu.Unlock()
	retired := queued.RetirePeerOwned("mesh", old.Recipient)
	if len(retired) != 1 || retired[0].MessageID != old.MessageID || retired[0].ConversationID != old.ConversationID {
		t.Fatalf("retired metadata = %#v", retired)
	}
	for _, value := range append(retainedPayload, retainedProof...) {
		if value != 0 {
			t.Fatal("retired envelope ownership was not zeroized")
		}
	}
	if again := queued.RetirePeerOwned("mesh", old.Recipient); len(again) != 0 {
		t.Fatalf("duplicate retirement evidence = %#v", again)
	}
}

func TestAcceptanceRandomizedOutboxConvergesWithoutExceedingMemoryBounds(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		random := rand.New(rand.NewSource(seed))
		outbox, err := New(Config{MessageCapacity: 16, ByteCapacity: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		var counter uint64
		for step := 0; step < 500; step++ {
			switch random.Intn(5) {
			case 0, 1:
				counter++
				_, err := outbox.Enqueue(mustAcceptanceOutboxEnvelope(t, counter, []byte{byte(counter), byte(counter >> 8)}))
				if err != nil && !errors.Is(err, ErrCapacity) {
					t.Fatalf("seed=%d step=%d enqueue: %v", seed, step, err)
				}
			case 2:
				pending, _ := outbox.Pending()
				if len(pending) != 0 {
					chosen := pending[random.Intn(len(pending))]
					ordinal, err := outbox.Enqueue(chosen.Clone())
					if err != nil || ordinal == 0 {
						t.Fatalf("seed=%d step=%d replay ordinal=%d err=%v", seed, step, ordinal, err)
					}
				}
			case 3:
				lastAssigned := outbox.nextOrdinal - 1
				if lastAssigned != 0 {
					candidate := outbox.handled + uint64(random.Intn(int(lastAssigned-outbox.handled+1)))
					if err := outbox.MarkHandled(candidate); err != nil {
						t.Fatalf("seed=%d step=%d handled=%d: %v", seed, step, candidate, err)
					}
				}
			case 4:
				pending, _ := outbox.Pending()
				if len(pending) != 0 {
					chosen := pending[random.Intn(len(pending))]
					if err := outbox.MarkTerminal(chosen.MessageID); err != nil {
						t.Fatalf("seed=%d step=%d terminal: %v", seed, step, err)
					}
				}
			}
			assertAcceptanceOutboxAccounting(t, outbox, 16, 8<<20)
		}
	}
}

func mustAcceptanceOutboxEnvelope(t *testing.T, counter uint64, canonical []byte) protocol.Envelope {
	t.Helper()
	envelope, err := acceptanceOutboxEnvelope(counter, canonical)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func acceptanceOutboxEnvelope(counter uint64, canonical []byte) (protocol.Envelope, error) {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[8:], counter)
	messageID := "msg_" + base64.RawURLEncoding.EncodeToString(raw[:])
	conversationID, err := conversation.DeriveID("mesh", "agent-a", "agent-b")
	if err != nil {
		return protocol.Envelope{}, err
	}
	payload, err := protocol.NewInlinePayload("aztm.native", canonical)
	if err != nil {
		return protocol.Envelope{}, err
	}
	return protocol.Envelope{
		Version: protocol.Version2, MessageID: messageID, ConversationID: conversationID,
		Sender: "agent-a", Recipient: "agent-b", MeshID: "mesh",
		Mode: protocol.ModeMessage, CreatedAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload, CredentialProof: []byte("proof"),
	}, nil
}

func assertAcceptanceOutboxAccounting(t *testing.T, outbox *Outbox, maxMessages int, maxBytes int64) {
	t.Helper()
	pending, err := outbox.Pending()
	if err != nil {
		t.Fatal(err)
	}
	messages, bytes := outbox.Usage()
	if messages != len(pending) || messages > maxMessages || bytes > maxBytes || bytes < 0 {
		t.Fatalf("bounded usage messages=%d pending=%d bytes=%d", messages, len(pending), bytes)
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	var exact int64
	for _, envelope := range pending {
		wire, err := codec.Encode(envelope)
		if err != nil {
			t.Fatal(err)
		}
		exact += int64(len(wire))
	}
	if bytes != exact {
		t.Fatalf("byte accounting=%d canonical sum=%d", bytes, exact)
	}
}

func sortedAcceptancePending(outbox *Outbox) []protocol.Envelope {
	pending, _ := outbox.Pending()
	sort.Slice(pending, func(i, j int) bool { return pending[i].MessageID < pending[j].MessageID })
	return pending
}
