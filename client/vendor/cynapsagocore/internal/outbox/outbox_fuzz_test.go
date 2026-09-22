package outbox

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func FuzzOutboxOperationSequences(f *testing.F) {
	for _, seed := range [][]byte{{}, {0, 0, 1, 2, 3, 4, 5}, {0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 2, 5, 3, 4, 1}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 256 {
			t.Skip()
		}
		outbox, err := New(Config{MessageCapacity: 8, ByteCapacity: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		var counter uint64
		for step, operation := range operations {
			switch operation % 6 {
			case 0:
				counter++
				envelope, err := acceptanceOutboxEnvelope(counter, []byte{operation, byte(step)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := outbox.Enqueue(envelope); err != nil && !errors.Is(err, ErrCapacity) {
					t.Fatalf("step %d enqueue = %v", step, err)
				}
			case 1:
				pending := sortedAcceptancePending(outbox)
				if len(pending) != 0 {
					chosen := pending[int(operation)%len(pending)]
					before := outbox.nextOrdinal
					ordinal, err := outbox.Enqueue(chosen.Clone())
					if err != nil || ordinal == 0 || outbox.nextOrdinal != before {
						t.Fatalf("step %d exact replay ordinal=%d next=%d/%d err=%v", step, ordinal, before, outbox.nextOrdinal, err)
					}
				}
			case 2:
				pending := sortedAcceptancePending(outbox)
				if len(pending) != 0 {
					conflict := pending[int(operation)%len(pending)].Clone()
					conflict.Payload, err = protocol.NewInlinePayload(conflict.Payload.Profile, []byte("conflict"))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := outbox.Enqueue(conflict); !errors.Is(err, ErrDuplicateConflict) {
						t.Fatalf("step %d conflict = %v", step, err)
					}
				}
			case 3:
				lastAssigned := outbox.nextOrdinal - 1
				if lastAssigned != 0 {
					candidate := uint64(operation) % (lastAssigned + 2)
					err := outbox.MarkHandled(candidate)
					switch {
					case candidate < outbox.handled:
						// A successful call updates handled before this branch, so
						// only an actual regression can remain below it.
						if err != nil && !errors.Is(err, ErrHandledRegression) {
							t.Fatalf("step %d regression = %v", step, err)
						}
					case candidate > lastAssigned:
						if !errors.Is(err, ErrInvalidEvidence) {
							t.Fatalf("step %d future evidence = %v", step, err)
						}
					case err != nil:
						t.Fatalf("step %d valid evidence = %v", step, err)
					}
				}
			case 4:
				pending := sortedAcceptancePending(outbox)
				if len(pending) != 0 {
					chosen := pending[int(operation)%len(pending)]
					if err := outbox.MarkTerminal(chosen.MessageID); err != nil {
						t.Fatalf("step %d terminal = %v", step, err)
					}
				}
			case 5:
				pending := sortedAcceptancePending(outbox)
				if len(pending) != 0 {
					chosen := pending[int(operation)%len(pending)]
					if chosen.Payload.Kind != protocol.PayloadInline {
						break
					}
					canonical := append([]byte(nil), chosen.Payload.Inline...)
					reference, err := protocol.NewReferencedPayload(
						protocol.PayloadTransferReference, chosen.Payload.Profile, "transfer://private/fuzz",
						int64(len(canonical)), sha256.Sum256(canonical), "enc-fuzz",
					)
					if err != nil {
						t.Fatal(err)
					}
					chosen.Payload = reference
					before := outbox.nextOrdinal
					if _, err := outbox.Enqueue(chosen); err != nil || outbox.nextOrdinal != before {
						t.Fatalf("step %d carrier convergence next=%d/%d err=%v", step, before, outbox.nextOrdinal, err)
					}
				}
			}
			assertAcceptanceOutboxAccounting(t, outbox, 8, 4<<20)
			pending := sortedAcceptancePending(outbox)
			if len(pending) != 0 {
				copySnapshot := pending[0].Clone()
				copySnapshot.CredentialProof[0] ^= 0xff
				again := sortedAcceptancePending(outbox)
				if again[0].CredentialProof[0] == copySnapshot.CredentialProof[0] {
					t.Fatalf("step %d replay clone aliased retained state", step)
				}
			}
		}
	})
}
