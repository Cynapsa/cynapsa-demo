package rank2xmpp

import (
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
)

func replayOwned(ordinal uint64, fill byte) outbox.Rank2Metadata {
	return outbox.Rank2Metadata{
		MessageID:        typed("msg_", fill),
		ConversationID:   typed("conv_", fill),
		Sender:           "a@example.test/mesh",
		Recipient:        "b@example.test/mesh",
		MeshID:           "mesh",
		TransportOrdinal: ordinal,
	}
}

func replayEnvelope(metadata outbox.Rank2Metadata) Stanza {
	return Stanza{
		Kind: StanzaEnvelope, From: metadata.Sender, To: metadata.Recipient,
		MeshID: metadata.MeshID, Ordinal: metadata.TransportOrdinal,
		MessageID: metadata.MessageID,
	}
}

func TestReconcileRank2ReplayRestoresRawAcknowledgedEnvelopePrefixInOrder(t *testing.T) {
	owned := []outbox.Rank2Metadata{
		replayOwned(10, 0x10), replayOwned(20, 0x20), replayOwned(30, 0x30),
	}
	retained := []Stanza{
		{Kind: StanzaTransferFinish, From: owned[1].Sender, To: owned[1].Recipient,
			MeshID: "mesh", TransferID: typed("xfer_", 0x44)},
		replayEnvelope(owned[1]),
	}

	got, err := reconcileRank2Replay(retained, owned, owned[0].Sender, "mesh", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("reconciled replay=%#v", got)
	}
	if got[0].Kind != StanzaEnvelope || got[0].Ordinal != 10 ||
		got[1].Kind != StanzaTransferFinish ||
		got[2].Kind != StanzaEnvelope || got[2].Ordinal != 20 ||
		got[3].Kind != StanzaEnvelope || got[3].Ordinal != 30 {
		t.Fatalf("raw-ACK restoration changed wire order: %#v", got)
	}
	for _, index := range []int{0, 2, 3} {
		if len(got[index].Data) != 0 || got[index].MessageID == "" {
			t.Fatalf("envelope %d was not metadata-only: %#v", index, got[index])
		}
	}
}

func TestReconcileRank2ReplayRestoresMultipleAcknowledgedPrefixEntries(t *testing.T) {
	owned := []outbox.Rank2Metadata{
		replayOwned(1, 0x51), replayOwned(2, 0x52), replayOwned(3, 0x53),
	}
	got, err := reconcileRank2Replay(
		[]Stanza{replayEnvelope(owned[2])}, owned, owned[0].Sender, "mesh", 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Ordinal != 1 || got[1].Ordinal != 2 || got[2].Ordinal != 3 {
		t.Fatalf("restored prefix order=%#v", got)
	}
}

func TestReconcileRank2ReplayRejectsMissingEnvelopeInLedgerMiddle(t *testing.T) {
	owned := []outbox.Rank2Metadata{
		replayOwned(10, 0x61), replayOwned(20, 0x62), replayOwned(30, 0x63),
	}
	retained := []Stanza{replayEnvelope(owned[0]), replayEnvelope(owned[2])}
	if got, err := reconcileRank2Replay(retained, owned, owned[0].Sender, "mesh", 0); !errors.Is(err, ErrStreamManagement) || got != nil {
		t.Fatalf("middle omission accepted: got=%#v err=%v", got, err)
	}
}

func TestFilterReplayAuthorityDropsRemovedPeerBeforeCleanSessionReplay(t *testing.T) {
	current := replayOwned(1, 0x71)
	removed := replayOwned(2, 0x72)
	removed.Recipient = "removed@example.test/mesh"
	replay := []Stanza{
		replayEnvelope(current),
		{Kind: StanzaSignal, From: removed.Sender, To: removed.Recipient, MeshID: removed.MeshID, AttemptID: typed("attempt_", 0x73)},
		replayEnvelope(removed),
	}
	filtered, err := filterReplayAuthority(
		replay,
		[]string{"a@example.test/mesh", "b@example.test/mesh"},
		"a@example.test/mesh",
		"mesh",
	)
	if err != nil {
		t.Fatal(err)
	}
	owned := filterOwnedReplayAuthority([]outbox.Rank2Metadata{current, removed}, []string{"a@example.test/mesh", "b@example.test/mesh"})
	filtered, err = reconcileRank2Replay(filtered, owned, current.Sender, current.MeshID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clearStanzas(filtered)
	if len(filtered) != 1 || filtered[0].Kind != StanzaEnvelope || filtered[0].MessageID != current.MessageID {
		t.Fatalf("authority-filtered replay = %#v", filtered)
	}
}
