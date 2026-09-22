package rank2xmpp

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestStreamManagementByteAdmissionAndAckRelease(t *testing.T) {
	stanza := Stanza{Kind: StanzaTransferChunk, From: "a@example.test/mesh", To: "b@example.test/mesh", MeshID: "mesh", TransferID: "xfer", Data: bytes.Repeat([]byte{0x5a}, 128)}
	charge, ok := retainedStanzaBytes(stanza)
	if !ok {
		t.Fatal("charge overflow")
	}
	management, err := NewStreamManagement(4, charge)
	if err != nil || management.Enable("resume", true) != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(stanza); err != nil {
		t.Fatal(err)
	}
	if got := management.PendingBytes(); got != charge {
		t.Fatalf("pending bytes=%d want=%d", got, charge)
	}
	if err = management.RecordSent(Stanza{Kind: StanzaTransferChunk, TransferID: "next"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("byte capacity error=%v", err)
	}
	retained := management.pending[0].stanza.Data
	if _, count, err := management.ApplyAckDetailed(1); err != nil || count != 1 {
		t.Fatalf("ack count=%d err=%v", count, err)
	}
	if management.Pending() != 0 || management.PendingBytes() != 0 {
		t.Fatalf("pending=%d bytes=%d", management.Pending(), management.PendingBytes())
	}
	if !bytes.Equal(retained, make([]byte, len(retained))) {
		t.Fatal("ack did not clear released replay bytes")
	}
}

func TestStreamManagementEnvelopeLedgerRetainsMetadataOnly(t *testing.T) {
	firstData := bytes.Repeat([]byte{0x41}, 128)
	secondData := bytes.Repeat([]byte{0x42}, 256)
	first := Stanza{Kind: StanzaEnvelope, From: "a@example.test/mesh", To: "b@example.test/mesh", MeshID: "mesh", Ordinal: 11, MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", Data: firstData}
	second := Stanza{Kind: StanzaEnvelope, From: first.From, To: first.To, MeshID: first.MeshID, Ordinal: 12, MessageID: "msg_BBBBBBBBBBBBBBBBBBBBBB", Data: secondData}
	firstCharge, ok := retainedStanzaBytes(first)
	if !ok {
		t.Fatal("first charge overflow")
	}
	secondCharge, ok := retainedStanzaBytes(second)
	if !ok {
		t.Fatal("second charge overflow")
	}
	management, err := NewStreamManagement(4, firstCharge+secondCharge)
	if err != nil || management.Enable("resume", true) != nil {
		t.Fatal(err)
	}
	if err := management.RecordSent(first); err != nil {
		t.Fatal(err)
	}
	if err := management.RecordSent(second); err != nil {
		t.Fatal(err)
	}
	if got := management.PendingBytes(); got != firstCharge+secondCharge {
		t.Fatalf("pending bytes=%d want=%d", got, firstCharge+secondCharge)
	}
	if len(management.pending[0].stanza.Data) != 0 || len(management.pending[1].stanza.Data) != 0 {
		t.Fatalf("envelope bytes retained in ledger: %#v", management.pending)
	}
	if !bytes.Equal(first.Data, firstData) || !bytes.Equal(second.Data, secondData) {
		t.Fatal("stream management mutated caller-owned envelope bytes")
	}
	snapshot := management.PendingSnapshot()
	if len(snapshot) != 2 || snapshot[0].MessageID != first.MessageID || snapshot[1].MessageID != second.MessageID || len(snapshot[0].Data) != 0 || len(snapshot[1].Data) != 0 {
		t.Fatalf("metadata snapshot=%#v", snapshot)
	}
	ordinal, count, err := management.ApplyAckDetailed(1)
	if err != nil || ordinal != first.Ordinal || count != 1 {
		t.Fatalf("ack ordinal=%d count=%d err=%v", ordinal, count, err)
	}
	replay := management.ResumeRejected()
	if len(replay) != 1 || replay[0].MessageID != second.MessageID || replay[0].Ordinal != second.Ordinal || len(replay[0].Data) != 0 {
		t.Fatalf("metadata replay=%#v", replay)
	}
	clearStanzas(snapshot)
	clearStanzas(replay)
}

func TestStreamManagementResumeRejectedMovesReplayOwnership(t *testing.T) {
	stanza := Stanza{Kind: StanzaTransferChunk, From: "a", To: "b", MeshID: "mesh", TransferID: "xfer", Data: []byte("sole replay buffer")}
	charge, _ := retainedStanzaBytes(stanza)
	management, err := NewStreamManagement(2, charge)
	if err != nil || management.Enable("resume", true) != nil || management.RecordSent(stanza) != nil {
		t.Fatal(err)
	}
	owned := &management.pending[0].stanza.Data[0]
	replay := management.ResumeRejected()
	if len(replay) != 1 || string(replay[0].Data) != "sole replay buffer" || &replay[0].Data[0] != owned {
		t.Fatalf("replay=%#v", replay)
	}
	if management.Pending() != 0 || management.PendingBytes() != 0 {
		t.Fatalf("rejected ledger retained pending=%d bytes=%d", management.Pending(), management.PendingBytes())
	}
	clearStanzas(replay)
}

func TestStreamManagementRejectsMissingOrExcessByteLimit(t *testing.T) {
	for _, limit := range []int64{0, minimumRetainedStanzaBytes - 1, MaximumStreamManagementBytes + 1} {
		if _, err := NewStreamManagement(1, limit); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("limit=%d error=%v", limit, err)
		}
	}
}

func TestStreamManagementCloseDestroysLedgerAndIsIdempotent(t *testing.T) {
	stanza := Stanza{Kind: StanzaTransferChunk, From: "sender", To: "recipient", MeshID: "mesh", TransferID: "xfer", MessageID: "message", Data: []byte("terminal ledger bytes")}
	charge, _ := retainedStanzaBytes(stanza)
	management, err := NewStreamManagement(2, charge*2)
	if err != nil || management.Enable("resume-secret", true) != nil || management.RecordSent(stanza) != nil {
		t.Fatal(err)
	}
	management.MarkHandledInbound()
	backing := management.pending
	owned := backing[0].stanza.Data
	management.Close()
	management.Close()
	entryCleared := backing[0].sequence == 0 && backing[0].bytes == 0 && backing[0].stanza.Kind == 0 && backing[0].stanza.From == "" && backing[0].stanza.To == "" && backing[0].stanza.MeshID == "" && backing[0].stanza.TransferID == "" && backing[0].stanza.MessageID == "" && backing[0].stanza.Data == nil
	if management.Pending() != 0 || management.PendingBytes() != 0 || !bytesAllZero(owned) || !entryCleared {
		t.Fatalf("terminal ledger pending=%d bytes=%d cleared=%t entry=%#v", management.Pending(), management.PendingBytes(), bytesAllZero(owned), backing[0])
	}
	if id, handled, resumable := management.ResumeState(); id != "" || handled != 0 || resumable {
		t.Fatalf("terminal markers id=%q handled=%d resumable=%t", id, handled, resumable)
	}
	if err = management.RecordSent(stanza); !errors.Is(err, ErrStreamManagement) {
		t.Fatalf("terminal record=%v", err)
	}
}

func TestMelliumCloseDestroysLedgerBeforeReturningAndRepeats(t *testing.T) {
	stanza := Stanza{Kind: StanzaTransferChunk, From: "a", To: "b", MeshID: "mesh", TransferID: "xfer", Data: []byte("session-owned-ledger")}
	charge, _ := retainedStanzaBytes(stanza)
	management, err := NewStreamManagement(2, charge)
	if err != nil || management.Enable("resume", true) != nil || management.RecordSent(stanza) != nil {
		t.Fatal(err)
	}
	owned := management.pending[0].stanza.Data
	session := &melliumSession{management: management}
	if err = session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if management.Pending() != 0 || management.PendingBytes() != 0 || !bytesAllZero(owned) {
		t.Fatalf("closed session pending=%d bytes=%d cleared=%t", management.Pending(), management.PendingBytes(), bytesAllZero(owned))
	}
}
