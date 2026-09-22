package rank2xmpp

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"

	"mellium.im/xmpp/jid"
)

func TestStreamManagementCorrelatedResultAdvancesCumulativeHandling(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-id", true); err != nil {
		t.Fatal(err)
	}
	if err = sm.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 41}); err != nil {
		t.Fatal(err)
	}
	sequence, err := sm.RecordSentTracked(Stanza{Kind: StanzaTimeCalibration, MessageID: "time-id"})
	if err != nil {
		t.Fatal(err)
	}
	ordinal, count, err := sm.ConfirmCorrelatedHandled(sequence)
	if err != nil || ordinal != 41 || count != 2 || sm.Pending() != 0 {
		t.Fatalf("confirmation = ordinal %d count %d pending %d err %v", ordinal, count, sm.Pending(), err)
	}
	if ordinal, count, err = sm.ConfirmCorrelatedHandled(sequence); err != nil || ordinal != 0 || count != 0 {
		t.Fatalf("idempotent confirmation = ordinal %d count %d err %v", ordinal, count, err)
	}
}

func TestStreamManagementCorrelatedResultRejectsUnknownFutureSequence(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-id", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err = sm.ConfirmCorrelatedHandled(1); err != ErrProtocol {
		t.Fatalf("unknown future sequence error = %v", err)
	}
}

func TestMalformedCorrelatedTimeResultCommitsOutboundProofOnly(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	document := `<iq xmlns='jabber:client' type='result' id='time-1' from='mesh.example.test' to='agent@mesh.example.test/mesh-1'><time xmlns='urn:xmpp:time'><tzo>Z</tzo><utc>coarse</utc></time></iq>`
	_, correlated, err := decodeCorrelatedEntityTimeResponse(xml.NewDecoder(strings.NewReader(document)), "time-1", from, to)
	if !correlated || !errors.Is(err, ErrProtocol) {
		t.Fatalf("correlated=%v error=%v", correlated, err)
	}
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-id", true); err != nil {
		t.Fatal(err)
	}
	if err = sm.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 71}); err != nil {
		t.Fatal(err)
	}
	sequence, err := sm.RecordSentTracked(Stanza{Kind: StanzaTimeCalibration, MessageID: "time-1"})
	if err != nil {
		t.Fatal(err)
	}
	ordinal, count, err := sm.ConfirmCorrelatedHandled(sequence)
	if err != nil || ordinal != 71 || count != 2 || sm.Pending() != 0 || sm.HandledInbound() != 0 {
		t.Fatalf("ordinal=%d count=%d pending=%d inbound=%d error=%v", ordinal, count, sm.Pending(), sm.HandledInbound(), err)
	}
}

func TestUnacknowledgedTimeCalibrationFencesResumeAndCleanReplay(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-id", true); err != nil {
		t.Fatal(err)
	}
	records := []Stanza{
		{Kind: StanzaEnvelope, Ordinal: 81, MessageID: "first"},
		{Kind: StanzaTimeCalibration, MessageID: "ambiguous-time"},
		{Kind: StanzaSignal, AttemptID: "signal-after-time"},
		{Kind: StanzaEnvelope, Ordinal: 82, MessageID: "last"},
	}
	for _, record := range records {
		if err = sm.RecordSent(record); err != nil {
			t.Fatal(err)
		}
	}
	fenced, err := sm.pendingKindAfterAck(1, StanzaTimeCalibration)
	if err != nil || !fenced {
		t.Fatal("ambiguous calibration did not fence resume")
	}
	replay, err := applicationReplay(sm.ResumeRejected())
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 3 || replay[0].MessageID != "first" || replay[1].AttemptID != "signal-after-time" || replay[2].MessageID != "last" {
		t.Fatalf("clean replay order=%#v", replay)
	}
	if _, _, resumable := sm.ResumeState(); resumable || sm.Pending() != 0 {
		t.Fatalf("old ledger remains resumable=%v pending=%d", resumable, sm.Pending())
	}

	// A clean session starts a fresh h ledger; no dropped control marker can
	// leave a phantom sequence between the replayed application records.
	clean, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = clean.Enable("new-resume-id", true); err != nil {
		t.Fatal(err)
	}
	for _, record := range replay {
		if err = clean.RecordSent(record); err != nil {
			t.Fatal(err)
		}
	}
	ordinal, count, err := clean.ApplyAckDetailed(3)
	if err != nil || ordinal != 82 || count != 3 || clean.Pending() != 0 {
		t.Fatalf("fresh ledger ordinal=%d count=%d pending=%d error=%v", ordinal, count, clean.Pending(), err)
	}
}
