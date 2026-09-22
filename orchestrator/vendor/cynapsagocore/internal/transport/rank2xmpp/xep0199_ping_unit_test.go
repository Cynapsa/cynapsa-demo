package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"mellium.im/xmpp"
	"mellium.im/xmpp/stanza"
)

const xep0199ValidPing = `<iq xmlns="jabber:client" from="example.test" to="a@example.test/mesh" type="get" id="idle-ping-1"><ping xmlns="urn:xmpp:ping"/></iq>`

func newXEP0199TestSession(t *testing.T) (*melliumSession, *StreamManagement) {
	t.Helper()
	management, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("ping-resume", true); err != nil {
		t.Fatal(err)
	}
	return &melliumSession{
		config:   MelliumConfig{StanzaBudgetBytes: 1 << 20},
		username: "a@example.test", meshID: "mesh", management: management,
	}, management
}

func handleEncodedXEP0199(t *testing.T, session *melliumSession, encoded string) (string, error) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(encoded))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		t.Fatalf("encoded ping start = %#v", token)
	}
	var output bytes.Buffer
	encoder := xml.NewEncoder(&output)
	stream := &qaTokenReadEncoder{decoder: decoder, encoder: encoder}
	err = session.handleElement(context.Background(), stream, &start)
	if flushErr := encoder.Flush(); err == nil && flushErr != nil {
		err = flushErr
	}
	return output.String(), err
}

func TestXEP0199ServerPingReturnsManagedResultRequestsAckAndAdvancesInboundH(t *testing.T) {
	session, management := newXEP0199TestSession(t)
	output, err := handleEncodedXEP0199(t, session, xep0199ValidPing)
	if err != nil {
		t.Fatal(err)
	}

	decoder := xml.NewDecoder(strings.NewReader(output))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		t.Fatalf("ping result start = %#v", token)
	}
	result, err := stanza.NewIQ(start)
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != stanza.ResultIQ || result.ID != "idle-ping-1" || result.From.String() != "a@example.test/mesh" || result.To.String() != "example.test" {
		t.Fatalf("ping result = %#v", result)
	}
	if token, err = decoder.Token(); err != nil || token != start.End() {
		t.Fatalf("ping result payload = %#v err=%v", token, err)
	}
	token, err = decoder.Token()
	request, ok := token.(xml.StartElement)
	if err != nil || !ok || request.Name != (xml.Name{Space: streamManagementNamespace, Local: "r"}) {
		t.Fatalf("ping result ack request = %#v err=%v", token, err)
	}
	if err = decodeSMRequest(decoder, request); err != nil {
		t.Fatalf("ping result ack request: %v", err)
	}
	if token, err = decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		t.Fatalf("ping result trailing content = %#v err=%v", token, err)
	}

	if handled := management.HandledInbound(); handled != 1 {
		t.Fatalf("handled inbound h=%d want 1", handled)
	}
	pending := management.PendingSnapshot()
	if len(pending) != 1 || pending[0].Kind != StanzaSessionPingResult || pending[0].From != "a@example.test/mesh" || pending[0].To != "example.test" || pending[0].MeshID != "mesh" || pending[0].AttemptID != "idle-ping-1" {
		t.Fatalf("managed ping result = %#v", pending)
	}
	if fenced, fenceErr := management.pendingControlAfterAck(0, "a@example.test/mesh", "example.test", "mesh"); fenceErr != nil || fenced {
		t.Fatalf("replay-safe ping result fence=%t err=%v", fenced, fenceErr)
	}

	sequence, err := management.RecordSentTracked(Stanza{Kind: StanzaEnvelope, Ordinal: 91, MessageID: "after-ping"})
	if err != nil || sequence != 2 {
		t.Fatalf("post-ping sequence=%d err=%v", sequence, err)
	}
	ordinal, count, err := management.ApplyAckDetailed(1)
	if err != nil || ordinal != 0 || count != 1 {
		t.Fatalf("ping result ack ordinal=%d count=%d err=%v", ordinal, count, err)
	}
	pending = management.PendingSnapshot()
	if len(pending) != 1 || pending[0].Kind != StanzaEnvelope || pending[0].Ordinal != 91 {
		t.Fatalf("ledger after exact ping-result ack = %#v", pending)
	}

	replay, err := applicationReplay([]Stanza{{Kind: StanzaSessionPingResult, MeshID: "mesh", AttemptID: "ambiguous-ping"}})
	if err != nil || len(replay) != 0 {
		t.Fatalf("clean-session ping replay = %#v err=%v", replay, err)
	}
}

func TestXEP0199ServerPingRejectsMalformedAndSpoofedIQsWithoutAccounting(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{name: "spoofed bare domain", wire: strings.Replace(xep0199ValidPing, `from="example.test"`, `from="attacker.test"`, 1)},
		{name: "hostile peer resource", wire: strings.Replace(xep0199ValidPing, `from="example.test"`, `from="peer@example.test/mesh"`, 1)},
		{name: "wrong recipient", wire: strings.Replace(xep0199ValidPing, `to="a@example.test/mesh"`, `to="other@example.test/mesh"`, 1)},
		{name: "empty id", wire: strings.Replace(xep0199ValidPing, `id="idle-ping-1"`, `id=""`, 1)},
		{name: "wrong type", wire: strings.Replace(xep0199ValidPing, `type="get"`, `type="set"`, 1)},
		{name: "unknown outer attribute", wire: strings.Replace(xep0199ValidPing, `type="get"`, `type="get" extra="true"`, 1)},
		{name: "wrong payload namespace", wire: strings.Replace(xep0199ValidPing, `urn:xmpp:ping`, `urn:attacker:ping`, 1)},
		{name: "payload attribute", wire: strings.Replace(xep0199ValidPing, `<ping xmlns="urn:xmpp:ping"/>`, `<ping xmlns="urn:xmpp:ping" extra="true"/>`, 1)},
		{name: "payload namespace metadata", wire: strings.Replace(xep0199ValidPing, `<ping xmlns="urn:xmpp:ping"/>`, `<ping xmlns="urn:xmpp:ping" xmlns:extra="urn:xmpp:ping"/>`, 1)},
		{name: "payload text", wire: strings.Replace(xep0199ValidPing, `<ping xmlns="urn:xmpp:ping"/>`, `<ping xmlns="urn:xmpp:ping"> </ping>`, 1)},
		{name: "payload child", wire: strings.Replace(xep0199ValidPing, `<ping xmlns="urn:xmpp:ping"/>`, `<ping xmlns="urn:xmpp:ping"><extra/></ping>`, 1)},
		{name: "extra iq child", wire: strings.Replace(xep0199ValidPing, `</iq>`, `<extra/></iq>`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, management := newXEP0199TestSession(t)
			output, err := handleEncodedXEP0199(t, session, test.wire)
			if err == nil {
				t.Fatal("malformed or spoofed ping was accepted")
			}
			if output != "" {
				t.Fatalf("rejected ping emitted output %q", output)
			}
			if handled := management.HandledInbound(); handled != 0 {
				t.Fatalf("rejected ping advanced inbound h=%d", handled)
			}
			if pending := management.Pending(); pending != 0 {
				t.Fatalf("rejected ping recorded %d outbound stanzas", pending)
			}
		})
	}
}

func TestXEP0199AmbiguousResultWriteRetainsReplayRecordWithoutInboundAck(t *testing.T) {
	session, management := newXEP0199TestSession(t)
	decoder := xml.NewDecoder(strings.NewReader(xep0199ValidPing))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := token.(xml.StartElement)
	stream := &qaFailingTokenReadEncoder{decoder: decoder}
	if err = session.handleElement(context.Background(), stream, &start); err == nil {
		t.Fatal("failed ping-result write was reported successful")
	}
	if management.HandledInbound() != 0 {
		t.Fatal("failed ping-result write advanced inbound h")
	}
	pending := management.PendingSnapshot()
	if len(pending) != 1 || pending[0].Kind != StanzaSessionPingResult {
		t.Fatalf("ambiguous ping result ledger = %#v", pending)
	}
	if fenced, fenceErr := management.pendingControlAfterAck(0, "a@example.test/mesh", "example.test", "mesh"); fenceErr != nil || fenced {
		t.Fatalf("ambiguous ping result fence=%t err=%v", fenced, fenceErr)
	}
}

func TestXEP0199ResumeFenceAllowsOnlyExactPingResultRoute(t *testing.T) {
	base := Stanza{Kind: StanzaSessionPingResult, From: "a@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "idle-ping-1"}
	tests := []struct {
		name   string
		mutate func(*Stanza)
	}{
		{name: "wrong local", mutate: func(value *Stanza) { value.From = "other@example.test/mesh" }},
		{name: "wrong resource", mutate: func(value *Stanza) { value.From = "a@example.test/other" }},
		{name: "wrong server", mutate: func(value *Stanza) { value.To = "attacker.test" }},
		{name: "server resource", mutate: func(value *Stanza) { value.To = "example.test/resource" }},
		{name: "wrong mesh", mutate: func(value *Stanza) { value.MeshID = "other" }},
		{name: "empty id", mutate: func(value *Stanza) { value.AttemptID = "" }},
		{name: "unexpected data", mutate: func(value *Stanza) { value.Data = []byte("payload") }},
		{name: "unexpected ordinal", mutate: func(value *Stanza) { value.Ordinal = 1 }},
		{name: "unexpected transfer id", mutate: func(value *Stanza) { value.TransferID = "transfer" }},
		{name: "unexpected message id", mutate: func(value *Stanza) { value.MessageID = "msg_0123456789abcdef" }},
		{name: "unexpected evidence transfer", mutate: func(value *Stanza) { value.Evidence.TransferID = "transfer" }},
		{name: "unexpected evidence message", mutate: func(value *Stanza) { value.Evidence.MessageID = "msg_0123456789abcdef" }},
		{name: "unexpected evidence digest", mutate: func(value *Stanza) { value.Evidence.Digest[0] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			management, err := NewStreamManagement(2, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err = management.Enable("resume", true); err != nil {
				t.Fatal(err)
			}
			record := base
			test.mutate(&record)
			if err = management.RecordSent(record); err != nil {
				t.Fatal(err)
			}
			fenced, fenceErr := management.pendingControlAfterAck(0, base.From, base.To, base.MeshID)
			if fenceErr != nil || !fenced {
				t.Fatalf("malformed ping fence=%t err=%v", fenced, fenceErr)
			}
		})
	}
}

func TestXEP0199ResumeFenceRejectsDuplicatePendingPingResultID(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	record := Stanza{Kind: StanzaSessionPingResult, From: "a@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "idle-ping-1"}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	fenced, fenceErr := management.pendingControlAfterAck(0, record.From, record.To, record.MeshID)
	if fenceErr != nil || !fenced {
		t.Fatalf("duplicate ping-result ID fence=%t err=%v", fenced, fenceErr)
	}
}

func TestXEP0199ReplayPreflightRestoresDuplicateRecordsWithoutWriting(t *testing.T) {
	record := Stanza{Kind: StanzaSessionPingResult, From: "a@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "idle-ping-1"}
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{
		session:          &xmpp.Session{},
		management:       management,
		ctx:              context.Background(),
		generation:       4,
		resumeGeneration: 4,
		resumeReplay:     []Stanza{record, record},
		username:         "a@example.test",
		meshID:           "mesh",
	}
	err = session.ReplayPrepared(context.Background(), func(Stanza) bool { return true }, func(value Stanza) (preparedReplayStanza, bool) {
		t.Fatal("duplicate preflight reached replay preparation")
		return preparedReplayStanza{}, false
	})
	if !errors.Is(err, ErrStreamManagement) {
		t.Fatalf("duplicate replay error=%v", err)
	}
	if session.resumeGeneration != 4 || len(session.resumeReplay) != 2 || !sameManagedStanza(session.resumeReplay[0], record) || !sameManagedStanza(session.resumeReplay[1], record) {
		t.Fatalf("duplicate replay ownership was not restored: generation=%d replay=%#v", session.resumeGeneration, session.resumeReplay)
	}
}

func TestXEP0199ResumeFenceStillRejectsEveryOtherSessionControl(t *testing.T) {
	for _, kind := range []StanzaKind{StanzaTimeCalibration, StanzaAuthoritySync, StanzaAuthorityDiscovery, StanzaUploadSlotQuery} {
		management, err := NewStreamManagement(2, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = management.Enable("resume", true); err != nil {
			t.Fatal(err)
		}
		if err = management.RecordSent(Stanza{Kind: kind, From: "a@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "control"}); err != nil {
			t.Fatal(err)
		}
		fenced, fenceErr := management.pendingControlAfterAck(0, "a@example.test/mesh", "example.test", "mesh")
		if fenceErr != nil || !fenced {
			t.Fatalf("kind=%d fence=%t err=%v", kind, fenced, fenceErr)
		}
	}
}

func TestReplayPreparedRejectsEveryNonReplayableSessionControl(t *testing.T) {
	for _, kind := range []StanzaKind{StanzaTimeCalibration, StanzaAuthoritySync, StanzaAuthorityDiscovery, StanzaUploadSlotQuery} {
		t.Run(strconv.Itoa(int(kind)), func(t *testing.T) {
			management, err := NewStreamManagement(2, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err = management.Enable("resume", true); err != nil {
				t.Fatal(err)
			}
			record := Stanza{
				Kind: kind, From: "a@example.test/mesh", To: "example.test",
				MeshID: "mesh", AttemptID: "control", MessageID: "control-id",
			}
			if err = management.RecordSent(record); err != nil {
				t.Fatal(err)
			}
			session := &melliumSession{
				session:          &xmpp.Session{},
				management:       management,
				ctx:              context.Background(),
				generation:       4,
				resumeGeneration: 4,
				resumeReplay:     []Stanza{record},
				username:         "a@example.test",
				meshID:           "mesh",
			}
			err = session.ReplayPrepared(context.Background(), func(Stanza) bool { return true }, func(Stanza) (preparedReplayStanza, bool) {
				t.Fatal("non-replayable control reached replay preparation")
				return preparedReplayStanza{}, false
			})
			if !errors.Is(err, ErrStreamManagement) {
				t.Fatalf("ReplayPrepared error=%v", err)
			}
			if session.resumeGeneration != 4 || len(session.resumeReplay) != 1 || !sameManagedStanza(session.resumeReplay[0], record) {
				t.Fatalf("failed preflight lost replay state: generation=%d replay=%#v", session.resumeGeneration, session.resumeReplay)
			}
			if pending := management.PendingSnapshot(); len(pending) != 1 || !sameManagedStanza(pending[0], record) {
				clearStanzas(pending)
				t.Fatalf("failed preflight changed ledger: %#v", pending)
			} else {
				clearStanzas(pending)
			}
		})
	}
}

func TestXEP0198ResumeAllowsOnlyExactExternalServiceQueryDescriptor(t *testing.T) {
	payload, err := encodeExternalServiceQuery("services", ExternalService{})
	if err != nil {
		t.Fatal(err)
	}
	record := Stanza{
		Kind: StanzaExternalServiceQuery, From: "a@example.test/mesh", To: "example.test",
		MeshID: "mesh", AttemptID: "services",
		MessageID: "cynapsa-extdisco-UG43NPMZB66XVFNBKNZOUCNOVU", Data: payload,
	}
	management, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	if fenced, fenceErr := management.pendingControlAfterAck(0, record.From, record.To, record.MeshID); fenceErr != nil || fenced {
		t.Fatalf("exact external query fence=%t err=%v", fenced, fenceErr)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	if fenced, fenceErr := management.pendingControlAfterAck(0, record.From, record.To, record.MeshID); fenceErr != nil || !fenced {
		t.Fatalf("duplicate external query ID fence=%t err=%v", fenced, fenceErr)
	}

	invalid := record.clone()
	invalid.Data = append(invalid.Data, ' ')
	other, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = other.RecordSent(invalid); err != nil {
		t.Fatal(err)
	}
	if fenced, fenceErr := other.pendingControlAfterAck(0, record.From, record.To, record.MeshID); fenceErr != nil || !fenced {
		t.Fatalf("malformed external query fence=%t err=%v", fenced, fenceErr)
	}

	invalidID := record.clone()
	invalidID.MessageID = externalServiceIDPrefix + strings.Repeat("!", externalServiceRandomLength)
	third, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = third.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = third.RecordSent(invalidID); err != nil {
		t.Fatal(err)
	}
	if fenced, fenceErr := third.pendingControlAfterAck(0, record.From, record.To, record.MeshID); fenceErr != nil || !fenced {
		t.Fatalf("invalid external query ID fence=%t err=%v", fenced, fenceErr)
	}
}

func TestResumedExternalServiceCorrelationOutlivesReplayPhaseAndIsOneUse(t *testing.T) {
	payload, err := encodeExternalServiceQuery("services", ExternalService{})
	if err != nil {
		t.Fatal(err)
	}
	record := Stanza{
		Kind: StanzaExternalServiceQuery, From: "a@example.test/mesh", To: "example.test",
		MeshID: "mesh", AttemptID: "services",
		MessageID: externalServiceIDPrefix + strings.Repeat("C", externalServiceRandomLength), Data: payload,
	}
	session := &melliumSession{
		generation:                 9,
		resumeGeneration:           0,
		resumeCorrelatedGeneration: 9,
		resumeCorrelated:           map[string]Stanza{record.MessageID: record.clone()},
	}
	if _, ok := session.claimResumedExternalService(8, record.MessageID); ok {
		t.Fatal("stale generation claimed resumed external-service result")
	}
	claimed, ok := session.claimResumedExternalService(9, record.MessageID)
	if !ok || !sameManagedStanza(claimed, record) {
		t.Fatalf("post-replay claim=(%#v,%t)", claimed, ok)
	}
	clearStanzaOwned(&claimed)
	if _, ok = session.claimResumedExternalService(9, record.MessageID); ok {
		t.Fatal("duplicate result claimed resumed external-service ownership")
	}
	if session.resumeCorrelated != nil || session.resumeCorrelatedGeneration != 0 {
		t.Fatalf("consumed ownership retained: generation=%d records=%v", session.resumeCorrelatedGeneration, session.resumeCorrelated)
	}
}

func TestResumedExternalServiceResultBeforeMarkerConsumesExactLedgerSlotAndSkipsReplay(t *testing.T) {
	payload, err := encodeExternalServiceQuery("services", ExternalService{})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(payload)
	record := Stanza{
		Kind: StanzaExternalServiceQuery, From: "a@example.test/mesh", To: "example.test",
		MeshID: "mesh", AttemptID: "services",
		MessageID: externalServiceIDPrefix + strings.Repeat("D", externalServiceRandomLength), Data: payload,
	}
	management, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 4, StanzaBudgetBytes: 1 << 20}, Endpoint{})
	session.username = "a@example.test"
	session.meshID = "mesh"
	session.management = management
	session.session = &xmpp.Session{}
	session.ctx = context.Background()
	session.generation = 9
	session.resumeGeneration = 9
	session.resumeReplay = []Stanza{record.clone()}
	session.resumeCorrelatedGeneration = 9
	session.resumeCorrelated = map[string]Stanza{record.MessageID: record.clone()}
	session.resumeBarrierGeneration = 9
	session.resumeBarrier = &resumeAuthorityBarrier{done: make(chan struct{})}
	session.setAuthorityIngressFence(func(uint64) bool { return true })
	if session.resumeMarkerSeen {
		t.Fatal("test did not begin before the post-resume marker")
	}

	response := `<iq xmlns="jabber:client" from="example.test" to="a@example.test/mesh" type="result" id="` + record.MessageID + `"><services xmlns="urn:xmpp:extdisco:2"><service host="stun.test" port="3478" transport="udp" type="stun"/></services></iq>`
	decoder := xml.NewDecoder(strings.NewReader(response))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := token.(xml.StartElement)
	var output bytes.Buffer
	encoder := xml.NewEncoder(&output)
	stream := &qaTokenReadEncoder{decoder: decoder, encoder: encoder}
	generationCtx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(9))
	if err = session.handleElement(generationCtx, stream, &start); err != nil {
		t.Fatalf("pre-marker response: %v", err)
	}
	if err = encoder.Flush(); err != nil || output.Len() != 0 {
		t.Fatalf("pre-marker response output=%q err=%v", output.String(), err)
	}
	if session.resumeMarkerSeen {
		t.Fatal("external-service response forged the authority phase marker")
	}
	if management.Pending() != 0 || management.HandledInbound() != 1 {
		t.Fatalf("ledger after response: pending=%d inbound=%d", management.Pending(), management.HandledInbound())
	}
	if session.resumeCorrelated != nil || session.resumeCorrelatedGeneration != 0 {
		t.Fatalf("correlation survived exact response: generation=%d records=%v", session.resumeCorrelatedGeneration, session.resumeCorrelated)
	}
	event := <-session.controlEvents
	if event.Kind != EventHandled || event.HandledCount != 1 || event.sessionGeneration != 9 {
		clearEventOwned(&event)
		t.Fatalf("handled event=%+v", event)
	}
	clearEventOwned(&event)

	prepared := false
	if err = session.ReplayPrepared(context.Background(), func(Stanza) bool { return true }, func(Stanza) (preparedReplayStanza, bool) {
		prepared = true
		return preparedReplayStanza{}, false
	}); err != nil {
		t.Fatalf("replay after pre-marker response: %v", err)
	}
	if prepared {
		t.Fatal("already answered external-service request was replayed")
	}
	if session.resumeGeneration != 0 || management.Pending() != 0 {
		t.Fatalf("replay cleanup: generation=%d pending=%d", session.resumeGeneration, management.Pending())
	}
}

func TestResumedExternalServiceMalformedResultCannotCorruptLedgerOrBeClaimedTwice(t *testing.T) {
	payload, err := encodeExternalServiceQuery("services", ExternalService{})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(payload)
	record := Stanza{
		Kind: StanzaExternalServiceQuery, From: "a@example.test/mesh", To: "example.test",
		MeshID: "mesh", AttemptID: "services",
		MessageID: externalServiceIDPrefix + strings.Repeat("E", externalServiceRandomLength), Data: payload,
	}
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(record); err != nil {
		t.Fatal(err)
	}
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 2, StanzaBudgetBytes: 1 << 20}, Endpoint{})
	session.username = "a@example.test"
	session.meshID = "mesh"
	session.management = management
	session.generation = 12
	session.resumeCorrelatedGeneration = 12
	session.resumeCorrelated = map[string]Stanza{record.MessageID: record.clone()}

	malformed := `<iq xmlns="jabber:client" from="example.test" to="a@example.test/mesh" type="result" id="` + record.MessageID + `"><services xmlns="urn:xmpp:extdisco:2"><unexpected/></services></iq>`
	decoder := xml.NewDecoder(strings.NewReader(malformed))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := token.(xml.StartElement)
	stream := &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(&bytes.Buffer{})}
	generationCtx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(12))
	if err = session.handleElement(generationCtx, stream, &start); !errors.Is(err, ErrProtocol) {
		t.Fatalf("malformed result error=%v", err)
	}
	if management.Pending() != 1 || management.HandledInbound() != 0 {
		t.Fatalf("malformed result changed ledger: pending=%d inbound=%d", management.Pending(), management.HandledInbound())
	}
	if _, ok := session.claimResumedExternalService(12, record.MessageID); ok {
		t.Fatal("malformed response correlation was claimable twice")
	}
	if pending := management.PendingSnapshot(); len(pending) != 1 || !sameManagedStanza(pending[0], record) {
		clearStanzas(pending)
		t.Fatalf("retained request changed after malformed result: %#v", pending)
	} else {
		clearStanzas(pending)
	}
}
