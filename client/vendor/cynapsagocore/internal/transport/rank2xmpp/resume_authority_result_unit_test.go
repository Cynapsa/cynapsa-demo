package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

func resumeAuthorityTestSession(t *testing.T) (*melliumSession, context.Context, *resumeAuthorityBarrier) {
	t.Helper()
	management, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	barrier := &resumeAuthorityBarrier{done: make(chan struct{})}
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 8, StanzaBudgetBytes: 4096}, Endpoint{})
	session.username = "agent@example.test"
	session.meshID = "mesh"
	session.management = management
	session.generation = 7
	session.resumeGeneration = 7
	session.resumeBarrier = barrier
	session.resumeBarrierGeneration = 7
	session.controlLaneActive = true
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	return session, ctx, barrier
}

func resumeAuthorityWire(t *testing.T, id, from, to, status string) (xml.StartElement, stanza.IQ, []byte) {
	t.Helper()
	fromJID, err := jid.Parse(from)
	if err != nil {
		t.Fatal(err)
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		t.Fatal(err)
	}
	outer := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: from},
		{Name: xml.Name{Local: "to"}, Value: to},
		{Name: xml.Name{Local: "type"}, Value: "result"},
		{Name: xml.Name{Local: "id"}, Value: id},
	}}
	iq := stanza.IQ{XMLName: outer.Name, From: fromJID, To: toJID, Type: stanza.ResultIQ, ID: id}
	payload := []byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="` + status + `"></resume-authority>`)
	return outer, iq, payload
}

func deliverResumeAuthority(t *testing.T, session *melliumSession, ctx context.Context, id, from, to, status string) error {
	t.Helper()
	outer, iq, payload := resumeAuthorityWire(t, id, from, to, status)
	return session.handleResumeAuthorityResult(ctx, melliumIQChildDecoder(t, payload, outer), outer, iq)
}

func appendXMLLang(outer *xml.StartElement, language string) {
	outer.Attr = append(outer.Attr, xml.Attr{
		Name:  xml.Name{Space: "http://www.w3.org/XML/1998/namespace", Local: "lang"},
		Value: language,
	})
}

func deliverLateServerTime(t *testing.T, session *melliumSession, ctx context.Context, id, from, to string, payload []byte) error {
	t.Helper()
	outer := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: from},
		{Name: xml.Name{Local: "to"}, Value: to},
		{Name: xml.Name{Local: "type"}, Value: "result"},
		{Name: xml.Name{Local: "id"}, Value: id},
	}}
	wire := &qaTokenReadEncoder{
		decoder: melliumIQChildDecoder(t, payload, outer),
		encoder: xml.NewEncoder(io.Discard),
	}
	return session.handleElement(ctx, wire, &outer)
}

func exactServerTimePayload() []byte {
	return []byte(`<time xmlns="urn:xmpp:time"><tzo>Z</tzo><utc>2030-01-02T03:04:05.000000Z</utc></time>`)
}

func receiveHandledControl(t *testing.T, session *melliumSession) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := session.ReceiveControl(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != EventHandled {
		clearEventOwned(&event)
		t.Fatalf("control event=%#v", event)
	}
	return event
}

func deliverResumePhaseSeparator(t *testing.T, session *melliumSession, ctx context.Context) {
	t.Helper()
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "r"}}
	wire := &qaTokenReadEncoder{decoder: &qaTokenReader{tokens: []xml.Token{start.End()}}, encoder: xml.NewEncoder(io.Discard)}
	if err := session.handleElement(ctx, wire, &start); err != nil {
		t.Fatal(err)
	}
}

func completeResumeAuthorityControl(t *testing.T, session *melliumSession) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := session.ReceiveControl(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != EventResumeAuthorityResult || event.resumeBarrier == nil {
		clearEventOwned(&event)
		t.Fatalf("authority event=%#v", event)
	}
	close(event.resumeBarrier.done)
	return event
}

func TestResumeAuthorityIgnoresOldQueuedResultBeforeCurrentPhase(t *testing.T) {
	session, ctx, barrier := resumeAuthorityTestSession(t)
	if err := deliverResumeAuthority(t, session, ctx, resumeAuthorityIDPrefix+"1", "example.test", "agent@example.test/mesh", "ready"); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.controlEvents:
		clearEventOwned(&event)
		t.Fatal("pre-separator result entered the current control barrier")
	default:
	}
	if session.resumeResultQueued || barrier.ready.Load() {
		t.Fatal("pre-separator result acquired current one-use ownership")
	}
	deliverResumePhaseSeparator(t, session, ctx)
	if err := deliverResumeAuthority(t, session, ctx, resumeAuthorityIDPrefix+"2", "example.test", "agent@example.test/mesh", "ready"); err != nil {
		t.Fatal(err)
	}
	event := completeResumeAuthorityControl(t, session)
	event.resumeBarrier = nil
	clearEventOwned(&event)
	if err := session.WaitResumeAuthorityResult(context.Background()); err != nil {
		t.Fatalf("current result rejected: %v", err)
	}
	if _, handled, _ := session.management.ResumeState(); handled != 2 {
		t.Fatalf("handled=%d, want both stale replay and current result", handled)
	}
}

func TestResumeAuthorityRequestAloneNeverReleasesBarrier(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	deliverResumePhaseSeparator(t, session, ctx)
	wait, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := session.WaitResumeAuthorityResult(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("<r/> released authority: %v", err)
	}
}

func TestResumeAuthorityNotReadyAndMalformedFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		status  string
	}{
		{name: "not-ready", status: "not-ready"},
		{name: "malformed", payload: []byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="ready" extra="x"/>`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, ctx, _ := resumeAuthorityTestSession(t)
			deliverResumePhaseSeparator(t, session, ctx)
			outer, iq, payload := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"3", "example.test", "agent@example.test/mesh", test.status)
			if test.payload != nil {
				payload = test.payload
			}
			err := session.handleResumeAuthorityResult(ctx, melliumIQChildDecoder(t, payload, outer), outer, iq)
			if test.name == "malformed" && !errors.Is(err, ErrProtocol) {
				t.Fatalf("malformed error=%v", err)
			}
			if test.name == "not-ready" && err != nil {
				t.Fatal(err)
			}
			event := completeResumeAuthorityControl(t, session)
			event.resumeBarrier = nil
			clearEventOwned(&event)
			if err = session.WaitResumeAuthorityResult(context.Background()); !errors.Is(err, ErrAuthorityRejected) {
				t.Fatalf("failed result released authority: %v", err)
			}
		})
	}
}

func TestResumeAuthorityDuplicateCannotReleaseReadyBarrier(t *testing.T) {
	session, ctx, barrier := resumeAuthorityTestSession(t)
	deliverResumePhaseSeparator(t, session, ctx)
	if err := deliverResumeAuthority(t, session, ctx, resumeAuthorityIDPrefix+"4", "example.test", "agent@example.test/mesh", "ready"); err != nil {
		t.Fatal(err)
	}
	if err := deliverResumeAuthority(t, session, ctx, resumeAuthorityIDPrefix+"5", "example.test", "agent@example.test/mesh", "ready"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("duplicate error=%v", err)
	}
	if barrier.ready.Load() {
		t.Fatal("duplicate retained a ready result")
	}
	event := completeResumeAuthorityControl(t, session)
	event.resumeBarrier = nil
	clearEventOwned(&event)
	if err := session.WaitResumeAuthorityResult(context.Background()); !errors.Is(err, ErrAuthorityRejected) {
		t.Fatalf("duplicate released authority: %v", err)
	}
}

func TestResumeAuthorityAcceptsCanonicalOptionalXMLLang(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	deliverResumePhaseSeparator(t, session, ctx)
	decoder := xml.NewDecoder(strings.NewReader(
		`<iq xmlns="jabber:client" from="example.test" to="agent@example.test/mesh" type="result" id="` +
			resumeAuthorityIDPrefix + `11" xml:lang="en-US"><resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="ready"/></iq>`,
	))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name != (xml.Name{Space: stanza.NSClient, Local: "iq"}) {
		t.Fatalf("outer token=%#v", token)
	}
	wire := &qaTokenReadEncoder{
		decoder: decoder,
		encoder: xml.NewEncoder(io.Discard),
	}
	if err = session.handleElement(ctx, wire, &outer); err != nil {
		t.Fatalf("canonical xml:lang result rejected: %v", err)
	}
	if got := session.management.HandledInbound(); got != 1 {
		t.Fatalf("inbound accounting before barrier release=%d, want 1", got)
	}
	event := completeResumeAuthorityControl(t, session)
	event.resumeBarrier = nil
	clearEventOwned(&event)
	if err := session.WaitResumeAuthorityResult(context.Background()); err != nil {
		t.Fatalf("canonical xml:lang result did not release barrier: %v", err)
	}
}

func TestResumeAuthorityCanonicalXMLLangOwnershipIsOneUse(t *testing.T) {
	session, ctx, barrier := resumeAuthorityTestSession(t)
	deliverResumePhaseSeparator(t, session, ctx)
	outer, _, payload := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"12", "example.test", "agent@example.test/mesh", "ready")
	appendXMLLang(&outer, "en-US")
	deliver := func() error {
		wire := &qaTokenReadEncoder{
			decoder: melliumIQChildDecoder(t, payload, outer),
			encoder: xml.NewEncoder(io.Discard),
		}
		return session.handleElement(ctx, wire, &outer)
	}
	if err := deliver(); err != nil {
		t.Fatal(err)
	}
	if err := deliver(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("duplicate result error=%v", err)
	}
	if barrier.ready.Load() {
		t.Fatal("duplicate result retained ready authority")
	}
	if got := session.management.HandledInbound(); got != 1 {
		t.Fatalf("duplicate changed inbound accounting to %d", got)
	}
	event := completeResumeAuthorityControl(t, session)
	event.resumeBarrier = nil
	clearEventOwned(&event)
	if err := session.WaitResumeAuthorityResult(context.Background()); !errors.Is(err, ErrAuthorityRejected) {
		t.Fatalf("duplicate released authority: %v", err)
	}
}

func TestResumeAuthorityRejectsInvalidXMLLangAndWrongAttributes(t *testing.T) {
	tests := map[string]func(*xml.StartElement){
		"duplicate-xml-lang": func(outer *xml.StartElement) {
			appendXMLLang(outer, "en-US")
			appendXMLLang(outer, "fr")
		},
		"empty-xml-lang": func(outer *xml.StartElement) {
			appendXMLLang(outer, "")
		},
		"invalid-xml-lang": func(outer *xml.StartElement) {
			appendXMLLang(outer, "en_US")
		},
		"wrong-lang-namespace": func(outer *xml.StartElement) {
			outer.Attr = append(outer.Attr, xml.Attr{Name: xml.Name{Space: "urn:evil", Local: "lang"}, Value: "en"})
		},
		"unknown-attribute": func(outer *xml.StartElement) {
			outer.Attr = append(outer.Attr, xml.Attr{Name: xml.Name{Local: "extra"}, Value: "x"})
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			session, ctx, barrier := resumeAuthorityTestSession(t)
			deliverResumePhaseSeparator(t, session, ctx)
			outer, _, payload := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"13", "example.test", "agent@example.test/mesh", "ready")
			mutate(&outer)
			wire := &qaTokenReadEncoder{
				decoder: melliumIQChildDecoder(t, payload, outer),
				encoder: xml.NewEncoder(io.Discard),
			}
			if err := session.handleElement(ctx, wire, &outer); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("invalid attributes error=%v", err)
			}
			if session.resumeResultQueued || barrier.ready.Load() || session.management.HandledInbound() != 0 {
				t.Fatalf("invalid attributes acquired ownership: queued=%t ready=%t inbound=%d", session.resumeResultQueued, barrier.ready.Load(), session.management.HandledInbound())
			}
			select {
			case event := <-session.controlEvents:
				clearEventOwned(&event)
				t.Fatal("invalid attributes emitted control event")
			default:
			}
		})
	}
}

func TestResumeAuthorityValidatesExactOwnerAndGeneration(t *testing.T) {
	for name, mutate := range map[string]func(*xml.StartElement, *stanza.IQ){
		"sender": func(outer *xml.StartElement, iq *stanza.IQ) {
			outer.Attr[0].Value = "evil.test"
			iq.From, _ = jid.Parse("evil.test")
		},
		"recipient": func(outer *xml.StartElement, iq *stanza.IQ) {
			outer.Attr[1].Value = "other@example.test/mesh"
			iq.To, _ = jid.Parse("other@example.test/mesh")
		},
		"id": func(outer *xml.StartElement, iq *stanza.IQ) {
			outer.Attr[3].Value = "client-chosen"
			iq.ID = "client-chosen"
		},
	} {
		t.Run(name, func(t *testing.T) {
			session, ctx, _ := resumeAuthorityTestSession(t)
			deliverResumePhaseSeparator(t, session, ctx)
			outer, iq, payload := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"6", "example.test", "agent@example.test/mesh", "ready")
			mutate(&outer, &iq)
			if err := session.handleResumeAuthorityResult(ctx, melliumIQChildDecoder(t, payload, outer), outer, iq); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("owner error=%v", err)
			}
			event := completeResumeAuthorityControl(t, session)
			event.resumeBarrier = nil
			clearEventOwned(&event)
			if err := session.WaitResumeAuthorityResult(context.Background()); !errors.Is(err, ErrAuthorityRejected) {
				t.Fatalf("invalid owner released authority: %v", err)
			}
		})
	}

	session, _, barrier := resumeAuthorityTestSession(t)
	stale := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(6))
	outer, iq, payload := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"7", "example.test", "agent@example.test/mesh", "ready")
	if err := session.handleResumeAuthorityResult(stale, melliumIQChildDecoder(t, payload, outer), outer, iq); !errors.Is(err, ErrProtocol) {
		t.Fatalf("stale generation error=%v", err)
	}
	if barrier.ready.Load() || session.resumeResultQueued {
		t.Fatal("stale generation mutated the current barrier")
	}
}

func TestResumeAuthorityPayloadDecoderIsCanonicalAndBounded(t *testing.T) {
	outer, _, _ := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"8", "example.test", "agent@example.test/mesh", "ready")
	oversized := []byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="ready">`)
	for len(oversized) < 5000 {
		oversized = append(oversized, 'x')
	}
	oversized = append(oversized, []byte(`</resume-authority>`)...)
	for _, payload := range [][]byte{
		[]byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="READY"/>`),
		[]byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:2" status="ready"/>`),
		[]byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="ready"><child/></resume-authority>`),
		oversized,
	} {
		if _, err := decodeResumeAuthorityResult(melliumIQChildDecoder(t, payload, outer), outer, 4096); !errors.Is(err, ErrProtocol) {
			t.Fatalf("non-canonical payload accepted: %v", err)
		}
	}
}

func TestResumeAuthorityReadyThenLateServerTimeResultKeepsStreamUsable(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	deliverResumePhaseSeparator(t, session, ctx)
	if err := deliverResumeAuthority(t, session, ctx, resumeAuthorityIDPrefix+"9", "example.test", "agent@example.test/mesh", "ready"); err != nil {
		t.Fatal(err)
	}
	event := completeResumeAuthorityControl(t, session)
	event.resumeBarrier = nil
	clearEventOwned(&event)
	if err := session.WaitResumeAuthorityResult(context.Background()); err != nil {
		t.Fatalf("ready barrier: %v", err)
	}

	id := entityTimeIDPrefix + strings.Repeat("A", entityTimeIDRandomLength)
	request := Stanza{
		Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
		MeshID: "mesh", MessageID: id,
	}
	if _, err := session.management.RecordSentTracked(request); err != nil {
		t.Fatal(err)
	}
	outer := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "example.test"},
		{Name: xml.Name{Local: "to"}, Value: "agent@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "result"},
		{Name: xml.Name{Local: "id"}, Value: id},
	}}
	payload := []byte(`<time xmlns="urn:xmpp:time"><tzo>Z</tzo><utc>2030-01-02T03:04:05.000000Z</utc></time>`)
	wire := &qaTokenReadEncoder{
		decoder: melliumIQChildDecoder(t, payload, outer),
		encoder: xml.NewEncoder(io.Discard),
	}
	if err := session.handleElement(ctx, wire, &outer); err != nil {
		t.Fatalf("late correlated time result retired resumed stream: %v", err)
	}
	if pending := session.management.PendingSnapshot(); len(pending) != 0 {
		t.Fatalf("late correlated time result remained ambiguous: %#v", pending)
	}
	session.mu.Lock()
	suspended := session.suspended
	session.mu.Unlock()
	if suspended {
		t.Fatal("late correlated time result suspended resumed stream")
	}
}

func TestBareServerResultDispatchRequiresExactCurrentOwner(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	resumeOuter, resumeIQ, _ := resumeAuthorityWire(t, resumeAuthorityIDPrefix+"10", "example.test", "agent@example.test/mesh", "ready")
	owner, err := session.classifyBareServerResult(ctx, resumeOuter, resumeIQ)
	if err != nil || owner.kind != bareServerResultResumeAuthority {
		t.Fatalf("resume owner=%v error=%v", owner.kind, err)
	}

	timeID := entityTimeIDPrefix + strings.Repeat("A", entityTimeIDRandomLength)
	timeOuter, timeIQ, _ := resumeAuthorityWire(t, timeID, "example.test", "agent@example.test/mesh", "ready")
	request := Stanza{
		Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
		MeshID: "mesh", MessageID: timeID,
	}
	if _, err = session.management.RecordSentTracked(request); err != nil {
		t.Fatal(err)
	}
	owner, err = session.classifyBareServerResult(ctx, timeOuter, timeIQ)
	if err != nil || owner.kind != bareServerResultEntityTime || owner.management != session.management {
		t.Fatalf("time owner=%#v error=%v", owner, err)
	}
	if duplicate, duplicateErr := session.classifyBareServerResult(ctx, timeOuter, timeIQ); duplicate.kind != bareServerResultUnowned || !errors.Is(duplicateErr, ErrAuthentication) {
		t.Fatalf("duplicate owner=%#v error=%v", duplicate, duplicateErr)
	}

	for name, id := range map[string]string{
		"resume-prefix-only": resumeAuthorityIDPrefix + "time-attack",
		"time-prefix-only":   entityTimeIDPrefix + "short",
		"discovery":          authorityDiscoveryIDPrefix + strings.Repeat("B", privateIQRandomLength),
		"other-correlated":   externalServiceIDPrefix + strings.Repeat("C", externalServiceRandomLength),
	} {
		t.Run(name, func(t *testing.T) {
			outer, iq, _ := resumeAuthorityWire(t, id, "example.test", "agent@example.test/mesh", "ready")
			if got, classifyErr := session.classifyBareServerResult(ctx, outer, iq); got.kind != bareServerResultUnowned || !errors.Is(classifyErr, ErrAuthentication) {
				t.Fatalf("owner=%v error=%v", got.kind, classifyErr)
			}
		})
	}

	stale := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(6))
	if got, classifyErr := session.classifyBareServerResult(stale, resumeOuter, resumeIQ); got.kind != bareServerResultUnowned || !errors.Is(classifyErr, ErrProtocol) {
		t.Fatalf("stale owner=%v error=%v", got.kind, classifyErr)
	}
}

func TestLateServerTimeRequiresExactRetainedIDOwnerAndGeneration(t *testing.T) {
	timeID := entityTimeIDPrefix + strings.Repeat("D", entityTimeIDRandomLength)
	otherID := entityTimeIDPrefix + strings.Repeat("E", entityTimeIDRandomLength)
	for name, mutate := range map[string]func(*context.Context, *string, *string, *string){
		"wrong-id":        func(_ *context.Context, id, _, _ *string) { *id = otherID },
		"wrong-sender":    func(_ *context.Context, _, from, _ *string) { *from = "evil.test" },
		"wrong-recipient": func(_ *context.Context, _, _, to *string) { *to = "other@example.test/mesh" },
		"stale-generation": func(ctx *context.Context, _, _, _ *string) {
			*ctx = context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(6))
		},
	} {
		t.Run(name, func(t *testing.T) {
			session, ctx, _ := resumeAuthorityTestSession(t)
			request := Stanza{
				Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
				MeshID: "mesh", MessageID: timeID,
			}
			if _, err := session.management.RecordSentTracked(request); err != nil {
				t.Fatal(err)
			}
			id, from, to := timeID, "example.test", "agent@example.test/mesh"
			mutate(&ctx, &id, &from, &to)
			err := deliverLateServerTime(t, session, ctx, id, from, to, exactServerTimePayload())
			if !errors.Is(err, ErrAuthentication) && !errors.Is(err, ErrProtocol) {
				t.Fatalf("error=%v", err)
			}
			pending := session.management.PendingSnapshot()
			if len(pending) != 1 || pending[0].Kind != StanzaTimeCalibration || pending[0].MessageID != timeID || session.management.HandledInbound() != 0 {
				t.Fatalf("pending=%#v inbound=%d", pending, session.management.HandledInbound())
			}
			select {
			case event := <-session.controlEvents:
				clearEventOwned(&event)
				t.Fatal("unowned result emitted handled evidence")
			default:
			}
		})
	}
}

func TestLateServerTimeExactResultAccountsOnceWithoutLedgerLeak(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	timeID := entityTimeIDPrefix + strings.Repeat("F", entityTimeIDRandomLength)
	if err := session.management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 41, MessageID: "msg"}); err != nil {
		t.Fatal(err)
	}
	request := Stanza{
		Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
		MeshID: "mesh", MessageID: timeID,
	}
	if _, err := session.management.RecordSentTracked(request); err != nil {
		t.Fatal(err)
	}
	if err := deliverLateServerTime(t, session, ctx, timeID, "example.test", "agent@example.test/mesh", exactServerTimePayload()); err != nil {
		t.Fatal(err)
	}
	event := receiveHandledControl(t, session)
	if event.HandledThrough != 41 || event.HandledCount != 2 {
		clearEventOwned(&event)
		t.Fatalf("handled event=%#v", event)
	}
	clearEventOwned(&event)
	if session.management.Pending() != 0 || session.management.PendingBytes() != 0 || session.management.HandledInbound() != 1 {
		t.Fatalf("pending=%d bytes=%d inbound=%d", session.management.Pending(), session.management.PendingBytes(), session.management.HandledInbound())
	}
	if err := deliverLateServerTime(t, session, ctx, timeID, "example.test", "agent@example.test/mesh", exactServerTimePayload()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("duplicate error=%v", err)
	}
	if session.management.Pending() != 0 || session.management.PendingBytes() != 0 || session.management.HandledInbound() != 1 {
		t.Fatalf("duplicate mutated ledger pending=%d bytes=%d inbound=%d", session.management.Pending(), session.management.PendingBytes(), session.management.HandledInbound())
	}
	select {
	case duplicate := <-session.controlEvents:
		clearEventOwned(&duplicate)
		t.Fatal("duplicate emitted a second acknowledgement")
	default:
	}
}

func TestLateServerTimeMalformedPayloadCommitsOutboundProofOnlyOnce(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	timeID := entityTimeIDPrefix + strings.Repeat("G", entityTimeIDRandomLength)
	if err := session.management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 51, MessageID: "msg"}); err != nil {
		t.Fatal(err)
	}
	request := Stanza{
		Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
		MeshID: "mesh", MessageID: timeID,
	}
	if _, err := session.management.RecordSentTracked(request); err != nil {
		t.Fatal(err)
	}
	malformed := []byte(`<time xmlns="urn:xmpp:time"><tzo>Z</tzo><utc>invalid</utc></time>`)
	if err := deliverLateServerTime(t, session, ctx, timeID, "example.test", "agent@example.test/mesh", malformed); !errors.Is(err, ErrProtocol) {
		t.Fatalf("malformed error=%v", err)
	}
	event := receiveHandledControl(t, session)
	if event.HandledThrough != 51 || event.HandledCount != 2 {
		clearEventOwned(&event)
		t.Fatalf("handled event=%#v", event)
	}
	clearEventOwned(&event)
	if session.management.Pending() != 0 || session.management.PendingBytes() != 0 || session.management.HandledInbound() != 0 {
		t.Fatalf("pending=%d bytes=%d inbound=%d", session.management.Pending(), session.management.PendingBytes(), session.management.HandledInbound())
	}
	if err := deliverLateServerTime(t, session, ctx, timeID, "example.test", "agent@example.test/mesh", malformed); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("duplicate malformed error=%v", err)
	}
	select {
	case duplicate := <-session.controlEvents:
		clearEventOwned(&duplicate)
		t.Fatal("duplicate malformed result emitted a second acknowledgement")
	default:
	}
}

func TestLateServerTimeDuplicateRetainedOwnerFailsWithoutMutation(t *testing.T) {
	session, ctx, _ := resumeAuthorityTestSession(t)
	timeID := entityTimeIDPrefix + strings.Repeat("H", entityTimeIDRandomLength)
	request := Stanza{
		Kind: StanzaTimeCalibration, From: "agent@example.test/mesh", To: "example.test",
		MeshID: "mesh", MessageID: timeID,
	}
	for range 2 {
		if _, err := session.management.RecordSentTracked(request); err != nil {
			t.Fatal(err)
		}
	}
	beforeBytes := session.management.PendingBytes()
	if err := deliverLateServerTime(t, session, ctx, timeID, "example.test", "agent@example.test/mesh", exactServerTimePayload()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("duplicate owner error=%v", err)
	}
	if session.management.Pending() != 2 || session.management.PendingBytes() != beforeBytes || session.management.HandledInbound() != 0 {
		t.Fatalf("pending=%d bytes=%d/%d inbound=%d", session.management.Pending(), session.management.PendingBytes(), beforeBytes, session.management.HandledInbound())
	}
}

func TestEntityTimeIDRequiresExactRandTextShape(t *testing.T) {
	valid := entityTimeIDPrefix + strings.Repeat("A", entityTimeIDRandomLength)
	if !validEntityTimeID(valid) {
		t.Fatal("canonical ID rejected")
	}
	for _, id := range []string{
		entityTimeIDPrefix,
		valid + "A",
		entityTimeIDPrefix + strings.Repeat("A", entityTimeIDRandomLength-1),
		entityTimeIDPrefix + strings.Repeat("a", entityTimeIDRandomLength),
		entityTimeIDPrefix + strings.Repeat("0", entityTimeIDRandomLength),
		resumeAuthorityIDPrefix + "1",
	} {
		if validEntityTimeID(id) {
			t.Fatalf("non-canonical ID accepted: %q", id)
		}
	}
}
