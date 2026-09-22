package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

type smTokenReadEncoder struct {
	xml.TokenReader
	writes int
}

type smErrorTokenReader struct{ err error }

func (reader smErrorTokenReader) Token() (xml.Token, error) { return nil, reader.err }

func (encoder *smTokenReadEncoder) Encode(any) error {
	encoder.writes++
	return nil
}

func (encoder *smTokenReadEncoder) EncodeElement(any, xml.StartElement) error {
	encoder.writes++
	return nil
}

func (encoder *smTokenReadEncoder) EncodeToken(xml.Token) error {
	encoder.writes++
	return nil
}

func newStrictStreamManagement(t *testing.T, capacity int) *StreamManagement {
	t.Helper()
	management, err := NewStreamManagement(capacity, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume-id", true); err != nil {
		t.Fatal(err)
	}
	return management
}

func smElementReader(t *testing.T, raw string) (xml.StartElement, xml.TokenReader) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		t.Fatalf("first token = %T", token)
	}
	return start, decoder
}

func TestXEP0198AckRejectsMalformedCompleteElementBeforeLedgerMutation(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		start *xml.StartElement
	}{
		{name: "foreign namespace h", start: &xml.StartElement{
			Name: xml.Name{Space: streamManagementNamespace, Local: "a"},
			Attr: []xml.Attr{{Name: xml.Name{Space: "urn:foreign", Local: "h"}, Value: "1"}},
		}},
		{name: "qualified h with valid declarations", raw: `<sm:a xmlns:sm="urn:xmpp:sm:3" xmlns:foreign="urn:foreign" foreign:h="1"/>`},
		{name: "duplicate h", raw: `<a xmlns="urn:xmpp:sm:3" h="1" h="1"/>`},
		{name: "child element", raw: `<a xmlns="urn:xmpp:sm:3" h="1"><child/></a>`},
		{name: "whitespace content", raw: `<a xmlns="urn:xmpp:sm:3" h="1"> </a>`},
		{name: "text content", raw: `<a xmlns="urn:xmpp:sm:3" h="1">unexpected</a>`},
		{name: "unknown attribute", raw: `<a xmlns="urn:xmpp:sm:3" h="1" extra="value"/>`},
		{name: "missing h", raw: `<a xmlns="urn:xmpp:sm:3"/>`},
		{name: "incomplete element", raw: `<a xmlns="urn:xmpp:sm:3" h="1">`},
		{name: "leading zero", raw: `<a xmlns="urn:xmpp:sm:3" h="01"/>`},
		{name: "leading plus", raw: `<a xmlns="urn:xmpp:sm:3" h="+1"/>`},
		{name: "uint32 overflow", raw: `<a xmlns="urn:xmpp:sm:3" h="4294967296"/>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			management := newStrictStreamManagement(t, 4)
			pending := Stanza{Kind: StanzaEnvelope, MeshID: "mesh", Ordinal: 73, MessageID: "msg_AAAAAAAAAAAAAAAAAAAAAA", Data: []byte("still-owned")}
			if err := management.RecordSent(pending); err != nil {
				t.Fatal(err)
			}
			session := &melliumSession{management: management, events: make(chan Event, 1)}
			var start xml.StartElement
			var reader xml.TokenReader
			if test.start != nil {
				start = *test.start
				reader = &qaTokenReader{tokens: []xml.Token{start.End()}}
			} else {
				start, reader = smElementReader(t, test.raw)
			}
			wire := &tokenEncoder{decoder: reader, encoder: xml.NewEncoder(io.Discard)}
			if err := session.handleElement(context.Background(), wire, &start); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error = %v", err)
			}
			if management.Pending() != 1 {
				t.Fatalf("pending = %d, want 1", management.Pending())
			}
			snapshot := management.PendingSnapshot()
			if len(snapshot) != 1 || snapshot[0].Ordinal != pending.Ordinal || snapshot[0].MessageID != pending.MessageID || len(snapshot[0].Data) != 0 {
				t.Fatalf("ledger changed: %#v", snapshot)
			}
			select {
			case event := <-session.events:
				t.Fatalf("rejected acknowledgement emitted outbox signal: %#v", event)
			default:
			}
		})
	}
}

func TestXEP0198AckAcceptsExactCompleteElement(t *testing.T) {
	management := newStrictStreamManagement(t, 2)
	if err := management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 91, Data: []byte("owned")}); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{management: management, events: make(chan Event, 1)}
	start, reader := smElementReader(t, `<a xmlns="urn:xmpp:sm:3" h="1"/>`)
	wire := &tokenEncoder{decoder: reader, encoder: xml.NewEncoder(io.Discard)}
	if err := session.handleElement(context.Background(), wire, &start); err != nil {
		t.Fatal(err)
	}
	if management.Pending() != 0 {
		t.Fatalf("pending = %d", management.Pending())
	}
	event := <-session.events
	if event.Kind != EventHandled || event.HandledThrough != 91 || event.HandledCount != 1 {
		t.Fatalf("event = %#v", event)
	}
}

func TestEnvelopeCustodyConfirmsLedgerPrefixWithoutDependingOnRawAck(t *testing.T) {
	management := newStrictStreamManagement(t, 4)
	firstID := typed("msg_", 0x41)
	secondID := typed("msg_", 0x42)
	for _, record := range []Stanza{
		{Kind: StanzaSignalResult, AttemptID: "before"},
		{Kind: StanzaEnvelope, Ordinal: 71, MessageID: firstID},
		{Kind: StanzaSignalResult, AttemptID: "after"},
		{Kind: StanzaEnvelope, Ordinal: 72, MessageID: secondID},
	} {
		if err := management.RecordSent(record); err != nil {
			t.Fatal(err)
		}
	}

	ordinal, count, err := management.ConfirmEnvelopeCustody(firstID)
	if err != nil || ordinal != 71 || count != 2 || management.Pending() != 2 {
		t.Fatalf("receipt prefix: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if ordinal, count, err = management.ApplyAckDetailed(2); err != nil || ordinal != 0 || count != 0 || management.Pending() != 2 {
		t.Fatalf("same raw ack: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if ordinal, count, err = management.ApplyAckDetailed(3); err != nil || ordinal != 0 || count != 1 || management.Pending() != 1 {
		t.Fatalf("later raw ack: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if ordinal, count, err = management.ConfirmEnvelopeCustody(secondID); err != nil || ordinal != 72 || count != 1 || management.Pending() != 0 {
		t.Fatalf("second receipt: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
}

func TestEnvelopeCustodyAfterRawAckIsLedgerIdempotent(t *testing.T) {
	management := newStrictStreamManagement(t, 1)
	messageID := typed("msg_", 0x43)
	if err := management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 73, MessageID: messageID}); err != nil {
		t.Fatal(err)
	}
	if ordinal, count, err := management.ApplyAckDetailed(1); err != nil || ordinal != 73 || count != 1 {
		t.Fatalf("raw ack: ordinal=%d count=%d err=%v", ordinal, count, err)
	}
	if ordinal, count, err := management.ConfirmEnvelopeCustody(messageID); err != nil || ordinal != 0 || count != 0 || management.Pending() != 0 {
		t.Fatalf("post-ack receipt: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
}

func TestEnvelopeCustodyAllowsEarlierMonotonicServerAck(t *testing.T) {
	management := newStrictStreamManagement(t, 3)
	messageID := typed("msg_", 0x44)
	for _, record := range []Stanza{
		{Kind: StanzaSignalResult, AttemptID: "setup"},
		{Kind: StanzaEnvelope, Ordinal: 74, MessageID: messageID},
		{Kind: StanzaSignalResult, AttemptID: "later"},
	} {
		if err := management.RecordSent(record); err != nil {
			t.Fatal(err)
		}
	}
	if ordinal, count, err := management.ConfirmEnvelopeCustody(messageID); err != nil || ordinal != 74 || count != 2 {
		t.Fatalf("custody: ordinal=%d count=%d err=%v", ordinal, count, err)
	}
	if ordinal, count, err := management.ApplyAckDetailed(1); err != nil || ordinal != 0 || count != 0 || management.Pending() != 1 {
		t.Fatalf("earlier server ack: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if ordinal, count, err := management.ApplyAckDetailed(2); err != nil || ordinal != 0 || count != 0 || management.Pending() != 1 {
		t.Fatalf("matching server ack: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if ordinal, count, err := management.ApplyAckDetailed(3); err != nil || ordinal != 0 || count != 1 || management.Pending() != 0 {
		t.Fatalf("later server ack: ordinal=%d count=%d pending=%d err=%v", ordinal, count, management.Pending(), err)
	}
	if _, _, err := management.ApplyAckDetailed(2); !errors.Is(err, ErrProtocol) {
		t.Fatalf("regressing server ack error=%v", err)
	}
}

func TestCorrelatedHandledAcceptsSequenceZeroAfterUint32Wrap(t *testing.T) {
	management := newStrictStreamManagement(t, 2)
	management.mu.Lock()
	management.outbound = math.MaxUint32
	management.acked = math.MaxUint32
	management.mu.Unlock()

	sequence, err := management.RecordSentTracked(Stanza{
		Kind: StanzaTimeCalibration, MessageID: "wrapped-sequence",
	})
	if err != nil || sequence != 0 {
		t.Fatalf("wrapped record: sequence=%d err=%v", sequence, err)
	}
	ordinal, count, err := management.ConfirmCorrelatedHandled(sequence)
	if err != nil || ordinal != 0 || count != 1 || management.Pending() != 0 {
		t.Fatalf("wrapped confirmation: ordinal=%d count=%d pending=%d err=%v",
			ordinal, count, management.Pending(), err)
	}
}

func TestXEP0198AckPreservesElementReadFailureWithoutLedgerMutation(t *testing.T) {
	management := newStrictStreamManagement(t, 2)
	if err := management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 92, Data: []byte("owned")}); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{management: management, events: make(chan Event, 1)}
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "a"}, Attr: []xml.Attr{{Name: xml.Name{Local: "h"}, Value: "1"}}}
	readErr := errors.New("injected element read failure")
	wire := &tokenEncoder{decoder: smErrorTokenReader{err: readErr}, encoder: xml.NewEncoder(io.Discard)}
	if err := session.handleElement(context.Background(), wire, &start); !errors.Is(err, readErr) {
		t.Fatalf("error = %v", err)
	}
	if management.Pending() != 1 {
		t.Fatalf("pending = %d, want 1", management.Pending())
	}
	select {
	case event := <-session.events:
		t.Fatalf("read failure emitted outbox signal: %#v", event)
	default:
	}
}

func TestXEP0198AcceptsNamespaceEquivalentPrefixedControls(t *testing.T) {
	management := newStrictStreamManagement(t, 2)
	if err := management.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 93, Data: []byte("owned")}); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{management: management, events: make(chan Event, 1)}
	start, reader := smElementReader(t, `<sm:a xmlns:sm="urn:xmpp:sm:3" xmlns="jabber:client" xmlns:unused="urn:unused" h="1"/>`)
	wire := &tokenEncoder{decoder: reader, encoder: xml.NewEncoder(io.Discard)}
	if err := session.handleElement(context.Background(), wire, &start); err != nil {
		t.Fatal(err)
	}
	if event := <-session.events; event.Kind != EventHandled || event.HandledThrough != 93 || event.HandledCount != 1 {
		t.Fatalf("ack event = %#v", event)
	}

	management.MarkHandledInbound()
	start, reader = smElementReader(t, `<sm:r xmlns:sm="urn:xmpp:sm:3"/>`)
	requestWire := &smTokenReadEncoder{TokenReader: reader}
	if err := session.handleElement(context.Background(), requestWire, &start); err != nil {
		t.Fatal(err)
	}
	if requestWire.writes != 2 {
		t.Fatalf("request response writes = %d, want 2", requestWire.writes)
	}

	start, reader = smElementReader(t, `<sm:enabled xmlns:sm="urn:xmpp:sm:3" id="resume-id" resume="true" max="30"/>`)
	if id, err := decodeSMEnabled(reader, start); err != nil || id != "resume-id" {
		t.Fatalf("enabled id = %q, error = %v", id, err)
	}
	start, reader = smElementReader(t, `<sm:resumed xmlns:sm="urn:xmpp:sm:3" h="0" previd="resume-id"/>`)
	if handled, err := decodeSMResumed(reader, start, "resume-id"); err != nil || handled != 0 {
		t.Fatalf("resumed h = %d, error = %v", handled, err)
	}

	start, reader = smElementReader(t, `<sm:a xmlns:sm="urn:foreign" h="0"/>`)
	if _, err := decodeSMAck(reader, start); !errors.Is(err, ErrProtocol) {
		t.Fatalf("wrong expanded name error = %v", err)
	}
	start, reader = smElementReader(t, `<sm:a xmlns:sm="urn:xmpp:sm:3" xmlns:xml="urn:wrong" h="0"/>`)
	if _, err := decodeSMAck(reader, start); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid reserved prefix declaration error = %v", err)
	}
}

func TestXEP0198RequestRejectsAttributesAndContentBeforeResponse(t *testing.T) {
	for _, raw := range []string{
		`<r xmlns="urn:xmpp:sm:3" extra="value"/>`,
		`<r xmlns="urn:xmpp:sm:3"><child/></r>`,
		`<r xmlns="urn:xmpp:sm:3"> </r>`,
	} {
		management := newStrictStreamManagement(t, 2)
		management.MarkHandledInbound()
		session := &melliumSession{management: management}
		start, reader := smElementReader(t, raw)
		wire := &smTokenReadEncoder{TokenReader: reader}
		if err := session.handleElement(context.Background(), wire, &start); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: error = %v", raw, err)
		}
		if wire.writes != 0 {
			t.Fatalf("%s: response writes = %d", raw, wire.writes)
		}
		if management.HandledInbound() != 1 {
			t.Fatalf("%s: handled = %d", raw, management.HandledInbound())
		}
	}
}

func TestXEP0198NegotiationResponsesRequireExactAttributesAndEmptyContent(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		start, reader := smElementReader(t, `<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="1" max="60" location="example.test:5222"/>`)
		id, err := decodeSMEnabled(reader, start)
		if err != nil || id != "resume-id" {
			t.Fatalf("id = %q, error = %v", id, err)
		}
		invalid := []string{
			`<enabled xmlns="urn:xmpp:sm:3" id="resume-id" id="other" resume="true"/>`,
			`<enabled xmlns="urn:xmpp:sm:3" xmlns:foreign="urn:foreign" id="resume-id" foreign:resume="true"/>`,
			`<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="true" extra="value"/>`,
			`<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="true"><child/></enabled>`,
			`<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="true"> </enabled>`,
			`<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="false"/>`,
		}
		for _, raw := range invalid {
			start, reader = smElementReader(t, raw)
			if _, err = decodeSMEnabled(reader, start); !errors.Is(err, ErrProtocol) {
				t.Fatalf("accepted %s: %v", raw, err)
			}
		}
	})

	t.Run("resumed", func(t *testing.T) {
		start, reader := smElementReader(t, `<resumed xmlns="urn:xmpp:sm:3" h="0" previd="resume-id"/>`)
		h, err := decodeSMResumed(reader, start, "resume-id")
		if err != nil || h != 0 {
			t.Fatalf("h = %d, error = %v", h, err)
		}
		invalid := []string{
			`<resumed xmlns="urn:xmpp:sm:3" h="0"/>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="0" previd="other"/>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="0" h="1" previd="resume-id"/>`,
			`<resumed xmlns="urn:xmpp:sm:3" xmlns:foreign="urn:foreign" foreign:h="0" previd="resume-id"/>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="0" previd="resume-id" extra="value"/>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="0" previd="resume-id"><child/></resumed>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="0" previd="resume-id"> </resumed>`,
			`<resumed xmlns="urn:xmpp:sm:3" h="00" previd="resume-id"/>`,
		}
		for _, raw := range invalid {
			start, reader = smElementReader(t, raw)
			if _, err = decodeSMResumed(reader, start, "resume-id"); !errors.Is(err, ErrProtocol) {
				t.Fatalf("accepted %s: %v", raw, err)
			}
		}
	})
}

func TestXEP0198EnabledMaxRequiresSchemaPositiveInteger(t *testing.T) {
	valid := []string{
		"1",
		"+1",
		"0001",
		" +001 ",
		"99999999999999999999999999999999999999999999999999",
	}
	for _, maximum := range valid {
		raw := `<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="true" max="` + maximum + `"/>`
		start, reader := smElementReader(t, raw)
		if _, err := decodeSMEnabled(reader, start); err != nil {
			t.Fatalf("valid max %q rejected: %v", maximum, err)
		}
	}

	invalid := []string{
		"",
		"0",
		"+0",
		"000",
		"-1",
		"not-a-number",
		"+",
		"1.0",
		"1 0",
	}
	for _, maximum := range invalid {
		raw := `<enabled xmlns="urn:xmpp:sm:3" id="resume-id" resume="true" max="` + maximum + `"/>`
		start, reader := smElementReader(t, raw)
		if _, err := decodeSMEnabled(reader, start); !errors.Is(err, ErrProtocol) {
			t.Fatalf("invalid max %q accepted: %v", maximum, err)
		}
	}
}
