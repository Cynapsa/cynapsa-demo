package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"mellium.im/xmpp/jid"
)

type qaApplicationXMLFrame struct {
	XMLName      xml.Name   `xml:"urn:cynapsa:aztm:1 frame"`
	Version      string     `xml:"v,attr"`
	Data         string     `xml:",chardata"`
	UnknownAttrs []xml.Attr `xml:",any,attr"`
}

func TestApplicationFrameFixtureIsCanonical(t *testing.T) {
	record := rank2xmpp.Stanza{Kind: rank2xmpp.StanzaEnvelope, From: "agent-a@mesh.test/e2e-mesh", To: "agent-b@mesh.test/e2e-mesh", MeshID: "e2e-mesh", Data: []byte("canonical-opaque-envelope")}
	frame, err := rank2xmpp.EncodeStanzaFrame(record, transport.MaximumControlFrameBytes, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rank2xmpp.DecodeStanzaFrame(frame, record.From, record.To, record.MeshID, transport.MaximumControlFrameBytes, 512<<10); err != nil {
		t.Fatal(err)
	}
	t.Logf("frame=%s", frame)
	decoder := xml.NewDecoder(bytes.NewReader(frame))
	var decoded qaApplicationXMLFrame
	if err = decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	remarshaled, err := xml.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("remarshaled=%s attrs=%#v", remarshaled, decoded.UnknownAttrs)
	if _, err = rank2xmpp.DecodeStanzaFrame(remarshaled, record.From, record.To, record.MeshID, transport.MaximumControlFrameBytes, 512<<10); err == nil {
		t.Fatal("duplicate-namespace remarshal unexpectedly remained canonical")
	}
}

func TestQueryServerTimeRequiresProductionSeam(t *testing.T) {
	if _, err := queryServerTime(nil, context.Background()); err == nil {
		t.Fatal("session without the production time seam was accepted")
	}
}

func TestEnabledResumeIDRequiresResumableIdentity(t *testing.T) {
	valid := xml.StartElement{Attr: []xml.Attr{
		{Name: xml.Name{Local: "resume"}, Value: "true"},
		{Name: xml.Name{Local: "id"}, Value: "bounded-test-id"},
	}}
	if got, err := enabledResumeID(valid); err != nil || got != "bounded-test-id" {
		t.Fatalf("resume ID=%q err=%v", got, err)
	}
	for _, invalid := range []xml.StartElement{
		{Attr: []xml.Attr{{Name: xml.Name{Local: "resume"}, Value: "true"}}},
		{Attr: []xml.Attr{{Name: xml.Name{Local: "id"}, Value: "bounded-test-id"}}},
		{Attr: []xml.Attr{{Name: xml.Name{Local: "resume"}, Value: "false"}, {Name: xml.Name{Local: "id"}, Value: "bounded-test-id"}}},
	} {
		if _, err := enabledResumeID(invalid); err == nil {
			t.Fatal("non-resumable response was accepted")
		}
	}
}

func TestTimeResponseValidationRejectsUntrustedAndMalformedInput(t *testing.T) {
	target := jid.MustParse("mesh.test")
	bound := jid.MustParse("agent-a@mesh.test/server-assigned")
	valid := `<iq xmlns="jabber:client" from="mesh.test" to="agent-a@mesh.test/server-assigned" type="result" id="time-1"><time xmlns="urn:xmpp:time"><tzo>+00:00</tzo><utc>2026-08-14T01:22:38Z</utc></time></iq>`
	parse := func(raw, requestID string, authenticated bool) (timeResult, error) {
		return parseTimeResponse(xml.NewDecoder(strings.NewReader(raw)), requestID, target, bound, authenticated)
	}
	result, err := parse(valid, "time-1", true)
	if err != nil || result.TZO != "+00:00" || result.RawUTC != "2026-08-14T01:22:38Z" || result.UTC.IsZero() {
		t.Fatalf("valid response=%#v err=%v", result, err)
	}
	tests := []struct {
		name          string
		raw           string
		requestID     string
		authenticated bool
	}{
		{name: "unauthenticated", raw: valid, requestID: "time-1", authenticated: false},
		{name: "mismatched id", raw: valid, requestID: "different", authenticated: true},
		{name: "mismatched sender", raw: strings.Replace(valid, `from="mesh.test"`, `from="other.test"`, 1), requestID: "time-1", authenticated: true},
		{name: "mismatched recipient", raw: strings.Replace(valid, `to="agent-a@mesh.test/server-assigned"`, `to="agent-b@mesh.test/other"`, 1), requestID: "time-1", authenticated: true},
		{name: "non utc", raw: strings.Replace(valid, "2026-08-14T01:22:38Z", "2026-08-14T03:22:38+02:00", 1), requestID: "time-1", authenticated: true},
		{name: "invalid tzo", raw: strings.Replace(valid, "+00:00", "+14:01", 1), requestID: "time-1", authenticated: true},
		{name: "missing utc", raw: strings.Replace(valid, "<utc>2026-08-14T01:22:38Z</utc>", "", 1), requestID: "time-1", authenticated: true},
		{name: "duplicate utc", raw: strings.Replace(valid, "</time>", "<utc>2026-08-14T01:22:38Z</utc></time>", 1), requestID: "time-1", authenticated: true},
		{name: "unknown field", raw: strings.Replace(valid, "</time>", "<future>value</future></time>", 1), requestID: "time-1", authenticated: true},
		{name: "wrong payload", raw: strings.Replace(valid, `time xmlns="urn:xmpp:time"`, `future xmlns="urn:future"`, 1), requestID: "time-1", authenticated: true},
		{name: "truncated", raw: strings.TrimSuffix(valid, "</iq>"), requestID: "time-1", authenticated: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parse(test.raw, test.requestID, test.authenticated); err == nil {
				t.Fatal("untrusted response was accepted")
			}
		})
	}
}

func TestStrictTimeFormats(t *testing.T) {
	for _, value := range []string{"+00:00", "-12:30", "+14:00"} {
		if !validTZO(value) {
			t.Errorf("valid TZO rejected: %s", value)
		}
	}
	for _, value := range []string{"Z", "00:00", "+14:01", "+15:00", "+01:60"} {
		if validTZO(value) {
			t.Errorf("invalid TZO accepted: %s", value)
		}
	}
	if _, err := strictUTC("2026-08-14T01:22:38.125Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := strictUTC("2026-08-14T01:22:38+00:00"); err == nil {
		t.Fatal("numeric UTC offset accepted instead of strict Z form")
	}
	if got := utcFractionDigits("2026-08-14T01:22:38.125000Z"); got != 6 {
		t.Fatalf("fraction digits=%d, want 6", got)
	}
	if got := utcFractionDigits("2026-08-14T01:22:38Z"); got != 0 {
		t.Fatalf("whole-second fraction digits=%d, want 0", got)
	}
}
