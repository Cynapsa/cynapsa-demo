package rank2xmpp

import (
	"bytes"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func TestDecodeEntityTimeResponseStrictSuccess(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	document := `<iq xmlns='jabber:client' xml:lang='en' type='result' id='time-1' from='mesh.example.test' to='agent@mesh.example.test/mesh-1'> <time xmlns='urn:xmpp:time'> <tzo>+00:00</tzo> <utc>2026-08-13T12:34:56.123456Z</utc> </time> </iq>`
	value, err := decodeEntityTimeResponse(xml.NewDecoder(strings.NewReader(document)), "time-1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 13, 12, 34, 56, 123456000, time.UTC)
	if !value.Equal(want) || value.Location() != time.UTC {
		t.Fatalf("decoded = %s, want %s UTC", value, want)
	}
}

func TestDecodeEntityTimeResponseRejectsMalformedOrUncorrelatedXML(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	valid := `<iq xmlns='jabber:client' type='result' id='time-1' from='mesh.example.test' to='agent@mesh.example.test/mesh-1'><time xmlns='urn:xmpp:time'><tzo>Z</tzo><utc>2026-08-13T12:34:56.123456Z</utc></time></iq>`
	tests := map[string]string{
		"wrong id":           strings.Replace(valid, `id='time-1'`, `id='other'`, 1),
		"wrong type":         strings.Replace(valid, `type='result'`, `type='error'`, 1),
		"wrong sender":       strings.Replace(valid, `from='mesh.example.test'`, `from='attacker.example'`, 1),
		"missing recipient":  strings.Replace(valid, ` to='agent@mesh.example.test/mesh-1'`, ``, 1),
		"unknown attribute":  strings.Replace(valid, `<time xmlns=`, `<time extra='x' xmlns=`, 1),
		"wrong namespace":    strings.Replace(valid, `urn:xmpp:time`, `urn:attacker:time`, 1),
		"elements reordered": strings.Replace(valid, `<tzo>Z</tzo><utc>2026-08-13T12:34:56.123456Z</utc>`, `<utc>2026-08-13T12:34:56.123456Z</utc><tzo>Z</tzo>`, 1),
		"non UTC timestamp":  strings.Replace(valid, `12:34:56.123456Z`, `12:34:56.123456+01:00`, 1),
		"coarse UTC":         strings.Replace(valid, `12:34:56.123456Z`, `12:34:56Z`, 1),
		"nanosecond UTC":     strings.Replace(valid, `.123456Z`, `.123456789Z`, 1),
		"invalid tzo":        strings.Replace(valid, `<tzo>Z</tzo>`, `<tzo>+14:01</tzo>`, 1),
		"trailing element":   strings.Replace(valid, `</iq>`, `<extra/></iq>`, 1),
		"nested timestamp":   strings.Replace(valid, `2026-08-13T12:34:56.123456Z`, `<b/>`, 1),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeEntityTimeResponse(xml.NewDecoder(strings.NewReader(document)), "time-1", from, to); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestDecodeEntityTimeResponseRejectsOversizedInput(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	document := `<iq xmlns='jabber:client' type='result' id='time-1' from='mesh.example.test' to='agent@mesh.example.test/mesh-1'><time xmlns='urn:xmpp:time'><tzo>Z</tzo><utc>` + strings.Repeat("1", maximumEntityTimeBytes) + `</utc></time></iq>`
	if _, err := decodeEntityTimeResponse(xml.NewDecoder(bytes.NewBufferString(document)), "time-1", from, to); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversized error = %v", err)
	}
}
