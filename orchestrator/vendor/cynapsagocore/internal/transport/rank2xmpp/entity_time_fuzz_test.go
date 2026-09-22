package rank2xmpp

import (
	"bytes"
	"encoding/xml"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func FuzzQAEntityTimeCorrelationAndBounds(f *testing.F) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	for _, seed := range []string{
		qaEntityTimeDocument("time-fuzz", from.String(), to.String(), "Z", "2026-08-13T12:34:56.123456Z"),
		qaEntityTimeDocument("other", from.String(), to.String(), "+14:01", "2026-08-13T12:34:56Z"),
		`<iq xmlns='jabber:client' type='result' id='time-fuzz' from='mesh.example.test' to='agent@mesh.example.test/mesh-1'/>`,
		"",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, document []byte) {
		if len(document) > 4<<10 {
			t.Skip()
		}
		value, correlated, err := decodeCorrelatedEntityTimeResponse(xml.NewDecoder(bytes.NewReader(document)), "time-fuzz", from, to)
		if err == nil {
			if !correlated || value.IsZero() || value.Location() != time.UTC || value.Year() < 1 || value.Year() > 9999 || value.Nanosecond()%1000 != 0 {
				t.Fatalf("accepted invalid result: value=%s correlated=%t", value, correlated)
			}
		}
	})
}
