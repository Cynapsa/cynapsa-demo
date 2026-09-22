package rank2xmpp

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func qaEntityTimeDocument(id, from, to, tzo, utc string) string {
	return `<iq xmlns='jabber:client' type='result' id='` + id + `' from='` + from + `' to='` + to + `'><time xmlns='urn:xmpp:time'><tzo>` + tzo + `</tzo><utc>` + utc + `</utc></time></iq>`
}

func TestQAEntityTimeParserRejectsUntrustedOuterIQBeforePayloadCorrelation(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	valid := qaEntityTimeDocument("time-qa", from.String(), to.String(), "+00:00", "2026-08-13T12:34:56.123456Z")
	tests := map[string]struct {
		document   string
		correlated bool
	}{
		"wrong id":               {strings.Replace(valid, `id='time-qa'`, `id='other'`, 1), false},
		"wrong sender":           {strings.Replace(valid, `from='mesh.example.test'`, `from='attacker.example'`, 1), false},
		"wrong recipient":        {strings.Replace(valid, `to='agent@mesh.example.test/mesh-1'`, `to='agent@mesh.example.test/other'`, 1), false},
		"duplicate outer id":     {strings.Replace(valid, `id='time-qa'`, `id='time-qa' id='time-qa'`, 1), false},
		"outer error result":     {strings.Replace(valid, `type='result'`, `type='error'`, 1), false},
		"coarse authenticated":   {strings.Replace(valid, `.123456Z`, `Z`, 1), true},
		"fraction overflow":      {strings.Replace(valid, `.123456Z`, `.1234567Z`, 1), true},
		"utc offset timestamp":   {strings.Replace(valid, `.123456Z`, `.123456+00:00`, 1), true},
		"tzo beyond positive 14": {strings.Replace(valid, `+00:00`, `+14:01`, 1), true},
		"tzo beyond negative 14": {strings.Replace(valid, `+00:00`, `-14:01`, 1), true},
		"payload namespace swap": {strings.Replace(valid, `urn:xmpp:time`, `urn:attacker:time`, 1), true},
		"trailing payload":       {strings.Replace(valid, `</iq>`, `<injected xmlns='urn:attacker:time'/></iq>`, 1), true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, correlated, err := decodeCorrelatedEntityTimeResponse(xml.NewDecoder(strings.NewReader(test.document)), "time-qa", from, to)
			if !errors.Is(err, ErrProtocol) || correlated != test.correlated {
				t.Fatalf("correlated=%t error=%v, want correlated=%t protocol error", correlated, err, test.correlated)
			}
		})
	}
}

func TestQAEntityTimeParserAcceptsOnlyStrictUTCWithBoundedTZD(t *testing.T) {
	from := jid.MustParse("mesh.example.test")
	to := jid.MustParse("agent@mesh.example.test/mesh-1")
	for _, test := range []struct {
		tzo, utc string
		want     time.Time
	}{
		{tzo: "Z", utc: "0001-01-01T00:00:00.000000Z", want: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{tzo: "+14:00", utc: "2026-08-13T12:34:56.000001Z", want: time.Date(2026, 8, 13, 12, 34, 56, 1000, time.UTC)},
		{tzo: "-14:00", utc: "9999-12-31T23:59:59.999999Z", want: time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)},
	} {
		document := qaEntityTimeDocument("time-qa", from.String(), to.String(), test.tzo, test.utc)
		got, correlated, err := decodeCorrelatedEntityTimeResponse(xml.NewDecoder(strings.NewReader(document)), "time-qa", from, to)
		if err != nil || !correlated || !got.Equal(test.want) || got.Location() != time.UTC {
			t.Fatalf("tzo=%q utc=%q: value=%s correlated=%t error=%v", test.tzo, test.utc, got, correlated, err)
		}
	}
}
