package rank2xmpp

import (
	"encoding/xml"
	"strings"
	"testing"
)

func FuzzQAExactBindResponse(f *testing.F) {
	f.Add(`<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`, "qa-request")
	f.Add(`<iq xmlns="jabber:client" type="result" id="other"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`, "qa-request")
	f.Add(`<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid><jid>a@example.test/other</jid></bind></iq>`, "qa-request")
	f.Add(string([]byte{0xff, 0xfe, 0xfd}), "qa-request")
	f.Fuzz(func(t *testing.T, input, requestID string) {
		bound, err := readExactBindResponse(xml.NewDecoder(strings.NewReader(input)), requestID)
		if err != nil {
			return
		}
		if requestID == "" || bound.String() == "" {
			t.Fatalf("invalid bind response accepted: request=%q bound=%q", requestID, bound.String())
		}
	})
}
