package rank2xmpp

import (
	"bytes"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
)

func TestExactResourceBindingRequestCarriesRequestedResource(t *testing.T) {
	var output bytes.Buffer
	writer := xml.NewEncoder(&output)
	if err := writeExactBindRequest(writer, "request-1", "mesh-one"); err != nil {
		t.Fatal(err)
	}
	want := `<iq xmlns="jabber:client" type="set" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource xmlns="urn:ietf:params:xml:ns:xmpp-bind">mesh-one</resource></bind></iq>`
	if output.String() != want {
		t.Fatalf("bind request = %q, want %q", output.String(), want)
	}
	if strings.Contains(output.String(), `<resource xmlns="urn:ietf:params:xml:ns:xmpp-bind"></resource>`) {
		t.Fatal("bind request omitted the exact resource")
	}
}

func TestExactResourceBindingResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		xml  string
		want string
		err  error
	}{
		{
			name: "exact response",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
			want: "a@example.test/mesh-one",
		},
		{
			name: "wrong request",
			xml:  `<iq xmlns="jabber:client" type="result" id="other"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "missing jid",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "wrong type",
			xml:  `<iq xmlns="jabber:client" type="get" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "unknown iq attribute",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1" injected="true"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "unknown iq child",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><injected xmlns="urn:example:wrong"/></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "unknown bind child",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid><injected xmlns="urn:example:wrong"/></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "duplicate bind",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/other</jid></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "duplicate jid",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid><jid>a@example.test/other</jid></bind></iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "non-whitespace data",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind>injected</iq>`,
			err:  ErrIdentityBinding,
		},
		{
			name: "result and error",
			xml:  `<iq xmlns="jabber:client" type="result" id="request-1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><error type="cancel"><bad-request xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`,
			err:  ErrIdentityBinding,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := readExactBindResponse(xml.NewDecoder(strings.NewReader(test.xml)), "request-1")
			if !errors.Is(err, test.err) {
				t.Fatalf("error = %v, want %v", err, test.err)
			}
			if got.String() != test.want {
				t.Fatalf("jid = %q, want %q", got.String(), test.want)
			}
		})
	}
}

func TestExactResourceBindingFeatureIsClosedAndDeterministic(t *testing.T) {
	feature := resourceBindingFeature("mesh-one", func() string { return "request-1" })
	if feature.Name != (xml.Name{Space: resourceBindingNamespace, Local: "bind"}) {
		t.Fatalf("feature name = %v", feature.Name)
	}
	if feature.Parse == nil || feature.Negotiate == nil {
		t.Fatal("feature is incomplete")
	}
	input := `<bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"></bind>`
	decoder := xml.NewDecoder(strings.NewReader(input))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := token.(xml.StartElement)
	required, data, err := feature.Parse(t.Context(), decoder, &start)
	if err != nil || !required || data != nil {
		t.Fatalf("parse = required:%t data:%v err:%v", required, data, err)
	}

	wrong := xml.StartElement{Name: xml.Name{Space: "wrong", Local: "bind"}}
	if _, _, err = feature.Parse(t.Context(), xml.NewDecoder(strings.NewReader("")), &wrong); !errors.Is(err, ErrIdentityBinding) {
		t.Fatalf("wrong feature error = %v", err)
	}
	if _, _, err = feature.Negotiate(t.Context(), nil, nil); !errors.Is(err, ErrIdentityBinding) {
		t.Fatalf("nil session error = %v", err)
	}
}
