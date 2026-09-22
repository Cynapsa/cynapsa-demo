package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQAExactBindRequestWireEncoding(t *testing.T) {
	var output bytes.Buffer
	if err := writeExactBindRequest(xml.NewEncoder(&output), "qa-request", "mesh-one"); err != nil {
		t.Fatal(err)
	}
	const want = `<iq xmlns="jabber:client" type="set" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource xmlns="urn:ietf:params:xml:ns:xmpp-bind">mesh-one</resource></bind></iq>`
	if got := output.String(); got != want {
		t.Fatalf("bind request = %q, want %q", got, want)
	}

	for name, values := range map[string][2]string{
		"missing id":       {"", "mesh-one"},
		"missing resource": {"qa-request", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if err := writeExactBindRequest(xml.NewEncoder(&bytes.Buffer{}), values[0], values[1]); !errors.Is(err, ErrIdentityBinding) {
				t.Fatalf("error = %v, want identity-binding failure", err)
			}
		})
	}
}

func TestQAExactBindResponseStrictCorrelationAndShape(t *testing.T) {
	const valid = `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`
	bound, err := readExactBindResponse(xml.NewDecoder(strings.NewReader(valid)), "qa-request")
	if err != nil || bound.String() != "a@example.test/mesh-one" {
		t.Fatalf("valid bind response = %q, %v", bound.String(), err)
	}

	tests := map[string]string{
		"wrong outer namespace":   `<iq xmlns="jabber:server" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"wrong outer element":     `<message xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></message>`,
		"missing id":              `<iq xmlns="jabber:client" type="result"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"wrong id":                `<iq xmlns="jabber:client" type="result" id="other"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"duplicate id":            `<iq xmlns="jabber:client" type="result" id="qa-request" id="other"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"missing type":            `<iq xmlns="jabber:client" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"request type":            `<iq xmlns="jabber:client" type="set" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"missing bind":            `<iq xmlns="jabber:client" type="result" id="qa-request"></iq>`,
		"wrong bind namespace":    `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:example:wrong"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"missing jid":             `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"></bind></iq>`,
		"empty jid":               `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid></jid></bind></iq>`,
		"malformed jid":           `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>@example.test/mesh-one</jid></bind></iq>`,
		"unknown iq attribute":    `<iq xmlns="jabber:client" type="result" id="qa-request" injected="true"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind></iq>`,
		"unknown iq child":        `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><injected xmlns="urn:example:wrong"/></iq>`,
		"unknown bind child":      `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid><injected xmlns="urn:example:wrong"/></bind></iq>`,
		"duplicate bind":          `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/other</jid></bind></iq>`,
		"duplicate jid":           `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid><jid>a@example.test/other</jid></bind></iq>`,
		"trailing character data": `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind>injected</iq>`,
		"result with error":       `<iq xmlns="jabber:client" type="result" id="qa-request"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><jid>a@example.test/mesh-one</jid></bind><error type="cancel"><bad-request xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if got, err := readExactBindResponse(xml.NewDecoder(strings.NewReader(input)), "qa-request"); err == nil {
				t.Fatalf("response accepted as %q with error %v", got.String(), err)
			}
		})
	}
}

func TestQAExactBindErrorResponseDoesNotExposeDependencyText(t *testing.T) {
	const response = `<iq xmlns="jabber:client" type="error" id="qa-request"><error type="cancel"><not-allowed xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><text xmlns="urn:ietf:params:xml:ns:xmpp-stanzas">private server canary</text></error></iq>`
	_, err := readExactBindResponse(xml.NewDecoder(strings.NewReader(response)), "qa-request")
	if err == nil {
		t.Fatal("bind error response was accepted")
	}
	normalized := normalizeEstablishment(phaseError(establishmentIdentityBinding, err), context.Background(), establishmentIdentityBinding)
	if !errors.Is(normalized, ErrIdentityBinding) || strings.Contains(normalized.Error(), "private server canary") {
		t.Fatalf("normalized bind error = %q", normalized)
	}
}

func TestQAExactBindFailureStaysInIdentityPhase(t *testing.T) {
	feature := trackMelliumFeature(resourceBindingFeature("mesh-one", func() string { return "qa-request" }), establishmentIdentityBinding, newNegotiationPhases())
	wrong := xml.StartElement{Name: xml.Name{Space: "urn:example:wrong", Local: "bind"}}
	if _, _, err := feature.Parse(t.Context(), xml.NewDecoder(strings.NewReader("")), &wrong); normalizeEstablishment(err, t.Context(), establishmentStreamManagement) != ErrIdentityBinding {
		t.Fatalf("parse taxonomy = %v", err)
	}
	if _, _, err := feature.Negotiate(t.Context(), nil, nil); normalizeEstablishment(err, t.Context(), establishmentStreamManagement) != ErrIdentityBinding {
		t.Fatalf("negotiation taxonomy = %v", err)
	}
}

type qaBindDeadlineSession struct {
	mu     sync.Mutex
	phases []string
	closed bool
	secret []byte
}

func (s *qaBindDeadlineSession) ConnectTLS(context.Context, string) error {
	s.mu.Lock()
	s.phases = append(s.phases, "tls")
	s.mu.Unlock()
	return nil
}
func (s *qaBindDeadlineSession) Authenticate(_ context.Context, _ string, password []byte) (string, []byte, error) {
	s.mu.Lock()
	s.phases = append(s.phases, "auth")
	s.secret = password
	s.mu.Unlock()
	return "a@example.test", nil, nil
}
func (s *qaBindDeadlineSession) BindResource(ctx context.Context, _ string) (string, error) {
	s.mu.Lock()
	s.phases = append(s.phases, "bind")
	s.mu.Unlock()
	<-ctx.Done()
	return "", ctx.Err()
}
func (s *qaBindDeadlineSession) EnableStreamManagement(context.Context, bool) error {
	s.mu.Lock()
	s.phases = append(s.phases, "sm")
	s.mu.Unlock()
	return nil
}
func (*qaBindDeadlineSession) Send(context.Context, Stanza) error { return nil }
func (*qaBindDeadlineSession) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}
func (*qaBindDeadlineSession) Resume(context.Context) (bool, error)           { return false, ErrUnavailable }
func (*qaBindDeadlineSession) CatchUp(context.Context, int) ([]Stanza, error) { return nil, nil }
func (s *qaBindDeadlineSession) Close(context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func TestQABindDeadlineStopsBeforeStreamManagementAndCleansSecret(t *testing.T) {
	session := &qaBindDeadlineSession{}
	client := qaUnstartedClient(t, fakeDialer{session})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := client.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error = %v", err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if got := strings.Join(session.phases, ","); got != "tls,auth,bind" {
		t.Fatalf("phases after bind timeout = %q", got)
	}
	if !session.closed {
		t.Fatal("failed bind session was not closed")
	}
	for _, b := range session.secret {
		if b != 0 {
			t.Fatal("borrowed authentication secret was not cleared after bind timeout")
		}
	}
}
