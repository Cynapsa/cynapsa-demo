package rank2xmpp

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const deniedMessageWire = `<message xmlns="jabber:client" xml:lang="en" to="native-server@mesh.test/simple-e2e" from="mesh.test" type="error" id="msg_H9ElaPKOw0-_yqwzCi_RGw"><error type="cancel"><policy-violation xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><text xml:lang="en" xmlns="urn:ietf:params:xml:ns:xmpp-stanzas">Cynapsa mesh routing policy rejected the stanza</text></error></message>`

func TestServerRoutingDenialDoesNotCloseXMPP(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	if output, err := handleJingleErrorTestXML(session, ctx, deniedMessageWire); err != nil || output != "" {
		t.Fatalf("routing denial output=%q err=%v", output, err)
	}
	if session.management.HandledInbound() != 1 || session.closed {
		t.Fatalf("denial changed stream: handled=%d closed=%v", session.management.HandledInbound(), session.closed)
	}
	select {
	case event := <-session.events:
		if event.Kind != EventRoutingFailure || event.MessageID != "msg_H9ElaPKOw0-_yqwzCi_RGw" || event.ErrorCondition != "policy-violation" {
			t.Fatalf("routing failure event = %#v", event)
		}
		event.inboundLease.Release()
	default:
		t.Fatal("correlated denial did not reach the control plane")
	}
	// XMPP does not require a message ID; the server can deny an uncorrelated
	// application/control message without making that a transport failure.
	withoutID := strings.Replace(deniedMessageWire, ` id="msg_H9ElaPKOw0-_yqwzCi_RGw"`, "", 1)
	if output, err := handleJingleErrorTestXML(session, ctx, withoutID); err != nil || output != "" {
		t.Fatalf("uncorrelated denial output=%q err=%v", output, err)
	}
	if session.management.HandledInbound() != 2 || session.closed {
		t.Fatalf("uncorrelated denial changed stream: handled=%d closed=%v", session.management.HandledInbound(), session.closed)
	}
	select {
	case event := <-session.events:
		t.Fatalf("uncorrelated denial became peer work: %#v", event)
	default:
	}
	// The same session must still answer stream-management control traffic.
	output, err := handleJingleErrorTestXML(session, ctx, `<r xmlns="urn:xmpp:sm:3"/>`)
	if err != nil || !strings.Contains(output, `h="2"`) {
		t.Fatalf("stream after denial output=%q err=%v", output, err)
	}
}

func TestRoutingFailureReachesOnlyCurrentClientControlHandler(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	client := stateOwnershipTestClient(session, nil, nil)
	current := newIngressGeneration(12, session, jingleErrorLocal, nil, context.Background())
	stale := newIngressGeneration(11, session, jingleErrorLocal, nil, context.Background())
	t.Cleanup(current.cancel)
	t.Cleanup(stale.cancel)
	client.ingress, client.sessionEpoch = current, current.id
	var seen []string
	if err := client.SetRoutingFailureHandler(func(messageID, condition string) {
		seen = append(seen, messageID+":"+condition)
	}); err != nil {
		t.Fatal(err)
	}
	event := Event{Kind: EventRoutingFailure, MessageID: "msg_H9ElaPKOw0-_yqwzCi_RGw", ErrorCondition: "policy-violation"}
	client.handleIngressEvent(stale, event)
	client.handleIngressEvent(current, Event{Kind: EventRoutingFailure, MessageID: "invalid", ErrorCondition: "policy-violation"})
	client.handleIngressEvent(current, event)
	if len(seen) != 1 || seen[0] != event.MessageID+":"+event.ErrorCondition {
		t.Fatalf("routing failures delivered = %v", seen)
	}
	if client.state != DurableLive {
		t.Fatal("routing failure changed connectivity")
	}
}

func TestServerRoutingDenialRequiresExactSanitizedServerStanza(t *testing.T) {
	for _, test := range []struct {
		name string
		wire string
	}{
		{"wrong-server", strings.Replace(deniedMessageWire, `from="mesh.test"`, `from="other.test"`, 1)},
		{"peer-forgery", strings.Replace(deniedMessageWire, `from="mesh.test"`, `from="agent@mesh.test/simple-e2e"`, 1)},
		{"wrong-local-session", strings.Replace(deniedMessageWire, `to="native-server@mesh.test/simple-e2e"`, `to="native-server@mesh.test/other"`, 1)},
		{"duplicate-address", strings.Replace(deniedMessageWire, `from="mesh.test"`, `from="mesh.test" from="mesh.test"`, 1)},
		{"extra-payload", strings.Replace(deniedMessageWire, `<error type="cancel">`, `<body>untrusted</body><error type="cancel">`, 1)},
		{"missing-condition", strings.Replace(deniedMessageWire, `<policy-violation xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/>`, ``, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			session, ctx := jingleErrorTestSession(t)
			if _, err := handleJingleErrorTestXML(session, ctx, test.wire); !errors.Is(err, ErrAuthentication) && !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid server denial accepted: %v", err)
			}
			if session.management.HandledInbound() != 0 {
				t.Fatal("invalid server denial advanced stream-management state")
			}
		})
	}
}
