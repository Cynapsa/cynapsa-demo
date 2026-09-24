package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func TestClientIdlePingRequiresExactServerIQResult(t *testing.T) {
	server := jid.MustParse("connect.example.test")
	local := jid.MustParse("a@connect.example.test/r2.local.nonce")
	valid := `<iq xmlns="jabber:client" from="connect.example.test" to="a@connect.example.test/r2.local.nonce" type="result" id="ping-1"/>`
	correlated, handled, err := decodeServerPingResult(xml.NewDecoder(strings.NewReader(valid)), "ping-1", server, local, maximumPrivateIQBytes, false)
	if err != nil || !correlated || !handled {
		t.Fatalf("valid ping correlated=%v handled=%v err=%v", correlated, handled, err)
	}
	for _, document := range []string{
		strings.Replace(valid, `id="ping-1"`, `id="other"`, 1),
		strings.Replace(valid, `from="connect.example.test"`, `from="evil.example.test"`, 1),
		strings.Replace(valid, `/>`, `><ping xmlns="urn:xmpp:ping"/></iq>`, 1),
	} {
		if _, _, err := decodeServerPingResult(xml.NewDecoder(strings.NewReader(document)), "ping-1", server, local, maximumPrivateIQBytes, false); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted invalid ping reply %q: %v", document, err)
		}
	}
}

type idleProbeTestSession struct {
	*authorityDiscoveryGateSession
	probed chan struct{}
}

func (s *idleProbeTestSession) PingServer(context.Context) error {
	select {
	case s.probed <- struct{}{}:
	default:
	}
	return nil
}

func TestDynamicClientProbesIdleSessionWithoutPython(t *testing.T) {
	session := &idleProbeTestSession{
		authorityDiscoveryGateSession: &authorityDiscoveryGateSession{fakeSession: &fakeSession{events: make(chan Event, 4)}},
		probed:                        make(chan struct{}, 1),
	}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.DynamicPeerAuthority = true
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	client.mu.Lock()
	client.progress = client.clock.Now().Add(-clientIdlePingInterval - time.Second)
	client.mu.Unlock()
	select {
	case <-session.probed:
	case <-time.After(3 * time.Second):
		t.Fatal("idle client did not issue its own server probe")
	}
}

func TestIdlePingResponseDeadlineStartsAfterCorrelatedGate(t *testing.T) {
	session := &melliumSession{}
	if err := session.acquireCorrelated(t.Context()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- session.PingServerAfterQueue(t.Context(), 10*time.Millisecond) }()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("idle probe expired while queued: %v", err)
	default:
	}
	session.releaseCorrelated()
	select {
	case err := <-result:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("probe after gate release = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle probe did not acquire released correlated gate")
	}
}
