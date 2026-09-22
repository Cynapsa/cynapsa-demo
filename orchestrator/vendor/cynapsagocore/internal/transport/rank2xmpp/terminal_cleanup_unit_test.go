package rank2xmpp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type retainedCloseIngressSession struct {
	*ingressSession
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

type controlledIngressSession struct {
	*ingressSession
	allow      chan struct{}
	returned   chan struct{}
	returnOnce sync.Once
	client     *Client
	secret     []byte
	withError  bool
}

func (session *controlledIngressSession) Receive(ctx context.Context) (Event, error) {
	select {
	case <-session.allow:
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
	if session.client != nil {
		go func() { _ = session.client.Close(context.Background()) }()
		for !session.client.isClosed() {
			time.Sleep(time.Millisecond)
		}
	}
	session.returnOnce.Do(func() { close(session.returned) })
	event := Event{Kind: EventStanza, Stanza: Stanza{Data: session.secret}}
	if session.withError {
		return event, errors.New("dependency returned value and error")
	}
	return event, nil
}

func (session *retainedCloseIngressSession) Close(context.Context) error {
	session.once.Do(func() { close(session.entered) })
	<-session.release
	return nil
}

func TestClientCloseIsComponentOwnedAndDrainsInboundBytes(t *testing.T) {
	session := &retainedCloseIngressSession{ingressSession: newIngressSession(), entered: make(chan struct{}), release: make(chan struct{})}
	client := newIngressClient(t, 2, session)
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	inboundLease, err := client.inboundBudget.Acquire(t.Context(), 6)
	if err != nil {
		t.Fatal(err)
	}
	signalLease, err := client.inboundBudget.Acquire(t.Context(), 6)
	if err != nil {
		t.Fatal(err)
	}
	inboundSecret := []byte("secret")
	signalSecret := []byte("signal")
	client.inbound <- AuthenticatedInbound{Envelope: ingressEnvelope(t, 1, string(inboundSecret)), inboundLease: inboundLease}
	client.signals <- Stanza{Data: signalSecret, inboundLease: signalLease}
	firstCtx, firstCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer firstCancel()
	if err = client.Close(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first retained close = %v", err)
	}
	<-session.entered
	secondCtx, secondCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer secondCancel()
	if err = client.Close(secondCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated retained close = %v", err)
	}
	close(session.release)
	if err = client.Close(t.Context()); err != nil {
		t.Fatalf("joined close = %v", err)
	}
	if stats := client.inboundBudget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("terminal inbound stats = %+v", stats)
	}
	if len(client.inbound) != 0 || len(client.signals) != 0 || client.config.Auth.Password != nil {
		t.Fatalf("terminal retained state inbound=%d signals=%d password=%x", len(client.inbound), len(client.signals), client.config.Auth.Password)
	}
	for _, value := range signalSecret {
		if value != 0 {
			t.Fatal("queued signal bytes were not zeroized")
		}
	}
}

func TestUnstartedClientCloseMakesReceiveSignalTerminal(t *testing.T) {
	client := newIngressClient(t, 1)
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReceiveSignal(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReceiveSignal after unstarted close = %v", err)
	}
}

func TestMelliumCloseJoinsSameProducerAndDrainsEvents(t *testing.T) {
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, Endpoint{})
	lease, err := session.inboundBudget.Acquire(t.Context(), 6)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("secret")
	session.events <- Event{Kind: EventStanza, Stanza: Stanza{Data: secret, inboundLease: lease}}
	session.serveWG.Add(1)
	release := make(chan struct{})
	go func() { <-release; session.serveWG.Done() }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err = session.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retained Mellium close = %v", err)
	}
	repeatCtx, repeatCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer repeatCancel()
	if err = session.Close(repeatCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated Mellium close = %v", err)
	}
	close(release)
	if err = session.Close(t.Context()); err != nil {
		t.Fatalf("joined Mellium close = %v", err)
	}
	if stats := session.inboundBudget.Stats(); stats.Count != 0 || stats.Bytes != 0 || len(session.events) != 0 {
		t.Fatalf("Mellium terminal state stats=%+v events=%d", stats, len(session.events))
	}
	for _, value := range secret {
		if value != 0 {
			t.Fatal("queued Mellium bytes were not zeroized")
		}
	}
	if _, err = session.Receive(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Mellium receive after close = %v", err)
	}
}

func TestIngressClearsDependencyValueReturnedWithError(t *testing.T) {
	secret := []byte("value-with-error")
	session := &controlledIngressSession{ingressSession: newIngressSession(), allow: make(chan struct{}), returned: make(chan struct{}), secret: secret, withError: true}
	client := newIngressClient(t, 1, session)
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(session.allow)
	<-session.returned
	deadline := time.Now().Add(time.Second)
	for secret[0] != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for _, value := range secret {
		if value != 0 {
			t.Fatal("event returned alongside dependency error was not cleared")
		}
	}
	_ = client.Close(t.Context())
}

func TestIngressClearsEventWhenCloseWinsAfterReceive(t *testing.T) {
	secret := []byte("stale-after-receive")
	session := &controlledIngressSession{ingressSession: newIngressSession(), allow: make(chan struct{}), returned: make(chan struct{}), secret: secret}
	client := newIngressClient(t, 1, session)
	session.client = client
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(session.allow)
	<-session.returned
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, value := range secret {
		if value != 0 {
			t.Fatal("event dequeued after close linearization was not cleared")
		}
	}
}
