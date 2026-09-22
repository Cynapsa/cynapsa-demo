package rank2xmpp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
)

type failingPhaseSession struct {
	failAt string
	err    error
}

func (s *failingPhaseSession) failure(phase string) error {
	if s.failAt == phase {
		return s.err
	}
	return nil
}

func (s *failingPhaseSession) ConnectTLS(context.Context, string) error {
	return s.failure("connect")
}
func (s *failingPhaseSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	if err := s.failure("authenticate"); err != nil {
		return "", []byte("failed-proof"), err
	}
	return "a@example.test", []byte("proof"), nil
}
func (s *failingPhaseSession) BindResource(context.Context, string) (string, error) {
	if err := s.failure("bind"); err != nil {
		return "", err
	}
	return "a@example.test/mesh", nil
}
func (s *failingPhaseSession) EnableStreamManagement(context.Context, bool) error {
	return s.failure("stream-management")
}
func (s *failingPhaseSession) QueryServerTime(context.Context) (time.Time, error) {
	if err := s.failure("time-calibration"); err != nil {
		return time.Time{}, err
	}
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), nil
}
func (*failingPhaseSession) Send(context.Context, Stanza) error { return nil }
func (*failingPhaseSession) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}
func (*failingPhaseSession) Resume(context.Context) (bool, error) { return false, nil }
func (*failingPhaseSession) CatchUp(context.Context, int) ([]Stanza, error) {
	return nil, nil
}
func (*failingPhaseSession) Close(context.Context) error { return nil }

func TestClientMapsExactFakeEstablishmentPhase(t *testing.T) {
	dependency := errors.New("same opaque dependency failure")
	tests := []struct {
		name    string
		failAt  string
		failure error
		want    error
	}{
		{name: "network connect", failAt: "connect", failure: dependency, want: ErrUnavailable},
		{name: "authentication", failAt: "authenticate", failure: dependency, want: ErrAuthentication},
		{name: "identity binding", failAt: "bind", failure: dependency, want: ErrIdentityBinding},
		{name: "required reliability", failAt: "stream-management", failure: dependency, want: ErrStreamManagement},
		{name: "mellium tls inside final call", failAt: "stream-management", failure: phaseError(establishmentNetworkTLS, dependency), want: ErrUnavailable},
		{name: "mellium sasl inside final call", failAt: "stream-management", failure: phaseError(establishmentAuthentication, dependency), want: ErrAuthentication},
		{name: "mellium bind inside final call", failAt: "stream-management", failure: phaseError(establishmentIdentityBinding, dependency), want: ErrIdentityBinding},
		{name: "mellium reliability inside final call", failAt: "stream-management", failure: phaseError(establishmentStreamManagement, dependency), want: ErrStreamManagement},
		{name: "authenticated time calibration", failAt: "time-calibration", failure: dependency, want: ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newIdentityClient(t, &identityDialer{sessions: []Session{&failingPhaseSession{failAt: test.failAt, err: test.failure}}})
			if err := client.Start(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("start error = %v, want %v", err, test.want)
			}
			_ = client.Close(context.Background())
		})
	}
}

func TestMelliumTLSNegotiationFailureIsUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(connection)
		if readErr := readUntil(reader, "<stream:stream"); readErr != nil {
			serverDone <- readErr
			return
		}
		_, writeErr := io.WriteString(connection, "<stream:stream from='example.test' id='phase-test' version='1.0' xmlns='jabber:client' xmlns:stream='http://etherx.jabber.org/streams'><stream:features><starttls xmlns='urn:ietf:params:xml:ns:xmpp-tls'><required/></starttls></stream:features>")
		if writeErr != nil {
			serverDone <- writeErr
			return
		}
		if readErr := readUntil(reader, "<starttls"); readErr != nil {
			serverDone <- readErr
			return
		}
		_, writeErr = io.WriteString(connection, "<failure xmlns='urn:ietf:params:xml:ns:xmpp-tls'/>")
		serverDone <- writeErr
	}()

	endpoint := listener.Addr().String()
	dialer, err := NewMelliumDialer(endpoint, validMelliumConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint:                       endpoint,
		Auth:                           Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity:                4,
		TransferWorkers:                1,
		TransferQueue:                  4,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     4,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   4,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, dialer, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.Start(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("TLS negotiation error = %v, want unavailable", err)
	}
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
	_ = client.Close(context.Background())
}

func readUntil(reader *bufio.Reader, marker string) error {
	var observed strings.Builder
	for observed.Len() < 64<<10 {
		part, err := reader.ReadString('>')
		observed.WriteString(part)
		if strings.Contains(observed.String(), marker) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return errors.New("scripted server input exceeded limit")
}
