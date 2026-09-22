package rank2xmpp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

type interruptibleMelliumOutputConn struct {
	writeEntered chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func newInterruptibleMelliumOutputConn() *interruptibleMelliumOutputConn {
	return &interruptibleMelliumOutputConn{
		writeEntered: make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (connection *interruptibleMelliumOutputConn) Read([]byte) (int, error) {
	<-connection.closed
	return 0, net.ErrClosed
}

func (connection *interruptibleMelliumOutputConn) Write([]byte) (int, error) {
	connection.writeOnce.Do(func() { close(connection.writeEntered) })
	<-connection.closed
	return 0, net.ErrClosed
}

func (connection *interruptibleMelliumOutputConn) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}

func (*interruptibleMelliumOutputConn) LocalAddr() net.Addr  { return closeOrderAddr("local") }
func (*interruptibleMelliumOutputConn) RemoteAddr() net.Addr { return closeOrderAddr("remote") }
func (*interruptibleMelliumOutputConn) SetDeadline(time.Time) error {
	return nil
}
func (*interruptibleMelliumOutputConn) SetReadDeadline(time.Time) error {
	return nil
}
func (*interruptibleMelliumOutputConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestMelliumServeTerminationFailStopsOnlyExactGeneration(t *testing.T) {
	endpoint, err := ParseEndpoint("127.0.0.1:5222")
	if err != nil {
		t.Fatal(err)
	}
	current := newInterruptibleMelliumOutputConn()
	currentSession := &xmpp.Session{}
	currentLifetime, cancelCurrent := context.WithCancel(context.Background())
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, endpoint)
	session.mu.Lock()
	session.conn = current
	session.session = currentSession
	session.ctx = currentLifetime
	session.cancel = cancelCurrent
	session.generation = 7
	session.mu.Unlock()

	session.retireServeGeneration(currentSession, 7)
	select {
	case <-current.closed:
	default:
		t.Fatal("current parser termination left its raw connection open")
	}
	if currentLifetime.Err() == nil {
		t.Fatal("current parser termination did not cancel its generation")
	}
	session.mu.Lock()
	suspended := session.suspended
	session.mu.Unlock()
	if !suspended {
		t.Fatal("current parser termination did not suspend the generation")
	}

	replacement := newInterruptibleMelliumOutputConn()
	replacementSession := &xmpp.Session{}
	replacementLifetime, cancelReplacement := context.WithCancel(context.Background())
	defer cancelReplacement()
	session.mu.Lock()
	session.conn = replacement
	session.session = replacementSession
	session.ctx = replacementLifetime
	session.cancel = cancelReplacement
	session.generation = 8
	session.suspended = false
	session.mu.Unlock()

	session.retireServeGeneration(currentSession, 7)
	select {
	case <-replacement.closed:
		t.Fatal("stale parser termination closed the replacement connection")
	default:
	}
	if replacementLifetime.Err() != nil {
		t.Fatal("stale parser termination canceled the replacement generation")
	}
	session.mu.Lock()
	suspended = session.suspended
	session.mu.Unlock()
	if suspended {
		t.Fatal("stale parser termination suspended the replacement generation")
	}
	_ = replacement.Close()
}

func TestMelliumEstablishmentHonorsContextAcrossHalfOpenTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	dialer, err := NewMelliumDialer(listener.Addr().String(), MelliumConfig{
		TLSConfig:       &tls.Config{MinVersion: tls.VersionTLS13},
		SASLMechanisms:  []sasl.Mechanism{sasl.ScramSha256},
		ReceiveCapacity: 1, StreamManagementCapacity: 1,
		StreamManagementByteCapacity: 1 << 20,
		MaximumFrameBytes:            1024, StanzaBudgetBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = session.ConnectTLS(context.Background(), listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	defer server.Close()
	if _, _, err = session.Authenticate(context.Background(), "agent@mesh.test", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if _, err = session.BindResource(context.Background(), "mesh"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	started := time.Now()
	err = session.EnableStreamManagement(ctx, true)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EnableStreamManagement()=%v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("half-open establishment returned after %s", elapsed)
	}
	closeContext, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err = session.Close(closeContext); err != nil {
		t.Fatal(err)
	}
}

func TestMelliumAbortSetupTerminallyDestroysCandidateState(t *testing.T) {
	endpoint, err := ParseEndpoint("127.0.0.1:5222")
	if err != nil {
		t.Fatal(err)
	}
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, endpoint)
	management, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("private-resume-id", true); err != nil {
		t.Fatal(err)
	}
	ledgerData := []byte("retained-ledger-data")
	if err = management.RecordSent(Stanza{Kind: StanzaSignal, Data: ledgerData}); err != nil {
		t.Fatal(err)
	}
	management.mu.Lock()
	ledgerOwned := management.pending[0].stanza.Data
	management.mu.Unlock()
	rejectedData := []byte("rejected-private-data")
	replayData := []byte("resume-private-data")
	password := []byte("retained-password")
	candidate, peer := net.Pipe()
	owned, cancel := context.WithCancel(context.Background())
	session.mu.Lock()
	session.conn = candidate
	session.management = management
	session.password = password
	session.rejected = []Stanza{{Kind: StanzaEnvelope, Data: rejectedData}}
	session.resumeReplay = []Stanza{{Kind: StanzaEnvelope, Data: replayData}}
	session.resumeGeneration = 7
	session.generation = 9
	session.suspended = true
	session.ctx = owned
	session.cancel = cancel
	session.username = "private@example.test"
	session.meshID = "private-mesh"
	session.mu.Unlock()
	if err = session.AbortSetup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(context.Background()); err != nil {
		t.Fatalf("idempotent Close() after abort = %v", err)
	}
	if owned.Err() == nil {
		t.Fatal("candidate lifetime was not canceled")
	}
	if management.Pending() != 0 || management.PendingBytes() != 0 {
		t.Fatalf("management ledger retained %d entries/%d bytes", management.Pending(), management.PendingBytes())
	}
	for name, secret := range map[string][]byte{"ledger": ledgerOwned, "rejected": rejectedData, "replay": replayData, "password": password} {
		for _, value := range secret {
			if value != 0 {
				t.Fatalf("%s secret survived abort", name)
			}
		}
	}
	session.mu.Lock()
	retained := session.conn != nil || session.session != nil || session.management != nil || session.cancel != nil || session.ctx != nil || session.generation != 0 || session.resumeGeneration != 0 || session.suspended || session.rejected != nil || session.resumeReplay != nil || session.password != nil || session.username != "" || session.meshID != ""
	session.mu.Unlock()
	if retained {
		t.Fatal("AbortSetup retained terminal candidate state")
	}
	buffer := make([]byte, 1)
	if _, err = peer.Read(buffer); !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
		t.Fatalf("peer read after raw abort = %v, want closed", err)
	}
	_ = peer.Close()
}

func TestMelliumSetupErrorPrefersExactContextTaxonomy(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := networkContextError(cancelled, ErrStreamManagement); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel classification = %v", err)
	}
	expired, release := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer release()
	if err := networkContextError(expired, ErrStreamManagement); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline classification = %v", err)
	}
}

func TestMelliumPostBindStreamManagementWaitIsCancelledByRawClose(t *testing.T) {
	for _, test := range []struct {
		name              string
		ctx               func() (context.Context, context.CancelFunc)
		cancelAfterEnable bool
		want              error
	}{
		{
			name: "deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 500*time.Millisecond)
			},
			want: context.DeadlineExceeded,
		},
		{
			name:              "cancel",
			cancelAfterEnable: true,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			want: context.Canceled,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			certificateServer := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			certificateServer.StartTLS()
			certificate := certificateServer.TLS.Certificates[0]
			roots := x509.NewCertPool()
			roots.AddCert(certificateServer.Certificate())
			certificateServer.Close()

			clientConn, serverConn := net.Pipe()
			serverDone := make(chan error, 1)
			enableObserved := make(chan struct{})
			go func() {
				serverTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}
				serverSession, err := xmpp.ReceiveClientSession(context.Background(), jid.MustParse("example.com"), serverConn,
					xmpp.StartTLS(serverTLS),
					xmpp.SASLServer(func(*sasl.Negotiator) bool { return true }, sasl.Plain),
					xmpp.BindCustom(func(jid.JID, string) (jid.JID, error) {
						return jid.Parse("a@example.com/mesh")
					}),
				)
				if err != nil {
					serverDone <- err
					return
				}
				reader := serverSession.TokenReader()
				defer reader.Close()
				decoder := xml.NewTokenDecoder(reader)
				token, err := decoder.Token()
				if err != nil {
					serverDone <- err
					return
				}
				start, ok := token.(xml.StartElement)
				if !ok || start.Name != (xml.Name{Space: streamManagementNamespace, Local: "enable"}) {
					serverDone <- ErrProtocol
					return
				}
				if err = decoder.Skip(); err != nil {
					serverDone <- err
					return
				}
				close(enableObserved)
				_, err = decoder.Token()
				serverDone <- err
			}()

			endpoint, err := ParseEndpoint("example.com:5222")
			if err != nil {
				t.Fatal(err)
			}
			session := newMelliumSession(MelliumConfig{
				TLSConfig:                    &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13},
				SASLMechanisms:               []sasl.Mechanism{sasl.Plain},
				ReceiveCapacity:              1,
				StreamManagementCapacity:     1,
				StreamManagementByteCapacity: 1 << 20,
				MaximumFrameBytes:            1024,
				StanzaBudgetBytes:            4096,
			}, endpoint)
			session.conn = clientConn
			session.username = "a@example.com"
			session.password = []byte("secret")
			session.meshID = "mesh"
			ctx, cancel := test.ctx()
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- session.EnableStreamManagement(ctx, true) }()
			select {
			case <-enableObserved:
			case err = <-serverDone:
				t.Fatalf("server failed before SM wait: %v", err)
			case <-time.After(time.Second):
				t.Fatal("server did not observe post-bind SM enable")
			}
			if test.cancelAfterEnable {
				cancel()
			}
			if err = <-result; !errors.Is(err, test.want) {
				t.Fatalf("EnableStreamManagement() = %v, want %v", err, test.want)
			}
			select {
			case err = <-serverDone:
				var syntaxError *xml.SyntaxError
				closedXMLStream := errors.As(err, &syntaxError) && syntaxError.Msg == "unexpected EOF"
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !closedXMLStream {
					t.Fatalf("withheld SM peer raw close = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("SM cancellation watcher returned without closing peer socket")
			}
			session.mu.Lock()
			published := session.session != nil || session.management != nil || session.generation != 0
			session.mu.Unlock()
			if published {
				t.Fatal("timed-out SM negotiation published serve state")
			}
			_ = session.AbortSetup(context.Background())
		})
	}
}

func TestMelliumSendCancellationInterruptsTrackedIQOutputMutexWaiter(t *testing.T) {
	management, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	connection := newInterruptibleMelliumOutputConn()
	xsession := newCloseOrderSession(t, connection)
	lifetime, retire := context.WithCancel(context.Background())
	defer retire()
	owner := newMelliumSession(MelliumConfig{
		ReceiveCapacity:              1,
		StreamManagementCapacity:     8,
		StreamManagementByteCapacity: 1 << 20,
		MaximumFrameBytes:            1 << 20,
		StanzaBudgetBytes:            1 << 20,
	}, Endpoint{})
	owner.mu.Lock()
	owner.conn = connection
	owner.session = xsession
	owner.management = management
	owner.ctx = lifetime
	owner.cancel = retire
	owner.username = "a@example.test"
	owner.meshID = "mesh"
	owner.mu.Unlock()

	origin := jid.MustParse("a@example.test/mesh")
	server := jid.MustParse("example.test")
	terminal := make(chan struct{})
	payload := &qaTerminalSignalReader{
		source:   xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}),
		terminal: terminal,
	}
	iq := stanza.IQ{
		XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"},
		ID:      "blocked-calibration",
		To:      server,
		Type:    stanza.GetIQ,
	}
	type trackedResult struct {
		sequence uint32
		err      error
	}
	trackedDone := make(chan trackedResult, 1)
	go func() {
		response, sequence, queryErr := owner.sendTrackedIQElement(
			context.Background(), xsession, management,
			Stanza{Kind: StanzaTimeCalibration, From: origin.String(), To: server.String(), MeshID: "mesh", MessageID: iq.ID},
			payload, iq,
		)
		if response != nil {
			_ = response.Close()
		}
		trackedDone <- trackedResult{sequence: sequence, err: queryErr}
	}()
	select {
	case <-terminal:
	case <-time.After(time.Second):
		connection.Close()
		t.Fatal("Mellium did not consume the tracked-IQ payload")
	}
	select {
	case <-connection.writeEntered:
	case <-time.After(time.Second):
		connection.Close()
		t.Fatal("tracked IQ did not block in raw Mellium output")
	}

	blockedSignal := sampleJingle()
	blockedSignal.Initiator = origin.String()
	blockedSignal.Responder = "b@example.test/mesh"
	blockedSignal.Content.Description.MeshID = "mesh"
	encodedJingle, err := EncodeJingle(blockedSignal)
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	sendCtx, cancelSend := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sendSession(owner, sendCtx, Stanza{
			Kind: StanzaSignal, From: origin.String(), To: "b@example.test/mesh", MeshID: "mesh",
			AttemptID: blockedSignal.SID, Data: encodedJingle,
		})
	}()
	deadline := time.Now().Add(time.Second)
	for management.Pending() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pending := management.Pending(); pending != 2 {
		cancelSend()
		connection.Close()
		t.Fatalf("second send did not pass the core write gate; pending=%d", pending)
	}
	started := time.Now()
	cancelSend()
	select {
	case sendErr := <-sendDone:
		if !errors.Is(sendErr, context.Canceled) {
			t.Fatalf("blocked Mellium send returned %v, want cancellation", sendErr)
		}
	case <-time.After(500 * time.Millisecond):
		connection.Close()
		t.Fatal("canceled Mellium output-mutex waiter remained blocked")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("hard interrupt returned after %s", elapsed)
	}
	result := <-trackedDone
	if result.err == nil || result.sequence == 0 {
		t.Fatalf("hard-interrupted tracked IQ sequence=%d err=%v", result.sequence, result.err)
	}
	owner.mu.Lock()
	retained := owner.suspended && owner.conn == connection && owner.session == xsession && owner.management == management
	owner.mu.Unlock()
	if !retained {
		t.Fatal("hard interrupt cleared or replaced resumable Mellium state")
	}
	if pending := management.Pending(); pending != 2 {
		t.Fatalf("hard interrupt discarded stream-management records; pending=%d", pending)
	}
}
