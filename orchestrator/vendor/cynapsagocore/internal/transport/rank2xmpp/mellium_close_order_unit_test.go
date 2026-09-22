package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stream"
)

type closeOrderAddr string

func (address closeOrderAddr) Network() string { return "close-order" }
func (address closeOrderAddr) String() string  { return string(address) }

type closeOrderConn struct {
	mu        sync.Mutex
	output    bytes.Buffer
	closed    bool
	writeErr  error
	closeErr  error
	writeWait time.Duration
	order     []string
}

func (connection *closeOrderConn) Read([]byte) (int, error) { return 0, io.EOF }

func (connection *closeOrderConn) Write(value []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.order = append(connection.order, "write")
	if connection.closed {
		return 0, net.ErrClosed
	}
	if connection.writeErr != nil {
		return 0, connection.writeErr
	}
	if connection.writeWait > 0 {
		time.Sleep(connection.writeWait)
	}
	return connection.output.Write(value)
}

func (connection *closeOrderConn) Close() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.order = append(connection.order, "close")
	if connection.closed {
		return net.ErrClosed
	}
	connection.closed = true
	return connection.closeErr
}

func (*closeOrderConn) LocalAddr() net.Addr              { return closeOrderAddr("local") }
func (*closeOrderConn) RemoteAddr() net.Addr             { return closeOrderAddr("remote") }
func (*closeOrderConn) SetDeadline(time.Time) error      { return nil }
func (*closeOrderConn) SetReadDeadline(time.Time) error  { return nil }
func (*closeOrderConn) SetWriteDeadline(time.Time) error { return nil }

func newCloseOrderSession(t testing.TB, connection net.Conn) *xmpp.Session {
	t.Helper()
	location, err := jid.Parse("mesh.test")
	if err != nil {
		t.Fatal(err)
	}
	origin, err := jid.Parse("agent@mesh.test/mesh")
	if err != nil {
		t.Fatal(err)
	}
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	session, err := xmpp.NewSession(context.Background(), location, origin, connection, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func newCloseOrderMelliumSession(connection net.Conn, session *xmpp.Session) *melliumSession {
	owner := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, Endpoint{})
	owner.mu.Lock()
	owner.conn = connection
	owner.session = session
	owner.mu.Unlock()
	return owner
}

func TestMelliumCloseWritesClosingStreamBeforeClosingConnection(t *testing.T) {
	connection := &closeOrderConn{}
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	if err := owner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 2 || connection.order[0] != "write" || connection.order[1] != "close" {
		t.Fatalf("terminal order = %v", connection.order)
	}
	if got := connection.output.String(); got != "</stream:stream>" {
		t.Fatalf("closing stream = %q", got)
	}
}

func TestMelliumCloseNilOwnershipAndSessionConnectionFallback(t *testing.T) {
	if err := closeMelliumStream(nil, nil, time.Millisecond); err != nil {
		t.Fatalf("empty ownership close = %v", err)
	}
	rawOnly := &closeOrderConn{}
	if err := closeMelliumStream(nil, rawOnly, time.Millisecond); err != nil {
		t.Fatalf("raw-only close = %v", err)
	}
	rawOnly.mu.Lock()
	if len(rawOnly.order) != 1 || rawOnly.order[0] != "close" {
		t.Fatalf("raw-only order = %v", rawOnly.order)
	}
	rawOnly.mu.Unlock()

	connection := &closeOrderConn{}
	session := newCloseOrderSession(t, connection)
	if err := closeMelliumStream(session, nil, time.Millisecond); err != nil {
		t.Fatalf("session fallback close = %v", err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 2 || connection.order[0] != "write" || connection.order[1] != "close" {
		t.Fatalf("session fallback order = %v", connection.order)
	}
}

func TestMelliumClosePreservesClosingStreamWriteFailure(t *testing.T) {
	want := errors.New("closing stream write failed")
	connection := &closeOrderConn{writeErr: want}
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	if err := owner.Close(t.Context()); !errors.Is(err, want) {
		t.Fatalf("Close() = %v, want write failure", err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 2 || connection.order[0] != "write" || connection.order[1] != "close" || !connection.closed {
		t.Fatalf("failed terminal order = %v closed=%t", connection.order, connection.closed)
	}
}

func TestMelliumClosePreservesUnexpectedConnectionCloseFailure(t *testing.T) {
	want := errors.New("connection close failed")
	connection := &closeOrderConn{closeErr: want}
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	if err := owner.Close(t.Context()); !errors.Is(err, want) {
		t.Fatalf("Close() = %v, want connection failure", err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 2 || connection.order[0] != "write" || connection.order[1] != "close" || !connection.closed {
		t.Fatalf("failed terminal order = %v closed=%t", connection.order, connection.closed)
	}
}

func TestMelliumCloseTreatsAlreadyClosedRawConnectionAsTerminalSuccess(t *testing.T) {
	connection := &closeOrderConn{closed: true}
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	if err := owner.Close(t.Context()); err != nil {
		t.Fatalf("Close() = %v, want terminal success", err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 2 || connection.order[0] != "write" || connection.order[1] != "close" {
		t.Fatalf("already-closed terminal order = %v", connection.order)
	}
}

func TestMelliumCloseTreatsRecognizedSocketTerminalWritesAsSuccess(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "net_closed", err: net.ErrClosed},
		{name: "closed_pipe", err: io.ErrClosedPipe},
		{name: "eof", err: io.EOF},
		{name: "broken_pipe", err: syscall.EPIPE},
		{name: "connection_reset", err: syscall.ECONNRESET},
		{name: "wrapped_broken_pipe", err: &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &closeOrderConn{writeErr: test.err}
			owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
			if err := owner.Close(t.Context()); err != nil {
				t.Fatalf("Close() = %v, want terminal success", err)
			}
		})
	}
}

func TestMelliumCloseNormalizationRetainsUniqueJoinedFailure(t *testing.T) {
	want := errors.New("unique close failure")
	err := normalizeMelliumTeardownError(errors.Join(syscall.EPIPE, want))
	if !errors.Is(err, want) || errors.Is(err, syscall.EPIPE) {
		t.Fatalf("normalized joined error = %v", err)
	}
}

func TestMelliumCloseNearDeadlineCompletionRaceCannotHang(t *testing.T) {
	const grace = time.Millisecond
	for iteration := range 200 {
		connection := &closeOrderConn{writeWait: grace}
		session := newCloseOrderSession(t, connection)
		started := time.Now()
		if err := closeMelliumStream(session, connection, grace); err != nil {
			t.Fatalf("iteration %d: Close() = %v", iteration, err)
		}
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
			t.Fatalf("iteration %d: near-deadline close took %v", iteration, elapsed)
		}
	}
}

func TestMelliumCloseAfterServeTerminalizedStreamClosesConnectionOnce(t *testing.T) {
	connection := &closeOrderConn{}
	session := newCloseOrderSession(t, connection)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	connection.order = nil
	connection.output.Reset()
	connection.mu.Unlock()
	owner := newCloseOrderMelliumSession(connection, session)
	if err := owner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.order) != 1 || connection.order[0] != "close" || !connection.closed {
		t.Fatalf("already-terminal order = %v closed=%t", connection.order, connection.closed)
	}
}

type blockingCloseConn struct {
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
	mu           sync.Mutex
	closeCalls   int
}

func newBlockingCloseConn() *blockingCloseConn {
	return &blockingCloseConn{writeStarted: make(chan struct{}), closed: make(chan struct{})}
}

func (connection *blockingCloseConn) Read([]byte) (int, error) {
	<-connection.closed
	return 0, io.EOF
}

func (connection *blockingCloseConn) Write([]byte) (int, error) {
	connection.writeOnce.Do(func() { close(connection.writeStarted) })
	<-connection.closed
	return 0, net.ErrClosed
}

func (connection *blockingCloseConn) Close() error {
	connection.mu.Lock()
	connection.closeCalls++
	connection.mu.Unlock()
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}

func (*blockingCloseConn) LocalAddr() net.Addr              { return closeOrderAddr("local") }
func (*blockingCloseConn) RemoteAddr() net.Addr             { return closeOrderAddr("remote") }
func (*blockingCloseConn) SetDeadline(time.Time) error      { return nil }
func (*blockingCloseConn) SetReadDeadline(time.Time) error  { return nil }
func (*blockingCloseConn) SetWriteDeadline(time.Time) error { return nil }

func TestMelliumCloseBoundsBlockedClosingStreamAndRawCloses(t *testing.T) {
	connection := newBlockingCloseConn()
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	started := time.Now()
	if err := owner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*melliumGracefulCloseTimeout {
		t.Fatalf("blocked graceful close took %v", elapsed)
	}
	select {
	case <-connection.writeStarted:
	default:
		t.Fatal("graceful closing write was never attempted")
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closeCalls != 1 {
		t.Fatalf("raw close calls = %d", connection.closeCalls)
	}
}

func TestMelliumCloseBoundsInFlightWriterBeforeTerminalWrite(t *testing.T) {
	connection := newBlockingCloseConn()
	session := newCloseOrderSession(t, connection)
	owner := newCloseOrderMelliumSession(connection, session)
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- session.Send(context.Background(), xml.NewDecoder(strings.NewReader("<message/>")))
	}()
	<-connection.writeStarted
	started := time.Now()
	if err := owner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*melliumGracefulCloseTimeout {
		t.Fatalf("in-flight writer close took %v", elapsed)
	}
	if err := <-sendDone; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("in-flight send = %v", err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closeCalls != 1 {
		t.Fatalf("raw close calls = %d", connection.closeCalls)
	}
}

func TestMelliumConcurrentCloseCallerTimeoutOnlyBoundsJoin(t *testing.T) {
	connection := newBlockingCloseConn()
	owner := newCloseOrderMelliumSession(connection, newCloseOrderSession(t, connection))
	caller, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := owner.Close(caller); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() = %v, want caller deadline", err)
	}

	const waiters = 8
	results := make(chan error, waiters)
	for range waiters {
		go func() { results <- owner.Close(t.Context()) }()
	}
	for range waiters {
		if err := <-results; err != nil {
			t.Fatalf("joined Close() = %v", err)
		}
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closeCalls != 1 {
		t.Fatalf("raw close calls = %d", connection.closeCalls)
	}
}
