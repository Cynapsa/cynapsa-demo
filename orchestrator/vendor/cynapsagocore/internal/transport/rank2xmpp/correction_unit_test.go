package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"mellium.im/xmlstream"
)

type tokenEncoder struct {
	decoder xml.TokenReader
	encoder *xml.Encoder
}

func (q *tokenEncoder) Token() (xml.Token, error)  { return q.decoder.Token() }
func (q *tokenEncoder) Encode(v interface{}) error { return q.encoder.Encode(v) }
func (q *tokenEncoder) EncodeElement(v interface{}, s xml.StartElement) error {
	return q.encoder.EncodeElement(v, s)
}
func (q *tokenEncoder) EncodeToken(v xml.Token) error { return q.encoder.EncodeToken(v) }

var _ xmlstream.TokenReadEncoder = (*tokenEncoder)(nil)

func TestXEP0198RequestDoesNotAdvanceHandled(t *testing.T) {
	sm, _ := NewStreamManagement(4, 1<<20)
	if err := sm.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	s := &melliumSession{meshID: "mesh", management: sm}
	var out bytes.Buffer
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "r"}}
	tokens := &tokenEncoder{decoder: &qaTokenReader{tokens: []xml.Token{start.End()}}, encoder: xml.NewEncoder(&out)}
	if err := s.handleElement(context.Background(), tokens, &start); err != nil {
		t.Fatal(err)
	}
	if _, h, _ := sm.ResumeState(); h != 0 {
		t.Fatalf("h=%d", h)
	}
}

func TestXEP0198AuthoritativeOrderIncludesAutomaticIQResult(t *testing.T) {
	sm, _ := NewStreamManagement(4, 1<<20)
	if err := sm.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	q := Stanza{Kind: StanzaSignalResult, From: "a@example.test/mesh", To: "b@example.test/mesh", MeshID: "mesh", AttemptID: "iq-1"}
	e := Stanza{Kind: StanzaEnvelope, From: q.From, To: q.To, MeshID: "mesh", Ordinal: 7, Data: []byte("envelope")}
	if err := sm.RecordSent(q); err != nil {
		t.Fatal(err)
	}
	if err := sm.RecordSent(e); err != nil {
		t.Fatal(err)
	}
	ordinal, count, err := sm.ApplyAckDetailed(1)
	if err != nil || ordinal != 0 || count != 1 || sm.Pending() != 1 {
		t.Fatalf("ack Q ordinal=%d count=%d pending=%d err=%v", ordinal, count, sm.Pending(), err)
	}
	ordinal, count, err = sm.ApplyAckDetailed(2)
	if err != nil || ordinal != 7 || count != 1 || sm.Pending() != 0 {
		t.Fatalf("ack E ordinal=%d count=%d pending=%d err=%v", ordinal, count, sm.Pending(), err)
	}
}

func TestIncomingSetIQResultAndEnvelopeShareExactAckOrder(t *testing.T) {
	sm, _ := NewStreamManagement(4, 1<<20)
	if err := sm.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	s := &melliumSession{username: "a@example.test", meshID: "mesh", management: sm, events: make(chan Event, 2)}
	jingle, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	start := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}, Attr: []xml.Attr{{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"}, {Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"}, {Name: xml.Name{Local: "type"}, Value: "set"}, {Name: xml.Name{Local: "id"}, Value: "iq-1"}}}
	var output bytes.Buffer
	tokens := &tokenEncoder{decoder: melliumIQChildDecoder(t, jingle, start), encoder: xml.NewEncoder(&output)}
	if err = s.handleElement(context.Background(), tokens, &start); err != nil {
		t.Fatal(err)
	}
	e := Stanza{Kind: StanzaEnvelope, Ordinal: 11, Data: []byte("envelope")}
	if err = sm.RecordSent(e); err != nil {
		t.Fatal(err)
	}
	ordinal, _, err := sm.ApplyAckDetailed(1)
	if err != nil || ordinal != 0 {
		t.Fatalf("Q ack ordinal=%d err=%v", ordinal, err)
	}
	ordinal, _, err = sm.ApplyAckDetailed(2)
	if err != nil || ordinal != 11 {
		t.Fatalf("E ack ordinal=%d err=%v", ordinal, err)
	}
}

func TestXEP0198AdmissionAndFailedWriteLeaveOneLedger(t *testing.T) {
	sm, _ := NewStreamManagement(1, 1<<20)
	if err := sm.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	q := Stanza{Kind: StanzaSignalResult, AttemptID: "iq"}
	e := Stanza{Kind: StanzaEnvelope, Ordinal: 9, Data: []byte("e")}
	if err := sm.RecordSent(q); err != nil {
		t.Fatal(err)
	}
	if err := sm.RecordSent(e); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("capacity=%v", err)
	}
	if got := sm.PendingSnapshot(); len(got) != 1 || got[0].Kind != StanzaSignalResult {
		t.Fatalf("ledger=%#v", got)
	}
	if _, _, err := sm.ApplyAckDetailed(1); err != nil {
		t.Fatal(err)
	}
	if err := sm.RecordSent(e); err != nil {
		t.Fatal(err)
	}
	if err := sm.RollbackLast(e); err != nil {
		t.Fatal(err)
	}
	if sm.Pending() != 0 {
		t.Fatalf("rollback pending=%d", sm.Pending())
	}
}

func TestManagedSendRollbackOnlyBeforeWire(t *testing.T) {
	tests := []struct {
		name        string
		stage       wireStage
		wantPending int
	}{
		{name: "definitive pre-wire", stage: wireNotStarted, wantPending: 0},
		{name: "ambiguous write", stage: wireAmbiguous, wantPending: 1},
		{name: "full stanza then r failure", stage: wireComplete, wantPending: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sm, _ := NewStreamManagement(2, 1<<20)
			if err := sm.Enable("resume", true); err != nil {
				t.Fatal(err)
			}
			record := Stanza{Kind: StanzaEnvelope, Ordinal: 3, Data: []byte("e")}
			if err := sm.RecordSent(record); err != nil {
				t.Fatal(err)
			}
			dependency := errors.New("dependency canary")
			err := finishManagedSend(sm, record, &wireError{stage: test.stage, cause: dependency})
			if !errors.Is(err, dependency) {
				t.Fatalf("error=%v", err)
			}
			if got := sm.Pending(); got != test.wantPending {
				t.Fatalf("pending=%d want=%d", got, test.wantPending)
			}
		})
	}
}

func TestStrictJingleAndMessageRejectTrailingContent(t *testing.T) {
	encoded, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeJingle(append(encoded, []byte(`<sdp>v=0</sdp>`)...)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("jingle=%v", err)
	}
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("e")}, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	input := append(frame, []byte(`<body xmlns="jabber:client">smuggled</body>`)...)
	if _, err = decodeMessageFrame(xml.NewDecoder(bytes.NewReader(input))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("message=%v", err)
	}
}

func TestStanzaBudgetIncludesCompleteRoutingWrapper(t *testing.T) {
	record := Stanza{Kind: StanzaEnvelope, From: "a@example.test/mesh", To: strings.Repeat("b", 200) + "@example.test/mesh", Data: []byte("e")}
	inner, err := EncodeStanzaFrame(record, 256, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = EncodeStanzaFrame(record, 256, len(inner)+1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("budget=%v", err)
	}
}

func TestInboundStanzaBudgetCountsOuterAttributesChildrenTextAndClose(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("e")}, 256, 1024)
	if err != nil {
		t.Fatal(err)
	}
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "message"}, Attr: []xml.Attr{{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"}, {Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"}, {Name: xml.Name{Local: "type"}, Value: "chat"}, {Name: xml.Name{Local: "attacker-padding"}, Value: "0123456789"}}}
	measure := func(maximum int) error {
		counter, makeErr := newStanzaBudget(xml.NewDecoder(bytes.NewReader(frame)), outer, maximum)
		if makeErr != nil {
			return makeErr
		}
		decoder := xml.NewTokenDecoder(counter)
		for {
			_, readErr := decoder.Token()
			if readErr != nil {
				return readErr
			}
		}
	}
	exact := xmlTokenSize(outer) + xmlTokenSize(outer.End())
	d := xml.NewDecoder(bytes.NewReader(frame))
	for {
		token, readErr := d.Token()
		if readErr != nil {
			break
		}
		exact += xmlTokenSize(token)
	}
	if err = measure(exact); !errors.Is(err, io.EOF) {
		t.Fatalf("exact=%v", err)
	}
	if err = measure(exact - 1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("over limit=%v", err)
	}
	jingle, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	iq := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}, Attr: append([]xml.Attr(nil), outer.Attr...)}
	iqExact := xmlTokenSize(iq) + xmlTokenSize(iq.End())
	jd := xml.NewDecoder(bytes.NewReader(jingle))
	for {
		token, readErr := jd.Token()
		if readErr != nil {
			break
		}
		iqExact += xmlTokenSize(token)
	}
	counter, err := newStanzaBudget(xml.NewDecoder(bytes.NewReader(jingle)), iq, iqExact-1)
	if err != nil {
		t.Fatal(err)
	}
	decoder := xml.NewTokenDecoder(counter)
	var signal Jingle
	if err = decoder.Decode(&signal); !errors.Is(err, ErrProtocol) {
		t.Fatalf("IQ over limit=%v", err)
	}
}

type lifecycleSession struct {
	done        chan struct{}
	once        sync.Once
	resumeCalls int
	mu          sync.Mutex
	sendErr     error
}

func newLifecycleSession() *lifecycleSession                         { return &lifecycleSession{done: make(chan struct{})} }
func (s *lifecycleSession) ConnectTLS(context.Context, string) error { return nil }
func (s *lifecycleSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return "a@example.test", nil, nil
}
func (s *lifecycleSession) BindResource(context.Context, string) (string, error) {
	return "a@example.test/mesh", nil
}
func (s *lifecycleSession) EnableStreamManagement(context.Context, bool) error { return nil }
func (s *lifecycleSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), nil
}
func (s *lifecycleSession) Send(context.Context, Stanza) error { return s.sendErr }
func (s *lifecycleSession) Receive(ctx context.Context) (Event, error) {
	select {
	case <-s.done:
		return Event{}, ErrClosed
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (s *lifecycleSession) Resume(context.Context) (bool, error) {
	s.mu.Lock()
	s.resumeCalls++
	s.mu.Unlock()
	return false, ErrUnavailable
}
func (s *lifecycleSession) CatchUp(context.Context, int) ([]Stanza, error) { return nil, nil }
func (s *lifecycleSession) Close(context.Context) error {
	s.once.Do(func() { close(s.done) })
	return nil
}

type blockingDialer struct {
	entered chan struct{}
	release chan struct{}
	session Session
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

type orderedDialer struct {
	mu       sync.Mutex
	sessions []Session
}

type blockingCloseSession struct {
	*fakeSession
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeOnce    sync.Once
}

func (session *blockingCloseSession) Close(ctx context.Context) error {
	session.closeOnce.Do(func() { close(session.closeEntered) })
	select {
	case <-session.closeRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *orderedDialer) Dial(context.Context) (Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.sessions) == 0 {
		return nil, ErrUnavailable
	}
	session := d.sessions[0]
	d.sessions = d.sessions[1:]
	return session, nil
}

func (d *blockingDialer) Dial(ctx context.Context) (Session, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.once.Do(func() { close(d.entered) })
	select {
	case <-d.release:
		return d.session, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func correctionClient(t *testing.T, d Dialer) *Client {
	t.Helper()
	box, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	c, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, d, box, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientStartSingleFlightAndCloseWins(t *testing.T) {
	t.Run("single flight", func(t *testing.T) {
		s := newLifecycleSession()
		d := &blockingDialer{entered: make(chan struct{}), release: make(chan struct{}), session: s}
		c := correctionClient(t, d)
		results := make(chan error, 2)
		go func() { results <- c.Start(context.Background()) }()
		<-d.entered
		go func() { results <- c.Start(context.Background()) }()
		time.Sleep(10 * time.Millisecond)
		d.mu.Lock()
		calls := d.calls
		d.mu.Unlock()
		if calls != 1 {
			t.Fatalf("calls=%d", calls)
		}
		close(d.release)
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		_ = c.Close(context.Background())
	})
	t.Run("close wins", func(t *testing.T) {
		s := newLifecycleSession()
		d := &blockingDialer{entered: make(chan struct{}), release: make(chan struct{}), session: s}
		c := correctionClient(t, d)
		result := make(chan error, 1)
		go func() { result <- c.Start(context.Background()) }()
		<-d.entered
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		close(d.release)
		if err := <-result; !errors.Is(err, ErrClosed) {
			t.Fatalf("start=%v", err)
		}
	})
}

func TestMelliumReceiveDiscardsStaleServeCompletion(t *testing.T) {
	s := &melliumSession{events: make(chan Event, 1), serveDone: make(chan serveResult, 2), generation: 2}
	s.serveDone <- serveResult{generation: 1, err: errors.New("stale old session")}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := s.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale result=%v", err)
	}
	s.serveDone <- serveResult{generation: 2, err: ErrUnavailable}
	if _, err := s.Receive(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("current result=%v", err)
	}
}

type failingSnapshotSession struct {
	*lifecycleSession
	snapshotCalls int
}

func (s *failingSnapshotSession) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	s.snapshotCalls++
	return nil, context.DeadlineExceeded
}

func TestReconnectSnapshotFailureKeepsOldSession(t *testing.T) {
	old := &failingSnapshotSession{lifecycleSession: newLifecycleSession()}
	box, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	c, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: 5 * time.Millisecond, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: 5 * time.Millisecond}, fakeDialer{old}, box, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if c.reconnect() {
		t.Fatal("reconnect succeeded without authoritative snapshot")
	}
	c.mu.Lock()
	current := c.session
	c.mu.Unlock()
	if current != old || old.snapshotCalls == 0 {
		t.Fatalf("old session replaced=%v calls=%d", current != old, old.snapshotCalls)
	}
}

func TestCleanReconnectReturnsLiveAfterMailboxProcessing(t *testing.T) {
	old := &fakeSession{events: make(chan Event), resume: false}
	replacement := &fakeSession{events: make(chan Event), mailbox: nil}
	dialer := &orderedDialer{sessions: []Session{old, replacement}}
	box, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	c, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, dialer, box, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if !c.reconnect() {
		t.Fatal("clean reconnect failed")
	}
	c.mu.Lock()
	state, active := c.state, c.session
	c.mu.Unlock()
	if state != DurableLive || active != replacement {
		t.Fatalf("state=%v replacement=%v", state, active == replacement)
	}
	c.mu.Lock()
	snapshotSession := c.authoritySnapshot.session
	c.mu.Unlock()
	if c.MembershipReady() || snapshotSession != replacement {
		t.Fatalf("replacement authority ready=%t snapshot-session=%t", c.MembershipReady(), snapshotSession == replacement)
	}
	if err = acknowledgeCurrentMembershipFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if !c.MembershipReady() {
		t.Fatal("replacement authority acknowledgement did not open membership")
	}
	envelope := durableEnvelope(t)
	if ownership := c.SendOwned(context.Background(), envelope); ownership != AcceptedOwned {
		t.Fatalf("post-reconnect ownership=%v", ownership)
	}
	replacement.mu.Lock()
	sent := append([]Stanza(nil), replacement.sent...)
	replacement.mu.Unlock()
	if len(sent) != 1 || sent[0].Kind != StanzaEnvelope || sent[0].MessageID != envelope.MessageID {
		t.Fatalf("post-reconnect sent=%#v", sent)
	}
	clearStanzas(sent)
}

func TestCleanReconnectRetiredSessionCloseDoesNotBlockReplacementSends(t *testing.T) {
	old := &blockingCloseSession{
		fakeSession:  &fakeSession{events: make(chan Event), resume: false},
		closeEntered: make(chan struct{}), closeRelease: make(chan struct{}),
	}
	replacement := &fakeSession{events: make(chan Event), mailbox: nil}
	dialer := &orderedDialer{sessions: []Session{old, replacement}}
	box, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	c, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, dialer, box, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())

	reconnected := make(chan bool, 1)
	go func() { reconnected <- c.reconnect() }()
	select {
	case <-old.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not begin bounded retirement of the old session")
	}
	if err = acknowledgeCurrentMembershipFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	sendCtx, cancelSend := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelSend()
	envelope := durableEnvelope(t)
	if ownership := c.SendOwned(sendCtx, envelope); ownership != AcceptedOwned {
		t.Fatalf("replacement send blocked by retired-session close: ownership=%v err=%v", ownership, sendCtx.Err())
	}
	replacement.mu.Lock()
	sent := append([]Stanza(nil), replacement.sent...)
	replacement.mu.Unlock()
	if len(sent) != 1 || sent[0].Kind != StanzaEnvelope || sent[0].MessageID != envelope.MessageID {
		t.Fatalf("replacement sent=%#v", sent)
	}
	clearStanzas(sent)

	close(old.closeRelease)
	select {
	case ok := <-reconnected:
		if !ok {
			t.Fatal("reconnect failed after retired-session close completed")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect did not finish after retired-session close completed")
	}
}

func TestSuccessfulResumeThawsFrozenMelliumSession(t *testing.T) {
	s := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, Endpoint{})
	s.suspended = true
	s.markResumedActive()
	s.mu.Lock()
	frozen := s.suspended
	s.mu.Unlock()
	if frozen {
		t.Fatal("successful resume left sends frozen")
	}
}

type blockingResumeSession struct {
	*lifecycleSession
	sawDeadline chan time.Time
}

func (s *blockingResumeSession) Resume(ctx context.Context) (bool, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false, errors.New("missing deadline")
	}
	select {
	case s.sawDeadline <- deadline:
	default:
	}
	<-ctx.Done()
	return false, ctx.Err()
}

func TestReconnectOperationsHaveConfiguredDeadline(t *testing.T) {
	base := newLifecycleSession()
	session := &blockingResumeSession{lifecycleSession: base, sawDeadline: make(chan time.Time, 1)}
	box, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	c, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: 10 * time.Millisecond, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: 10 * time.Millisecond}, fakeDialer{session}, box, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	started := time.Now()
	_ = c.reconnect()
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("reconnect pinned for %s", elapsed)
	}
	select {
	case deadline := <-session.sawDeadline:
		if deadline.Sub(started) > 50*time.Millisecond {
			t.Fatalf("deadline=%v", deadline)
		}
	default:
		t.Fatal("resume did not receive deadline")
	}
}

func TestOutboundFailureDemandsReconnect(t *testing.T) {
	s := newLifecycleSession()
	s.sendErr = errors.New("secret")
	c := correctionClient(t, fakeDialer{s})
	if err := startCurrentMembershipFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if err := c.Send(context.Background(), durableEnvelope(t)); err == nil {
		t.Fatal("send succeeded")
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		s.mu.Lock()
		calls := s.resumeCalls
		s.mu.Unlock()
		if calls > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no reconnect demand")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestXMPPChunkCanonicalPeerAndTerminalCleanup(t *testing.T) {
	s := &fakeSession{events: make(chan Event, 8)}
	c := correctionClient(t, fakeDialer{s})
	if err := startCurrentMembershipFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	carrier, err := NewPayloadChunks(c, PayloadChunksConfig{MaximumFrameBytes: 1024, MaximumChunkBytes: 256, StanzaBudgetBytes: 900, XMLOverheadBytes: 128, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh", MessageID: typed("msg_", 7)}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: typed("xfer_", 7), Encoding: payload.FrameText, Data: []byte("not canonical ***")}); err == nil {
		t.Fatal("noncanonical accepted")
	}
	id := typed("xfer_", 8)
	data := []byte(base64.RawStdEncoding.EncodeToString([]byte("manifest")))
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: id, Encoding: payload.FrameText, Data: data}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err = carrier.Finish(ctx, route, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finish=%v", err)
	}
	second := typed("xfer_", 9)
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: second, Encoding: payload.FrameText, Data: data}); err != nil {
		t.Fatalf("capacity=%v", err)
	}
	_ = carrier.Abort(context.Background(), route, second)
}
