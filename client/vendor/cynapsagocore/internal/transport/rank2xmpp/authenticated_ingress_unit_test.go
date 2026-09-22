package rank2xmpp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	ingressLocal  = "local@example.test/mesh"
	ingressRemote = "remote@example.test/r2.00000000-0000-4000-8000-000000000001.AAAAAAAA"
	ingressMesh   = "mesh"
)

type ingressSession struct {
	events  chan Event
	mailbox []Stanza
	resume  bool
}

type ingressTrackedSession struct {
	*ingressSession
	receiveEntered chan struct{}
	receiveOnce    sync.Once
}

func newIngressTrackedSession() *ingressTrackedSession {
	return &ingressTrackedSession{ingressSession: newIngressSession(), receiveEntered: make(chan struct{})}
}

func (session *ingressTrackedSession) Receive(ctx context.Context) (Event, error) {
	session.receiveOnce.Do(func() { close(session.receiveEntered) })
	return session.ingressSession.Receive(ctx)
}

func newIngressSession() *ingressSession {
	return &ingressSession{events: make(chan Event, 8)}
}

func (*ingressSession) ConnectTLS(context.Context, string) error { return nil }
func (*ingressSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return "local@example.test", nil, nil
}
func (*ingressSession) BindResource(context.Context, string) (string, error) {
	return ingressLocal, nil
}
func (*ingressSession) EnableStreamManagement(context.Context, bool) error { return nil }
func (*ingressSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC), nil
}
func (*ingressSession) Send(context.Context, Stanza) error { return nil }
func (session *ingressSession) Receive(ctx context.Context) (Event, error) {
	select {
	case event := <-session.events:
		return event, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (session *ingressSession) Resume(context.Context) (bool, error) {
	return session.resume, nil
}
func (session *ingressSession) CatchUp(context.Context, int) ([]Stanza, error) {
	return cloneStanzas(session.mailbox), nil
}
func (*ingressSession) Close(context.Context) error { return nil }

type ingressDialer struct {
	mu       sync.Mutex
	sessions []Session
}

func (dialer *ingressDialer) Dial(context.Context) (Session, error) {
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.sessions) == 0 {
		return nil, ErrUnavailable
	}
	session := dialer.sessions[0]
	dialer.sessions = dialer.sessions[1:]
	return session, nil
}

func newIngressClient(t *testing.T, capacity int, sessions ...Session) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint: "localhost:5222",
		Auth: Authentication{
			Username: "local@example.test",
			Password: []byte("secret"),
			MeshID:   ingressMesh,
		},
		ReceiveCapacity:                capacity,
		TransferWorkers:                1,
		TransferQueue:                  1,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     1,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   8,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, &ingressDialer{sessions: sessions}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func ingressEnvelope(t testing.TB, _ uint64, body string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("native", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	conversationDigest := sha256.Sum256([]byte("authenticated-ingress"))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   "conv_" + base64.RawURLEncoding.EncodeToString(conversationDigest[:]),
		Sender:           ingressRemote,
		Recipient:        ingressLocal,
		MeshID:           ingressMesh,
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
		ClockUncertainty: time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func ingressStanza(t testing.TB, envelope protocol.Envelope) Stanza {
	t.Helper()
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return Stanza{Kind: StanzaEnvelope, From: ingressRemote, To: ingressLocal, MeshID: ingressMesh, Data: encoded}
}

func TestReceiveAuthenticatedPreservesStanzaProvenanceAndOwnership(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 2, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	firstEnvelope := ingressEnvelope(t, 1, "first")
	firstStanza := ingressStanza(t, firstEnvelope)
	session.events <- Event{Kind: EventStanza, Stanza: firstStanza}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := client.ReceiveAuthenticated(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.AuthenticatedSender != firstStanza.From || got.Envelope.Sender != firstEnvelope.Sender || got.Envelope.MessageID != firstEnvelope.MessageID {
		t.Fatalf("unexpected authenticated inbound: %#v", got)
	}

	decoded, ok := decodeAuthenticatedInbound(firstStanza)
	if !ok {
		t.Fatal("valid authenticated stanza was rejected")
	}
	owned := decoded.clone()
	owned.Envelope.Payload.Inline[0] ^= 0xff
	if string(decoded.Envelope.Payload.Inline) != "first" {
		t.Fatal("returned clone aliases queued envelope bytes")
	}

	secondEnvelope := ingressEnvelope(t, 2, "second")
	session.events <- Event{Kind: EventStanza, Stanza: ingressStanza(t, secondEnvelope)}
	compatibility, err := client.Receive(ctx)
	if err != nil || compatibility.MessageID != secondEnvelope.MessageID {
		t.Fatalf("compatibility receive=%#v error=%v", compatibility, err)
	}
}

func TestReceiveAuthenticatedRejectsEnvelopeClaimWithoutStanzaProvenance(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 1, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	stanza := ingressStanza(t, ingressEnvelope(t, 1, "forged"))
	stanza.From = "other@example.test/mesh"
	client.handleEvent(Event{Kind: EventStanza, Stanza: stanza})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := client.ReceiveAuthenticated(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mismatched sender was delivered: %v", err)
	}
}

func TestReceiveAuthenticatedLifecycleAndCapacity(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 1, session)
	if _, err := client.ReceiveAuthenticated(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-start receive=%v", err)
	}
	if cap(client.inbound) != 1 {
		t.Fatalf("inbound capacity=%d", cap(client.inbound))
	}
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.ReceiveAuthenticated(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled receive=%v", err)
	}

	waiting := make(chan error, 1)
	go func() {
		_, err := client.ReceiveAuthenticated(context.Background())
		waiting <- err
	}()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-waiting; !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked receive after close=%v", err)
	}
	client.inbound <- AuthenticatedInbound{AuthenticatedSender: ingressRemote, Envelope: ingressEnvelope(t, 1, "buffered")}
	if _, err := client.ReceiveAuthenticated(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close buffered receive=%v", err)
	}
}

func TestReceiveAuthenticatedCancelledCallDoesNotConsumeReadyRecord(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 1, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	for sequence := uint64(1); sequence <= 100; sequence++ {
		want := AuthenticatedInbound{AuthenticatedSender: ingressRemote, Envelope: ingressEnvelope(t, sequence, "ready")}
		client.inbound <- want
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := client.ReceiveAuthenticated(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled receive %d=%v", sequence, err)
		}
		if len(client.inbound) != 1 {
			t.Fatalf("cancelled receive consumed record %d", sequence)
		}
		got, err := client.ReceiveAuthenticated(context.Background())
		if err != nil || got.Envelope.MessageID != want.Envelope.MessageID {
			t.Fatalf("active receive %d=%#v, %v", sequence, got, err)
		}
	}
}

func TestReceiveAuthenticatedCancellationAtClientLockBarrierDoesNotConsume(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 1, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	want := AuthenticatedInbound{AuthenticatedSender: ingressRemote, Envelope: ingressEnvelope(t, 1, "ready")}
	client.inbound <- want
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		inbound AuthenticatedInbound
		err     error
	}
	result := make(chan outcome, 1)
	client.mu.Lock()
	go func() {
		inbound, err := client.ReceiveAuthenticated(ctx)
		result <- outcome{inbound: inbound, err: err}
	}()
	time.Sleep(time.Millisecond)
	cancel()
	client.mu.Unlock()

	got := <-result
	if !errors.Is(got.err, context.Canceled) || got.inbound.Envelope.MessageID != "" {
		t.Fatalf("receive across cancellation barrier=%#v, %v", got.inbound, got.err)
	}
	if len(client.inbound) != 1 {
		t.Fatal("cancelled receive consumed ready record")
	}
	if _, err := client.ReceiveAuthenticated(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartRejectsMailboxBeyondSharedReceiveCapacity(t *testing.T) {
	session := newIngressSession()
	first := ingressEnvelope(t, 1, "one")
	second := ingressEnvelope(t, 2, "two")
	session.mailbox = []Stanza{ingressStanza(t, first), ingressStanza(t, second)}
	client := newIngressClient(t, 1, session)
	if err := client.Start(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Start mailbox beyond shared capacity=%v", err)
	}
	if stats := client.inboundBudget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("rejected mailbox retained budget=%+v", stats)
	}
}

func TestStartRejectsMailboxProviderOverReturn(t *testing.T) {
	session := newIngressSession()
	for sequence := uint64(1); sequence <= 9; sequence++ {
		session.mailbox = append(session.mailbox, ingressStanza(t, ingressEnvelope(t, sequence, "over-limit")))
	}
	client := newIngressClient(t, 1, session)
	if err := client.Start(context.Background()); err == nil {
		t.Fatal("Start accepted mailbox provider over-return")
	}
	if _, err := client.ReceiveAuthenticated(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("receive after rejected start=%v", err)
	}
}

func TestCloseDrainsInitialMailboxBudget(t *testing.T) {
	session := newIngressSession()
	session.mailbox = []Stanza{ingressStanza(t, ingressEnvelope(t, 1, "one"))}
	client := newIngressClient(t, 1, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if stats := client.inboundBudget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("close retained mailbox budget=%+v", stats)
	}
}

func TestAuthenticatedAndCompatibilityReceiversShareOneConsumerQueue(t *testing.T) {
	session := newIngressSession()
	client := newIngressClient(t, 1, session)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		messageID string
		err       error
	}
	results := make(chan result, 2)
	go func() {
		inbound, err := client.ReceiveAuthenticated(ctx)
		results <- result{messageID: inbound.Envelope.MessageID, err: err}
	}()
	go func() {
		envelope, err := client.Receive(ctx)
		results <- result{messageID: envelope.MessageID, err: err}
	}()

	envelope := ingressEnvelope(t, 1, "single-consumer")
	session.events <- Event{Kind: EventStanza, Stanza: ingressStanza(t, envelope)}
	first := <-results
	if first.err != nil || first.messageID != envelope.MessageID {
		t.Fatalf("first receiver=%#v", first)
	}
	cancel()
	second := <-results
	if !errors.Is(second.err, context.Canceled) || second.messageID != "" {
		t.Fatalf("second receiver=%#v", second)
	}
}

func TestReceiveAuthenticatedWaitSurvivesCleanReconnectMailbox(t *testing.T) {
	first := newIngressSession()
	second := newIngressSession()
	mailboxEnvelope := ingressEnvelope(t, 1, "mailbox")
	second.mailbox = []Stanza{ingressStanza(t, mailboxEnvelope)}
	client := newIngressClient(t, 2, first, second)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	type receiveResult struct {
		inbound AuthenticatedInbound
		err     error
	}
	result := make(chan receiveResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		inbound, err := client.ReceiveAuthenticated(ctx)
		result <- receiveResult{inbound: inbound, err: err}
	}()
	if !client.reconnect() {
		t.Fatal("clean reconnect failed")
	}
	if err := acknowledgeCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil || got.inbound.AuthenticatedSender != ingressRemote || got.inbound.Envelope.MessageID != mailboxEnvelope.MessageID {
		t.Fatalf("reconnect receive=%#v error=%v", got.inbound, got.err)
	}
}

func TestCleanReconnectDiscardsLateReplacedSessionResult(t *testing.T) {
	old := newIngressTrackedSession()
	replacement := newIngressSession()
	client := newIngressClient(t, 2, old, replacement)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	select {
	case <-old.receiveEntered:
	case <-time.After(time.Second):
		t.Fatal("old session receive did not start")
	}
	if !client.reconnect() {
		t.Fatal("clean reconnect failed")
	}
	if err := acknowledgeCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	old.events <- Event{Kind: EventStanza, Stanza: ingressStanza(t, ingressEnvelope(t, 1, "stale"))}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got, err := client.ReceiveAuthenticated(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale result escaped: %#v, %v", got, err)
	}
}

func TestCleanReconnectRejectsMailboxBeyondSharedCapacityBeforeReplacementLiveRead(t *testing.T) {
	old := newIngressTrackedSession()
	replacement := newIngressTrackedSession()
	replacement.mailbox = []Stanza{
		ingressStanza(t, ingressEnvelope(t, 1, "one")),
		ingressStanza(t, ingressEnvelope(t, 2, "two")),
	}
	client := newIngressClient(t, 1, old, replacement)
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	select {
	case <-old.receiveEntered:
	case <-time.After(time.Second):
		t.Fatal("old session receive did not start")
	}
	if client.reconnect() {
		t.Fatal("reconnect accepted mailbox beyond shared receive capacity")
	}
	select {
	case <-replacement.receiveEntered:
		t.Fatal("replacement live Receive started after rejected mailbox")
	case <-time.After(20 * time.Millisecond):
	}
	if stats := client.inboundBudget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("rejected reconnect retained mailbox budget=%+v", stats)
	}
}

func FuzzDecodeAuthenticatedInboundPreservesProvenance(f *testing.F) {
	seed := ingressStanza(f, ingressEnvelope(f, 1, "seed"))
	f.Add(seed.From, seed.To, seed.MeshID, seed.Data)
	f.Add("other@example.test/mesh", seed.To, seed.MeshID, seed.Data)
	f.Add(seed.From, seed.To, seed.MeshID, []byte("not an envelope"))
	f.Fuzz(func(t *testing.T, from, to, meshID string, data []byte) {
		stanza := Stanza{Kind: StanzaEnvelope, From: from, To: to, MeshID: meshID, Data: append([]byte(nil), data...)}
		inbound, ok := decodeAuthenticatedInbound(stanza)
		if !ok {
			return
		}
		if inbound.AuthenticatedSender != from || inbound.Envelope.Sender != from || inbound.Envelope.Recipient != to || inbound.Envelope.MeshID != meshID {
			t.Fatalf("provenance mismatch: stanza=%#v inbound=%#v", stanza, inbound)
		}
		if protocol.ValidateEnvelope(inbound.Envelope) != nil {
			t.Fatal("decoder admitted an invalid envelope")
		}
	})
}
