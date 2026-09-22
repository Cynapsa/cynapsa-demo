package rank2xmpp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type ownershipSession struct {
	*fakeSession
	send func(context.Context, Stanza) error
}

func newOwnershipSession() *ownershipSession {
	return &ownershipSession{fakeSession: &fakeSession{events: make(chan Event)}}
}

func (session *ownershipSession) Send(ctx context.Context, stanza Stanza) error {
	if session.send != nil {
		return session.send(ctx, stanza)
	}
	return session.fakeSession.Send(ctx, stanza)
}

// Close is deliberately stateless: reconnect and client shutdown may close the
// same test session concurrently, and the production Session contract permits
// that overlap.
func (*ownershipSession) Close(context.Context) error { return nil }

type blockedControlOwnershipSession struct {
	*fakeSession
	controlEntered chan struct{}
	controlRelease chan struct{}
	controlOnce    sync.Once
	mu             sync.Mutex
	active         int
	maximumActive  int
	kinds          []StanzaKind
}

type postClaimAuthorityFenceSession struct {
	*fakeSession
	resumeEntered chan struct{}
	resumeRelease chan struct{}
	resumeOnce    sync.Once
	wireMu        sync.Mutex
	active        int
	maximumActive int
	envelopes     []uint64
}

type sendConcurrencyTracker struct {
	mu      sync.Mutex
	active  int
	maximum int
}

func (tracker *sendConcurrencyTracker) enter() func() {
	tracker.mu.Lock()
	tracker.active++
	if tracker.active > tracker.maximum {
		tracker.maximum = tracker.active
	}
	tracker.mu.Unlock()
	return func() {
		tracker.mu.Lock()
		tracker.active--
		tracker.mu.Unlock()
	}
}

func (tracker *sendConcurrencyTracker) maximumActive() int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.maximum
}

type hardInterruptedOwnershipSession struct {
	*fakeSession
	tracker          *sendConcurrencyTracker
	controlEntered   chan struct{}
	interrupted      chan struct{}
	controlEnterOnce sync.Once
	interruptOnce    sync.Once
	mu               sync.Mutex
	interrupts       int
	envelopeOrdinals []uint64
}

func newHardInterruptedOwnershipSession(tracker *sendConcurrencyTracker) *hardInterruptedOwnershipSession {
	return &hardInterruptedOwnershipSession{
		fakeSession:    &fakeSession{events: make(chan Event)},
		tracker:        tracker,
		controlEntered: make(chan struct{}),
		interrupted:    make(chan struct{}),
	}
}

func (session *hardInterruptedOwnershipSession) Send(ctx context.Context, stanza Stanza) error {
	release := session.tracker.enter()
	defer release()
	if stanza.Kind == StanzaSignal {
		session.controlEnterOnce.Do(func() { close(session.controlEntered) })
		// Deliberately ignore ctx. Only the exact-session hard-interrupt seam may
		// release this dependency and allow reconnect to become the wire owner.
		<-session.interrupted
		return ErrUnavailable
	}
	if stanza.Kind == StanzaEnvelope {
		session.mu.Lock()
		session.envelopeOrdinals = append(session.envelopeOrdinals, stanza.Ordinal)
		session.mu.Unlock()
	}
	return session.fakeSession.Send(ctx, stanza)
}

func (session *hardInterruptedOwnershipSession) interruptIO() {
	session.mu.Lock()
	session.interrupts++
	session.mu.Unlock()
	session.interruptOnce.Do(func() { close(session.interrupted) })
}

func (*hardInterruptedOwnershipSession) Close(context.Context) error { return nil }

func (session *hardInterruptedOwnershipSession) snapshot() (int, []uint64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.interrupts, append([]uint64(nil), session.envelopeOrdinals...)
}

type replacementOwnershipSession struct {
	*fakeSession
	tracker  *sendConcurrencyTracker
	mu       sync.Mutex
	ordinals []uint64
}

func newReplacementOwnershipSession(tracker *sendConcurrencyTracker) *replacementOwnershipSession {
	return &replacementOwnershipSession{fakeSession: &fakeSession{events: make(chan Event)}, tracker: tracker}
}

func (session *replacementOwnershipSession) Send(ctx context.Context, stanza Stanza) error {
	release := session.tracker.enter()
	defer release()
	if stanza.Kind == StanzaEnvelope {
		session.mu.Lock()
		session.ordinals = append(session.ordinals, stanza.Ordinal)
		session.mu.Unlock()
	}
	return session.fakeSession.Send(ctx, stanza)
}

func (*replacementOwnershipSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, err
	}
	return AuthoritySnapshot{Members: []string{"a@example.test/mesh", "b@example.test/mesh"}}, nil
}

func (*replacementOwnershipSession) Close(context.Context) error { return nil }

func (session *replacementOwnershipSession) envelopeOrdinals() []uint64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	return append([]uint64(nil), session.ordinals...)
}

type ownershipSequenceDialer struct {
	mu       sync.Mutex
	sessions []Session
	next     int
}

func (dialer *ownershipSequenceDialer) Dial(context.Context) (Session, error) {
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if dialer.next >= len(dialer.sessions) {
		return nil, ErrUnavailable
	}
	session := dialer.sessions[dialer.next]
	dialer.next++
	return session, nil
}

func newPostClaimAuthorityFenceSession() *postClaimAuthorityFenceSession {
	return &postClaimAuthorityFenceSession{
		fakeSession:   &fakeSession{events: make(chan Event)},
		resumeEntered: make(chan struct{}),
		resumeRelease: make(chan struct{}),
	}
}

func (session *postClaimAuthorityFenceSession) Send(ctx context.Context, stanza Stanza) error {
	session.wireMu.Lock()
	session.active++
	if session.active > session.maximumActive {
		session.maximumActive = session.active
	}
	if stanza.Kind == StanzaEnvelope {
		session.envelopes = append(session.envelopes, stanza.Ordinal)
	}
	session.wireMu.Unlock()
	defer func() {
		session.wireMu.Lock()
		session.active--
		session.wireMu.Unlock()
	}()
	return session.fakeSession.Send(ctx, stanza)
}

func (session *postClaimAuthorityFenceSession) Resume(ctx context.Context) (bool, error) {
	session.resumeOnce.Do(func() { close(session.resumeEntered) })
	select {
	case <-session.resumeRelease:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (*postClaimAuthorityFenceSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, err
	}
	return AuthoritySnapshot{Members: []string{"a@example.test/mesh", "b@example.test/mesh"}}, nil
}

func (*postClaimAuthorityFenceSession) Close(context.Context) error { return nil }

func (session *postClaimAuthorityFenceSession) wireSnapshot() (int, []uint64) {
	session.wireMu.Lock()
	defer session.wireMu.Unlock()
	return session.maximumActive, append([]uint64(nil), session.envelopes...)
}

func newBlockedControlOwnershipSession() *blockedControlOwnershipSession {
	return &blockedControlOwnershipSession{
		fakeSession:    &fakeSession{events: make(chan Event)},
		controlEntered: make(chan struct{}),
		controlRelease: make(chan struct{}),
	}
}

func (session *blockedControlOwnershipSession) Send(ctx context.Context, stanza Stanza) error {
	session.mu.Lock()
	session.active++
	if session.active > session.maximumActive {
		session.maximumActive = session.active
	}
	session.kinds = append(session.kinds, stanza.Kind)
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		session.active--
		session.mu.Unlock()
	}()
	if stanza.Kind == StanzaSignal {
		session.controlOnce.Do(func() { close(session.controlEntered) })
		// Deliberately violate prompt context cancellation to model a dependency
		// stuck below Session.Send. The client must not introduce a second writer
		// merely to make durable ownership progress.
		<-session.controlRelease
		return ctx.Err()
	}
	return session.fakeSession.Send(ctx, stanza)
}

func (*blockedControlOwnershipSession) Close(context.Context) error { return nil }

func (session *blockedControlOwnershipSession) snapshotWrites() (int, []StanzaKind) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.maximumActive, append([]StanzaKind(nil), session.kinds...)
}

func TestSendOwnedClassifiesOnlyPreHandoffFailures(t *testing.T) {
	t.Run("invalid before encoding", func(t *testing.T) {
		client := newOwnershipClient(t, 2, newOwnershipSession())
		if got := client.SendOwned(context.Background(), protocol.Envelope{}); got != Rejected {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 0)
	})

	t.Run("lifecycle unavailable", func(t *testing.T) {
		client := newOwnershipClient(t, 2, newOwnershipSession())
		if got := client.SendOwned(context.Background(), durableEnvelope(t)); got != UnavailableNoHandoff {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 0)
	})

	t.Run("pre-cancelled", func(t *testing.T) {
		client := newOwnershipClient(t, 2, newOwnershipSession())
		startOwnershipClient(t, client)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := client.SendOwned(ctx, durableEnvelope(t)); got != UnavailableNoHandoff {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 0)
		closeOwnershipClient(t, client)
	})

	t.Run("identity mismatch", func(t *testing.T) {
		client := newOwnershipClient(t, 2, newOwnershipSession())
		startOwnershipClient(t, client)
		envelope := durableEnvelope(t)
		envelope.Sender = "other@example.test/mesh"
		if got := client.SendOwned(context.Background(), envelope); got != Rejected {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 0)
		closeOwnershipClient(t, client)
	})

	t.Run("capacity", func(t *testing.T) {
		client := newOwnershipClient(t, 1, newOwnershipSession())
		startOwnershipClient(t, client)
		if _, err := client.outbox.Enqueue(durableEnvelope(t)); err != nil {
			t.Fatal(err)
		}
		if got := client.SendOwned(context.Background(), durableEnvelope(t)); got != Capacity {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 1)
		closeOwnershipClient(t, client)
	})

	t.Run("conflicting identity", func(t *testing.T) {
		client := newOwnershipClient(t, 2, newOwnershipSession())
		startOwnershipClient(t, client)
		original := durableEnvelope(t)
		if _, err := client.outbox.Enqueue(original); err != nil {
			t.Fatal(err)
		}
		conflict := original.Clone()
		changed, err := protocol.NewInlinePayload("native", []byte("different"))
		if err != nil {
			t.Fatal(err)
		}
		conflict.Payload = changed
		if got := client.SendOwned(context.Background(), conflict); got != Rejected {
			t.Fatalf("ownership = %v", got)
		}
		assertOwnershipOutbox(t, client, 1)
		closeOwnershipClient(t, client)
	})
}

func TestSendOwnedRetainsAcceptedOwnershipAcrossAttemptFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		send func(context.Context, Stanza) error
	}{
		{name: "session unavailable", send: func(context.Context, Stanza) error { return ErrUnavailable }},
		{name: "wire ambiguous", send: func(context.Context, Stanza) error { return transport.ErrSendAmbiguous }},
		{name: "private dependency", send: func(context.Context, Stanza) error { return errors.New("private wire canary") }},
		{name: "context after enqueue", send: func(ctx context.Context, _ Stanza) error {
			if cancel, ok := ctx.Value(ownershipCancelKey{}).(context.CancelFunc); ok {
				cancel()
			}
			return ctx.Err()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newOwnershipSession()
			session.send = test.send
			client := newOwnershipClient(t, 2, session)
			startOwnershipClient(t, client)
			ctx := context.Background()
			if test.name == "context after enqueue" {
				owned, cancel := context.WithCancel(ctx)
				defer cancel()
				ctx = context.WithValue(owned, ownershipCancelKey{}, context.CancelFunc(cancel))
			}
			if got := client.SendOwned(ctx, durableEnvelope(t)); got != AcceptedOwned {
				t.Fatalf("ownership = %v", got)
			}
			assertOwnershipOutbox(t, client, 1)
			closeOwnershipClient(t, client)
		})
	}
}

type preparedHydrationSession struct {
	*fakeSession
	replay []Stanza
}

func (session *preparedHydrationSession) PrepareResume(context.Context) (bool, error) {
	return true, nil
}

func (session *preparedHydrationSession) ReplayPrepared(ctx context.Context, allow func(Stanza) bool, prepare func(Stanza) (preparedReplayStanza, bool)) error {
	for _, record := range session.replay {
		if !allow(record) {
			return ErrStreamManagement
		}
	}
	for _, record := range session.replay {
		prepared, ok := prepare(record)
		if !ok {
			return ErrStreamManagement
		}
		if err := session.Send(ctx, prepared.Stanza); err != nil {
			if prepared.Release != nil {
				prepared.Release()
			}
			return err
		}
		if prepared.Release != nil {
			prepared.Release()
		}
	}
	return nil
}

func TestPreparedResumeHydratesEnvelopeBytesFromOutboxAtReplayTime(t *testing.T) {
	session := &preparedHydrationSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer closeOwnershipClient(t, client)

	envelope := durableEnvelope(t)
	if got := client.SendOwned(context.Background(), envelope); got != AcceptedOwned {
		t.Fatalf("ownership = %v", got)
	}
	session.mu.Lock()
	if len(session.sent) != 1 {
		t.Fatalf("initial sent = %d", len(session.sent))
	}
	metadata := session.sent[0].streamManagementRecord()
	session.sent = nil
	session.mu.Unlock()
	if len(metadata.Data) != 0 || metadata.MessageID != envelope.MessageID || metadata.Ordinal == 0 {
		t.Fatalf("metadata replay record = %#v", metadata)
	}
	session.replay = []Stanza{metadata}

	if err := client.replayPrepared(context.Background(), session, session); err != nil {
		t.Fatalf("prepared replay = %v", err)
	}
	session.mu.Lock()
	replayed := append([]Stanza(nil), session.sent...)
	session.mu.Unlock()
	if len(replayed) != 1 || replayed[0].MessageID != envelope.MessageID || replayed[0].Ordinal != metadata.Ordinal || len(replayed[0].Data) == 0 {
		t.Fatalf("hydrated replay = %#v", replayed)
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(replayed[0].Data)
	if err != nil {
		t.Fatalf("decode hydrated replay: %v", err)
	}
	defer clear(decoded.Payload.Inline)
	defer clear(decoded.CredentialProof)
	if decoded.MessageID != envelope.MessageID || decoded.Recipient != envelope.Recipient || decoded.MeshID != envelope.MeshID {
		t.Fatalf("decoded replay = %#v", decoded)
	}
	clearStanzas(replayed)
}

func TestReplayMeshAllowedRejectsEnvelopeMetadataFromWrongMesh(t *testing.T) {
	stanza := Stanza{
		Kind:      StanzaEnvelope,
		From:      "a@example.test/mesh-a",
		To:        "b@example.test/mesh-a",
		MeshID:    "mesh-a",
		MessageID: "msg_AQEBAQEBAQEBAQEBAQEBAQ",
		Ordinal:   1,
	}
	if !replayMeshAllowed(stanza, "mesh-a") {
		t.Fatal("valid current-mesh envelope metadata rejected")
	}
	if replayMeshAllowed(stanza, "mesh-b") {
		t.Fatal("wrong-mesh envelope metadata accepted")
	}
}

func TestReplayMeshAllowedRejectsMalformedEnvelopeMetadata(t *testing.T) {
	valid := Stanza{
		Kind:      StanzaEnvelope,
		From:      "a@example.test/mesh",
		To:        "b@example.test/mesh",
		MeshID:    "mesh",
		MessageID: "msg_AQEBAQEBAQEBAQEBAQEBAQ",
		Ordinal:   1,
	}
	tests := []struct {
		name string
		edit func(*Stanza)
	}{
		{name: "missing ordinal", edit: func(stanza *Stanza) { stanza.Ordinal = 0 }},
		{name: "bad message id", edit: func(stanza *Stanza) { stanza.MessageID = "msg_short" }},
		{name: "missing sender", edit: func(stanza *Stanza) { stanza.From = "" }},
		{name: "missing recipient", edit: func(stanza *Stanza) { stanza.To = "" }},
		{name: "retained bytes", edit: func(stanza *Stanza) { stanza.Data = []byte("forbidden") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stanza := valid
			test.edit(&stanza)
			if replayMeshAllowed(stanza, "mesh") {
				t.Fatalf("malformed envelope metadata accepted: %#v", stanza)
			}
		})
	}
}

func TestPreparedResumeRejectsEnvelopeMetadataThatDoesNotMatchOutbox(t *testing.T) {
	session := &preparedHydrationSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer closeOwnershipClient(t, client)

	envelope := durableEnvelope(t)
	if got := client.SendOwned(context.Background(), envelope); got != AcceptedOwned {
		t.Fatalf("ownership = %v", got)
	}
	session.mu.Lock()
	metadata := session.sent[0].streamManagementRecord()
	session.sent = nil
	session.mu.Unlock()

	metadata.To = "other@example.test/mesh"
	session.replay = []Stanza{metadata}
	if err := client.replayPrepared(context.Background(), session, session); !errors.Is(err, ErrStreamManagement) {
		t.Fatalf("mismatched metadata replay error = %v", err)
	}
}

func TestStaleIngressHandledEventDoesNotRetireRank2Ownership(t *testing.T) {
	session := newOwnershipSession()
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer closeOwnershipClient(t, client)

	envelope := durableEnvelope(t)
	if got := client.SendOwned(context.Background(), envelope); got != AcceptedOwned {
		t.Fatalf("ownership = %v", got)
	}
	metadata := client.outbox.Rank2Metadata()
	if len(metadata) != 1 || metadata[0].MessageID != envelope.MessageID {
		t.Fatalf("rank2 metadata = %#v", metadata)
	}
	client.mu.Lock()
	current := client.ingress
	stale := newIngressGeneration(current.id+1, current.session, current.boundIdentity, nil, context.Background())
	client.mu.Unlock()

	client.handleIngressEvent(stale, Event{Kind: EventHandled, HandledThrough: metadata[0].TransportOrdinal})
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("stale handled event retired current Rank2 ownership")
	}
	client.handleIngressEvent(stale, Event{Kind: EventCustodyAccepted, MessageID: envelope.MessageID})
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("stale custody event retired current Rank2 ownership")
	}
	client.handleIngressEvent(current, Event{Kind: EventHandled, HandledThrough: metadata[0].TransportOrdinal})
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("raw stream acknowledgement falsely retired Rank2 ownership")
	}
	client.handleIngressEvent(current, Event{Kind: EventCustodyAccepted, MessageID: envelope.MessageID})
	if client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("positive custody acceptance did not retire Rank2 ownership")
	}

	other := durableEnvelope(t)
	other.MessageID = typed("msg_", 0x6d)
	if got := client.SendOwned(context.Background(), other); got != AcceptedOwned {
		t.Fatalf("second ownership = %v", got)
	}
	client.handleIngressEvent(current, Event{Kind: EventCustodyAccepted, MessageID: envelope.MessageID})
	client.handleIngressEvent(current, Event{Kind: EventCustodyAccepted, MessageID: typed("msg_", 0x7e)})
	client.handleIngressEvent(current, Event{Kind: EventCustodyAccepted, MessageID: "not-canonical"})
	if !client.outbox.OwnsRank2(other.MessageID) {
		t.Fatal("duplicate, unknown, or malformed custody acceptance retired another owned message")
	}
}

func TestSendCompatibilityKeepsNormalizedAttemptError(t *testing.T) {
	session := newOwnershipSession()
	session.send = func(context.Context, Stanza) error { return errors.New("private wire canary") }
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	if err := client.Send(context.Background(), durableEnvelope(t)); !errors.Is(err, transport.ErrSendAmbiguous) || err.Error() != transport.ErrSendAmbiguous.Error() {
		t.Fatalf("Send() = %v", err)
	}
	assertOwnershipOutbox(t, client, 1)
	closeOwnershipClient(t, client)
}

func TestSendOwnedCancellationBeforeSerializedHandoffDoesNotOwn(t *testing.T) {
	client := newOwnershipClient(t, 1, newOwnershipSession())
	startOwnershipClient(t, client)
	client.sendMu.Lock()
	defer func() {
		client.sendMu.Unlock()
		closeOwnershipClient(t, client)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan SendOwnership, 1)
	go func() { done <- client.SendOwned(ctx, durableEnvelope(t)) }()
	cancel()
	select {
	case got := <-done:
		if got != UnavailableNoHandoff {
			t.Fatalf("pre-handoff cancellation ownership = %v", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("post-handoff cancellation remained blocked on send ownership")
	}
	assertOwnershipOutbox(t, client, 0)
}

func TestBlockedJingleSendDoesNotStarveQueuedDurableOwnership(t *testing.T) {
	session := newBlockedControlOwnershipSession()
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer func() {
		select {
		case <-session.controlRelease:
		default:
			close(session.controlRelease)
		}
		closeOwnershipClient(t, client)
	}()

	signal := sampleJingle()
	signal.Initiator = "a@example.test/mesh"
	signal.Responder = "b@example.test/mesh"
	signal.Content.Description.MeshID = "mesh"
	controlCtx, cancelControl := context.WithCancel(context.Background())
	controlDone := make(chan error, 1)
	go func() { controlDone <- client.SendJingle(controlCtx, signal.Responder, signal) }()
	select {
	case <-session.controlEntered:
	case <-time.After(time.Second):
		t.Fatal("Jingle send did not enter the blocking dependency")
	}
	cancelControl()

	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope.Clone()); err != nil {
		t.Fatal(err)
	}
	reservation, err := client.outbox.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" {
		t.Fatalf("reserve queued envelope = %#v, %v", reservation, err)
	}
	sendCtx, cancelSend := context.WithTimeout(context.Background(), 50*time.Millisecond)
	started := time.Now()
	ownership := client.SendQueuedOwned(sendCtx, envelope)
	cancelSend()
	if !client.outbox.Release(reservation) {
		t.Fatal("queued ownership did not preserve the caller reservation")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("queued ownership exceeded its bound: %s", elapsed)
	}
	if ownership != AcceptedOwned {
		t.Fatalf("queued ownership = %v, want %v", ownership, AcceptedOwned)
	}
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("queued envelope did not transfer to durable Rank2 ownership")
	}
	maximumActive, kinds := session.snapshotWrites()
	if maximumActive != 1 || len(kinds) != 1 || kinds[0] != StanzaSignal {
		t.Fatalf("wire serialization corrupted: maximum-active=%d kinds=%v", maximumActive, kinds)
	}

	close(session.controlRelease)
	select {
	case err := <-controlDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Jingle result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Jingle send did not return after dependency release")
	}
}

func TestQueuedOwnershipAuthorityFenceAfterClaimReplaysExactOrdinal(t *testing.T) {
	session := newPostClaimAuthorityFenceSession()
	client := newOwnershipClient(t, 2, session)
	startOwnershipClient(t, client)
	defer func() {
		select {
		case <-session.resumeRelease:
		default:
			close(session.resumeRelease)
		}
		closeOwnershipClient(t, client)
	}()

	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope.Clone()); err != nil {
		t.Fatal(err)
	}
	reservation, err := client.outbox.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" {
		t.Fatalf("reserve queued envelope = %#v, %v", reservation, err)
	}

	if err := client.sendMu.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	sendDone := make(chan SendOwnership, 1)
	go func() { sendDone <- client.SendQueuedOwned(context.Background(), envelope) }()

	deadline := time.Now().Add(time.Second)
	for !client.outbox.OwnsRank2(envelope.MessageID) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		client.sendMu.Unlock()
		t.Fatal("queued send did not claim Rank2 ownership while waiting for the writer")
	}
	metadata := client.outbox.Rank2Metadata()
	if len(metadata) != 1 || metadata[0].MessageID != envelope.MessageID || metadata[0].TransportOrdinal == 0 {
		client.sendMu.Unlock()
		t.Fatalf("claimed metadata = %#v", metadata)
	}

	client.mu.Lock()
	if client.session != session || client.state != DurableLive || !client.membershipReady {
		client.mu.Unlock()
		client.sendMu.Unlock()
		t.Fatal("fixture did not retain the exact live session before the authority fence")
	}
	client.pauseMembershipLocked("post-claim-authority-fence")
	client.mu.Unlock()
	client.sendMu.Unlock()

	select {
	case got := <-sendDone:
		if got != AcceptedOwned {
			t.Fatalf("post-claim ownership = %v, want %v", got, AcceptedOwned)
		}
	case <-time.After(time.Second):
		t.Fatal("post-claim send did not return after writer release")
	}
	if !client.outbox.Release(reservation) {
		t.Fatal("queued ownership did not preserve the caller reservation")
	}

	select {
	case <-session.resumeEntered:
	case <-time.After(time.Second):
		t.Fatal("post-claim authority fence did not request recovery")
	}
	if state := client.DurableState(); state != DurablePending {
		t.Fatalf("durable state before replay = %v, want pending", state)
	}
	if maximumActive, ordinals := session.wireSnapshot(); maximumActive != 0 || len(ordinals) != 0 {
		t.Fatalf("owned envelope entered the old session: maximum-active=%d ordinals=%v", maximumActive, ordinals)
	}

	close(session.resumeRelease)
	deadline = time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		freshPending := client.state == DurableLive && !client.membershipReady && client.authoritySnapshot.nonce != ""
		client.mu.Unlock()
		if freshPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("clean replacement did not reach pending authority installation")
		}
		time.Sleep(time.Millisecond)
	}
	if _, ordinals := session.wireSnapshot(); len(ordinals) != 0 {
		t.Fatalf("clean replacement replay escaped before authority installation: %v", ordinals)
	}
	if err := acknowledgeCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatalf("acknowledge clean replacement membership: %v", err)
	}
	deadline = time.Now().Add(time.Second)
	for {
		maximumActive, ordinals := session.wireSnapshot()
		if len(ordinals) == 1 {
			if maximumActive != 1 || ordinals[0] != metadata[0].TransportOrdinal {
				t.Fatalf("replay serialization = maximum-active=%d ordinals=%v, want [%d]", maximumActive, ordinals, metadata[0].TransportOrdinal)
			}
			break
		}
		if len(ordinals) > 1 || time.Now().After(deadline) {
			t.Fatalf("exact owned ordinal was not replayed once: ordinals=%v", ordinals)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPostClaimWriterStarvationHardInterruptsAndReplaysOnReplacement(t *testing.T) {
	tracker := &sendConcurrencyTracker{}
	first := newHardInterruptedOwnershipSession(tracker)
	replacement := newReplacementOwnershipSession(tracker)
	dialer := &ownershipSequenceDialer{sessions: []Session{first, replacement}}
	client := newOwnershipClientWithDialer(t, 4, dialer)
	startOwnershipClient(t, client)
	defer closeOwnershipClient(t, client)

	signal := sampleJingle()
	signal.Initiator = "a@example.test/mesh"
	signal.Responder = "b@example.test/mesh"
	signal.Content.Description.MeshID = "mesh"
	controlDone := make(chan error, 1)
	go func() { controlDone <- client.SendJingle(context.Background(), signal.Responder, signal) }()
	select {
	case <-first.controlEntered:
	case <-time.After(time.Second):
		t.Fatal("Jingle send did not acquire the original session writer")
	}

	firstEnvelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(firstEnvelope.Clone()); err != nil {
		t.Fatal(err)
	}
	reservation, err := client.outbox.ReserveEligible(firstEnvelope.MessageID)
	if err != nil || reservation.MessageID == "" {
		t.Fatalf("reserve queued envelope = %#v, %v", reservation, err)
	}
	claimCtx, cancelClaim := context.WithTimeout(context.Background(), 50*time.Millisecond)
	ownership := client.SendQueuedOwned(claimCtx, firstEnvelope)
	cancelClaim()
	if !client.outbox.Release(reservation) {
		t.Fatal("queued ownership did not preserve its caller reservation")
	}
	if ownership != AcceptedOwned || !client.outbox.OwnsRank2(firstEnvelope.MessageID) {
		t.Fatalf("post-claim ownership=%v owns=%t", ownership, client.outbox.OwnsRank2(firstEnvelope.MessageID))
	}
	metadata := client.outbox.Rank2Metadata()
	if len(metadata) != 1 || metadata[0].MessageID != firstEnvelope.MessageID || metadata[0].TransportOrdinal == 0 {
		t.Fatalf("claimed metadata = %#v", metadata)
	}

	select {
	case err := <-controlDone:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("hard-interrupted Jingle result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hard interrupt did not release the original writer")
	}
	deadline := time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		published := client.session == replacement && client.state == DurableLive
		client.mu.Unlock()
		if published {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement session was not published")
		}
		time.Sleep(time.Millisecond)
	}
	if ordinals := replacement.envelopeOrdinals(); len(ordinals) != 0 {
		t.Fatalf("clean replacement replay escaped before authority installation: %v", ordinals)
	}
	if err := acknowledgeCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatalf("acknowledge replacement membership: %v", err)
	}
	deadline = time.Now().Add(time.Second)
	for len(replacement.envelopeOrdinals()) < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ordinals := replacement.envelopeOrdinals(); len(ordinals) != 1 || ordinals[0] != metadata[0].TransportOrdinal {
		t.Fatalf("replacement replay ordinals=%v, want [%d]", ordinals, metadata[0].TransportOrdinal)
	}

	second := durableEnvelope(t)
	second.MessageID = typed("msg_", 0x72)
	if got := client.SendOwned(context.Background(), second); got != AcceptedOwned {
		t.Fatalf("fresh replacement ownership = %v", got)
	}
	deadline = time.Now().Add(time.Second)
	for len(replacement.envelopeOrdinals()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ordinals := replacement.envelopeOrdinals()
	if len(ordinals) != 2 || ordinals[0] != metadata[0].TransportOrdinal || ordinals[1] == 0 || ordinals[1] == ordinals[0] {
		t.Fatalf("replacement envelope ordinals = %v", ordinals)
	}
	interrupts, oldOrdinals := first.snapshot()
	if interrupts != 1 || len(oldOrdinals) != 0 {
		t.Fatalf("original session interrupts=%d envelope-ordinals=%v", interrupts, oldOrdinals)
	}
	if maximum := tracker.maximumActive(); maximum != 1 {
		t.Fatalf("maximum concurrent Session.Send calls = %d, want 1", maximum)
	}
}

type ownershipCancelKey struct{}

func newOwnershipClient(t *testing.T, capacity int, session Session) *Client {
	return newOwnershipClientWithDialer(t, capacity, fakeDialer{session})
}

func newOwnershipClientWithDialer(t *testing.T, capacity int, dialer Dialer) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: capacity, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint:        "localhost:5222",
		Auth:            Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity: capacity, TransferWorkers: 1, TransferQueue: capacity, MailboxLimit: capacity,
		TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: capacity,
		UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second,
		ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond,
		ReconnectOperationTimeout: time.Second,
	}, dialer, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func startOwnershipClient(t *testing.T, client *Client) {
	t.Helper()
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
}

func closeOwnershipClient(t *testing.T, client *Client) {
	t.Helper()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertOwnershipOutbox(t *testing.T, client *Client, want int) {
	t.Helper()
	got, _ := client.outbox.Usage()
	if got != want {
		t.Fatalf("private outbox messages = %d, want %d", got, want)
	}
	if diagnostic := client.PendingEnvelopeCount(); diagnostic != uint64(want) {
		t.Fatalf("pending envelope diagnostic = %d, want %d", diagnostic, want)
	}
}
