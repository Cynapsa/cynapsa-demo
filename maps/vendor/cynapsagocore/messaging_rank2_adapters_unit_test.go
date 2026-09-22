package cynapsagocore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type rootOwnedSender struct {
	disposition rank2xmpp.SendOwnership
	received    protocol.Envelope
}

type rootMembershipReadiness bool

func (ready rootMembershipReadiness) MembershipReady() bool { return bool(ready) }

func TestRank2MembershipRefreshUsesLevelConditionAfterCoalescedStateEdges(t *testing.T) {
	if rank2MembershipRefreshRequired(rootMembershipReadiness(true), false) {
		t.Fatal("acknowledged live membership requested a redundant refresh")
	}
	if !rank2MembershipRefreshRequired(rootMembershipReadiness(false), false) {
		t.Fatal("unacknowledged replacement snapshot was hidden by a coalesced Pending edge")
	}
	if rank2MembershipRefreshRequired(rootMembershipReadiness(true), true) {
		t.Fatal("a stale Pending edge overrode the fully published level state")
	}
	clientReady, compositeAuthorityReady := rootMembershipReadiness(true), rootMembershipReadiness(false)
	if !clientReady.MembershipReady() || !rank2MembershipRefreshRequired(compositeAuthorityReady, false) {
		t.Fatal("narrow Rank2 readiness hid fenced peer lanes")
	}
}

func TestRank2AuthorityRetryWaitsForStateOrBackoffAndStopsWithContext(t *testing.T) {
	changed := make(chan struct{})
	close(changed)
	if !waitRank2AuthorityRetry(context.Background(), changed, time.Hour) {
		t.Fatal("state edge did not wake authority retry")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if waitRank2AuthorityRetry(cancelled, make(chan struct{}), time.Hour) {
		t.Fatal("cancelled authority retry remained live")
	}
}

func (sender *rootOwnedSender) SendOwned(_ context.Context, envelope protocol.Envelope) rank2xmpp.SendOwnership {
	sender.received = envelope.Clone()
	clear(envelope.Payload.Inline)
	clear(envelope.CredentialProof)
	return sender.disposition
}

func TestRank2EnvelopeCarrierMapsOnlyProvenOwnership(t *testing.T) {
	tests := []struct {
		name string
		in   rank2xmpp.SendOwnership
		want mesh.CarrierDisposition
	}{
		{name: "accepted rank2 owned", in: rank2xmpp.AcceptedOwned, want: mesh.CarrierAmbiguous},
		{name: "unavailable no handoff", in: rank2xmpp.UnavailableNoHandoff, want: mesh.CarrierUnavailable},
		{name: "rejected", in: rank2xmpp.Rejected, want: mesh.CarrierRejected},
		{name: "capacity", in: rank2xmpp.Capacity, want: mesh.CarrierCapacity},
		{name: "unknown retains caller ownership", in: rank2xmpp.SendOwnership(255), want: mesh.CarrierUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender := &rootOwnedSender{disposition: test.in}
			carrier := newRank2EnvelopeCarrier(sender)
			envelope := rootRank2Envelope(t, 1, "one")
			original := envelope.Clone()
			if got := carrier.Send(context.Background(), envelope); got != test.want {
				t.Fatalf("Send() = %v, want %v", got, test.want)
			}
			if sender.received.MessageID != original.MessageID || string(sender.received.Payload.Inline) != string(original.Payload.Inline) || string(sender.received.CredentialProof) != string(original.CredentialProof) {
				t.Fatal("carrier changed the projected envelope")
			}
			if string(envelope.Payload.Inline) != string(original.Payload.Inline) || string(envelope.CredentialProof) != string(original.CredentialProof) {
				t.Fatal("sender mutation escaped carrier ownership copy")
			}
		})
	}
	if got := (*rank2EnvelopeCarrier)(nil).Send(context.Background(), protocol.Envelope{}); got != mesh.CarrierUnavailable {
		t.Fatalf("nil carrier = %v", got)
	}
}

type rootInboundStep struct {
	inbound rank2xmpp.AuthenticatedInbound
	err     error
}

type rootInboundSource struct {
	mu        sync.Mutex
	steps     []rootInboundStep
	next      int
	entered   chan struct{}
	active    bool
	closed    bool
	afterStop bool
}

func (source *rootInboundSource) ReceiveAuthenticated(ctx context.Context) (rank2xmpp.AuthenticatedInbound, error) {
	source.mu.Lock()
	if source.closed {
		source.afterStop = true
		source.mu.Unlock()
		return rank2xmpp.AuthenticatedInbound{}, rank2xmpp.ErrClosed
	}
	source.active = true
	if source.next < len(source.steps) {
		step := source.steps[source.next]
		source.next++
		source.active = false
		source.mu.Unlock()
		step.inbound.Envelope = step.inbound.Envelope.Clone()
		return step.inbound, step.err
	}
	entered := source.entered
	source.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	source.mu.Lock()
	source.active = false
	source.mu.Unlock()
	return rank2xmpp.AuthenticatedInbound{}, ctx.Err()
}

func (source *rootInboundSource) ReceiveAuthenticatedForQuarantine(ctx context.Context) (rank2xmpp.AuthenticatedInbound, error) {
	return source.ReceiveAuthenticated(ctx)
}

func (source *rootInboundSource) closeAfterJoin(t *testing.T) {
	t.Helper()
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.active {
		t.Fatal("source closed before pump joined")
	}
	source.closed = true
}

type rootInboundReceiver struct {
	mu        sync.Mutex
	senders   []string
	envelopes []protocol.Envelope
	failAt    int
	failure   *mesh.Failure
}

type rootPanickingInboundReceiver struct {
	calls    int
	retained [][]byte
}

type rootRank2DispositionReceiver struct {
	accepted bool
	failure  *mesh.Failure
}

func (receiver *rootRank2DispositionReceiver) Receive(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) *mesh.Failure {
	return receiver.failure
}

func (receiver *rootRank2DispositionReceiver) ReceiveRank2(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) (bool, *mesh.Failure) {
	return receiver.accepted, receiver.failure
}

func (receiver *rootPanickingInboundReceiver) Receive(_ context.Context, _ mesh.AuthenticatedProvenance, envelope protocol.Envelope) *mesh.Failure {
	receiver.calls++
	receiver.retained = append(receiver.retained, envelope.Payload.Inline)
	if receiver.calls == 1 {
		panic("receiver panic canary")
	}
	return nil
}

func rootRank2InboundItem(envelope protocol.Envelope, sender string, accepts, rejects *int) rank2InboundItem {
	return rank2InboundItem{
		authenticatedSender: sender,
		envelope:            envelope,
		accept: func(context.Context) error {
			*accepts++
			return nil
		},
		reject: func() { *rejects++ },
	}
}

func rootBytesCleared(value []byte) bool {
	for _, octet := range value {
		if octet != 0 {
			return false
		}
	}
	return true
}

func TestRank2InboundItemRejectsProvenanceFailureAndClearsPayload(t *testing.T) {
	envelope := rootRank2Envelope(t, 1, "provenance-secret")
	payload := envelope.Payload.Inline
	proof := envelope.CredentialProof
	accepts, rejects := 0, 0
	item := rootRank2InboundItem(envelope, "not-a-bound-identity", &accepts, &rejects)
	pump := newRank2InboundPump(&rootInboundSource{}, &rootInboundReceiver{}, "local@example.test/mesh", "mesh")

	failure := pump.processInboundItem(context.Background(), item)
	if failure == nil || failure.Code != mesh.FailureInternal {
		t.Fatalf("processInboundItem() = %#v", failure)
	}
	if accepts != 0 || rejects != 1 {
		t.Fatalf("terminal decisions: accept=%d reject=%d", accepts, rejects)
	}
	if !rootBytesCleared(payload) || !rootBytesCleared(proof) {
		t.Fatal("provenance failure retained transport-owned payload bytes")
	}
}

func TestRank2InboundItemAcknowledgesCapacityDrop(t *testing.T) {
	envelope := rootRank2Envelope(t, 1, "capacity-secret")
	payload := envelope.Payload.Inline
	proof := envelope.CredentialProof
	accepts, rejects := 0, 0
	receiver := &rootRank2DispositionReceiver{accepted: true, failure: &mesh.Failure{Code: mesh.FailureCapacity}}
	pump := newRank2InboundPump(&rootInboundSource{}, receiver, "local@example.test/mesh", "mesh")

	failure := pump.processInboundItem(context.Background(), rootRank2InboundItem(envelope, "peer@example.test/mesh", &accepts, &rejects))
	if failure == nil || failure.Code != mesh.FailureCapacity {
		t.Fatalf("processInboundItem() = %#v, want capacity", failure)
	}
	if accepts != 1 || rejects != 0 {
		t.Fatalf("terminal decisions: accept=%d reject=%d", accepts, rejects)
	}
	if !rootBytesCleared(payload) || !rootBytesCleared(proof) {
		t.Fatal("capacity drop retained transport-owned payload bytes")
	}
}

func TestRank2InboundItemRejectsReceiverPanicClearsPayloadAndAllowsNextItem(t *testing.T) {
	receiver := &rootPanickingInboundReceiver{}
	pump := newRank2InboundPump(&rootInboundSource{}, receiver, "local@example.test/mesh", "mesh")
	first := rootRank2Envelope(t, 1, "panic-secret")
	firstPayload := first.Payload.Inline
	firstProof := first.CredentialProof
	firstAccepts, firstRejects := 0, 0

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("receiver panic did not propagate to the worker boundary")
			}
		}()
		_ = pump.processInboundItem(context.Background(), rootRank2InboundItem(first, "peer@example.test/mesh", &firstAccepts, &firstRejects))
	}()
	if firstAccepts != 0 || firstRejects != 1 {
		t.Fatalf("panicking terminal decisions: accept=%d reject=%d", firstAccepts, firstRejects)
	}
	if !rootBytesCleared(firstPayload) || !rootBytesCleared(firstProof) || len(receiver.retained) != 1 || !rootBytesCleared(receiver.retained[0]) {
		t.Fatal("receiver panic retained transport-owned or receiver-copy payload bytes")
	}

	second := rootRank2Envelope(t, 2, "next-secret")
	secondPayload := second.Payload.Inline
	secondProof := second.CredentialProof
	secondAccepts, secondRejects := 0, 0
	if failure := pump.processInboundItem(context.Background(), rootRank2InboundItem(second, "peer@example.test/mesh", &secondAccepts, &secondRejects)); failure != nil {
		t.Fatalf("subsequent processInboundItem() = %#v", failure)
	}
	if secondAccepts != 1 || secondRejects != 0 {
		t.Fatalf("subsequent terminal decisions: accept=%d reject=%d", secondAccepts, secondRejects)
	}
	if receiver.calls != 2 || !rootBytesCleared(secondPayload) || !rootBytesCleared(secondProof) || len(receiver.retained) != 2 || !rootBytesCleared(receiver.retained[1]) {
		t.Fatal("subsequent item did not complete with bounded payload ownership")
	}
}

func (receiver *rootInboundReceiver) Receive(_ context.Context, provenance mesh.AuthenticatedProvenance, envelope protocol.Envelope) *mesh.Failure {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	receiver.senders = append(receiver.senders, provenance.Sender())
	receiver.envelopes = append(receiver.envelopes, envelope.Clone())
	clear(envelope.Payload.Inline)
	clear(envelope.CredentialProof)
	if receiver.failure != nil && len(receiver.envelopes) == receiver.failAt {
		return receiver.failure
	}
	return nil
}

func TestRank2InboundPumpPreservesProvenanceOrderAndOwnership(t *testing.T) {
	first := rootRank2Envelope(t, 1, "first")
	second := rootRank2Envelope(t, 2, "second")
	// The claimed envelope sender is deliberately distinct from transport
	// provenance. Root must forward only the separately authenticated value.
	source := &rootInboundSource{steps: []rootInboundStep{
		{inbound: rank2xmpp.AuthenticatedInbound{AuthenticatedSender: "peer-a@example.test/mesh", Envelope: first}},
		{inbound: rank2xmpp.AuthenticatedInbound{AuthenticatedSender: "peer-a@example.test/mesh", Envelope: second}},
		{err: rank2xmpp.ErrClosed},
	}}
	receiver := &rootInboundReceiver{failAt: 2, failure: &mesh.Failure{Code: mesh.FailureRejected}}
	pump := newRank2InboundPump(source, receiver, "local@example.test/mesh", "mesh")
	if failure := pump.Run(context.Background()); failure == nil || failure.Code != mesh.FailureUnavailable {
		t.Fatalf("Run() = %#v", failure)
	}
	if len(receiver.envelopes) != 2 || string(receiver.envelopes[0].Payload.Inline) != "first" || string(receiver.envelopes[1].Payload.Inline) != "second" {
		t.Fatalf("receive order = %#v", receiver.envelopes)
	}
	if receiver.senders[0] != "peer-a@example.test/mesh" || receiver.senders[1] != "peer-a@example.test/mesh" {
		t.Fatalf("authenticated senders = %q", receiver.senders)
	}
	if string(source.steps[0].inbound.Envelope.Payload.Inline) != "first" || string(first.Payload.Inline) != "first" {
		t.Fatal("receiver mutation escaped pump ownership copies")
	}
}

func TestRank2InboundPumpCancellationJoinsBeforeSourceClose(t *testing.T) {
	source := &rootInboundSource{entered: make(chan struct{}, 1)}
	pump := newRank2InboundPump(source, &rootInboundReceiver{}, "local@example.test/mesh", "mesh")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *mesh.Failure, 1)
	go func() { done <- pump.Run(ctx) }()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("pump did not enter source receive")
	}
	cancel()
	select {
	case failure := <-done:
		if failure == nil || failure.Code != mesh.FailureCancelled {
			t.Fatalf("Run() = %#v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("pump did not stop after cancellation")
	}
	source.closeAfterJoin(t)
	source.mu.Lock()
	afterStop := source.afterStop
	source.mu.Unlock()
	if afterStop {
		t.Fatal("pump read source after reverse shutdown")
	}
}

func TestRank2InboundPumpBoundsSourceAndReceiverFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want mesh.FailureCode
	}{
		{name: "closed", err: rank2xmpp.ErrClosed, want: mesh.FailureUnavailable},
		{name: "unavailable", err: rank2xmpp.ErrUnavailable, want: mesh.FailureUnavailable},
		{name: "deadline", err: context.DeadlineExceeded, want: mesh.FailureDeadline},
		{name: "cancelled", err: context.Canceled, want: mesh.FailureCancelled},
		{name: "private", err: errors.New("private source canary"), want: mesh.FailureInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &rootInboundSource{steps: []rootInboundStep{{err: test.err}}}
			failure := newRank2InboundPump(source, &rootInboundReceiver{}, "local@example.test/mesh", "mesh").Run(context.Background())
			if failure == nil || failure.Code != test.want {
				t.Fatalf("Run() = %#v, want %v", failure, test.want)
			}
		})
	}

	envelope := rootRank2Envelope(t, 1, "one")
	source := &rootInboundSource{steps: []rootInboundStep{{inbound: rank2xmpp.AuthenticatedInbound{AuthenticatedSender: "peer@example.test/mesh", Envelope: envelope}}}}
	receiver := &rootInboundReceiver{failAt: 1, failure: &mesh.Failure{Code: mesh.FailureCode(255)}}
	if failure := newRank2InboundPump(source, receiver, "local@example.test/mesh", "mesh").Run(context.Background()); failure == nil || failure.Code != mesh.FailureInternal {
		t.Fatalf("unbounded receiver failure = %#v", failure)
	}
	if failure := (*rank2InboundPump)(nil).Run(context.Background()); failure == nil || failure.Code != mesh.FailureInternal {
		t.Fatalf("nil pump = %#v", failure)
	}
}

func rootRank2Envelope(t *testing.T, _ uint64, body string) protocol.Envelope {
	t.Helper()
	payload, err := protocol.NewInlinePayload("native", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("root-rank2-adapter-conversation"))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   "conv_" + base64.RawURLEncoding.EncodeToString(digest[:]),
		Sender:           "claimed@example.test/mesh",
		Recipient:        "local@example.test/mesh",
		MeshID:           "mesh",
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Unix(1, 0).UTC(),
		ClockUncertainty: time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

type rootLiveCarrier struct {
	mu       sync.Mutex
	sent     []protocol.Envelope
	state    transport.HealthState
	progress time.Time
}

func (*rootLiveCarrier) Kind() transport.Kind        { return transport.KindLive }
func (*rootLiveCarrier) Start(context.Context) error { return nil }
func (carrier *rootLiveCarrier) Observe() transport.Observation {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	state := carrier.state
	if state == transport.HealthUnknown {
		state = transport.HealthHealthy
	}
	return transport.Observation{State: state, LastProgress: carrier.progress}
}
func (*rootLiveCarrier) Close(context.Context) error { return nil }
func (*rootLiveCarrier) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (carrier *rootLiveCarrier) Send(_ context.Context, envelope protocol.Envelope) error {
	carrier.mu.Lock()
	carrier.sent = append(carrier.sent, envelope.Clone())
	carrier.mu.Unlock()
	return nil
}

type rootDurableCarrier struct {
	mu    sync.Mutex
	sends int
}

func (carrier *rootDurableCarrier) Send(context.Context, protocol.Envelope) mesh.CarrierDisposition {
	carrier.mu.Lock()
	carrier.sends++
	carrier.mu.Unlock()
	return mesh.CarrierAccepted
}

type orderedDurableCarrier struct {
	entered   chan struct{}
	release   chan struct{}
	completed chan struct{}
	once      sync.Once
}

func (carrier *orderedDurableCarrier) Send(context.Context, protocol.Envelope) mesh.CarrierDisposition {
	carrier.once.Do(func() { close(carrier.entered) })
	<-carrier.release
	close(carrier.completed)
	return mesh.CarrierAccepted
}

type durableOrderedRecoverer struct {
	durableCompleted <-chan struct{}
	started          chan bool
}

func (recoverer *durableOrderedRecoverer) Recover(context.Context, string) error {
	completed := false
	select {
	case <-recoverer.durableCompleted:
		completed = true
	default:
	}
	recoverer.started <- completed
	return nil
}

func TestRankedCarrierTransfersDurableOwnershipBeforeStartingRecovery(t *testing.T) {
	for _, operation := range []string{"send", "send with receipt", "fallback"} {
		t.Run(operation, func(t *testing.T) {
			live, err := transport.NewLiveManager(1)
			if err != nil {
				t.Fatal(err)
			}
			if err = live.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := live.Close(context.Background()); err != nil {
					t.Errorf("close live manager: %v", err)
				}
			}()

			durable := &orderedDurableCarrier{
				entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{}),
			}
			recoverer := &durableOrderedRecoverer{durableCompleted: durable.completed, started: make(chan bool, 1)}
			carrier := newRankedEnvelopeCarrier(live, recoverer, durable, time.Second, time.Millisecond, transport.ClockFunc(time.Now))
			defer func() {
				carrier.stop()
				carrier.wait()
			}()

			envelope := rootRank2Envelope(t, 1, operation)
			done := make(chan mesh.CarrierDisposition, 1)
			go func() {
				switch operation {
				case "send":
					done <- carrier.Send(context.Background(), envelope)
				case "send with receipt":
					receipt, disposition := carrier.SendWithReceipt(context.Background(), envelope)
					if receipt != nil {
						t.Errorf("receipt = %#v, want nil durable fallback receipt", receipt)
					}
					done <- disposition
				case "fallback":
					done <- carrier.Fallback(context.Background(), envelope)
				}
			}()
			select {
			case <-durable.entered:
			case <-time.After(time.Second):
				t.Fatal("durable send was not entered")
			}
			carrier.mu.Lock()
			pendingRecoveries := len(carrier.pending)
			carrier.mu.Unlock()
			if pendingRecoveries != 0 {
				t.Fatalf("recovery demand was published before durable Send completed: pending=%d", pendingRecoveries)
			}

			close(durable.release)
			select {
			case disposition := <-done:
				if disposition != mesh.CarrierAccepted {
					t.Fatalf("disposition = %v, want accepted", disposition)
				}
			case <-time.After(time.Second):
				t.Fatal("durable carrier call did not return")
			}
			select {
			case completed := <-recoverer.started:
				if !completed {
					t.Fatal("recovery began before durable Send completed")
				}
			case <-time.After(time.Second):
				t.Fatal("post-handoff recovery was not started")
			}
		})
	}
}

type rootHandshakeNegotiator struct {
	established chan string
	release     <-chan struct{}
	results     <-chan error
}

func (negotiator rootHandshakeNegotiator) Establish(_ context.Context, attempt handshake.Attempt) error {
	if negotiator.established != nil {
		negotiator.established <- attempt.PeerID
	}
	if negotiator.release != nil {
		<-negotiator.release
	}
	if negotiator.results != nil {
		return <-negotiator.results
	}
	return nil
}
func (rootHandshakeNegotiator) Accept(context.Context, handshake.Attempt, handshake.Signal) error {
	return nil
}
func (rootHandshakeNegotiator) Apply(context.Context, handshake.Attempt, handshake.Signal) error {
	return nil
}

func TestRankedCarrierDemotesStaleDisconnectedLinkAndDemandsReplacement(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter := &rootLiveCarrier{state: transport.HealthDisconnected, progress: now.Add(-30 * time.Second)}
	peerID := "local@example.test/mesh"
	if err = live.InstallLive(context.Background(), peerID, adapter); err != nil {
		t.Fatal(err)
	}
	established := make(chan string, 1)
	handshakes, err := handshake.NewManager(handshake.Config{
		LocalIdentity: "claimed@example.test/mesh", MaximumPeers: 2, MaximumAttempts: 2,
		AttemptTimeout: time.Second, CooldownInitial: time.Millisecond, CooldownMaximum: time.Second,
		Clock: transport.ClockFunc(func() time.Time { return now }), Random: bytes.NewReader(bytes.Repeat([]byte{2}, 64)), Jitter: func(value time.Duration) time.Duration { return value },
	}, rootHandshakeNegotiator{established: established})
	if err != nil {
		t.Fatal(err)
	}
	durable := &rootDurableCarrier{}
	carrier := newRankedEnvelopeCarrier(live, handshakes, durable, time.Second, time.Millisecond, transport.ClockFunc(func() time.Time { return now }))
	envelope := rootRank2Envelope(t, 1, "recover")
	if disposition := carrier.Send(context.Background(), envelope); disposition != mesh.CarrierAccepted {
		t.Fatalf("disposition=%v", disposition)
	}
	adapter.mu.Lock()
	liveSends := len(adapter.sent)
	adapter.mu.Unlock()
	if liveSends != 0 {
		t.Fatalf("stale disconnected link accepted %d sends", liveSends)
	}
	durable.mu.Lock()
	durableSends := durable.sends
	durable.mu.Unlock()
	if durableSends != 1 {
		t.Fatalf("durable sends=%d", durableSends)
	}
	select {
	case peer := <-established:
		if peer != peerID {
			t.Fatalf("replacement peer=%q", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement was not demanded")
	}
	carrier.stop()
	handshakes.Close()
	carrier.wait()
	if err = live.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRankedCarrierHealthAdmissionBoundary(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	carrier := &rankedEnvelopeCarrier{clock: transport.ClockFunc(func() time.Time { return now })}
	tests := []struct {
		name        string
		observation transport.Observation
		want        bool
	}{
		{name: "healthy", observation: transport.Observation{State: transport.HealthHealthy}, want: true},
		{name: "temporary disconnect", observation: transport.Observation{State: transport.HealthDisconnected, LastProgress: now.Add(-30*time.Second + time.Nanosecond)}, want: true},
		{name: "demotion boundary", observation: transport.Observation{State: transport.HealthDisconnected, LastProgress: now.Add(-30 * time.Second)}},
		{name: "missing progress", observation: transport.Observation{State: transport.HealthDisconnected}},
		{name: "future progress", observation: transport.Observation{State: transport.HealthDisconnected, LastProgress: now.Add(time.Nanosecond)}},
		{name: "hard failure", observation: transport.Observation{State: transport.HealthFailed, LastProgress: now}},
		{name: "closed", observation: transport.Observation{State: transport.HealthClosed, LastProgress: now}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := carrier.liveEligible(test.observation); got != test.want {
				t.Fatalf("liveEligible()=%t want %t", got, test.want)
			}
		})
	}
}

func TestRankedCarrierDoesNotLoseRecoveryDemandWhileAttemptIsInFlight(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter := &rootLiveCarrier{state: transport.HealthFailed, progress: now}
	peerID := "local@example.test/mesh"
	if err = live.InstallLive(context.Background(), peerID, adapter); err != nil {
		t.Fatal(err)
	}
	established := make(chan string, 1)
	release := make(chan struct{})
	results := make(chan error, 2)
	results <- errors.New("first recovery failed")
	results <- nil
	handshakes, err := handshake.NewManager(handshake.Config{
		LocalIdentity: "claimed@example.test/mesh", MaximumPeers: 2, MaximumAttempts: 4,
		AttemptTimeout: time.Second, CooldownInitial: time.Millisecond, CooldownMaximum: time.Second,
		Clock: transport.ClockFunc(func() time.Time { return now }), Random: bytes.NewReader(append(bytes.Repeat([]byte{3}, 32), bytes.Repeat([]byte{4}, 32)...)), Jitter: func(time.Duration) time.Duration { return 0 },
	}, rootHandshakeNegotiator{established: established, release: release, results: results})
	if err != nil {
		t.Fatal(err)
	}
	carrier := newRankedEnvelopeCarrier(live, handshakes, &rootDurableCarrier{}, time.Second, time.Millisecond, transport.ClockFunc(func() time.Time { return now }))
	envelope := rootRank2Envelope(t, 1, "queued-recovery")
	if disposition := carrier.Send(context.Background(), envelope); disposition != mesh.CarrierAccepted {
		t.Fatalf("first disposition=%v", disposition)
	}
	select {
	case <-established:
	case <-time.After(time.Second):
		t.Fatal("first recovery did not start")
	}
	if disposition := carrier.Send(context.Background(), envelope); disposition != mesh.CarrierAccepted {
		t.Fatalf("second disposition=%v", disposition)
	}
	close(release)
	select {
	case peer := <-established:
		if peer != peerID {
			t.Fatalf("queued recovery peer=%q", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced recovery demand was lost")
	}
	carrier.stop()
	handshakes.Close()
	carrier.wait()
	if err = live.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRankedCarrierDiscardsQueuedDemandAfterSuccessfulRecovery(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	live, err := transport.NewLiveManager(2)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter := &rootLiveCarrier{state: transport.HealthFailed, progress: now}
	peerID := "local@example.test/mesh"
	if err = live.InstallLive(context.Background(), peerID, adapter); err != nil {
		t.Fatal(err)
	}
	established := make(chan string, 1)
	release := make(chan struct{})
	results := make(chan error, 1)
	results <- nil
	handshakes, err := handshake.NewManager(handshake.Config{
		LocalIdentity: "claimed@example.test/mesh", MaximumPeers: 2, MaximumAttempts: 4,
		AttemptTimeout: time.Second, CooldownInitial: time.Millisecond, CooldownMaximum: time.Second,
		Clock: transport.ClockFunc(func() time.Time { return now }), Random: bytes.NewReader(bytes.Repeat([]byte{5}, 64)), Jitter: func(value time.Duration) time.Duration { return value },
	}, rootHandshakeNegotiator{established: established, release: release, results: results})
	if err != nil {
		t.Fatal(err)
	}
	carrier := newRankedEnvelopeCarrier(live, handshakes, &rootDurableCarrier{}, time.Second, time.Millisecond, transport.ClockFunc(func() time.Time { return now }))
	envelope := rootRank2Envelope(t, 1, "successful-coalescing")
	carrier.Send(context.Background(), envelope)
	select {
	case <-established:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	carrier.Send(context.Background(), envelope)
	adapter.mu.Lock()
	adapter.state = transport.HealthHealthy
	adapter.progress = now
	adapter.mu.Unlock()
	close(release)
	deadline := time.Now().Add(time.Second)
	for carrier.recoveryInProgress(peerID) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if carrier.recoveryInProgress(peerID) {
		t.Fatal("successful recovery did not retire its demand")
	}
	select {
	case <-established:
		t.Fatal("successful recovery was followed by an obsolete queued attempt")
	default:
	}
	carrier.stop()
	handshakes.Close()
	carrier.wait()
	if err = live.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type rootPathRefresher struct {
	mu       sync.Mutex
	calls    int
	err      error
	wait     bool
	after    func()
	ctx      context.Context
	expected transport.Transport
}

func (refresher *rootPathRefresher) RecoverInPlace(ctx context.Context, _ string, expected transport.Transport) error {
	refresher.mu.Lock()
	refresher.calls++
	refresher.ctx = ctx
	refresher.expected = expected
	wait, err, after := refresher.wait, refresher.err, refresher.after
	refresher.mu.Unlock()
	if wait {
		<-ctx.Done()
		return ctx.Err()
	}
	if after != nil {
		after()
	}
	return err
}

type rootReplacementRecoverer struct {
	mu      sync.Mutex
	calls   int
	err     error
	ctx     context.Context
	wait    bool
	entered chan struct{}
}

func (recoverer *rootReplacementRecoverer) Recover(ctx context.Context, _ string) error {
	recoverer.mu.Lock()
	recoverer.calls++
	recoverer.ctx = ctx
	err, wait, entered := recoverer.err, recoverer.wait, recoverer.entered
	recoverer.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

type rootRecoveryLiveObserver struct {
	peer    string
	adapter transport.Transport
	present bool
}

func (observer rootRecoveryLiveObserver) LivePeer(peer string) (transport.Transport, bool) {
	return observer.adapter, observer.present && peer == observer.peer
}

func (observer rootRecoveryLiveObserver) IsLivePeer(peer string, expected transport.Transport) bool {
	adapter, ok := observer.LivePeer(peer)
	return ok && adapter == expected
}

type rootRecoveryLiveTransport struct {
	observation     transport.Observation
	restartEligible bool
}

func (*rootRecoveryLiveTransport) Kind() transport.Kind                          { return transport.KindLive }
func (*rootRecoveryLiveTransport) Start(context.Context) error                   { return nil }
func (*rootRecoveryLiveTransport) Send(context.Context, protocol.Envelope) error { return nil }
func (*rootRecoveryLiveTransport) Receive(ctx context.Context) (protocol.Envelope, error) {
	<-ctx.Done()
	return protocol.Envelope{}, ctx.Err()
}
func (adapter *rootRecoveryLiveTransport) Observe() transport.Observation {
	return adapter.observation
}
func (*rootRecoveryLiveTransport) Close(context.Context) error { return nil }
func (adapter *rootRecoveryLiveTransport) RestartEligible() bool {
	return adapter.restartEligible
}

func rootRecoveryObserver(state transport.HealthState, progress time.Time, restartEligible bool) rootRecoveryLiveObserver {
	const peer = "peer@example.test/mesh"
	return rootRecoveryLiveObserver{
		peer: peer, present: true,
		adapter: &rootRecoveryLiveTransport{observation: transport.Observation{State: state, LastProgress: progress}, restartEligible: restartEligible},
	}
}

func TestRank1RecoverySkipsUnprovenInPlacePathAndPreservesCallerBudget(t *testing.T) {
	progress := time.Now().UTC()
	for _, test := range []struct {
		name     string
		observer rootRecoveryLiveObserver
	}{
		{name: "no exact peer", observer: rootRecoveryLiveObserver{peer: "peer@example.test/mesh"}},
		{name: "one sided local open", observer: rootRecoveryObserver(transport.HealthConnecting, time.Time{}, true)},
		{name: "healthy without peer progress", observer: rootRecoveryObserver(transport.HealthHealthy, time.Time{}, true)},
		{name: "unknown", observer: rootRecoveryObserver(transport.HealthUnknown, progress, true)},
		{name: "closed", observer: rootRecoveryObserver(transport.HealthClosed, progress, true)},
		{name: "hard failed", observer: rootRecoveryObserver(transport.HealthFailed, progress, false)},
	} {
		t.Run(test.name, func(t *testing.T) {
			refresh := &rootPathRefresher{}
			replacement := &rootReplacementRecoverer{}
			controller := &rank1RecoveryController{live: test.observer, refresh: refresh, replace: replacement, refreshTimeout: time.Second}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := controller.Recover(ctx, "peer@example.test/mesh"); err != nil {
				t.Fatal(err)
			}
			refresh.mu.Lock()
			refreshCalls := refresh.calls
			refresh.mu.Unlock()
			replacement.mu.Lock()
			replacementCalls, replacementContext := replacement.calls, replacement.ctx
			replacement.mu.Unlock()
			if refreshCalls != 0 || replacementCalls != 1 {
				t.Fatalf("refresh=%d replacement=%d", refreshCalls, replacementCalls)
			}
			if replacementContext != ctx {
				t.Fatal("direct replacement did not receive the full caller context")
			}
		})
	}
}

func TestRank1RecoveryPeerConfirmedRestartEligibleStatesRefreshFirst(t *testing.T) {
	progress := time.Now().UTC()
	for _, test := range []struct {
		name            string
		state           transport.HealthState
		restartEligible bool
	}{
		{name: "connecting after prior progress", state: transport.HealthConnecting, restartEligible: true},
		{name: "healthy", state: transport.HealthHealthy, restartEligible: true},
		{name: "disconnected", state: transport.HealthDisconnected, restartEligible: true},
		{name: "restartable ICE failure", state: transport.HealthFailed, restartEligible: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			refresh := &rootPathRefresher{err: transport.ErrUnavailable}
			replacement := &rootReplacementRecoverer{}
			controller := &rank1RecoveryController{
				live:    rootRecoveryObserver(test.state, progress, test.restartEligible),
				refresh: refresh, replace: replacement, refreshTimeout: time.Second,
			}
			if err := controller.Recover(context.Background(), "peer@example.test/mesh"); err != nil {
				t.Fatal(err)
			}
			refresh.mu.Lock()
			refreshCalls := refresh.calls
			refresh.mu.Unlock()
			replacement.mu.Lock()
			replacementCalls := replacement.calls
			replacement.mu.Unlock()
			if refreshCalls != 1 || replacementCalls != 1 {
				t.Fatalf("refresh=%d replacement=%d", refreshCalls, replacementCalls)
			}
		})
	}
}

func TestRank1RecoveryRefreshesBeforeBoundedReplacement(t *testing.T) {
	for _, test := range []struct {
		name             string
		refresh          *rootPathRefresher
		freshHealth      bool
		wantReplacements int
	}{
		{name: "refresh proves fresh health", refresh: &rootPathRefresher{}, freshHealth: true},
		{name: "refresh without fresh health", refresh: &rootPathRefresher{}, wantReplacements: 1},
		{name: "refresh rejects", refresh: &rootPathRefresher{err: transport.ErrUnavailable}, wantReplacements: 1},
		{name: "refresh times out", refresh: &rootPathRefresher{wait: true}, wantReplacements: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := time.Now().UTC()
			observer := rootRecoveryObserver(transport.HealthDisconnected, progress, true)
			if test.freshHealth {
				adapter := observer.adapter.(*rootRecoveryLiveTransport)
				test.refresh.after = func() {
					adapter.observation = transport.Observation{State: transport.HealthHealthy, LastProgress: progress.Add(time.Second)}
				}
			}
			replacement := &rootReplacementRecoverer{}
			controller := &rank1RecoveryController{live: observer, refresh: test.refresh, replace: replacement, refreshTimeout: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := controller.Recover(ctx, "peer@example.test/mesh"); err != nil {
				t.Fatal(err)
			}
			replacement.mu.Lock()
			calls := replacement.calls
			replacement.mu.Unlock()
			if calls != test.wantReplacements {
				t.Fatalf("replacement calls=%d want %d", calls, test.wantReplacements)
			}
		})
	}
}

func TestRank1RecoveryFailedRefreshFallbackHonorsCallerCancellation(t *testing.T) {
	refresh := &rootPathRefresher{err: transport.ErrUnavailable}
	replacement := &rootReplacementRecoverer{wait: true, entered: make(chan struct{}, 1)}
	controller := &rank1RecoveryController{
		live:    rootRecoveryObserver(transport.HealthDisconnected, time.Now().UTC(), true),
		refresh: refresh, replace: replacement, refreshTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- controller.Recover(ctx, "peer@example.test/mesh") }()
	select {
	case <-replacement.entered:
	case <-time.After(time.Second):
		t.Fatal("replacement did not start after failed refresh")
	}
	replacement.mu.Lock()
	replacementContext := replacement.ctx
	replacement.mu.Unlock()
	if replacementContext != ctx {
		t.Fatal("fallback replacement did not retain the caller context")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recover()=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not stop replacement")
	}
}

func TestRank1RecoveryPreCancelledCallerStartsNoTier(t *testing.T) {
	refresh := &rootPathRefresher{wait: true}
	replacement := &rootReplacementRecoverer{}
	controller := &rank1RecoveryController{
		live:    rootRecoveryObserver(transport.HealthDisconnected, time.Now().UTC(), true),
		refresh: refresh, replace: replacement, refreshTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.Recover(ctx, "peer@example.test/mesh"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover()=%v", err)
	}
	refresh.mu.Lock()
	refreshCalls := refresh.calls
	refresh.mu.Unlock()
	replacement.mu.Lock()
	replacementCalls := replacement.calls
	replacement.mu.Unlock()
	if refreshCalls != 0 || replacementCalls != 0 {
		t.Fatalf("cancelled recovery started tiers: refresh=%d replacement=%d", refreshCalls, replacementCalls)
	}
}

type rootMutableRecoveryObserver struct {
	mu             sync.Mutex
	peer           string
	adapter        transport.Transport
	present        bool
	replaceOnCheck transport.Transport
	removeCalls    int
	removed        transport.Transport
}

func (observer *rootMutableRecoveryObserver) LivePeer(peer string) (transport.Transport, bool) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.adapter, observer.present && peer == observer.peer
}

func (observer *rootMutableRecoveryObserver) IsLivePeer(peer string, expected transport.Transport) bool {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.replaceOnCheck != nil {
		observer.adapter = observer.replaceOnCheck
		observer.replaceOnCheck = nil
	}
	return observer.present && peer == observer.peer && observer.adapter == expected
}

func (observer *rootMutableRecoveryObserver) replace(adapter transport.Transport) {
	observer.mu.Lock()
	observer.adapter = adapter
	observer.mu.Unlock()
}

func (observer *rootMutableRecoveryObserver) RemoveLiveIf(ctx context.Context, peer string, expected transport.Transport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.removeCalls++
	if observer.present && peer == observer.peer && observer.adapter == expected {
		observer.removed = observer.adapter
		observer.adapter = nil
		observer.present = false
	}
	return nil
}

type rootControlledPathRefresher struct {
	mu       sync.Mutex
	calls    int
	expected transport.Transport
	entered  chan struct{}
	release  chan struct{}
}

func (refresher *rootControlledPathRefresher) RecoverInPlace(ctx context.Context, _ string, expected transport.Transport) error {
	refresher.mu.Lock()
	refresher.calls++
	refresher.expected = expected
	entered, release := refresher.entered, refresher.release
	refresher.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRank1RecoveryReplacementBetweenObservationAndRefreshSkipsStaleToken(t *testing.T) {
	const peer = "peer@example.test/mesh"
	old := &rootRecoveryLiveTransport{observation: transport.Observation{State: transport.HealthHealthy, LastProgress: time.Now().UTC()}, restartEligible: true}
	replacementAdapter := &rootRecoveryLiveTransport{observation: old.observation, restartEligible: true}
	observer := &rootMutableRecoveryObserver{peer: peer, adapter: old, present: true, replaceOnCheck: replacementAdapter}
	refresh := &rootPathRefresher{}
	replacement := &rootReplacementRecoverer{}
	controller := &rank1RecoveryController{live: observer, refresh: refresh, replace: replacement, refreshTimeout: time.Second}
	if err := controller.Recover(context.Background(), peer); err != nil {
		t.Fatal(err)
	}
	refresh.mu.Lock()
	refreshCalls := refresh.calls
	refresh.mu.Unlock()
	replacement.mu.Lock()
	replacementCalls := replacement.calls
	replacement.mu.Unlock()
	if refreshCalls != 0 || replacementCalls != 1 {
		t.Fatalf("refresh=%d replacement=%d", refreshCalls, replacementCalls)
	}
}

func TestRank1RecoveryReplacementDuringRefreshCannotPublishStaleSuccess(t *testing.T) {
	const peer = "peer@example.test/mesh"
	old := &rootRecoveryLiveTransport{observation: transport.Observation{State: transport.HealthHealthy, LastProgress: time.Now().UTC()}, restartEligible: true}
	replacementAdapter := &rootRecoveryLiveTransport{observation: old.observation, restartEligible: true}
	observer := &rootMutableRecoveryObserver{peer: peer, adapter: old, present: true}
	refresh := &rootControlledPathRefresher{entered: make(chan struct{}, 1), release: make(chan struct{})}
	replacement := &rootReplacementRecoverer{}
	controller := &rank1RecoveryController{live: observer, refresh: refresh, replace: replacement, refreshTimeout: time.Second}
	result := make(chan error, 1)
	go func() { result <- controller.Recover(context.Background(), peer) }()
	select {
	case <-refresh.entered:
	case <-time.After(time.Second):
		t.Fatal("token-bound refresh did not start")
	}
	observer.replace(replacementAdapter)
	close(refresh.release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale refresh did not fall back to replacement")
	}
	refresh.mu.Lock()
	expected, refreshCalls := refresh.expected, refresh.calls
	refresh.mu.Unlock()
	replacement.mu.Lock()
	replacementCalls := replacement.calls
	replacement.mu.Unlock()
	if expected != old || refreshCalls != 1 || replacementCalls != 1 {
		t.Fatalf("expected=%p old=%p refresh=%d replacement=%d", expected, old, refreshCalls, replacementCalls)
	}
}

func TestRank1RecoveryFailedRefreshRetiresExactLinkBeforeReplacement(t *testing.T) {
	const peer = "peer@example.test/mesh"
	old := &rootRecoveryLiveTransport{observation: transport.Observation{State: transport.HealthDisconnected, LastProgress: time.Now().UTC()}, restartEligible: true}
	observer := &rootMutableRecoveryObserver{peer: peer, adapter: old, present: true}
	refresh := &rootPathRefresher{err: transport.ErrUnavailable}
	replacement := &rootReplacementRecoverer{}
	controller := &rank1RecoveryController{live: observer, refresh: refresh, replace: replacement, refreshTimeout: time.Second}
	if err := controller.Recover(context.Background(), peer); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	removeCalls, removed, present := observer.removeCalls, observer.removed, observer.present
	observer.mu.Unlock()
	replacement.mu.Lock()
	replacementCalls := replacement.calls
	replacement.mu.Unlock()
	if removeCalls != 1 || removed != old || present || replacementCalls != 1 {
		t.Fatalf("remove=%d removed=%p old=%p present=%t replacement=%d", removeCalls, removed, old, present, replacementCalls)
	}
}

func TestRank1RecoveryCancellationStopsTokenBoundRefreshWithoutReplacement(t *testing.T) {
	const peer = "peer@example.test/mesh"
	old := &rootRecoveryLiveTransport{observation: transport.Observation{State: transport.HealthDisconnected, LastProgress: time.Now().UTC()}, restartEligible: true}
	observer := &rootMutableRecoveryObserver{peer: peer, adapter: old, present: true}
	refresh := &rootControlledPathRefresher{entered: make(chan struct{}, 1), release: make(chan struct{})}
	replacement := &rootReplacementRecoverer{}
	controller := &rank1RecoveryController{live: observer, refresh: refresh, replace: replacement, refreshTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- controller.Recover(ctx, peer) }()
	select {
	case <-refresh.entered:
	case <-time.After(time.Second):
		t.Fatal("token-bound refresh did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recover()=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop token-bound refresh")
	}
	refresh.mu.Lock()
	expected := refresh.expected
	refresh.mu.Unlock()
	replacement.mu.Lock()
	replacementCalls := replacement.calls
	replacement.mu.Unlock()
	if expected != old || replacementCalls != 0 {
		t.Fatalf("expected=%p old=%p replacement=%d", expected, old, replacementCalls)
	}
}
