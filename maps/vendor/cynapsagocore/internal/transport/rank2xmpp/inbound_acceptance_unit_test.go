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
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func inboundAcceptanceWire(t *testing.T, sequence uint64) (string, protocol.Envelope) {
	t.Helper()
	envelope := ingressEnvelope(t, sequence, "deferred-sm-acceptance")
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	frame, err := EncodeStanzaFrame(Stanza{
		Kind: StanzaEnvelope, From: ingressRemote, To: ingressLocal,
		MeshID: ingressMesh, MessageID: envelope.MessageID, Data: encoded,
	}, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return `<message xmlns="jabber:client" from="` + ingressRemote + `" to="` + ingressLocal + `" type="chat" id="` + envelope.MessageID + `">` + string(frame) + `</message>`, envelope
}

func newInboundAcceptanceTestSession(t *testing.T, capacity int) (*melliumSession, *StreamManagement) {
	t.Helper()
	management, err := NewStreamManagement(capacity, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	return &melliumSession{
		config: MelliumConfig{
			MaximumFrameBytes: 1 << 20,
			StanzaBudgetBytes: 1 << 20,
		},
		username:   "local@example.test",
		meshID:     ingressMesh,
		management: management,
		events:     make(chan Event, capacity),
	}, management
}

func handleInboundAcceptanceXML(t *testing.T, session *melliumSession, ctx context.Context, wire string) (string, error) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(wire))
	token, err := decoder.Token()
	if err != nil {
		t.Fatal(err)
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		t.Fatalf("outer start=%#v", token)
	}
	var output bytes.Buffer
	encoder := xml.NewEncoder(&output)
	tokens := &qaTokenReadEncoder{decoder: decoder, encoder: encoder}
	err = session.handleElement(ctx, tokens, &start)
	if flushErr := encoder.Flush(); err == nil && flushErr != nil {
		err = flushErr
	}
	return output.String(), err
}

func handleDeferredEnvelope(t *testing.T, session *melliumSession, ctx context.Context, wire string) Event {
	t.Helper()
	if output, err := handleInboundAcceptanceXML(t, session, ctx, wire); err != nil || output != "" {
		t.Fatalf("nonblocking envelope parse output=%q err=%v", output, err)
	}
	select {
	case event := <-session.events:
		return event
	default:
		t.Fatal("parser returned without bounded event handoff")
		return Event{}
	}
}

func decodeInboundAcceptanceEvent(t *testing.T, event *Event) AuthenticatedInbound {
	t.Helper()
	inbound, ok := decodeAuthenticatedInboundOwned(&event.Stanza)
	if !ok {
		clearEventOwned(event)
		t.Fatal("transport-authenticated event did not decode")
	}
	event.Stanza = Stanza{}
	return inbound
}

func smRequestWire() string { return `<r xmlns="urn:xmpp:sm:3"/>` }

func smAckHandled(t *testing.T, output string) uint32 {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(output))
	token, err := decoder.Token()
	start, ok := token.(xml.StartElement)
	if err != nil || !ok || start.Name != (xml.Name{Space: streamManagementNamespace, Local: "a"}) {
		t.Fatalf("SM ack start=%#v err=%v", token, err)
	}
	handled, err := decodeSMAck(decoder, start)
	if err != nil {
		t.Fatal(err)
	}
	if token, err = decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		t.Fatalf("SM ack trailing token=%#v err=%v", token, err)
	}
	return handled
}

func TestEnvelopeParserReturnsBeforeApplicationAcceptanceAndAdvancesContiguousPrefix(t *testing.T) {
	session, management := newInboundAcceptanceTestSession(t, 4)
	wireOne, _ := inboundAcceptanceWire(t, 1)
	wireTwo, _ := inboundAcceptanceWire(t, 2)
	eventOne := handleDeferredEnvelope(t, session, context.Background(), wireOne)
	eventTwo := handleDeferredEnvelope(t, session, context.Background(), wireTwo)
	inboundOne := decodeInboundAcceptanceEvent(t, &eventOne)
	inboundTwo := decodeInboundAcceptanceEvent(t, &eventTwo)
	defer clearAuthenticatedInbound(&inboundOne)
	defer clearAuthenticatedInbound(&inboundTwo)

	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("bounded parser handoff advanced h=%d before SDK acceptance", handled)
	}
	if err := inboundTwo.Accept(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("out-of-order acceptance advanced h=%d past unresolved prefix", handled)
	}
	management.MarkHandledInbound() // immediately handled control after both envelopes
	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("later control advanced h=%d past unresolved prefix", handled)
	}

	// Copy only the capability: envelope bytes and the inbound lease have one
	// owner, while value-copied SDK handles must still converge on one decision.
	copyOfFirst := AuthenticatedInbound{inboundAccept: inboundOne.inboundAccept}
	acceptedCopy := make(chan error, 1)
	go func() { acceptedCopy <- copyOfFirst.Accept(context.Background()) }()
	if err := inboundOne.Accept(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-acceptedCopy; err != nil {
		t.Fatalf("copied acceptance did not converge: %v", err)
	}
	copyOfFirst.Reject()
	if handled := management.HandledInbound(); handled != 3 {
		t.Fatalf("contiguous accepted prefix h=%d want 3", handled)
	}
}

func TestPendingEnvelopeDoesNotBlockSMRequestOrXEP0199Ping(t *testing.T) {
	session, management := newInboundAcceptanceTestSession(t, 4)
	wire, _ := inboundAcceptanceWire(t, 1)
	event := handleDeferredEnvelope(t, session, context.Background(), wire)
	inbound := decodeInboundAcceptanceEvent(t, &event)
	defer clearAuthenticatedInbound(&inbound)

	ping := strings.ReplaceAll(xep0199ValidPing, "a@example.test/mesh", ingressLocal)
	pingOutput, err := handleInboundAcceptanceXML(t, session, context.Background(), ping)
	if err != nil {
		t.Fatalf("ping while acceptance pending: %v", err)
	}
	if !strings.Contains(pingOutput, `type="result"`) || !strings.Contains(pingOutput, `<r xmlns="urn:xmpp:sm:3"></r>`) {
		t.Fatalf("pending-acceptance ping response=%q", pingOutput)
	}
	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("ping overtook pending envelope with h=%d", handled)
	}

	ackOutput, err := handleInboundAcceptanceXML(t, session, context.Background(), smRequestWire())
	if err != nil {
		t.Fatalf("SM request while acceptance pending: %v", err)
	}
	if handled := smAckHandled(t, ackOutput); handled != 0 {
		t.Fatalf("pending SM ack h=%d want 0", handled)
	}

	if err = inbound.Accept(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handled := management.HandledInbound(); handled != 2 {
		t.Fatalf("accepted envelope plus ping h=%d want 2", handled)
	}
	ackOutput, err = handleInboundAcceptanceXML(t, session, context.Background(), smRequestWire())
	if err != nil {
		t.Fatal(err)
	}
	if handled := smAckHandled(t, ackOutput); handled != 2 {
		t.Fatalf("post-acceptance SM ack h=%d want 2", handled)
	}
}

func TestDeferredInboundCapacityIsBoundedWithoutChargingImmediateControls(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("bounded", true); err != nil {
		t.Fatal(err)
	}
	first, err := management.DeferHandledInbound(nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := management.DeferHandledInbound(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = management.DeferHandledInbound(nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third deferred envelope error=%v want capacity", err)
	}
	for range 100 {
		management.MarkHandledInbound()
	}
	if management.HandledInbound() != 0 {
		t.Fatal("immediate controls crossed a full deferred prefix")
	}
	if !second.decide(true) || management.HandledInbound() != 0 {
		t.Fatal("second acceptance overtook first")
	}
	if !first.decide(true) || management.HandledInbound() != 102 {
		t.Fatalf("compressed control suffix h=%d want 102", management.HandledInbound())
	}
}

func TestRejectedEnvelopeDoesNotAdvanceHAndFailStopsExactGeneration(t *testing.T) {
	session, management := newInboundAcceptanceTestSession(t, 2)
	local, remote := net.Pipe()
	defer remote.Close()
	session.conn = local
	session.generation = 7
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	wire, _ := inboundAcceptanceWire(t, 1)
	event := handleDeferredEnvelope(t, session, ctx, wire)
	inbound := decodeInboundAcceptanceEvent(t, &event)
	inbound.Reject()
	clearAuthenticatedInbound(&inbound)

	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("rejected envelope advanced h=%d", handled)
	}
	if _, handled, resumable := management.ResumeState(); handled != 0 || resumable {
		t.Fatalf("rejected resume state h=%d resumable=%t", handled, resumable)
	}
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := remote.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("rejected exact generation remained open: %v", err)
	}
}

func TestFailedEnvelopeEventHandoffRejectsAndFencesResumption(t *testing.T) {
	session, management := newInboundAcceptanceTestSession(t, 1)
	session.events = make(chan Event)
	local, remote := net.Pipe()
	defer remote.Close()
	session.conn = local
	session.generation = 9
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = context.WithValue(ctx, melliumSessionGenerationKey{}, uint64(9))
	wire, _ := inboundAcceptanceWire(t, 1)

	if output, err := handleInboundAcceptanceXML(t, session, ctx, wire); !errors.Is(err, context.Canceled) || output != "" {
		t.Fatalf("failed event handoff output=%q err=%v", output, err)
	}
	if handled := management.HandledInbound(); handled != 0 {
		t.Fatalf("failed event handoff advanced h=%d", handled)
	}
	if _, handled, resumable := management.ResumeState(); handled != 0 || resumable {
		t.Fatalf("failed handoff resume state h=%d resumable=%t", handled, resumable)
	}
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := remote.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("failed handoff generation remained open: %v", err)
	}
}

func TestSessionLossAloneDoesNotAdvancePendingHAndCleanResetInvalidatesCapability(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("loss", true); err != nil {
		t.Fatal(err)
	}
	acceptance, err := management.DeferHandledInbound(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, handled, resumable := management.ResumeState(); handled != 0 || !resumable {
		t.Fatalf("pending loss state h=%d resumable=%t", handled, resumable)
	}
	management.ResumeRejected()
	if acceptance.decide(true) {
		t.Fatal("retired clean-session capability advanced replacement h")
	}
	if management.HandledInbound() != 0 {
		t.Fatal("clean reset retained pending h")
	}
}

func TestDeferredInboundConcurrentCopiedAcceptancesConvergeWithoutRaces(t *testing.T) {
	const count = 128
	management, err := NewStreamManagement(count, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("race", true); err != nil {
		t.Fatal(err)
	}
	acceptances := make([]*inboundAcceptance, count)
	for i := range acceptances {
		acceptances[i], err = management.DeferHandledInbound(nil)
		if err != nil {
			t.Fatal(err)
		}
		management.MarkHandledInbound()
	}

	var workers sync.WaitGroup
	errorsSeen := make(chan error, count*2)
	for i := len(acceptances) - 1; i >= 0; i-- {
		for range 2 {
			workers.Add(1)
			acceptance := acceptances[i]
			go func() {
				defer workers.Done()
				if err := acceptance.accept(context.Background()); err != nil {
					errorsSeen <- err
				}
			}()
		}
	}
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("copied concurrent acceptance: %v", err)
	}
	if handled := management.HandledInbound(); handled != count*2 {
		t.Fatalf("concurrent contiguous h=%d want %d", handled, count*2)
	}
}

func TestEnvelopeAcceptanceSurvivesProductionClientHandoff(t *testing.T) {
	session, management := newInboundAcceptanceTestSession(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingress := newIngressGeneration(1, session, ingressLocal, nil, ctx)
	defer ingress.cancel()
	client := &Client{
		config:  Config{Auth: Authentication{MeshID: ingressMesh}},
		clock:   transport.NewCalibratedClock(nil),
		started: true, state: DurableLive, session: session, ingress: ingress,
		ctx: ctx, generation: 1, sessionEpoch: 1, membershipReady: true,
		membershipReadySignal: make(chan struct{}),
		inbound:               make(chan AuthenticatedInbound, 1),
	}

	wire, want := inboundAcceptanceWire(t, 1)
	event := handleDeferredEnvelope(t, session, context.Background(), wire)
	handoffDone := make(chan struct{})
	go func() {
		client.handleIngressEvent(ingress, event)
		close(handoffDone)
	}()

	inbound, err := client.ReceiveAuthenticatedForQuarantine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if inbound.AuthenticatedSender != ingressRemote || inbound.Envelope.MessageID != want.MessageID {
		clearAuthenticatedInbound(&inbound)
		t.Fatalf("inbound provenance/message = %q/%q", inbound.AuthenticatedSender, inbound.Envelope.MessageID)
	}
	if err = inbound.Accept(context.Background()); err != nil {
		clearAuthenticatedInbound(&inbound)
		t.Fatal(err)
	}
	clearAuthenticatedInbound(&inbound)
	select {
	case <-handoffDone:
	case <-time.After(time.Second):
		t.Fatal("client event handoff did not finish")
	}
	if handled := management.HandledInbound(); handled != 1 {
		t.Fatalf("terminal client handoff h=%d want 1", handled)
	}
}
