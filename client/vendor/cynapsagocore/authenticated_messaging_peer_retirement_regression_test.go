package cynapsagocore

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

const (
	peerRetirementPeerA = "peer@example.test"
	peerRetirementPeerB = "peer-b@example.test"
	peerRetirementPath  = "/incoming"
)

func TestRank2PumpContinuesAfterAcceptedPeerLaneRetirement(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := peerRetirementCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-pump-auth")

	qaStageBAllow(t, core, "qa-pump-policy", peerRetirementPath, peerRetirementPeerA)
	warmup := qaStageBInboundEvent(t, "transport-warmup")
	warmup.Stanza.Data = append(warmup.Stanza.Data, 0xff)
	session.emit(warmup)

	for sequence := 1; sequence <= 10; sequence++ {
		body := fmt.Sprintf("peer-a-%02d", sequence)
		session.emit(qaStageBInboundEvent(t, body))
		received := waitPeerRetirementDelivery(t, core, sequence)
		message, ok := received.Payload.(v1.MessageReceivedEvent)
		native, nativeOK := message.Payload.Value.(v1.NativePayload)
		if !ok || !nativeOK || message.FromAgentID != peerRetirementPeerA || native.Path != peerRetirementPath || string(native.Body) != body {
			t.Fatalf("peer A delivery %d = %#v", sequence, received)
		}
		acceptPeerRetirementDelivery(t, core, received.ID, sequence)
	}

	// Removing A uses the real peer-registry terminal lane seam. Its owner
	// fences and joins that lane and closes only A's live transport; the sole
	// mesh-wide Rank2 pump must remain available for another peer.
	session.replaceGroup(rank2xmpp.AuthoritySnapshot{Members: []string{
		"agent@example.test/mesh-one",
		peerRetirementPeerB + "/mesh-one",
	}}, nil)
	refresh := qaStageBCompletion(t, core, v1.MeshMembershipRefreshCommand{
		CommandBase: qaStageBBase("peer-retirement-refresh"),
	})
	if !refresh.OK || refresh.Error != nil {
		t.Fatalf("retire peer A lane = %#v", refresh)
	}
	qaStageBAllow(t, core, "peer-retirement-policy-b", peerRetirementPath, peerRetirementPeerB)

	session.emit(peerRetirementInboundEvent(t, peerRetirementPeerB, "peer-b-after-a-retired"))
	received := waitPeerRetirementDelivery(t, core, 11)
	message, ok := received.Payload.(v1.MessageReceivedEvent)
	native, nativeOK := message.Payload.Value.(v1.NativePayload)
	if !ok || !nativeOK || message.FromAgentID != peerRetirementPeerB || native.Path != peerRetirementPath || string(native.Body) != "peer-b-after-a-retired" {
		t.Fatalf("peer B delivery after A retirement = %#v", received)
	}
	acceptPeerRetirementDelivery(t, core, received.ID, 11)
	qaStageBCloseCore(t, core)
}

func waitPeerRetirementDelivery(t *testing.T, core *Core, sequence int) v1.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		event, err := core.NextEvent(ctx)
		if err != nil {
			t.Fatalf("wait for Rank2 delivery sequence %d: %v", sequence, err)
		}
		if event.Name == v1.EventMessageReceived {
			return event
		}
	}
}

func acceptPeerRetirementDelivery(t *testing.T, core *Core, eventID v1.EventID, sequence int) {
	t.Helper()
	completion := qaStageBCompletion(t, core, v1.DeliveryAcceptCommand{
		CommandBase: qaStageBBase(fmt.Sprintf("retire-accept-%02d", sequence)),
		EventID:     eventID,
	})
	if !completion.OK || completion.Error != nil {
		t.Fatalf("accept Rank2 delivery %d = %#v", sequence, completion)
	}
}

func peerRetirementCore(t *testing.T, dialer rank2xmpp.Dialer) *Core {
	t.Helper()
	boundary, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	factory := newBuiltInConnectivityFactory(func(endpoint string, _ sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		if endpoint != "mesh.example.test:5222" {
			return nil, rank2xmpp.ErrInvalidConfig
		}
		return dialer, nil
	})
	core, err := newCoreWithConnectivity(Config{QueueLimit: 16, PayloadLimit: 1 << 20}, boundary, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err = core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return core
}

func peerRetirementInboundEvent(t *testing.T, senderBare, body string) rank2xmpp.Event {
	t.Helper()
	canonical, err := payload.SerializeCanonical(model.Payload{Value: model.NativePayload{
		ContentType: "application/octet-stream", Path: peerRetirementPath, Body: []byte(body),
	}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := protocol.NewInlinePayload(payload.ProfileNative, canonical)
	if err != nil {
		t.Fatal(err)
	}
	sender := senderBare + "/mesh-one"
	conversationID, err := conversation.DeriveID("mesh-one", "agent@example.test/mesh-one", sender)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID:   conversationID,
		Sender:           sender,
		Recipient:        "agent@example.test/mesh-one",
		MeshID:           "mesh-one",
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Date(2026, 8, 13, 12, 0, 0, 123456000, time.UTC),
		ClockUncertainty: time.Millisecond,
		Payload:          descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return rank2xmpp.Event{Kind: rank2xmpp.EventStanza, Stanza: rank2xmpp.Stanza{
		Kind: rank2xmpp.StanzaEnvelope, From: envelope.Sender, To: envelope.Recipient,
		MeshID: envelope.MeshID, MessageID: envelope.MessageID, Data: encoded,
	}}
}

type cancellationAcceptanceSource struct {
	inbound rank2xmpp.AuthenticatedInbound
	once    bool
}

func (source *cancellationAcceptanceSource) ReceiveAuthenticatedForQuarantine(ctx context.Context) (rank2xmpp.AuthenticatedInbound, error) {
	if !source.once {
		source.once = true
		return source.inbound, nil
	}
	<-ctx.Done()
	return rank2xmpp.AuthenticatedInbound{}, ctx.Err()
}

type cancellationAcceptanceReceiver struct{ entered chan struct{} }

func (receiver cancellationAcceptanceReceiver) Receive(context.Context, mesh.AuthenticatedProvenance, protocol.Envelope) *mesh.Failure {
	return &mesh.Failure{Code: mesh.FailureInternal}
}

func (receiver cancellationAcceptanceReceiver) ReceiveRank2(ctx context.Context, _ mesh.AuthenticatedProvenance, _ protocol.Envelope) (bool, *mesh.Failure) {
	close(receiver.entered)
	<-ctx.Done()
	return false, &mesh.Failure{Code: mesh.FailureCancelled}
}

func TestRank2PumpCancellationReturnsWithoutStrandingReceiver(t *testing.T) {
	inbound := rank2xmpp.AuthenticatedInbound{
		AuthenticatedSender: "peer@example.test/mesh",
		Envelope:            rootRank2Envelope(t, 1, "cancel-with-owned-acceptance"),
	}
	source := &cancellationAcceptanceSource{inbound: inbound}
	receiver := cancellationAcceptanceReceiver{entered: make(chan struct{})}
	pump := newRank2InboundPump(source, receiver, "local@example.test/mesh", "mesh")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *mesh.Failure, 1)
	go func() { done <- pump.Run(ctx) }()
	select {
	case <-receiver.entered:
	case <-time.After(time.Second):
		t.Fatal("Rank2 receiver did not assume inbound ownership")
	}
	cancel()
	select {
	case failure := <-done:
		if failure == nil || failure.Code != mesh.FailureCancelled {
			t.Fatalf("cancelled pump = %#v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Rank2 pump stranded the acceptance decision")
	}
}
