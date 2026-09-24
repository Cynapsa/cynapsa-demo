package cynapsagocore

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/conversation"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

const qaStageBSessionID v1.SDKSessionID = "qa-session"

func (session *qaStageBEndpointSession) replaceGroup(snapshot rank2xmpp.AuthoritySnapshot, err error) {
	snapshot.Members = append([]string(nil), snapshot.Members...)
	session.mu.Lock()
	session.group = snapshot
	session.groupErr = err
	session.mu.Unlock()
}

func (session *qaStageBEndpointSession) sentSnapshot() []rank2xmpp.Stanza {
	session.mu.Lock()
	defer session.mu.Unlock()
	result := append([]rank2xmpp.Stanza(nil), session.sent...)
	for index := range result {
		result[index].Data = append([]byte(nil), result[index].Data...)
	}
	return result
}

func (session *qaStageBEndpointSession) emit(event rank2xmpp.Event) {
	event.Stanza.Data = append([]byte(nil), event.Stanza.Data...)
	session.events <- event
}

func qaStageBBase(id string) v1.CommandBase {
	return v1.CommandBase{CommandID: v1.CommandID(id), SDKSessionID: qaStageBSessionID}
}

func qaStageBCompletion(t *testing.T, core *Core, command v1.Command) v1.Completion {
	t.Helper()
	admission, err := core.Submit(context.Background(), command)
	if err != nil || !admission.Accepted || admission.Error != nil {
		t.Fatalf("submit %s = %#v, %v", command.Name(), admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("complete %s = %#v, %v", command.Name(), completion, err)
	}
	return completion
}

func qaStageBAuthenticate(t *testing.T, core *Core, id string) v1.AuthResult {
	t.Helper()
	completion := qaStageBCompletion(t, core, qaStageBAuthCommand(v1.CommandAuthConnect, id))
	result, ok := completion.Result.(v1.AuthResult)
	if !completion.OK || completion.Error != nil || !ok {
		t.Fatalf("authenticate = %#v", completion)
	}
	return result
}

func qaStageBAllow(t *testing.T, core *Core, id, path, peer string) {
	t.Helper()
	completion := qaStageBCompletion(t, core, v1.PolicySetCommand{
		CommandBase: qaStageBBase(id),
		Rules:       []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path, AgentID: v1.AgentID(peer)}},
	})
	if !completion.OK || completion.Error != nil {
		t.Fatalf("policy allow = %#v", completion)
	}
}

func TestStageBAcceptancePublicMessagingHandshakesOnFirstSend(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-messaging-auth")

	list := qaStageBCompletion(t, core, v1.MeshListCommand{CommandBase: qaStageBBase("qa-mesh-list")})
	meshes, ok := list.Result.(v1.MeshListResult)
	if !list.OK || list.Error != nil || !ok || len(meshes.Meshes) != 1 || meshes.Meshes[0].MeshID != "mesh-one" || !meshes.Meshes[0].Active {
		t.Fatalf("mesh.list = %#v", list)
	}
	refresh := qaStageBCompletion(t, core, v1.MeshMembershipRefreshCommand{CommandBase: qaStageBBase("qa-mesh-refresh")})
	if !refresh.OK || refresh.Error != nil {
		t.Fatalf("mesh refresh = %#v", refresh)
	}
	if phases := session.observedPhases(); len(phases) != 5 || phases[4] != "time" {
		t.Fatalf("login/list/refresh unexpectedly downloaded membership: %v", phases)
	}
	if bare, exact, _ := session.peerQuerySnapshot(); len(bare) != 0 || len(exact) != 0 {
		t.Fatalf("peer handshake before first contact: bare=%v exact=%v", bare, exact)
	}

	qaStageBAllow(t, core, "qa-send-policy", "/messages", "peer@example.test")
	send := qaStageBCompletion(t, core, v1.MessageSendCommand{
		CommandBase: qaStageBBase("qa-send"),
		To:          "peer@example.test",
		Payload: v1.Payload{Value: v1.NativePayload{
			ContentType: "application/octet-stream", Path: "/messages", Body: []byte("first-contact"),
		}},
	})
	result, ok := send.Result.(v1.SendResult)
	if !send.OK || send.Error != nil || !ok || !result.Accepted || result.MessageID == "" || result.ConversationID == "" {
		t.Fatalf("message.send = %#v", send)
	}
	// First send resolves the bare recipient. Rank1 establishment may run
	// alongside it and must verify the exact installation before connecting.
	if bare, exact, _ := session.peerQuerySnapshot(); len(bare) != 1 || bare[0] != "peer@example.test" || len(exact) > 1 || (len(exact) == 1 && exact[0] != "peer@example.test/r2.install-peer.nonce-1") {
		t.Fatalf("first send handshake = bare=%v exact=%v", bare, exact)
	}

	var application rank2xmpp.Stanza
	for _, stanza := range session.sentSnapshot() {
		if stanza.Kind == rank2xmpp.StanzaEnvelope {
			application = stanza
		}
	}
	if application.Kind != rank2xmpp.StanzaEnvelope || application.From != "agent@example.test/mesh-one" || application.To != "peer@example.test/r2.install-peer.nonce-1" || application.MeshID != "mesh-one" {
		t.Fatalf("Rank2 projection = %#v", application)
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := codec.Decode(application.Data)
	if err != nil {
		t.Fatalf("decode Rank2 envelope: %v", err)
	}
	if envelope.MessageID != string(result.MessageID) || envelope.ConversationID != string(result.ConversationID) || envelope.Sender != application.From || envelope.Recipient != application.To {
		t.Fatalf("server-authorized envelope = %#v", envelope)
	}
	qaStageBCloseCore(t, core)
}

func TestStageBRank1RejectsCachedInstallationWhenExactServerAuthorizationFails(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-rank1-exact-auth")

	core.diagnosticsFeed.mu.RLock()
	source := core.diagnosticsFeed.diagnostics
	core.diagnosticsFeed.mu.RUnlock()
	service, ok := source.(*authenticatedMessagingService)
	if !ok || service.messaging == nil || service.rank1 == nil || service.rank1.authority == nil {
		t.Fatalf("authenticated messaging service unavailable: %T", source)
	}

	const oldFull = "peer@example.test/r2.install-peer.nonce-1"
	if failure := service.messaging.AuthorizeExactPeer(context.Background(), oldFull); failure != nil {
		t.Fatalf("authorize initial exact installation: %#v", failure)
	}
	if failure := service.messaging.AuthorizeCurrentPeer(oldFull); failure != nil {
		t.Fatalf("initial installation not cached: %#v", failure)
	}
	// The server now selects installation B for bare-name resolution, but A
	// is no longer authorized. A bare lookup must not validate A's Rank1 link.
	session.setPeer("peer@example.test", rank2xmpp.AuthorizedPeer{
		FullJID: "peer@example.test/r2.install-other.nonce-2", InstallationID: "install-other", SessionGeneration: "session-2",
	}, nil)
	if err := service.rank1.authority.AuthorizeRank1(context.Background(), oldFull); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("Rank1 old installation authorization = %v, want unavailable", err)
	}
	bare, exact, _ := session.peerQuerySnapshot()
	if len(bare) != 0 || len(exact) != 2 || exact[0] != oldFull || exact[1] != oldFull {
		t.Fatalf("Rank1 used a bare query or skipped exact recheck: bare=%v exact=%v", bare, exact)
	}
	qaStageBCloseCore(t, core)
}

func TestStageBAcceptanceAdvertisesTransportProtectedLargePayloads(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-v1-large-payload-auth")

	capabilityCompletion := qaStageBCompletion(t, core, v1.CoreCapabilitiesCommand{CommandBase: qaStageBBase("qa-v1-large-payload-capabilities")})
	capabilities, ok := capabilityCompletion.Result.(v1.Capabilities)
	if !capabilityCompletion.OK || capabilityCompletion.Error != nil || !ok {
		t.Fatalf("capabilities = %#v", capabilityCompletion)
	}
	found := false
	for _, feature := range capabilities.Features {
		if feature == v1.CapabilityLargePayloads {
			found = true
		}
	}
	if !found {
		t.Fatal("authenticated default Core omitted large-payload support")
	}
	qaStageBCloseCore(t, core)
}

func TestStageBAcceptanceUnavailableAndForbiddenTargetsFailBeforeSend(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		code    v1.ErrorCode
	}{
		{name: "unavailable", failure: rank2xmpp.ErrUnavailable, code: v1.ErrorCodeConnectivityUnavailable},
		{name: "forbidden", failure: rank2xmpp.ErrAuthentication, code: v1.ErrorCodeAuthorizationRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newQAStageBEndpointSession("")
			session.setPeer("peer@example.test", rank2xmpp.AuthorizedPeer{}, test.failure)
			core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
			qaStageBAuthenticate(t, core, "qa-peer-auth-"+test.name)
			qaStageBAllow(t, core, "qa-peer-policy-"+test.name, "/messages", "peer@example.test")
			failed := qaStageBCompletion(t, core, v1.MessageSendCommand{CommandBase: qaStageBBase("qa-peer-send-" + test.name), To: "peer@example.test", Payload: v1.Payload{Value: v1.NativePayload{ContentType: "text/plain", Path: "/messages", Body: []byte("not sent")}}})
			if failed.OK || failed.Error == nil || failed.Error.Code != test.code || len(session.sentSnapshot()) != 0 {
				t.Fatalf("%s target failure=%#v sent=%#v", test.name, failed, session.sentSnapshot())
			}
			if bare, _, _ := session.peerQuerySnapshot(); len(bare) != 1 || bare[0] != "peer@example.test" {
				t.Fatalf("%s handshake calls=%v", test.name, bare)
			}
			qaStageBCloseCore(t, core)
		})
	}
}

func TestStageBAcceptanceIngressPumpContinuesAfterEnvelopeLocalRejection(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-pump-auth")
	qaStageBAllow(t, core, "qa-pump-policy", "/incoming", "peer@example.test")

	rejected := qaStageBInboundEvent(t, "rejected")
	rejected.Stanza.Data = append(rejected.Stanza.Data, 0xff)
	session.emit(rejected)
	session.emit(qaStageBInboundEvent(t, "accepted"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var received v1.Event
	for received.Name != v1.EventMessageReceived {
		event, err := core.NextEvent(ctx)
		if err != nil {
			t.Fatalf("wait for valid event after stale rejection: %v", err)
		}
		received = event
	}
	message, ok := received.Payload.(v1.MessageReceivedEvent)
	native, nativeOK := message.Payload.Value.(v1.NativePayload)
	if !ok || !nativeOK || message.FromAgentID != "peer@example.test" || message.MeshID != "mesh-one" || native.Path != "/incoming" || string(native.Body) != "accepted" {
		t.Fatalf("public inbound event = %#v", received)
	}
	accepted := qaStageBCompletion(t, core, v1.DeliveryAcceptCommand{CommandBase: qaStageBBase("qa-pump-accept"), EventID: received.ID})
	if !accepted.OK || accepted.Error != nil {
		t.Fatalf("mandatory accept = %#v", accepted)
	}
	qaStageBCloseCore(t, core)
}

func TestStageBAcceptanceShutdownCancelsOutstandingInboundBeforeClientClose(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-shutdown-auth")
	qaStageBAllow(t, core, "qa-shutdown-policy", "/incoming", "peer@example.test")
	session.emit(qaStageBInboundEvent(t, "unaccepted"))

	deadline := time.Now().Add(time.Second)
	for {
		status := qaStageBCompletion(t, core, v1.DeliveryQueueStatusCommand{CommandBase: qaStageBBase("qa-shutdown-queue")})
		queue, ok := status.Result.(v1.DeliveryQueueStatus)
		if status.OK && ok && queue.Queued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("inbound ownership did not reach the sole event queue")
		}
		time.Sleep(time.Millisecond)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- core.Shutdown(context.Background()) }()
	deadline = time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Fatalf("shutdown with unaccepted inbound: %v", err)
			}
			goto shutdownComplete
		default:
		}
		wait, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, _ = core.NextCompletion(wait)
		cancel()
		wait, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
		event, err := core.NextEvent(wait)
		cancel()
		if err == nil && event.Name == v1.EventMessageReceived {
			t.Fatalf("shutdown transferred an unaccepted inbound event: %#v", event)
		}
		if time.Now().After(deadline) {
			t.Fatal("shutdown did not retire the outstanding inbound delivery")
		}
	}

shutdownComplete:
	select {
	case <-session.closed:
	default:
		t.Fatal("connectivity client was not closed after pump cancellation joined")
	}
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func qaStageBInboundEvent(t *testing.T, body string) rank2xmpp.Event {
	t.Helper()
	canonical, err := payload.SerializeCanonical(model.Payload{Value: model.NativePayload{
		ContentType: "application/octet-stream", Path: "/incoming", Body: []byte(body),
	}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := protocol.NewInlinePayload(payload.ProfileNative, canonical)
	if err != nil {
		t.Fatal(err)
	}
	conversationID, err := conversation.DeriveID("mesh-one", "agent@example.test/mesh-one", "peer@example.test/r2.install-peer.nonce-1")
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		ConversationID: conversationID, Sender: "peer@example.test/r2.install-peer.nonce-1", Recipient: "agent@example.test/mesh-one",
		MeshID: "mesh-one", Mode: protocol.ModeMessage,
		CreatedAt: time.Date(2026, 8, 13, 12, 0, 0, 123456000, time.UTC), ClockUncertainty: time.Millisecond,
		Payload: descriptor,
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

func TestStageBAcceptancePublicMessagingCancellationTaxonomy(t *testing.T) {
	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-cancel-auth")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := core.Submit(cancelled, v1.MeshListCommand{CommandBase: qaStageBBase("qa-cancel-list")})
	if err == nil {
		t.Fatal("pre-cancelled public messaging command was admitted")
	}
	var public *v1.Error
	if !errors.As(err, &public) || public.Code != v1.ErrorCodeRequestCancelled {
		t.Fatalf("pre-cancelled error = %v", err)
	}
	qaStageBCloseCore(t, core)
}

func TestStageBAcceptanceFailureTaxonomy(t *testing.T) {
	unauthenticatedSession := newQAStageBEndpointSession("")
	unauthenticated := qaStageBCore(t, qaStageBEndpointDialer{session: unauthenticatedSession})
	unavailable := qaStageBCompletion(t, unauthenticated, v1.MessageSendCommand{
		CommandBase: qaStageBBase("qa-unavailable-send"), To: "peer@example.test",
		Payload: v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/queued", Body: []byte("unavailable")}},
	})
	if unavailable.OK || unavailable.Result != nil || unavailable.Error == nil || unavailable.Error.Code != v1.ErrorCodeConnectivityUnavailable || unavailable.Error.Stage != v1.ErrorStageDelivery || !unavailable.Error.Retryable {
		t.Fatalf("unauthenticated message.send taxonomy = %#v", unavailable)
	}
	qaStageBCloseCore(t, unauthenticated)

	session := newQAStageBEndpointSession("")
	core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
	qaStageBAuthenticate(t, core, "qa-taxonomy-auth")

	rejected := qaStageBCompletion(t, core, v1.MessageSendCommand{
		CommandBase: qaStageBBase("qa-policy-rejected"), To: "peer@example.test",
		Payload: v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/denied", Body: []byte("denied")}},
	})
	if rejected.OK || rejected.Result != nil || rejected.Error == nil || rejected.Error.Code != v1.ErrorCodeAuthorizationRejected || rejected.Error.Stage != v1.ErrorStagePolicy || rejected.Error.Location != v1.ErrorLocationLocal || rejected.Error.Retryable {
		bare, exact, _ := session.peerQuerySnapshot()
		t.Fatalf("policy-rejected message.send taxonomy = %#v error=%+v queries=%v/%v", rejected, rejected.Error, bare, exact)
	}

	qaStageBCloseCore(t, core)
}
