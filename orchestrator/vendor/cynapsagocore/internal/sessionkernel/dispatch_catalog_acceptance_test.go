package sessionkernel_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

type qaCallLog struct {
	mu    sync.Mutex
	calls map[string]int
}

func newQACallLog() *qaCallLog { return &qaCallLog{calls: make(map[string]int)} }

func (log *qaCallLog) record(name string) {
	log.mu.Lock()
	log.calls[name]++
	log.mu.Unlock()
}

func (log *qaCallLog) count(name string) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.calls[name]
}

func qaOperation[A any, R model.ResultValue](log *qaCallLog, name string, value R) sessionkernel.Operation[A, R] {
	return func(context.Context, coreruntime.Services, A) (R, *sessionkernel.ProviderError) {
		log.record(name)
		return value, nil
	}
}

func qaOperations(log *qaCallLog) (sessionkernel.LocalOperations, sessionkernel.AuthenticatedOperations) {
	empty := model.EmptyResult{}
	local := sessionkernel.LocalOperations{
		CoreInit: func(_ context.Context, _ coreruntime.Services, sessionID string, _ model.EmptyArgs) (model.CoreInitResult, *sessionkernel.ProviderError) {
			log.record("core.init")
			return model.CoreInitResult{SessionID: sessionID}, nil
		},
		CoreCapabilities: qaOperation[model.EmptyArgs](log, "core.capabilities", model.CapabilitiesResult{}),
		CoreStatus:       qaOperation[model.EmptyArgs](log, "core.status", model.StatusResult{}),
		CoreShutdown:     qaOperation[model.EmptyArgs](log, "core.shutdown", empty),
		ConfigGet:        qaOperation[model.EmptyArgs](log, "session.config.get", model.ConfigResult{}),
		ConfigUpdate:     qaOperation[model.ConfigUpdateArgs](log, "session.config.update", model.ConfigResult{}),

		CommandChannelRegister: qaOperation[model.ChannelRegisterArgs](log, "command.channel.register", model.CompletionChannelResult{}),
		CommandChannelClear:    qaOperation[model.ChannelIDArgs](log, "command.channel.clear", empty),
		CommandCancel:          qaOperation[model.CommandCancelArgs](log, "command.cancel", empty),
		EventSinkRegister:      qaOperation[model.EventSinkRegisterArgs](log, "event.sink.register", model.EventSinkResult{}),
		EventSinkClear:         qaOperation[model.EventSinkIDArgs](log, "event.sink.clear", empty),
		EventSinkBind:          qaOperation[model.EventSinkIDArgs](log, "event.sink.bind", empty),

		AddressPut:     qaOperation[model.AddressPutArgs](log, "address.map.put", empty),
		AddressRemove:  qaOperation[model.AddressRemoveArgs](log, "address.map.remove", empty),
		AddressList:    qaOperation[model.EmptyArgs](log, "address.map.list", model.AddressMappingsResult{}),
		AddressResolve: qaOperation[model.AddressResolveArgs](log, "address.resolve", model.AddressResolution{}),

		DeliveryNext:        qaOperation[model.EmptyArgs](log, "delivery.next", model.EventResult{}),
		DeliveryAccept:      qaOperation[model.DeliveryAcceptArgs](log, "delivery.accept", empty),
		DeliveryQueueStatus: qaOperation[model.EmptyArgs](log, "delivery.queue.status", model.DeliveryQueueStatus{}),
		DeliveryPause:       qaOperation[model.EmptyArgs](log, "delivery.pause", empty),
		DeliveryResume:      qaOperation[model.EmptyArgs](log, "delivery.resume", empty),
		HandlerRegister:     qaOperation[model.HandlerPathArgs](log, "handler.register", empty),
		HandlerUnregister:   qaOperation[model.HandlerPathArgs](log, "handler.unregister", empty),

		PayloadOpen:    qaOperation[model.EmptyArgs](log, "payload.open", model.PayloadHandleResult{}),
		PayloadWrite:   qaOperation[model.PayloadWriteArgs](log, "payload.write_chunk", model.PayloadHandleResult{}),
		PayloadFinish:  qaOperation[model.PayloadHandleArgs](log, "payload.finish", model.PayloadHandleResult{}),
		PayloadCancel:  qaOperation[model.PayloadHandleArgs](log, "payload.cancel", empty),
		PayloadRead:    qaOperation[model.PayloadReadArgs](log, "payload.read", model.PayloadHandleResult{}),
		PayloadClose:   qaOperation[model.PayloadHandleArgs](log, "payload.close", empty),
		PayloadRetain:  qaOperation[model.PayloadHandleArgs](log, "payload.retain", model.PayloadHandleResult{}),
		PayloadRelease: qaOperation[model.PayloadHandleArgs](log, "payload.release", empty),

		PolicySet:           qaOperation[model.PolicySetArgs](log, "policy.set", model.PolicyResult{}),
		PolicyGet:           qaOperation[model.EmptyArgs](log, "policy.get", model.PolicyResult{}),
		PolicyTest:          qaOperation[model.PolicyTestArgs](log, "policy.test", model.PolicyResult{}),
		DiagnosticsSnapshot: qaOperation[model.EmptyArgs](log, "diagnostics.snapshot", model.DiagnosticSnapshot{}),
		DiagnosticsLogs:     qaOperation[model.DiagnosticsLogsArgs](log, "diagnostics.logs.subscribe", empty),
	}
	remote := sessionkernel.AuthenticatedOperations{
		Logout:         qaOperation[model.EmptyArgs](log, "auth.logout", empty),
		MeshList:       qaOperation[model.EmptyArgs](log, "mesh.list", model.MeshListResult{}),
		MeshRefresh:    qaOperation[model.EmptyArgs](log, "mesh.membership.refresh", empty),
		MessageSend:    qaOperation[model.MessageSendArgs](log, "message.send", model.SendResult{}),
		MessageRequest: qaOperation[model.MessageRequestArgs](log, "message.request", model.ResponseResult{}),
		MessageReply:   qaOperation[model.MessageReplyArgs](log, "message.reply", model.SendResult{}),
		DeliveryRetry:  qaOperation[model.MessageIDArgs](log, "delivery.retry", empty),
		DeliveryDrop:   qaOperation[model.MessageIDArgs](log, "delivery.drop", empty),

		ConversationList:   qaOperation[model.EmptyArgs](log, "conversation.list", model.ConversationListResult{}),
		ConversationStatus: qaOperation[model.ConversationIDArgs](log, "conversation.status", model.ConversationStatus{}),
		ConversationClose:  qaOperation[model.ConversationIDArgs](log, "conversation.close", empty),
		DiagnosticsPeer:    qaOperation[model.DiagnosticsPeerArgs](log, "diagnostics.peer_status", model.PeerStatus{}),
		DiagnosticsNetwork: qaOperation[model.EmptyArgs](log, "diagnostics.connectivity_status", model.ConnectivityStatus{}),
	}
	return local, remote
}

type qaDispatchCase struct {
	args model.CommandArgs
	want model.ResultValue
}

func qaDispatchCases() map[string]qaDispatchCase {
	emptyArgs := model.EmptyArgs{}
	emptyResult := model.EmptyResult{}
	return map[string]qaDispatchCase{
		"core.init": {emptyArgs, model.CoreInitResult{}}, "core.capabilities": {emptyArgs, model.CapabilitiesResult{}},
		"core.status": {emptyArgs, model.StatusResult{}}, "core.shutdown": {emptyArgs, emptyResult},
		"session.config.get": {emptyArgs, model.ConfigResult{}}, "session.config.update": {model.ConfigUpdateArgs{}, model.ConfigResult{}},
		"command.channel.register": {model.ChannelRegisterArgs{}, model.CompletionChannelResult{}}, "command.channel.clear": {model.ChannelIDArgs{}, emptyResult},
		"command.cancel": {model.CommandCancelArgs{}, emptyResult}, "event.sink.register": {model.EventSinkRegisterArgs{}, model.EventSinkResult{}},
		"event.sink.clear": {model.EventSinkIDArgs{}, emptyResult}, "event.sink.bind": {model.EventSinkIDArgs{}, emptyResult},
		"auth.login": {model.AuthArgs{}, model.AuthResult{}}, "auth.connect": {model.AuthArgs{}, model.AuthResult{}},
		"auth.token_login": {model.TokenAuthArgs{}, model.AuthResult{}}, "auth.token_connect": {model.TokenAuthArgs{}, model.AuthResult{}},
		"auth.installation_login": {model.InstallationAuthArgs{}, model.AuthResult{}}, "auth.installation_connect": {model.InstallationAuthArgs{}, model.AuthResult{}},
		"auth.logout": {emptyArgs, emptyResult}, "auth.agent_id": {emptyArgs, model.AgentIDResult{}},
		"mesh.list": {emptyArgs, model.MeshListResult{}}, "mesh.membership.refresh": {emptyArgs, emptyResult},
		"address.map.put": {model.AddressPutArgs{}, emptyResult}, "address.map.remove": {model.AddressRemoveArgs{}, emptyResult},
		"address.map.list": {emptyArgs, model.AddressMappingsResult{}}, "address.resolve": {model.AddressResolveArgs{}, model.AddressResolution{}},
		"message.send": {model.MessageSendArgs{}, model.SendResult{}}, "message.request": {model.MessageRequestArgs{}, model.ResponseResult{}},
		"message.reply": {model.MessageReplyArgs{}, model.SendResult{}}, "delivery.next": {emptyArgs, model.EventResult{}},
		"delivery.accept": {model.DeliveryAcceptArgs{}, emptyResult}, "delivery.queue.status": {emptyArgs, model.DeliveryQueueStatus{}},
		"delivery.retry": {model.MessageIDArgs{}, emptyResult}, "delivery.pause": {emptyArgs, emptyResult},
		"delivery.resume": {emptyArgs, emptyResult}, "delivery.drop": {model.MessageIDArgs{}, emptyResult},
		"handler.register": {model.HandlerPathArgs{}, emptyResult}, "handler.unregister": {model.HandlerPathArgs{}, emptyResult},
		"payload.open": {emptyArgs, model.PayloadHandleResult{}}, "payload.write_chunk": {model.PayloadWriteArgs{}, model.PayloadHandleResult{}},
		"payload.finish": {model.PayloadHandleArgs{}, model.PayloadHandleResult{}}, "payload.cancel": {model.PayloadHandleArgs{}, emptyResult},
		"payload.read": {model.PayloadReadArgs{}, model.PayloadHandleResult{}}, "payload.close": {model.PayloadHandleArgs{}, emptyResult},
		"payload.retain": {model.PayloadHandleArgs{}, model.PayloadHandleResult{}}, "payload.release": {model.PayloadHandleArgs{}, emptyResult},
		"conversation.list": {emptyArgs, model.ConversationListResult{}}, "conversation.status": {model.ConversationIDArgs{}, model.ConversationStatus{}},
		"conversation.close": {model.ConversationIDArgs{}, emptyResult}, "policy.set": {model.PolicySetArgs{}, model.PolicyResult{}},
		"policy.get": {emptyArgs, model.PolicyResult{}}, "policy.test": {model.PolicyTestArgs{}, model.PolicyResult{}},
		"diagnostics.peer_status": {model.DiagnosticsPeerArgs{}, model.PeerStatus{}}, "diagnostics.connectivity_status": {emptyArgs, model.ConnectivityStatus{}},
		"diagnostics.snapshot": {emptyArgs, model.DiagnosticSnapshot{}}, "diagnostics.logs.subscribe": {model.DiagnosticsLogsArgs{}, emptyResult},
	}
}

func TestStageBAllCommandsHaveExactTypedDispatchAndResultCoupling(t *testing.T) {
	cases := qaDispatchCases()
	catalog := coreruntime.CommandCatalog()
	if len(catalog) != 56 || len(cases) != len(catalog) {
		t.Fatalf("command counts: catalog=%d cases=%d, want 56", len(catalog), len(cases))
	}
	wantNames := make([]string, 0, len(cases))
	for name := range cases {
		wantNames = append(wantNames, name)
	}
	sort.Strings(wantNames)
	if !reflect.DeepEqual(catalog, wantNames) {
		t.Fatalf("catalog mismatch\n got: %v\nwant: %v", catalog, wantNames)
	}

	log := newQACallLog()
	local, remote := qaOperations(log)
	connectivity := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}, remote: remote}
	_, dependencies := qaFactory(t, sessionkernel.Dependencies{Local: local, Connectivity: qaConnectivityFactory{service: connectivity}})
	services := qaServices{}
	connected, err := dependencies.Handlers["auth.connect"](context.Background(), services, qaAuthCommand("auth.connect"))
	if err != nil || connected.Err != nil || reflect.TypeOf(connected.Value) != reflect.TypeOf(model.AuthResult{}) {
		t.Fatalf("auth.connect = (%+v, %v)", connected, err)
	}

	for _, name := range catalog {
		if name == "auth.login" || name == "auth.connect" || name == "auth.token_login" || name == "auth.token_connect" || name == "auth.installation_login" || name == "auth.installation_connect" {
			continue
		}
		test := cases[name]
		command := model.Command{ID: "qa-" + name, Name: name, SessionID: "session-exact", Args: test.args}
		result, handlerErr := dependencies.Handlers[name](context.Background(), services, command)
		if handlerErr != nil || result.Err != nil {
			t.Errorf("%s = (%+v, %v)", name, result, handlerErr)
			continue
		}
		if reflect.TypeOf(result.Value) != reflect.TypeOf(test.want) {
			t.Errorf("%s result type = %T, want %T", name, result.Value, test.want)
		}
		if name != "auth.agent_id" && log.count(name) != 1 {
			t.Errorf("%s selected operation count = %d, want 1", name, log.count(name))
		}
	}

	loginService := &qaConnectivity{identity: connectivity.identity}
	_, loginDependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: loginService}})
	login, err := loginDependencies.Handlers["auth.login"](context.Background(), services, qaAuthCommand("auth.login"))
	if err != nil || login.Err != nil {
		t.Fatalf("auth.login = (%+v, %v)", login, err)
	}
	value, ok := login.Value.(model.AuthResult)
	if !ok || value.Personality != string(sessionkernel.PersonalityHTTPBridge) {
		t.Fatalf("auth.login result = %+v", login)
	}
}

func TestStageBEveryAuthenticatedCommandFailsClosedBeforeAuthentication(t *testing.T) {
	_, dependencies := qaFactory(t, sessionkernel.Dependencies{})
	cases := qaDispatchCases()
	remote := []string{
		"auth.logout", "auth.agent_id", "mesh.list", "mesh.membership.refresh",
		"message.send", "message.request", "message.reply", "delivery.retry", "delivery.drop",
		"conversation.list", "conversation.status", "conversation.close",
		"diagnostics.peer_status", "diagnostics.connectivity_status",
	}
	for _, name := range remote {
		result, err := dependencies.Handlers[name](context.Background(), qaServices{}, model.Command{ID: "preauth-" + name, Name: name, Args: cases[name].args})
		if err != nil || result.Value != nil || result.Err == nil || result.Err.Code != "connectivity_unavailable" || !result.Err.Retryable {
			t.Errorf("preauth %s = (%+v, %v), want retryable connectivity_unavailable", name, result, err)
		}
		if result.Err != nil && fmt.Sprint(result.Err.Cause) != sessionkernel.ErrServiceUnavailable.Error() {
			t.Errorf("preauth %s cause = %q", name, fmt.Sprint(result.Err.Cause))
		}
	}
}
