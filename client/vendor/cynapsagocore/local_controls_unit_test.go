package cynapsagocore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

func TestPublicLocalConfigurationAndRegistrationControlsUseLiveRuntime(t *testing.T) {
	core := newStartedLocalControlCore(t, 16)
	defer destroyLocalControlCore(t, core)

	initial := requireLocalCompletion(t, core, v1.ConfigGetCommand{CommandBase: localCommandBase("config-get")})
	initialConfig, ok := initial.Result.(v1.ConfigResult)
	if !ok || initialConfig.Config.CommandTimeout != 30*time.Second || initialConfig.Config.RPCTimeout != 30*time.Second {
		t.Fatalf("initial config = %#v", initial.Result)
	}

	commandTimeout, rpcTimeout := 7*time.Second, 11*time.Second
	updated := requireLocalCompletion(t, core, v1.ConfigUpdateCommand{
		CommandBase: localCommandBase("config-update"),
		Update:      v1.ConfigUpdate{CommandTimeout: &commandTimeout, RPCTimeout: &rpcTimeout},
	})
	updatedConfig, ok := updated.Result.(v1.ConfigResult)
	if !ok || updatedConfig.Config.CommandTimeout != commandTimeout || updatedConfig.Config.RPCTimeout != rpcTimeout || core.DefaultTimeout() != commandTimeout {
		t.Fatalf("updated config = %#v, default = %v", updated.Result, core.DefaultTimeout())
	}
	got := requireLocalCompletion(t, core, v1.ConfigGetCommand{CommandBase: localCommandBase("config-get-updated")})
	if !reflect.DeepEqual(got.Result, updated.Result) {
		t.Fatalf("live config get = %#v, want %#v", got.Result, updated.Result)
	}
	immutable := uint32(16)
	requireLocalFailure(t, core, v1.ConfigUpdateCommand{
		CommandBase: localCommandBase("config-update-immutable"), Update: v1.ConfigUpdate{QueueLimit: &immutable},
	}, v1.ErrorCodeCommand)

	channel := requireLocalCompletion(t, core, v1.CommandChannelRegisterCommand{
		CommandBase: localCommandBase("channel-register"), Capacity: 3,
	})
	channelResult, ok := channel.Result.(v1.CompletionChannelResult)
	if !ok || channelResult.ChannelID == "" || channelResult.MaxInFlight != 3 {
		t.Fatalf("channel result = %#v", channel.Result)
	}
	requireLocalFailure(t, core, v1.CommandChannelRegisterCommand{
		CommandBase: localCommandBase("channel-register-duplicate"), Capacity: 1,
	}, v1.ErrorCodeCommand)
	requireLocalFailure(t, core, v1.CommandChannelClearCommand{
		CommandBase: localCommandBase("channel-clear-stale"), ChannelID: "completion-stale",
	}, v1.ErrorCodeInvalidHandle)
	requireLocalCompletion(t, core, v1.CommandChannelClearCommand{
		CommandBase: localCommandBase("channel-clear"), ChannelID: channelResult.ChannelID,
	})

	sink := requireLocalCompletion(t, core, v1.EventSinkRegisterCommand{
		CommandBase: localCommandBase("sink-register"), Capacity: 4,
	})
	sinkResult, ok := sink.Result.(v1.EventSinkResult)
	if !ok || sinkResult.SinkID == "" {
		t.Fatalf("sink result = %#v", sink.Result)
	}
	requireLocalFailure(t, core, v1.EventSinkBindCommand{
		CommandBase: localCommandBase("sink-bind-stale"), SinkID: "sink-stale",
	}, v1.ErrorCodeInvalidHandle)
	requireLocalCompletion(t, core, v1.EventSinkBindCommand{
		CommandBase: localCommandBase("sink-bind"), SinkID: sinkResult.SinkID,
	})
	requireLocalCompletion(t, core, v1.EventSinkClearCommand{
		CommandBase: localCommandBase("sink-clear"), SinkID: sinkResult.SinkID,
	})

	logEvent := model.Event{
		ID: "diagnostic-before", Name: "diagnostics.log", CreatedAt: time.UnixMilli(1).UTC(),
		Value: model.DiagnosticLogEvent{Level: "info", Code: "core_state", DiagnosticID: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	}
	if err := core.runtime.PublishEvent(context.Background(), logEvent); !errors.Is(err, coreruntime.ErrDiagnosticLogsOff) {
		t.Fatalf("diagnostic before subscription = %v", err)
	}
	requireLocalCompletion(t, core, v1.DiagnosticsLogsCommand{CommandBase: localCommandBase("logs-on"), Enabled: true})
	if err := core.runtime.PublishEvent(context.Background(), logEvent); err != nil {
		t.Fatalf("diagnostic after subscription = %v", err)
	}
	requireLocalCompletion(t, core, v1.DiagnosticsLogsCommand{CommandBase: localCommandBase("logs-off"), Enabled: false})
}

func TestLocalHandlerAndPolicyCancellationDoesNotMutateSharedState(t *testing.T) {
	handlers, err := mesh.NewHandlerRegistry(2)
	if err != nil {
		t.Fatal(err)
	}
	policies, err := mesh.NewPolicyController(nil)
	if err != nil {
		t.Fatal(err)
	}
	services := &localServices{handlers: handlers, policies: policies}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, failure := services.handlerRegister(ctx, nil, model.HandlerPathArgs{Path: "/cancelled"}); failure == nil || failure.Code != sessionkernel.ProviderCancelled {
		t.Fatalf("cancelled handler failure = %#v", failure)
	}
	if handlers.Len() != 0 {
		t.Fatal("cancelled handler registration mutated shared state")
	}
	if _, failure := services.policySet(ctx, nil, model.PolicySetArgs{Rules: []model.PolicyRule{{Action: "allow", Path: "/", AgentID: "agent"}}}); failure == nil || failure.Code != sessionkernel.ProviderCancelled {
		t.Fatalf("cancelled policy failure = %#v", failure)
	}
	if len(policies.Result().Rules) != 0 {
		t.Fatal("cancelled policy update mutated shared state")
	}
}

func TestPublicLocalDeliveryControlsShareOneAuthoritativeQueue(t *testing.T) {
	core := newStartedLocalControlCore(t, 8)
	defer destroyLocalControlCore(t, core)

	event := model.Event{
		ID: "delivery-event", Name: "core.error", CreatedAt: time.UnixMilli(2).UTC(),
		Value: model.CoreErrorEvent{Err: &model.Error{Code: "core_error", Stage: "command", Location: "local"}},
	}
	if err := core.runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}

	requireLocalCompletion(t, core, v1.DeliveryPauseCommand{CommandBase: localCommandBase("delivery-pause")})
	status := requireLocalCompletion(t, core, v1.DeliveryQueueStatusCommand{CommandBase: localCommandBase("delivery-status-paused")})
	queue, ok := status.Result.(v1.DeliveryQueueStatus)
	if !ok || queue.Queued != 1 || !queue.Paused {
		t.Fatalf("paused queue = %#v", status.Result)
	}
	requireLocalFailure(t, core, v1.DeliveryNextCommand{CommandBase: localCommandBase("delivery-next-paused")}, v1.ErrorCodeCommand)
	requireLocalCompletion(t, core, v1.DeliveryResumeCommand{CommandBase: localCommandBase("delivery-resume")})

	next := requireLocalCompletion(t, core, v1.DeliveryNextCommand{CommandBase: localCommandBase("delivery-next")})
	pulled, ok := next.Result.(v1.EventResult)
	if !ok || pulled.Event.ID != v1.EventID(event.ID) || pulled.Event.Name != v1.EventCoreError {
		t.Fatalf("delivery result = %#v", next.Result)
	}
	requireLocalFailure(t, core, v1.DeliveryAcceptCommand{
		CommandBase: localCommandBase("delivery-accept-wrong"), EventID: "wrong-event",
	}, v1.ErrorCodeInvalidHandle)
	requireLocalCompletion(t, core, v1.DeliveryAcceptCommand{
		CommandBase: localCommandBase("delivery-accept"), EventID: pulled.Event.ID,
	})
	requireLocalFailure(t, core, v1.DeliveryAcceptCommand{
		CommandBase: localCommandBase("delivery-accept-repeat"), EventID: pulled.Event.ID,
	}, v1.ErrorCodeInvalidHandle)

	status = requireLocalCompletion(t, core, v1.DeliveryQueueStatusCommand{CommandBase: localCommandBase("delivery-status-empty")})
	queue, ok = status.Result.(v1.DeliveryQueueStatus)
	if !ok || queue.Queued != 0 || queue.Paused {
		t.Fatalf("empty queue = %#v", status.Result)
	}
}

func TestPublicLocalHandlerAndPolicyControlsShareCanonicalState(t *testing.T) {
	core := newStartedLocalControlCore(t, 4)
	defer destroyLocalControlCore(t, core)

	register := func(id string) {
		requireLocalCompletion(t, core, v1.HandlerRegisterCommand{CommandBase: localCommandBase(id), Path: "/orders/create"})
	}
	register("handler-register")
	register("handler-register-idempotent")
	if core.handlers == nil || !core.handlers.Contains("/orders/create") || core.handlers.Len() != 1 {
		t.Fatalf("handler registry = %#v, length %d", core.handlers, core.handlers.Len())
	}
	requireLocalCompletion(t, core, v1.HandlerUnregisterCommand{CommandBase: localCommandBase("handler-unregister"), Path: "/orders/create"})
	requireLocalCompletion(t, core, v1.HandlerUnregisterCommand{CommandBase: localCommandBase("handler-unregister-idempotent"), Path: "/orders/create"})

	rules := []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: "/orders/create", AgentID: "agent-a@example.test"}}
	set := requireLocalCompletion(t, core, v1.PolicySetCommand{CommandBase: localCommandBase("policy-set"), Rules: rules})
	setResult, ok := set.Result.(v1.PolicyResult)
	if !ok || !reflect.DeepEqual(setResult.Rules, rules) || setResult.Allowed {
		t.Fatalf("policy set = %#v", set.Result)
	}
	get := requireLocalCompletion(t, core, v1.PolicyGetCommand{CommandBase: localCommandBase("policy-get")})
	if !reflect.DeepEqual(get.Result, set.Result) {
		t.Fatalf("policy get = %#v, want %#v", get.Result, set.Result)
	}

	payload := v1.Payload{Value: v1.NativePayload{ContentType: "application/json", Path: "/orders/create", Body: []byte(`{"id":1}`)}}
	allowed := requireLocalCompletion(t, core, v1.PolicyTestCommand{
		CommandBase: localCommandBase("policy-test-allow"), Input: v1.PolicyTestInput{To: "agent-a@example.test", Payload: payload},
	})
	allowedResult, ok := allowed.Result.(v1.PolicyResult)
	if !ok || !allowedResult.Allowed || !reflect.DeepEqual(allowedResult.Rules, rules) {
		t.Fatalf("allowed policy test = %#v", allowed.Result)
	}
	denied := requireLocalCompletion(t, core, v1.PolicyTestCommand{
		CommandBase: localCommandBase("policy-test-deny"), Input: v1.PolicyTestInput{To: "agent-b@example.test", Payload: payload},
	})
	deniedResult, ok := denied.Result.(v1.PolicyResult)
	if !ok || deniedResult.Allowed {
		t.Fatalf("denied policy test = %#v", denied.Result)
	}

	prepared, err := core.payloadPipeline.Prepare(model.Payload{Value: model.NativePayload{
		ContentType: "application/json", Path: "/orders/create", Body: []byte(`{"id":2}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := core.PayloadOpen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = core.PayloadWrite(context.Background(), handle, prepared.Canonical); err != nil {
		t.Fatal(err)
	}
	clearPipelineBytes(prepared.Canonical)
	if _, err = core.PayloadFinish(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	fromHandle := requireLocalCompletion(t, core, v1.PolicyTestCommand{
		CommandBase: localCommandBase("policy-test-handle"),
		Input:       v1.PolicyTestInput{To: "agent-a@example.test", Payload: v1.Payload{Value: handle}},
	})
	if result, ok := fromHandle.Result.(v1.PolicyResult); !ok || !result.Allowed {
		t.Fatalf("handle policy test = %#v", fromHandle.Result)
	}
	if err := core.PayloadRelease(context.Background(), handle); err != nil {
		t.Fatal(err)
	}

	if core.policies == nil || core.payloadFactory == nil || core.payloadPipeline == nil {
		t.Fatal("Core did not retain the shared local policy/payload graph")
	}
}

func TestWildcardPolicyCannotApproveMalformedDestination(t *testing.T) {
	core := newStartedLocalControlCore(t, 4)
	defer destroyLocalControlCore(t, core)

	requireLocalCompletion(t, core, v1.PolicySetCommand{
		CommandBase: localCommandBase("policy-set-wildcard"),
		Rules:       []v1.PolicyRule{{Action: v1.PolicyActionAllow}},
	})
	payload := v1.Payload{Value: v1.NativePayload{Path: "/orders"}}
	before := core.gate.Stats()
	admission, err := core.Submit(context.Background(), v1.PolicyTestCommand{
		CommandBase: localCommandBase("policy-test-malformed"),
		Input:       v1.PolicyTestInput{To: v1.AgentID("peer\x00injected"), Payload: payload},
	})
	if err == nil {
		t.Fatalf("malformed wildcard policy test was accepted: %#v", admission)
	}
	var publicError *v1.Error
	if !errors.As(err, &publicError) || publicError.Code != v1.ErrorCodeMalformedInput {
		t.Fatalf("malformed wildcard policy error=%v", err)
	}
	if admission != (v1.Admission{}) {
		t.Fatalf("malformed wildcard policy produced admission=%#v", admission)
	}
	if after := core.gate.Stats(); after != before {
		t.Fatalf("malformed wildcard policy reached admission: before=%+v after=%+v", before, after)
	}

	valid := requireLocalCompletion(t, core, v1.PolicyTestCommand{
		CommandBase: localCommandBase("policy-test-valid-wildcard"),
		Input:       v1.PolicyTestInput{To: "peer", Payload: payload},
	})
	result, ok := valid.Result.(v1.PolicyResult)
	if !ok || !result.Allowed {
		t.Fatalf("valid wildcard policy test=%#v", valid.Result)
	}
}

func newStartedLocalControlCore(t *testing.T, queue uint32) *Core {
	t.Helper()
	core, err := New(Config{QueueLimit: queue, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return core
}

func destroyLocalControlCore(t *testing.T, core *Core) {
	t.Helper()
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func localCommandBase(id string) v1.CommandBase {
	return v1.CommandBase{CommandID: v1.CommandID(id), SDKSessionID: "local-session"}
}

func requireLocalCompletion(t *testing.T, core *Core, command v1.Command) v1.Completion {
	t.Helper()
	admission, err := core.Submit(context.Background(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("submit %s = %#v, %v", command.Name(), admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil {
		t.Fatalf("complete %s = %#v, %v", command.Name(), completion, err)
	}
	if !completion.OK || completion.Error != nil {
		t.Fatalf("complete %s = %#v", command.Name(), completion)
	}
	return completion
}

func requireLocalFailure(t *testing.T, core *Core, command v1.Command, code v1.ErrorCode) v1.Completion {
	t.Helper()
	admission, err := core.Submit(context.Background(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("submit %s = %#v, %v", command.Name(), admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || completion.OK || completion.Error == nil || completion.Error.Code != code {
		t.Fatalf("failed completion %s = %#v, %v", command.Name(), completion, err)
	}
	return completion
}
