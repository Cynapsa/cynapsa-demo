// Command go-agent is an external-process probe for the supported Go facade.
// It deliberately imports only the root facade and api/v1.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

type observation struct {
	Scenario     string        `json:"scenario"`
	Admitted     bool          `json:"admitted,omitempty"`
	CompletionOK bool          `json:"completion_ok,omitempty"`
	ResultKind   string        `json:"result_kind,omitempty"`
	Lifecycle    string        `json:"lifecycle,omitempty"`
	ErrorCode    v1.ErrorCode  `json:"error_code,omitempty"`
	ErrorStage   v1.ErrorStage `json:"error_stage,omitempty"`
	PayloadBytes int           `json:"payload_bytes,omitempty"`
	PayloadEOF   bool          `json:"payload_eof,omitempty"`
	Count        int           `json:"count,omitempty"`
	Value        string        `json:"value,omitempty"`
	Allowed      bool          `json:"allowed,omitempty"`
	Queued       uint64        `json:"queued,omitempty"`
	Paused       bool          `json:"paused,omitempty"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go-agent <local-contract|auth-negative|auth-real|payload-contract|lifecycle-contract>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "local-contract":
		err = runLocalContract()
	case "auth-negative":
		err = runAuthNegative()
	case "auth-real":
		err = runAuthReal()
	case "payload-contract":
		err = runPayloadContract()
	case "lifecycle-contract":
		err = runLifecycleContract()
	default:
		fmt.Fprintln(os.Stderr, "usage: go-agent <local-contract|auth-negative|auth-real|payload-contract|lifecycle-contract>")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLocalContract() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runtime, err := core.New(core.Config{QueueLimit: 8, PayloadLimit: 1 << 20})
	if err != nil {
		return fmt.Errorf("create core: %w", err)
	}
	if err := runtime.Start(ctx); err != nil {
		return fmt.Errorf("start core: %w", err)
	}
	status, err := runtime.Status(ctx)
	if err != nil {
		return fmt.Errorf("initial status: %w", err)
	}
	emit(observation{Scenario: "lifecycle_start_positive", Lifecycle: string(status.Lifecycle)})

	secondStart := runtime.Start(ctx)
	emit(withError(observation{Scenario: "lifecycle_second_start_negative"}, secondStart))

	if _, _, err := observeCommandResult(ctx, runtime, "config_get_positive", v1.ConfigGetCommand{CommandBase: commandBase("config-get")}, func(result v1.Result, observed *observation) {
		config, ok := result.(v1.ConfigResult)
		if ok {
			observed.Value = fmt.Sprintf("%d|%d", config.Config.CommandTimeout.Milliseconds(), config.Config.RPCTimeout.Milliseconds())
		}
	}); err != nil {
		return err
	}
	commandTimeout, rpcTimeout := 7*time.Second, 11*time.Second
	if _, _, err := observeCommandResult(ctx, runtime, "config_update_positive", v1.ConfigUpdateCommand{
		CommandBase: commandBase("config-update"), Update: v1.ConfigUpdate{CommandTimeout: &commandTimeout, RPCTimeout: &rpcTimeout},
	}, func(result v1.Result, observed *observation) {
		config, ok := result.(v1.ConfigResult)
		if ok {
			observed.Value = fmt.Sprintf("%d|%d", config.Config.CommandTimeout.Milliseconds(), config.Config.RPCTimeout.Milliseconds())
		}
	}); err != nil {
		return err
	}
	immutableQueue := uint32(8)
	if err := observeCommand(ctx, runtime, "config_immutable_update_negative", v1.ConfigUpdateCommand{
		CommandBase: commandBase("config-immutable"), Update: v1.ConfigUpdate{QueueLimit: &immutableQueue},
	}); err != nil {
		return err
	}

	_, channelCompletion, err := observeCommandResult(ctx, runtime, "completion_channel_register_positive", v1.CommandChannelRegisterCommand{
		CommandBase: commandBase("channel-register"), Capacity: 3,
	}, func(result v1.Result, observed *observation) {
		channel, ok := result.(v1.CompletionChannelResult)
		if ok {
			observed.Value = string(channel.ChannelID)
			observed.Count = int(channel.MaxInFlight)
		}
	})
	if err != nil {
		return err
	}
	channel, ok := channelCompletion.Result.(v1.CompletionChannelResult)
	if !ok {
		return fmt.Errorf("completion channel result type %T", channelCompletion.Result)
	}
	if err := observeCommand(ctx, runtime, "completion_channel_duplicate_negative", v1.CommandChannelRegisterCommand{CommandBase: commandBase("channel-register-again"), Capacity: 1}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "completion_channel_stale_clear_negative", v1.CommandChannelClearCommand{CommandBase: commandBase("channel-clear-stale"), ChannelID: "completion-stale"}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "completion_channel_clear_positive", v1.CommandChannelClearCommand{CommandBase: commandBase("channel-clear"), ChannelID: channel.ChannelID}); err != nil {
		return err
	}

	_, sinkCompletion, err := observeCommandResult(ctx, runtime, "event_sink_register_positive", v1.EventSinkRegisterCommand{
		CommandBase: commandBase("sink-register"), Capacity: 4,
	}, func(result v1.Result, observed *observation) {
		sink, ok := result.(v1.EventSinkResult)
		if ok {
			observed.Value = string(sink.SinkID)
		}
	})
	if err != nil {
		return err
	}
	sink, ok := sinkCompletion.Result.(v1.EventSinkResult)
	if !ok {
		return fmt.Errorf("event sink result type %T", sinkCompletion.Result)
	}
	if err := observeCommand(ctx, runtime, "event_sink_stale_bind_negative", v1.EventSinkBindCommand{CommandBase: commandBase("sink-bind-stale"), SinkID: "sink-stale"}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "event_sink_bind_positive", v1.EventSinkBindCommand{CommandBase: commandBase("sink-bind"), SinkID: sink.SinkID}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "event_sink_clear_positive", v1.EventSinkClearCommand{CommandBase: commandBase("sink-clear"), SinkID: sink.SinkID}); err != nil {
		return err
	}

	capabilitiesAdmission, _, err := observeCommandResult(ctx, runtime, "capabilities_positive", v1.CoreCapabilitiesCommand{CommandBase: commandBase("capabilities")}, nil)
	if err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "command_cancel_completed_negative", v1.CommandCancelCommand{
		CommandBase: commandBase("cancel-completed"), CommandHandle: capabilitiesAdmission.CommandHandle,
	}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "address_put_positive", v1.AddressMapPutCommand{
		CommandBase: commandBase("address-put"),
		Mapping:     v1.AddressMapping{VirtualOrigin: "HTTPS://Service.Example:443", Recipient: "peer@example.test"},
	}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, runtime, "address_resolve_equivalent_positive", v1.AddressResolveCommand{
		CommandBase: commandBase("address-resolve"), URL: "https://service.example/orders%2Fnew?x=1&x=2",
	}, func(result v1.Result, observed *observation) {
		resolved, ok := result.(v1.AddressResolution)
		if ok {
			observed.Value = string(resolved.Recipient) + "|" + resolved.Path + "|" + resolved.Query
		}
	}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, runtime, "address_list_spelling_positive", v1.AddressMapListCommand{CommandBase: commandBase("address-list")}, func(result v1.Result, observed *observation) {
		listed, ok := result.(v1.AddressMappingsResult)
		if ok {
			observed.Count = len(listed.Mappings)
			if len(listed.Mappings) == 1 {
				observed.Value = listed.Mappings[0].VirtualOrigin
			}
		}
	}); err != nil {
		return err
	}
	for _, test := range []struct {
		scenario string
		command  v1.Command
	}{
		{scenario: "address_remove_positive", command: v1.AddressMapRemoveCommand{CommandBase: commandBase("address-remove"), VirtualOrigin: "https://SERVICE.example"}},
		{scenario: "address_remove_idempotent_positive", command: v1.AddressMapRemoveCommand{CommandBase: commandBase("address-remove-idempotent"), VirtualOrigin: "HTTPS://service.example:443"}},
	} {
		if err := observeCommand(ctx, runtime, test.scenario, test.command); err != nil {
			return err
		}
	}
	if err := observeCommand(ctx, runtime, "address_unknown_resolve_negative", v1.AddressResolveCommand{
		CommandBase: commandBase("address-unknown"), URL: "https://service.example/orders",
	}); err != nil {
		return err
	}
	emit(observeSubmissionError(ctx, runtime, "address_malformed_origin_negative", v1.AddressMapPutCommand{
		CommandBase: commandBase("address-bad"),
		Mapping:     v1.AddressMapping{VirtualOrigin: "not-an-origin", Recipient: "peer@example.test"},
	}))
	if err := observeCommand(ctx, runtime, "policy_set_positive", v1.PolicySetCommand{
		CommandBase: commandBase("policy-set"),
		Rules:       []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: "/orders/create", AgentID: "peer@example.test"}},
	}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, runtime, "policy_get_positive", v1.PolicyGetCommand{CommandBase: commandBase("policy-get")}, func(result v1.Result, observed *observation) {
		policy, ok := result.(v1.PolicyResult)
		if ok {
			observed.Count = len(policy.Rules)
		}
	}); err != nil {
		return err
	}
	policyPayload := v1.Payload{Value: v1.NativePayload{ContentType: "application/json", Path: "/orders/create", Body: []byte(`{"id":1}`)}}
	for _, test := range []struct {
		scenario string
		to       v1.AgentID
	}{
		{scenario: "policy_test_allow_positive", to: "peer@example.test"},
		{scenario: "policy_test_deny_positive", to: "other@example.test"},
	} {
		if _, _, err := observeCommandResult(ctx, runtime, test.scenario, v1.PolicyTestCommand{
			CommandBase: commandBase(test.scenario), Input: v1.PolicyTestInput{To: test.to, Payload: policyPayload},
		}, func(result v1.Result, observed *observation) {
			policy, ok := result.(v1.PolicyResult)
			if ok {
				observed.Count = len(policy.Rules)
				observed.Allowed = policy.Allowed
			}
		}); err != nil {
			return err
		}
	}
	emit(observeSubmissionError(ctx, runtime, "policy_malformed_action_negative", v1.PolicySetCommand{
		CommandBase: commandBase("policy-bad"),
		Rules:       []v1.PolicyRule{{Action: v1.PolicyAction("unexpected"), Path: "/", AgentID: "peer@example.test"}},
	}))

	for _, test := range []struct {
		scenario string
		command  v1.Command
	}{
		{scenario: "handler_register_positive", command: v1.HandlerRegisterCommand{CommandBase: commandBase("handler-register"), Path: "/orders/create"}},
		{scenario: "handler_register_idempotent_positive", command: v1.HandlerRegisterCommand{CommandBase: commandBase("handler-register-again"), Path: "/orders/create"}},
		{scenario: "handler_unregister_positive", command: v1.HandlerUnregisterCommand{CommandBase: commandBase("handler-unregister"), Path: "/orders/create"}},
		{scenario: "handler_unregister_idempotent_positive", command: v1.HandlerUnregisterCommand{CommandBase: commandBase("handler-unregister-again"), Path: "/orders/create"}},
		{scenario: "diagnostics_logs_enable_positive", command: v1.DiagnosticsLogsCommand{CommandBase: commandBase("logs-enable"), Enabled: true}},
		{scenario: "diagnostics_logs_disable_positive", command: v1.DiagnosticsLogsCommand{CommandBase: commandBase("logs-disable"), Enabled: false}},
		{scenario: "delivery_pause_positive", command: v1.DeliveryPauseCommand{CommandBase: commandBase("delivery-pause")}},
	} {
		if err := observeCommand(ctx, runtime, test.scenario, test.command); err != nil {
			return err
		}
	}
	if _, _, err := observeCommandResult(ctx, runtime, "delivery_status_paused_positive", v1.DeliveryQueueStatusCommand{CommandBase: commandBase("delivery-status-paused")}, func(result v1.Result, observed *observation) {
		status, ok := result.(v1.DeliveryQueueStatus)
		if ok {
			observed.Queued, observed.Paused = status.Queued, status.Paused
		}
	}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "delivery_next_paused_negative", v1.DeliveryNextCommand{CommandBase: commandBase("delivery-next-paused")}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "delivery_resume_positive", v1.DeliveryResumeCommand{CommandBase: commandBase("delivery-resume")}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "delivery_next_empty_negative", v1.DeliveryNextCommand{CommandBase: commandBase("delivery-next-empty")}); err != nil {
		return err
	}
	if err := observeCommand(ctx, runtime, "delivery_accept_unknown_negative", v1.DeliveryAcceptCommand{CommandBase: commandBase("delivery-accept-unknown"), EventID: "event-unknown"}); err != nil {
		return err
	}

	handle, err := runtime.PayloadOpen(ctx)
	if err != nil {
		return fmt.Errorf("payload open: %w", err)
	}
	payload, err := hex.DecodeString("a500010100026003612f044178")
	if err != nil {
		return fmt.Errorf("decode canonical payload fixture: %w", err)
	}
	if _, err := runtime.PayloadWrite(ctx, handle, payload); err != nil {
		return fmt.Errorf("payload write: %w", err)
	}
	if _, err := runtime.PayloadFinish(ctx, handle); err != nil {
		return fmt.Errorf("payload finish: %w", err)
	}
	got, eof, err := runtime.PayloadRead(ctx, handle, 0, len(payload))
	if err != nil {
		return fmt.Errorf("payload read: %w", err)
	}
	emit(observation{Scenario: "payload_lifecycle_positive", PayloadBytes: len(got), PayloadEOF: eof})
	if err := runtime.PayloadRelease(ctx, handle); err != nil {
		return fmt.Errorf("payload release: %w", err)
	}
	emit(withError(observation{Scenario: "payload_stale_release_negative"}, runtime.PayloadRelease(ctx, handle)))

	if err := shutdownAndDrain(ctx, runtime); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	closed, err := runtime.Status(ctx)
	if err != nil {
		return fmt.Errorf("closed status: %w", err)
	}
	emit(observation{Scenario: "lifecycle_shutdown_positive", Lifecycle: string(closed.Lifecycle)})
	if err := runtime.Destroy(); err != nil {
		return fmt.Errorf("destroy: %w", err)
	}
	if err := runtime.Destroy(); err != nil {
		return fmt.Errorf("second destroy: %w", err)
	}
	emit(observation{Scenario: "lifecycle_destroy_idempotent_positive", CompletionOK: true})
	_, err = runtime.Status(ctx)
	emit(withError(observation{Scenario: "lifecycle_post_destroy_negative"}, err))
	return nil
}

func runAuthNegative() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime, err := core.New(core.Config{QueueLimit: 8, PayloadLimit: 1 << 20})
	if err != nil {
		return fmt.Errorf("create core: %w", err)
	}
	if err := runtime.Start(ctx); err != nil {
		return fmt.Errorf("start core: %w", err)
	}

	invalidEndpoints := []struct {
		scenario string
		endpoint string
	}{
		{scenario: "auth_endpoint_scheme_negative", endpoint: "tls://mesh.example.test:5222"},
		{scenario: "auth_endpoint_trailing_dot_negative", endpoint: "mesh.example.test.:5222"},
		{scenario: "auth_endpoint_unicode_negative", endpoint: "m\u00e9sh.example.test:5222"},
		{scenario: "auth_endpoint_zone_negative", endpoint: "[fe80::1%lo0]:5222"},
		{scenario: "auth_endpoint_missing_port_negative", endpoint: "mesh.example.test"},
		{scenario: "auth_endpoint_zero_port_negative", endpoint: "mesh.example.test:0"},
		{scenario: "auth_endpoint_large_port_negative", endpoint: "mesh.example.test:65536"},
	}
	for index, test := range invalidEndpoints {
		command := authConnect("invalid-endpoint-"+fmt.Sprint(index), test.endpoint)
		emit(observeSubmissionError(ctx, runtime, test.scenario, command))
	}

	if err := observeCommand(ctx, runtime, "auth_server_unavailable_negative", authConnect("unavailable", "127.0.0.1:1")); err != nil {
		return err
	}
	status, err := runtime.Status(ctx)
	if err != nil {
		return fmt.Errorf("status after failed auth: %w", err)
	}
	emit(observation{Scenario: "auth_failure_state_positive", Lifecycle: string(status.Lifecycle), ResultKind: string(status.Personality)})
	if err := shutdownAndDrain(ctx, runtime); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return runtime.Destroy()
}

func runAuthReal() error {
	endpoint := requiredEnvironment("CYNAPSA_EJABBERD_ENDPOINT")
	agent := requiredEnvironment("CYNAPSA_EJABBERD_AGENT_A")
	password := requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A")
	peer := requiredEnvironment("CYNAPSA_EJABBERD_AGENT_B")
	peerPassword := requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_B")
	if endpoint == "" || agent == "" || password == "" || peer == "" || peerPassword == "" {
		return fmt.Errorf("real authentication requires the disposable service environment")
	}
	const meshID = "e2e-mesh"

	wrong, err := realCore()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := observeCommand(ctx, wrong, "auth_bad_credentials_negative", realAuthCommand("bad-credentials", endpoint, agent, "incorrect-synthetic-secret", meshID)); err != nil {
		return err
	}
	status, err := wrong.Status(ctx)
	if err != nil {
		return fmt.Errorf("bad-credential status: %w", err)
	}
	emit(observation{Scenario: "auth_bad_credentials_state_positive", Lifecycle: string(status.Lifecycle), ResultKind: string(status.Personality)})
	if err := shutdownAndDrain(ctx, wrong); err != nil {
		return fmt.Errorf("bad-credential shutdown: %w", err)
	}
	if err := wrong.Destroy(); err != nil {
		return fmt.Errorf("bad-credential destroy: %w", err)
	}

	authenticated, err := realCore()
	if err != nil {
		return err
	}
	if err := observeCommand(ctx, authenticated, "auth_connect_real_positive", realAuthCommand("real-auth", endpoint, agent, password, meshID)); err != nil {
		return err
	}
	receiver, err := realCore()
	if err != nil {
		return err
	}
	if err := observeCommand(ctx, receiver, "auth_receiver_real_positive", realAuthCommand("receiver-auth", endpoint, peer, peerPassword, meshID)); err != nil {
		return err
	}
	status, err = authenticated.Status(ctx)
	if err != nil {
		return fmt.Errorf("authenticated status: %w", err)
	}
	emit(observation{Scenario: "auth_ready_status_positive", Lifecycle: string(status.Lifecycle), ResultKind: string(status.Personality)})
	if err := observeCommand(ctx, authenticated, "auth_agent_id_real_positive", v1.AuthAgentIDCommand{CommandBase: commandBase("agent-id")}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, authenticated, "diagnostics_connectivity_real_positive", v1.DiagnosticsConnectivityCommand{CommandBase: commandBase("connectivity-status")}, func(result v1.Result, observed *observation) {
		connectivity, ok := result.(v1.ConnectivityStatus)
		if ok {
			observed.Value = string(connectivity.State)
		}
	}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, authenticated, "mesh_list_real_positive", v1.MeshListCommand{CommandBase: commandBase("mesh-list")}, func(result v1.Result, observed *observation) {
		meshes, ok := result.(v1.MeshListResult)
		if ok && len(meshes.Meshes) == 1 {
			observed.Count = 1
			observed.Value = string(meshes.Meshes[0].MeshID) + "|" + fmt.Sprint(meshes.Meshes[0].Active)
		}
	}); err != nil {
		return err
	}
	if err := observeCommand(ctx, authenticated, "mesh_refresh_real_positive", v1.MeshMembershipRefreshCommand{CommandBase: commandBase("mesh-refresh")}); err != nil {
		return err
	}
	if _, _, err := observeCommandResult(ctx, authenticated, "diagnostics_snapshot_real_positive", v1.DiagnosticsSnapshotCommand{CommandBase: commandBase("diagnostics-snapshot")}, func(result v1.Result, observed *observation) {
		if snapshot, ok := result.(v1.DiagnosticSnapshot); ok {
			observed.Value = fmt.Sprintf("%d|%d|%d|%d", snapshot.PeerCount, snapshot.QueuedMessageCount, snapshot.PendingRPCCount, snapshot.PayloadTransferCount)
			observed.Queued = snapshot.Status.QueuedMessageCount
		}
	}); err != nil {
		return err
	}
	for _, setup := range []struct {
		runtime  *core.Core
		scenario string
		id       string
		peer     string
	}{
		{runtime: authenticated, scenario: "sender_policy_real_positive", id: "sender-policy", peer: peer},
		{runtime: receiver, scenario: "receiver_policy_real_positive", id: "receiver-policy", peer: agent},
	} {
		if err := observeCommand(ctx, setup.runtime, setup.scenario, v1.PolicySetCommand{
			CommandBase: commandBase(setup.id), Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, AgentID: v1.AgentID(setup.peer)}},
		}); err != nil {
			return err
		}
	}

	largeBody := make([]byte, 96<<10)
	for index := range largeBody {
		largeBody[index] = byte(index % 251)
	}
	messagePayload := v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/e2e/message", Body: largeBody}}
	if _, _, err := observeCommandResult(ctx, authenticated, "public_same_mesh_send_positive", v1.MessageSendCommand{CommandBase: commandBase("same-mesh-send"), To: v1.AgentID(peer), Payload: messagePayload}, func(result v1.Result, observed *observation) {
		if sent, ok := result.(v1.SendResult); ok {
			observed.Allowed = sent.Accepted
		}
	}); err != nil {
		return err
	}
	messageEvent, message, err := nextPublicMessage(ctx, receiver, v1.MessageModeOneWay)
	if err != nil {
		return fmt.Errorf("same-mesh message receive: %w", err)
	}
	receivedNative, ok := message.Payload.Value.(v1.NativePayload)
	if !ok || !bytes.Equal(receivedNative.Body, largeBody) {
		return errors.New("same-mesh large payload changed across private fallback")
	}
	emit(observePublicMessage("public_same_mesh_message_received_positive", message))
	if err := observeCommand(ctx, receiver, "public_same_mesh_message_accept_positive", v1.DeliveryAcceptCommand{CommandBase: commandBase("same-mesh-message-accept"), EventID: messageEvent.ID}); err != nil {
		return err
	}
	peerStatus, err := waitForRank1Peer(ctx, authenticated, peer)
	if err != nil {
		return fmt.Errorf("peer establishment: %w", err)
	}
	emit(observation{Scenario: "diagnostics_peer_available_positive", ResultKind: "peer_status", Value: string(peerStatus.Connectivity), Allowed: peerStatus.Reachable && !peerStatus.RecoveryInProgress})

	requestPayload := v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/e2e/request", Body: []byte("public-rank2-rpc")}}
	requestAdmission, err := authenticated.Submit(ctx, v1.MessageRequestCommand{CommandBase: commandBase("same-mesh-request"), To: v1.AgentID(peer), Payload: requestPayload, TTL: 20 * time.Second})
	if err != nil || !requestAdmission.Accepted {
		return fmt.Errorf("same-mesh request admission: %#v, %w", requestAdmission, err)
	}
	requestEvent, request, err := nextPublicMessage(ctx, receiver, v1.MessageModeRequest)
	if err != nil {
		return fmt.Errorf("same-mesh request receive: %w", err)
	}
	requestObservation := observePublicMessage("public_same_mesh_request_received_positive", request)
	requestObservation.Allowed = request.RequestHandle != ""
	emit(requestObservation)
	if _, _, err := observeCommandResult(ctx, receiver, "diagnostics_pending_rpc_real_positive", v1.DiagnosticsSnapshotCommand{CommandBase: commandBase("diagnostics-pending-rpc")}, func(result v1.Result, observed *observation) {
		if snapshot, ok := result.(v1.DiagnosticSnapshot); ok {
			observed.Value = fmt.Sprintf("%d|%d|%d|%d", snapshot.PeerCount, snapshot.QueuedMessageCount, snapshot.PendingRPCCount, snapshot.PayloadTransferCount)
		}
	}); err != nil {
		return err
	}
	if err := observeCommand(ctx, receiver, "public_same_mesh_request_accept_positive", v1.DeliveryAcceptCommand{CommandBase: commandBase("same-mesh-request-accept"), EventID: requestEvent.ID}); err != nil {
		return err
	}
	replyPayload := v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/e2e/reply", Body: []byte("public-rank2-ok")}}
	if _, _, err := observeCommandResult(ctx, receiver, "public_same_mesh_reply_positive", v1.MessageReplyCommand{CommandBase: commandBase("same-mesh-reply"), RequestHandle: request.RequestHandle, Payload: replyPayload}, func(result v1.Result, observed *observation) {
		if sent, ok := result.(v1.SendResult); ok {
			observed.Allowed = sent.Accepted
		}
	}); err != nil {
		return err
	}
	requestCompletion, err := authenticated.NextCompletion(ctx)
	if err != nil {
		return fmt.Errorf("same-mesh request completion: %w", err)
	}
	requestResult := observation{Scenario: "public_same_mesh_request_positive", Admitted: requestAdmission.Accepted, CompletionOK: requestCompletion.OK, ResultKind: publicResultKind(requestCompletion.Result)}
	conversationID := v1.ConversationID("")
	responseMessageID := v1.MessageID("")
	if response, ok := requestCompletion.Result.(v1.ResponseResult); ok {
		conversationID = response.ConversationID
		responseMessageID = response.MessageID
		requestResult.Value = string(response.FromAgentID)
		if native, nativeOK := response.Payload.Value.(v1.NativePayload); nativeOK {
			requestResult.Value += "|" + native.Path
			requestResult.PayloadBytes = len(native.Body)
		}
	}
	if requestCompletion.Error != nil {
		requestResult = withError(requestResult, requestCompletion.Error)
	}
	emit(requestResult)
	if conversationID == "" || responseMessageID == "" {
		return errors.New("completed request omitted conversation or message identity")
	}
	if _, _, err := observeCommandResult(ctx, authenticated, "conversation_list_real_positive", v1.ConversationListCommand{CommandBase: commandBase("conversation-list")}, func(result v1.Result, observed *observation) {
		if list, ok := result.(v1.ConversationListResult); ok {
			observed.Count = len(list.Conversations)
			for _, item := range list.Conversations {
				if item.ConversationID == conversationID {
					observed.Value = string(item.Peer)
					observed.Allowed = !item.Blocked
				}
			}
		}
	}); err != nil {
		return err
	}
	conversationStatus, err := waitForConversationReady(ctx, authenticated, conversationID)
	if err != nil {
		return err
	}
	emit(observation{
		Scenario: "conversation_status_real_positive", Admitted: true, CompletionOK: true,
		ResultKind: "conversation_status", Value: string(conversationStatus.Peer) + "|" + string(conversationStatus.DeliveryState),
		Queued: conversationStatus.QueuedMessageCount, Allowed: !conversationStatus.Blocked,
	})
	if err := waitForConversationClose(ctx, authenticated, conversationID); err != nil {
		return err
	}
	emit(observation{Scenario: "conversation_close_real_positive", Admitted: true, CompletionOK: true, ResultKind: "empty"})
	if err := observeCommand(ctx, authenticated, "conversation_closed_status_negative", v1.ConversationStatusCommand{CommandBase: commandBase("conversation-status-closed"), ConversationID: conversationID}); err != nil {
		return err
	}
	if err := observeCommand(ctx, authenticated, "delivery_retry_terminal_negative", v1.DeliveryRetryCommand{CommandBase: commandBase("delivery-retry-terminal"), MessageID: responseMessageID}); err != nil {
		return err
	}
	if err := observeCommand(ctx, authenticated, "delivery_drop_terminal_negative", v1.DeliveryDropCommand{CommandBase: commandBase("delivery-drop-terminal"), MessageID: responseMessageID}); err != nil {
		return err
	}

	if err := shutdownAndDrain(ctx, receiver); err != nil {
		return fmt.Errorf("receiver shutdown: %w", err)
	}
	if err := receiver.Destroy(); err != nil {
		return fmt.Errorf("receiver destroy: %w", err)
	}
	if err := observeCommand(ctx, authenticated, "auth_logout_completion_positive", v1.AuthLogoutCommand{CommandBase: commandBase("logout")}); err != nil {
		return err
	}
	_, err = authenticated.Submit(ctx, v1.CoreCapabilitiesCommand{CommandBase: commandBase("after-logout")})
	emit(withError(observation{Scenario: "auth_logout_later_admission_negative"}, err))
	if err := shutdownAndDrain(ctx, authenticated); err != nil {
		return fmt.Errorf("logout shutdown join: %w", err)
	}
	status, err = authenticated.Status(ctx)
	if err != nil {
		return fmt.Errorf("logout closed status: %w", err)
	}
	emit(observation{Scenario: "auth_logout_closed_positive", Lifecycle: string(status.Lifecycle)})
	if err := authenticated.Destroy(); err != nil {
		return fmt.Errorf("logout destroy: %w", err)
	}

	reconnected, err := realCore()
	if err != nil {
		return err
	}
	if err := observeCommand(ctx, reconnected, "auth_reconnect_new_core_positive", realAuthCommand("real-reauth", endpoint, agent, password, meshID)); err != nil {
		return err
	}
	if err := shutdownAndDrain(ctx, reconnected); err != nil {
		return fmt.Errorf("reconnected shutdown: %w", err)
	}
	return reconnected.Destroy()
}

func waitForRank1Peer(ctx context.Context, runtime *core.Core, peer string) (v1.PeerStatus, error) {
	deadline := time.Now().Add(15 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		admission, err := runtime.Submit(ctx, v1.DiagnosticsPeerCommand{CommandBase: commandBase(fmt.Sprintf("peer-status-%d", attempt)), Peer: v1.AgentID(peer)})
		if err != nil || !admission.Accepted {
			return v1.PeerStatus{}, fmt.Errorf("diagnostics admission: %#v, %w", admission, err)
		}
		completion, err := runtime.NextCompletion(ctx)
		if err != nil {
			return v1.PeerStatus{}, err
		}
		if status, ok := completion.Result.(v1.PeerStatus); ok && completion.OK && status.Reachable && status.Connectivity == v1.ConnectivityAvailable {
			return status, nil
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return v1.PeerStatus{}, ctx.Err()
		}
	}
	return v1.PeerStatus{}, errors.New("peer did not become available")
}

func waitForConversationReady(ctx context.Context, runtime *core.Core, conversationID v1.ConversationID) (v1.ConversationStatus, error) {
	deadline := time.Now().Add(15 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		admission, err := runtime.Submit(ctx, v1.ConversationStatusCommand{
			CommandBase: commandBase(fmt.Sprintf("conversation-status-%d", attempt)), ConversationID: conversationID,
		})
		if err != nil || !admission.Accepted {
			return v1.ConversationStatus{}, fmt.Errorf("conversation status admission: %#v, %w", admission, err)
		}
		completion, err := runtime.NextCompletion(ctx)
		if err != nil {
			return v1.ConversationStatus{}, fmt.Errorf("conversation status completion: %w", err)
		}
		status, ok := completion.Result.(v1.ConversationStatus)
		if completion.OK && ok && status.DeliveryState == v1.DeliveryStateReady && status.QueuedMessageCount == 0 {
			return status, nil
		}
		if !completion.OK {
			return v1.ConversationStatus{}, fmt.Errorf("conversation status failed: %v", completion.Error)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return v1.ConversationStatus{}, ctx.Err()
		}
	}
	return v1.ConversationStatus{}, errors.New("conversation did not become ready")
}

func waitForConversationClose(ctx context.Context, runtime *core.Core, conversationID v1.ConversationID) error {
	deadline := time.Now().Add(15 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		admission, err := runtime.Submit(ctx, v1.ConversationCloseCommand{
			CommandBase: commandBase(fmt.Sprintf("conversation-close-%d", attempt)), ConversationID: conversationID,
		})
		if err != nil || !admission.Accepted {
			return fmt.Errorf("conversation close admission: %#v, %w", admission, err)
		}
		completion, err := runtime.NextCompletion(ctx)
		if err != nil {
			return fmt.Errorf("conversation close completion: %w", err)
		}
		if completion.OK {
			return nil
		}
		var public *v1.Error
		if !errors.As(completion.Error, &public) || public.Code != v1.ErrorCodeCommand || public.Stage != v1.ErrorStageDelivery {
			return fmt.Errorf("conversation close failed: %v", completion.Error)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("conversation did not become idle for close")
}

func realCore() (*core.Core, error) {
	runtime, err := core.New(core.Config{QueueLimit: 16, PayloadLimit: 1 << 20})
	if err != nil {
		return nil, fmt.Errorf("real core create: %w", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("real core start: %w", err)
	}
	return runtime, nil
}

func realAuthCommand(id, endpoint, agent, password, meshID string) v1.AuthConnectCommand {
	return v1.AuthConnectCommand{
		CommandBase: commandBase(id),
		Auth:        v1.AuthInput{MeshEndpoint: endpoint, Username: v1.AgentID(agent), Password: password, MeshID: v1.MeshID(meshID), AgentInstanceID: "e2e-process"},
	}
}

func requiredEnvironment(name string) string {
	return os.Getenv(name)
}

func nextPublicMessage(ctx context.Context, runtime *core.Core, mode v1.MessageMode) (v1.Event, v1.MessageReceivedEvent, error) {
	for {
		event, err := runtime.NextEvent(ctx)
		if err != nil {
			return v1.Event{}, v1.MessageReceivedEvent{}, err
		}
		message, ok := event.Payload.(v1.MessageReceivedEvent)
		if event.Name == v1.EventMessageReceived && ok && message.Mode == mode {
			return event, message, nil
		}
	}
}

func observePublicMessage(scenario string, message v1.MessageReceivedEvent) observation {
	result := observation{Scenario: scenario, ResultKind: "event", Value: string(message.FromAgentID) + "|" + string(message.Mode)}
	if native, ok := message.Payload.Value.(v1.NativePayload); ok {
		result.Value += "|" + native.Path
		result.PayloadBytes = len(native.Body)
	}
	return result
}

func runPayloadContract() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	left, err := core.New(core.Config{QueueLimit: 8, PayloadLimit: 13})
	if err != nil {
		return fmt.Errorf("create left core: %w", err)
	}
	right, err := core.New(core.Config{QueueLimit: 8, PayloadLimit: 13})
	if err != nil {
		return fmt.Errorf("create right core: %w", err)
	}
	if err := left.Start(ctx); err != nil {
		return fmt.Errorf("start left core: %w", err)
	}
	if err := right.Start(ctx); err != nil {
		return fmt.Errorf("start right core: %w", err)
	}

	open, err := left.PayloadOpen(ctx)
	if err != nil {
		return fmt.Errorf("open cancellation handle: %w", err)
	}
	_, err = left.PayloadWrite(ctx, open, make([]byte, 14))
	emit(withError(observation{Scenario: "payload_over_limit_negative"}, err))
	if err := left.PayloadCancel(ctx, open); err != nil {
		return fmt.Errorf("first cancel: %w", err)
	}
	if err := left.PayloadCancel(ctx, open); err != nil {
		return fmt.Errorf("second cancel: %w", err)
	}
	emit(observation{Scenario: "payload_cancel_idempotent_positive", CompletionOK: true})
	_, err = left.PayloadWrite(ctx, open, []byte{0})
	emit(withError(observation{Scenario: "payload_write_after_cancel_negative"}, err))
	if err := left.PayloadRelease(ctx, open); err != nil {
		return fmt.Errorf("release cancelled handle: %w", err)
	}
	emit(withError(observation{Scenario: "payload_cancelled_double_release_negative"}, left.PayloadRelease(ctx, open)))

	sealed, err := left.PayloadOpen(ctx)
	if err != nil {
		return fmt.Errorf("open sealed handle: %w", err)
	}
	canonical, err := hex.DecodeString("a500010100026003612f044178")
	if err != nil {
		return fmt.Errorf("decode canonical payload: %w", err)
	}
	if _, err := left.PayloadWrite(ctx, sealed, canonical); err != nil {
		return fmt.Errorf("write sealed handle: %w", err)
	}
	if _, err := left.PayloadFinish(ctx, sealed); err != nil {
		return fmt.Errorf("finish sealed handle: %w", err)
	}
	if err := left.PayloadRetain(ctx, sealed); err != nil {
		return fmt.Errorf("retain sealed handle: %w", err)
	}
	if err := left.PayloadRelease(ctx, sealed); err != nil {
		return fmt.Errorf("release retained reference: %w", err)
	}
	bytes, eof, err := left.PayloadRead(ctx, sealed, 0, len(canonical))
	if err != nil {
		return fmt.Errorf("read retained reference: %w", err)
	}
	emit(observation{Scenario: "payload_retain_release_positive", PayloadBytes: len(bytes), PayloadEOF: eof})
	_, _, err = right.PayloadRead(ctx, sealed, 0, len(canonical))
	emit(withError(observation{Scenario: "payload_cross_core_negative"}, err))
	if err := left.PayloadRelease(ctx, sealed); err != nil {
		return fmt.Errorf("final sealed release: %w", err)
	}
	_, _, err = left.PayloadRead(ctx, sealed, 0, len(canonical))
	emit(withError(observation{Scenario: "payload_stale_read_negative"}, err))
	_, _, err = left.PayloadRead(ctx, v1.PayloadHandle("payh_invalid"), 0, 1)
	emit(withError(observation{Scenario: "payload_malformed_handle_negative"}, err))

	if err := shutdownAndDrain(ctx, left); err != nil {
		return fmt.Errorf("shutdown left: %w", err)
	}
	if err := shutdownAndDrain(ctx, right); err != nil {
		return fmt.Errorf("shutdown right: %w", err)
	}
	if err := left.Destroy(); err != nil {
		return fmt.Errorf("destroy left: %w", err)
	}
	return right.Destroy()
}

func runLifecycleContract() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	invalidConfigs := []struct {
		scenario string
		config   core.Config
	}{
		{scenario: "config_zero_queue_negative", config: core.Config{QueueLimit: 0, PayloadLimit: 1}},
		{scenario: "config_large_queue_negative", config: core.Config{QueueLimit: 65537, PayloadLimit: 1}},
		{scenario: "config_zero_payload_negative", config: core.Config{QueueLimit: 1, PayloadLimit: 0}},
		{scenario: "config_large_payload_negative", config: core.Config{QueueLimit: 1, PayloadLimit: v1.MaximumPayloadBytes + 1}},
		{scenario: "config_negative_timeout_negative", config: core.Config{CommandTimeout: -time.Millisecond, QueueLimit: 1, PayloadLimit: 1}},
	}
	for _, test := range invalidConfigs {
		_, err := core.New(test.config)
		emit(withError(observation{Scenario: test.scenario}, err))
	}

	for iteration := 0; iteration < 20; iteration++ {
		runtime, err := core.New(core.Config{QueueLimit: 4, PayloadLimit: 1 << 20})
		if err != nil {
			return fmt.Errorf("repeat create %d: %w", iteration, err)
		}
		if err := runtime.Start(ctx); err != nil {
			return fmt.Errorf("repeat start %d: %w", iteration, err)
		}
		if err := shutdownAndDrain(ctx, runtime); err != nil {
			return fmt.Errorf("repeat shutdown %d: %w", iteration, err)
		}
		if err := runtime.Destroy(); err != nil {
			return fmt.Errorf("repeat destroy %d: %w", iteration, err)
		}
	}
	emit(observation{Scenario: "lifecycle_repeat_positive", PayloadBytes: 20, CompletionOK: true})

	runtime, err := core.New(core.Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		return fmt.Errorf("create shutdown core: %w", err)
	}
	if err := runtime.Start(ctx); err != nil {
		return fmt.Errorf("start shutdown core: %w", err)
	}
	emit(withError(observation{Scenario: "lifecycle_destroy_before_shutdown_negative"}, runtime.Destroy()))
	admission, err := runtime.Submit(ctx, v1.CoreInitCommand{CommandBase: commandBase("shutdown-pending")})
	if err != nil || !admission.Accepted {
		return fmt.Errorf("submit pending command: %w", err)
	}
	expired, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	shutdownErr := runtime.Shutdown(expired)
	expiredCancel()
	emit(withError(observation{Scenario: "lifecycle_shutdown_timeout_negative"}, shutdownErr))
	_, err = runtime.Submit(ctx, v1.CoreInitCommand{CommandBase: commandBase("after-shutdown")})
	emit(withError(observation{Scenario: "lifecycle_admission_after_shutdown_negative"}, err))
	if err := shutdownAndDrain(ctx, runtime); err != nil {
		return fmt.Errorf("join shutdown: %w", err)
	}
	if err := runtime.Destroy(); err != nil {
		return fmt.Errorf("destroy shutdown core: %w", err)
	}
	emit(observation{Scenario: "lifecycle_shutdown_join_positive", CompletionOK: true})

	terminal, err := core.New(core.Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		return fmt.Errorf("create terminal command core: %w", err)
	}
	if err := terminal.Start(ctx); err != nil {
		return fmt.Errorf("start terminal command core: %w", err)
	}
	if err := observeCommand(ctx, terminal, "core_shutdown_command_completion_positive", v1.CoreShutdownCommand{CommandBase: commandBase("core-shutdown-command")}); err != nil {
		return err
	}
	_, err = terminal.Submit(ctx, v1.CoreCapabilitiesCommand{CommandBase: commandBase("after-core-shutdown-command")})
	emit(withError(observation{Scenario: "core_shutdown_command_late_admission_negative"}, err))
	_, err = terminal.PayloadOpen(ctx)
	emit(withError(observation{Scenario: "core_shutdown_command_direct_payload_negative"}, err))
	if err := shutdownAndDrain(ctx, terminal); err != nil {
		return fmt.Errorf("terminal command shutdown join: %w", err)
	}
	terminalStatus, err := terminal.Status(ctx)
	if err != nil {
		return fmt.Errorf("terminal command closed status: %w", err)
	}
	emit(observation{Scenario: "core_shutdown_command_closed_positive", Lifecycle: string(terminalStatus.Lifecycle)})
	if err := terminal.Destroy(); err != nil {
		return fmt.Errorf("terminal command destroy: %w", err)
	}
	return nil
}

func authConnect(id, endpoint string) v1.AuthConnectCommand {
	return v1.AuthConnectCommand{
		CommandBase: commandBase(id),
		Auth: v1.AuthInput{
			MeshEndpoint:    endpoint,
			Username:        "agent@example.test",
			Password:        "synthetic-test-secret",
			MeshID:          "mesh-test",
			AgentInstanceID: "process-test",
		},
	}
}

func commandBase(id string) v1.CommandBase {
	return v1.CommandBase{CommandID: v1.CommandID("e2e-" + id), SDKSessionID: "e2e-session"}
}

func observeCommand(ctx context.Context, runtime *core.Core, scenario string, command v1.Command) error {
	_, _, err := observeCommandResult(ctx, runtime, scenario, command, nil)
	return err
}

func observeCommandResult(ctx context.Context, runtime *core.Core, scenario string, command v1.Command, inspect func(v1.Result, *observation)) (v1.Admission, v1.Completion, error) {
	admission, err := runtime.Submit(ctx, command)
	if err != nil {
		emit(withError(observation{Scenario: scenario}, err))
		return admission, v1.Completion{}, nil
	}
	result := observation{Scenario: scenario, Admitted: admission.Accepted}
	if !admission.Accepted {
		if admission.Error != nil {
			result = withError(result, admission.Error)
		}
		emit(result)
		return admission, v1.Completion{}, nil
	}
	completion, err := runtime.NextCompletion(ctx)
	if err != nil {
		return admission, v1.Completion{}, fmt.Errorf("%s completion: %w", scenario, err)
	}
	result.CompletionOK = completion.OK
	result.ResultKind = publicResultKind(completion.Result)
	if inspect != nil {
		inspect(completion.Result, &result)
	}
	if completion.Error != nil {
		result = withError(result, completion.Error)
	}
	emit(result)
	return admission, completion, nil
}

func observeSubmissionError(ctx context.Context, runtime *core.Core, scenario string, command v1.Command) observation {
	admission, err := runtime.Submit(ctx, command)
	result := observation{Scenario: scenario, Admitted: admission.Accepted}
	if err != nil {
		return withError(result, err)
	}
	if admission.Error != nil {
		return withError(result, admission.Error)
	}
	return result
}

func publicResultKind(result v1.Result) string {
	switch result.(type) {
	case v1.Capabilities:
		return "capabilities"
	case v1.AuthResult:
		return "auth"
	case v1.AgentIDResult:
		return "agent_id"
	case v1.EmptyResult:
		return "empty"
	case v1.PolicyResult:
		return "policy"
	case v1.ConfigResult:
		return "config"
	case v1.CompletionChannelResult:
		return "completion_channel"
	case v1.EventSinkResult:
		return "event_sink"
	case v1.AddressMappingsResult:
		return "address_mappings"
	case v1.AddressResolution:
		return "address_resolution"
	case v1.DeliveryQueueStatus:
		return "delivery_queue_status"
	case v1.EventResult:
		return "event"
	case v1.DiagnosticSnapshot:
		return "diagnostic_snapshot"
	case v1.ConnectivityStatus:
		return "connectivity_status"
	case v1.MeshListResult:
		return "mesh_list"
	case v1.SendResult:
		return "send"
	case v1.ResponseResult:
		return "response"
	case v1.ConversationListResult:
		return "conversation_list"
	case v1.ConversationStatus:
		return "conversation_status"
	default:
		return ""
	}
}

func withError(result observation, err error) observation {
	if err == nil {
		return result
	}
	var public *v1.Error
	if errors.As(err, &public) {
		result.ErrorCode = public.Code
		result.ErrorStage = public.Stage
		return result
	}
	result.ErrorCode = v1.ErrorCodeCore
	return result
}

func emit(value observation) {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}

func shutdownAndDrain(ctx context.Context, runtime *core.Core) error {
	result := make(chan error, 1)
	go func() { result <- runtime.Shutdown(ctx) }()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		wait, cancel := context.WithTimeout(ctx, 2*time.Millisecond)
		_, completionErr := runtime.NextCompletion(wait)
		cancel()
		if !expectedDrainError(completionErr) {
			return completionErr
		}
		wait, cancel = context.WithTimeout(ctx, 2*time.Millisecond)
		_, eventErr := runtime.NextEvent(wait)
		cancel()
		if !expectedDrainError(eventErr) {
			return eventErr
		}
	}
}

func expectedDrainError(err error) bool {
	if err == nil {
		return true
	}
	var public *v1.Error
	return errors.As(err, &public) && (public.Code == v1.ErrorCodeDeliveryTimeout || public.Code == v1.ErrorCodeShutdownInProgress)
}
