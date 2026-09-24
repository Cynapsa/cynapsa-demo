package sdkboundary

import (
	"encoding/json"
	"fmt"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

type errorWire struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Retryable    bool   `json:"retryable"`
	Stage        string `json:"stage"`
	Location     string `json:"local_or_remote"`
	DiagnosticID string `json:"diagnostic_id,omitempty"`
}

type standaloneErrorWire struct {
	ABIVersion uint32    `json:"abi_version"`
	Error      errorWire `json:"error"`
}

type admissionWire struct {
	ABIVersion    uint32     `json:"abi_version"`
	CommandID     string     `json:"command_id"`
	CommandHandle string     `json:"command_handle,omitempty"`
	Accepted      bool       `json:"accepted"`
	Error         *errorWire `json:"error,omitempty"`
}

type completionWire struct {
	ABIVersion uint32          `json:"abi_version"`
	CommandID  string          `json:"command_id"`
	OK         bool            `json:"ok"`
	ResultType string          `json:"result_type,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      *errorWire      `json:"error,omitempty"`
}

type eventWire struct {
	ABIVersion uint32          `json:"abi_version"`
	EventID    string          `json:"event_id"`
	EventName  string          `json:"event_name"`
	CreatedAt  string          `json:"created_at"`
	Payload    json.RawMessage `json:"payload"`
}

type statusWire struct {
	Lifecycle          string `json:"lifecycle"`
	Connectivity       string `json:"connectivity"`
	Personality        string `json:"personality"`
	AgentID            string `json:"agent_id"`
	MeshID             string `json:"mesh_id"`
	MeshEndpoint       string `json:"mesh_endpoint"`
	QueuedMessageCount uint64 `json:"queued_message_count"`
}

type authResultWire struct {
	AgentID                       string `json:"agent_id"`
	MeshID                        string `json:"mesh_id"`
	AgentInstanceID               string `json:"agent_instance_id"`
	Personality                   string `json:"personality"`
	ProfileID                     string `json:"profile_id,omitempty"`
	CredentialExpiresAt           string `json:"credential_expires_at,omitempty"`
	OfflineStartDeadline          string `json:"offline_start_deadline,omitempty"`
	OfflineColdStartTargetSeconds *int64 `json:"offline_cold_start_target_seconds,omitempty"`
	OfflineTargetSatisfied        *bool  `json:"offline_target_satisfied,omitempty"`
	PolicyRevision                *int64 `json:"policy_revision,omitempty"`
	SessionExpiryMode             string `json:"session_expiry_mode,omitempty"`
	PreparationStatus             string `json:"preparation_status,omitempty"`
}

type meshSummaryWire struct {
	MeshID string `json:"mesh_id"`
	Active bool   `json:"active"`
}

type diagnosticSnapshotWire struct {
	Status               statusWire `json:"status"`
	CommandQueueDepth    uint64     `json:"command_queue_depth"`
	EventQueueDepth      uint64     `json:"event_queue_depth"`
	PeerCount            uint64     `json:"peer_count"`
	QueuedMessageCount   uint64     `json:"queued_message_count"`
	PendingRPCCount      uint64     `json:"pending_rpc_count"`
	PayloadTransferCount uint64     `json:"payload_transfer_count"`
}

func (a *Adapter) EncodeABIAdmission(admission v1.Admission) ([]byte, error) {
	if admission.Accepted == (admission.Error != nil) || (!admission.Accepted && admission.CommandHandle != "") {
		return nil, fmt.Errorf("%w: inconsistent admission", ErrMalformedInput)
	}
	if err := requiredBounded("command_id", string(admission.CommandID), maxIdentifierLength); err != nil {
		return nil, err
	}
	if admission.Error != nil && !validPublicError(*admission.Error) {
		return nil, malformed("unnormalized admission error")
	}
	if admission.Accepted {
		if err := validateCommandHandle(admission.CommandHandle); err != nil {
			return nil, err
		}
	}
	return stableMarshal(admissionWire{ABIVersion: v1.CurrentSchemaVersion, CommandID: string(admission.CommandID), CommandHandle: string(admission.CommandHandle), Accepted: admission.Accepted, Error: encodeError(admission.Error)})
}

// EncodeABIError emits one versioned standalone normalized error for native
// operations that do not otherwise return a serialized result document.
func (a *Adapter) EncodeABIError(value v1.Error) ([]byte, error) {
	if !validPublicError(value) {
		return nil, malformed("unnormalized error")
	}
	wire := encodeError(&value)
	return stableMarshal(standaloneErrorWire{ABIVersion: v1.CurrentSchemaVersion, Error: *wire})
}

func (a *Adapter) EncodeABICompletion(completion v1.Completion) ([]byte, error) {
	if completion.OK == (completion.Error != nil) || completion.OK == (completion.Result == nil) {
		return nil, fmt.Errorf("%w: inconsistent completion", ErrMalformedInput)
	}
	if err := requiredBounded("command_id", string(completion.CommandID), maxIdentifierLength); err != nil {
		return nil, err
	}
	if completion.Error != nil && !validPublicError(*completion.Error) {
		return nil, malformed("unnormalized completion error")
	}
	wire := completionWire{ABIVersion: v1.CurrentSchemaVersion, CommandID: string(completion.CommandID), OK: completion.OK, Error: encodeError(completion.Error)}
	if completion.OK {
		resultType, result, err := encodeResult(completion.Result)
		if err != nil {
			return nil, err
		}
		wire.ResultType, wire.Result = resultType, result
	}
	return stableMarshal(wire)
}

func (a *Adapter) EncodeABIEvent(event v1.Event) ([]byte, error) {
	wire, err := encodePublicEvent(event)
	if err != nil {
		return nil, err
	}
	return stableMarshal(wire)
}

func encodePublicEvent(event v1.Event) (eventWire, error) {
	if event.Name == "" || event.CreatedAt.IsZero() || event.Payload == nil {
		return eventWire{}, fmt.Errorf("%w: incomplete event", ErrMalformedInput)
	}
	if err := requiredBounded("event_id", string(event.ID), maxIdentifierLength); err != nil {
		return eventWire{}, err
	}
	payload, err := encodeEventPayload(event.Name, event.Payload)
	if err != nil {
		return eventWire{}, err
	}
	return eventWire{ABIVersion: v1.CurrentSchemaVersion, EventID: string(event.ID), EventName: string(event.Name), CreatedAt: event.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"), Payload: payload}, nil
}

func (a *Adapter) EncodeABIStatus(status v1.Status) ([]byte, error) {
	if err := validatePublicStatus(status); err != nil {
		return nil, err
	}
	return stableMarshal(struct {
		ABIVersion uint32     `json:"abi_version"`
		Status     statusWire `json:"status"`
	}{ABIVersion: v1.CurrentSchemaVersion, Status: publicStatusWire(status)})
}

func encodeError(value *v1.Error) *errorWire {
	if value == nil {
		return nil
	}
	return &errorWire{Code: string(value.Code), Message: value.Message, Retryable: value.Retryable, Stage: string(value.Stage), Location: string(value.Location), DiagnosticID: string(value.DiagnosticID)}
}

func encodeResult(result v1.Result) (string, json.RawMessage, error) {
	marshal := func(name string, value any) (string, json.RawMessage, error) {
		data, err := stableMarshal(value)
		return name, data, err
	}
	switch value := result.(type) {
	case v1.EmptyResult:
		return marshal("empty", struct{}{})
	case v1.Capabilities:
		if len(value.Commands) > maxQueueCapacity || len(value.Features) > maxQueueCapacity {
			return "", nil, ErrInputTooLarge
		}
		for _, command := range value.Commands {
			if !IsPublicCommand(command) {
				return "", nil, fmt.Errorf("%w: unknown capability command", ErrMalformedInput)
			}
		}
		for _, feature := range value.Features {
			if !IsPublicCapability(feature) {
				return "", nil, fmt.Errorf("%w: unknown capability feature", ErrMalformedInput)
			}
		}
		return marshal("capabilities", struct {
			Commands []v1.CommandName `json:"commands"`
			Features []v1.Capability  `json:"features"`
		}{Commands: append([]v1.CommandName(nil), value.Commands...), Features: append([]v1.Capability(nil), value.Features...)})
	case v1.Status:
		if err := validatePublicStatus(value); err != nil {
			return "", nil, err
		}
		return marshal("status", publicStatusWire(value))
	case v1.CoreInitResult:
		if err := requiredBounded("sdk_session_id", string(value.SDKSessionID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		return marshal("core_init", struct {
			SDKSessionID string `json:"sdk_session_id"`
		}{string(value.SDKSessionID)})
	case v1.ConfigResult:
		if value.Config.CommandTimeout <= 0 || value.Config.RPCTimeout <= 0 || value.Config.QueueLimit == 0 || value.Config.QueueLimit > maxQueueCapacity || value.Config.PayloadLimit == 0 || value.Config.PayloadLimit > maxCanonicalPayloadSize {
			return "", nil, malformed("config result")
		}
		commandMS, err := durationMilliseconds(value.Config.CommandTimeout)
		if err != nil {
			return "", nil, err
		}
		rpcMS, err := durationMilliseconds(value.Config.RPCTimeout)
		if err != nil {
			return "", nil, err
		}
		return marshal("config", struct {
			CommandTimeoutMS int64  `json:"command_timeout_ms"`
			RPCTimeoutMS     int64  `json:"rpc_timeout_ms"`
			QueueLimit       uint32 `json:"queue_limit"`
			PayloadLimit     uint64 `json:"payload_limit"`
		}{commandMS, rpcMS, value.Config.QueueLimit, value.Config.PayloadLimit})
	case v1.AuthResult:
		if err := requiredBounded("agent_id", string(value.AgentID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if err := requiredBounded("mesh_id", string(value.MeshID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if len(value.AgentInstanceID) > maxIdentifierLength || (value.Personality != v1.SDKPersonalityHTTPBridge && value.Personality != v1.SDKPersonalityNative) {
			return "", nil, malformed("auth result")
		}
		expiresAt, offlineDeadline := "", ""
		var target *int64
		var satisfied *bool
		var policyRevision *int64
		if !value.CredentialExpiresAt.IsZero() {
			expiresAt = value.CredentialExpiresAt.UTC().Format(time.RFC3339Nano)
		}
		if !value.OfflineStartDeadline.IsZero() {
			offlineDeadline = value.OfflineStartDeadline.UTC().Format(time.RFC3339Nano)
		}
		if value.ProfileID != "" {
			target = &value.OfflineColdStartTargetSeconds
			satisfied = &value.OfflineTargetSatisfied
			policyRevision = &value.PolicyRevision
		}
		return marshal("auth", authResultWire{AgentID: string(value.AgentID), MeshID: string(value.MeshID), AgentInstanceID: value.AgentInstanceID,
			Personality: string(value.Personality), ProfileID: value.ProfileID, CredentialExpiresAt: expiresAt,
			OfflineStartDeadline: offlineDeadline, OfflineColdStartTargetSeconds: target,
			OfflineTargetSatisfied: satisfied, PolicyRevision: policyRevision,
			SessionExpiryMode: value.SessionExpiryMode, PreparationStatus: value.PreparationStatus})
	case v1.AgentIDResult:
		if err := requiredBounded("agent_id", string(value.AgentID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		return marshal("agent_id", struct {
			AgentID string `json:"agent_id"`
		}{string(value.AgentID)})
	case v1.MeshListResult:
		if len(value.Meshes) > maxQueueCapacity {
			return "", nil, ErrInputTooLarge
		}
		for _, item := range value.Meshes {
			if err := requiredBounded("mesh_id", string(item.MeshID), maxIdentifierLength); err != nil {
				return "", nil, err
			}
		}
		items := make([]meshSummaryWire, len(value.Meshes))
		for i, item := range value.Meshes {
			items[i].MeshID, items[i].Active = string(item.MeshID), item.Active
		}
		return marshal("mesh_list", struct {
			Meshes []meshSummaryWire `json:"meshes"`
		}{items})
	case v1.AddressMappingsResult:
		if len(value.Mappings) > maxQueueCapacity {
			return "", nil, ErrInputTooLarge
		}
		for _, item := range value.Mappings {
			if err := validateOrigin(item.VirtualOrigin); err != nil {
				return "", nil, err
			}
			if err := requiredBounded("recipient", string(item.Recipient), maxIdentifierLength); err != nil {
				return "", nil, err
			}
		}
		items := make([]addressPutWire, len(value.Mappings))
		for i, item := range value.Mappings {
			items[i] = addressPutWire{VirtualOrigin: item.VirtualOrigin, Recipient: string(item.Recipient)}
		}
		return marshal("address_mappings", struct {
			Mappings []addressPutWire `json:"mappings"`
		}{items})
	case v1.AddressResolution:
		if err := requiredBounded("recipient", string(value.Recipient), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if err := validatePath(value.Path); err != nil {
			return "", nil, err
		}
		if len(value.Query) > maxPublicString {
			return "", nil, ErrInputTooLarge
		}
		return marshal("address_resolution", struct {
			Recipient string `json:"recipient"`
			Path      string `json:"path"`
			Query     string `json:"query"`
		}{string(value.Recipient), value.Path, value.Query})
	case v1.SendResult:
		if err := requiredBounded("message_id", string(value.MessageID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if err := requiredBounded("conversation_id", string(value.ConversationID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if !value.Accepted {
			return "", nil, malformed("send acceptance")
		}
		return marshal("send", struct {
			MessageID      string `json:"message_id"`
			ConversationID string `json:"conversation_id"`
			Accepted       bool   `json:"accepted"`
		}{string(value.MessageID), string(value.ConversationID), value.Accepted})
	case v1.ResponseResult:
		for field, identifier := range map[string]string{"message_id": string(value.MessageID), "conversation_id": string(value.ConversationID), "from_agent_id": string(value.FromAgentID), "mesh_id": string(value.MeshID)} {
			if err := requiredBounded(field, identifier, maxIdentifierLength); err != nil {
				return "", nil, err
			}
		}
		payload, err := wirePayload(value.Payload)
		if err != nil {
			return "", nil, err
		}
		return marshal("response", struct {
			MessageID      string      `json:"message_id"`
			ConversationID string      `json:"conversation_id"`
			FromAgentID    string      `json:"from_agent_id"`
			MeshID         string      `json:"mesh_id"`
			Payload        payloadWire `json:"payload"`
		}{string(value.MessageID), string(value.ConversationID), string(value.FromAgentID), string(value.MeshID), payload})
	case v1.EventResult:
		event, err := encodePublicEvent(value.Event)
		if err != nil {
			return "", nil, err
		}
		return marshal("event", event)
	case v1.DeliveryQueueStatus:
		if value.Queued > maxQueueCapacity {
			return "", nil, malformed("delivery queue count")
		}
		return marshal("delivery_queue_status", struct {
			Queued uint64 `json:"queued"`
			Paused bool   `json:"paused"`
		}{value.Queued, value.Paused})
	case v1.PayloadHandleResult:
		if err := validatePayloadHandle(value.Handle); err != nil {
			return "", nil, err
		}
		if value.Size > maxCanonicalPayloadSize || len(value.Chunk) > maxPayloadChunkBytes || uint64(len(value.Chunk)) > value.Size {
			return "", nil, ErrInputTooLarge
		}
		return marshal("payload_handle", struct {
			Handle string `json:"handle"`
			Size   uint64 `json:"size"`
			EOF    bool   `json:"eof"`
			Chunk  []byte `json:"chunk"`
		}{string(value.Handle), value.Size, value.EOF, append([]byte(nil), value.Chunk...)})
	case v1.ConversationStatus:
		if err := validatePublicConversation(value); err != nil {
			return "", nil, err
		}
		return marshal("conversation_status", wireConversation(value))
	case v1.ConversationListResult:
		if len(value.Conversations) > maxQueueCapacity {
			return "", nil, ErrInputTooLarge
		}
		for _, item := range value.Conversations {
			if err := validatePublicConversation(item); err != nil {
				return "", nil, err
			}
		}
		items := make([]conversationWire, len(value.Conversations))
		for i, item := range value.Conversations {
			items[i] = wireConversation(item)
		}
		return marshal("conversation_list", struct {
			Conversations []conversationWire `json:"conversations"`
		}{items})
	case v1.PolicyResult:
		if err := validatePublicPolicyRules(value.Rules); err != nil {
			return "", nil, err
		}
		return marshal("policy", struct {
			Rules   []policyRuleWire `json:"rules"`
			Allowed bool             `json:"allowed"`
		}{wireRules(value.Rules), value.Allowed})
	case v1.PeerStatus:
		if err := requiredBounded("peer", string(value.Peer), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		if !isConnectivity(value.Connectivity) {
			return "", nil, malformed("peer connectivity")
		}
		return marshal("peer_status", struct {
			Peer               string `json:"peer"`
			Connectivity       string `json:"connectivity"`
			Reachable          bool   `json:"reachable"`
			RecoveryInProgress bool   `json:"recovery_in_progress"`
		}{string(value.Peer), string(value.Connectivity), value.Reachable, value.RecoveryInProgress})
	case v1.ConnectivityStatus:
		if !isConnectivity(value.State) {
			return "", nil, malformed("connectivity state")
		}
		return marshal("connectivity_status", struct {
			State string `json:"state"`
		}{string(value.State)})
	case v1.DiagnosticSnapshot:
		if err := validatePublicDiagnosticSnapshot(value); err != nil {
			return "", nil, err
		}
		return marshal("diagnostic_snapshot", diagnosticWire(value))
	case v1.CompletionChannelResult:
		if value.MaxInFlight == 0 || value.MaxInFlight > maxQueueCapacity {
			return "", nil, malformed("max in flight")
		}
		if err := requiredBounded("channel_id", string(value.ChannelID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		return marshal("completion_channel", struct {
			ChannelID   string `json:"channel_id"`
			MaxInFlight uint32 `json:"max_in_flight"`
		}{string(value.ChannelID), value.MaxInFlight})
	case v1.EventSinkResult:
		if err := requiredBounded("sink_id", string(value.SinkID), maxIdentifierLength); err != nil {
			return "", nil, err
		}
		return marshal("event_sink", struct {
			SinkID string `json:"sink_id"`
		}{string(value.SinkID)})
	default:
		return "", nil, fmt.Errorf("%w: unknown result type", ErrMalformedInput)
	}
}

type conversationWire struct {
	ConversationID     string `json:"conversation_id"`
	MeshID             string `json:"mesh_id"`
	Peer               string `json:"peer"`
	DeliveryState      string `json:"delivery_state"`
	QueuedMessageCount uint64 `json:"queued_message_count"`
	Blocked            bool   `json:"blocked"`
}

func wireConversation(value v1.ConversationStatus) conversationWire {
	return conversationWire{string(value.ConversationID), string(value.MeshID), string(value.Peer), string(value.DeliveryState), value.QueuedMessageCount, value.Blocked}
}

func validatePublicStatus(value v1.Status) error {
	if !isLifecycle(value.Lifecycle) || !isConnectivity(value.Connectivity) || !isPersonality(value.Personality) ||
		len(value.AgentID) > maxIdentifierLength || len(value.MeshID) > maxIdentifierLength || len(value.MeshEndpoint) > maxPublicString || value.QueuedMessageCount > maxQueueCapacity {
		return malformed("status")
	}
	return nil
}

func validatePublicConversation(value v1.ConversationStatus) error {
	for field, identifier := range map[string]string{"conversation_id": string(value.ConversationID), "mesh_id": string(value.MeshID), "peer": string(value.Peer)} {
		if err := requiredBounded(field, identifier, maxIdentifierLength); err != nil {
			return err
		}
	}
	state, known := publicDeliveryState(string(value.DeliveryState))
	if !known || state != value.DeliveryState || value.QueuedMessageCount > maxQueueCapacity {
		return malformed("conversation")
	}
	return nil
}

func validatePublicPolicyRules(values []v1.PolicyRule) error {
	if len(values) > maxPolicyRules {
		return ErrInputTooLarge
	}
	for _, value := range values {
		if value.Action != v1.PolicyActionAllow && value.Action != v1.PolicyActionDeny {
			return malformed("policy action")
		}
		if err := validatePolicyPath(value.Path); err != nil {
			return err
		}
		if len(value.AgentID) > maxIdentifierLength {
			return ErrInputTooLarge
		}
	}
	return nil
}

func validatePublicDiagnosticSnapshot(value v1.DiagnosticSnapshot) error {
	if err := validatePublicStatus(value.Status); err != nil {
		return err
	}
	if value.CommandQueueDepth > maxQueueCapacity || value.EventQueueDepth > maxQueueCapacity || value.PeerCount > maxQueueCapacity || value.QueuedMessageCount > maxQueueCapacity || value.PendingRPCCount > maxQueueCapacity || value.PayloadTransferCount > maxQueueCapacity {
		return malformed("diagnostic counter")
	}
	return nil
}
func wirePayload(value v1.Payload) (payloadWire, error) {
	if err := validatePublicPayload(value); err != nil {
		return payloadWire{}, err
	}
	var output payloadWire
	switch payload := value.Value.(type) {
	case v1.NativePayload:
		output.Native = &nativePayloadWire{ContentType: payload.ContentType, Path: payload.Path, Body: wireBody(payload.Body)}
	case v1.HTTPRequestPayload:
		output.HTTPRequest = &httpRequestPayloadWire{Method: payload.Method, Path: payload.Path, Query: payload.Query, Headers: wireHeaders(payload.Headers), Body: wireBody(payload.Body)}
	case v1.HTTPResponsePayload:
		output.HTTPResponse = &httpResponsePayloadWire{StatusCode: payload.StatusCode, Reason: payload.Reason, Headers: wireHeaders(payload.Headers), Body: wireBody(payload.Body), Error: wireApplicationError(payload.Error)}
	case v1.PayloadHandle:
		handle := string(payload)
		output.Handle = &handle
	}
	return output, nil
}
func wireBody(value []byte) []byte {
	// The canonical ABI encodes an empty body as base64 text "", never null.
	return append([]byte{}, value...)
}
func wireHeaders(values []v1.Header) []headerWire {
	output := make([]headerWire, len(values))
	for i, value := range values {
		output[i] = headerWire{Name: value.Name, Value: value.Value}
	}
	return output
}
func wireRules(values []v1.PolicyRule) []policyRuleWire {
	output := make([]policyRuleWire, len(values))
	for i, value := range values {
		output[i] = policyRuleWire{Action: string(value.Action), Path: value.Path, AgentID: string(value.AgentID)}
	}
	return output
}
func publicStatusWire(value v1.Status) statusWire {
	return statusWire{string(value.Lifecycle), string(value.Connectivity), string(value.Personality), string(value.AgentID), string(value.MeshID), value.MeshEndpoint, value.QueuedMessageCount}
}

func diagnosticWire(value v1.DiagnosticSnapshot) diagnosticSnapshotWire {
	return diagnosticSnapshotWire{publicStatusWire(value.Status), value.CommandQueueDepth, value.EventQueueDepth, value.PeerCount, value.QueuedMessageCount, value.PendingRPCCount, value.PayloadTransferCount}
}

func encodeEventPayload(name v1.EventName, payload v1.EventPayload) (json.RawMessage, error) {
	marshal := func(value any) (json.RawMessage, error) { return stableMarshal(value) }
	switch value := payload.(type) {
	case v1.SessionStateChangedEvent:
		if name != v1.EventSessionStateChanged || !isLifecycle(value.Previous) || !isLifecycle(value.Current) {
			break
		}
		return marshal(struct {
			Previous string `json:"previous"`
			Current  string `json:"current"`
		}{string(value.Previous), string(value.Current)})
	case v1.ConnectivityChangedEvent:
		if name != v1.EventConnectivityStateChanged || !isConnectivity(value.Previous) || !isConnectivity(value.Current) {
			break
		}
		return marshal(struct {
			Previous string `json:"previous"`
			Current  string `json:"current"`
		}{string(value.Previous), string(value.Current)})
	case v1.PeerReachabilityEvent:
		if (name != v1.EventPeerReachable && name != v1.EventPeerUnreachable) || (name == v1.EventPeerReachable) != value.Reachable {
			break
		}
		if err := requiredBounded("peer", string(value.Peer), maxIdentifierLength); err != nil {
			return nil, err
		}
		return marshal(struct {
			Peer      string `json:"peer"`
			Reachable bool   `json:"reachable"`
		}{string(value.Peer), value.Reachable})
	case v1.MessageReceivedEvent:
		if name != v1.EventMessageReceived {
			break
		}
		if value.Mode != v1.MessageModeOneWay && value.Mode != v1.MessageModeRequest {
			break
		}
		if (value.Mode == v1.MessageModeRequest && value.RequestHandle == "") || (value.Mode == v1.MessageModeOneWay && value.RequestHandle != "") {
			break
		}
		for field, identifier := range map[string]string{"message_id": string(value.MessageID), "conversation_id": string(value.ConversationID), "from_agent_id": string(value.FromAgentID), "mesh_id": string(value.MeshID)} {
			if err := requiredBounded(field, identifier, maxIdentifierLength); err != nil {
				return nil, err
			}
		}
		if value.Mode == v1.MessageModeRequest {
			if err := validateRequestHandle(value.RequestHandle); err != nil {
				return nil, err
			}
		}
		wire, err := wirePayload(value.Payload)
		if err != nil {
			return nil, err
		}
		return marshal(struct {
			MessageID      string      `json:"message_id"`
			ConversationID string      `json:"conversation_id"`
			FromAgentID    string      `json:"from_agent_id"`
			MeshID         string      `json:"mesh_id"`
			Mode           string      `json:"mode"`
			RequestHandle  string      `json:"request_handle"`
			Payload        payloadWire `json:"payload"`
		}{string(value.MessageID), string(value.ConversationID), string(value.FromAgentID), string(value.MeshID), string(value.Mode), string(value.RequestHandle), wire})
	case v1.MessageStateEvent:
		if expected, ok := expectedMessageEventState(name); ok && value.State == expected {
			if err := requiredBounded("message_id", string(value.MessageID), maxIdentifierLength); err != nil {
				return nil, err
			}
			if err := requiredBounded("conversation_id", string(value.ConversationID), maxIdentifierLength); err != nil {
				return nil, err
			}
			return marshal(struct {
				MessageID      string `json:"message_id"`
				ConversationID string `json:"conversation_id"`
				State          string `json:"state"`
			}{string(value.MessageID), string(value.ConversationID), string(value.State)})
		}
	case v1.PayloadTransferEvent:
		if expected, ok := expectedPayloadEventState(name); ok && value.State == expected {
			if value.Completed > value.Total || value.Total > maxCanonicalPayloadSize {
				break
			}
			if err := validatePayloadHandle(value.Handle); err != nil {
				return nil, err
			}
			return marshal(struct {
				Handle    string `json:"handle"`
				State     string `json:"state"`
				Completed uint64 `json:"completed"`
				Total     uint64 `json:"total"`
			}{string(value.Handle), string(value.State), value.Completed, value.Total})
		}
	case v1.PolicyRejectedEvent:
		if name != v1.EventPolicyRejected {
			break
		}
		if err := requiredBounded("message_id", string(value.MessageID), maxIdentifierLength); err != nil {
			return nil, err
		}
		if err := requiredBounded("peer", string(value.Peer), maxIdentifierLength); err != nil {
			return nil, err
		}
		if err := validatePath(value.Path); err != nil {
			return nil, err
		}
		return marshal(struct {
			MessageID string `json:"message_id"`
			Peer      string `json:"peer"`
			Path      string `json:"path"`
		}{string(value.MessageID), string(value.Peer), value.Path})
	case v1.RPCTimeoutEvent:
		if name != v1.EventRPCTimeout {
			break
		}
		if err := requiredBounded("command_id", string(value.CommandID), maxIdentifierLength); err != nil {
			return nil, err
		}
		return marshal(struct {
			CommandID string `json:"command_id"`
		}{string(value.CommandID)})
	case v1.QueueCapacityEvent:
		if (name != v1.EventCommandQueueFull && name != v1.EventEventQueueFull) ||
			(name == v1.EventCommandQueueFull && value.Queue != v1.QueueNameCommand) ||
			(name == v1.EventEventQueueFull && value.Queue != v1.QueueNameEvent) ||
			value.Capacity == 0 || value.Capacity > maxQueueCapacity {
			break
		}
		return marshal(struct {
			Queue    string `json:"queue"`
			Capacity uint32 `json:"capacity"`
		}{string(value.Queue), value.Capacity})
	case v1.CoreErrorEvent:
		if name != v1.EventCoreError || !validPublicError(value.Error) {
			break
		}
		return marshal(struct {
			Error *errorWire `json:"error"`
		}{encodeError(&value.Error)})
	case v1.DiagnosticLogEvent:
		if name != v1.EventDiagnosticLog || !isDiagnosticLogLevel(value.Level) || !isDiagnosticLogCode(value.Code) || len(value.Message) == 0 || len(value.Message) > maxPublicString {
			break
		}
		if value.DiagnosticID != "" && publicDiagnosticID(string(value.DiagnosticID)) == "" {
			break
		}
		_, expectedMessage, known := publicDiagnosticLog(string(value.Code))
		if !known || value.Message != expectedMessage {
			break
		}
		return marshal(struct {
			Level        string `json:"level"`
			Code         string `json:"code"`
			Message      string `json:"message"`
			DiagnosticID string `json:"diagnostic_id,omitempty"`
		}{string(value.Level), string(value.Code), value.Message, string(value.DiagnosticID)})
	}
	return nil, fmt.Errorf("%w: event name and payload mismatch", ErrMalformedInput)
}

func validPublicError(value v1.Error) bool {
	code, message := publicErrorCode(string(value.Code))
	if code != value.Code || message != value.Message || !isPublicErrorStage(value.Stage) || !isPublicErrorLocation(value.Location) {
		return false
	}
	return value.DiagnosticID == "" || publicDiagnosticID(string(value.DiagnosticID)) != ""
}

func isPublicErrorStage(value v1.ErrorStage) bool {
	switch value {
	case v1.ErrorStageSDK, v1.ErrorStageCommand, v1.ErrorStageAuth, v1.ErrorStagePolicy, v1.ErrorStageConnectivity, v1.ErrorStageDelivery, v1.ErrorStagePayload, v1.ErrorStageHandler, v1.ErrorStageRPC, v1.ErrorStageShutdown:
		return true
	default:
		return false
	}
}

func isPublicErrorLocation(value v1.ErrorLocation) bool {
	return value == v1.ErrorLocationLocal || value == v1.ErrorLocationRemote
}
func isDiagnosticLogLevel(value v1.DiagnosticLogLevel) bool {
	switch value {
	case v1.DiagnosticLogDebug, v1.DiagnosticLogInfo, v1.DiagnosticLogWarn, v1.DiagnosticLogError:
		return true
	}
	return false
}
func isDiagnosticLogCode(value v1.DiagnosticLogCode) bool {
	switch value {
	case v1.DiagnosticLogCoreState, v1.DiagnosticLogConnectivity, v1.DiagnosticLogQueuePressure, v1.DiagnosticLogPayloadTransfer:
		return true
	}
	return false
}

func isLifecycle(value v1.LifecycleState) bool {
	switch value {
	case v1.LifecycleCreated, v1.LifecycleConnecting, v1.LifecycleReady, v1.LifecycleDegraded, v1.LifecycleClosing, v1.LifecycleClosed, v1.LifecycleFailed:
		return true
	}
	return false
}
func isConnectivity(value v1.ConnectivityState) bool {
	switch value {
	case v1.ConnectivityUnknown, v1.ConnectivityAvailable, v1.ConnectivityDegraded, v1.ConnectivityUnavailable:
		return true
	}
	return false
}
func isPersonality(value v1.SDKPersonality) bool {
	switch value {
	case v1.SDKPersonalityUnset, v1.SDKPersonalityHTTPBridge, v1.SDKPersonalityNative:
		return true
	}
	return false
}
