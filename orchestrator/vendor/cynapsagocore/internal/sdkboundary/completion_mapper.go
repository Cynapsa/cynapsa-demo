package sdkboundary

import (
	"fmt"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapCompletion explicitly projects every frozen internal result variant.
func (a *Adapter) MapCompletion(result model.Result) (v1.Completion, error) {
	if err := validateProjectedIdentifier("command_id", result.CommandID); err != nil {
		return v1.Completion{}, err
	}
	completion := v1.Completion{CommandID: v1.CommandID(result.CommandID), OK: result.Err == nil, Error: a.MapError(result.Err)}
	if result.Err != nil {
		if result.Value != nil {
			return v1.Completion{}, fmt.Errorf("%w: result with error", ErrMalformedInput)
		}
		return completion, nil
	}
	if result.Value == nil {
		return v1.Completion{}, fmt.Errorf("%w: missing result", ErrMalformedInput)
	}
	value, err := a.mapResultValue(result.Value)
	if err != nil {
		return v1.Completion{}, err
	}
	completion.Result = value
	return completion, nil
}

func (a *Adapter) mapResultValue(input model.ResultValue) (v1.Result, error) {
	switch value := input.(type) {
	case model.EmptyResult:
		return v1.EmptyResult{}, nil
	case model.CapabilitiesResult:
		if len(value.Commands) > maxQueueCapacity || len(value.Features) > maxQueueCapacity {
			return nil, ErrInputTooLarge
		}
		for _, item := range value.Commands {
			if !IsPublicCommand(v1.CommandName(item)) {
				return nil, fmt.Errorf("%w: private capability", ErrMalformedInput)
			}
		}
		for _, item := range value.Features {
			if !IsPublicCapability(v1.Capability(item)) {
				return nil, fmt.Errorf("%w: private capability", ErrMalformedInput)
			}
		}
		commands := make([]v1.CommandName, len(value.Commands))
		for i, item := range value.Commands {
			commands[i] = v1.CommandName(item)
		}
		features := make([]v1.Capability, len(value.Features))
		for i, item := range value.Features {
			features[i] = v1.Capability(item)
		}
		return v1.Capabilities{Commands: commands, Features: features}, nil
	case model.StatusResult:
		return a.MapStatus(value.Status), nil
	case model.ConfigResult:
		if value.Config.CommandTimeout <= 0 || value.Config.RPCTimeout <= 0 || value.Config.QueueLimit == 0 || value.Config.QueueLimit > maxQueueCapacity || value.Config.PayloadLimit == 0 || value.Config.PayloadLimit > maxCanonicalPayloadSize {
			return nil, malformed("config result")
		}
		return v1.ConfigResult{Config: publicConfig(value.Config)}, nil
	case model.AuthResult:
		if err := validateProjectedIdentifier("agent_id", value.AgentID); err != nil {
			return nil, err
		}
		if err := validateProjectedIdentifier("mesh_id", value.MeshID); err != nil {
			return nil, err
		}
		if len(value.AgentInstanceID) > maxIdentifierLength || publicPersonality(value.Personality) == v1.SDKPersonalityUnset {
			return nil, malformed("auth result")
		}
		return v1.AuthResult{AgentID: v1.AgentID(value.AgentID), MeshID: v1.MeshID(value.MeshID), AgentInstanceID: value.AgentInstanceID, Personality: publicPersonality(value.Personality),
			ProfileID: value.ProfileID, CredentialExpiresAt: value.CredentialExpiresAt, OfflineStartDeadline: value.OfflineStartDeadline,
			OfflineColdStartTargetSeconds: value.OfflineColdStartTargetSeconds, OfflineTargetSatisfied: value.OfflineTargetSatisfied,
			PolicyRevision: value.PolicyRevision, SessionExpiryMode: value.SessionExpiryMode, PreparationStatus: value.PreparationStatus}, nil
	case model.AgentIDResult:
		if err := validateProjectedIdentifier("agent_id", value.AgentID); err != nil {
			return nil, err
		}
		return v1.AgentIDResult{AgentID: v1.AgentID(value.AgentID)}, nil
	case model.MeshListResult:
		if len(value.Meshes) > maxQueueCapacity {
			return nil, ErrInputTooLarge
		}
		for _, item := range value.Meshes {
			if err := validateProjectedIdentifier("mesh_id", item.MeshID); err != nil {
				return nil, err
			}
		}
		items := make([]v1.MeshSummary, len(value.Meshes))
		for i, item := range value.Meshes {
			items[i] = v1.MeshSummary{MeshID: v1.MeshID(item.MeshID), Active: item.Active}
		}
		return v1.MeshListResult{Meshes: items}, nil
	case model.AddressMappingsResult:
		if len(value.Mappings) > maxQueueCapacity {
			return nil, ErrInputTooLarge
		}
		for _, item := range value.Mappings {
			if err := validateOrigin(item.VirtualOrigin); err != nil {
				return nil, err
			}
			if err := validateProjectedIdentifier("recipient", item.Recipient); err != nil {
				return nil, err
			}
		}
		items := make([]v1.AddressMapping, len(value.Mappings))
		for i, item := range value.Mappings {
			items[i] = v1.AddressMapping{VirtualOrigin: item.VirtualOrigin, Recipient: v1.AgentID(item.Recipient)}
		}
		return v1.AddressMappingsResult{Mappings: items}, nil
	case model.AddressResolution:
		if err := validateProjectedIdentifier("recipient", value.Recipient); err != nil {
			return nil, err
		}
		if err := validatePath(value.Path); err != nil {
			return nil, err
		}
		if len(value.Query) > maxPublicString {
			return nil, ErrInputTooLarge
		}
		return v1.AddressResolution{Recipient: v1.AgentID(value.Recipient), Path: value.Path, Query: value.Query}, nil
	case model.SendResult:
		if err := validateProjectedIdentifier("message_id", value.MessageID); err != nil {
			return nil, err
		}
		if err := validateProjectedIdentifier("conversation_id", value.ConversationID); err != nil {
			return nil, err
		}
		if !value.Accepted {
			return nil, malformed("send acceptance")
		}
		return v1.SendResult{MessageID: v1.MessageID(value.MessageID), ConversationID: v1.ConversationID(value.ConversationID), Accepted: value.Accepted}, nil
	case model.ResponseResult:
		for field, identifier := range map[string]string{"message_id": value.MessageID, "conversation_id": value.ConversationID, "from_agent_id": value.FromAgentID, "mesh_id": value.MeshID} {
			if err := validateProjectedIdentifier(field, identifier); err != nil {
				return nil, err
			}
		}
		payload, err := a.MapPayload(value.Payload)
		if err != nil {
			return nil, err
		}
		return v1.ResponseResult{MessageID: v1.MessageID(value.MessageID), ConversationID: v1.ConversationID(value.ConversationID), FromAgentID: v1.AgentID(value.FromAgentID), MeshID: v1.MeshID(value.MeshID), Payload: payload}, nil
	case model.EventResult:
		event, err := a.MapEvent(value.Event)
		if err != nil {
			return nil, err
		}
		return v1.EventResult{Event: event}, nil
	case model.DeliveryQueueStatus:
		if value.Queued > maxQueueCapacity {
			return nil, malformed("delivery queue count")
		}
		return v1.DeliveryQueueStatus{Queued: value.Queued, Paused: value.Paused}, nil
	case model.PayloadHandleResult:
		if err := validatePayloadHandle(v1.PayloadHandle(value.Handle)); err != nil {
			return nil, err
		}
		if value.Size > maxCanonicalPayloadSize || len(value.Chunk) > maxPayloadChunkBytes || uint64(len(value.Chunk)) > value.Size {
			return nil, ErrInputTooLarge
		}
		return v1.PayloadHandleResult{Handle: v1.PayloadHandle(value.Handle), Size: value.Size, EOF: value.EOF, Chunk: cloneBytes(value.Chunk)}, nil
	case model.ConversationStatus:
		public, err := publicConversation(value)
		if err != nil {
			return nil, err
		}
		return public, nil
	case model.ConversationListResult:
		if len(value.Conversations) > maxQueueCapacity {
			return nil, ErrInputTooLarge
		}
		for _, item := range value.Conversations {
			if _, err := publicConversation(item); err != nil {
				return nil, err
			}
		}
		items := make([]v1.ConversationStatus, len(value.Conversations))
		for i, item := range value.Conversations {
			public, err := publicConversation(item)
			if err != nil {
				return nil, err
			}
			items[i] = public
		}
		return v1.ConversationListResult{Conversations: items}, nil
	case model.PolicyResult:
		if len(value.Rules) > maxPolicyRules {
			return nil, ErrInputTooLarge
		}
		for _, item := range value.Rules {
			action := v1.PolicyAction(item.Action)
			if action != v1.PolicyActionAllow && action != v1.PolicyActionDeny {
				return nil, fmt.Errorf("%w: unknown policy action", ErrMalformedInput)
			}
			if err := validatePolicyPath(item.Path); err != nil {
				return nil, err
			}
			if len(item.AgentID) > maxIdentifierLength {
				return nil, ErrInputTooLarge
			}
		}
		rules := make([]v1.PolicyRule, len(value.Rules))
		for i, item := range value.Rules {
			rules[i] = v1.PolicyRule{Action: v1.PolicyAction(item.Action), Path: item.Path, AgentID: v1.AgentID(item.AgentID)}
		}
		return v1.PolicyResult{Rules: rules, Allowed: value.Allowed}, nil
	case model.PeerStatus:
		if err := validateProjectedIdentifier("peer", value.Peer); err != nil {
			return nil, err
		}
		return v1.PeerStatus{Peer: v1.AgentID(value.Peer), Connectivity: publicConnectivity(value.Connectivity), Reachable: value.Reachable, RecoveryInProgress: value.RecoveryInProgress}, nil
	case model.ConnectivityStatus:
		return v1.ConnectivityStatus{State: publicConnectivity(value.State)}, nil
	case model.DiagnosticSnapshot:
		return a.MapDiagnostics(value)
	case model.CoreInitResult:
		if err := validateProjectedIdentifier("sdk_session_id", value.SessionID); err != nil {
			return nil, err
		}
		return v1.CoreInitResult{SDKSessionID: v1.SDKSessionID(value.SessionID)}, nil
	case model.CompletionChannelResult:
		if err := validateProjectedIdentifier("channel_id", value.ChannelID); err != nil {
			return nil, err
		}
		if value.MaxInFlight == 0 || value.MaxInFlight > maxQueueCapacity {
			return nil, malformed("max in flight")
		}
		return v1.CompletionChannelResult{ChannelID: v1.CompletionChannelID(value.ChannelID), MaxInFlight: value.MaxInFlight}, nil
	case model.EventSinkResult:
		if err := validateProjectedIdentifier("sink_id", value.SinkID); err != nil {
			return nil, err
		}
		return v1.EventSinkResult{SinkID: v1.EventSinkID(value.SinkID)}, nil
	default:
		return nil, fmt.Errorf("%w: unknown internal result", ErrMalformedInput)
	}
}

func publicConfig(value model.RuntimeConfig) v1.Config {
	return v1.Config{CommandTimeout: value.CommandTimeout, RPCTimeout: value.RPCTimeout, QueueLimit: value.QueueLimit, PayloadLimit: value.PayloadLimit}
}
func publicConversation(value model.ConversationStatus) (v1.ConversationStatus, error) {
	if value.QueuedMessageCount > maxQueueCapacity {
		return v1.ConversationStatus{}, fmt.Errorf("%w: conversation count", ErrMalformedInput)
	}
	for field, identifier := range map[string]string{"conversation_id": value.ConversationID, "mesh_id": value.MeshID, "peer": value.Peer} {
		if err := validateProjectedIdentifier(field, identifier); err != nil {
			return v1.ConversationStatus{}, err
		}
	}
	state, ok := publicDeliveryState(value.DeliveryState)
	if !ok {
		return v1.ConversationStatus{}, fmt.Errorf("%w: unknown delivery state", ErrMalformedInput)
	}
	return v1.ConversationStatus{ConversationID: v1.ConversationID(value.ConversationID), MeshID: v1.MeshID(value.MeshID), Peer: v1.AgentID(value.Peer), DeliveryState: state, QueuedMessageCount: value.QueuedMessageCount, Blocked: value.Blocked}, nil
}
