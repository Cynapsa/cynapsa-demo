package sdkboundary

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

type abiConfigWire struct {
	ABIVersion       uint32 `json:"abi_version"`
	CommandTimeoutMS int64  `json:"command_timeout_ms"`
	RPCTimeoutMS     int64  `json:"rpc_timeout_ms"`
	QueueLimit       uint32 `json:"queue_limit"`
	PayloadLimit     uint64 `json:"payload_limit"`
}

type abiCommandWire struct {
	ABIVersion  uint32          `json:"abi_version"`
	CommandID   string          `json:"command_id"`
	CommandName string          `json:"command_name"`
	SessionID   string          `json:"sdk_session_id"`
	Args        json.RawMessage `json:"args"`
}

type authArgsWire struct {
	MeshEndpoint    string          `json:"mesh_endpoint"`
	Username        string          `json:"username"`
	Password        clearableSecret `json:"password"`
	MeshID          string          `json:"mesh_id"`
	AgentInstanceID string          `json:"agent_instance_id"`
}

type tokenAuthArgsWire struct {
	Token       clearableToken `json:"token"`
	MeshID      string         `json:"mesh_id"`
	ProfileID   string         `json:"profile_id,omitempty"`
	ForceEnroll bool           `json:"force_enroll,omitempty"`
}

type installationAuthArgsWire struct {
	ProfileID string `json:"profile_id"`
	MeshID    string `json:"mesh_id"`
}

type clearableToken []byte

func (secret *clearableToken) UnmarshalJSON(data []byte) error {
	decoded, err := decodeJSONStringBytesBounded(data, maxAuthTokenBytes, "token")
	if err != nil {
		return err
	}
	clear(*secret)
	*secret = decoded
	return nil
}

type clearableSecret []byte

func (secret *clearableSecret) UnmarshalJSON(data []byte) error {
	decoded, err := decodeJSONStringBytes(data)
	if err != nil {
		return err
	}
	clear(*secret)
	*secret = decoded
	return nil
}

type headerWire struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type nativePayloadWire struct {
	ContentType string `json:"content_type"`
	Path        string `json:"path"`
	Body        []byte `json:"body"`
}

type httpRequestPayloadWire struct {
	Method  string       `json:"method"`
	Path    string       `json:"path"`
	Query   string       `json:"query"`
	Headers []headerWire `json:"headers"`
	Body    []byte       `json:"body"`
}

type applicationErrorWire struct {
	Code        string `json:"code"`
	Detail      string `json:"detail"`
	DetailsJSON string `json:"details_json"`
}

type httpResponsePayloadWire struct {
	StatusCode uint16                `json:"status_code"`
	Reason     string                `json:"reason"`
	Headers    []headerWire          `json:"headers"`
	Body       []byte                `json:"body"`
	Error      *applicationErrorWire `json:"error,omitempty"`
}

type payloadWire struct {
	Native       *nativePayloadWire       `json:"native,omitempty"`
	HTTPRequest  *httpRequestPayloadWire  `json:"http_request,omitempty"`
	HTTPResponse *httpResponsePayloadWire `json:"http_response,omitempty"`
	Handle       *string                  `json:"payload_handle,omitempty"`
}

type messageSendArgsWire struct {
	To      string      `json:"to"`
	Payload payloadWire `json:"payload"`
}

type messageRequestArgsWire struct {
	To      string      `json:"to"`
	Payload payloadWire `json:"payload"`
	TTLMS   int64       `json:"ttl_ms"`
}

type messageReplyArgsWire struct {
	RequestHandle string      `json:"request_handle"`
	Payload       payloadWire `json:"payload"`
}

type policyRuleWire struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	AgentID string `json:"agent_id"`
}

// DecodeABIConfig strictly decodes process-local Core creation limits.
func (a *Adapter) DecodeABIConfig(data []byte) (v1.Config, error) {
	var wire abiConfigWire
	if err := strictDecode(data, &wire); err != nil {
		return v1.Config{}, err
	}
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return v1.Config{}, err
	}
	commandTimeout, err := milliseconds(wire.CommandTimeoutMS)
	if err != nil {
		return v1.Config{}, err
	}
	rpcTimeout, err := milliseconds(wire.RPCTimeoutMS)
	if err != nil {
		return v1.Config{}, err
	}
	config := v1.Config{CommandTimeout: commandTimeout, RPCTimeout: rpcTimeout, QueueLimit: wire.QueueLimit, PayloadLimit: wire.PayloadLimit}
	mapped, err := a.MapConfig(config)
	if err != nil {
		return v1.Config{}, err
	}
	config.CommandTimeout = mapped.CommandTimeout
	config.RPCTimeout = mapped.RPCTimeout
	return config, nil
}

// DecodeABICommand strictly decodes one versioned command and validates it through the same public boundary path as Go callers.
func (a *Adapter) DecodeABICommand(data []byte) (v1.Command, error) {
	command, internal, err := a.decodeABICommand(data, false, nil, false, nil, false)
	model.ClearCommand(&internal)
	return command, err
}

// DecodeABIInternalCommand extracts secrets before generic JSON decoding and
// freezes the process-owned input. It clears byte-bearing temporaries on return.
func (a *Adapter) DecodeABIInternalCommand(data []byte) (model.Command, error) {
	sanitized, password, foundPassword, err := redactABISecretField(data, "password")
	if err != nil {
		return model.Command{}, err
	}
	defer clear(sanitized)
	defer clear(password)
	redacted, token, foundToken, err := redactABISecretField(sanitized, "token")
	if err != nil {
		return model.Command{}, err
	}
	defer clear(redacted)
	defer clear(token)
	command, internal, err := a.decodeABICommand(redacted, true, password, foundPassword, token, foundToken)
	clearDecodedABICommand(command)
	return internal, err
}

func (a *Adapter) decodeABICommand(data []byte, internalOnly bool, password []byte, foundPassword bool, token []byte, foundToken bool) (v1.Command, model.Command, error) {
	var wire abiCommandWire
	if err := strictDecode(data, &wire); err != nil {
		return nil, model.Command{}, err
	}
	defer clear(wire.Args)
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return nil, model.Command{}, err
	}
	name := v1.CommandName(wire.CommandName)
	if !IsPublicCommand(name) || len(wire.Args) == 0 || bytes.Equal(bytes.TrimSpace(wire.Args), []byte("null")) {
		return nil, model.Command{}, fmt.Errorf("%w: command envelope", ErrMalformedInput)
	}
	base := v1.CommandBase{CommandID: v1.CommandID(wire.CommandID), SDKSessionID: v1.SDKSessionID(wire.SessionID)}
	if internalOnly && (name == v1.CommandAuthLogin || name == v1.CommandAuthConnect) {
		internal, err := decodeABIInternalAuth(name, base, wire.Args, password, foundPassword)
		return nil, internal, err
	}
	if internalOnly && (name == v1.CommandAuthTokenLogin || name == v1.CommandAuthTokenConnect) {
		internal, err := decodeABIInternalTokenAuth(name, base, wire.Args, token, foundToken)
		return nil, internal, err
	}
	command, err := decodeABICommandArgs(name, base, wire.Args)
	if err != nil {
		return nil, model.Command{}, err
	}
	validated, err := a.DecodeCommand(context.Background(), command)
	if err != nil {
		clearDecodedABICommand(command)
		return nil, model.Command{}, err
	}
	return command, validated, nil
}

func decodeABIInternalAuth(name v1.CommandName, base v1.CommandBase, raw json.RawMessage, password []byte, foundPassword bool) (model.Command, error) {
	var args authArgsWire
	defer clear(args.Password)
	if err := strictDecode(raw, &args); err != nil {
		return model.Command{}, err
	}
	if foundPassword {
		clear(args.Password)
		args.Password = password
	}
	if err := requiredBounded("command_id", string(base.CommandID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	if err := requiredBounded("sdk_session_id", string(base.SDKSessionID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	if err := validateAuthByteFields(args.Username, args.Password, args.MeshID, args.MeshEndpoint); err != nil {
		return model.Command{}, err
	}
	if len(args.AgentInstanceID) > maxIdentifierLength {
		return model.Command{}, ErrInputTooLarge
	}
	endpoint, err := model.ParseMeshEndpoint(args.MeshEndpoint)
	if err != nil {
		return model.Command{}, malformed("mesh_endpoint")
	}
	rawCommand := model.Command{ID: string(base.CommandID), Name: string(name), SessionID: string(base.SDKSessionID), Args: model.AuthArgs{MeshEndpoint: endpoint.DialAddress(), Username: args.Username, Password: args.Password, MeshID: args.MeshID, AgentInstanceID: args.AgentInstanceID}}
	frozen, _, err := model.FreezeCommand(rawCommand)
	return frozen, err
}

func validateAuthByteFields(username string, password []byte, meshID, endpoint string) error {
	if err := requiredBounded("username", username, maxIdentifierLength); err != nil {
		return err
	}
	if len(password) == 0 || !utf8.Valid(password) {
		return malformed("password")
	}
	if len(password) > maxPublicString {
		return ErrInputTooLarge
	}
	if err := requiredBounded("mesh_id", meshID, maxIdentifierLength); err != nil {
		return err
	}
	return requiredBounded("mesh_endpoint", endpoint, maxPublicString)
}

func clearDecodedABICommand(command v1.Command) {
	switch value := command.(type) {
	case v1.MessageSendCommand:
		clearDecodedABIPayload(value.Payload)
	case v1.MessageRequestCommand:
		clearDecodedABIPayload(value.Payload)
	case v1.MessageReplyCommand:
		clearDecodedABIPayload(value.Payload)
	case v1.PayloadWriteCommand:
		clear(value.Chunk)
	}
}

func clearDecodedABIPayload(payload v1.Payload) {
	switch value := payload.Value.(type) {
	case v1.NativePayload:
		clear(value.Body)
	case v1.HTTPRequestPayload:
		clear(value.Body)
		clear(value.Headers)
	case v1.HTTPResponsePayload:
		clear(value.Body)
		clear(value.Headers)
		if value.Error != nil {
			*value.Error = v1.ApplicationError{}
		}
	}
}

func decodeABICommandArgs(name v1.CommandName, base v1.CommandBase, raw json.RawMessage) (v1.Command, error) {
	empty := func() error { var args struct{}; return strictDecode(raw, &args) }
	switch name {
	case v1.CommandCoreInit:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.CoreInitCommand{CommandBase: base}, nil
	case v1.CommandCoreCapabilities:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.CoreCapabilitiesCommand{CommandBase: base}, nil
	case v1.CommandCoreStatus:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.CoreStatusCommand{CommandBase: base}, nil
	case v1.CommandCoreShutdown:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.CoreShutdownCommand{CommandBase: base}, nil
	case v1.CommandConfigGet:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.ConfigGetCommand{CommandBase: base}, nil
	case v1.CommandConfigUpdate:
		var args configUpdateWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		update, err := decodeConfigUpdate(args.CommandTimeoutMS, args.RPCTimeoutMS, args.QueueLimit, args.PayloadLimit)
		if err != nil {
			return nil, err
		}
		return v1.ConfigUpdateCommand{CommandBase: base, Update: update}, nil
	case v1.CommandChannelRegister:
		var args capacityWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.CommandChannelRegisterCommand{CommandBase: base, Capacity: args.Capacity}, nil
	case v1.CommandChannelClear:
		var args channelIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.CommandChannelClearCommand{CommandBase: base, ChannelID: v1.CompletionChannelID(args.ChannelID)}, nil
	case v1.CommandCancel:
		var args commandHandleWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.CommandCancelCommand{CommandBase: base, CommandHandle: v1.CommandHandle(args.CommandHandle)}, nil
	case v1.CommandEventSinkRegister:
		var args capacityWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.EventSinkRegisterCommand{CommandBase: base, Capacity: args.Capacity}, nil
	case v1.CommandEventSinkClear:
		var args sinkIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.EventSinkClearCommand{CommandBase: base, SinkID: v1.EventSinkID(args.SinkID)}, nil
	case v1.CommandEventSinkBind:
		var args sinkIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.EventSinkBindCommand{CommandBase: base, SinkID: v1.EventSinkID(args.SinkID)}, nil
	case v1.CommandAuthLogin, v1.CommandAuthConnect:
		var args authArgsWire
		defer clear(args.Password)
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		auth := v1.AuthInput{MeshEndpoint: args.MeshEndpoint, Username: v1.AgentID(args.Username), Password: string(args.Password), MeshID: v1.MeshID(args.MeshID), AgentInstanceID: args.AgentInstanceID}
		if name == v1.CommandAuthLogin {
			return v1.AuthLoginCommand{CommandBase: base, Auth: auth}, nil
		}
		return v1.AuthConnectCommand{CommandBase: base, Auth: auth}, nil
	case v1.CommandAuthTokenLogin, v1.CommandAuthTokenConnect:
		var args tokenAuthArgsWire
		defer func() { clear(args.Token) }()
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		auth := v1.AuthTokenInput{Token: string(args.Token), MeshID: v1.MeshID(args.MeshID), ProfileID: args.ProfileID, ForceEnroll: args.ForceEnroll}
		if name == v1.CommandAuthTokenLogin {
			return v1.AuthTokenLoginCommand{CommandBase: base, Auth: auth}, nil
		}
		return v1.AuthTokenConnectCommand{CommandBase: base, Auth: auth}, nil
	case v1.CommandAuthInstallationLogin, v1.CommandAuthInstallationConnect:
		var args installationAuthArgsWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		auth := v1.AuthInstallationInput{ProfileID: args.ProfileID, MeshID: v1.MeshID(args.MeshID)}
		if name == v1.CommandAuthInstallationLogin {
			return v1.AuthInstallationLoginCommand{CommandBase: base, Auth: auth}, nil
		}
		return v1.AuthInstallationConnectCommand{CommandBase: base, Auth: auth}, nil
	case v1.CommandAuthLogout:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.AuthLogoutCommand{CommandBase: base}, nil
	case v1.CommandAuthAgentID:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.AuthAgentIDCommand{CommandBase: base}, nil
	case v1.CommandMeshList:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.MeshListCommand{CommandBase: base}, nil
	case v1.CommandMeshRefresh:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.MeshMembershipRefreshCommand{CommandBase: base}, nil
	case v1.CommandAddressPut:
		var args addressPutWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.AddressMapPutCommand{CommandBase: base, Mapping: v1.AddressMapping{VirtualOrigin: args.VirtualOrigin, Recipient: v1.AgentID(args.Recipient)}}, nil
	case v1.CommandAddressRemove:
		var args virtualOriginWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.AddressMapRemoveCommand{CommandBase: base, VirtualOrigin: args.VirtualOrigin}, nil
	case v1.CommandAddressList:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.AddressMapListCommand{CommandBase: base}, nil
	case v1.CommandAddressResolve:
		var args urlWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.AddressResolveCommand{CommandBase: base, URL: args.URL}, nil
	case v1.CommandMessageSend:
		var args messageSendArgsWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return messageSendCommand(base, args)
	case v1.CommandMessageRequest:
		var args messageRequestArgsWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		if err := validateAgentIdentity("to", args.To); err != nil {
			return nil, err
		}
		ttl, err := milliseconds(args.TTLMS)
		if err != nil {
			return nil, err
		}
		payload, err := publicPayload(args.Payload)
		if err != nil {
			return nil, err
		}
		return v1.MessageRequestCommand{CommandBase: base, To: v1.AgentID(args.To), Payload: payload, TTL: ttl}, nil
	case v1.CommandMessageReply:
		var args messageReplyArgsWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		payload, err := publicPayload(args.Payload)
		if err != nil {
			return nil, err
		}
		return v1.MessageReplyCommand{CommandBase: base, RequestHandle: v1.RequestHandle(args.RequestHandle), Payload: payload}, nil
	case v1.CommandDeliveryNext:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DeliveryNextCommand{CommandBase: base}, nil
	case v1.CommandDeliveryAccept:
		var args eventIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.DeliveryAcceptCommand{CommandBase: base, EventID: v1.EventID(args.EventID)}, nil
	case v1.CommandDeliveryQueueStatus:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DeliveryQueueStatusCommand{CommandBase: base}, nil
	case v1.CommandDeliveryRetry:
		var args messageIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.DeliveryRetryCommand{CommandBase: base, MessageID: v1.MessageID(args.MessageID)}, nil
	case v1.CommandDeliveryPause:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DeliveryPauseCommand{CommandBase: base}, nil
	case v1.CommandDeliveryResume:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DeliveryResumeCommand{CommandBase: base}, nil
	case v1.CommandDeliveryDrop:
		var args messageIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.DeliveryDropCommand{CommandBase: base, MessageID: v1.MessageID(args.MessageID)}, nil
	case v1.CommandHandlerRegister, v1.CommandHandlerUnregister:
		var args pathWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		if name == v1.CommandHandlerRegister {
			return v1.HandlerRegisterCommand{CommandBase: base, Path: args.Path}, nil
		}
		return v1.HandlerUnregisterCommand{CommandBase: base, Path: args.Path}, nil
	case v1.CommandPayloadOpen:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.PayloadOpenCommand{CommandBase: base}, nil
	case v1.CommandPayloadWrite:
		var args payloadWriteWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.PayloadWriteCommand{CommandBase: base, Handle: v1.PayloadHandle(args.Handle), Chunk: append([]byte(nil), args.Chunk...)}, nil
	case v1.CommandPayloadFinish, v1.CommandPayloadCancel, v1.CommandPayloadClose, v1.CommandPayloadRetain, v1.CommandPayloadRelease:
		var args payloadHandleWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		handle := v1.PayloadHandle(args.Handle)
		switch name {
		case v1.CommandPayloadFinish:
			return v1.PayloadFinishCommand{CommandBase: base, Handle: handle}, nil
		case v1.CommandPayloadCancel:
			return v1.PayloadCancelCommand{CommandBase: base, Handle: handle}, nil
		case v1.CommandPayloadClose:
			return v1.PayloadCloseCommand{CommandBase: base, Handle: handle}, nil
		case v1.CommandPayloadRetain:
			return v1.PayloadRetainCommand{CommandBase: base, Handle: handle}, nil
		default:
			return v1.PayloadReleaseCommand{CommandBase: base, Handle: handle}, nil
		}
	case v1.CommandPayloadRead:
		var args payloadReadWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.PayloadReadCommand{CommandBase: base, Handle: v1.PayloadHandle(args.Handle), Offset: args.Offset, Limit: args.Limit}, nil
	case v1.CommandConversationList:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.ConversationListCommand{CommandBase: base}, nil
	case v1.CommandConversationStatus, v1.CommandConversationClose:
		var args conversationIDWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		id := v1.ConversationID(args.ConversationID)
		if name == v1.CommandConversationStatus {
			return v1.ConversationStatusCommand{CommandBase: base, ConversationID: id}, nil
		}
		return v1.ConversationCloseCommand{CommandBase: base, ConversationID: id}, nil
	case v1.CommandPolicySet:
		var args policySetWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		if err := validateWirePolicyRules(args.Rules); err != nil {
			return nil, err
		}
		return v1.PolicySetCommand{CommandBase: base, Rules: publicRules(args.Rules)}, nil
	case v1.CommandPolicyGet:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.PolicyGetCommand{CommandBase: base}, nil
	case v1.CommandPolicyTest:
		var args policyTestWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		nested, err := messageSendCommand(v1.CommandBase{}, args.Input)
		if err != nil {
			return nil, err
		}
		send := nested.(v1.MessageSendCommand)
		return v1.PolicyTestCommand{CommandBase: base, Input: v1.PolicyTestInput{To: send.To, Payload: send.Payload}}, nil
	case v1.CommandDiagnosticsPeer:
		var args peerWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.DiagnosticsPeerCommand{CommandBase: base, Peer: v1.AgentID(args.Peer)}, nil
	case v1.CommandDiagnosticsConnectivity:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DiagnosticsConnectivityCommand{CommandBase: base}, nil
	case v1.CommandDiagnosticsSnapshot:
		if err := empty(); err != nil {
			return nil, err
		}
		return v1.DiagnosticsSnapshotCommand{CommandBase: base}, nil
	case v1.CommandDiagnosticsLogs:
		var args enabledWire
		if err := strictDecode(raw, &args); err != nil {
			return nil, err
		}
		return v1.DiagnosticsLogsCommand{CommandBase: base, Enabled: args.Enabled}, nil
	default:
		return nil, fmt.Errorf("%w: command name", ErrMalformedInput)
	}
}

type configUpdateWire struct {
	CommandTimeoutMS *int64  `json:"command_timeout_ms"`
	RPCTimeoutMS     *int64  `json:"rpc_timeout_ms"`
	QueueLimit       *uint32 `json:"queue_limit"`
	PayloadLimit     *uint64 `json:"payload_limit"`
}
type capacityWire struct {
	Capacity uint32 `json:"capacity"`
}
type channelIDWire struct {
	ChannelID string `json:"channel_id"`
}
type commandHandleWire struct {
	CommandHandle string `json:"command_handle"`
}
type sinkIDWire struct {
	SinkID string `json:"sink_id"`
}
type addressPutWire struct {
	VirtualOrigin string `json:"virtual_origin"`
	Recipient     string `json:"recipient"`
}
type virtualOriginWire struct {
	VirtualOrigin string `json:"virtual_origin"`
}
type urlWire struct {
	URL string `json:"url"`
}
type eventIDWire struct {
	EventID string `json:"event_id"`
}
type messageIDWire struct {
	MessageID string `json:"message_id"`
}
type pathWire struct {
	Path string `json:"path"`
}
type payloadWriteWire struct {
	Handle string `json:"handle"`
	Chunk  []byte `json:"chunk"`
}
type payloadHandleWire struct {
	Handle string `json:"handle"`
}
type payloadReadWire struct {
	Handle string `json:"handle"`
	Offset uint64 `json:"offset"`
	Limit  uint32 `json:"limit"`
}
type conversationIDWire struct {
	ConversationID string `json:"conversation_id"`
}
type policySetWire struct {
	Rules []policyRuleWire `json:"rules"`
}
type policyTestWire struct {
	Input messageSendArgsWire `json:"input"`
}
type peerWire struct {
	Peer string `json:"peer"`
}
type enabledWire struct {
	Enabled bool `json:"enabled"`
}

func decodeConfigUpdate(commandMS, rpcMS *int64, queue *uint32, payload *uint64) (v1.ConfigUpdate, error) {
	var update v1.ConfigUpdate
	if commandMS != nil {
		value, durationErr := milliseconds(*commandMS)
		if durationErr != nil {
			return update, durationErr
		}
		update.CommandTimeout = &value
	}
	if rpcMS != nil {
		value, durationErr := milliseconds(*rpcMS)
		if durationErr != nil {
			return update, durationErr
		}
		update.RPCTimeout = &value
	}
	update.QueueLimit = cloneUint32(queue)
	update.PayloadLimit = cloneUint64(payload)
	return update, nil
}

func messageSendCommand(base v1.CommandBase, args messageSendArgsWire) (v1.Command, error) {
	if err := validateAgentIdentity("to", args.To); err != nil {
		return nil, err
	}
	payload, err := publicPayload(args.Payload)
	if err != nil {
		return nil, err
	}
	return v1.MessageSendCommand{CommandBase: base, To: v1.AgentID(args.To), Payload: payload}, nil
}
func publicPayload(value payloadWire) (v1.Payload, error) {
	if err := validateWirePayload(value); err != nil {
		return v1.Payload{}, err
	}
	var output v1.Payload
	if value.Native != nil {
		output.Value = v1.NativePayload{ContentType: value.Native.ContentType, Path: value.Native.Path, Body: cloneBytes(value.Native.Body)}
	}
	if value.HTTPRequest != nil {
		output.Value = v1.HTTPRequestPayload{Method: value.HTTPRequest.Method, Path: value.HTTPRequest.Path, Query: value.HTTPRequest.Query, Headers: publicHeaders(value.HTTPRequest.Headers), Body: cloneBytes(value.HTTPRequest.Body)}
	}
	if value.HTTPResponse != nil {
		output.Value = v1.HTTPResponsePayload{StatusCode: value.HTTPResponse.StatusCode, Reason: value.HTTPResponse.Reason, Headers: publicHeaders(value.HTTPResponse.Headers), Body: cloneBytes(value.HTTPResponse.Body), Error: publicApplicationError(value.HTTPResponse.Error)}
	}
	if value.Handle != nil {
		output.Value = v1.PayloadHandle(*value.Handle)
	}
	return output, nil
}

// validateWirePayload rejects attacker-controlled sizes and metadata before
// publicPayload allocates header or body copies.
func validateWirePayload(value payloadWire) error {
	variants := 0
	if value.Native != nil {
		variants++
	}
	if value.HTTPRequest != nil {
		variants++
	}
	if value.HTTPResponse != nil {
		variants++
	}
	if value.Handle != nil {
		variants++
	}
	if variants != 1 {
		return malformed("payload variant")
	}
	if value.Native != nil {
		if err := validatePath(value.Native.Path); err != nil {
			return err
		}
		if err := validateContentType(value.Native.ContentType); err != nil {
			return err
		}
		if canonicalSize(value.Native.ContentType, value.Native.Path)+len(value.Native.Body) > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	}
	if value.HTTPRequest != nil {
		if err := validateHTTPMethod(value.HTTPRequest.Method); err != nil {
			return err
		}
		if err := validatePath(value.HTTPRequest.Path); err != nil {
			return err
		}
		if len(value.HTTPRequest.Query) > maxPublicString {
			return ErrInputTooLarge
		}
		headerSize, err := validateWireHeaders(value.HTTPRequest.Headers)
		if err != nil {
			return err
		}
		if headerSize+canonicalSize(value.HTTPRequest.Method, value.HTTPRequest.Path, value.HTTPRequest.Query)+len(value.HTTPRequest.Body) > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	}
	if value.HTTPResponse != nil {
		if value.HTTPResponse.StatusCode < 100 || value.HTTPResponse.StatusCode > 599 {
			return malformed("http response")
		}
		if err := validateReason(value.HTTPResponse.Reason); err != nil {
			return err
		}
		if value.HTTPResponse.Error != nil && value.HTTPResponse.StatusCode < 400 {
			return malformed("http response application error")
		}
		headerSize, err := validateWireHeaders(value.HTTPResponse.Headers)
		if err != nil {
			return err
		}
		errorSize, err := validateWireApplicationError(value.HTTPResponse.Error)
		if err != nil {
			return err
		}
		if headerSize+len(value.HTTPResponse.Reason)+len(value.HTTPResponse.Body)+errorSize > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	}
	return validatePayloadHandle(v1.PayloadHandle(*value.Handle))
}

func validateWireHeaders(values []headerWire) (int, error) {
	if len(values) > maxHeaderCount {
		return 0, ErrInputTooLarge
	}
	total := 0
	for _, value := range values {
		if len(value.Name) > maxIdentifierLength || len(value.Value) > maxPublicString {
			return 0, ErrInputTooLarge
		}
		if !validHTTPToken(value.Name) || strings.ContainsAny(value.Value, "\r\n") {
			return 0, malformed("header")
		}
		total += len(value.Name) + len(value.Value)
		if total > maxInlinePayloadBytes {
			return 0, ErrInputTooLarge
		}
	}
	return total, nil
}
func publicHeaders(values []headerWire) []v1.Header {
	output := make([]v1.Header, len(values))
	for i, value := range values {
		output[i] = v1.Header{Name: value.Name, Value: value.Value}
	}
	return output
}
func publicRules(values []policyRuleWire) []v1.PolicyRule {
	output := make([]v1.PolicyRule, len(values))
	for i, value := range values {
		output[i] = v1.PolicyRule{Action: v1.PolicyAction(value.Action), Path: value.Path, AgentID: v1.AgentID(value.AgentID)}
	}
	return output
}

func validateWirePolicyRules(values []policyRuleWire) error {
	if len(values) > maxPolicyRules {
		return ErrInputTooLarge
	}
	for _, value := range values {
		if value.Action != string(v1.PolicyActionAllow) && value.Action != string(v1.PolicyActionDeny) {
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
func milliseconds(value int64) (time.Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: negative timeout", ErrMalformedInput)
	}
	if value > int64((1<<63-1)/time.Millisecond) {
		return 0, ErrInputTooLarge
	}
	return time.Duration(value) * time.Millisecond, nil
}
func requireABIVersion(version uint32) error {
	if version != v1.CurrentSchemaVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	return nil
}

func stableMarshal[T any](value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode public value", ErrMalformedInput)
	}
	return data, nil
}

func durationMilliseconds(value time.Duration) (int64, error) {
	if value < 0 || value%time.Millisecond != 0 {
		return 0, fmt.Errorf("%w: timeout precision", ErrMalformedInput)
	}
	return int64(value / time.Millisecond), nil
}

func decodeJSONStringBytes(data []byte) ([]byte, error) {
	return decodeJSONStringBytesBounded(data, maxPublicString, "password")
}

func decodeJSONStringBytesBounded(data []byte, limit int, field string) ([]byte, error) {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return nil, malformed(field)
	}
	output := make([]byte, 0, min(len(data)-2, limit))
	appendBounded := func(values ...byte) error {
		if len(values) > limit-len(output) {
			return ErrInputTooLarge
		}
		output = append(output, values...)
		return nil
	}
	for index := 1; index < len(data)-1; {
		value := data[index]
		index++
		if value < 0x20 || value == '"' {
			clear(output)
			return nil, malformed(field)
		}
		if value != '\\' {
			if err := appendBounded(value); err != nil {
				clear(output)
				return nil, err
			}
			continue
		}
		if index >= len(data)-1 {
			clear(output)
			return nil, malformed(field)
		}
		escape := data[index]
		index++
		switch escape {
		case '"', '\\', '/':
			if err := appendBounded(escape); err != nil {
				clear(output)
				return nil, err
			}
		case 'b', 'f', 'n', 'r', 't':
			decoded := map[byte]byte{'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}[escape]
			if err := appendBounded(decoded); err != nil {
				clear(output)
				return nil, err
			}
		case 'u':
			unit, next, ok := decodeJSONHexUnit(data, index)
			if !ok {
				clear(output)
				return nil, malformed(field)
			}
			index = next
			decodedRune := rune(unit)
			if unit >= 0xd800 && unit <= 0xdbff {
				if index+2 > len(data)-1 || data[index] != '\\' || data[index+1] != 'u' {
					clear(output)
					return nil, malformed(field)
				}
				low, afterLow, lowOK := decodeJSONHexUnit(data, index+2)
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					clear(output)
					return nil, malformed(field)
				}
				index = afterLow
				decodedRune = 0x10000 + (rune(unit)-0xd800)<<10 + rune(low) - 0xdc00
			} else if unit >= 0xdc00 && unit <= 0xdfff {
				clear(output)
				return nil, malformed(field)
			}
			encoded := utf8.AppendRune(nil, decodedRune)
			if err := appendBounded(encoded...); err != nil {
				clear(output)
				return nil, err
			}
		default:
			clear(output)
			return nil, malformed(field)
		}
	}
	if !utf8.Valid(output) {
		clear(output)
		return nil, malformed(field)
	}
	return output, nil
}

func decodeJSONHexUnit(data []byte, index int) (uint16, int, bool) {
	if index < 0 || index+4 > len(data)-1 {
		return 0, index, false
	}
	var value uint16
	for _, digit := range data[index : index+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value += uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value += uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value += uint16(digit-'A') + 10
		default:
			return 0, index, false
		}
	}
	return value, index + 4, true
}

func redactABISecretField(data []byte, field string) ([]byte, []byte, bool, error) {
	if len(data) == 0 {
		return nil, nil, false, ErrMalformedInput
	}
	if len(data) > maxABIInputBytes {
		return nil, nil, false, ErrInputTooLarge
	}
	if !utf8.Valid(data) {
		return nil, nil, false, malformed("invalid UTF-8")
	}
	output := make([]byte, 0, len(data))
	cursor := 0
	var secret []byte
	found := false
	for index := 0; index < len(data); {
		if data[index] != '"' {
			index++
			continue
		}
		end, ok := scanJSONStringEnd(data, index)
		if !ok {
			clear(output)
			clear(secret)
			return nil, nil, false, ErrMalformedInput
		}
		next := skipJSONSpace(data, end)
		if next >= len(data) || data[next] != ':' {
			index = end
			continue
		}
		key, err := decodeJSONStringBytes(data[index:end])
		if err != nil {
			clear(output)
			clear(secret)
			return nil, nil, false, err
		}
		matches := string(key) == field
		clear(key)
		if !matches {
			index = end
			continue
		}
		valueStart := skipJSONSpace(data, next+1)
		if valueStart >= len(data) || data[valueStart] != '"' {
			index = end
			continue
		}
		valueEnd, ok := scanJSONStringEnd(data, valueStart)
		if !ok {
			clear(output)
			clear(secret)
			return nil, nil, false, ErrMalformedInput
		}
		limit := maxPublicString
		if field == "token" {
			limit = maxAuthTokenBytes
		}
		decoded, err := decodeJSONStringBytesBounded(data[valueStart:valueEnd], limit, field)
		if err != nil {
			clear(output)
			clear(secret)
			return nil, nil, false, err
		}
		clear(secret)
		secret = decoded
		found = true
		output = append(output, data[cursor:valueStart]...)
		output = append(output, `"redacted"`...)
		cursor = valueEnd
		index = valueEnd
	}
	output = append(output, data[cursor:]...)
	return output, secret, found, nil
}

func scanJSONStringEnd(data []byte, start int) (int, bool) {
	if start < 0 || start >= len(data) || data[start] != '"' {
		return start, false
	}
	for index := start + 1; index < len(data); index++ {
		switch data[index] {
		case '"':
			return index + 1, true
		case '\\':
			index++
			if index >= len(data) {
				return index, false
			}
		}
	}
	return len(data), false
}

func skipJSONSpace(data []byte, index int) int {
	for index < len(data) {
		switch data[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func decodeABIInternalTokenAuth(name v1.CommandName, base v1.CommandBase, raw json.RawMessage, token []byte, foundToken bool) (model.Command, error) {
	var args tokenAuthArgsWire
	defer func() { clear(args.Token) }()
	if err := strictDecode(raw, &args); err != nil {
		return model.Command{}, err
	}
	if foundToken {
		clear(args.Token)
		args.Token = token
	}
	if err := requiredBounded("command_id", string(base.CommandID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	if err := requiredBounded("sdk_session_id", string(base.SDKSessionID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	if len(args.Token) == 0 || !utf8.Valid(args.Token) {
		return model.Command{}, malformed("token")
	}
	if len(args.Token) > maxAuthTokenBytes {
		return model.Command{}, ErrInputTooLarge
	}
	if err := validateTokenAuthContext(args.MeshID); err != nil {
		return model.Command{}, err
	}
	profileID := args.ProfileID
	if profileID == "" {
		profileID = enrollment.DefaultProfileID
	}
	if !enrollment.ValidProfileID(profileID) {
		return model.Command{}, malformed("profile_id")
	}
	bare, err := enrollment.ParsePublicToken(args.Token)
	if err != nil {
		return model.Command{}, malformed("token")
	}
	defer clear(bare)
	rawCommand := model.Command{ID: string(base.CommandID), Name: string(name), SessionID: string(base.SDKSessionID), Args: model.TokenAuthArgs{Token: bare, MeshID: args.MeshID, ProfileID: profileID, ForceEnroll: args.ForceEnroll}}
	frozen, _, err := model.FreezeCommand(rawCommand)
	return frozen, err
}
