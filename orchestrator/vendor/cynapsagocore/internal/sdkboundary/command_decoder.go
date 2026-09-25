package sdkboundary

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const (
	maxIdentifierLength     = 512
	maxAgentIdentityLength  = 256
	maxPathLength           = 2048
	maxPublicString         = 8192
	maxAuthTokenBytes       = 88
	maxHeaderCount          = 4096
	maxPolicyRules          = 1024
	maxInlinePayloadBytes   = 256 << 10
	maxPayloadChunkBytes    = 1 << 20
	maxCanonicalPayloadSize = v1.MaximumPayloadBytes
	maxQueueCapacity        = 65536
)

// DecodeCommand validates one public command and constructs a separately owned internal command.
func (a *Adapter) DecodeCommand(ctx context.Context, command v1.Command) (model.Command, error) {
	if ctx == nil {
		return model.Command{}, fmt.Errorf("%w: nil context", ErrMalformedInput)
	}
	if err := ctx.Err(); err != nil {
		return model.Command{}, err
	}
	if command == nil {
		return model.Command{}, fmt.Errorf("%w: unknown command", ErrMalformedInput)
	}
	var err error
	command, err = normalizeCommandValue(command)
	if err != nil {
		return model.Command{}, err
	}
	if !IsPublicCommand(command.Name()) {
		return model.Command{}, fmt.Errorf("%w: unknown command", ErrMalformedInput)
	}

	base, args, err := decodeCommandVariant(command)
	if err != nil {
		return model.Command{}, err
	}
	raw := model.Command{ID: string(base.CommandID), Name: string(command.Name()), SessionID: string(base.SDKSessionID), Args: args}
	defer model.ClearCommand(&raw)
	if err := requiredBounded("command_id", string(base.CommandID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	if err := requiredBounded("sdk_session_id", string(base.SDKSessionID), maxIdentifierLength); err != nil {
		return model.Command{}, err
	}
	frozen, _, err := model.FreezeCommand(raw)
	if err != nil {
		return model.Command{}, fmt.Errorf("%w: command ownership", ErrInputTooLarge)
	}
	return frozen, nil
}

func normalizeCommandValue(command v1.Command) (v1.Command, error) {
	switch value := command.(type) {
	case *v1.CoreInitCommand:
		return commandValue(value)
	case *v1.CoreCapabilitiesCommand:
		return commandValue(value)
	case *v1.CoreStatusCommand:
		return commandValue(value)
	case *v1.CoreShutdownCommand:
		return commandValue(value)
	case *v1.ConfigGetCommand:
		return commandValue(value)
	case *v1.ConfigUpdateCommand:
		return commandValue(value)
	case *v1.CommandChannelRegisterCommand:
		return commandValue(value)
	case *v1.CommandChannelClearCommand:
		return commandValue(value)
	case *v1.CommandCancelCommand:
		return commandValue(value)
	case *v1.EventSinkRegisterCommand:
		return commandValue(value)
	case *v1.EventSinkClearCommand:
		return commandValue(value)
	case *v1.EventSinkBindCommand:
		return commandValue(value)
	case *v1.AuthTokenLoginCommand:
		return commandValue(value)
	case *v1.AuthTokenConnectCommand:
		return commandValue(value)
	case *v1.AuthInstallationLoginCommand:
		return commandValue(value)
	case *v1.AuthInstallationConnectCommand:
		return commandValue(value)
	case *v1.AuthLoginCommand:
		return commandValue(value)
	case *v1.AuthConnectCommand:
		return commandValue(value)
	case *v1.AuthLogoutCommand:
		return commandValue(value)
	case *v1.AuthAgentIDCommand:
		return commandValue(value)
	case *v1.MeshListCommand:
		return commandValue(value)
	case *v1.MeshMembershipRefreshCommand:
		return commandValue(value)
	case *v1.AddressMapPutCommand:
		return commandValue(value)
	case *v1.AddressMapRemoveCommand:
		return commandValue(value)
	case *v1.AddressMapListCommand:
		return commandValue(value)
	case *v1.AddressResolveCommand:
		return commandValue(value)
	case *v1.MessageSendCommand:
		return commandValue(value)
	case *v1.MessageRequestCommand:
		return commandValue(value)
	case *v1.MessageReplyCommand:
		return commandValue(value)
	case *v1.DeliveryNextCommand:
		return commandValue(value)
	case *v1.DeliveryAcceptCommand:
		return commandValue(value)
	case *v1.DeliveryQueueStatusCommand:
		return commandValue(value)
	case *v1.DeliveryRetryCommand:
		return commandValue(value)
	case *v1.DeliveryPauseCommand:
		return commandValue(value)
	case *v1.DeliveryResumeCommand:
		return commandValue(value)
	case *v1.DeliveryDropCommand:
		return commandValue(value)
	case *v1.HandlerRegisterCommand:
		return commandValue(value)
	case *v1.HandlerUnregisterCommand:
		return commandValue(value)
	case *v1.PayloadOpenCommand:
		return commandValue(value)
	case *v1.PayloadWriteCommand:
		return commandValue(value)
	case *v1.PayloadFinishCommand:
		return commandValue(value)
	case *v1.PayloadCancelCommand:
		return commandValue(value)
	case *v1.PayloadReadCommand:
		return commandValue(value)
	case *v1.PayloadCloseCommand:
		return commandValue(value)
	case *v1.PayloadRetainCommand:
		return commandValue(value)
	case *v1.PayloadReleaseCommand:
		return commandValue(value)
	case *v1.ConversationListCommand:
		return commandValue(value)
	case *v1.ConversationStatusCommand:
		return commandValue(value)
	case *v1.ConversationCloseCommand:
		return commandValue(value)
	case *v1.PolicySetCommand:
		return commandValue(value)
	case *v1.PolicyGetCommand:
		return commandValue(value)
	case *v1.PolicyTestCommand:
		return commandValue(value)
	case *v1.DiagnosticsPeerCommand:
		return commandValue(value)
	case *v1.DiagnosticsConnectivityCommand:
		return commandValue(value)
	case *v1.DiagnosticsSnapshotCommand:
		return commandValue(value)
	case *v1.DiagnosticsLogsCommand:
		return commandValue(value)
	default:
		return command, nil
	}
}

func commandValue[T v1.Command](value *T) (v1.Command, error) {
	if value == nil {
		return nil, fmt.Errorf("%w: nil command", ErrMalformedInput)
	}
	return *value, nil
}

func decodeCommandVariant(command v1.Command) (v1.CommandBase, model.CommandArgs, error) {
	switch value := command.(type) {
	case v1.CoreInitCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.CoreCapabilitiesCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.CoreStatusCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.CoreShutdownCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.ConfigGetCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.ConfigUpdateCommand:
		if err := validateConfigUpdate(value.Update); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.ConfigUpdateArgs{CommandTimeout: normalizedDuration(value.Update.CommandTimeout), RPCTimeout: normalizedDuration(value.Update.RPCTimeout), QueueLimit: cloneUint32(value.Update.QueueLimit), PayloadLimit: cloneUint64(value.Update.PayloadLimit)}, nil
	case v1.CommandChannelRegisterCommand:
		if value.Capacity == 0 || value.Capacity > maxQueueCapacity {
			return v1.CommandBase{}, nil, malformed("capacity")
		}
		return value.CommandBase, model.ChannelRegisterArgs{Capacity: value.Capacity}, nil
	case v1.CommandChannelClearCommand:
		if err := requiredBounded("channel_id", string(value.ChannelID), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.ChannelIDArgs{ChannelID: string(value.ChannelID)}, nil
	case v1.CommandCancelCommand:
		if err := validateCommandHandle(value.CommandHandle); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.CommandCancelArgs{CommandHandle: string(value.CommandHandle)}, nil
	case v1.EventSinkRegisterCommand:
		if value.Capacity == 0 || value.Capacity > maxQueueCapacity {
			return v1.CommandBase{}, nil, malformed("capacity")
		}
		return value.CommandBase, model.EventSinkRegisterArgs{Capacity: value.Capacity}, nil
	case v1.EventSinkClearCommand:
		if err := requiredBounded("sink_id", string(value.SinkID), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.EventSinkIDArgs{SinkID: string(value.SinkID)}, nil
	case v1.EventSinkBindCommand:
		if err := requiredBounded("sink_id", string(value.SinkID), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.EventSinkIDArgs{SinkID: string(value.SinkID)}, nil
	case v1.AuthTokenLoginCommand:
		return decodeTokenAuth(value.CommandBase, value.Auth)
	case v1.AuthTokenConnectCommand:
		return decodeTokenAuth(value.CommandBase, value.Auth)
	case v1.AuthInstallationLoginCommand:
		return decodeInstallationAuth(value.CommandBase, value.Auth)
	case v1.AuthInstallationConnectCommand:
		return decodeInstallationAuth(value.CommandBase, value.Auth)
	case v1.AuthLoginCommand:
		return decodeAuth(value.CommandBase, value.Auth)
	case v1.AuthConnectCommand:
		return decodeAuth(value.CommandBase, value.Auth)
	case v1.AuthLogoutCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.AuthAgentIDCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.MeshListCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.MeshMembershipRefreshCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.AddressMapPutCommand:
		if err := validateOrigin(value.Mapping.VirtualOrigin); err != nil {
			return v1.CommandBase{}, nil, err
		}
		if err := requiredBounded("recipient", string(value.Mapping.Recipient), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.AddressPutArgs{Mapping: model.AddressMapping{VirtualOrigin: value.Mapping.VirtualOrigin, Recipient: string(value.Mapping.Recipient)}}, nil
	case v1.AddressMapRemoveCommand:
		if err := validateOrigin(value.VirtualOrigin); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.AddressRemoveArgs{VirtualOrigin: value.VirtualOrigin}, nil
	case v1.AddressMapListCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.AddressResolveCommand:
		if err := validateApplicationURL(value.URL); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.AddressResolveArgs{URL: value.URL}, nil
	case v1.MessageSendCommand:
		args, err := decodeSend(value.To, value.Payload)
		return value.CommandBase, args, err
	case v1.MessageRequestCommand:
		if value.TTL < 0 {
			return v1.CommandBase{}, nil, malformed("ttl")
		}
		send, err := decodeSend(value.To, value.Payload)
		return value.CommandBase, model.MessageRequestArgs{To: send.To, Payload: send.Payload, TTL: value.TTL}, err
	case v1.MessageReplyCommand:
		if err := validateRequestHandle(value.RequestHandle); err != nil {
			return v1.CommandBase{}, nil, err
		}
		payload, err := decodePayload(value.Payload)
		if err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.MessageReplyArgs{RequestHandle: string(value.RequestHandle), Payload: payload}, nil
	case v1.DeliveryNextCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DeliveryAcceptCommand:
		if err := requiredBounded("event_id", string(value.EventID), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.DeliveryAcceptArgs{EventID: string(value.EventID)}, nil
	case v1.DeliveryQueueStatusCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DeliveryRetryCommand:
		return messageIDCommand(value.CommandBase, value.MessageID)
	case v1.DeliveryPauseCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DeliveryResumeCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DeliveryDropCommand:
		return messageIDCommand(value.CommandBase, value.MessageID)
	case v1.HandlerRegisterCommand:
		return handlerCommand(value.CommandBase, value.Path)
	case v1.HandlerUnregisterCommand:
		return handlerCommand(value.CommandBase, value.Path)
	case v1.PayloadOpenCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.PayloadWriteCommand:
		if err := validatePayloadHandle(value.Handle); err != nil {
			return v1.CommandBase{}, nil, err
		}
		if len(value.Chunk) > maxPayloadChunkBytes {
			return v1.CommandBase{}, nil, ErrInputTooLarge
		}
		return value.CommandBase, model.PayloadWriteArgs{Handle: string(value.Handle), Chunk: cloneBytes(value.Chunk)}, nil
	case v1.PayloadFinishCommand:
		return payloadHandleCommand(value.CommandBase, value.Handle)
	case v1.PayloadCancelCommand:
		return payloadHandleCommand(value.CommandBase, value.Handle)
	case v1.PayloadReadCommand:
		if err := validatePayloadHandle(value.Handle); err != nil {
			return v1.CommandBase{}, nil, err
		}
		if value.Offset > maxCanonicalPayloadSize || value.Limit > maxPayloadChunkBytes || value.Offset+uint64(value.Limit) > maxCanonicalPayloadSize {
			return v1.CommandBase{}, nil, malformed("payload range")
		}
		return value.CommandBase, model.PayloadReadArgs{Handle: string(value.Handle), Offset: value.Offset, Limit: value.Limit}, nil
	case v1.PayloadCloseCommand:
		return payloadHandleCommand(value.CommandBase, value.Handle)
	case v1.PayloadRetainCommand:
		return payloadHandleCommand(value.CommandBase, value.Handle)
	case v1.PayloadReleaseCommand:
		return payloadHandleCommand(value.CommandBase, value.Handle)
	case v1.ConversationListCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.ConversationStatusCommand:
		return conversationCommand(value.CommandBase, value.ConversationID)
	case v1.ConversationCloseCommand:
		return conversationCommand(value.CommandBase, value.ConversationID)
	case v1.PolicySetCommand:
		rules, err := decodeRules(value.Rules)
		return value.CommandBase, model.PolicySetArgs{Rules: rules}, err
	case v1.PolicyGetCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.PolicyTestCommand:
		send, err := decodeSend(value.Input.To, value.Input.Payload)
		return value.CommandBase, model.PolicyTestArgs{Input: send}, err
	case v1.DiagnosticsPeerCommand:
		if err := requiredBounded("peer", string(value.Peer), maxIdentifierLength); err != nil {
			return v1.CommandBase{}, nil, err
		}
		return value.CommandBase, model.DiagnosticsPeerArgs{Peer: string(value.Peer)}, nil
	case v1.DiagnosticsConnectivityCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DiagnosticsSnapshotCommand:
		return value.CommandBase, model.EmptyArgs{}, nil
	case v1.DiagnosticsLogsCommand:
		return value.CommandBase, model.DiagnosticsLogsArgs{Enabled: value.Enabled}, nil
	default:
		return v1.CommandBase{}, nil, fmt.Errorf("%w: unsupported command type", ErrMalformedInput)
	}
}

func decodeAuth(base v1.CommandBase, input v1.AuthInput) (v1.CommandBase, model.CommandArgs, error) {
	if err := validateAuthFields(string(input.Username), input.Password, string(input.MeshID), input.MeshEndpoint); err != nil {
		return v1.CommandBase{}, nil, err
	}
	if len(input.AgentInstanceID) > maxIdentifierLength {
		return v1.CommandBase{}, nil, ErrInputTooLarge
	}
	endpoint, err := model.ParseMeshEndpoint(input.MeshEndpoint)
	if err != nil {
		return v1.CommandBase{}, nil, malformed("mesh_endpoint")
	}
	return base, model.AuthArgs{MeshEndpoint: endpoint.DialAddress(), Username: string(input.Username), Password: []byte(input.Password), MeshID: string(input.MeshID), AgentInstanceID: input.AgentInstanceID}, nil
}

func validateAuthFields(username, password, meshID, endpoint string) error {
	if err := requiredBounded("username", username, maxIdentifierLength); err != nil {
		return err
	}
	if err := requiredBounded("password", password, maxPublicString); err != nil {
		return err
	}
	if err := requiredBounded("mesh_id", meshID, maxIdentifierLength); err != nil {
		return err
	}
	return requiredBounded("mesh_endpoint", endpoint, maxPublicString)
}

func decodeSend(to v1.AgentID, payload v1.Payload) (model.MessageSendArgs, error) {
	if err := validateAgentIdentity("to", string(to)); err != nil {
		return model.MessageSendArgs{}, err
	}
	decoded, err := decodePayload(payload)
	if err != nil {
		return model.MessageSendArgs{}, err
	}
	return model.MessageSendArgs{To: string(to), Payload: decoded}, nil
}

// validateAgentIdentity enforces the transport-opaque public AgentID contract.
// It deliberately does not parse, normalize, or assign routing meaning to the
// identifier.
func validateAgentIdentity(name, value string) error {
	if value == "" {
		return malformed(name)
	}
	if len(value) > maxAgentIdentityLength {
		return ErrInputTooLarge
	}
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return malformed(name)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return malformed(name)
		}
	}
	return nil
}

func decodePayload(payload v1.Payload) (model.Payload, error) {
	if err := validatePublicPayload(payload); err != nil {
		return model.Payload{}, err
	}
	switch value := payload.Value.(type) {
	case v1.NativePayload:
		return model.Payload{Value: model.NativePayload{ContentType: value.ContentType, Path: value.Path, Body: cloneBytes(value.Body)}}, nil
	case v1.HTTPRequestPayload:
		headers, _, err := decodeHeaders(value.Headers)
		if err != nil {
			return model.Payload{}, err
		}
		return model.Payload{Value: model.HTTPRequestPayload{Method: value.Method, Path: value.Path, Query: value.Query, Headers: headers, Body: cloneBytes(value.Body)}}, nil
	case v1.HTTPResponsePayload:
		headers, _, err := decodeHeaders(value.Headers)
		if err != nil {
			return model.Payload{}, err
		}
		return model.Payload{Value: model.HTTPResponsePayload{StatusCode: value.StatusCode, Reason: value.Reason, Headers: headers, Body: cloneBytes(value.Body), Error: decodeApplicationError(value.Error)}}, nil
	case v1.PayloadHandle:
		return model.Payload{Value: model.PayloadHandle{Handle: string(value)}}, nil
	default:
		return model.Payload{}, malformed("payload variant")
	}
}

func validatePublicPayload(payload v1.Payload) error {
	switch value := payload.Value.(type) {
	case v1.NativePayload:
		if err := validatePath(value.Path); err != nil {
			return err
		}
		if err := validateContentType(value.ContentType); err != nil {
			return err
		}
		if canonicalSize(value.ContentType, value.Path)+len(value.Body) > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	case v1.HTTPRequestPayload:
		if err := validateHTTPMethod(value.Method); err != nil {
			return err
		}
		if err := validatePath(value.Path); err != nil {
			return err
		}
		if len(value.Query) > maxPublicString {
			return ErrInputTooLarge
		}
		headerSize, err := validatePublicHeaders(value.Headers)
		if err != nil {
			return err
		}
		if headerSize+canonicalSize(value.Method, value.Path, value.Query)+len(value.Body) > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	case v1.HTTPResponsePayload:
		if value.StatusCode < 100 || value.StatusCode > 599 {
			return malformed("http response")
		}
		if err := validateReason(value.Reason); err != nil {
			return err
		}
		if value.Error != nil && value.StatusCode < 400 {
			return malformed("http response application error")
		}
		headerSize, err := validatePublicHeaders(value.Headers)
		if err != nil {
			return err
		}
		errorSize, err := validatePublicApplicationError(value.Error)
		if err != nil {
			return err
		}
		if headerSize+len(value.Reason)+len(value.Body)+errorSize > maxInlinePayloadBytes {
			return ErrInputTooLarge
		}
		return nil
	case v1.PayloadHandle:
		return validatePayloadHandle(value)
	default:
		return malformed("payload variant")
	}
}

func validatePublicHeaders(values []v1.Header) (int, error) {
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

func decodeHeaders(values []v1.Header) ([]model.Header, int, error) {
	if len(values) > maxHeaderCount {
		return nil, 0, ErrInputTooLarge
	}
	output := make([]model.Header, len(values))
	total := 0
	for i, value := range values {
		if len(value.Name) > maxIdentifierLength || len(value.Value) > maxPublicString {
			return nil, 0, ErrInputTooLarge
		}
		if !validHTTPToken(value.Name) || strings.ContainsAny(value.Value, "\r\n") {
			return nil, 0, malformed("header")
		}
		total += len(value.Name) + len(value.Value)
		if total > maxInlinePayloadBytes {
			return nil, 0, ErrInputTooLarge
		}
		output[i] = model.Header{Name: value.Name, Value: value.Value}
	}
	return output, total, nil
}

func canonicalSize(values ...string) int {
	total := 0
	for _, value := range values {
		total += len(value)
	}
	return total
}

func validatePath(path string) error {
	if path == "" || !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#") {
		return malformed("path")
	}
	if len(path) > maxPathLength {
		return ErrInputTooLarge
	}
	return nil
}

func validateHTTPMethod(method string) error {
	if len(method) > 32 {
		return ErrInputTooLarge
	}
	if !validHTTPToken(method) {
		return malformed("method")
	}
	return nil
}

func validateContentType(value string) error {
	if len(value) > maxIdentifierLength {
		return ErrInputTooLarge
	}
	if !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n") {
		return malformed("content_type")
	}
	return nil
}

func validateReason(value string) error {
	if len(value) > maxIdentifierLength {
		return ErrInputTooLarge
	}
	if !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n") {
		return malformed("reason")
	}
	return nil
}

func validHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for index := range value {
		character := value[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func validateConfigUpdate(update v1.ConfigUpdate) error {
	if update.CommandTimeout != nil && *update.CommandTimeout < 0 || update.RPCTimeout != nil && *update.RPCTimeout < 0 || update.QueueLimit != nil && (*update.QueueLimit == 0 || *update.QueueLimit > maxQueueCapacity) || update.PayloadLimit != nil && (*update.PayloadLimit == 0 || *update.PayloadLimit > maxCanonicalPayloadSize) {
		return malformed("config update")
	}
	return nil
}

func normalizedDuration(value *time.Duration) *time.Duration {
	copy := cloneDuration(value)
	if copy != nil && *copy == 0 {
		*copy = defaultApplicationTimeout
	}
	return copy
}
func validateOrigin(raw string) error {
	if err := requiredBounded("virtual_origin", raw, maxPublicString); err != nil {
		return err
	}
	if _, err := model.ParseAddressOrigin(raw); err != nil {
		return malformed("virtual_origin")
	}
	return nil
}
func validateApplicationURL(raw string) error {
	if err := requiredBounded("url", raw, maxPublicString); err != nil {
		return err
	}
	parsed, err := model.ParseAddressURL(raw)
	if err != nil {
		return malformed("url")
	}
	if err := validatePath(parsed.Path()); err != nil {
		return err
	}
	return nil
}
func decodeRules(input []v1.PolicyRule) ([]model.PolicyRule, error) {
	if len(input) > maxPolicyRules {
		return nil, ErrInputTooLarge
	}
	for _, rule := range input {
		if rule.Action != v1.PolicyActionAllow && rule.Action != v1.PolicyActionDeny {
			return nil, malformed("policy action")
		}
		if err := validatePolicyPath(rule.Path); err != nil {
			return nil, err
		}
		if len(rule.AgentID) > maxIdentifierLength {
			return nil, ErrInputTooLarge
		}
	}
	output := make([]model.PolicyRule, len(input))
	for i, rule := range input {
		output[i] = model.PolicyRule{Action: string(rule.Action), Path: rule.Path, AgentID: string(rule.AgentID)}
	}
	return output, nil
}

func validatePolicyPath(path string) error {
	if path == "" {
		return nil
	}
	return validatePath(path)
}
func payloadHandleCommand(base v1.CommandBase, handle v1.PayloadHandle) (v1.CommandBase, model.CommandArgs, error) {
	if err := validatePayloadHandle(handle); err != nil {
		return v1.CommandBase{}, nil, err
	}
	return base, model.PayloadHandleArgs{Handle: string(handle)}, nil
}
func messageIDCommand(base v1.CommandBase, id v1.MessageID) (v1.CommandBase, model.CommandArgs, error) {
	if err := requiredBounded("message_id", string(id), maxIdentifierLength); err != nil {
		return v1.CommandBase{}, nil, err
	}
	return base, model.MessageIDArgs{MessageID: string(id)}, nil
}
func conversationCommand(base v1.CommandBase, id v1.ConversationID) (v1.CommandBase, model.CommandArgs, error) {
	if err := requiredBounded("conversation_id", string(id), maxIdentifierLength); err != nil {
		return v1.CommandBase{}, nil, err
	}
	return base, model.ConversationIDArgs{ConversationID: string(id)}, nil
}
func handlerCommand(base v1.CommandBase, path string) (v1.CommandBase, model.CommandArgs, error) {
	if err := validatePath(path); err != nil {
		return v1.CommandBase{}, nil, err
	}
	return base, model.HandlerPathArgs{Path: path}, nil
}
func validatePayloadHandle(handle v1.PayloadHandle) error {
	return validateClassHandle("payload_handle", string(handle), payloadHandlePrefix, payloadHandleBytes)
}
func validateRequestHandle(handle v1.RequestHandle) error {
	return validateClassHandle("request_handle", string(handle), requestHandlePrefix, requestHandleBytes)
}
func validateCommandHandle(handle v1.CommandHandle) error {
	return validateClassHandle("command_handle", string(handle), commandHandlePrefix, commandHandleBytes)
}

// ValidateCommandHandle applies the frozen command-handle shape before the
// private registry performs Core-scope and lifetime validation.
func (a *Adapter) ValidateCommandHandle(handle v1.CommandHandle) error {
	return validateCommandHandle(handle)
}
func validateClassHandle(name, handle, prefix string, byteLength int) error {
	if handle == "" || len(handle) > maxIdentifierLength {
		return invalidHandle(name)
	}
	encoded := strings.TrimPrefix(handle, prefix)
	if encoded == handle {
		return invalidHandle(name)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != byteLength || prefix+base64.RawURLEncoding.EncodeToString(decoded) != handle {
		return invalidHandle(name)
	}
	return nil
}
func requiredBounded(name, value string, limit int) error {
	if value == "" {
		return malformed(name)
	}
	if len(value) > limit {
		return ErrInputTooLarge
	}
	return nil
}
func malformed(field string) error     { return fmt.Errorf("%w: %s", ErrMalformedInput, field) }
func invalidHandle(field string) error { return fmt.Errorf("%w: %s", ErrInvalidHandle, field) }
func cloneBytes(value []byte) []byte   { return append([]byte(nil), value...) }
func cloneDuration(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func decodeTokenAuth(base v1.CommandBase, input v1.AuthTokenInput) (v1.CommandBase, model.CommandArgs, error) {
	if !utf8.ValidString(input.Token) {
		return v1.CommandBase{}, nil, malformed("token")
	}
	if err := requiredBounded("token", input.Token, maxAuthTokenBytes); err != nil {
		return v1.CommandBase{}, nil, err
	}
	if err := validateTokenAuthContext(string(input.MeshID)); err != nil {
		return v1.CommandBase{}, nil, err
	}
	bare, err := enrollment.ParsePublicToken([]byte(input.Token))
	if err != nil {
		return v1.CommandBase{}, nil, malformed("token")
	}
	profileID := input.ProfileID
	if profileID == "" {
		profileID = enrollment.DefaultProfileID
	}
	if !enrollment.ValidProfileID(profileID) {
		clear(bare)
		return v1.CommandBase{}, nil, malformed("profile_id")
	}
	return base, model.TokenAuthArgs{Token: bare, MeshID: string(input.MeshID), ProfileID: profileID, ForceEnroll: input.ForceEnroll}, nil
}

func decodeInstallationAuth(base v1.CommandBase, input v1.AuthInstallationInput) (v1.CommandBase, model.CommandArgs, error) {
	if !enrollment.ValidProfileID(input.ProfileID) {
		return v1.CommandBase{}, nil, malformed("profile_id")
	}
	if err := validateTokenAuthContext(string(input.MeshID)); err != nil {
		return v1.CommandBase{}, nil, err
	}
	return base, model.InstallationAuthArgs{ProfileID: input.ProfileID, MeshID: string(input.MeshID)}, nil
}

func validateTokenAuthContext(meshID string) error {
	return requiredBounded("mesh_id", meshID, 255)
}
func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
