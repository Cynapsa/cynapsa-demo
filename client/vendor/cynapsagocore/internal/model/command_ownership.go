package model

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var ErrInvalidCommandOwnership = errors.New("model: invalid command ownership")

// MaximumFrozenCommandBytes is the checked hard ceiling for one internal
// command snapshot. It is shared by boundary freezing and gate admission.
const MaximumFrozenCommandBytes int64 = 4 << 20

// commandOwnership is an unforgeable one-shot token attached only to a fully
// frozen command snapshot. Gate consumes it when the boundary-to-gate move
// succeeds, avoiding a second clone while plain internal Gate.Submit callers
// are still frozen independently by the gate.
type commandOwnership struct {
	mu          sync.Mutex
	bytes       int64
	fingerprint [2]uint64
	// canonical is the sole graph that may cross into the gate. The exported
	// Command returned by FreezeCommand is an empty opaque facade and shares no
	// canonical dynamic backing.
	canonical Command
	taken     bool
}

// FreezeCommand copies every reachable string, slice, pointer value, and
// secret from the closed CommandArgs catalog. The returned command shares no
// mutable backing or substring backing with input.
func FreezeCommand(input Command) (Command, int64, error) {
	inspection, err := inspectCommand(input)
	if err != nil || inspection.bytes > MaximumFrozenCommandBytes {
		return Command{}, 0, ErrInvalidCommandOwnership
	}
	command, total, err := freezeCommand(input)
	frozenInspection, inspectErr := inspectCommand(command)
	if err != nil || inspectErr != nil || total != inspection.bytes || frozenInspection != inspection {
		ClearCommand(&command)
		return Command{}, 0, ErrInvalidCommandOwnership
	}
	canonical := command
	canonical.ownership = nil
	ownership := &commandOwnership{bytes: total, fingerprint: inspection.fingerprint, canonical: canonical}
	// Return only an opaque facade. The canonical graph is reachable solely
	// through the unexported token, so copying or mutating the facade cannot
	// retain, observe, or alter canonical backing before or after transfer.
	return Command{ownership: ownership}, total, nil
}

// MeasureCommand computes the exact retained snapshot charge without
// allocating or retaining any caller-owned backing.
func MeasureCommand(input Command) (int64, error) {
	inspection, err := inspectCommand(input)
	if err != nil {
		return 0, err
	}
	return inspection.bytes, nil
}

func freezeCommand(input Command) (Command, int64, error) {
	return snapshotCommand(input, true)
}

func snapshotCommand(input Command, clone bool) (Command, int64, error) {
	var total int64
	if !addOwnedBytes(&total, int64(unsafe.Sizeof(Command{}))) {
		return Command{}, 0, ErrInvalidCommandOwnership
	}
	if !addOwnedBytes(&total, int64(unsafe.Sizeof(commandOwnership{}))) {
		return Command{}, 0, ErrInvalidCommandOwnership
	}
	command := Command{
		ID:        freezeString(input.ID, &total, clone),
		Name:      freezeString(input.Name, &total, clone),
		SessionID: freezeString(input.SessionID, &total, clone),
	}
	args, ok := freezeCommandArgs(input.Args, &total, clone)
	if !ok || total < 0 {
		ClearCommand(&command)
		return Command{}, 0, ErrInvalidCommandOwnership
	}
	command.Args = args
	return command, total, nil
}

// FrozenCommandBytes reports the exact retained byte charge of an unconsumed
// frozen command. It does not transfer ownership.
func FrozenCommandBytes(command Command) (int64, bool) {
	if command.ownership == nil {
		return 0, false
	}
	command.ownership.mu.Lock()
	defer command.ownership.mu.Unlock()
	if command.ownership.taken {
		return 0, false
	}
	return command.ownership.bytes, true
}

// FrozenCommandID lends the canonical identifier from an unconsumed opaque
// wrapper to the gate for pre-transfer admission checks. The later successful
// move carries the same backing, so registry and command ownership stay one
// charged snapshot rather than cloning an identifier.
func FrozenCommandID(command Command) (string, bool) {
	if command.ownership == nil {
		return "", false
	}
	command.ownership.mu.Lock()
	defer command.ownership.mu.Unlock()
	if command.ownership.taken || !opaqueFrozenFacade(command) {
		return "", false
	}
	return command.ownership.canonical.ID, true
}

// ValidateFrozenCommand nonallocatingly remeasures an unconsumed snapshot and
// verifies both its exact charge and its keyed closed-graph fingerprint.
func ValidateFrozenCommand(command Command) (int64, bool) {
	if command.ownership == nil {
		return 0, false
	}
	command.ownership.mu.Lock()
	defer command.ownership.mu.Unlock()
	if command.ownership.taken {
		return 0, false
	}
	if !opaqueFrozenFacade(command) {
		return 0, false
	}
	inspection, err := inspectCommand(command.ownership.canonical)
	if err != nil || inspection.bytes != command.ownership.bytes || inspection.fingerprint != command.ownership.fingerprint {
		return 0, false
	}
	return inspection.bytes, true
}

// TakeFrozenCommand performs the one-shot boundary-to-gate ownership move.
// On success input is emptied without clearing the moved backing.
func TakeFrozenCommand(input *Command) (Command, int64, bool) {
	if input == nil || input.ownership == nil {
		return Command{}, 0, false
	}
	ownership := input.ownership
	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if ownership.taken {
		return Command{}, 0, false
	}
	if !opaqueFrozenFacade(*input) {
		return Command{}, 0, false
	}
	inspection, err := inspectCommand(ownership.canonical)
	if err != nil || inspection.bytes != ownership.bytes || inspection.fingerprint != ownership.fingerprint {
		return Command{}, 0, false
	}
	ownership.taken = true
	command := ownership.canonical
	command.ownership = ownership
	command.canonical = true
	bytes := ownership.bytes
	ownership.canonical = Command{}
	*input = Command{}
	return command, bytes, true
}

// ClearCommand releases every reachable reference and zeroes byte-bearing
// secret/payload storage. It is idempotent and safe on a moved command.
func ClearCommand(command *Command) {
	if command == nil {
		return
	}
	if ownership := command.ownership; ownership != nil {
		ownership.mu.Lock()
		if !ownership.taken {
			// Clear only the private canonical graph. The opaque facade may contain
			// caller-injected aliases after freezing; cleanup must not mutate them.
			ownership.taken = true
			canonical := ownership.canonical
			ownership.canonical = Command{}
			ownership.mu.Unlock()
			clearCommandArgs(canonical.Args)
		} else {
			ownership.mu.Unlock()
			if command.canonical {
				clearCommandArgs(command.Args)
			}
		}
		// A copied facade whose shared token was already consumed has canonical
		// false and only drops its injected references. The moved canonical
		// command clears its owned graph above.
		*command = Command{}
		return
	}
	clearCommandArgs(command.Args)
	*command = Command{}
}

func opaqueFrozenFacade(command Command) bool {
	return command.ID == "" && command.Name == "" && command.SessionID == "" && command.Args == nil && !command.canonical
}

func freezeCommandArgs(input CommandArgs, total *int64, clone bool) (CommandArgs, bool) {
	switch value := input.(type) {
	case nil:
		return nil, true
	case EmptyArgs:
		addTypeBytes[EmptyArgs](total)
		return value, true
	case ConfigUpdateArgs:
		addTypeBytes[ConfigUpdateArgs](total)
		return ConfigUpdateArgs{CommandTimeout: freezeDuration(value.CommandTimeout, total, clone), RPCTimeout: freezeDuration(value.RPCTimeout, total, clone), QueueLimit: freezeUint32(value.QueueLimit, total, clone), PayloadLimit: freezeUint64(value.PayloadLimit, total, clone)}, true
	case TokenAuthArgs:
		addTypeBytes[TokenAuthArgs](total)
		return TokenAuthArgs{Token: freezeBytes(value.Token, total, clone), MeshID: freezeString(value.MeshID, total, clone), ProfileID: freezeString(value.ProfileID, total, clone), ForceEnroll: value.ForceEnroll}, true
	case InstallationAuthArgs:
		addTypeBytes[InstallationAuthArgs](total)
		return InstallationAuthArgs{ProfileID: freezeString(value.ProfileID, total, clone), MeshID: freezeString(value.MeshID, total, clone)}, true
	case AuthArgs:
		addTypeBytes[AuthArgs](total)
		return AuthArgs{MeshEndpoint: freezeString(value.MeshEndpoint, total, clone), Username: freezeString(value.Username, total, clone), Password: freezeBytes(value.Password, total, clone), MeshID: freezeString(value.MeshID, total, clone), AgentInstanceID: freezeString(value.AgentInstanceID, total, clone)}, true
	case AddressPutArgs:
		addTypeBytes[AddressPutArgs](total)
		return AddressPutArgs{Mapping: AddressMapping{VirtualOrigin: freezeString(value.Mapping.VirtualOrigin, total, clone), Recipient: freezeString(value.Mapping.Recipient, total, clone)}}, true
	case AddressRemoveArgs:
		addTypeBytes[AddressRemoveArgs](total)
		return AddressRemoveArgs{VirtualOrigin: freezeString(value.VirtualOrigin, total, clone)}, true
	case AddressResolveArgs:
		addTypeBytes[AddressResolveArgs](total)
		return AddressResolveArgs{URL: freezeString(value.URL, total, clone)}, true
	case MessageSendArgs:
		addTypeBytes[MessageSendArgs](total)
		frozen, ok := freezeMessageSend(value, total, clone)
		return frozen, ok
	case MessageRequestArgs:
		addTypeBytes[MessageRequestArgs](total)
		payload, ok := freezePayload(value.Payload, total, clone)
		return MessageRequestArgs{To: freezeString(value.To, total, clone), Payload: payload, TTL: value.TTL}, ok
	case MessageReplyArgs:
		addTypeBytes[MessageReplyArgs](total)
		payload, ok := freezePayload(value.Payload, total, clone)
		return MessageReplyArgs{RequestHandle: freezeString(value.RequestHandle, total, clone), Payload: payload}, ok
	case DeliveryAcceptArgs:
		addTypeBytes[DeliveryAcceptArgs](total)
		return DeliveryAcceptArgs{EventID: freezeString(value.EventID, total, clone)}, true
	case MessageIDArgs:
		addTypeBytes[MessageIDArgs](total)
		return MessageIDArgs{MessageID: freezeString(value.MessageID, total, clone)}, true
	case HandlerPathArgs:
		addTypeBytes[HandlerPathArgs](total)
		return HandlerPathArgs{Path: freezeString(value.Path, total, clone)}, true
	case PayloadHandleArgs:
		addTypeBytes[PayloadHandleArgs](total)
		return PayloadHandleArgs{Handle: freezeString(value.Handle, total, clone)}, true
	case PayloadWriteArgs:
		addTypeBytes[PayloadWriteArgs](total)
		return PayloadWriteArgs{Handle: freezeString(value.Handle, total, clone), Chunk: freezeBytes(value.Chunk, total, clone)}, true
	case PayloadReadArgs:
		addTypeBytes[PayloadReadArgs](total)
		return PayloadReadArgs{Handle: freezeString(value.Handle, total, clone), Offset: value.Offset, Limit: value.Limit}, true
	case ConversationIDArgs:
		addTypeBytes[ConversationIDArgs](total)
		return ConversationIDArgs{ConversationID: freezeString(value.ConversationID, total, clone)}, true
	case PolicySetArgs:
		addTypeBytes[PolicySetArgs](total)
		var rules []PolicyRule
		if clone {
			rules = make([]PolicyRule, len(value.Rules))
		}
		addSliceBytes[PolicyRule](total, len(value.Rules))
		for index, rule := range value.Rules {
			frozen := PolicyRule{Action: freezeString(rule.Action, total, clone), Path: freezeString(rule.Path, total, clone), AgentID: freezeString(rule.AgentID, total, clone)}
			if clone {
				rules[index] = frozen
			}
		}
		return PolicySetArgs{Rules: rules}, true
	case PolicyTestArgs:
		addTypeBytes[PolicyTestArgs](total)
		input, ok := freezeMessageSend(value.Input, total, clone)
		return PolicyTestArgs{Input: input}, ok
	case DiagnosticsPeerArgs:
		addTypeBytes[DiagnosticsPeerArgs](total)
		return DiagnosticsPeerArgs{Peer: freezeString(value.Peer, total, clone)}, true
	case DiagnosticsLogsArgs:
		addTypeBytes[DiagnosticsLogsArgs](total)
		return value, true
	case ChannelRegisterArgs:
		addTypeBytes[ChannelRegisterArgs](total)
		return value, true
	case ChannelIDArgs:
		addTypeBytes[ChannelIDArgs](total)
		return ChannelIDArgs{ChannelID: freezeString(value.ChannelID, total, clone)}, true
	case CommandCancelArgs:
		addTypeBytes[CommandCancelArgs](total)
		return CommandCancelArgs{CommandHandle: freezeString(value.CommandHandle, total, clone)}, true
	case EventSinkRegisterArgs:
		addTypeBytes[EventSinkRegisterArgs](total)
		return value, true
	case EventSinkIDArgs:
		addTypeBytes[EventSinkIDArgs](total)
		return EventSinkIDArgs{SinkID: freezeString(value.SinkID, total, clone)}, true
	case *EmptyArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *ConfigUpdateArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *TokenAuthArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *InstallationAuthArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *AuthArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *AddressPutArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *AddressRemoveArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *AddressResolveArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *MessageSendArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *MessageRequestArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *MessageReplyArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *DeliveryAcceptArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *MessageIDArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *HandlerPathArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *PayloadHandleArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *PayloadWriteArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *PayloadReadArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *ConversationIDArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *PolicySetArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *PolicyTestArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *DiagnosticsPeerArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *DiagnosticsLogsArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *ChannelRegisterArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *ChannelIDArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *CommandCancelArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *EventSinkRegisterArgs:
		return freezeCommandArgsPointer(value, total, clone)
	case *EventSinkIDArgs:
		return freezeCommandArgsPointer(value, total, clone)
	default:
		return nil, false
	}
}

func freezeCommandArgsPointer[T CommandArgs](value *T, total *int64, clone bool) (CommandArgs, bool) {
	if value == nil {
		return nil, false
	}
	frozen, ok := freezeCommandArgs(*value, total, clone)
	if !ok {
		return nil, false
	}
	typed, ok := frozen.(T)
	if !ok {
		return nil, false
	}
	pointer, ok := any(&typed).(CommandArgs)
	return pointer, ok
}

func freezeMessageSend(value MessageSendArgs, total *int64, clone bool) (MessageSendArgs, bool) {
	payload, ok := freezePayload(value.Payload, total, clone)
	return MessageSendArgs{To: freezeString(value.To, total, clone), Payload: payload}, ok
}

func freezePayload(input Payload, total *int64, clone bool) (Payload, bool) {
	switch value := input.Value.(type) {
	case nil:
		return Payload{}, true
	case NativePayload:
		addTypeBytes[NativePayload](total)
		return Payload{Value: NativePayload{ContentType: freezeString(value.ContentType, total, clone), Path: freezeString(value.Path, total, clone), Body: freezeBytes(value.Body, total, clone)}}, true
	case HTTPRequestPayload:
		addTypeBytes[HTTPRequestPayload](total)
		return Payload{Value: HTTPRequestPayload{Method: freezeString(value.Method, total, clone), Path: freezeString(value.Path, total, clone), Query: freezeString(value.Query, total, clone), Headers: freezeHeaders(value.Headers, total, clone), Body: freezeBytes(value.Body, total, clone)}}, true
	case HTTPResponsePayload:
		addTypeBytes[HTTPResponsePayload](total)
		return Payload{Value: HTTPResponsePayload{StatusCode: value.StatusCode, Reason: freezeString(value.Reason, total, clone), Headers: freezeHeaders(value.Headers, total, clone), Body: freezeBytes(value.Body, total, clone), Error: freezeApplicationError(value.Error, total, clone)}}, true
	case PayloadHandle:
		addTypeBytes[PayloadHandle](total)
		return Payload{Value: PayloadHandle{Handle: freezeString(value.Handle, total, clone)}}, true
	case *NativePayload:
		return freezePayloadPointer(value, total, clone)
	case *HTTPRequestPayload:
		return freezePayloadPointer(value, total, clone)
	case *HTTPResponsePayload:
		return freezePayloadPointer(value, total, clone)
	case *PayloadHandle:
		return freezePayloadPointer(value, total, clone)
	default:
		return Payload{}, false
	}
}

func freezeApplicationError(value *ApplicationError, total *int64, clone bool) *ApplicationError {
	if value == nil {
		return nil
	}
	addTypeBytes[ApplicationError](total)
	return &ApplicationError{
		Code:        freezeString(value.Code, total, clone),
		Detail:      freezeString(value.Detail, total, clone),
		DetailsJSON: freezeString(value.DetailsJSON, total, clone),
	}
}

func freezePayloadPointer[T PayloadValue](value *T, total *int64, clone bool) (Payload, bool) {
	if value == nil {
		return Payload{}, false
	}
	frozen, ok := freezePayload(Payload{Value: *value}, total, clone)
	if !ok {
		return Payload{}, false
	}
	typed, ok := frozen.Value.(T)
	if !ok {
		return Payload{}, false
	}
	pointer, ok := any(&typed).(PayloadValue)
	if !ok {
		return Payload{}, false
	}
	return Payload{Value: pointer}, true
}

func freezeHeaders(input []Header, total *int64, clone bool) []Header {
	var output []Header
	if clone {
		output = make([]Header, len(input))
	}
	addSliceBytes[Header](total, len(input))
	for index, header := range input {
		frozen := Header{Name: freezeString(header.Name, total, clone), Value: freezeString(header.Value, total, clone)}
		if clone {
			output[index] = frozen
		}
	}
	return output
}

func freezeString(value string, total *int64, clone bool) string {
	if !addOwnedBytes(total, int64(len(value))) {
		return ""
	}
	if !clone {
		return ""
	}
	return strings.Clone(value)
}

func freezeBytes(value []byte, total *int64, clone bool) []byte {
	if !addOwnedBytes(total, int64(len(value))) {
		return nil
	}
	if !clone {
		return nil
	}
	output := make([]byte, len(value))
	copy(output, value)
	return output
}

func freezeDuration(value *time.Duration, total *int64, clone bool) *time.Duration {
	if value == nil {
		return nil
	}
	addOwnedBytes(total, int64(unsafe.Sizeof(*value)))
	if !clone {
		return nil
	}
	copy := *value
	return &copy
}

func freezeUint32(value *uint32, total *int64, clone bool) *uint32 {
	if value == nil {
		return nil
	}
	addOwnedBytes(total, int64(unsafe.Sizeof(*value)))
	if !clone {
		return nil
	}
	copy := *value
	return &copy
}

func freezeUint64(value *uint64, total *int64, clone bool) *uint64 {
	if value == nil {
		return nil
	}
	addOwnedBytes(total, int64(unsafe.Sizeof(*value)))
	if !clone {
		return nil
	}
	copy := *value
	return &copy
}

func addTypeBytes[T any](total *int64) {
	addOwnedBytes(total, int64(unsafe.Sizeof(*new(T))))
}
func addSliceBytes[T any](total *int64, count int) {
	size := int64(unsafe.Sizeof(*new(T)))
	if count < 0 || size != 0 && int64(count) > math.MaxInt64/size {
		if total != nil {
			*total = -1
		}
		return
	}
	addOwnedBytes(total, int64(count)*size)
}
func addOwnedBytes(total *int64, value int64) bool {
	if total == nil || *total < 0 || value < 0 || *total > math.MaxInt64-value {
		if total != nil {
			*total = -1
		}
		return false
	}
	*total += value
	return true
}

func clearCommandArgs(input CommandArgs) {
	switch value := input.(type) {
	case TokenAuthArgs:
		clear(value.Token)
	case AuthArgs:
		clear(value.Password)
	case PayloadWriteArgs:
		clear(value.Chunk)
	case MessageSendArgs:
		clearPayload(value.Payload)
	case MessageRequestArgs:
		clearPayload(value.Payload)
	case MessageReplyArgs:
		clearPayload(value.Payload)
	case PolicySetArgs:
		clear(value.Rules)
	case PolicyTestArgs:
		clearPayload(value.Input.Payload)
	case *MessageIDArgs:
		if value != nil {
			*value = MessageIDArgs{}
		}
	case *EmptyArgs:
		clearCommandArgsPointer(value)
	case *ConfigUpdateArgs:
		clearCommandArgsPointer(value)
	case *TokenAuthArgs:
		clearCommandArgsPointer(value)
	case *InstallationAuthArgs:
		clearCommandArgsPointer(value)
	case *AuthArgs:
		clearCommandArgsPointer(value)
	case *AddressPutArgs:
		clearCommandArgsPointer(value)
	case *AddressRemoveArgs:
		clearCommandArgsPointer(value)
	case *AddressResolveArgs:
		clearCommandArgsPointer(value)
	case *MessageSendArgs:
		clearCommandArgsPointer(value)
	case *MessageRequestArgs:
		clearCommandArgsPointer(value)
	case *MessageReplyArgs:
		clearCommandArgsPointer(value)
	case *DeliveryAcceptArgs:
		clearCommandArgsPointer(value)
	case *HandlerPathArgs:
		clearCommandArgsPointer(value)
	case *PayloadHandleArgs:
		clearCommandArgsPointer(value)
	case *PayloadWriteArgs:
		clearCommandArgsPointer(value)
	case *PayloadReadArgs:
		clearCommandArgsPointer(value)
	case *ConversationIDArgs:
		clearCommandArgsPointer(value)
	case *PolicySetArgs:
		clearCommandArgsPointer(value)
	case *PolicyTestArgs:
		clearCommandArgsPointer(value)
	case *DiagnosticsPeerArgs:
		clearCommandArgsPointer(value)
	case *DiagnosticsLogsArgs:
		clearCommandArgsPointer(value)
	case *ChannelRegisterArgs:
		clearCommandArgsPointer(value)
	case *ChannelIDArgs:
		clearCommandArgsPointer(value)
	case *CommandCancelArgs:
		clearCommandArgsPointer(value)
	case *EventSinkRegisterArgs:
		clearCommandArgsPointer(value)
	case *EventSinkIDArgs:
		clearCommandArgsPointer(value)
	}
}

func clearCommandArgsPointer[T CommandArgs](value *T) {
	if value == nil {
		return
	}
	clearCommandArgs(*value)
	var zero T
	*value = zero
}

func clearPayload(payload Payload) {
	switch value := payload.Value.(type) {
	case NativePayload:
		clear(value.Body)
	case HTTPRequestPayload:
		clear(value.Body)
		clear(value.Headers)
	case HTTPResponsePayload:
		clear(value.Body)
		clear(value.Headers)
		if value.Error != nil {
			*value.Error = ApplicationError{}
		}
	case *NativePayload:
		clearPayloadPointer(value)
	case *HTTPRequestPayload:
		clearPayloadPointer(value)
	case *HTTPResponsePayload:
		clearPayloadPointer(value)
	case *PayloadHandle:
		clearPayloadPointer(value)
	}
}

func clearPayloadPointer[T PayloadValue](value *T) {
	if value == nil {
		return
	}
	clearPayload(Payload{Value: *value})
	var zero T
	*value = zero
}
