package model

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"time"
	"unsafe"
)

var commandFingerprintSeeds = [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}

type commandInspection struct {
	bytes       int64
	fingerprint [2]uint64
}

type commandInspector struct {
	bytes int64
	hash  [2]maphash.Hash
}

func inspectCommand(command Command) (commandInspection, error) {
	var inspector commandInspector
	inspector.hash[0].SetSeed(commandFingerprintSeeds[0])
	inspector.hash[1].SetSeed(commandFingerprintSeeds[1])
	if !inspector.add(int64(unsafe.Sizeof(Command{}))) || !inspector.add(int64(unsafe.Sizeof(commandOwnership{}))) {
		return commandInspection{}, ErrInvalidCommandOwnership
	}
	inspector.string(command.ID)
	inspector.string(command.Name)
	inspector.string(command.SessionID)
	if !inspectCommandArgs(&inspector, command.Args) || inspector.bytes < 0 {
		return commandInspection{}, ErrInvalidCommandOwnership
	}
	return commandInspection{bytes: inspector.bytes, fingerprint: [2]uint64{inspector.hash[0].Sum64(), inspector.hash[1].Sum64()}}, nil
}

func inspectCommandArgs(inspector *commandInspector, input CommandArgs) bool {
	switch value := input.(type) {
	case nil:
		inspector.tag(0)
		return true
	case EmptyArgs:
		return inspectType[EmptyArgs](inspector, 1)
	case ConfigUpdateArgs:
		if !inspectType[ConfigUpdateArgs](inspector, 2) {
			return false
		}
		return inspector.duration(value.CommandTimeout) && inspector.duration(value.RPCTimeout) && inspector.uint32Pointer(value.QueueLimit) && inspector.uint64Pointer(value.PayloadLimit)
	case TokenAuthArgs:
		if !inspectType[TokenAuthArgs](inspector, 26) {
			return false
		}
		inspector.byteSlice(value.Token)
		inspector.string(value.MeshID)
		inspector.string(value.ProfileID)
		return inspector.valid()
	case InstallationAuthArgs:
		if !inspectType[InstallationAuthArgs](inspector, 27) {
			return false
		}
		inspector.string(value.ProfileID)
		inspector.string(value.MeshID)
		return inspector.valid()
	case AuthArgs:
		if !inspectType[AuthArgs](inspector, 3) {
			return false
		}
		inspector.string(value.MeshEndpoint)
		inspector.string(value.Username)
		inspector.byteSlice(value.Password)
		inspector.string(value.MeshID)
		inspector.string(value.AgentInstanceID)
		return inspector.valid()
	case AddressPutArgs:
		if !inspectType[AddressPutArgs](inspector, 4) {
			return false
		}
		inspector.string(value.Mapping.VirtualOrigin)
		inspector.string(value.Mapping.Recipient)
		return inspector.valid()
	case AddressRemoveArgs:
		if !inspectType[AddressRemoveArgs](inspector, 5) {
			return false
		}
		inspector.string(value.VirtualOrigin)
		return inspector.valid()
	case AddressResolveArgs:
		if !inspectType[AddressResolveArgs](inspector, 6) {
			return false
		}
		inspector.string(value.URL)
		return inspector.valid()
	case MessageSendArgs:
		return inspectType[MessageSendArgs](inspector, 7) && inspectMessageSend(inspector, value)
	case MessageRequestArgs:
		if !inspectType[MessageRequestArgs](inspector, 8) {
			return false
		}
		inspector.string(value.To)
		inspector.signed64(int64(value.TTL))
		return inspectPayload(inspector, value.Payload)
	case MessageReplyArgs:
		if !inspectType[MessageReplyArgs](inspector, 9) {
			return false
		}
		inspector.string(value.RequestHandle)
		return inspectPayload(inspector, value.Payload)
	case DeliveryAcceptArgs:
		return inspectSingleString[DeliveryAcceptArgs](inspector, 10, value.EventID)
	case MessageIDArgs:
		return inspectSingleString[MessageIDArgs](inspector, 11, value.MessageID)
	case HandlerPathArgs:
		return inspectSingleString[HandlerPathArgs](inspector, 12, value.Path)
	case PayloadHandleArgs:
		return inspectSingleString[PayloadHandleArgs](inspector, 13, value.Handle)
	case PayloadWriteArgs:
		if !inspectType[PayloadWriteArgs](inspector, 14) {
			return false
		}
		inspector.string(value.Handle)
		inspector.byteSlice(value.Chunk)
		return inspector.valid()
	case PayloadReadArgs:
		if !inspectType[PayloadReadArgs](inspector, 15) {
			return false
		}
		inspector.string(value.Handle)
		inspector.unsigned64(value.Offset)
		inspector.unsigned64(uint64(value.Limit))
		return inspector.valid()
	case ConversationIDArgs:
		return inspectSingleString[ConversationIDArgs](inspector, 16, value.ConversationID)
	case PolicySetArgs:
		if !inspectType[PolicySetArgs](inspector, 17) || !inspectSlice[PolicyRule](inspector, len(value.Rules)) {
			return false
		}
		for _, rule := range value.Rules {
			inspector.string(rule.Action)
			inspector.string(rule.Path)
			inspector.string(rule.AgentID)
		}
		return inspector.valid()
	case PolicyTestArgs:
		return inspectType[PolicyTestArgs](inspector, 18) && inspectMessageSend(inspector, value.Input)
	case DiagnosticsPeerArgs:
		return inspectSingleString[DiagnosticsPeerArgs](inspector, 19, value.Peer)
	case DiagnosticsLogsArgs:
		if !inspectType[DiagnosticsLogsArgs](inspector, 20) {
			return false
		}
		inspector.boolean(value.Enabled)
		return true
	case ChannelRegisterArgs:
		return inspectUint32[ChannelRegisterArgs](inspector, 21, value.Capacity)
	case ChannelIDArgs:
		return inspectSingleString[ChannelIDArgs](inspector, 22, value.ChannelID)
	case CommandCancelArgs:
		return inspectSingleString[CommandCancelArgs](inspector, 23, value.CommandHandle)
	case EventSinkRegisterArgs:
		return inspectUint32[EventSinkRegisterArgs](inspector, 24, value.Capacity)
	case EventSinkIDArgs:
		return inspectSingleString[EventSinkIDArgs](inspector, 25, value.SinkID)
	case *EmptyArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *ConfigUpdateArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *TokenAuthArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *InstallationAuthArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *AuthArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *AddressPutArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *AddressRemoveArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *AddressResolveArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *MessageSendArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *MessageRequestArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *MessageReplyArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *DeliveryAcceptArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *MessageIDArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *HandlerPathArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *PayloadHandleArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *PayloadWriteArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *PayloadReadArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *ConversationIDArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *PolicySetArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *PolicyTestArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *DiagnosticsPeerArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *DiagnosticsLogsArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *ChannelRegisterArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *ChannelIDArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *CommandCancelArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *EventSinkRegisterArgs:
		return inspectCommandArgsPointer(inspector, value)
	case *EventSinkIDArgs:
		return inspectCommandArgsPointer(inspector, value)
	default:
		return false
	}
}

func inspectCommandArgsPointer[T CommandArgs](inspector *commandInspector, value *T) bool {
	if value == nil {
		return false
	}
	inspector.tag(0xfe)
	return inspectCommandArgs(inspector, *value)
}

func inspectMessageSend(inspector *commandInspector, value MessageSendArgs) bool {
	inspector.string(value.To)
	return inspectPayload(inspector, value.Payload)
}

func inspectPayload(inspector *commandInspector, payload Payload) bool {
	switch value := payload.Value.(type) {
	case nil:
		inspector.tag(0)
		return true
	case NativePayload:
		if !inspectType[NativePayload](inspector, 1) {
			return false
		}
		inspector.string(value.ContentType)
		inspector.string(value.Path)
		inspector.byteSlice(value.Body)
		return inspector.valid()
	case HTTPRequestPayload:
		if !inspectType[HTTPRequestPayload](inspector, 2) || !inspectSlice[Header](inspector, len(value.Headers)) {
			return false
		}
		inspector.string(value.Method)
		inspector.string(value.Path)
		inspector.string(value.Query)
		for _, header := range value.Headers {
			inspector.string(header.Name)
			inspector.string(header.Value)
		}
		inspector.byteSlice(value.Body)
		return inspector.valid()
	case HTTPResponsePayload:
		if !inspectType[HTTPResponsePayload](inspector, 3) || !inspectSlice[Header](inspector, len(value.Headers)) {
			return false
		}
		inspector.unsigned64(uint64(value.StatusCode))
		inspector.string(value.Reason)
		for _, header := range value.Headers {
			inspector.string(header.Name)
			inspector.string(header.Value)
		}
		inspector.byteSlice(value.Body)
		inspector.boolean(value.Error != nil)
		if value.Error != nil {
			if !inspectType[ApplicationError](inspector, 0xfc) {
				return false
			}
			inspector.string(value.Error.Code)
			inspector.string(value.Error.Detail)
			inspector.string(value.Error.DetailsJSON)
		}
		return inspector.valid()
	case PayloadHandle:
		return inspectSingleString[PayloadHandle](inspector, 4, value.Handle)
	case *NativePayload:
		return inspectPayloadPointer(inspector, value)
	case *HTTPRequestPayload:
		return inspectPayloadPointer(inspector, value)
	case *HTTPResponsePayload:
		return inspectPayloadPointer(inspector, value)
	case *PayloadHandle:
		return inspectPayloadPointer(inspector, value)
	default:
		return false
	}
}

func inspectPayloadPointer[T PayloadValue](inspector *commandInspector, value *T) bool {
	if value == nil {
		return false
	}
	inspector.tag(0xfd)
	return inspectPayload(inspector, Payload{Value: *value})
}

func inspectType[T any](inspector *commandInspector, tag byte) bool {
	inspector.tag(tag)
	return inspector.add(int64(unsafe.Sizeof(*new(T))))
}

func inspectSingleString[T any](inspector *commandInspector, tag byte, value string) bool {
	if !inspectType[T](inspector, tag) {
		return false
	}
	inspector.string(value)
	return inspector.valid()
}

func inspectUint32[T any](inspector *commandInspector, tag byte, value uint32) bool {
	if !inspectType[T](inspector, tag) {
		return false
	}
	inspector.unsigned64(uint64(value))
	return true
}

func (inspector *commandInspector) valid() bool { return inspector != nil && inspector.bytes >= 0 }

func (inspector *commandInspector) add(value int64) bool {
	if inspector == nil || inspector.bytes < 0 || value < 0 || inspector.bytes > math.MaxInt64-value {
		if inspector != nil {
			inspector.bytes = -1
		}
		return false
	}
	inspector.bytes += value
	return true
}

func (inspector *commandInspector) tag(value byte) {
	encoded := [1]byte{value}
	inspector.write(encoded[:])
}

func (inspector *commandInspector) write(value []byte) {
	_, _ = inspector.hash[0].Write(value)
	_, _ = inspector.hash[1].Write(value)
}

func (inspector *commandInspector) unsigned64(value uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	inspector.write(encoded[:])
}

func (inspector *commandInspector) signed64(value int64) { inspector.unsigned64(uint64(value)) }

func (inspector *commandInspector) boolean(value bool) {
	if value {
		inspector.tag(1)
		return
	}
	inspector.tag(0)
}

func (inspector *commandInspector) string(value string) {
	inspector.unsigned64(uint64(len(value)))
	_, _ = inspector.hash[0].WriteString(value)
	_, _ = inspector.hash[1].WriteString(value)
	inspector.add(int64(len(value)))
}

func (inspector *commandInspector) byteSlice(value []byte) {
	inspector.unsigned64(uint64(len(value)))
	inspector.write(value)
	inspector.add(int64(len(value)))
}

func inspectSlice[T any](inspector *commandInspector, count int) bool {
	size := int64(unsafe.Sizeof(*new(T)))
	if count < 0 || size != 0 && int64(count) > math.MaxInt64/size {
		inspector.bytes = -1
		return false
	}
	inspector.unsigned64(uint64(count))
	return inspector.add(int64(count) * size)
}

func (inspector *commandInspector) duration(value *time.Duration) bool {
	if value == nil {
		inspector.tag(0)
		return true
	}
	inspector.tag(1)
	inspector.signed64(int64(*value))
	return inspector.add(int64(unsafe.Sizeof(*value)))
}

func (inspector *commandInspector) uint32Pointer(value *uint32) bool {
	if value == nil {
		inspector.tag(0)
		return true
	}
	inspector.tag(1)
	inspector.unsigned64(uint64(*value))
	return inspector.add(int64(unsafe.Sizeof(*value)))
}

func (inspector *commandInspector) uint64Pointer(value *uint64) bool {
	if value == nil {
		inspector.tag(0)
		return true
	}
	inspector.tag(1)
	inspector.unsigned64(*value)
	return inspector.add(int64(unsafe.Sizeof(*value)))
}
