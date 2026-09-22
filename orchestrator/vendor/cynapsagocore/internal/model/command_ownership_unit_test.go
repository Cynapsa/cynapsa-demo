package model

import (
	"reflect"
	"testing"
	"time"
)

func TestFreezeCommandCoversClosedValueAndPointerCatalog(t *testing.T) {
	duration := time.Second
	queue := uint32(4)
	payloadLimit := uint64(1024)
	values := []CommandArgs{
		EmptyArgs{},
		ConfigUpdateArgs{CommandTimeout: &duration, RPCTimeout: &duration, QueueLimit: &queue, PayloadLimit: &payloadLimit},
		TokenAuthArgs{Token: []byte("token"), MeshID: "mesh"},
		AuthArgs{MeshEndpoint: "mesh", Username: "agent", Password: []byte("secret"), MeshID: "mesh", AgentInstanceID: "instance"},
		AddressPutArgs{Mapping: AddressMapping{VirtualOrigin: "https://origin", Recipient: "agent"}},
		AddressRemoveArgs{VirtualOrigin: "https://origin"},
		AddressResolveArgs{URL: "https://origin/path"},
		MessageSendArgs{To: "agent", Payload: Payload{Value: NativePayload{ContentType: "text/plain", Path: "/", Body: []byte("body")}}},
		MessageRequestArgs{To: "agent", Payload: Payload{Value: HTTPRequestPayload{Method: "GET", Path: "/", Headers: []Header{{Name: "x", Value: "y"}}, Body: []byte("body")}}, TTL: time.Second},
		MessageReplyArgs{RequestHandle: "request", Payload: Payload{Value: HTTPResponsePayload{StatusCode: 200, Body: []byte("body")}}},
		DeliveryAcceptArgs{EventID: "event"}, MessageIDArgs{MessageID: "message"}, HandlerPathArgs{Path: "/"},
		PayloadHandleArgs{Handle: "payload"}, PayloadWriteArgs{Handle: "payload", Chunk: []byte("chunk")}, PayloadReadArgs{Handle: "payload", Limit: 1},
		ConversationIDArgs{ConversationID: "conversation"}, PolicySetArgs{Rules: []PolicyRule{{Action: "allow", Path: "/", AgentID: "agent"}}},
		PolicyTestArgs{Input: MessageSendArgs{To: "agent", Payload: Payload{Value: PayloadHandle{Handle: "payload"}}}},
		DiagnosticsPeerArgs{Peer: "agent"}, DiagnosticsLogsArgs{Enabled: true}, ChannelRegisterArgs{Capacity: 1}, ChannelIDArgs{ChannelID: "channel"},
		CommandCancelArgs{CommandHandle: "command"}, EventSinkRegisterArgs{Capacity: 1}, EventSinkIDArgs{SinkID: "sink"},
	}
	for index, value := range values {
		valueCommand := Command{ID: "value", Name: "test", SessionID: "session", Args: value}
		frozenValue, valueBytes, err := FreezeCommand(valueCommand)
		if err != nil {
			t.Fatalf("value %d (%T): %v", index, value, err)
		}
		canonicalValue, movedValueBytes, ok := TakeFrozenCommand(&frozenValue)
		if !ok || movedValueBytes != valueBytes {
			t.Fatalf("value %d (%T) move = (%d, %v), want (%d, true)", index, value, movedValueBytes, ok, valueBytes)
		}
		pointer := reflect.New(reflect.TypeOf(value))
		pointer.Elem().Set(reflect.ValueOf(value))
		pointerArgs, ok := pointer.Interface().(CommandArgs)
		if !ok {
			t.Fatalf("pointer %d (%T) is outside closed catalog", index, value)
		}
		pointerCommand := Command{ID: "value", Name: "test", SessionID: "session", Args: pointerArgs}
		frozenPointer, pointerBytes, err := FreezeCommand(pointerCommand)
		if err != nil || pointerBytes != valueBytes {
			t.Fatalf("pointer %d (%T): bytes=%d want=%d err=%v", index, value, pointerBytes, valueBytes, err)
		}
		canonicalPointer, movedPointerBytes, ok := TakeFrozenCommand(&frozenPointer)
		if !ok || movedPointerBytes != pointerBytes {
			t.Fatalf("pointer %d (%T) move = (%d, %v), want (%d, true)", index, value, movedPointerBytes, ok, pointerBytes)
		}
		ClearCommand(&canonicalValue)
		ClearCommand(&canonicalPointer)
	}
}

func TestFreezeCommandCoversPayloadPointerCatalog(t *testing.T) {
	values := []PayloadValue{
		&NativePayload{Body: []byte("native")},
		&HTTPRequestPayload{Headers: []Header{{Name: "authorization", Value: "secret"}}, Body: []byte("request")},
		&HTTPResponsePayload{Headers: []Header{{Name: "set-cookie", Value: "secret"}}, Body: []byte("response")},
		&PayloadHandle{Handle: "payload"},
	}
	for index, value := range values {
		command := Command{ID: "pointer", Name: "message.send", Args: MessageSendArgs{To: "agent", Payload: Payload{Value: value}}}
		frozen, _, err := FreezeCommand(command)
		if err != nil {
			t.Fatalf("payload pointer %d (%T): %v", index, value, err)
		}
		canonical, _, ok := TakeFrozenCommand(&frozen)
		if !ok {
			t.Fatalf("payload pointer %d ownership move failed", index)
		}
		if reflect.TypeOf(canonical.Args.(MessageSendArgs).Payload.Value) != reflect.TypeOf(value) {
			t.Fatalf("payload pointer %d changed type from %T to %T", index, value, canonical.Args.(MessageSendArgs).Payload.Value)
		}
		ClearCommand(&canonical)
	}
	var nilNative *NativePayload
	if _, _, err := FreezeCommand(Command{ID: "nil", Name: "message.send", Args: MessageSendArgs{Payload: Payload{Value: nilNative}}}); err == nil {
		t.Fatal("typed nil payload pointer was accepted")
	}
}

func TestFreezeCommandOwnsAndClearsApplicationError(t *testing.T) {
	inputError := &ApplicationError{Code: "not_found", Detail: "Missing", DetailsJSON: `{"id":7}`}
	command := Command{ID: "command", Name: "message.reply", Args: MessageReplyArgs{
		RequestHandle: "request",
		Payload:       Payload{Value: HTTPResponsePayload{StatusCode: 404, Error: inputError}},
	}}
	frozen, _, err := FreezeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _, ok := TakeFrozenCommand(&frozen)
	if !ok {
		t.Fatal("ownership move failed")
	}
	ownedError := canonical.Args.(MessageReplyArgs).Payload.Value.(HTTPResponsePayload).Error
	inputError.Code = "mutated"
	if ownedError == nil || ownedError == inputError || ownedError.Code != "not_found" {
		t.Fatalf("application error was not independently frozen: %#v", ownedError)
	}
	ClearCommand(&canonical)
	if *ownedError != (ApplicationError{}) {
		t.Fatalf("application error was not cleared: %#v", ownedError)
	}
}

func TestValidateFrozenCommandRemeasureIsAllocationFree(t *testing.T) {
	frozen, bytes, err := FreezeCommand(Command{ID: "command", Name: "message.send", SessionID: "session", Args: &MessageSendArgs{To: "agent", Payload: Payload{Value: &HTTPRequestPayload{Method: "POST", Path: "/", Headers: []Header{{Name: "x", Value: "y"}}, Body: []byte("body")}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ClearCommand(&frozen)
	if measured, ok := ValidateFrozenCommand(frozen); !ok || measured != bytes {
		t.Fatalf("ValidateFrozenCommand = (%d, %v), want (%d, true)", measured, ok, bytes)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, ok := ValidateFrozenCommand(frozen); !ok {
			panic("frozen command became invalid")
		}
	}); allocations != 0 {
		t.Fatalf("ValidateFrozenCommand allocations = %v, want 0", allocations)
	}
}
