package sdkboundary

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestDeliveryNormalizesPointerEventBeforeSDKMapping(t *testing.T) {
	input := &model.MessageReceivedEvent{
		MessageID:      "message",
		ConversationID: "conversation",
		FromAgentID:    "agent",
		MeshID:         "mesh",
		Mode:           "msg",
		Payload: model.Payload{Value: &model.NativePayload{
			ContentType: "application/octet-stream",
			Path:        "/",
			Body:        []byte("value"),
		}},
	}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: input}
	dispatcher, err := delivery.NewWithByteCapacity(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	got, err := dispatcher.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gotValue, ok := got.Value.(model.MessageReceivedEvent)
	if !ok {
		t.Fatalf("dispatcher retained pointer event %T", got.Value)
	}
	if _, ok = gotValue.Payload.Value.(model.NativePayload); !ok {
		t.Fatalf("dispatcher retained pointer payload %T", gotValue.Payload.Value)
	}
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.MapEvent(got); err != nil {
		t.Fatalf("normalized event did not map: %v", err)
	}
	if input.MessageID != "message" || string(input.Payload.Value.(*model.NativePayload).Body) != "value" {
		t.Fatal("successful normalization mutated caller pointer")
	}
}
