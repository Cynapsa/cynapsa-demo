package sdkboundary

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// TestEveryFrozenCapacityValueIsAccepted exhaustively verifies the inclusive
// 1..65,536 V1 domain on every Stage A capacity-bearing boundary surface.
func TestEveryFrozenCapacityValueIsAccepted(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	base := v1.CommandBase{CommandID: "command", SDKSessionID: "session"}
	now := time.Unix(1, 2).UTC()

	for capacity := uint32(1); capacity <= maxQueueCapacity; capacity++ {
		config, err := adapter.MapConfig(v1.Config{QueueLimit: capacity, PayloadLimit: 1})
		if err != nil {
			t.Fatalf("MapConfig capacity %d: %v", capacity, err)
		}
		if config.CommandTimeout != defaultApplicationTimeout || config.RPCTimeout != defaultApplicationTimeout {
			t.Fatalf("MapConfig capacity %d default timeouts = (%s, %s), want (%s, %s)", capacity, config.CommandTimeout, config.RPCTimeout, defaultApplicationTimeout, defaultApplicationTimeout)
		}
		if _, err := adapter.DecodeCommand(context.Background(), v1.CommandChannelRegisterCommand{CommandBase: base, Capacity: capacity}); err != nil {
			t.Fatalf("command channel capacity %d: %v", capacity, err)
		}
		if _, err := adapter.DecodeCommand(context.Background(), v1.EventSinkRegisterCommand{CommandBase: base, Capacity: capacity}); err != nil {
			t.Fatalf("event sink capacity %d: %v", capacity, err)
		}
		completion, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: capacity}})
		if err != nil {
			t.Fatalf("completion capacity %d: %v", capacity, err)
		}
		if _, err := adapter.EncodeABICompletion(completion); err != nil {
			t.Fatalf("encode completion capacity %d: %v", capacity, err)
		}
		for _, queueCase := range []struct {
			name  v1.EventName
			queue v1.QueueName
		}{
			{name: v1.EventCommandQueueFull, queue: v1.QueueNameCommand},
			{name: v1.EventEventQueueFull, queue: v1.QueueNameEvent},
		} {
			event, err := adapter.MapEvent(model.Event{
				ID: "event", Name: string(queueCase.name), CreatedAt: now,
				Value: model.QueueCapacityEvent{Queue: string(queueCase.queue), Capacity: capacity},
			})
			if err != nil {
				t.Fatalf("%s capacity %d: %v", queueCase.name, capacity, err)
			}
			if _, err := adapter.EncodeABIEvent(event); err != nil {
				t.Fatalf("encode %s capacity %d: %v", queueCase.name, capacity, err)
			}
		}
	}
}

func TestEveryFrozenCapacitySurfaceRejectsOutsideDomain(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	base := v1.CommandBase{CommandID: "command", SDKSessionID: "session"}
	now := time.Unix(1, 2).UTC()
	for _, capacity := range []uint32{0, maxQueueCapacity + 1, ^uint32(0)} {
		if _, err := adapter.MapConfig(v1.Config{QueueLimit: capacity, PayloadLimit: 1}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("MapConfig capacity %d error = %v", capacity, err)
		}
		if _, err := adapter.DecodeCommand(context.Background(), v1.CommandChannelRegisterCommand{CommandBase: base, Capacity: capacity}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("command channel capacity %d error = %v", capacity, err)
		}
		if _, err := adapter.DecodeCommand(context.Background(), v1.EventSinkRegisterCommand{CommandBase: base, Capacity: capacity}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("event sink capacity %d error = %v", capacity, err)
		}
		if _, err := adapter.MapCompletion(model.Result{CommandID: "command", Value: model.CompletionChannelResult{ChannelID: "channel", MaxInFlight: capacity}}); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("completion capacity %d error = %v", capacity, err)
		}
		for _, queueCase := range []struct {
			name  v1.EventName
			queue v1.QueueName
		}{
			{name: v1.EventCommandQueueFull, queue: v1.QueueNameCommand},
			{name: v1.EventEventQueueFull, queue: v1.QueueNameEvent},
		} {
			if _, err := adapter.MapEvent(model.Event{ID: "event", Name: string(queueCase.name), CreatedAt: now, Value: model.QueueCapacityEvent{Queue: string(queueCase.queue), Capacity: capacity}}); !errors.Is(err, ErrMalformedInput) {
				t.Errorf("%s capacity %d error = %v", queueCase.name, capacity, err)
			}
		}
	}
}
