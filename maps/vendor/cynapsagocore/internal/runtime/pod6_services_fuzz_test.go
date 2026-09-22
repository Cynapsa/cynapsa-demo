package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func FuzzQAPod6DeliveryAcceptanceNeverConsumesWrongOrRepeatedID(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{1, 1, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, actions []byte) {
		gate, err := commandgate.New(1)
		if err != nil {
			t.Fatal(err)
		}
		r, err := New(testConfig(1), gate)
		if err != nil {
			t.Fatal(err)
		}
		event := qaPod2MessageEvent("fuzz-event", 1)
		if err = r.PublishEvent(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		result, err := r.PollDelivery(context.Background(), model.EmptyArgs{})
		if err != nil || result.Event.ID != event.ID {
			t.Fatalf("PollDelivery = %+v, %v", result, err)
		}
		accepted := false
		if len(actions) > 64 {
			actions = actions[:64]
		}
		for _, action := range actions {
			id := "wrong"
			if action&1 == 0 {
				id = event.ID
			}
			err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: id})
			if id == event.ID && !accepted {
				if err != nil {
					t.Fatalf("first exact acceptance = %v", err)
				}
				accepted = true
				continue
			}
			if !errors.Is(err, ErrDeliveryNotPending) {
				t.Fatalf("accept(%q, accepted=%v) = %v", id, accepted, err)
			}
		}
		if !accepted {
			if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
				t.Fatal(err)
			}
		}
		qaPod2ShutdownAndDrain(t, r, nil)
	})
}
