package cynapsagocore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

type deliveryRuntimeFunc func(context.Context, model.MessageReceivedEvent) error

func (function deliveryRuntimeFunc) DeliverInbound(ctx context.Context, value model.MessageReceivedEvent) error {
	return function(ctx, value)
}

func TestRuntimeDeliverySinkProjectsExactOwnedEvent(t *testing.T) {
	body := []byte("request body")
	headers := []model.Header{{Name: "X-First", Value: "one"}, {Name: "X-First", Value: "two"}}
	inbound := mesh.InboundDelivery{
		MessageID: "message", ConversationID: "conversation", FromAgentID: "sender", MeshID: "mesh",
		Mode: protocol.ModeRequest, RequestHandle: "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Payload: model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: "/agent", Query: "a=1", Headers: headers, Body: body}},
	}
	var captured model.MessageReceivedEvent
	sink := &runtimeDeliverySink{target: deliveryRuntimeFunc(func(_ context.Context, value model.MessageReceivedEvent) error {
		captured = value
		return nil
	})}
	if got := sink.Deliver(context.Background(), inbound); got != mesh.DeliveryAccepted {
		t.Fatalf("Deliver() = %v", got)
	}
	if captured.MessageID != inbound.MessageID || captured.ConversationID != inbound.ConversationID || captured.FromAgentID != inbound.FromAgentID || captured.MeshID != inbound.MeshID || captured.Mode != string(inbound.Mode) || captured.RequestHandle != inbound.RequestHandle {
		t.Fatalf("projection = %#v", captured)
	}
	projected, ok := captured.Payload.Value.(model.HTTPRequestPayload)
	if !ok || projected.Method != "POST" || projected.Path != "/agent" || projected.Query != "a=1" || !bytes.Equal(projected.Body, body) || len(projected.Headers) != 2 || projected.Headers[1].Value != "two" {
		t.Fatalf("payload projection = %#v", captured.Payload.Value)
	}
	body[0] = 'X'
	headers[0].Value = "mutated"
	if string(projected.Body) != "request body" || projected.Headers[0].Value != "one" {
		t.Fatal("runtime payload ownership aliases mesh caller storage")
	}
}

func TestRuntimeDeliverySinkWaitsForExactMandatoryAcceptance(t *testing.T) {
	r := newAdapterRuntime(t, 2)
	sink := newRuntimeDeliverySink(r)
	done := make(chan mesh.DeliveryDisposition, 1)
	go func() { done <- sink.Deliver(context.Background(), adapterInbound("tracked")) }()

	event, err := r.NextEvent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "message.received" {
		t.Fatalf("event name = %q", event.Name)
	}
	projected, ok := event.Value.(model.MessageReceivedEvent)
	if !ok || projected.MessageID != "tracked" {
		t.Fatalf("event value = %#v", event.Value)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: "wrong"}); !errors.Is(err, coreruntime.ErrDeliveryNotPending) {
		t.Fatalf("wrong acceptance = %v", err)
	}
	select {
	case disposition := <-done:
		t.Fatalf("Deliver returned before exact accept: %v", disposition)
	default:
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
		t.Fatal(err)
	}
	if disposition := <-done; disposition != mesh.DeliveryAccepted {
		t.Fatalf("accepted disposition = %v", disposition)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); !errors.Is(err, coreruntime.ErrDeliveryNotPending) {
		t.Fatalf("stale acceptance = %v", err)
	}
	shutdownAdapterRuntime(t, r)
}

func TestRuntimeDeliverySinkCancellationAndShutdownRetireOwnership(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		r := newAdapterRuntime(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan mesh.DeliveryDisposition, 1)
		go func() { done <- newRuntimeDeliverySink(r).Deliver(ctx, adapterInbound("cancel")) }()
		event, err := r.NextEvent(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if disposition := <-done; disposition != mesh.DeliveryUnavailable {
			t.Fatalf("cancel disposition = %v", disposition)
		}
		if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); !errors.Is(err, coreruntime.ErrDeliveryNotPending) {
			t.Fatalf("cancelled acceptance = %v", err)
		}
		shutdownAdapterRuntime(t, r)
	})

	t.Run("runtime shutdown", func(t *testing.T) {
		r := newAdapterRuntime(t, 2)
		done := make(chan mesh.DeliveryDisposition, 1)
		go func() { done <- newRuntimeDeliverySink(r).Deliver(context.Background(), adapterInbound("shutdown")) }()
		if _, err := r.NextEvent(context.Background()); err != nil {
			t.Fatal(err)
		}
		r.BeginShutdown()
		if disposition := <-done; disposition != mesh.DeliveryUnavailable {
			t.Fatalf("shutdown disposition = %v", disposition)
		}
		shutdownAdapterRuntime(t, r)
	})
}

func TestRuntimeDeliverySinkClosedErrorTaxonomy(t *testing.T) {
	private := errors.New("private dependency canary")
	for _, test := range []struct {
		name string
		err  error
		want mesh.DeliveryDisposition
	}{
		{name: "accepted", want: mesh.DeliveryAccepted},
		{name: "cancelled", err: context.Canceled, want: mesh.DeliveryUnavailable},
		{name: "deadline", err: context.DeadlineExceeded, want: mesh.DeliveryUnavailable},
		{name: "queue capacity", err: delivery.ErrQueueFull, want: mesh.DeliveryCapacity},
		{name: "identity capacity", err: delivery.ErrAdmissionExhausted, want: mesh.DeliveryCapacity},
		{name: "runtime closing", err: coreruntime.ErrClosing, want: mesh.DeliveryUnavailable},
		{name: "runtime closed", err: coreruntime.ErrClosed, want: mesh.DeliveryUnavailable},
		{name: "queue closing", err: delivery.ErrClosing, want: mesh.DeliveryUnavailable},
		{name: "queue closed", err: delivery.ErrClosed, want: mesh.DeliveryUnavailable},
		{name: "private rejection", err: private, want: mesh.DeliveryRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			sink := &runtimeDeliverySink{target: deliveryRuntimeFunc(func(context.Context, model.MessageReceivedEvent) error {
				called = true
				return test.err
			})}
			if got := sink.Deliver(context.Background(), adapterInbound("taxonomy")); got != test.want || !called {
				t.Fatalf("Deliver() = %v called=%t, want %v", got, called, test.want)
			}
		})
	}
	invalid := []mesh.InboundDelivery{
		{},
		{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: protocol.ModeResponse, Payload: model.Payload{Value: model.NativePayload{Path: "/"}}},
		{MessageID: "m", ConversationID: "c", FromAgentID: "a", MeshID: "mesh", Mode: protocol.ModeRequest, Payload: model.Payload{Value: model.NativePayload{Path: "/"}}},
	}
	for index, inbound := range invalid {
		if got := (&runtimeDeliverySink{target: deliveryRuntimeFunc(func(context.Context, model.MessageReceivedEvent) error { return nil })}).Deliver(context.Background(), inbound); got != mesh.DeliveryRejected {
			t.Fatalf("invalid %d disposition = %v", index, got)
		}
	}
	if got := newRuntimeDeliverySink(nil).Deliver(context.Background(), adapterInbound("nil-runtime")); got != mesh.DeliveryRejected {
		t.Fatalf("nil runtime disposition = %v", got)
	}
}

func TestRuntimeDeliverySinkRejectsNonCanonicalRequestHandlesBeforeHandoff(t *testing.T) {
	valid := "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, handle := range []string{
		"",
		"not-a-request-handle",
		"reqh_",
		valid + "=",
		"payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+",
		"reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/",
		"reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
	} {
		t.Run(handle, func(t *testing.T) {
			called := false
			sink := &runtimeDeliverySink{target: deliveryRuntimeFunc(func(context.Context, model.MessageReceivedEvent) error {
				called = true
				return nil
			})}
			inbound := mesh.InboundDelivery{
				MessageID: "message", ConversationID: "conversation", FromAgentID: "sender", MeshID: "mesh",
				Mode: protocol.ModeRequest, RequestHandle: handle,
				Payload: model.Payload{Value: model.NativePayload{Path: "/"}},
			}
			if got := sink.Deliver(context.Background(), inbound); got != mesh.DeliveryRejected {
				t.Fatalf("Deliver(%q) = %v", handle, got)
			}
			if called {
				t.Fatal("malformed request handle reached Runtime")
			}
		})
	}
}

func newAdapterRuntime(t *testing.T, capacity int) *coreruntime.Runtime {
	t.Helper()
	gate, err := commandgate.New(capacity)
	if err != nil {
		t.Fatal(err)
	}
	r, err := coreruntime.New(coreruntime.Config{
		RuntimeConfig:  model.RuntimeConfig{QueueLimit: uint32(capacity), PayloadLimit: 1 << 20},
		CleanupTimeout: time.Second,
	}, gate)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func shutdownAdapterRuntime(t *testing.T, r *coreruntime.Runtime) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Shutdown(context.Background()) }()
	for {
		_, err := r.NextEvent(context.Background())
		if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func adapterInbound(id string) mesh.InboundDelivery {
	return mesh.InboundDelivery{
		MessageID: id, ConversationID: "conversation", FromAgentID: "sender", MeshID: "mesh",
		Mode:    protocol.ModeMessage,
		Payload: model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/", Body: []byte(id)}},
	}
}
