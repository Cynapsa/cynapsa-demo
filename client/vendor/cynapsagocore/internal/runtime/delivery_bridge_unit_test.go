package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestDeliverInboundWaitsForExactMandatoryAcceptance(t *testing.T) {
	r := testRuntime(t, 2, Dependencies{})
	done := startInboundDelivery(t, r, context.Background(), inboundValue("one"))
	waitForDeliveryDepth(t, r, 1)

	result, err := r.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Event.Name != "message.received" {
		t.Fatalf("event name = %q", result.Event.Name)
	}
	if got := result.Event.Value.(model.MessageReceivedEvent); got.MessageID != "one" {
		t.Fatalf("event value = %+v", got)
	}
	assertDeliveryStillWaiting(t, done)
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: "wrong"}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("wrong AcceptDelivery() = %v", err)
	}
	assertDeliveryStillWaiting(t, done)
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: result.Event.ID}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatalf("DeliverInbound() = %v", err)
	}
}

func TestCallbackStyleEventReservationWaitsForPriorTrackedAcceptance(t *testing.T) {
	r := testRuntime(t, 2, Dependencies{})
	firstDone := startInboundDelivery(t, r, context.Background(), inboundValue("first"))
	waitForDeliveryDepth(t, r, 1)
	secondDone := startInboundDelivery(t, r, context.Background(), inboundValue("second"))
	waitForDeliveryDepth(t, r, 2)

	first, firstLease, err := r.ReserveEvent(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.CommitEvent(firstLease); err != nil {
		t.Fatal(err)
	}

	type reserved struct {
		event model.Event
		lease *EventLease
		err   error
	}
	secondReserved := make(chan reserved, 1)
	go func() {
		event, lease, reserveErr := r.ReserveEvent(context.Background(), true)
		secondReserved <- reserved{event: event, lease: lease, err: reserveErr}
	}()
	select {
	case got := <-secondReserved:
		if got.lease != nil {
			_ = r.RollbackEvent(got.lease)
		}
		t.Fatalf("second tracked reservation passed pending acceptance: %+v, %v", got.event, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: first.ID}); err != nil {
		t.Fatal(err)
	}
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}

	var second reserved
	select {
	case second = <-secondReserved:
	case <-time.After(time.Second):
		t.Fatal("second tracked reservation did not resume after exact acceptance")
	}
	if second.err != nil {
		t.Fatal(second.err)
	}
	if got := second.event.Value.(model.MessageReceivedEvent).MessageID; got != "second" {
		_ = r.RollbackEvent(second.lease)
		t.Fatalf("second tracked message = %q", got)
	}
	if err = r.CommitEvent(second.lease); err != nil {
		t.Fatal(err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: second.event.ID}); err != nil {
		t.Fatal(err)
	}
	if err = <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestNextEventDeadlineWhileTransactionalLeaseOwnsConsumer(t *testing.T) {
	r := testRuntime(t, 2, Dependencies{})
	if err := r.PublishEvent(context.Background(), bridgeOrdinaryEvent("held")); err != nil {
		t.Fatal(err)
	}
	command := model.Command{ID: "held-command", Name: "delivery.next", Args: model.EmptyArgs{}}
	value, err := r.PollDelivery(context.WithValue(context.Background(), deliveryCommandContextKey{}, command.ID), model.EmptyArgs{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err = r.NextEvent(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("NextEvent() = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("NextEvent deadline delayed by consumer ownership: %v", elapsed)
	}
	r.commandCompletionDisposition(command, model.Result{CommandID: command.ID, Value: value}, false)
}

func TestDeliverInboundPrivateAdmissionCannotAliasPublicEventID(t *testing.T) {
	r := testRuntime(t, 3, Dependencies{})
	if err := r.PublishEvent(context.Background(), bridgeOrdinaryEvent("event-1")); err != nil {
		t.Fatal(err)
	}
	done := startInboundDelivery(t, r, context.Background(), inboundValue("tracked"))
	waitForDeliveryDepth(t, r, 2)
	ordinary, err := r.NextEvent(context.Background())
	if err != nil || ordinary.ID != "event-1" {
		t.Fatalf("ordinary NextEvent() = %+v, %v", ordinary, err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: ordinary.ID}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("ordinary AcceptDelivery() = %v", err)
	}
	assertDeliveryStillWaiting(t, done)
	tracked, err := r.NextEvent(context.Background())
	if err != nil || tracked.ID != "event-1" || tracked.Name != "message.received" {
		t.Fatalf("tracked NextEvent() = %+v, %v", tracked, err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: tracked.ID}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNextEventNormalizesTerminalLeaseInvalidation(t *testing.T) {
	r := testRuntime(t, 1, Dependencies{})
	if err := r.PublishEvent(context.Background(), bridgeOrdinaryEvent("terminal")); err != nil {
		t.Fatal(err)
	}
	r.controlMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := r.NextEvent(context.Background())
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, lease, err := r.events.ReserveNext(context.Background())
		if errors.Is(err, delivery.ErrConsumerBusy) {
			break
		}
		if err == nil {
			_ = r.events.Rollback(lease)
		}
		if time.Now().After(deadline) {
			r.controlMu.Unlock()
			t.Fatal("NextEvent did not reserve its lease")
		}
		time.Sleep(time.Millisecond)
	}
	r.events.BeginShutdown()
	if err := r.events.Clear(context.Background()); err != nil {
		r.controlMu.Unlock()
		t.Fatal(err)
	}
	r.controlMu.Unlock()
	if err := <-done; !errors.Is(err, delivery.ErrClosed) {
		t.Fatalf("NextEvent terminal lease loss = %v", err)
	}
}

func TestDeliverInboundDirectPullUsesAcceptanceOnlyForTrackedEvents(t *testing.T) {
	r := testRuntime(t, 3, Dependencies{})
	done := startInboundDelivery(t, r, context.Background(), inboundValue("tracked"))
	waitForDeliveryDepth(t, r, 1)

	tracked, err := r.NextEvent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertDeliveryStillWaiting(t, done)
	if err = r.PublishEvent(context.Background(), bridgeOrdinaryEvent("ordinary-one")); err != nil {
		t.Fatal(err)
	}
	ordinary, err := r.NextEvent(context.Background())
	if err != nil || ordinary.ID != "ordinary-one" {
		t.Fatalf("NextEvent(non-ack while tracked pending) = %+v, %v", ordinary, err)
	}
	assertDeliveryStillWaiting(t, done)
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: tracked.ID}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = r.PublishEvent(context.Background(), bridgeOrdinaryEvent("ordinary-two")); err != nil {
		t.Fatal(err)
	}
	ordinary, err = r.NextEvent(context.Background())
	if err != nil || ordinary.ID != "ordinary-two" {
		t.Fatalf("second non-ack event = %+v, %v", ordinary, err)
	}
}

func TestDeliverInboundCancellationRetiresQueuedAndActiveLease(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		r := testRuntime(t, 1, Dependencies{})
		ctx, cancel := context.WithCancel(context.Background())
		done := startInboundDelivery(t, r, ctx, inboundValue("cancel-queued"))
		waitForDeliveryDepth(t, r, 1)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("DeliverInbound() = %v", err)
		}
		if stats := r.events.Stats(); stats.Depth != 0 {
			t.Fatalf("queue after cancellation = %+v", stats)
		}
		if err := r.PublishEvent(context.Background(), bridgeOrdinaryEvent("recovery")); err != nil {
			t.Fatal(err)
		}
		if got, err := r.NextEvent(context.Background()); err != nil || got.ID != "recovery" {
			t.Fatalf("NextEvent() after cancellation = %+v, %v", got, err)
		}
	})

	t.Run("active command lease", func(t *testing.T) {
		r := testRuntime(t, 2, Dependencies{})
		ctx, cancel := context.WithCancel(context.Background())
		done := startInboundDelivery(t, r, ctx, inboundValue("cancel-leased"))
		waitForDeliveryDepth(t, r, 1)
		command := model.Command{ID: "delivery-command", Name: "delivery.next", Args: model.EmptyArgs{}}
		value, err := r.PollDelivery(context.WithValue(context.Background(), deliveryCommandContextKey{}, command.ID), model.EmptyArgs{})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if err = <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("DeliverInbound() = %v", err)
		}
		r.commandCompletionDisposition(command, model.Result{CommandID: command.ID, Value: value}, true)
		if r.outstandingDelivery != "" {
			t.Fatalf("late completion created outstanding delivery %q", r.outstandingDelivery)
		}
		if stats := r.events.Stats(); stats.Depth != 0 {
			t.Fatalf("queue after late completion = %+v", stats)
		}
	})
}

func TestDeliverInboundCapacityFIFOAndShutdownRetirement(t *testing.T) {
	t.Run("capacity and FIFO", func(t *testing.T) {
		r := testRuntime(t, 2, Dependencies{})
		first := startInboundDelivery(t, r, context.Background(), inboundValue("first"))
		waitForDeliveryDepth(t, r, 1)
		second := startInboundDelivery(t, r, context.Background(), inboundValue("second"))
		waitForDeliveryDepth(t, r, 2)
		if err := r.DeliverInbound(context.Background(), inboundValue("overflow")); !errors.Is(err, delivery.ErrQueueFull) {
			t.Fatalf("overflow DeliverInbound() = %v", err)
		}
		for index, expected := range []struct {
			message string
			done    <-chan error
		}{{"first", first}, {"second", second}} {
			result, err := r.PollDelivery(context.Background(), model.EmptyArgs{})
			if err != nil {
				t.Fatalf("PollDelivery(%d) = %v", index, err)
			}
			got := result.Event.Value.(model.MessageReceivedEvent)
			if got.MessageID != expected.message {
				t.Fatalf("PollDelivery(%d) message = %q", index, got.MessageID)
			}
			if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: result.Event.ID}); err != nil {
				t.Fatal(err)
			}
			if err = <-expected.done; err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		r := testRuntime(t, 2, Dependencies{})
		done := startInboundDelivery(t, r, context.Background(), inboundValue("shutdown"))
		waitForDeliveryDepth(t, r, 1)
		r.BeginShutdown()
		if err := <-done; !errors.Is(err, ErrClosing) {
			t.Fatalf("DeliverInbound() during shutdown = %v", err)
		}
		for {
			event, err := r.NextEvent(context.Background())
			if errors.Is(err, delivery.ErrClosing) || errors.Is(err, delivery.ErrClosed) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if event.Name == "message.received" {
				t.Fatalf("retired inbound event escaped during shutdown: %+v", event)
			}
		}
		if err := r.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("shutdown with active command lease", func(t *testing.T) {
		r := testRuntime(t, 2, Dependencies{})
		done := startInboundDelivery(t, r, context.Background(), inboundValue("shutdown-leased"))
		waitForDeliveryDepth(t, r, 1)
		command := model.Command{ID: "shutdown-command", Name: "delivery.next", Args: model.EmptyArgs{}}
		value, err := r.PollDelivery(context.WithValue(context.Background(), deliveryCommandContextKey{}, command.ID), model.EmptyArgs{})
		if err != nil {
			t.Fatal(err)
		}
		r.BeginShutdown()
		if err = <-done; !errors.Is(err, ErrClosing) {
			t.Fatalf("DeliverInbound() during leased shutdown = %v", err)
		}
		r.commandCompletionDisposition(command, model.Result{CommandID: command.ID, Value: value}, true)
		for {
			event, nextErr := r.NextEvent(context.Background())
			if errors.Is(nextErr, delivery.ErrClosing) || errors.Is(nextErr, delivery.ErrClosed) {
				break
			}
			if nextErr != nil {
				t.Fatal(nextErr)
			}
			if event.Name == "message.received" {
				t.Fatalf("retired leased event escaped during shutdown: %+v", event)
			}
		}
		if err = r.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDeliverInboundAcceptanceCancellationRaceHasOneWinner(t *testing.T) {
	r := testRuntime(t, 1, Dependencies{})
	for iteration := range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		done := startInboundDelivery(t, r, ctx, inboundValue(fmt.Sprintf("race-%d", iteration)))
		waitForDeliveryDepth(t, r, 1)
		event, err := r.NextEvent(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		accepted := make(chan error, 1)
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			cancel()
		}()
		go func() {
			defer wait.Done()
			<-start
			accepted <- r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID})
		}()
		close(start)
		wait.Wait()
		acceptErr := <-accepted
		deliveryErr := <-done
		switch {
		case acceptErr == nil && deliveryErr == nil:
		case errors.Is(acceptErr, ErrDeliveryNotPending) && errors.Is(deliveryErr, context.Canceled):
		default:
			t.Fatalf("iteration %d outcomes = accept %v, delivery %v", iteration, acceptErr, deliveryErr)
		}
		if r.outstandingDelivery != "" || r.events.Stats().Depth != 0 {
			t.Fatalf("iteration %d retained state: outstanding=%q stats=%+v", iteration, r.outstandingDelivery, r.events.Stats())
		}
	}
}

func startInboundDelivery(t *testing.T, r *Runtime, ctx context.Context, value model.MessageReceivedEvent) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.DeliverInbound(ctx, value) }()
	return done
}

func waitForDeliveryDepth(t *testing.T, r *Runtime, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for r.events.Stats().Depth != want {
		if time.Now().After(deadline) {
			t.Fatalf("delivery depth = %d, want %d", r.events.Stats().Depth, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertDeliveryStillWaiting(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("DeliverInbound returned before acceptance: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
}

func inboundValue(id string) model.MessageReceivedEvent {
	return model.MessageReceivedEvent{
		MessageID:      id,
		ConversationID: "conversation",
		FromAgentID:    "sender",
		MeshID:         "mesh",
		Mode:           "msg",
		Payload:        model.Payload{Value: model.NativePayload{ContentType: "application/octet-stream", Path: "/", Body: []byte(id)}},
	}
}

func bridgeOrdinaryEvent(id string) model.Event {
	return model.Event{ID: id, Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "test"}}}
}
