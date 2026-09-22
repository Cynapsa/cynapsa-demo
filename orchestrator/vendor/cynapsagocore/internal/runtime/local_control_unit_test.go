package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestUpdateConfigIsAtomicAndRejectsImmutableFields(t *testing.T) {
	runtime := testRuntime(t, 4, Dependencies{})
	runtime.config.Connectivity.BootstrapData = []byte{1}
	original := runtime.ConfigSnapshot()
	original.Connectivity.BootstrapData[0] ^= 0xff
	if runtime.ConfigSnapshot().Connectivity.BootstrapData[0] == original.Connectivity.BootstrapData[0] {
		t.Fatal("ConfigSnapshot exposed Runtime-owned bootstrap bytes")
	}

	commandTimeout := 2 * time.Second
	rpcTimeout := 3 * time.Second
	updated, err := runtime.UpdateConfig(context.Background(), model.ConfigUpdateArgs{CommandTimeout: &commandTimeout, RPCTimeout: &rpcTimeout})
	if err != nil {
		t.Fatalf("UpdateConfig() error = %v", err)
	}
	if updated.CommandTimeout != commandTimeout || updated.RPCTimeout != rpcTimeout {
		t.Fatalf("UpdateConfig() = %+v", updated)
	}

	queue := updated.QueueLimit
	rejectedTimeout := 7 * time.Second
	if _, err := runtime.UpdateConfig(context.Background(), model.ConfigUpdateArgs{CommandTimeout: &rejectedTimeout, QueueLimit: &queue}); !errors.Is(err, ErrImmutableConfig) {
		t.Fatalf("UpdateConfig(immutable) error = %v, want ErrImmutableConfig", err)
	}
	if got := runtime.ConfigSnapshot().CommandTimeout; got != commandTimeout {
		t.Fatalf("partial mutable update committed: %v", got)
	}

	for _, invalid := range []time.Duration{0, -time.Second} {
		if _, err := runtime.UpdateConfig(context.Background(), model.ConfigUpdateArgs{RPCTimeout: &invalid}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("UpdateConfig(%v) error = %v, want ErrInvalidConfig", invalid, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.UpdateConfig(ctx, model.ConfigUpdateArgs{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateConfig(cancelled) error = %v", err)
	}
}

func TestLocalControlRegistrationsAreSingleBoundedAndClosed(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	for _, capacity := range []uint32{0, 3} {
		if _, err := runtime.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: capacity}); !errors.Is(err, ErrControlCapacity) {
			t.Fatalf("RegisterCompletionChannel(%d) error = %v", capacity, err)
		}
		if _, err := runtime.RegisterEventSink(context.Background(), model.EventSinkRegisterArgs{Capacity: capacity}); !errors.Is(err, ErrControlCapacity) {
			t.Fatalf("RegisterEventSink(%d) error = %v", capacity, err)
		}
	}

	channel, err := runtime.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if channel.ChannelID == "" || channel.MaxInFlight != 2 {
		t.Fatalf("completion registration = %+v", channel)
	}
	if _, err := runtime.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 1}); !errors.Is(err, ErrControlRegistered) {
		t.Fatalf("duplicate completion registration error = %v", err)
	}
	if err := runtime.ClearCompletionChannel(context.Background(), model.ChannelIDArgs{ChannelID: "unknown"}); !errors.Is(err, ErrControlNotFound) {
		t.Fatalf("ClearCompletionChannel(unknown) error = %v", err)
	}
	if err := runtime.ClearCompletionChannel(context.Background(), model.ChannelIDArgs{ChannelID: channel.ChannelID}); err != nil {
		t.Fatal(err)
	}
	replacement, err := runtime.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 1})
	if err != nil || replacement.ChannelID == channel.ChannelID {
		t.Fatalf("replacement completion registration = %+v, %v", replacement, err)
	}

	sink, err := runtime.RegisterEventSink(context.Background(), model.EventSinkRegisterArgs{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.BindEventSink(context.Background(), model.EventSinkIDArgs{SinkID: "unknown"}); !errors.Is(err, ErrControlNotFound) {
		t.Fatalf("BindEventSink(unknown) error = %v", err)
	}
	if err := runtime.BindEventSink(context.Background(), model.EventSinkIDArgs{SinkID: sink.SinkID}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ClearEventSink(context.Background(), model.EventSinkIDArgs{SinkID: sink.SinkID}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.BindEventSink(context.Background(), model.EventSinkIDArgs{SinkID: sink.SinkID}); !errors.Is(err, ErrControlNotFound) {
		t.Fatalf("BindEventSink(cleared) error = %v", err)
	}
}

func TestConcurrentControlRegistrationHasOneWinner(t *testing.T) {
	runtime := testRuntime(t, 64, Dependencies{})
	const contenders = 64
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := runtime.RegisterEventSink(context.Background(), model.EventSinkRegisterArgs{Capacity: 1})
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	winners := 0
	for err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrControlRegistered):
		default:
			t.Fatalf("unexpected registration error = %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("registration winners = %d, want 1", winners)
	}
}

func TestDiagnosticLogSubscriptionGatesOnlyDiagnosticLogs(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	logEvent := model.Event{
		ID:        "diagnostic-1",
		Name:      "diagnostics.log",
		CreatedAt: time.Unix(1, 0),
		Value:     model.DiagnosticLogEvent{Level: "info", Code: "ready", DiagnosticID: "diagnostic"},
	}
	if err := runtime.PublishEvent(context.Background(), logEvent); !errors.Is(err, ErrDiagnosticLogsOff) {
		t.Fatalf("PublishEvent(unsubscribed diagnostic) error = %v", err)
	}
	if err := runtime.SetDiagnosticLogSubscription(context.Background(), model.DiagnosticsLogsArgs{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PublishEvent(context.Background(), logEvent); err != nil {
		t.Fatalf("PublishEvent(subscribed diagnostic) error = %v", err)
	}
	if event, err := runtime.NextEvent(context.Background()); err != nil || event.ID != logEvent.ID {
		t.Fatalf("NextEvent() = %+v, %v", event, err)
	}
	if err := runtime.SetDiagnosticLogSubscription(context.Background(), model.DiagnosticsLogsArgs{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	ordinary := model.Event{ID: "ordinary", Name: "core.error", CreatedAt: time.Unix(2, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: "bounded"}}}
	if err := runtime.PublishEvent(context.Background(), ordinary); err != nil {
		t.Fatalf("PublishEvent(ordinary) error = %v", err)
	}
}

func TestPromptDeliveryControlsRetainPausedQueue(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	if _, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{}); !errors.Is(err, delivery.ErrQueueEmpty) {
		t.Fatalf("PollDelivery(empty) error = %v, want ErrQueueEmpty", err)
	}
	event := model.Event{ID: "delivery", Name: "message.received", CreatedAt: time.Unix(3, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PauseDelivery(context.Background(), model.EmptyArgs{}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{}); !errors.Is(err, ErrDeliveryPaused) {
		t.Fatalf("PollDelivery(paused) error = %v", err)
	}
	status, err := runtime.DeliveryQueueStatus(context.Background(), model.EmptyArgs{})
	if err != nil || status.Queued != 1 || !status.Paused {
		t.Fatalf("DeliveryQueueStatus(paused) = %+v, %v", status, err)
	}
	if err := runtime.ResumeDelivery(context.Background(), model.EmptyArgs{}); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || result.Event.ID != event.ID {
		t.Fatalf("PollDelivery() = %+v, %v", result, err)
	}
	if _, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{}); !errors.Is(err, ErrDeliveryAcceptPending) {
		t.Fatalf("PollDelivery(unaccepted) error = %v", err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: "wrong"}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("AcceptDelivery(wrong) error = %v", err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
		t.Fatalf("AcceptDelivery() error = %v", err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("AcceptDelivery(repeated) error = %v", err)
	}
	status, err = runtime.DeliveryQueueStatus(context.Background(), model.EmptyArgs{})
	if err != nil || status.Queued != 0 || status.Paused {
		t.Fatalf("DeliveryQueueStatus(resumed) = %+v, %v", status, err)
	}
}

func TestCommandAndDirectDeliveryShareQueueWithoutDuplicates(t *testing.T) {
	runtime := testRuntime(t, 3, Dependencies{})
	for index, id := range []string{"one", "two", "three"} {
		event := model.Event{ID: id, Name: "message.received", CreatedAt: time.Unix(int64(index+1), 0), Value: model.MessageReceivedEvent{MessageID: "message-" + id}}
		if err := runtime.PublishEvent(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	first, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || first.Event.ID != "one" {
		t.Fatalf("PollDelivery(first) = %+v, %v", first, err)
	}
	second, err := runtime.NextEvent(context.Background())
	if err != nil || second.ID != "two" {
		t.Fatalf("NextEvent(second) = %+v, %v", second, err)
	}
	if _, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{}); !errors.Is(err, ErrDeliveryAcceptPending) {
		t.Fatalf("PollDelivery(pending) error = %v", err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: first.Event.ID}); err != nil {
		t.Fatal(err)
	}
	third, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || third.Event.ID != "three" {
		t.Fatalf("PollDelivery(third) = %+v, %v", third, err)
	}
	seen := map[string]bool{first.Event.ID: true, second.ID: true, third.Event.ID: true}
	if len(seen) != 3 {
		t.Fatalf("consumers observed duplicate IDs: %+v", seen)
	}
}

func TestConcurrentDeliveryAcceptHasOneWinner(t *testing.T) {
	runtime := testRuntime(t, 1, Dependencies{})
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{}); err != nil {
		t.Fatal(err)
	}
	const contenders = 64
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs <- runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID})
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	winners := 0
	for err := range errs {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrDeliveryNotPending) {
			t.Fatalf("unexpected AcceptDelivery error = %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("AcceptDelivery winners = %d, want 1", winners)
	}
}

func TestDeliveryCommandCompletionLossRollsBackWithoutWedge(t *testing.T) {
	runtime := testRuntime(t, 2, Dependencies{})
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	command := model.Command{ID: "delivery-command", Name: "delivery.next", Args: model.EmptyArgs{}}
	value, err := runtime.PollDelivery(context.WithValue(context.Background(), deliveryCommandContextKey{}, command.ID), model.EmptyArgs{})
	result := model.Result{CommandID: command.ID, Value: value}
	if err != nil {
		t.Fatalf("PollDelivery() = %+v, %v", result, err)
	}
	runtime.commandCompletionDisposition(command, result, false)
	if runtime.outstandingDelivery != "" {
		t.Fatalf("outstanding delivery = %q after rollback", runtime.outstandingDelivery)
	}
	redelivered, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || redelivered.Event.ID != event.ID {
		t.Fatalf("PollDelivery() after rollback = %+v, %v", redelivered, err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
		t.Fatalf("AcceptDelivery() after redelivery error = %v", err)
	}
}

func TestDeliveryCommandCompletionCommitCreatesAcceptanceOnlyAfterWin(t *testing.T) {
	runtime := testRuntime(t, 1, Dependencies{})
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	command := model.Command{ID: "delivery-command", Name: "delivery.next", Args: model.EmptyArgs{}}
	value, err := runtime.PollDelivery(context.WithValue(context.Background(), deliveryCommandContextKey{}, command.ID), model.EmptyArgs{})
	result := model.Result{CommandID: command.ID, Value: value}
	if err != nil {
		t.Fatalf("PollDelivery() = %+v, %v", result, err)
	}
	if runtime.outstandingDelivery != "" {
		t.Fatalf("outstanding delivery published before completion win: %q", runtime.outstandingDelivery)
	}
	runtime.commandCompletionDisposition(command, result, true)
	if runtime.outstandingDelivery != event.ID {
		t.Fatalf("outstanding delivery = %q, want %q", runtime.outstandingDelivery, event.ID)
	}
}

func TestDeliveryNextCancellationWinnerRollsBackThroughRuntimeWorker(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	var runtime *Runtime
	reserved := make(chan struct{})
	release := make(chan struct{})
	runtime, err = NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"delivery.next": func(ctx context.Context, _ Services, command model.Command) (model.Result, error) {
			value, pollErr := runtime.PollDelivery(ctx, model.EmptyArgs{})
			close(reserved)
			<-release
			if pollErr != nil {
				return runtimeFailure(command.ID, "delivery_unavailable", pollErr), nil
			}
			return model.Result{CommandID: command.ID, Value: value}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	admission, err := gate.Submit(context.Background(), model.Command{ID: "delivery-command", Name: "delivery.next", Args: model.EmptyArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	<-reserved
	if err := gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatal(err)
	}
	close(release)
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "delivery-command" || completion.Err == nil {
		t.Fatalf("cancel completion = %+v, %v", completion, err)
	}
	redelivered, err := runtime.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || redelivered.Event.ID != event.ID {
		t.Fatalf("PollDelivery() after cancel winner = %+v, %v", redelivered, err)
	}
	if err := runtime.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
		t.Fatal(err)
	}
	forceShutdown(t, runtime)
}

func TestDeliveryNextShutdownWinnerRestoresEventAheadOfClosing(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	var runtime *Runtime
	reserved := make(chan struct{})
	release := make(chan struct{})
	runtime, err = NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"delivery.next": func(ctx context.Context, _ Services, command model.Command) (model.Result, error) {
			value, pollErr := runtime.PollDelivery(ctx, model.EmptyArgs{})
			close(reserved)
			<-release
			if pollErr != nil {
				return runtimeFailure(command.ID, "delivery_unavailable", pollErr), nil
			}
			return model.Result{CommandID: command.ID, Value: value}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message"}}
	if err := runtime.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Submit(context.Background(), model.Command{ID: "delivery-command", Name: "delivery.next", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	<-reserved
	runtime.BeginShutdown()
	close(release)
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "delivery-command" || completion.Err == nil {
		t.Fatalf("shutdown completion = %+v, %v", completion, err)
	}
	restored, err := runtime.NextEvent(context.Background())
	if err != nil || restored.ID != event.ID {
		t.Fatalf("NextEvent() after shutdown winner = %+v, %v", restored, err)
	}
	closing, err := runtime.NextEvent(context.Background())
	if err != nil {
		t.Fatalf("NextEvent(closing) error = %v", err)
	}
	state, ok := closing.Value.(model.SessionStateChangedEvent)
	if !ok || state.Current != model.LifecycleClosing {
		t.Fatalf("closing event = %+v", closing)
	}
	closed, err := runtime.NextEvent(context.Background())
	if err != nil {
		t.Fatalf("NextEvent(closed) error = %v", err)
	}
	state, ok = closed.Value.(model.SessionStateChangedEvent)
	if !ok || state.Current != model.LifecycleClosed {
		t.Fatalf("closed event = %+v", closed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}
