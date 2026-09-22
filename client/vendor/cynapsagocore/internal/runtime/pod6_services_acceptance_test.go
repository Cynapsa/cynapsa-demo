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

func TestQAPod6TerminalLogoutCompletionPrecedesShutdownAndRejectsLateAdmission(t *testing.T) {
	gate, err := commandgate.New(3)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	r, err := NewWithDependencies(testConfig(3), gate, Dependencies{Handlers: map[string]Handler{
		"auth.logout": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			close(entered)
			<-release
			return model.Result{CommandID: command.ID, Value: model.EmptyResult{}}, nil
		},
		"core.status": func(_ context.Context, _ Services, command model.Command) (model.Result, error) {
			return model.Result{CommandID: command.ID, Value: model.StatusResult{}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "logout", Name: "auth.logout", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err = gate.Submit(context.Background(), model.Command{ID: "queued", Name: "core.status", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	close(release)

	first, err := gate.NextCompletion(context.Background())
	if err != nil || first.CommandID != "logout" || first.Err != nil {
		t.Fatalf("first completion = %+v, %v", first, err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "late", Name: "core.status", Args: model.EmptyArgs{}}); !errors.Is(err, commandgate.ErrGateClosing) && !errors.Is(err, commandgate.ErrGateClosed) {
		t.Fatalf("late admission error = %v", err)
	}
	second, err := gate.NextCompletion(context.Background())
	if err != nil || second.CommandID != "queued" || second.Err == nil {
		t.Fatalf("shutdown completion = %+v, %v", second, err)
	}

	qaPod2ShutdownAndDrain(t, r, nil)
	if got := r.Status().Lifecycle; got != model.LifecycleClosed {
		t.Fatalf("lifecycle = %q", got)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "after-closed", Name: "core.status", Args: model.EmptyArgs{}}); !errors.Is(err, commandgate.ErrGateClosed) {
		t.Fatalf("post-close admission error = %v", err)
	}
}

func TestQAPod6DeliveryAcceptIsMandatoryExactAndSingleUse(t *testing.T) {
	gate, err := commandgate.New(4)
	if err != nil {
		t.Fatal(err)
	}
	var r *Runtime
	r, err = NewWithDependencies(testConfig(4), gate, Dependencies{Handlers: map[string]Handler{
		"delivery.next": func(ctx context.Context, _ Services, command model.Command) (model.Result, error) {
			value, pollErr := r.PollDelivery(ctx, model.EmptyArgs{})
			if pollErr != nil {
				return runtimeFailure(command.ID, "delivery_unavailable", pollErr), nil
			}
			return model.Result{CommandID: command.ID, Value: value}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{"event-one", "event-two"} {
		if err = r.PublishEvent(context.Background(), qaPod2MessageEvent(id, int64(index+1))); err != nil {
			t.Fatal(err)
		}
	}
	first := qaPod2CommandDelivery(t, gate, "next-one")
	if first.ID != "event-one" {
		t.Fatalf("first event = %q", first.ID)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "blocked-next", Name: "delivery.next", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	blocked, err := gate.NextCompletion(context.Background())
	if err != nil || blocked.Err == nil || !errors.Is(blocked.Err.Cause, ErrDeliveryAcceptPending) {
		t.Fatalf("unaccepted next = %+v, %v", blocked, err)
	}
	for _, stale := range []string{"", "wrong", "event-two"} {
		if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: stale}); !errors.Is(err, ErrDeliveryNotPending) {
			t.Fatalf("AcceptDelivery(%q) = %v", stale, err)
		}
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: first.ID}); err != nil {
		t.Fatal(err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: first.ID}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("repeated acceptance = %v", err)
	}
	second := qaPod2CommandDelivery(t, gate, "next-two")
	if second.ID != "event-two" {
		t.Fatalf("second event = %q", second.ID)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: first.ID}); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("stale acceptance changed current lease: %v", err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: second.ID}); err != nil {
		t.Fatal(err)
	}
	qaPod2ShutdownAndDrain(t, r, nil)
}

func TestQAPod6DeliveryLeaseRollsBackWhenCancellationWins(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	reserved := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var r *Runtime
	r, err = NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"delivery.next": func(ctx context.Context, _ Services, command model.Command) (model.Result, error) {
			value, pollErr := r.PollDelivery(ctx, model.EmptyArgs{})
			once.Do(func() { close(reserved) })
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
	if err = r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := qaPod2MessageEvent("cancelled-lease", 1)
	if err = r.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	admission, err := gate.Submit(context.Background(), model.Command{ID: "next", Name: "delivery.next", Args: model.EmptyArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	<-reserved
	if err = gate.Cancel(admission.CommandHandle); err != nil {
		t.Fatal(err)
	}
	close(release)
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.Err == nil {
		t.Fatalf("cancel completion = %+v, %v", completion, err)
	}
	redelivered, err := r.PollDelivery(context.Background(), model.EmptyArgs{})
	if err != nil || redelivered.Event.ID != event.ID {
		t.Fatalf("redelivery = %+v, %v", redelivered, err)
	}
	if err = r.AcceptDelivery(context.Background(), model.DeliveryAcceptArgs{EventID: event.ID}); err != nil {
		t.Fatal(err)
	}
	qaPod2ShutdownAndDrain(t, r, nil)
}

func TestQAPod6DeliveryLeaseRollsBackAheadOfShutdownEvents(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	reserved := make(chan struct{})
	release := make(chan struct{})
	var r *Runtime
	r, err = NewWithDependencies(testConfig(2), gate, Dependencies{Handlers: map[string]Handler{
		"delivery.next": func(ctx context.Context, _ Services, command model.Command) (model.Result, error) {
			value, pollErr := r.PollDelivery(ctx, model.EmptyArgs{})
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
	if err = r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := qaPod2MessageEvent("shutdown-lease", 1)
	if err = r.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Submit(context.Background(), model.Command{ID: "next", Name: "delivery.next", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	<-reserved
	r.BeginShutdown()
	close(release)
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.Err == nil {
		t.Fatalf("shutdown completion = %+v, %v", completion, err)
	}
	qaPod2ShutdownAndDrain(t, r, []string{event.ID})
}

func TestQAPod6ConfigAndLogicalControlsAreBoundedAtomicAndLifecycleChecked(t *testing.T) {
	r := testRuntime(t, 4, Dependencies{})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	original := r.ConfigSnapshot()
	commandTimeout, rpcTimeout := 2*time.Second, 3*time.Second
	updated, err := r.UpdateConfig(context.Background(), model.ConfigUpdateArgs{CommandTimeout: &commandTimeout, RPCTimeout: &rpcTimeout})
	if err != nil || updated.CommandTimeout != commandTimeout || updated.RPCTimeout != rpcTimeout {
		t.Fatalf("UpdateConfig = %+v, %v", updated, err)
	}
	queue := original.QueueLimit
	rejectedTimeout := 9 * time.Second
	if _, err = r.UpdateConfig(context.Background(), model.ConfigUpdateArgs{QueueLimit: &queue, CommandTimeout: &rejectedTimeout}); !errors.Is(err, ErrImmutableConfig) {
		t.Fatalf("immutable update = %v", err)
	}
	if got := r.ConfigSnapshot().CommandTimeout; got != commandTimeout {
		t.Fatalf("partial update committed: %v", got)
	}

	channel, err := r.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 4})
	if err != nil || channel.ChannelID == "" || channel.MaxInFlight != 4 {
		t.Fatalf("channel = %+v, %v", channel, err)
	}
	if _, err = r.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 1}); !errors.Is(err, ErrControlRegistered) {
		t.Fatalf("duplicate channel = %v", err)
	}
	if err = r.ClearCompletionChannel(context.Background(), model.ChannelIDArgs{ChannelID: "stale"}); !errors.Is(err, ErrControlNotFound) {
		t.Fatalf("stale channel clear = %v", err)
	}
	if err = r.ClearCompletionChannel(context.Background(), model.ChannelIDArgs{ChannelID: channel.ChannelID}); err != nil {
		t.Fatal(err)
	}
	replacement, err := r.RegisterCompletionChannel(context.Background(), model.ChannelRegisterArgs{Capacity: 1})
	if err != nil || replacement.ChannelID == channel.ChannelID {
		t.Fatalf("replacement channel = %+v, %v", replacement, err)
	}

	sink, err := r.RegisterEventSink(context.Background(), model.EventSinkRegisterArgs{Capacity: 2})
	if err != nil || sink.SinkID == "" {
		t.Fatalf("sink = %+v, %v", sink, err)
	}
	if err = r.BindEventSink(context.Background(), model.EventSinkIDArgs{SinkID: "wrong"}); !errors.Is(err, ErrControlNotFound) {
		t.Fatalf("wrong sink bind = %v", err)
	}
	if err = r.BindEventSink(context.Background(), model.EventSinkIDArgs{SinkID: sink.SinkID}); err != nil {
		t.Fatal(err)
	}
	logEvent := model.Event{ID: "log", Name: "diagnostics.log", CreatedAt: time.Unix(10, 0), Value: model.DiagnosticLogEvent{Level: "info", Code: "bounded", DiagnosticID: "safe"}}
	if err = r.PublishEvent(context.Background(), logEvent); !errors.Is(err, ErrDiagnosticLogsOff) {
		t.Fatalf("unsubscribed log = %v", err)
	}
	if err = r.SetDiagnosticLogSubscription(context.Background(), model.DiagnosticsLogsArgs{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err = r.PublishEvent(context.Background(), logEvent); err != nil {
		t.Fatal(err)
	}
	if got, nextErr := r.NextEvent(context.Background()); nextErr != nil || got.ID != logEvent.ID {
		t.Fatalf("diagnostic event = %+v, %v", got, nextErr)
	}
	if err = r.ClearEventSink(context.Background(), model.EventSinkIDArgs{SinkID: sink.SinkID}); err != nil {
		t.Fatal(err)
	}

	r.BeginShutdown()
	if _, err = r.UpdateConfig(context.Background(), model.ConfigUpdateArgs{}); !errors.Is(err, ErrClosing) && !errors.Is(err, ErrClosed) {
		t.Fatalf("closing config update = %v", err)
	}
	if _, err = r.RegisterEventSink(context.Background(), model.EventSinkRegisterArgs{Capacity: 1}); !errors.Is(err, ErrClosing) && !errors.Is(err, ErrClosed) {
		t.Fatalf("closing sink registration = %v", err)
	}
	qaPod2ShutdownAndDrain(t, r, nil)
}

func qaPod2CommandDelivery(t *testing.T, gate *commandgate.Gate, commandID string) model.Event {
	t.Helper()
	if _, err := gate.Submit(context.Background(), model.Command{ID: commandID, Name: "delivery.next", Args: model.EmptyArgs{}}); err != nil {
		t.Fatal(err)
	}
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.Err != nil {
		t.Fatalf("delivery completion = %+v, %v", completion, err)
	}
	value, ok := completion.Value.(model.EventResult)
	if !ok {
		t.Fatalf("delivery result = %T", completion.Value)
	}
	return value.Event
}

func qaPod2MessageEvent(id string, second int64) model.Event {
	return model.Event{ID: id, Name: "message.received", CreatedAt: time.Unix(second, 0), Value: model.MessageReceivedEvent{MessageID: "message-" + id}}
}

func qaPod2ShutdownAndDrain(t *testing.T, r *Runtime, prefix []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Shutdown(ctx) }()
	for _, id := range prefix {
		event, err := r.NextEvent(ctx)
		if err != nil || event.ID != id {
			t.Fatalf("pre-terminal event = %+v, %v, want %s", event, err, id)
		}
	}
	for _, want := range []model.LifecycleState{model.LifecycleClosing, model.LifecycleClosed} {
		event, err := r.NextEvent(ctx)
		if err != nil {
			t.Fatalf("NextEvent(%s) = %v", want, err)
		}
		state, ok := event.Value.(model.SessionStateChangedEvent)
		if !ok || state.Current != want {
			t.Fatalf("terminal event = %+v, want %s", event, want)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if _, err := r.NextEvent(context.Background()); !errors.Is(err, delivery.ErrClosed) {
		t.Fatalf("post-shutdown NextEvent = %v", err)
	}
}
