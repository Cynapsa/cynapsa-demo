package delivery

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestEventByteCapacityDerivationAndConstructorBounds(t *testing.T) {
	if got, err := DeriveOrdinaryByteCapacity(1, 1); err != nil || got != eventMetadataAllowance+1 {
		t.Fatalf("small derivation=%d, %v", got, err)
	}
	if got, err := DeriveOrdinaryByteCapacity(1, 0); err != nil || got != eventMetadataAllowance {
		t.Fatalf("zero-payload derivation=%d, %v", got, err)
	}
	if got, err := DeriveOrdinaryByteCapacity(math.MaxUint32, v1.MaximumPayloadBytes); err != nil || got != maximumOrdinaryEventBytes {
		t.Fatalf("capped derivation=%d, %v", got, err)
	}
	for _, test := range []struct {
		queue   uint32
		payload uint64
	}{
		{0, 1}, {1, math.MaxUint64},
	} {
		if _, err := DeriveOrdinaryByteCapacity(test.queue, test.payload); !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("DeriveOrdinaryByteCapacity(%d,%d)=%v", test.queue, test.payload, err)
		}
	}
	for _, bytes := range []uint64{0, maximumOrdinaryEventBytes + 1} {
		if dispatcher, err := NewWithByteCapacity(1, bytes); dispatcher != nil || !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("NewWithByteCapacity(%d)=(%v,%v)", bytes, dispatcher, err)
		}
	}
}

func TestEventByteAdmissionExactBoundPlusOneAndCountIndependent(t *testing.T) {
	exact := memoryEvent("exact", "metadata")
	exactBytes, err := eventOwnedBytes(exact)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(8, exactBytes*2)
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Push(context.Background(), exact); err != nil {
		t.Fatal(err)
	}
	second := memoryEvent("exact", "metadata")
	if err = dispatcher.Push(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 2 || stats.Capacity != 8 || stats.OwnedBytes != exactBytes*2 || stats.OrdinaryEventBytes != exactBytes*2 {
		t.Fatalf("byte-only saturation stats=%+v", stats)
	}
	if err = dispatcher.Push(context.Background(), memoryEvent("x", "y")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("byte-only overflow=%v", err)
	}

	exactDispatcher, _ := NewWithByteCapacity(2, exactBytes)
	if err = exactDispatcher.Push(context.Background(), exact); err != nil {
		t.Fatalf("exact bound=%v", err)
	}
	plusOne := memoryEvent("exact", "metadata+")
	plusOneBytes, sizeErr := eventOwnedBytes(plusOne)
	if sizeErr != nil || plusOneBytes <= exactBytes {
		t.Fatalf("plus-one size=%d/%d err=%v", plusOneBytes, exactBytes, sizeErr)
	}
	plusOneDispatcher, _ := NewWithByteCapacity(2, plusOneBytes-1)
	if err = plusOneDispatcher.Push(context.Background(), plusOne); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("plus-one admission=%v", err)
	}
}

func TestEventByteAccountingMovesAcrossLeaseRollbackCommitAndRetirement(t *testing.T) {
	event := memoryEvent("lease", "owned")
	bytes, _ := eventOwnedBytes(event)
	dispatcher, _ := NewWithByteCapacity(4, bytes*4)
	admission, err := dispatcher.DeliverTracked(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 1 || stats.OwnedBytes != bytes {
		t.Fatalf("active lease stats=%+v", stats)
	}
	if err = dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 1 || stats.OwnedBytes != bytes {
		t.Fatalf("rollback stats=%+v", stats)
	}
	_, lease, err = dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !dispatcher.RetireAdmission(admission) {
		t.Fatal("active admission not retired")
	}
	if stats := dispatcher.Stats(); stats.Depth != 1 || stats.OwnedBytes != bytes {
		t.Fatalf("retired active lease released before finalizer=%+v", stats)
	}
	if err = dispatcher.Commit(lease); !errors.Is(err, ErrLeaseRetired) {
		t.Fatalf("retired commit=%v", err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 || stats.OwnedBytes != 0 {
		t.Fatalf("retired final stats=%+v", stats)
	}

	frontAdmission, err := dispatcher.DeliverTracked(context.Background(), memoryEvent("front", "owned"))
	if err != nil {
		t.Fatal(err)
	}
	_, lease, _ = dispatcher.ReserveNext(context.Background())
	if err = dispatcher.Rollback(lease); err != nil {
		t.Fatal(err)
	}
	if !dispatcher.RetireAdmission(frontAdmission) || dispatcher.Stats().OwnedBytes != 0 {
		t.Fatalf("front retirement stats=%+v", dispatcher.Stats())
	}
}

func TestEventByteAccountingIncludesFixedShutdownReserveExactlyOnce(t *testing.T) {
	ordinary := memoryEvent("ordinary", "owned")
	ordinaryBytes, _ := eventOwnedBytes(ordinary)
	dispatcher, _ := NewWithByteCapacity(1, ordinaryBytes)
	closing := model.Event{ID: "closing", Name: "session.state_changed", CreatedAt: time.Unix(2, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleReady, Current: model.LifecycleClosing}}
	closed := model.Event{ID: "closed", Name: "session.state_changed", CreatedAt: time.Unix(3, 0), Value: model.SessionStateChangedEvent{Previous: model.LifecycleClosing, Current: model.LifecycleClosed}}
	closingBytes, _ := eventOwnedBytes(closing)
	closedBytes, _ := eventOwnedBytes(closed)
	if err := dispatcher.Push(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.BeginShutdownWithEvent(context.Background(), closing); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.SealShutdownWithEvent(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	stats := dispatcher.Stats()
	if stats.OwnedBytes != ordinaryBytes+closingBytes+closedBytes || stats.OrdinaryEventBytes != ordinaryBytes || stats.ShutdownEventBytes != closingBytes+closedBytes || stats.ByteCapacity != ordinaryBytes+shutdownEventByteCapacity {
		t.Fatalf("staged lifecycle stats=%+v", stats)
	}
	for _, want := range []string{"ordinary", "closing", "closed"} {
		event, err := dispatcher.Next(context.Background())
		if err != nil || event.ID != want {
			t.Fatalf("Next=%q,%v want=%q", event.ID, err, want)
		}
	}
	if stats = dispatcher.Stats(); stats.OwnedBytes != 0 || stats.ShutdownEventBytes != 0 || !stats.Closed {
		t.Fatalf("drained lifecycle stats=%+v", stats)
	}
}

func TestEventByteAccountingActiveLeaseShutdownReleasesExactlyOnce(t *testing.T) {
	event := memoryEvent("lease", "secret")
	bytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(1, bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	_, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 1 || stats.OwnedBytes != bytes {
		t.Fatalf("leased stats=%+v", stats)
	}
	dispatcher.BeginShutdown()
	if err = dispatcher.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 || stats.OwnedBytes != 0 || !stats.Closed {
		t.Fatalf("cleared lease stats=%+v", stats)
	}
	if err = dispatcher.Commit(lease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("abandoned lease commit=%v", err)
	}
}

func TestOversizedLifecycleEventRejectsWithoutTransitionOrMutation(t *testing.T) {
	large := strings.Repeat("x", shutdownEventMaximumBytes)
	value := &model.CoreErrorEvent{Err: &model.Error{Code: large}}
	event := model.Event{ID: "closing", Name: "session.state_changed", CreatedAt: time.Unix(1, 0), Value: value}
	dispatcher, err := NewWithByteCapacity(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.BeginShutdownWithEvent(context.Background(), event); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("oversized lifecycle admission=%v", err)
	}
	if stats := dispatcher.Stats(); stats.Closing || stats.OwnedBytes != 0 || stats.ShutdownEventDepth != 0 {
		t.Fatalf("rejected lifecycle changed state=%+v", stats)
	}
	if value.Err == nil || value.Err.Code != large {
		t.Fatal("rejected lifecycle event mutated caller ownership")
	}
}

func TestEventAdmissionFreezesAliasesAndDropsOpaqueErrorCause(t *testing.T) {
	t.Run("oversized slice capacity rejected without mutation", func(t *testing.T) {
		body := make([]byte, 1, (3<<20)/2)
		body[0] = 0x5a
		value := &model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: &model.NativePayload{ContentType: "application/octet-stream", Path: "/", Body: body}}}
		event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: value}
		dispatcher, _ := NewWithByteCapacity(2, 1<<20)
		if err := dispatcher.Push(context.Background(), event); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("oversized-cap admission=%v", err)
		}
		payload := value.Payload.Value.(*model.NativePayload)
		if len(payload.Body) != 1 || cap(payload.Body) != cap(body) || unsafe.SliceData(payload.Body) != unsafe.SliceData(body) || payload.Body[0] != 0x5a {
			t.Fatal("rejection mutated caller-owned body")
		}
	})
	t.Run("pointer input becomes independent value with exact frozen backing", func(t *testing.T) {
		large := strings.Repeat("x", 2<<20)
		substring := large[:1]
		body := make([]byte, 1, 1024)
		body[0] = 0x7b
		payload := &model.NativePayload{ContentType: substring, Path: "/", Body: body}
		value := &model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: payload}}
		event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: value}
		bytes, _ := eventOwnedBytes(event)
		dispatcher, _ := NewWithByteCapacity(1, bytes)
		if err := dispatcher.Push(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		if value.Payload.Value != payload || cap(payload.Body) != cap(body) || unsafe.SliceData(payload.Body) != unsafe.SliceData(body) || unsafe.StringData(payload.ContentType) != unsafe.StringData(substring) {
			t.Fatal("successful pointer admission mutated caller ownership")
		}
		stats := dispatcher.Stats()
		got, err := dispatcher.Next(context.Background())
		gotValue, ok := got.Value.(model.MessageReceivedEvent)
		gotPayload, payloadOK := gotValue.Payload.Value.(model.NativePayload)
		frozenBytes, frozenErr := eventOwnedBytes(got)
		if err != nil || !ok || !payloadOK || cap(gotPayload.Body) != len(gotPayload.Body) || unsafe.SliceData(gotPayload.Body) == unsafe.SliceData(body) || unsafe.StringData(gotPayload.ContentType) == unsafe.StringData(substring) || frozenErr != nil || stats.OwnedBytes != frozenBytes {
			t.Fatalf("frozen pointer transfer=%#v,%v", got.Value, err)
		}
		body[0] = 0
		if gotPayload.Body[0] != 0x7b {
			t.Fatal("caller mutation changed frozen event body")
		}
	})
	t.Run("opaque cause never invoked or retained", func(t *testing.T) {
		cause := &panicEventCause{retained: make([]byte, 2<<20)}
		value := &model.CoreErrorEvent{Err: &model.Error{Code: "core", Cause: cause}}
		event := model.Event{ID: "error", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: value}
		bytes, _ := eventOwnedBytes(event)
		dispatcher, _ := NewWithByteCapacity(1, bytes)
		if err := dispatcher.Push(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		if value.Err == nil || value.Err.Cause != cause || value.Err.Code != "core" {
			t.Fatalf("successful admission mutated caller error=%#v", value.Err)
		}
		got, nextErr := dispatcher.Next(context.Background())
		gotValue, ok := got.Value.(model.CoreErrorEvent)
		if nextErr != nil || !ok || gotValue.Err == nil || gotValue.Err.Cause != nil || gotValue.Err.Code != "core" || gotValue.Err == value.Err {
			t.Fatalf("frozen error=%#v err=%v", got.Value, nextErr)
		}
	})
	t.Run("rejected opaque cause unchanged", func(t *testing.T) {
		dispatcher, _ := NewWithByteCapacity(1, uint64(len("fulltestcore.error")))
		if err := dispatcher.Push(context.Background(), model.Event{ID: "full", Name: "test", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{}}); err != nil {
			t.Fatal(err)
		}
		cause := &panicEventCause{}
		value := &model.CoreErrorEvent{Err: &model.Error{Code: "core", Cause: cause}}
		event := model.Event{ID: "error", Name: "core.error", CreatedAt: time.Unix(1, 0), Value: value}
		if err := dispatcher.Push(context.Background(), event); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("rejected cause admission=%v", err)
		}
		if value.Err.Cause != cause {
			t.Fatal("rejection changed opaque cause")
		}
	})
}

func TestEventAdmissionFreezesHeaderBackingAndPayloadVariants(t *testing.T) {
	large := strings.Repeat("h", 2<<20)
	headers := make([]model.Header, 1, 1024)
	headers[0] = model.Header{Name: large[:1], Value: large[1:2]}
	body := make([]byte, 2, 4096)
	body[0], body[1] = 0x51, 0x52
	request := &model.HTTPRequestPayload{Method: large[:1], Path: "/", Query: "q", Headers: headers, Body: body}
	value := &model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: request}}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: value}
	preflightBytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(1, preflightBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if value.Payload.Value != request || cap(request.Headers) != cap(headers) || cap(request.Body) != cap(body) ||
		unsafe.StringData(request.Method) != unsafe.StringData(large) || unsafe.StringData(request.Headers[0].Name) != unsafe.StringData(large) {
		t.Fatal("successful HTTP pointer admission mutated caller ownership")
	}
	stats := dispatcher.Stats()
	got, nextErr := dispatcher.Next(context.Background())
	gotValue, ok := got.Value.(model.MessageReceivedEvent)
	gotRequest, payloadOK := gotValue.Payload.Value.(model.HTTPRequestPayload)
	owned, ownedErr := eventOwnedBytes(got)
	if nextErr != nil || !ok || !payloadOK || cap(gotRequest.Headers) != len(gotRequest.Headers) || cap(gotRequest.Body) != len(gotRequest.Body) ||
		unsafe.StringData(gotRequest.Method) == unsafe.StringData(large) || unsafe.StringData(gotRequest.Headers[0].Name) == unsafe.StringData(large) ||
		ownedErr != nil || owned >= preflightBytes || stats.OwnedBytes != owned {
		t.Fatalf("frozen request bytes=%d preflight=%d stats=%+v err=%v event=%#v", owned, preflightBytes, stats, ownedErr, got.Value)
	}

	for index, payload := range []model.PayloadValue{
		model.NativePayload{ContentType: "a", Path: "/", Body: []byte("b")}, &model.NativePayload{ContentType: "a", Path: "/", Body: []byte("b")},
		model.HTTPRequestPayload{Method: "GET", Path: "/", Headers: []model.Header{{Name: "a", Value: "b"}}, Body: []byte("c")}, &model.HTTPRequestPayload{Method: "GET", Path: "/", Headers: []model.Header{{Name: "a", Value: "b"}}, Body: []byte("c")},
		model.HTTPResponsePayload{StatusCode: 200, Reason: "ok", Headers: []model.Header{{Name: "a", Value: "b"}}, Body: []byte("c")}, &model.HTTPResponsePayload{StatusCode: 200, Reason: "ok", Headers: []model.Header{{Name: "a", Value: "b"}}, Body: []byte("c")},
		model.PayloadHandle{Handle: "handle"}, &model.PayloadHandle{Handle: "handle"},
	} {
		candidate := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: payload}}}
		if _, sizeErr := eventOwnedBytes(candidate); sizeErr != nil {
			t.Fatalf("payload variant %d (%T)=%v", index, payload, sizeErr)
		}
	}
}

func TestDispatcherOwnsAndClearsApplicationError(t *testing.T) {
	inputError := &model.ApplicationError{Code: "handler_error", Detail: "Safe", DetailsJSON: `{"retry":false}`}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{
		MessageID: "message",
		Payload:   model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500, Error: inputError}},
	}}
	bytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(1, bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	inputError.Code = "mutated"
	got, err := dispatcher.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ownedError := got.Value.(model.MessageReceivedEvent).Payload.Value.(model.HTTPResponsePayload).Error
	if ownedError == nil || ownedError == inputError || ownedError.Code != "handler_error" {
		t.Fatalf("application error was not independently frozen: %#v", ownedError)
	}
	clearOwnedEvent(&got)
	if *ownedError != (model.ApplicationError{}) {
		t.Fatalf("application error was not cleared: %#v", ownedError)
	}
}

func TestApplicationErrorEventAccountingChargesAllocatedObjectAtExactBoundary(t *testing.T) {
	base := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{
		MessageID: "message", Payload: model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500}},
	}}
	withError := base
	withError.Value = model.MessageReceivedEvent{
		MessageID: "message", Payload: model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500, Error: &model.ApplicationError{}}},
	}
	baseBytes, err := eventOwnedBytes(base)
	if err != nil {
		t.Fatal(err)
	}
	errorBytes, err := eventOwnedBytes(withError)
	if err != nil {
		t.Fatal(err)
	}
	wantDelta := uint64(unsafe.Sizeof(model.ApplicationError{}))
	if errorBytes-baseBytes != wantDelta {
		t.Fatalf("application error charge delta=%d, want allocated object size %d", errorBytes-baseBytes, wantDelta)
	}
	exact, err := NewWithByteCapacity(1, errorBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = exact.Push(context.Background(), withError); err != nil {
		t.Fatalf("exact application error boundary=%v", err)
	}
	short, err := NewWithByteCapacity(1, errorBytes-1)
	if err != nil {
		t.Fatal(err)
	}
	if err = short.Push(context.Background(), withError); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("undercharged application error boundary=%v", err)
	}
}

func TestAcceptedPointerEventAndPayloadVariantsNormalizeToValues(t *testing.T) {
	now := time.Unix(1, 0)
	values := []model.EventValue{
		&model.SessionStateChangedEvent{}, &model.ConnectivityChangedEvent{}, &model.PeerReachabilityEvent{},
		&model.MessageStateEvent{}, &model.PayloadTransferEvent{}, &model.PolicyRejectedEvent{},
		&model.RPCTimeoutEvent{}, &model.QueueCapacityEvent{}, &model.CoreErrorEvent{}, &model.DiagnosticLogEvent{},
	}
	for index, value := range values {
		event := model.Event{ID: "event", Name: "test", CreatedAt: now, Value: value}
		bytes, err := eventOwnedBytes(event)
		if err != nil {
			t.Fatalf("variant %d preflight=%v", index, err)
		}
		dispatcher, _ := NewWithByteCapacity(1, bytes)
		if err = dispatcher.Push(context.Background(), event); err != nil {
			t.Fatalf("variant %d push=%v", index, err)
		}
		got, nextErr := dispatcher.Next(context.Background())
		if nextErr != nil || reflect.TypeOf(got.Value).Kind() == reflect.Pointer {
			t.Fatalf("variant %d normalized as %T, err=%v", index, got.Value, nextErr)
		}
	}

	for index, payload := range []model.PayloadValue{
		&model.NativePayload{Body: []byte("a")}, &model.HTTPRequestPayload{Body: []byte("a")},
		&model.HTTPResponsePayload{Body: []byte("a")}, &model.PayloadHandle{Handle: "handle"},
	} {
		event := model.Event{ID: "event", Name: "message.received", CreatedAt: now, Value: &model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: payload}}}
		bytes, err := eventOwnedBytes(event)
		if err != nil {
			t.Fatalf("payload %d preflight=%v", index, err)
		}
		dispatcher, _ := NewWithByteCapacity(1, bytes)
		if err = dispatcher.Push(context.Background(), event); err != nil {
			t.Fatalf("payload %d push=%v", index, err)
		}
		got, nextErr := dispatcher.Next(context.Background())
		gotValue, ok := got.Value.(model.MessageReceivedEvent)
		if nextErr != nil || !ok || reflect.TypeOf(gotValue.Payload.Value).Kind() == reflect.Pointer {
			t.Fatalf("payload %d normalized as %T/%T, err=%v", index, got.Value, gotValue.Payload.Value, nextErr)
		}
	}
}

func TestInteriorEventPointerDoesNotRetainCallerBacking(t *testing.T) {
	backing := make([]model.MessageReceivedEvent, 4096)
	backing[0] = model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: &model.NativePayload{Body: []byte("owned")}}}
	callerPointer := &backing[0]
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: callerPointer}
	bytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := NewWithByteCapacity(1, bytes)
	if err = dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	got, err := dispatcher.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gotValue, ok := got.Value.(model.MessageReceivedEvent)
	gotPayload, payloadOK := gotValue.Payload.Value.(model.NativePayload)
	if !ok || !payloadOK || got.Value == callerPointer || string(gotPayload.Body) != "owned" {
		t.Fatalf("interior pointer normalized as %T/%T", got.Value, gotValue.Payload.Value)
	}
	backing[0].MessageID = "changed"
	backing[0].Payload.Value.(*model.NativePayload).Body[0] = 0
	if gotValue.MessageID != "message" || string(gotPayload.Body) != "owned" {
		t.Fatal("frozen event retained caller interior backing")
	}
}

func TestShutdownInvalidatesActiveLeaseWithoutMutatingBorrowedEvent(t *testing.T) {
	body := []byte("secret-borrowed-by-consumer")
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: model.MessageReceivedEvent{
		MessageID: "message", Payload: model.Payload{Value: model.NativePayload{Body: body}},
	}}
	bytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := NewWithByteCapacity(1, bytes)
	if err = dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	borrowed, lease, err := dispatcher.ReserveNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	borrowedBody := borrowed.Value.(model.MessageReceivedEvent).Payload.Value.(model.NativePayload).Body
	started := make(chan struct{})
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	var observed atomic.Uint64
	go func() {
		close(started)
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
				for _, value := range borrowedBody {
					observed.Add(uint64(value))
				}
			}
		}
	}()
	<-started
	dispatcher.BeginShutdown()
	if err = dispatcher.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-readerDone
	_ = observed.Load()
	if string(borrowedBody) != "secret-borrowed-by-consumer" {
		t.Fatalf("shutdown mutated borrowed bytes: %q", borrowedBody)
	}
	if err = dispatcher.Commit(lease); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("invalidated commit=%v", err)
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 || stats.OwnedBytes != 0 || !stats.Closed {
		t.Fatalf("invalidated lease stats=%+v", stats)
	}
}

func TestEventAdmissionDropsCustomTimestampLocationOwnership(t *testing.T) {
	locationName := strings.Repeat("zone", 1<<18)
	original := time.Date(2026, 8, 21, 12, 0, 0, 0, time.FixedZone(locationName, 3600))
	event := model.Event{ID: "event", Name: "core.error", CreatedAt: original, Value: model.CoreErrorEvent{}}
	bytes, err := eventOwnedBytes(event)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(1, bytes)
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
	if !got.CreatedAt.Equal(original) || got.CreatedAt.Location() != time.UTC {
		t.Fatalf("frozen timestamp=%v location=%v", got.CreatedAt, got.CreatedAt.Location())
	}
	if event.CreatedAt.Location() == time.UTC {
		t.Fatal("admission mutated caller timestamp")
	}
}

func TestEventByteAccountingUnderflowPanicsInsteadOfClamping(t *testing.T) {
	dispatcher, err := NewWithByteCapacity(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("impossible accounting underflow was silently concealed")
		}
	}()
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	dispatcher.releaseRecordLocked(queuedEvent{bytes: 1}, false)
}

func TestEventAbandonmentZeroesFrozenPayloadAndReleasesBytes(t *testing.T) {
	body := []byte("secret-canary")
	payload := &model.NativePayload{ContentType: "application/octet-stream", Path: "/", Body: body}
	value := &model.MessageReceivedEvent{MessageID: "message", Payload: model.Payload{Value: payload}}
	event := model.Event{ID: "event", Name: "message.received", CreatedAt: time.Unix(1, 0), Value: value}
	bytes, _ := eventOwnedBytes(event)
	dispatcher, _ := NewWithByteCapacity(1, bytes)
	if err := dispatcher.Push(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	dispatcher.mu.Lock()
	record := <-dispatcher.queue
	dispatcher.queue <- record
	dispatcher.mu.Unlock()
	frozenValue := record.event.Value.(model.MessageReceivedEvent)
	frozen := frozenValue.Payload.Value.(model.NativePayload).Body
	dispatcher.BeginShutdown()
	if err := dispatcher.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index, value := range frozen {
		if value != 0 {
			t.Fatalf("frozen byte %d not cleared", index)
		}
	}
	if value.MessageID != "message" || string(payload.Body) != "secret-canary" {
		t.Fatalf("shutdown mutated caller ownership: %#v %#v", value, payload)
	}
	if stats := dispatcher.Stats(); stats.OwnedBytes != 0 || stats.Depth != 0 || !stats.Closed {
		t.Fatalf("clear stats=%+v", stats)
	}
}

func TestConcurrentEventByteAccountingNeverExceedsOrLeaks(t *testing.T) {
	prototype := memoryEvent("event-000", strings.Repeat("x", 256))
	bytes, _ := eventOwnedBytes(prototype)
	dispatcher, _ := NewWithByteCapacity(64, bytes*16)
	const producers = 256
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(producers)
	for index := 0; index < producers; index++ {
		go func(index int) {
			defer group.Done()
			<-start
			event := memoryEvent("event-000", strings.Repeat(string(rune('a'+index%26)), 256))
			_ = dispatcher.Push(context.Background(), event)
		}(index)
	}
	close(start)
	group.Wait()
	stats := dispatcher.Stats()
	if stats.Depth > stats.Capacity || stats.OwnedBytes > stats.ByteCapacity || stats.OrdinaryEventBytes > stats.OrdinaryByteCapacity {
		t.Fatalf("concurrent saturation stats=%+v", stats)
	}
	dispatcher.BeginShutdown()
	if err := dispatcher.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats = dispatcher.Stats(); stats.Depth != 0 || stats.OwnedBytes != 0 {
		t.Fatalf("concurrent cleanup stats=%+v", stats)
	}
}

func TestConcurrentEventProducersConsumerAndStatsRemainExact(t *testing.T) {
	prototype := memoryEvent("event", strings.Repeat("x", 64))
	bytes, err := eventOwnedBytes(prototype)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewWithByteCapacity(32, bytes*32)
	if err != nil {
		t.Fatal(err)
	}
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			if _, nextErr := dispatcher.Next(consumeCtx); nextErr != nil {
				return
			}
		}
	}()

	stopObserver := make(chan struct{})
	observerDone := make(chan struct{})
	var incoherent atomic.Bool
	go func() {
		defer close(observerDone)
		for {
			select {
			case <-stopObserver:
				return
			default:
			}
			stats := dispatcher.Stats()
			if stats.OwnedBytes != uint64(stats.Depth)*bytes || stats.OrdinaryEventBytes != stats.OwnedBytes || stats.OwnedBytes > stats.ByteCapacity {
				incoherent.Store(true)
				return
			}
			runtime.Gosched()
		}
	}()

	const producers = 8
	const attempts = 500
	var group sync.WaitGroup
	group.Add(producers)
	for range producers {
		go func() {
			defer group.Done()
			for range attempts {
				err := dispatcher.Push(context.Background(), memoryEvent("event", strings.Repeat("x", 64)))
				if err != nil && !errors.Is(err, ErrQueueFull) {
					incoherent.Store(true)
					return
				}
			}
		}()
	}
	group.Wait()
	cancelConsume()
	<-consumerDone
	close(stopObserver)
	<-observerDone
	if incoherent.Load() {
		t.Fatal("concurrent event ownership snapshot was incoherent")
	}
	for {
		_, nextErr := dispatcher.TryNext(context.Background())
		if errors.Is(nextErr, ErrQueueEmpty) {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
	}
	if stats := dispatcher.Stats(); stats.Depth != 0 || stats.OwnedBytes != 0 {
		t.Fatalf("concurrent final stats=%+v", stats)
	}
}

func TestEventSizerClosedVariantsTypedNilAndOverflow(t *testing.T) {
	now := time.Unix(1, 0)
	values := []model.EventValue{
		model.SessionStateChangedEvent{}, &model.SessionStateChangedEvent{}, model.ConnectivityChangedEvent{}, &model.ConnectivityChangedEvent{},
		model.PeerReachabilityEvent{}, &model.PeerReachabilityEvent{}, model.MessageReceivedEvent{}, &model.MessageReceivedEvent{},
		model.MessageStateEvent{}, &model.MessageStateEvent{}, model.PayloadTransferEvent{}, &model.PayloadTransferEvent{},
		model.PolicyRejectedEvent{}, &model.PolicyRejectedEvent{}, model.RPCTimeoutEvent{}, &model.RPCTimeoutEvent{},
		model.QueueCapacityEvent{}, &model.QueueCapacityEvent{}, model.CoreErrorEvent{}, &model.CoreErrorEvent{},
		model.DiagnosticLogEvent{}, &model.DiagnosticLogEvent{},
	}
	for index, value := range values {
		if _, err := eventOwnedBytes(model.Event{ID: "id", Name: "name", CreatedAt: now, Value: value}); err != nil {
			t.Fatalf("variant %d (%T)=%v", index, value, err)
		}
	}
	var typedNil *model.MessageReceivedEvent
	if _, err := eventOwnedBytes(model.Event{ID: "id", Name: "name", CreatedAt: now, Value: typedNil}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("typed nil=%v", err)
	}
	if _, ok := checkedAdd(math.MaxUint64, 1); ok {
		t.Fatal("checkedAdd accepted overflow")
	}
	if _, ok := checkedMultiply(math.MaxUint64, 2); ok {
		t.Fatal("checkedMultiply accepted overflow")
	}
}

type panicEventCause struct{ retained []byte }

func (*panicEventCause) Error() string       { panic("opaque event cause invoked") }
func (cause *panicEventCause) Unwrap() error { return cause }

func memoryEvent(id, code string) model.Event {
	return model.Event{ID: id, Name: "core.error", CreatedAt: time.Unix(1, 0), Value: model.CoreErrorEvent{Err: &model.Error{Code: code}}}
}
