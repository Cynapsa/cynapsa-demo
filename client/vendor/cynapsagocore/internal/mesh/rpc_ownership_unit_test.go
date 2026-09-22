package mesh

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/rpc"
)

func TestMessageRequestScrubsOwnedResponseAfterPostWaitCancellation(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(ctx, model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := <-notify
	responseValue := cloneModelPayload(model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK", Body: []byte("owned response secret")}})
	ownedBody := responseValue.Value.(model.HTTPResponsePayload).Body
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, responseValue)
	charge := modelPayloadDynamicBytes(responseValue) + uint64(len(response.CorrelationID))

	service.mu.Lock()
	if !service.rpcBudget.ReserveOwned(charge) {
		service.mu.Unlock()
		t.Fatal("response value budget admission failed")
	}
	service.responseValues[strings.Clone(response.CorrelationID)] = retainedPayload{value: responseValue, bytes: charge}
	if err := service.outboundRPC.Complete(response); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	cancel()
	service.mu.Unlock()

	select {
	case failure := <-done:
		if failure == nil || failure.Code != FailureCancelled {
			t.Fatalf("failure=%#v", failure)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("MessageRequest did not return")
	}
	if !bytes.Equal(ownedBody, make([]byte, len(ownedBody))) {
		t.Fatalf("retained response not scrubbed: %q", ownedBody)
	}
	service.mu.Lock()
	remaining := len(service.responseValues)
	service.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("response values retained=%d", remaining)
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatal(failure)
	}
}

func TestAuthenticatedResponseCapacityFailureWakesWaiterAndClearsValue(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	done := make(chan *Failure, 1)
	go func() {
		_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
		done <- failure
	}()
	request := <-notify
	responseValue := cloneModelPayload(model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK", Body: []byte("capacity secret")}})
	ownedBody := responseValue.Value.(model.HTTPResponsePayload).Body
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, responseValue)
	charge := modelPayloadDynamicBytes(responseValue) + uint64(len(response.CorrelationID))
	service.mu.Lock()
	if !service.rpcBudget.ReserveOwned(charge) {
		service.mu.Unlock()
		t.Fatal("response value reserve failed")
	}
	service.responseValues[strings.Clone(response.CorrelationID)] = retainedPayload{value: responseValue, bytes: charge}
	service.mu.Unlock()
	service.failAuthenticatedResponse(response, rpc.ErrCapacity)
	select {
	case failure := <-done:
		if failure == nil || failure.Code != FailureCapacity {
			t.Fatalf("waiter failure=%#v", failure)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("capacity failure left waiter live")
	}
	if !bytes.Equal(ownedBody, make([]byte, len(ownedBody))) {
		t.Fatal("capacity failure did not scrub response value")
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatal(failure)
	}
}

func TestResponsePayloadCloneOwnsAndClearsApplicationError(t *testing.T) {
	inputError := &model.ApplicationError{Code: "not_found", Detail: "Missing", DetailsJSON: `{"id":7}`}
	cloned := cloneModelPayload(model.Payload{Value: model.HTTPResponsePayload{StatusCode: 404, Error: inputError}})
	ownedError := cloned.Value.(model.HTTPResponsePayload).Error
	inputError.Code = "mutated"
	if ownedError == nil || ownedError == inputError || ownedError.Code != "not_found" {
		t.Fatalf("application error was not independently cloned: %#v", ownedError)
	}
	zeroModelPayload(cloned)
	if *ownedError != (model.ApplicationError{}) {
		t.Fatalf("application error was not cleared: %#v", ownedError)
	}
}

func TestResponsePayloadDynamicAccountingChargesAllocatedApplicationError(t *testing.T) {
	base := model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500}}
	withError := model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500, Error: &model.ApplicationError{}}}
	baseBytes := modelPayloadDynamicBytes(base)
	errorBytes := modelPayloadDynamicBytes(withError)
	wantDelta := uint64(unsafe.Sizeof(model.ApplicationError{}))
	if errorBytes-baseBytes != wantDelta {
		t.Fatalf("application error charge delta=%d, want allocated object size %d", errorBytes-baseBytes, wantDelta)
	}

	metadata := &model.ApplicationError{Code: "code", Detail: "detail", DetailsJSON: `{"safe":true}`}
	withMetadata := model.Payload{Value: model.HTTPResponsePayload{StatusCode: 500, Error: metadata}}
	want := wantDelta + uint64(len(metadata.Code)+len(metadata.Detail)+len(metadata.DetailsJSON))
	if got := modelPayloadDynamicBytes(withMetadata) - baseBytes; got != want {
		t.Fatalf("application error metadata charge=%d, want %d", got, want)
	}
}

func TestOutOfOrderResponseFreezesChargedInboundMapKey(t *testing.T) {
	notify := make(chan protocol.Envelope, 1)
	service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = service.MessageRequest(ctx, model.MessageRequestArgs{To: testPeerBare, Payload: nativePayload("/rpc", "question")})
	}()
	request := <-notify
	responseValue := model.Payload{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK", Body: []byte("out of order secret")}}
	response := signedInboundWithIDs(t, now, protocol.ModeResponse, request.CorrelationID, request.MessageID, request.ConversationID, responseValue)
	source := strings.Repeat("x", 1<<20) + response.MessageID + strings.Repeat("y", 1<<20)
	response.MessageID = source[1<<20 : (1<<20)+len(response.MessageID)]
	if failure := service.Receive(context.Background(), testInboundProvenance(t, response), response); failure != nil {
		t.Fatalf("receive=%#v", failure)
	}
	service.mu.Lock()
	retainedKey := ""
	for key := range service.inboundValues {
		if key == response.MessageID {
			retainedKey = key
			break
		}
	}
	service.mu.Unlock()
	keyStart := uintptr(unsafe.Pointer(unsafe.StringData(retainedKey)))
	sourceStart := uintptr(unsafe.Pointer(unsafe.StringData(source)))
	if keyStart >= sourceStart && keyStart < sourceStart+uintptr(len(source)) {
		t.Fatal("charged inbound key aliases caller backing")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not cancel")
	}
	if failure := service.Shutdown(context.Background()); failure != nil {
		t.Fatal(failure)
	}
}
