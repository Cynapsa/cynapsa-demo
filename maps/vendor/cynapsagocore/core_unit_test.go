package cynapsagocore

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestCoreCreationPayloadLimitExactBoundary(t *testing.T) {
	core, err := New(Config{QueueLimit: 1, PayloadLimit: v1.MaximumPayloadBytes})
	if err != nil {
		t.Fatalf("exact maximum rejected: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if core, err := New(Config{QueueLimit: 1, PayloadLimit: v1.MaximumPayloadBytes + 1}); core != nil || publicCode(err) != v1.ErrorCodeMalformedInput {
		t.Fatalf("maximum + 1 = (%v, %v), want malformed_input", core, err)
	}
}

func TestCoreFacadeStartSubmitCompletionStatusAndDestroy(t *testing.T) {
	core, err := New(Config{QueueLimit: 8, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	status, err := core.Status(context.Background())
	if err != nil || status.Lifecycle != v1.LifecycleCreated {
		t.Fatalf("Status = %#v, %v", status, err)
	}

	admission, err := core.Submit(context.Background(), v1.CoreInitCommand{CommandBase: v1.CommandBase{
		CommandID:    "command-1",
		SDKSessionID: "session-1",
	}})
	if err != nil || !admission.Accepted || admission.CommandHandle == "" || admission.Error != nil {
		t.Fatalf("Submit = %#v, %v", admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || !completion.OK || completion.CommandID != "command-1" {
		t.Fatalf("NextCompletion = %#v, %v", completion, err)
	}
	initialized, ok := completion.Result.(v1.CoreInitResult)
	if !ok || initialized.SDKSessionID != "session-1" {
		t.Fatalf("result = %#v", completion.Result)
	}

	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if err := core.Destroy(); err != nil {
		t.Fatalf("repeated Destroy: %v", err)
	}
	if _, err := core.Status(context.Background()); publicCode(err) != v1.ErrorCodeInvalidHandle {
		t.Fatalf("Status after destroy = %v", err)
	}
}

func TestCoreCapabilitiesAdvertiseComposedLargePayloads(t *testing.T) {
	core, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	admission, err := core.Submit(context.Background(), v1.CoreCapabilitiesCommand{CommandBase: v1.CommandBase{
		CommandID: "capabilities-deferred-encryption", SDKSessionID: "session-1",
	}})
	if err != nil || !admission.Accepted || admission.Error != nil {
		t.Fatalf("capability admission = %#v, %v", admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || !completion.OK || completion.Error != nil {
		t.Fatalf("capability completion = %#v, %v", completion, err)
	}
	capabilities, ok := completion.Result.(v1.Capabilities)
	if !ok {
		t.Fatalf("capability result = %#v", completion.Result)
	}
	seenMessaging := false
	seenLargePayloads := false
	for _, feature := range capabilities.Features {
		if feature == v1.CapabilityLargePayloads {
			seenLargePayloads = true
		}
		if feature == v1.CapabilityNativeMessaging {
			seenMessaging = true
		}
	}
	if !seenMessaging {
		t.Fatal("default Core omitted available native messaging capability")
	}
	if !seenLargePayloads {
		t.Fatal("default Core omitted composed large-payload support")
	}
	shutdownAndDrain(t, core)
	if err = core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestCorePayloadFinishReadAndCommandCloseAlias(t *testing.T) {
	core, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	handle, err := core.PayloadOpen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := hex.DecodeString("a500010100026003612f044178")
	if written, err := core.PayloadWrite(context.Background(), handle, canonical); err != nil || written != len(canonical) {
		t.Fatalf("write = %d, %v", written, err)
	}
	if size, err := core.PayloadFinish(context.Background(), handle); err != nil || size != int64(len(canonical)) {
		t.Fatalf("finish = %d, %v", size, err)
	}
	if err := core.PayloadCancel(context.Background(), handle); err != nil {
		t.Fatalf("completed cancel must be idempotent: %v", err)
	}
	chunk, eof, err := core.PayloadRead(context.Background(), handle, 0, len(canonical))
	if err != nil || !eof || string(chunk) != string(canonical) {
		t.Fatalf("read = %x, %t, %v", chunk, eof, err)
	}

	admission, err := core.Submit(context.Background(), v1.PayloadCloseCommand{
		CommandBase: v1.CommandBase{CommandID: "payload-close-1", SDKSessionID: "session-1"},
		Handle:      handle,
	})
	if err != nil || !admission.Accepted {
		t.Fatalf("close admission = %#v, %v", admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || !completion.OK {
		t.Fatalf("close completion = %#v, %v", completion, err)
	}
	if err := core.PayloadRelease(context.Background(), handle); publicCode(err) != v1.ErrorCodeInvalidHandle {
		t.Fatalf("payload.close did not consume exactly one reference: %v", err)
	}

	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestCoreFacadeRejectsMalformedAndNormalizesUnavailableCommand(t *testing.T) {
	core := newTestCoreWithConnectivity(t, unavailableBuiltInFactory())
	if _, err := core.Submit(nil, v1.CoreStatusCommand{}); publicCode(err) != v1.ErrorCodeMalformedInput {
		t.Fatalf("nil context = %v", err)
	}
	if _, err := core.Submit(context.Background(), v1.CoreStatusCommand{}); publicCode(err) != v1.ErrorCodeMalformedInput {
		t.Fatalf("malformed command = %v", err)
	}

	admission, err := core.Submit(context.Background(), v1.AuthConnectCommand{
		CommandBase: v1.CommandBase{CommandID: "command-auth", SDKSessionID: "session-1"},
		Auth:        v1.AuthInput{MeshEndpoint: "mesh.example:5222", Username: "agent-a@example.test", Password: "secret", MeshID: "mesh-1"},
	})
	if err != nil || !admission.Accepted {
		t.Fatalf("auth admission = %#v, %v", admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || completion.OK || completion.Error == nil || completion.Error.Code != v1.ErrorCodeConnectivityUnavailable {
		t.Fatalf("unavailable completion = %#v, %v", completion, err)
	}
	if completion.Error.Message != "Connectivity is unavailable" {
		t.Fatalf("unexpected stable message %q", completion.Error.Message)
	}

	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
}

func TestCoreShutdownCallerTimeoutDoesNotAbandonCleanup(t *testing.T) {
	core, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	admission, err := core.Submit(context.Background(), v1.CoreInitCommand{CommandBase: v1.CommandBase{CommandID: "command-pending", SDKSessionID: "session-1"}})
	if err != nil || !admission.Accepted {
		t.Fatalf("Submit = %#v, %v", admission, err)
	}

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := core.Shutdown(expired); publicCode(err) != v1.ErrorCodeShutdownTimeout {
		t.Fatalf("Shutdown expired = %v", err)
	}
	status, err := core.Status(context.Background())
	if err != nil || status.Lifecycle != v1.LifecycleClosing {
		t.Fatalf("Status during continuing cleanup = %#v, %v", status, err)
	}
	if _, err := core.PayloadOpen(context.Background()); publicCode(err) != v1.ErrorCodeShutdownInProgress {
		t.Fatalf("new payload admission during shutdown = %v", err)
	}

	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
}

func TestCorePayloadOpenWriteCancelReleaseAndCrossCoreRejection(t *testing.T) {
	left, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatalf("left New: %v", err)
	}
	right, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatalf("right New: %v", err)
	}
	handle, err := left.PayloadOpen(context.Background())
	if err != nil {
		t.Fatalf("PayloadOpen: %v", err)
	}
	chunk := []byte("owned")
	written, err := left.PayloadWrite(context.Background(), handle, chunk)
	if err != nil || written != len(chunk) {
		t.Fatalf("PayloadWrite = %d, %v", written, err)
	}
	if _, err := right.PayloadWrite(context.Background(), handle, chunk); publicCode(err) != v1.ErrorCodeInvalidHandle {
		t.Fatalf("cross-Core write = %v", err)
	}
	if err := left.PayloadCancel(context.Background(), handle); err != nil {
		t.Fatalf("PayloadCancel: %v", err)
	}
	if err := left.PayloadCancel(context.Background(), handle); err != nil {
		t.Fatalf("repeated PayloadCancel: %v", err)
	}
	if err := left.PayloadRelease(context.Background(), handle); err != nil {
		t.Fatalf("PayloadRelease: %v", err)
	}
	if err := left.PayloadRelease(context.Background(), handle); publicCode(err) != v1.ErrorCodeInvalidHandle {
		t.Fatalf("double PayloadRelease = %v", err)
	}
	for _, core := range []*Core{left, right} {
		shutdownAndDrain(t, core)
		if err := core.Destroy(); err != nil {
			t.Fatalf("Destroy: %v", err)
		}
	}
}

func shutdownAndDrain(t *testing.T, core *Core) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- core.Shutdown(context.Background()) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Shutdown: %v", err)
			}
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, completionErr := core.NextCompletion(ctx)
		cancel()
		if completionErr != nil && publicCode(completionErr) != v1.ErrorCodeDeliveryTimeout && publicCode(completionErr) != v1.ErrorCodeShutdownInProgress {
			t.Fatalf("drain completion: %v", completionErr)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, eventErr := core.NextEvent(ctx)
		cancel()
		if eventErr != nil && publicCode(eventErr) != v1.ErrorCodeDeliveryTimeout && publicCode(eventErr) != v1.ErrorCodeShutdownInProgress {
			t.Fatalf("drain event: %v", eventErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("shutdown did not complete")
		}
	}
}

func publicCode(err error) v1.ErrorCode {
	var public *v1.Error
	if errors.As(err, &public) {
		return public.Code
	}
	return ""
}
