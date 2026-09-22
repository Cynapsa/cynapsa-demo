package sdkboundary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

func TestMapAdmissionIsExplicitAndFailsClosed(t *testing.T) {
	adapter, _ := New()
	handle, err := NewCommandHandle([16]byte{1}, 1)
	if err != nil {
		t.Fatalf("NewCommandHandle: %v", err)
	}
	accepted, err := adapter.MapAdmission(commandgate.Admission{
		CommandID: "command-1", CommandHandle: commandgate.CommandHandle(handle), Accepted: true,
	}, nil)
	if err != nil || !accepted.Accepted || accepted.CommandHandle != v1.CommandHandle(handle) || accepted.Error != nil {
		t.Fatalf("accepted = %#v, %v", accepted, err)
	}

	for _, test := range []struct {
		reason string
		code   v1.ErrorCode
	}{
		{commandgate.ReasonQueueFull, v1.ErrorCodeQueueFull},
		{commandgate.ReasonContextCancelled, v1.ErrorCodeRequestCancelled},
		{commandgate.ReasonShuttingDown, v1.ErrorCodeShutdownInProgress},
		{"new_private_reason", v1.ErrorCodeCommand},
	} {
		mapped, mapErr := adapter.MapAdmission(commandgate.Admission{CommandID: "command-1", Reason: test.reason}, errors.New("secret dependency text"))
		if mapErr != nil || mapped.Accepted || mapped.Error == nil || mapped.Error.Code != test.code {
			t.Fatalf("reason %q = %#v, %v", test.reason, mapped, mapErr)
		}
		if strings.Contains(mapped.Error.Message, "secret") || strings.Contains(mapped.Error.Message, test.reason) {
			t.Fatalf("private text leaked: %#v", mapped.Error)
		}
	}

	if _, err := adapter.MapAdmission(commandgate.Admission{CommandID: "command-1", Accepted: true}, nil); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("accepted without handle = %v", err)
	}
	if _, err := adapter.MapAdmission(commandgate.Admission{CommandID: "command-1"}, nil); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("rejected without reason = %v", err)
	}
}

func TestMapFailureIsBoundedAndNeverFormatsCause(t *testing.T) {
	adapter, _ := New()
	for _, test := range []struct {
		cause     error
		operation FailureOperation
		code      v1.ErrorCode
		stage     v1.ErrorStage
	}{
		{ErrMalformedInput, FailureCommand, v1.ErrorCodeMalformedInput, v1.ErrorStageSDK},
		{ErrUnsupportedVersion, FailureCommand, v1.ErrorCodeUnsupportedVersion, v1.ErrorStageSDK},
		{commandgate.ErrCommandNotFound, FailureCommand, v1.ErrorCodeInvalidHandle, v1.ErrorStageCommand},
		{commandgate.ErrRegistryFull, FailureCommand, v1.ErrorCodeQueueFull, v1.ErrorStageCommand},
		{payload.ErrPayloadTooLarge, FailurePayload, v1.ErrorCodePayloadTooLarge, v1.ErrorStagePayload},
		{payload.ErrMalformedCanonical, FailurePayload, v1.ErrorCodePayloadTransferFailed, v1.ErrorStagePayload},
		{payload.ErrCarrierUpload, FailurePayload, v1.ErrorCodePayloadTransferFailed, v1.ErrorStagePayload},
		{payload.ErrIntegrity, FailurePayload, v1.ErrorCodePayloadIntegrityFailed, v1.ErrorStagePayload},
		{commandgate.ErrCommandCompleted, FailureCommand, v1.ErrorCodeInvalidHandle, v1.ErrorStageCommand},
		{context.DeadlineExceeded, FailureDeliveryWait, v1.ErrorCodeDeliveryTimeout, v1.ErrorStageDelivery},
		{fmt.Errorf("private wrapped deadline: %w", context.DeadlineExceeded), FailureShutdown, v1.ErrorCodeShutdownTimeout, v1.ErrorStageShutdown},
		{fmt.Errorf("private wrapped deadline: %w", context.DeadlineExceeded), FailureCommand, v1.ErrorCodeCore, v1.ErrorStageCommand},
		{fmt.Errorf("private wrapped deadline: %w", context.DeadlineExceeded), FailurePayload, v1.ErrorCodeCore, v1.ErrorStagePayload},
		{fmt.Errorf("private wrapped deadline: %w", context.DeadlineExceeded), FailureOperation(255), v1.ErrorCodeCore, v1.ErrorStageCommand},
		{errors.Join(payload.ErrIntegrity, context.DeadlineExceeded), FailurePayload, v1.ErrorCodePayloadIntegrityFailed, v1.ErrorStagePayload},
		{errors.New("private canary"), FailureCommand, v1.ErrorCodeCore, v1.ErrorStageCommand},
	} {
		mapped := adapter.MapFailure(test.cause, test.operation)
		if mapped == nil || mapped.Code != test.code || mapped.Stage != test.stage || mapped.Location != v1.ErrorLocationLocal {
			t.Fatalf("MapFailure(%v) = %#v", test.cause, mapped)
		}
		if strings.Contains(mapped.Message, "private") || strings.Contains(mapped.Message, "canary") || strings.Contains(mapped.Message, "wrapped") {
			t.Fatalf("private cause leaked: %#v", mapped)
		}
	}
	if adapter.MapFailure(nil, FailureCommand) != nil {
		t.Fatal("nil failure did not map to nil")
	}
}

func TestMapFailureDeadlineMatrixForDirectSingleAndMultipleWrapping(t *testing.T) {
	adapter, _ := New()
	operations := []struct {
		name      string
		operation FailureOperation
		code      v1.ErrorCode
		stage     v1.ErrorStage
	}{
		{"command", FailureCommand, v1.ErrorCodeCore, v1.ErrorStageCommand},
		{"delivery", FailureDeliveryWait, v1.ErrorCodeDeliveryTimeout, v1.ErrorStageDelivery},
		{"payload", FailurePayload, v1.ErrorCodeCore, v1.ErrorStagePayload},
		{"shutdown", FailureShutdown, v1.ErrorCodeShutdownTimeout, v1.ErrorStageShutdown},
	}
	causes := []struct {
		name  string
		cause error
	}{
		{"direct", context.DeadlineExceeded},
		{"single_wrap", fmt.Errorf("private deadline detail: %w", context.DeadlineExceeded)},
		{"multiple_wrap", fmt.Errorf("private outer detail: %w", fmt.Errorf("private inner detail: %w", context.DeadlineExceeded))},
	}
	for _, operation := range operations {
		for _, cause := range causes {
			t.Run(operation.name+"/"+cause.name, func(t *testing.T) {
				mapped := adapter.MapFailure(cause.cause, operation.operation)
				if mapped == nil || mapped.Code != operation.code || mapped.Stage != operation.stage || mapped.Location != v1.ErrorLocationLocal {
					t.Fatalf("MapFailure(%v, %d) = %#v", cause.cause, operation.operation, mapped)
				}
				if strings.Contains(mapped.Message, "private") || strings.Contains(mapped.Message, "detail") || strings.Contains(mapped.Message, "deadline exceeded") {
					t.Fatalf("private wrapped deadline leaked: %#v", mapped)
				}
			})
		}
	}
}

func TestMapFailureTypedErrorsPrecedeJoinedAndWrappedDeadline(t *testing.T) {
	adapter, _ := New()
	for _, test := range []struct {
		name      string
		cause     error
		operation FailureOperation
		code      v1.ErrorCode
		stage     v1.ErrorStage
	}{
		{
			name: "command",
			cause: fmt.Errorf("private outer: %w", errors.Join(
				fmt.Errorf("private typed: %w", commandgate.ErrCommandNotFound),
				fmt.Errorf("private deadline: %w", context.DeadlineExceeded),
			)),
			operation: FailureCommand,
			code:      v1.ErrorCodeInvalidHandle,
			stage:     v1.ErrorStageCommand,
		},
		{
			name: "payload",
			cause: fmt.Errorf("private outer: %w", errors.Join(
				fmt.Errorf("private typed: %w", payload.ErrIntegrity),
				fmt.Errorf("private deadline: %w", context.DeadlineExceeded),
			)),
			operation: FailurePayload,
			code:      v1.ErrorCodePayloadIntegrityFailed,
			stage:     v1.ErrorStagePayload,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mapped := adapter.MapFailure(test.cause, test.operation)
			if mapped == nil || mapped.Code != test.code || mapped.Stage != test.stage {
				t.Fatalf("MapFailure(%v) = %#v", test.cause, mapped)
			}
			if strings.Contains(mapped.Message, "private") || strings.Contains(mapped.Message, "deadline") {
				t.Fatalf("private joined cause leaked: %#v", mapped)
			}
		})
	}
}

func TestDecodeCommandRejectsNilContextWithoutPanic(t *testing.T) {
	adapter, _ := New()
	_, err := adapter.DecodeCommand(nil, v1.CoreStatusCommand{CommandBase: v1.CommandBase{CommandID: "command-1", SDKSessionID: "session-1"}})
	if !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("DecodeCommand(nil) = %v", err)
	}
}
