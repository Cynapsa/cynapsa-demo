package sdkboundary

import (
	"context"
	"errors"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/delivery"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// FailureOperation is a closed private discriminator used when a facade
// operation fails before an internal model.Error exists. It prevents callers
// from supplying arbitrary public error text or stages.
type FailureOperation uint8

const (
	FailureCommand FailureOperation = iota + 1
	FailureDeliveryWait
	FailurePayload
	FailureShutdown
)

// MapAdmission projects the command gate's local admission result without
// forwarding its private reason strings or error values.
func (a *Adapter) MapAdmission(admission commandgate.Admission, cause error) (v1.Admission, error) {
	if err := validateProjectedIdentifier("command_id", admission.CommandID); err != nil {
		return v1.Admission{}, err
	}
	if admission.Accepted {
		if cause != nil || admission.Reason != "" {
			return v1.Admission{}, malformed("inconsistent admission")
		}
		handle := v1.CommandHandle(admission.CommandHandle)
		if err := validateCommandHandle(handle); err != nil {
			return v1.Admission{}, err
		}
		return v1.Admission{
			CommandID:     v1.CommandID(admission.CommandID),
			CommandHandle: handle,
			Accepted:      true,
		}, nil
	}
	if admission.CommandHandle != "" || admission.Reason == "" {
		return v1.Admission{}, malformed("inconsistent admission")
	}
	public := v1.Admission{CommandID: v1.CommandID(admission.CommandID)}
	public.Error = admissionError(admission.Reason, cause)
	return public, nil
}

func admissionError(reason string, cause error) *v1.Error {
	private := &model.Error{Code: "command_error", Stage: "command", Location: "local"}
	switch reason {
	case commandgate.ReasonCapacityReached, commandgate.ReasonQueueFull:
		private.Code = "queue_full"
		private.Retryable = true
	case commandgate.ReasonContextCancelled:
		private.Code = "request_cancelled"
	case commandgate.ReasonShuttingDown:
		private.Code = "shutdown_in_progress"
		private.Stage = "shutdown"
	case commandgate.ReasonInvalidCommandID, commandgate.ReasonDuplicate, commandgate.ReasonHandleUnavailable:
		// The stable command_error projection deliberately does not disclose
		// which private registry check rejected the admission.
	default:
		// Unknown private reasons fail to the same bounded generic projection.
	}
	private.Cause = cause
	adapter := Adapter{}
	return adapter.MapError(private)
}

// MapFailure maps a facade/runtime failure through a closed operation class.
// It never formats cause or returns dependency text.
func (a *Adapter) MapFailure(cause error, operation FailureOperation) *v1.Error {
	if cause == nil {
		return nil
	}
	private := &model.Error{Code: "core_error", Stage: "command", Location: "local"}
	switch operation {
	case FailureDeliveryWait:
		private.Stage = "delivery"
	case FailurePayload:
		private.Stage = "payload"
	case FailureShutdown:
		private.Stage = "shutdown"
	}

	switch {
	case errors.Is(cause, ErrUnsupportedVersion):
		private.Code = "unsupported_version"
		private.Stage = "sdk"
	case errors.Is(cause, ErrMalformedInput), errors.Is(cause, ErrInputTooLarge):
		private.Code = "malformed_input"
		private.Stage = "sdk"
	case errors.Is(cause, ErrInvalidHandle),
		errors.Is(cause, commandgate.ErrCommandNotFound),
		errors.Is(cause, commandgate.ErrEmptyCommandHandle),
		errors.Is(cause, commandgate.ErrAlreadyTerminal),
		errors.Is(cause, commandgate.ErrCommandCompleted),
		errors.Is(cause, payload.ErrInvalidHandle),
		errors.Is(cause, payload.ErrInvalidHandleState),
		errors.Is(cause, payload.ErrReferenceOverflow):
		private.Code = "invalid_handle"
	case errors.Is(cause, commandgate.ErrCommandQueueFull),
		errors.Is(cause, commandgate.ErrRegistryFull),
		errors.Is(cause, commandgate.ErrCommandBytesExceeded),
		errors.Is(cause, delivery.ErrQueueFull),
		errors.Is(cause, payload.ErrHandleCapacity),
		errors.Is(cause, payload.ErrQueueFull):
		private.Code = "queue_full"
		private.Retryable = true
	case errors.Is(cause, commandgate.ErrGateClosing),
		errors.Is(cause, commandgate.ErrGateClosed),
		errors.Is(cause, delivery.ErrClosing),
		errors.Is(cause, delivery.ErrClosed),
		errors.Is(cause, coreruntime.ErrClosing),
		errors.Is(cause, coreruntime.ErrClosed):
		private.Code = "shutdown_in_progress"
		private.Stage = "shutdown"
	case errors.Is(cause, payload.ErrPayloadTooLarge), errors.Is(cause, payload.ErrInvalidLimits):
		private.Code = "payload_too_large"
		private.Stage = "payload"
	case errors.Is(cause, payload.ErrIntegrity):
		private.Code = "payload_integrity_failed"
		private.Stage = "payload"
	case errors.Is(cause, payload.ErrMalformedCanonical),
		errors.Is(cause, payload.ErrUnsupportedVersion),
		errors.Is(cause, payload.ErrUnsupportedVariant),
		errors.Is(cause, payload.ErrNonCanonical),
		errors.Is(cause, payload.ErrFrameTooLarge),
		errors.Is(cause, payload.ErrInlineLimitExceeded),
		errors.Is(cause, payload.ErrInvalidManifest),
		errors.Is(cause, payload.ErrInvalidChunk),
		errors.Is(cause, payload.ErrChunkConflict),
		errors.Is(cause, payload.ErrTransferExists),
		errors.Is(cause, payload.ErrTransferNotFound),
		errors.Is(cause, payload.ErrTransferIncomplete),
		errors.Is(cause, payload.ErrTransferCompleted),
		errors.Is(cause, payload.ErrTransferExpired),
		errors.Is(cause, payload.ErrReassemblyQuota),
		errors.Is(cause, payload.ErrCarrierUnavailable),
		errors.Is(cause, payload.ErrCarrierUnsupported),
		errors.Is(cause, payload.ErrCarrierRejected),
		errors.Is(cause, payload.ErrCarrierTimeout),
		errors.Is(cause, payload.ErrCarrierUpload),
		errors.Is(cause, payload.ErrCarrierMaterialization),
		errors.Is(cause, payload.ErrEnvelopePublication),
		errors.Is(cause, payload.ErrAmbiguousCompletion),
		errors.Is(cause, payload.ErrAllCarriersFailed),
		errors.Is(cause, payload.ErrAuthentication),
		errors.Is(cause, payload.ErrEncryption),
		errors.Is(cause, payload.ErrWorkerClosed),
		errors.Is(cause, payload.ErrWorkerPanic):
		private.Code = "payload_transfer_failed"
		private.Stage = "payload"
	case errors.Is(cause, context.DeadlineExceeded):
		switch operation {
		case FailureDeliveryWait:
			private.Code = "delivery_timeout"
			private.Stage = "delivery"
		case FailureShutdown:
			private.Code = "shutdown_timeout"
			private.Stage = "shutdown"
		case FailurePayload:
			private.Stage = "payload"
		default:
			// Command and unknown operation classes retain the bounded generic
			// fallback rather than claiming that delivery timed out.
			private.Stage = "command"
		}
	case errors.Is(cause, context.Canceled), errors.Is(cause, commandgate.ErrCommandCancelled):
		private.Code = "request_cancelled"
	default:
		// Unknown internal errors deliberately remain core_error.
	}
	private.Cause = cause
	return a.MapError(private)
}
