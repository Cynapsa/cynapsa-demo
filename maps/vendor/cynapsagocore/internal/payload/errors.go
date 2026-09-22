package payload

import (
	"context"
	"errors"
)

var (
	ErrInvalidLimits          = errors.New("payload: invalid limits")
	ErrMalformedCanonical     = errors.New("payload: malformed canonical payload")
	ErrUnsupportedVersion     = errors.New("payload: unsupported canonical version")
	ErrUnsupportedVariant     = errors.New("payload: unsupported canonical variant")
	ErrNonCanonical           = errors.New("payload: non-canonical payload encoding")
	ErrPayloadTooLarge        = errors.New("payload: payload exceeds configured limit")
	ErrFrameTooLarge          = errors.New("payload: encoded carrier frame exceeds configured limit")
	ErrInlineLimitExceeded    = errors.New("payload: inline limit exceeded")
	ErrInvalidHandle          = errors.New("payload: invalid or foreign handle")
	ErrInvalidHandleState     = errors.New("payload: invalid handle state")
	ErrHandleCapacity         = errors.New("payload: handle capacity exhausted")
	ErrReferenceOverflow      = errors.New("payload: handle reference count exhausted")
	ErrIntegrity              = errors.New("payload: integrity verification failed")
	ErrInvalidManifest        = errors.New("payload: invalid transfer manifest")
	ErrInvalidChunk           = errors.New("payload: invalid transfer chunk")
	ErrChunkConflict          = errors.New("payload: conflicting duplicate chunk")
	ErrTransferExists         = errors.New("payload: transfer already exists")
	ErrTransferNotFound       = errors.New("payload: transfer not found")
	ErrTransferIncomplete     = errors.New("payload: transfer is incomplete")
	ErrTransferCompleted      = errors.New("payload: transfer is already completed")
	ErrTransferExpired        = errors.New("payload: transfer expired")
	ErrReassemblyQuota        = errors.New("payload: reassembly quota exhausted")
	ErrCarrierUnavailable     = errors.New("payload: carrier unavailable")
	ErrCarrierUnsupported     = errors.New("payload: carrier unsupported")
	ErrCarrierRejected        = errors.New("payload: carrier rejected transfer")
	ErrCarrierTimeout         = errors.New("payload: carrier timed out")
	ErrCarrierUpload          = errors.New("payload: object upload failed")
	ErrCarrierMaterialization = errors.New("payload: remote materialization failed")
	ErrEnvelopePublication    = errors.New("payload: logical envelope publication failed")
	ErrPublicationRejected    = errors.New("payload: logical envelope publication rejected")
	ErrAuthorization          = errors.New("payload: local authorization rejected")
	ErrPublicationInternal    = errors.New("payload: logical envelope publication internal failure")
	ErrAmbiguousCompletion    = errors.New("payload: carrier completion is ambiguous")
	ErrAllCarriersFailed      = errors.New("payload: all permitted carriers failed")
	ErrAuthentication         = errors.New("payload: authentication failed")
	ErrEncryption             = errors.New("payload: encryption failed")
	ErrQueueFull              = errors.New("payload: transfer worker queue full")
	ErrWorkerClosed           = errors.New("payload: transfer worker closed")
	ErrWorkerPanic            = errors.New("payload: transfer worker panic contained")
)

// dependencyContextError preserves cancellation class without retaining any
// wrapped dependency text, URLs, credentials, or other private canaries.
func dependencyContextError(ctx context.Context, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}
