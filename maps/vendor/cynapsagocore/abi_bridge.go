package cynapsagocore

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

// DefaultTimeout returns the configured bounded wait used when the native ABI
// receives timeout_ms=0.
func (c *Core) DefaultTimeout() time.Duration {
	if c == nil || c.runtime == nil {
		return 0
	}
	return c.runtime.ConfigSnapshot().CommandTimeout
}

// NewFromABI strictly decodes versioned Core configuration through the same
// adapter instance that owns every later projection for the returned Core.
// Native wrappers use this bridge instead of importing private codecs.
func NewFromABI(data []byte) (*Core, error) {
	boundary, err := sdkboundary.New()
	if err != nil {
		return nil, normalizedCreationError(boundary, err)
	}
	config, err := boundary.DecodeABIConfig(data)
	if err != nil {
		return nil, boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return newCore(config, boundary)
}

// SubmitABI strictly decodes one versioned command, admits it through the
// typed facade, and returns a deterministic versioned admission document.
func (c *Core) SubmitABI(ctx context.Context, data []byte) ([]byte, error) {
	if c == nil {
		return nil, invalidCoreError()
	}
	ownedInput := append([]byte(nil), data...)
	defer clear(ownedInput)
	command, err := c.boundary.DecodeABIInternalCommand(ownedInput)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	defer model.ClearCommand(&command)
	if err := c.beginOperation(sdkboundary.FailureCommand, false); err != nil {
		return nil, err
	}
	defer c.end()
	if ctx == nil {
		return nil, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailureCommand)
	}
	admission, admissionErr := c.gate.SubmitOwned(ctx, &command)
	publicAdmission, err := c.boundary.MapAdmission(admission, admissionErr)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	encoded, err := c.boundary.EncodeABIAdmission(publicAdmission)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return encoded, nil
}

// NextCompletionABI returns the deterministic encoding of the same normalized
// completion consumed by the typed polling surface.
func (c *Core) NextCompletionABI(ctx context.Context) ([]byte, error) {
	completion, err := c.NextCompletion(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := c.boundary.EncodeABICompletion(completion)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return encoded, nil
}

// NextEventABI returns the deterministic encoding of the same normalized event
// consumed by the typed polling surface.
func (c *Core) NextEventABI(ctx context.Context) ([]byte, error) {
	event, err := c.NextEvent(ctx)
	if err != nil {
		return nil, err
	}
	return c.EncodeEventABI(event)
}

// EncodeCompletionABI is the sole strict encoder used after either polling or
// callback consumption of an already normalized completion.
func (c *Core) EncodeCompletionABI(completion v1.Completion) ([]byte, error) {
	encoded, err := c.boundary.EncodeABICompletion(completion)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return encoded, nil
}

// EncodeEventABI is the sole strict encoder used after either polling or
// callback consumption of an already normalized event.
func (c *Core) EncodeEventABI(event v1.Event) ([]byte, error) {
	encoded, err := c.boundary.EncodeABIEvent(event)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return encoded, nil
}

// StatusABI returns a deterministic versioned status document.
func (c *Core) StatusABI(ctx context.Context) ([]byte, error) {
	status, err := c.Status(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := c.boundary.EncodeABIStatus(status)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return encoded, nil
}

func (c *Core) CancelABI(ctx context.Context, data []byte) error {
	handle, err := c.boundary.DecodeABICommandHandle(data)
	if err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	return c.Cancel(ctx, handle)
}

// BeginShutdownABI starts the Core-owned shutdown without waiting. The native
// wrapper uses it before callback quiescence, then joins through Shutdown with
// the host call's own bounded wait context.
func (c *Core) BeginShutdownABI() error { return c.beginShutdown() }

func (c *Core) PayloadOpenABI(ctx context.Context) ([]byte, error) {
	handle, err := c.PayloadOpen(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := c.boundary.EncodeABIPayloadOpen(handle)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return encoded, nil
}

func (c *Core) PayloadWriteABI(ctx context.Context, data []byte) ([]byte, error) {
	handle, chunk, err := c.boundary.DecodeABIPayloadWrite(data)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	accepted, err := c.PayloadWrite(ctx, handle, chunk)
	clear(chunk)
	if err != nil {
		return nil, err
	}
	encoded, err := c.boundary.EncodeABIPayloadWrite(uint64(accepted))
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return encoded, nil
}

func (c *Core) PayloadFinishABI(ctx context.Context, data []byte) ([]byte, error) {
	handle, err := c.boundary.DecodeABIPayloadHandle(data)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	size, err := c.PayloadFinish(ctx, handle)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, c.boundary.MapFailure(fmt.Errorf("%w: payload size", sdkboundary.ErrMalformedInput), sdkboundary.FailurePayload)
	}
	encoded, err := c.boundary.EncodeABIPayloadFinish(handle, uint64(size))
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return encoded, nil
}

func (c *Core) PayloadReadABI(ctx context.Context, data []byte) ([]byte, error) {
	handle, offset, limit, err := c.boundary.DecodeABIPayloadRead(data)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	chunk, eof, err := c.PayloadRead(ctx, handle, int64(offset), int(limit))
	if err != nil {
		return nil, err
	}
	encoded, err := c.boundary.EncodeABIPayloadRead(chunk, eof)
	clear(chunk)
	if err != nil {
		return nil, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return encoded, nil
}

func (c *Core) PayloadCancelABI(ctx context.Context, data []byte) error {
	handle, err := c.boundary.DecodeABIPayloadHandle(data)
	if err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return c.PayloadCancel(ctx, handle)
}

func (c *Core) PayloadRetainABI(ctx context.Context, data []byte) error {
	handle, err := c.boundary.DecodeABIPayloadHandle(data)
	if err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return c.PayloadRetain(ctx, handle)
}

func (c *Core) PayloadReleaseABI(ctx context.Context, data []byte) error {
	handle, err := c.boundary.DecodeABIPayloadHandle(data)
	if err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return c.PayloadRelease(ctx, handle)
}

// EncodeABIError serializes only a normalized public error. Unknown Go errors
// collapse to the generic boundary-owned Core failure without formatting them.
func EncodeABIError(err error) ([]byte, error) {
	boundary, constructionErr := sdkboundary.New()
	if constructionErr != nil {
		return nil, constructionErr
	}
	var public *v1.Error
	if !errors.As(err, &public) || public == nil {
		public = boundary.MapFailure(err, sdkboundary.FailureCommand)
	}
	if public == nil {
		return nil, nil
	}
	return boundary.EncodeABIError(*public)
}
