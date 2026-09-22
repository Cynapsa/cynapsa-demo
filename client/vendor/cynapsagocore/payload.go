package cynapsagocore

import (
	"context"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

// PayloadOpen allocates an opaque local handle for incremental payload creation.
func (c *Core) PayloadOpen(ctx context.Context) (v1.PayloadHandle, error) {
	if err := c.beginOperation(sdkboundary.FailurePayload, false); err != nil {
		return "", err
	}
	defer c.end()
	if ctx == nil {
		return "", c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailurePayload)
	}
	if err := ctx.Err(); err != nil {
		return "", c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	if c.payloads == nil {
		return "", c.boundary.MapFailure(payload.ErrInvalidLimits, sdkboundary.FailurePayload)
	}
	handle, err := c.payloads.Open()
	if err != nil {
		return "", c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return v1.PayloadHandle(handle), nil
}

// PayloadWrite appends one byte chunk to an open local payload handle.
func (c *Core) PayloadWrite(ctx context.Context, handle v1.PayloadHandle, chunk []byte) (int, error) {
	if err := c.beginOperation(sdkboundary.FailurePayload, false); err != nil {
		return 0, err
	}
	defer c.end()
	if ctx == nil {
		return 0, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailurePayload)
	}
	if err := ctx.Err(); err != nil {
		return 0, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	if c.payloads == nil {
		return 0, c.boundary.MapFailure(payload.ErrInvalidHandle, sdkboundary.FailurePayload)
	}
	if err := c.payloads.Write(string(handle), chunk); err != nil {
		return 0, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return len(chunk), nil
}

// PayloadFinish seals an open payload and returns its final application-visible size.
func (c *Core) PayloadFinish(ctx context.Context, handle v1.PayloadHandle) (int64, error) {
	if err := c.beginOperation(sdkboundary.FailurePayload, false); err != nil {
		return 0, err
	}
	defer c.end()
	if ctx == nil {
		return 0, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailurePayload)
	}
	if err := ctx.Err(); err != nil {
		return 0, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	if c.payloads == nil {
		return 0, c.boundary.MapFailure(payload.ErrInvalidHandle, sdkboundary.FailurePayload)
	}
	if err := c.payloads.Finish(string(handle)); err != nil {
		return 0, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	size, err := c.payloads.Size(string(handle))
	if err != nil {
		return 0, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return size, nil
}

// PayloadRead reads a bounded range from a completed local payload handle.
func (c *Core) PayloadRead(ctx context.Context, handle v1.PayloadHandle, offset int64, limit int) ([]byte, bool, error) {
	if err := c.beginOperation(sdkboundary.FailurePayload, false); err != nil {
		return nil, false, err
	}
	defer c.end()
	if ctx == nil {
		return nil, false, c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailurePayload)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	if c.payloads == nil {
		return nil, false, c.boundary.MapFailure(payload.ErrInvalidHandle, sdkboundary.FailurePayload)
	}
	chunk, eof, err := c.payloads.Read(string(handle), offset, limit)
	if err != nil {
		return nil, false, c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	return chunk, eof, nil
}

// PayloadCancel abandons an unfinished local payload handle.
func (c *Core) PayloadCancel(ctx context.Context, handle v1.PayloadHandle) error {
	return c.payloadMutation(ctx, handle, (*payload.HandleStore).Cancel)
}

// PayloadRetain adds one local ownership reference to a payload handle.
func (c *Core) PayloadRetain(ctx context.Context, handle v1.PayloadHandle) error {
	return c.payloadMutation(ctx, handle, (*payload.HandleStore).Retain)
}

// PayloadRelease removes one local ownership reference from a payload handle.
func (c *Core) PayloadRelease(ctx context.Context, handle v1.PayloadHandle) error {
	return c.payloadMutation(ctx, handle, (*payload.HandleStore).Release)
}

func (c *Core) payloadMutation(ctx context.Context, handle v1.PayloadHandle, operation func(*payload.HandleStore, string) error) error {
	if err := c.beginOperation(sdkboundary.FailurePayload, false); err != nil {
		return err
	}
	defer c.end()
	if ctx == nil {
		return c.boundary.MapFailure(sdkboundary.ErrMalformedInput, sdkboundary.FailurePayload)
	}
	if err := ctx.Err(); err != nil {
		return c.boundary.MapFailure(err, sdkboundary.FailurePayload)
	}
	if c.payloads == nil {
		return c.boundary.MapFailure(payload.ErrInvalidHandle, sdkboundary.FailurePayload)
	}
	return publicFailure(c.boundary, operation(c.payloads, string(handle)), sdkboundary.FailurePayload)
}
