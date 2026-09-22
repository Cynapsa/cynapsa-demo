package sdkboundary

import "errors"

var (
	ErrMalformedInput     = errors.New("malformed public input")
	ErrInvalidHandle      = errors.New("invalid public handle")
	ErrUnsupportedVersion = errors.New("unsupported public schema version")
	ErrInputTooLarge      = errors.New("public input exceeds its limit")
)
