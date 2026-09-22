package protocol

import "errors"

var (
	ErrMalformedEncoding   = errors.New("protocol: malformed envelope encoding")
	ErrNonCanonical        = errors.New("protocol: non-canonical envelope encoding")
	ErrEnvelopeTooLarge    = errors.New("protocol: envelope exceeds size limit")
	ErrUnsupportedVersion  = errors.New("protocol: unsupported envelope version")
	ErrInvalidEnvelope     = errors.New("protocol: invalid envelope")
	ErrInvalidIdentifier   = errors.New("protocol: invalid identifier")
	ErrInvalidMode         = errors.New("protocol: invalid message mode")
	ErrInvalidPayload      = errors.New("protocol: invalid payload descriptor")
	ErrMissingProof        = errors.New("protocol: missing credential proof")
	ErrIdentifierEntropy   = errors.New("protocol: identifier entropy unavailable")
	ErrIdentifierCollision = errors.New("protocol: identifier collision limit reached")
)

// FieldError identifies a rejected private field without echoing its value.
type FieldError struct {
	Field string
	Cause error
}

func (e *FieldError) Error() string {
	if e == nil {
		return "protocol: invalid field"
	}
	return "protocol: invalid field " + e.Field
}

func (e *FieldError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
