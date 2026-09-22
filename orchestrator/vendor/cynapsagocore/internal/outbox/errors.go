package outbox

import "errors"

var (
	ErrInvalidConfig     = errors.New("outbox: invalid configuration")
	ErrCapacity          = errors.New("outbox: capacity exhausted")
	ErrDuplicateConflict = errors.New("outbox: conflicting message identity")
	ErrUnknownEntry      = errors.New("outbox: entry unavailable")
	ErrOwnedEntry        = errors.New("outbox: entry is owned by a carrier")
	ErrHandledRegression = errors.New("outbox: handled-through evidence regressed")
	ErrInvalidEvidence   = errors.New("outbox: invalid terminal evidence")
	ErrOrdinalExhausted  = errors.New("outbox: ordinal exhausted")
)
