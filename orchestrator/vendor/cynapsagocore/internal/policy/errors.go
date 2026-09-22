package policy

import "errors"

var (
	ErrInvalidRule   = errors.New("policy: invalid rule")
	ErrAmbiguousRule = errors.New("policy: conflicting rule selector")
	ErrRuleCapacity  = errors.New("policy: rule capacity exhausted")
	ErrInvalidGate   = errors.New("policy: invalid gate configuration")
	ErrIntegrity     = errors.New("policy: envelope integrity rejected")
	ErrProvenance    = errors.New("policy: carrier provenance rejected")
	// ErrCredential remains an internal compatibility alias for owner tests
	// while ADR 0006 removes credential proof from production authorization.
	ErrCredential              = ErrProvenance
	ErrClock                   = errors.New("policy: calibrated clock rejected envelope time")
	ErrIdentity                = errors.New("policy: authenticated identity mismatch")
	ErrMesh                    = errors.New("policy: mesh scope rejected")
	ErrMembership              = errors.New("policy: current membership required")
	ErrApplicationDenied       = errors.New("policy: application action denied")
	ErrMaterializationRejected = errors.New("policy: canonical payload materialization rejected")
	ErrInvalidPermit           = errors.New("policy: invalid inbound permit")
)
