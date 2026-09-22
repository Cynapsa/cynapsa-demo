package mesh

import "errors"

var (
	ErrInvalidCredential = errors.New("mesh: invalid session credential")
	ErrCredentialMissing = errors.New("mesh: session credential unavailable")
	ErrInvalidIdentity   = errors.New("mesh: invalid identity")
	ErrIdentityBinding   = errors.New("mesh: authenticated identity binding rejected")
	ErrInvalidConfig     = errors.New("mesh: invalid session configuration")
	ErrDirectoryCapacity = errors.New("mesh: directory capacity exhausted")
	ErrIdentityUnknown   = errors.New("mesh: identity unavailable")
	ErrSnapshotInvalid   = errors.New("mesh: invalid trusted snapshot")
	ErrSnapshotStale     = errors.New("mesh: trusted snapshot expired")
	ErrMembershipMissing = errors.New("mesh: current membership required")
	ErrHandlerInvalid    = errors.New("mesh: invalid handler path")
	ErrHandlerCapacity   = errors.New("mesh: handler capacity exhausted")
)
