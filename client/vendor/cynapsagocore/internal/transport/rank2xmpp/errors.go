package rank2xmpp

import "errors"

var (
	ErrInvalidConfig       = errors.New("durable transport: invalid configuration")
	ErrAuthentication      = errors.New("durable transport: authentication failed")
	ErrIdentityBinding     = errors.New("durable transport: identity binding failed")
	ErrStreamManagement    = errors.New("durable transport: required reliability unavailable")
	ErrUnavailable         = errors.New("durable transport: unavailable")
	ErrQueueFull           = errors.New("durable transport: queue full")
	ErrCapacity            = errors.New("durable transport: capacity exceeded")
	ErrClosed              = errors.New("durable transport: closed")
	ErrProtocol            = errors.New("durable transport: protocol violation")
	ErrAuthorityRejected   = errors.New("durable transport: authority continuity rejected")
	ErrCompletionAmbiguous = errors.New("durable transport: completion ambiguous")
)
