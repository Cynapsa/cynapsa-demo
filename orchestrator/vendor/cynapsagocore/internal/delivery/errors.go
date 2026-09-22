package delivery

import "errors"

var (
	ErrInvalidCapacity    = errors.New("delivery: capacity must be positive")
	ErrNilContext         = errors.New("delivery: nil context")
	ErrInvalidEvent       = errors.New("delivery: invalid event")
	ErrQueueFull          = errors.New("delivery: event queue capacity reached")
	ErrQueueEmpty         = errors.New("delivery: event queue is empty")
	ErrConsumerBusy       = errors.New("delivery: another event consumer is waiting")
	ErrInvalidLease       = errors.New("delivery: invalid event lease")
	ErrLeaseRetired       = errors.New("delivery: event lease was retired")
	ErrAdmissionExhausted = errors.New("delivery: tracked admission identity exhausted")
	ErrNotClosing         = errors.New("delivery: shutdown has not started")
	ErrClosing            = errors.New("delivery: shutdown in progress")
	ErrClosed             = errors.New("delivery: closed")
)
