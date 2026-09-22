package peer

import "errors"

var (
	ErrInvalidConfig  = errors.New("peer: invalid configuration")
	ErrClosed         = errors.New("peer: worker closed")
	ErrQueueFull      = errors.New("peer: queue full")
	ErrEpochExhausted = errors.New("peer: member epoch exhausted")
	ErrUnavailable    = errors.New("peer: no usable path")
	ErrUnauthorized   = errors.New("peer: current mesh membership required")
)
