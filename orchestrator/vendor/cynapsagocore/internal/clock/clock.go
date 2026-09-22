// Package clock provides injectable time for deterministic state-machine tests.
package clock

import "time"

// Timer is a clock-owned one-shot timer. Stop releases an active timer and is
// safe to call more than once.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock abstracts current time and cancellable timers.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
	NewTimer(time.Duration) Timer
}

// Real uses the process wall clock.
type Real struct{}

// Now returns current wall-clock time.
func (Real) Now() time.Time {
	return time.Now()
}

// After returns a timer channel for the requested duration. Callers that need
// to release a timer before it fires must use NewTimer.
func (Real) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// NewTimer returns a cancellable process timer.
func (Real) NewTimer(d time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(d)}
}

type realTimer struct {
	timer *time.Timer
}

func (t *realTimer) C() <-chan time.Time { return t.timer.C }
func (t *realTimer) Stop() bool          { return t.timer.Stop() }
