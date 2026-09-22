package main

import (
	"context"
	"sync"
)

// callbackState owns callback admission and quiescence. Actual host invocation
// remains a Stage B ABI concern.
type callbackState struct {
	mu        sync.Mutex
	accepting bool
	inFlight  int
	quiet     chan struct{}
}

func newCallbackState() *callbackState {
	return &callbackState{accepting: true, quiet: make(chan struct{})}
}

func (s *callbackState) begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting {
		return false
	}
	s.inFlight++
	return true
}

func (s *callbackState) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight == 0 {
		panic("callbackState.end without begin")
	}
	s.inFlight--
	if !s.accepting && s.inFlight == 0 {
		select {
		case <-s.quiet:
		default:
			close(s.quiet)
		}
	}
}

// endAndCloseAdmission terminalizes callback admission at the same
// linearization point that releases the dispatcher's current in-flight claim.
// It is used for an internal delivery-commit panic: no later prepared job may
// begin while cancellation and producer joining proceed.
func (s *callbackState) endAndCloseAdmission() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight == 0 {
		panic("callbackState.endAndCloseAdmission without begin")
	}
	s.accepting = false
	s.inFlight--
	if s.inFlight == 0 {
		select {
		case <-s.quiet:
		default:
			close(s.quiet)
		}
	}
}

func (s *callbackState) clear(ctx context.Context) error {
	s.mu.Lock()
	s.accepting = false
	if s.inFlight == 0 {
		select {
		case <-s.quiet:
		default:
			close(s.quiet)
		}
	}
	quiet := s.quiet
	s.mu.Unlock()
	select {
	case <-quiet:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
