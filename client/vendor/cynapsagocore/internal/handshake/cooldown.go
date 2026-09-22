package handshake

import (
	"sync"
	"time"
)

// Cooldown tracks caller-configured bounded exponential backoff. Values are
// configuration, not a new wire-level constant.
type Cooldown struct {
	mu      sync.Mutex
	initial time.Duration
	maximum time.Duration
	current time.Duration
	next    time.Time
	jitter  func(time.Duration) time.Duration
}

func NewCooldown(initial, maximum time.Duration, jitter func(time.Duration) time.Duration) (*Cooldown, error) {
	if initial <= 0 || maximum < initial {
		return nil, ErrInvalidConfig
	}
	return &Cooldown{initial: initial, maximum: maximum, jitter: jitter}, nil
}

func (c *Cooldown) Ready(now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !now.Before(c.next)
}

// Next resets on success or advances bounded exponential delay on failure.
func (c *Cooldown) Next(now time.Time, success bool) time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if success {
		c.current = 0
		c.next = time.Time{}
		return c.next
	}
	if c.current == 0 {
		c.current = c.initial
	} else if c.current >= c.maximum/2 {
		c.current = c.maximum
	} else {
		c.current *= 2
	}
	delay := c.current
	if c.jitter != nil {
		candidate := c.jitter(delay)
		if candidate >= 0 && candidate <= c.maximum {
			delay = candidate
		}
	}
	c.next = now.Add(delay)
	return c.next
}

func (c *Cooldown) Snapshot() (time.Duration, time.Time) {
	if c == nil {
		return 0, time.Time{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.next
}
