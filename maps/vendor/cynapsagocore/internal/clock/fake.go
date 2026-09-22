package clock

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

var ErrNegativeAdvance = errors.New("clock: cannot move fake time backwards")

// Fake is a manually advanced deterministic test clock. Timers with equal
// deadlines fire in creation order.
type Fake struct {
	mu        sync.Mutex
	now       time.Time
	nextOrder uint64
	timers    timerHeap
}

// NewFake creates a fake clock at the supplied instant.
func NewFake(now time.Time) *Fake {
	return &Fake{now: now}
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After registers a one-shot fake timer. Callers that need cancellation must
// use NewTimer.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.NewTimer(d).C()
}

// NewTimer registers a cancellable one-shot fake timer.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextOrder++
	timer := &fakeTimer{
		clock:    f,
		channel:  make(chan time.Time, 1),
		deadline: f.now.Add(d),
		order:    f.nextOrder,
		index:    -1,
		active:   true,
	}
	if d <= 0 {
		timer.active = false
		timer.channel <- f.now
		return timer
	}
	heap.Push(&f.timers, timer)
	return timer
}

// Advance moves fake time forward and fires every due timer. Timer channels
// are buffered, so advancing never depends on a consumer goroutine.
func (f *Fake) Advance(d time.Duration) error {
	if d < 0 {
		return ErrNegativeAdvance
	}

	f.mu.Lock()
	f.now = f.now.Add(d)
	for f.timers.Len() > 0 {
		timer := f.timers[0]
		if timer.deadline.After(f.now) {
			break
		}
		heap.Pop(&f.timers)
		if !timer.active {
			continue
		}
		timer.active = false
		timer.channel <- timer.deadline
	}
	f.mu.Unlock()
	return nil
}

// Pending returns the number of active fake timers.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timers.Len()
}

type fakeTimer struct {
	clock    *Fake
	channel  chan time.Time
	deadline time.Time
	order    uint64
	index    int
	active   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.channel }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	if t.index >= 0 {
		heap.Remove(&t.clock.timers, t.index)
	}
	return true
}

type timerHeap []*fakeTimer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].order < h[j].order
	}
	return h[i].deadline.Before(h[j].deadline)
}
func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *timerHeap) Push(value any) {
	timer := value.(*fakeTimer)
	timer.index = len(*h)
	*h = append(*h, timer)
}
func (h *timerHeap) Pop() any {
	old := *h
	last := len(old) - 1
	timer := old[last]
	old[last] = nil
	timer.index = -1
	*h = old[:last]
	return timer
}
