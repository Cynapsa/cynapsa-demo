package clock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAcceptanceFakeClockExactBoundariesAndEqualDeadlineCancellation(t *testing.T) {
	start := time.Unix(1_700_000_000, 123)
	fake := NewFake(start)

	const timerCount = 512
	timers := make([]Timer, timerCount)
	for index := range timers {
		timers[index] = fake.NewTimer(time.Second)
	}
	for index := 0; index < timerCount; index += 3 {
		if !timers[index].Stop() {
			t.Fatalf("timer %d was not active", index)
		}
	}

	if err := fake.Advance(time.Second - time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	for index, timer := range timers {
		select {
		case fired := <-timer.C():
			t.Fatalf("timer %d fired early at %v", index, fired)
		default:
		}
	}

	if err := fake.Advance(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	wantDeadline := start.Add(time.Second)
	for index, timer := range timers {
		select {
		case fired := <-timer.C():
			if index%3 == 0 {
				t.Fatalf("cancelled timer %d fired", index)
			}
			if !fired.Equal(wantDeadline) {
				t.Fatalf("timer %d fired at %v, want %v", index, fired, wantDeadline)
			}
		default:
			if index%3 != 0 {
				t.Fatalf("timer %d did not fire at its exact deadline", index)
			}
		}
	}
	if pending := fake.Pending(); pending != 0 {
		t.Fatalf("Pending() = %d after exact-boundary advance", pending)
	}
	if err := fake.Advance(-time.Nanosecond); !errors.Is(err, ErrNegativeAdvance) {
		t.Fatalf("backward Advance() error = %v", err)
	}
}

func TestAcceptanceFakeClockConcurrentCreateStopAdvanceRemainsBounded(t *testing.T) {
	fake := NewFake(time.Unix(10, 0))
	const workers = 64
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)
	for worker := range workers {
		worker := worker
		go func() {
			defer group.Done()
			<-start
			timer := fake.NewTimer(time.Duration(worker%4+1) * time.Second)
			if worker%2 == 0 {
				timer.Stop()
			}
			_ = fake.Now()
			_ = fake.Pending()
		}()
	}
	close(start)
	group.Wait()
	if err := fake.Advance(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if pending := fake.Pending(); pending != 0 {
		t.Fatalf("Pending() = %d after large advance", pending)
	}
}
