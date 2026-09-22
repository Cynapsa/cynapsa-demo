package clock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestFakeClockOrdersAndFiresTimersAtDeadlines(t *testing.T) {
	start := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	fake := NewFake(start)
	later := fake.NewTimer(2 * time.Second)
	firstEqual := fake.NewTimer(time.Second)
	secondEqual := fake.NewTimer(time.Second)

	if fake.Pending() != 3 {
		t.Fatalf("Pending() = %d, want 3", fake.Pending())
	}
	if err := fake.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	for index, timer := range []Timer{firstEqual, secondEqual} {
		select {
		case got := <-timer.C():
			if want := start.Add(time.Second); !got.Equal(want) {
				t.Fatalf("equal timer %d fired at %v, want %v", index, got, want)
			}
		default:
			t.Fatalf("equal timer %d did not fire", index)
		}
	}
	select {
	case <-later.C():
		t.Fatal("later timer fired early")
	default:
	}
	if fake.Pending() != 1 {
		t.Fatalf("Pending() = %d, want 1", fake.Pending())
	}
	if err := fake.Advance(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := <-later.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("later timer fired at %v", got)
	}
	if !fake.Now().Equal(start.Add(24*time.Hour + time.Second)) {
		t.Fatalf("Now() = %v", fake.Now())
	}
}

func TestFakeClockTimerCancellationAndImmediateTimers(t *testing.T) {
	start := time.Unix(100, 0)
	fake := NewFake(start)
	cancelled := fake.NewTimer(time.Hour)
	if !cancelled.Stop() || cancelled.Stop() {
		t.Fatal("Stop() did not report active then inactive")
	}
	if fake.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", fake.Pending())
	}
	if err := fake.Advance(2 * time.Hour); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled.C():
		t.Fatal("cancelled timer fired")
	default:
	}

	immediate := fake.NewTimer(0)
	if got := <-immediate.C(); !got.Equal(fake.Now()) {
		t.Fatalf("immediate timer fired at %v, want %v", got, fake.Now())
	}
	if immediate.Stop() {
		t.Fatal("fired timer reported active")
	}
}

func TestFakeClockRejectsBackwardAdvanceWithoutMutation(t *testing.T) {
	start := time.Unix(200, 0)
	fake := NewFake(start)
	if err := fake.Advance(-time.Nanosecond); !errors.Is(err, ErrNegativeAdvance) {
		t.Fatalf("Advance() error = %v, want ErrNegativeAdvance", err)
	}
	if !fake.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", fake.Now(), start)
	}
}

func TestRealClockImplementsClockAndCancellableTimer(t *testing.T) {
	var process Clock = Real{}
	before := time.Now()
	now := process.Now()
	if now.Before(before) || now.After(time.Now()) {
		t.Fatalf("Now() = %v outside call interval", now)
	}
	timer := process.NewTimer(time.Hour)
	if !timer.Stop() {
		t.Fatal("new real timer was not active")
	}
}

func TestFakeClockConcurrentStopAndAdvanceIsRaceSafe(t *testing.T) {
	fake := NewFake(time.Unix(300, 0))
	const count = 64
	timers := make([]Timer, count)
	stopped := make([]bool, count)
	for index := range timers {
		timers[index] = fake.NewTimer(time.Second)
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := 0; index < count; index += 2 {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			stopped[index] = timers[index].Stop()
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		if err := fake.Advance(time.Second); err != nil {
			t.Errorf("Advance() error = %v", err)
		}
	}()
	close(start)
	group.Wait()
	if fake.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", fake.Pending())
	}
	for index, timer := range timers {
		select {
		case <-timer.C():
			if stopped[index] {
				t.Fatalf("timer %d fired after successful Stop", index)
			}
		default:
			if !stopped[index] {
				t.Fatalf("timer %d neither fired nor stopped", index)
			}
		}
	}
}
