package clock

import (
	"testing"
	"time"
)

func FuzzFakeClockReferenceModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{1, 1, 1, 0, 3, 4, 2, 5})

	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 512 {
			operations = operations[:512]
		}
		fake := NewFake(time.Unix(0, 0))
		timers := make([]Timer, 0, 64)
		active := make([]bool, 0, 64)
		for _, operation := range operations {
			switch operation % 4 {
			case 0:
				if len(timers) == cap(timers) {
					continue
				}
				timers = append(timers, fake.NewTimer(time.Duration(operation%8+1)*time.Nanosecond))
				active = append(active, true)
			case 1:
				if len(timers) == 0 {
					continue
				}
				index := int(operation) % len(timers)
				stopped := timers[index].Stop()
				if stopped != active[index] {
					t.Fatalf("Stop(%d) = %v, model active=%v", index, stopped, active[index])
				}
				active[index] = false
			case 2:
				advance := time.Duration(operation%8+1) * time.Nanosecond
				before := fake.Now()
				if err := fake.Advance(advance); err != nil {
					t.Fatal(err)
				}
				if got := fake.Now(); !got.Equal(before.Add(advance)) {
					t.Fatalf("Now() = %v, want %v", got, before.Add(advance))
				}
				for index, timer := range timers {
					select {
					case <-timer.C():
						active[index] = false
					default:
					}
				}
			case 3:
				_ = fake.Now()
			}
			wantPending := 0
			for _, isActive := range active {
				if isActive {
					wantPending++
				}
			}
			if got := fake.Pending(); got != wantPending {
				t.Fatalf("Pending() = %d, model=%d", got, wantPending)
			}
		}
	})
}
