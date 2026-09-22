package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type qaCalibrationSource struct {
	mu        sync.Mutex
	monotonic time.Time
	wall      time.Time
}

func (source *qaCalibrationSource) sample() localClockSample {
	source.mu.Lock()
	defer source.mu.Unlock()
	return localClockSample{monotonic: source.monotonic, wall: source.wall}
}

func (source *qaCalibrationSource) advance(monotonic, wall time.Duration) {
	source.mu.Lock()
	source.monotonic = source.monotonic.Add(monotonic)
	source.wall = source.wall.Add(wall)
	source.mu.Unlock()
}

func TestQACalibrationMidpointUncertaintyCeilingAndAtomicFailure(t *testing.T) {
	local := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	source := &qaCalibrationSource{monotonic: local, wall: local}
	clock := newCalibratedClockWithSource(source.sample)
	serverUTC := time.Date(2031, 2, 3, 4, 5, 6, 123456000, time.UTC)
	const roundTrip = 1999 * time.Microsecond
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		source.advance(roundTrip, roundTrip)
		return serverUTC, nil
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := clock.Snapshot()
	wantUTC := serverUTC.Add(roundTrip / 2)
	wantUncertainty := roundTrip/2 + calibrationResolution
	if !ok || !snapshot.UTC.Equal(wantUTC) || snapshot.UTC.Location() != time.UTC || snapshot.Uncertainty != wantUncertainty {
		t.Fatalf("snapshot=%#v ready=%t, want UTC=%s U=%s", snapshot, ok, wantUTC, wantUncertainty)
	}

	// A sample just beyond the one-second uncertainty cap must not replace the
	// prior authenticated calibration. The old clock remains monotonic-live.
	const excessiveRoundTrip = 2 * time.Second
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		source.advance(excessiveRoundTrip, excessiveRoundTrip)
		return serverUTC.Add(time.Hour), nil
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("excessive uncertainty error=%v", err)
	}
	after, ok := clock.Snapshot()
	if !ok || after.Uncertainty != wantUncertainty || !after.UTC.Equal(wantUTC.Add(excessiveRoundTrip)) {
		t.Fatalf("failed calibration changed prior clock: before=%#v after=%#v ready=%t", snapshot, after, ok)
	}
}

func TestQAClockDiscontinuityLatchesAcrossConcurrentReadersUntilRecalibration(t *testing.T) {
	local := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	source := &qaCalibrationSource{monotonic: local, wall: local}
	clock := newCalibratedClockWithSource(source.sample)
	serverUTC := time.Date(2032, 3, 4, 5, 6, 7, 654321000, time.UTC)
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		source.advance(2*time.Millisecond, 2*time.Millisecond)
		return serverUTC, nil
	}); err != nil {
		t.Fatal(err)
	}
	invalidated := clock.Invalidation()
	source.advance(time.Millisecond, time.Millisecond+MaximumClockUncertainty+time.Nanosecond)

	const readers = 32
	results := make(chan bool, readers)
	var group sync.WaitGroup
	for range readers {
		group.Add(1)
		go func() {
			defer group.Done()
			_, ok := clock.Snapshot()
			results <- ok
		}()
	}
	group.Wait()
	close(results)
	for ready := range results {
		if ready {
			t.Fatal("discontinuous clock remained ready")
		}
	}
	if !channelClosed(invalidated) || channelClosed(clock.Ready()) || !clock.Now().IsZero() {
		t.Fatal("discontinuity did not latch invalid/unready state")
	}

	// Returning wall time to agreement is not authentication. Only a new
	// server query may revive the clock and close the replacement Ready channel.
	source.advance(time.Millisecond, -MaximumClockUncertainty+time.Millisecond-time.Nanosecond)
	if _, ok := clock.Snapshot(); ok {
		t.Fatal("latched clock revived without authenticated calibration")
	}
	ready := clock.Ready()
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		source.advance(2*time.Millisecond, 2*time.Millisecond)
		return serverUTC.Add(time.Minute), nil
	}); err != nil {
		t.Fatal(err)
	}
	if !channelClosed(ready) {
		t.Fatal("successful recalibration did not publish readiness")
	}
	if snapshot, ok := clock.Snapshot(); !ok || snapshot.UTC.IsZero() || snapshot.Uncertainty <= 0 || snapshot.Uncertainty > MaximumClockUncertainty {
		t.Fatalf("recalibrated snapshot=%#v ready=%t", snapshot, ok)
	}
}
