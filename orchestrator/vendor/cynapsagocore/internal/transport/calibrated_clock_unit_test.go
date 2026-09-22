package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCalibratedClockUsesMidpointAndMonotonicElapsedTime(t *testing.T) {
	local := time.Date(2026, 8, 13, 12, 0, 0, 0, time.Local)
	clock := NewCalibratedClock(func() time.Time { return local })
	if got := clock.Now(); !got.IsZero() {
		t.Fatalf("uncalibrated Now = %s", got)
	}
	server := time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC)
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		local = local.Add(2 * time.Millisecond)
		return server, nil
	}); err != nil {
		t.Fatal(err)
	}

	want := server.Add(time.Millisecond)
	if got := clock.Now(); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("Now = %s, want %s UTC", got, want)
	}
	local = local.Add(5 * time.Second)
	if got := clock.Now(); !got.Equal(want.Add(5 * time.Second)) {
		t.Fatalf("advanced Now = %s", got)
	}
	snapshot, ok := clock.Snapshot()
	if !ok || snapshot.Uncertainty != time.Millisecond+time.Microsecond || !snapshot.UTC.Equal(want.Add(5*time.Second)) {
		t.Fatalf("snapshot = %#v, ok=%t", snapshot, ok)
	}
}

func TestCalibratedClockFailurePreservesPreviousGeneration(t *testing.T) {
	local := time.Unix(100, 0)
	clock := NewCalibratedClock(func() time.Time { return local })
	server := time.Unix(200, 0).UTC()
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) { return server, nil }); err != nil {
		t.Fatal(err)
	}
	before, _ := clock.Snapshot()
	canary := errors.New("PRIVATE_TIME_SERVER_CANARY")
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) { return time.Time{}, canary }); !errors.Is(err, ErrReceive) || err.Error() == canary.Error() {
		t.Fatalf("query error = %v", err)
	}
	after, ok := clock.Snapshot()
	if !ok || after != before {
		t.Fatalf("failed calibration changed snapshot: before=%#v after=%#v", before, after)
	}
}

func TestCalibratedClockRejectsExcessiveUncertainty(t *testing.T) {
	local := time.Unix(100, 0)
	clock := NewCalibratedClock(func() time.Time { return local })
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		local = local.Add(2 * MaximumClockUncertainty)
		return time.Unix(200, 0).UTC(), nil
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("excessive uncertainty error = %v", err)
	}
	if _, ok := clock.Snapshot(); ok {
		t.Fatal("excessive uncertainty committed")
	}
}

func TestCalibratedClockUncertaintyUsesCeilingAndAcceptsExactMaximum(t *testing.T) {
	local := time.Unix(100, 0)
	clock := NewCalibratedClock(func() time.Time { return local })
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		local = local.Add(time.Nanosecond)
		return time.Unix(200, 0).UTC(), nil
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := clock.Snapshot()
	if !ok || snapshot.Uncertainty != time.Microsecond+time.Nanosecond {
		t.Fatalf("odd RTT snapshot = %#v, ready=%t", snapshot, ok)
	}

	local = time.Unix(300, 0)
	clock = NewCalibratedClock(func() time.Time { return local })
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		local = local.Add(2 * (MaximumClockUncertainty - calibrationResolution))
		return time.Unix(400, 0).UTC(), nil
	}); err != nil {
		t.Fatalf("exact maximum rejected: %v", err)
	}
	if snapshot, ok = clock.Snapshot(); !ok || snapshot.Uncertainty != MaximumClockUncertainty {
		t.Fatalf("maximum snapshot = %#v, ready=%t", snapshot, ok)
	}
}

func TestCalibratedClockRejectsMalformedSamplesAndContainsPanic(t *testing.T) {
	local := time.Unix(100, 0)
	tests := []struct {
		name  string
		query ServerTimeQuery
	}{
		{name: "zero", query: func(context.Context) (time.Time, error) { return time.Time{}, nil }},
		{name: "non UTC", query: func(context.Context) (time.Time, error) {
			return time.Unix(200, 0).In(time.FixedZone("other", 3600)), nil
		}},
		{name: "panic", query: func(context.Context) (time.Time, error) { panic("PRIVATE_TIME_PANIC_CANARY") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := NewCalibratedClock(func() time.Time { return local })
			if err := clock.Calibrate(context.Background(), test.query); err == nil {
				t.Fatal("invalid calibration succeeded")
			}
			if _, ok := clock.Snapshot(); ok {
				t.Fatal("invalid calibration committed")
			}
		})
	}
}

func TestCalibratedClockLatchesWallMonotonicDiscontinuityUntilRecalibration(t *testing.T) {
	monotonic := time.Now()
	wall := monotonic.Round(0)
	clock := newCalibratedClockWithSource(func() localClockSample {
		return localClockSample{monotonic: monotonic, wall: wall}
	})
	server := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		monotonic = monotonic.Add(2 * time.Millisecond)
		wall = wall.Add(2 * time.Millisecond)
		return server, nil
	}); err != nil {
		t.Fatal(err)
	}
	invalidated := clock.Invalidation()
	monotonic = monotonic.Add(time.Millisecond)
	wall = wall.Add(time.Millisecond + MaximumClockUncertainty + time.Nanosecond)
	if got := clock.Now(); !got.IsZero() {
		t.Fatalf("Now after discontinuity = %s", got)
	}
	select {
	case <-invalidated:
	default:
		t.Fatal("discontinuity did not publish invalidation")
	}
	select {
	case <-clock.Ready():
		t.Fatal("invalidated clock remained ready")
	default:
	}

	// Returning wall time to the prior relationship cannot silently revive a
	// clock once a discontinuity has been observed.
	wall = wall.Add(-MaximumClockUncertainty - time.Nanosecond)
	if _, ok := clock.Snapshot(); ok {
		t.Fatal("latched invalid clock revived without calibration")
	}
	if err := clock.Calibrate(context.Background(), func(context.Context) (time.Time, error) {
		monotonic = monotonic.Add(2 * time.Millisecond)
		wall = wall.Add(2 * time.Millisecond)
		return server.Add(time.Second), nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-clock.Ready():
	default:
		t.Fatal("authenticated recalibration did not restore readiness")
	}
	if got := clock.Now(); got.IsZero() {
		t.Fatal("recalibrated clock is unavailable")
	}
}
