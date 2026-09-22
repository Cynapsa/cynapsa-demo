package transport

import (
	"context"
	"sync"
	"time"
)

const (
	calibrationResolution   = time.Microsecond
	MaximumClockUncertainty = time.Second
)

// Clock is the private time source shared by connectivity components. A
// CalibratedClock remains unready until an authenticated server sample is
// committed; its Now method then advances only from the local monotonic clock.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a deterministic clock function for private composition and
// tests. Production authenticated graphs use one *CalibratedClock instance.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time {
	if f == nil {
		return time.Time{}
	}
	return f()
}

// ServerTimeQuery obtains one authenticated UTC sample. Implementations must
// honor ctx and return only after the complete response has been validated.
type ServerTimeQuery func(context.Context) (time.Time, error)

// CalibrationSnapshot is immutable private calibration evidence. Uncertainty
// is the measured half round-trip plus the one-microsecond server resolution
// allowance. V1 does not yet use it as a peer acceptance policy.
type CalibrationSnapshot struct {
	UTC         time.Time
	Uncertainty time.Duration
}

// CalibratedClock converts authenticated server UTC into a monotonic-backed
// process clock without changing the operating-system clock.
type CalibratedClock struct {
	calibrate sync.Mutex
	mu        sync.RWMutex
	source    localClockSource
	localMono time.Time
	localWall time.Time
	utc       time.Time
	uncert    time.Duration
	gen       uint64
	invalid   bool
	invalidCh chan struct{}
	readyCh   chan struct{}
}

type localClockSample struct {
	monotonic time.Time
	wall      time.Time
}

type localClockSource func() localClockSample

// NewCalibratedClock creates an unready clock. localNow is injectable only for
// deterministic private tests; production passes nil and uses time.Now.
func NewCalibratedClock(localNow func() time.Time) *CalibratedClock {
	if localNow == nil {
		localNow = time.Now
	}
	return newCalibratedClockWithSource(func() localClockSample {
		now := localNow()
		return localClockSample{monotonic: now, wall: now.Round(0)}
	})
}

func newCalibratedClockWithSource(source localClockSource) *CalibratedClock {
	return &CalibratedClock{source: source, invalidCh: make(chan struct{}), readyCh: make(chan struct{})}
}

// Calibrate measures one authenticated query at its local send and receive
// boundaries and atomically commits the midpoint estimate. A failed query or
// invalid sample leaves the previous calibration unchanged.
func (c *CalibratedClock) Calibrate(ctx context.Context, query ServerTimeQuery) (err error) {
	if c == nil || ctx == nil || query == nil {
		return ErrInvalidConfig
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	c.calibrate.Lock()
	defer c.calibrate.Unlock()
	sent := c.measure()
	serverUTC, err := callServerTimeQuery(ctx, query)
	received := c.measure()
	if err != nil {
		return classify(err, ctx)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = validateCalibrationSample(sent, received, serverUTC); err != nil {
		return err
	}
	rtt := received.monotonic.Sub(sent.monotonic)
	if rtt > time.Duration(1<<63-1)-calibrationResolution {
		return ErrProtocol
	}
	halfRoundTrip := rtt / 2
	if rtt%2 != 0 {
		halfRoundTrip++
	}
	uncertainty := halfRoundTrip + calibrationResolution
	if uncertainty > MaximumClockUncertainty {
		return ErrProtocol
	}
	serverAtReceive := serverUTC.Add(rtt / 2)
	if serverAtReceive.Year() < 1 || serverAtReceive.Year() > 9999 {
		return ErrProtocol
	}

	c.mu.Lock()
	c.localMono = received.monotonic
	c.localWall = received.wall
	c.utc = serverAtReceive.UTC()
	c.uncert = uncertainty
	c.gen++
	if c.invalid {
		c.invalid = false
		c.invalidCh = make(chan struct{})
	}
	if !channelClosed(c.readyCh) {
		close(c.readyCh)
	}
	c.mu.Unlock()
	return nil
}

// Now returns zero until the first calibration succeeds. Afterwards it uses
// only elapsed monotonic local time added to the authenticated UTC anchor.
func (c *CalibratedClock) Now() time.Time {
	now, _, ok := c.current()
	if !ok {
		return time.Time{}
	}
	return now
}

// Snapshot returns current typed calibration evidence and false while the
// clock is uncalibrated or its local monotonic source has become invalid.
func (c *CalibratedClock) Snapshot() (CalibrationSnapshot, bool) {
	if c == nil {
		return CalibrationSnapshot{}, false
	}
	now, uncertainty, ok := c.current()
	if !ok {
		return CalibrationSnapshot{}, false
	}
	return CalibrationSnapshot{UTC: now, Uncertainty: uncertainty}, true
}

// Invalidation closes when a previously calibrated clock detects a local
// wall/monotonic discontinuity. Consumers use this only to demand an
// authenticated Rank 2 re-establishment; wall time never adjusts UTC.
func (c *CalibratedClock) Invalidation() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	c.mu.RLock()
	result := c.invalidCh
	c.mu.RUnlock()
	return result
}

// Ready closes after a successful calibration. A discontinuity replaces it
// with a new open channel until authenticated re-establishment recalibrates.
func (c *CalibratedClock) Ready() <-chan struct{} {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	result := c.readyCh
	c.mu.RUnlock()
	return result
}

func (c *CalibratedClock) current() (time.Time, time.Duration, bool) {
	if c == nil {
		return time.Time{}, 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.invalid || c.gen == 0 || c.localMono.IsZero() || c.localWall.IsZero() || c.utc.IsZero() || c.source == nil {
		return time.Time{}, 0, false
	}
	sample := callLocalClockSource(c.source)
	if sample.monotonic.IsZero() || sample.wall.IsZero() {
		c.invalidateLocked()
		return time.Time{}, 0, false
	}
	monotonicElapsed := sample.monotonic.Sub(c.localMono)
	wallElapsed := sample.wall.Sub(c.localWall)
	if elapsedDiverged(monotonicElapsed, wallElapsed, MaximumClockUncertainty) {
		c.invalidateLocked()
		return time.Time{}, 0, false
	}
	result := c.utc.Add(monotonicElapsed).UTC()
	if result.Year() < 1 || result.Year() > 9999 {
		c.invalidateLocked()
		return time.Time{}, 0, false
	}
	return result, c.uncert, true
}

func (c *CalibratedClock) invalidateLocked() {
	if c.invalid {
		return
	}
	c.invalid = true
	if !channelClosed(c.invalidCh) {
		close(c.invalidCh)
	}
	c.readyCh = make(chan struct{})
}

func (c *CalibratedClock) measure() localClockSample {
	c.mu.RLock()
	source := c.source
	c.mu.RUnlock()
	return callLocalClockSource(source)
}

func callLocalClockSource(source localClockSource) (sample localClockSample) {
	if source == nil {
		return localClockSample{}
	}
	defer func() {
		if recover() != nil {
			sample = localClockSample{}
		}
	}()
	return source()
}

func elapsedDiverged(monotonic, wall, limit time.Duration) bool {
	if monotonic < 0 || wall < 0 {
		return true
	}
	if monotonic >= wall {
		return monotonic-wall > limit
	}
	return wall-monotonic > limit
}

func channelClosed(channel <-chan struct{}) bool {
	if channel == nil {
		return false
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func callServerTimeQuery(ctx context.Context, query ServerTimeQuery) (server time.Time, err error) {
	defer func() {
		if recover() != nil {
			server = time.Time{}
			err = ErrUnavailable
		}
	}()
	return query(ctx)
}

func validateCalibrationSample(sent, received localClockSample, serverUTC time.Time) error {
	if sent.monotonic.IsZero() || sent.wall.IsZero() || received.monotonic.IsZero() || received.wall.IsZero() || serverUTC.IsZero() || elapsedDiverged(received.monotonic.Sub(sent.monotonic), received.wall.Sub(sent.wall), MaximumClockUncertainty) || serverUTC.Location() != time.UTC || serverUTC.Year() < 1 || serverUTC.Year() > 9999 {
		return ErrProtocol
	}
	return nil
}

var _ Clock = (*CalibratedClock)(nil)
