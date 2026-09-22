package rank2xmpp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type qaSerializedRecoverySession struct {
	*qaLifecycleSession
	firstEntered chan struct{}
	firstRelease chan struct{}
	enterOnce    sync.Once
	cooperative  bool
	firstResumed bool
	resumeCalls  atomic.Int32
	active       atomic.Int32
	maximum      atomic.Int32
	queries      atomic.Int32
}

func newQASerializedRecoverySession(cooperative bool) *qaSerializedRecoverySession {
	return &qaSerializedRecoverySession{
		qaLifecycleSession: newQALifecycleSession(),
		firstEntered:       make(chan struct{}),
		firstRelease:       make(chan struct{}),
		cooperative:        cooperative,
		firstResumed:       true,
	}
}

func (s *qaSerializedRecoverySession) Resume(ctx context.Context) (bool, error) {
	call := s.resumeCalls.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maximum.Load()
		if active <= maximum || s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	if call == 1 {
		s.enterOnce.Do(func() { close(s.firstEntered) })
		if s.cooperative {
			select {
			case <-s.firstRelease:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		} else {
			<-s.firstRelease
		}
		return s.firstResumed, nil
	}
	return true, nil
}

func (s *qaSerializedRecoverySession) QueryServerTime(context.Context) (time.Time, error) {
	s.queries.Add(1)
	return qaAuthenticatedServerTime, nil
}

func qaRecoveryResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("recovery operation did not terminate")
		return nil
	}
}

func TestQAConcurrentResumeWaitersNeverCrossCompoundCalibrationFence(t *testing.T) {
	session := newQASerializedRecoverySession(false)
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	first := make(chan error, 1)
	go func() {
		resumed, err := client.Resume(context.Background())
		if err == nil && !resumed {
			err = errors.New("resume returned false")
		}
		first <- err
	}()
	<-session.firstEntered

	const waiters = 16
	waiterResults := make(chan error, waiters)
	launched := make(chan struct{}, waiters)
	cancels := make([]context.CancelFunc, 0, waiters)
	for range waiters {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func() {
			launched <- struct{}{}
			_, err := client.Resume(ctx)
			waiterResults <- err
		}()
	}
	for range waiters {
		<-launched
	}
	for _, cancel := range cancels {
		cancel()
	}
	for range waiters {
		if err := qaRecoveryResult(t, waiterResults); !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting Resume error=%v", err)
		}
	}
	if calls := session.resumeCalls.Load(); calls != 1 {
		t.Fatalf("dependency Resume calls before owner release=%d", calls)
	}
	close(session.firstRelease)
	if err := qaRecoveryResult(t, first); err != nil {
		t.Fatalf("owning Resume error=%v", err)
	}
	if maximum, queries := session.maximum.Load(), session.queries.Load(); maximum != 1 || queries != 1 {
		t.Fatalf("maximum concurrent Resume=%d time queries=%d", maximum, queries)
	}
}

func TestQAPublicResumeAndReconnectCannotOverlapCalibrationOwnership(t *testing.T) {
	session := newQASerializedRecoverySession(false)
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	public := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		public <- err
	}()
	<-session.firstEntered

	reconnectStarted := make(chan struct{})
	reconnected := make(chan bool, 1)
	go func() {
		close(reconnectStarted)
		reconnected <- client.reconnect()
	}()
	<-reconnectStarted
	close(session.firstRelease)
	if err := qaRecoveryResult(t, public); err != nil {
		t.Fatalf("public Resume error=%v", err)
	}
	select {
	case ok := <-reconnected:
		if !ok {
			t.Fatal("serialized reconnect failed")
		}
	case <-time.After(time.Second):
		t.Fatal("serialized reconnect did not terminate")
	}
	if calls, maximum, queries := session.resumeCalls.Load(), session.maximum.Load(), session.queries.Load(); calls != 2 || maximum != 1 || queries != 2 {
		t.Fatalf("Resume calls=%d maximum concurrency=%d time queries=%d", calls, maximum, queries)
	}
}

func TestQACloseCancelsRecoveryWaiterAndFencesLateGeneration(t *testing.T) {
	session := newQASerializedRecoverySession(false)
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	client.ctx, client.cancel = lifetime, cancelLifetime
	client.mu.Lock()
	generation := client.generation
	client.mu.Unlock()

	owner := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		owner <- err
	}()
	<-session.firstEntered
	waiterContext := &qaObservedContext{Context: context.Background(), observed: make(chan struct{})}
	waiter := make(chan error, 1)
	go func() {
		_, err := client.Resume(waiterContext)
		waiter <- err
	}()
	<-waiterContext.observed
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close error=%v", err)
	}
	if err := qaRecoveryResult(t, waiter); !errors.Is(err, ErrClosed) {
		t.Fatalf("waiting Resume after Close error=%v", err)
	}
	close(session.firstRelease)
	if err := qaRecoveryResult(t, owner); !errors.Is(err, ErrClosed) {
		t.Fatalf("late owning Resume error=%v", err)
	}
	client.mu.Lock()
	afterGeneration, state, closed := client.generation, client.state, client.closed
	client.mu.Unlock()
	if !closed || afterGeneration != generation+1 || state == DurableLive {
		t.Fatalf("closed=%t generation=%d want=%d state=%v", closed, afterGeneration, generation+1, state)
	}
	if _, ready := client.TimeCalibration(); ready || session.queries.Load() != 0 {
		t.Fatalf("late generation published calibration: ready=%t queries=%d", ready, session.queries.Load())
	}
}

func TestQARecoveryTimeoutReleasesOwnershipForLaterResume(t *testing.T) {
	session := newQASerializedRecoverySession(true)
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	client.config.ReconnectOperationTimeout = 5 * time.Millisecond
	first := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		first <- err
	}()
	<-session.firstEntered
	if err := qaRecoveryResult(t, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out Resume error=%v", err)
	}
	resumed, err := client.Resume(context.Background())
	if err != nil || !resumed {
		t.Fatalf("later Resume resumed=%t error=%v", resumed, err)
	}
	if calls, maximum, queries := session.resumeCalls.Load(), session.maximum.Load(), session.queries.Load(); calls != 2 || maximum != 1 || queries != 1 {
		t.Fatalf("Resume calls=%d maximum concurrency=%d time queries=%d", calls, maximum, queries)
	}
}

type qaResumeResult struct {
	resumed bool
	err     error
}

type qaObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (ctx *qaObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

func TestQACallerCancellationPrecedesLateBenignResumeRejection(t *testing.T) {
	session := newQASerializedRecoverySession(false)
	session.firstResumed = false
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan qaResumeResult, 1)
	go func() {
		resumed, err := client.Resume(ctx)
		result <- qaResumeResult{resumed: resumed, err: err}
	}()
	<-session.firstEntered
	cancel()
	close(session.firstRelease)
	select {
	case got := <-result:
		if got.resumed || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("late rejection resumed=%t error=%v", got.resumed, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Resume did not terminate")
	}
	if session.queries.Load() != 0 || client.DurableState() == DurableLive {
		t.Fatalf("late rejection queried time=%d state=%v", session.queries.Load(), client.DurableState())
	}
}

func TestQACloseGenerationPrecedesLateBenignResumeRejection(t *testing.T) {
	session := newQASerializedRecoverySession(false)
	session.firstResumed = false
	client := qaReconnectClient(t, session, &qaSequenceDialer{})
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	client.ctx, client.cancel = lifetime, cancelLifetime
	client.mu.Lock()
	generation := client.generation
	client.mu.Unlock()
	result := make(chan qaResumeResult, 1)
	go func() {
		resumed, err := client.Resume(context.Background())
		result <- qaResumeResult{resumed: resumed, err: err}
	}()
	<-session.firstEntered
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close error=%v", err)
	}
	close(session.firstRelease)
	select {
	case got := <-result:
		if got.resumed || !errors.Is(got.err, ErrClosed) {
			t.Fatalf("late rejection resumed=%t error=%v", got.resumed, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed Resume did not terminate")
	}
	client.mu.Lock()
	afterGeneration, state := client.generation, client.state
	client.mu.Unlock()
	if afterGeneration != generation+1 || state == DurableLive || session.queries.Load() != 0 {
		t.Fatalf("generation=%d want=%d state=%v time queries=%d", afterGeneration, generation+1, state, session.queries.Load())
	}
}
