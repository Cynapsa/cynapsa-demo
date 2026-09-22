package rank2xmpp

import (
	"context"
	"sync"
	"time"
)

const maximumSetupPhaseTimeout = 30 * time.Second

// setupCandidate owns one unpublished session while the ordered connection
// phases run. A cancellation callback may retire it, but commit is permitted
// only after every callback has been stopped and joined by runSetupPhase.
type setupCandidate struct {
	mu        sync.Mutex
	session   Session
	committed bool
	closed    bool
	closeWait time.Duration
}

type setupAborter interface {
	AbortSetup(context.Context) error
}

func newSetupCandidate(session Session, closeWait time.Duration) *setupCandidate {
	if closeWait <= 0 || closeWait > maximumSetupPhaseTimeout {
		closeWait = maximumSetupPhaseTimeout
	}
	return &setupCandidate{session: session, closeWait: closeWait}
}

func (candidate *setupCandidate) abort() {
	if candidate == nil {
		return
	}
	candidate.mu.Lock()
	if candidate.committed || candidate.closed || candidate.session == nil {
		candidate.mu.Unlock()
		return
	}
	candidate.closed = true
	session := candidate.session
	wait := candidate.closeWait
	candidate.mu.Unlock()
	operation, cancel := context.WithTimeout(context.Background(), wait)
	if aborter, ok := session.(setupAborter); ok {
		_ = aborter.AbortSetup(operation)
	} else {
		done := make(chan struct{})
		go func() {
			_ = closeSession(session, operation)
			close(done)
		}()
		select {
		case <-done:
		case <-operation.Done():
		}
	}
	cancel()
}

func (candidate *setupCandidate) commit() bool {
	if candidate == nil {
		return false
	}
	candidate.mu.Lock()
	defer candidate.mu.Unlock()
	if candidate.closed || candidate.committed || candidate.session == nil {
		return false
	}
	candidate.committed = true
	return true
}

// setupAttempt owns one absolute clean-session lifetime from the first network
// phase through calibration, catch-up, proof consumption, and atomic client
// publication. Phase contexts may shorten this lifetime but never replace it.
type setupAttempt struct {
	ctx           context.Context
	cancel        context.CancelFunc
	candidate     *setupCandidate
	session       Session
	identity      Authenticated
	proof         []byte
	callbackDone  chan struct{}
	stopCallback  func() bool
	finishedMutex sync.Mutex
	finished      bool
}

func newSetupAttempt(ctx context.Context, cancel context.CancelFunc, maximum time.Duration, session Session) *setupAttempt {
	attempt := &setupAttempt{
		ctx:          ctx,
		cancel:       cancel,
		candidate:    newSetupCandidate(session, boundedSetupPhaseTimeout(maximum)),
		session:      session,
		callbackDone: make(chan struct{}),
	}
	attempt.stopCallback = context.AfterFunc(ctx, func() {
		attempt.candidate.abort()
		close(attempt.callbackDone)
	})
	return attempt
}

func (attempt *setupAttempt) setIdentity(identity Authenticated, proof []byte) {
	attempt.identity = identity
	attempt.proof = proof
}

func (attempt *setupAttempt) clearProof() {
	if attempt == nil {
		return
	}
	clear(attempt.proof)
	attempt.proof = nil
}

// finish stops or joins cancellation and returns its exact terminal cause.
// A commit=true call holds the Client publication lock and transfers candidate
// ownership exactly once; commit=false terminally retires it.
func (attempt *setupAttempt) finish(commit bool) error {
	if attempt == nil {
		return ErrUnavailable
	}
	attempt.finishedMutex.Lock()
	defer attempt.finishedMutex.Unlock()
	if attempt.finished {
		return ErrUnavailable
	}
	attempt.finished = true
	if !attempt.stopCallback() {
		<-attempt.callbackDone
	}
	result := ErrUnavailable
	if contextErr := exactContextError(attempt.ctx); contextErr != nil {
		result = contextErr
	} else if commit && attempt.candidate.commit() {
		result = nil
	}
	if result != nil {
		attempt.candidate.abort()
	}
	attempt.clearProof()
	attempt.cancel()
	return result
}

func boundedSetupPhaseTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return 0
	}
	if configured > maximumSetupPhaseTimeout {
		return maximumSetupPhaseTimeout
	}
	return configured
}

// runSetupPhase layers a per-phase deadline beneath the complete setup
// deadline. Cancellation closes the unpublished candidate to interrupt
// dependencies that are blocked in a socket/XML read despite ignoring ctx.
// The callback is always stopped or joined before this function returns.
func runSetupPhase(overall, caller context.Context, maximum time.Duration, candidate *setupCandidate, operation func(context.Context) error) error {
	if overall == nil || caller == nil || candidate == nil || operation == nil || maximum <= 0 {
		return ErrInvalidConfig
	}
	if err := setupContextError(caller, overall, nil); err != nil {
		candidate.abort()
		return err
	}
	phase, cancel := context.WithTimeout(overall, maximum)
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(phase, func() {
		candidate.abort()
		close(callbackDone)
	})
	err := operation(phase)
	if !stop() {
		<-callbackDone
	}
	contextErr := setupContextError(caller, overall, phase)
	cancel()
	if contextErr != nil {
		candidate.abort()
		return contextErr
	}
	return err
}

func setupContextError(caller, overall, phase context.Context) error {
	if err := exactContextError(caller); err != nil {
		return err
	}
	if err := exactContextError(overall); err != nil {
		return err
	}
	return exactContextError(phase)
}

func exactContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
