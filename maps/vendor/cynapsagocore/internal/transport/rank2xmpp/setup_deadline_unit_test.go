package rank2xmpp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type setupTestPhase uint8

const (
	setupTestConnect setupTestPhase = iota + 1
	setupTestAuthenticate
	setupTestBind
	setupTestStreamManagement
	setupTestAuthorityDiscovery
)

type setupTestSession struct {
	block setupTestPhase
	delay time.Duration
	proof []byte

	mu           sync.Mutex
	entered      chan struct{}
	enteredOnce  sync.Once
	release      chan struct{}
	releaseOnce  sync.Once
	closeCount   int
	phaseCalls   []setupTestPhase
	boundedCalls int
	password     []byte
}

func newSetupTestSession(block setupTestPhase) *setupTestSession {
	return &setupTestSession{block: block, entered: make(chan struct{}), release: make(chan struct{})}
}

func (session *setupTestSession) operation(ctx context.Context, phase setupTestPhase) error {
	session.mu.Lock()
	session.phaseCalls = append(session.phaseCalls, phase)
	if _, ok := ctx.Deadline(); ok {
		session.boundedCalls++
	}
	session.mu.Unlock()
	if session.block != phase {
		if session.delay == 0 {
			return nil
		}
		timer := time.NewTimer(session.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	session.enteredOnce.Do(func() { close(session.entered) })
	// Deliberately ignore ctx. Candidate Close must interrupt the operation.
	<-session.release
	return nil
}

func (session *setupTestSession) ConnectTLS(ctx context.Context, _ string) error {
	return session.operation(ctx, setupTestConnect)
}

func (session *setupTestSession) Authenticate(ctx context.Context, _ string, password []byte) (string, []byte, error) {
	session.mu.Lock()
	session.password = password
	session.mu.Unlock()
	err := session.operation(ctx, setupTestAuthenticate)
	return "a@example.test", append([]byte(nil), session.proof...), err
}

func (session *setupTestSession) BindResource(ctx context.Context, _ string) (string, error) {
	err := session.operation(ctx, setupTestBind)
	return "a@example.test/mesh", err
}

func (session *setupTestSession) EnableStreamManagement(ctx context.Context, _ bool) error {
	return session.operation(ctx, setupTestStreamManagement)
}

func (session *setupTestSession) DiscoverAuthority(ctx context.Context) error {
	return session.operation(ctx, setupTestAuthorityDiscovery)
}

func (*setupTestSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}

func (*setupTestSession) Send(context.Context, Stanza) error { return nil }

func (*setupTestSession) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}

func (*setupTestSession) Resume(context.Context) (bool, error) { return false, ErrUnavailable }

func (*setupTestSession) CatchUp(context.Context, int) ([]Stanza, error) { return nil, nil }

func (session *setupTestSession) Close(context.Context) error {
	session.mu.Lock()
	session.closeCount++
	session.mu.Unlock()
	session.releaseOnce.Do(func() { close(session.release) })
	return nil
}

func (session *setupTestSession) snapshot() (closeCount, boundedCalls int, phases []setupTestPhase, password []byte) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.closeCount, session.boundedCalls, append([]setupTestPhase(nil), session.phaseCalls...), append([]byte(nil), session.password...)
}

func TestSetupDeadlineClosesEveryUnfinishedCandidatePhase(t *testing.T) {
	for _, phase := range []setupTestPhase{setupTestConnect, setupTestAuthenticate, setupTestBind, setupTestStreamManagement, setupTestAuthorityDiscovery} {
		t.Run(setupPhaseName(phase), func(t *testing.T) {
			session := newSetupTestSession(phase)
			client := qaUnstartedClient(t, fakeDialer{session})
			client.config.ReconnectOperationTimeout = 10 * time.Millisecond
			started := time.Now()
			err := client.Start(context.Background())
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Start() = %v, want setup deadline", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("blocked phase returned after %s", elapsed)
			}
			closeCount, bounded, phases, password := session.snapshot()
			if closeCount != 1 {
				t.Fatalf("candidate close count = %d, want 1", closeCount)
			}
			if bounded != len(phases) || len(phases) == 0 || phases[len(phases)-1] != phase {
				t.Fatalf("bounded calls=%d phases=%v", bounded, phases)
			}
			if phase == setupTestAuthenticate {
				for _, value := range password {
					if value != 0 {
						t.Fatal("borrowed authentication bytes survived timeout")
					}
				}
			}
		})
	}
}

func setupPhaseName(phase setupTestPhase) string {
	switch phase {
	case setupTestConnect:
		return "tcp"
	case setupTestAuthenticate:
		return "sasl"
	case setupTestBind:
		return "resource-bind"
	case setupTestStreamManagement:
		return "stream-management"
	case setupTestAuthorityDiscovery:
		return "authority-discovery"
	default:
		return "unknown"
	}
}

func TestSetupCallerCancellationPrecedesCandidateCloseError(t *testing.T) {
	session := newSetupTestSession(setupTestAuthenticate)
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Start(ctx) }()
	<-session.entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() = %v, want caller cancellation", err)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("candidate close count = %d, want 1", closeCount)
	}
}

func TestSetupExpiredBudgetNeverEntersNextPhase(t *testing.T) {
	session := newSetupTestSession(0)
	candidate := newSetupCandidate(session, time.Second)
	overall, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	called := false
	err := runSetupPhase(overall, context.Background(), time.Second, candidate, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("runSetupPhase()=(%v, called=%t), want deadline before operation", err, called)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("expired candidate close count = %d, want 1", closeCount)
	}
}

func TestSetupSuccessDisarmsCallbackBeforeSessionPublication(t *testing.T) {
	session := newSetupTestSession(0)
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * client.config.ReconnectOperationTimeout)
	if closeCount, bounded, phases, _ := session.snapshot(); closeCount != 0 || bounded != 5 || len(phases) != 5 {
		t.Fatalf("published candidate closed=%d bounded=%d phases=%v", closeCount, bounded, phases)
	}
	if _, err := client.AuthenticatedIdentity(); err != nil {
		t.Fatalf("published identity unavailable after stale deadline window: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("terminal close count = %d, want 1", closeCount)
	}
}

type setupSequenceDialer struct {
	mu       sync.Mutex
	sessions []Session
}

type setupCatchUpSession struct {
	*setupTestSession
	catchUpEntered chan struct{}
	catchUpOnce    sync.Once
}

func (session *setupCatchUpSession) CatchUp(context.Context, int) ([]Stanza, error) {
	session.catchUpOnce.Do(func() { close(session.catchUpEntered) })
	<-session.release
	return nil, nil
}

func TestSetupCatchUpRemainsOwnedUntilAtomicPublication(t *testing.T) {
	session := &setupCatchUpSession{setupTestSession: newSetupTestSession(0), catchUpEntered: make(chan struct{})}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want catch-up deadline", err)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("unpublished catch-up candidate close count = %d, want 1", closeCount)
	}
	client.mu.Lock()
	published := client.session != nil || client.started
	client.mu.Unlock()
	if published {
		t.Fatal("timed-out catch-up candidate was published")
	}
}

func TestSetupProofAdmissionIsJoinedByTerminalClose(t *testing.T) {
	session := newSetupTestSession(0)
	session.proof = []byte("short-lived-proof")
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = time.Second
	sinkEntered := make(chan struct{})
	sinkRelease := make(chan struct{})
	client.config.ProofSink = func(context.Context, string, []byte) error {
		close(sinkEntered)
		<-sinkRelease
		return nil
	}
	startResult := make(chan error, 1)
	go func() { startResult <- client.Start(context.Background()) }()
	<-sinkEntered
	closeResult := make(chan error, 1)
	go func() { closeResult <- client.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before admitted proof callback joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(sinkRelease)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	if err := <-startResult; !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() = %v, want closed generation", err)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("Close returned before candidate cleanup: close count=%d", closeCount)
	}
}

func TestSetupProofSinkReceivesAndHonorsAbsoluteAttemptContext(t *testing.T) {
	session := newSetupTestSession(0)
	session.proof = []byte("attempt-proof")
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 50 * time.Millisecond
	sinkEntered := make(chan struct{})
	client.config.ProofSink = func(ctx context.Context, _ string, _ []byte) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("proof context was unbounded")
		}
		close(sinkEntered)
		<-ctx.Done()
		return ctx.Err()
	}
	result := make(chan error, 1)
	go func() { result <- client.Start(context.Background()) }()
	<-sinkEntered
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want proof-phase setup deadline", err)
	}
	if closeCount, _, _, _ := session.snapshot(); closeCount != 1 {
		t.Fatalf("proof timeout candidate close count = %d, want 1", closeCount)
	}
}

type setupNoncooperativeCloseSession struct {
	*setupTestSession
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeDone    chan struct{}
	closeOnce    sync.Once
}

func (session *setupNoncooperativeCloseSession) ConnectTLS(context.Context, string) error {
	<-session.closeEntered
	return nil
}

func (session *setupNoncooperativeCloseSession) Close(context.Context) error {
	session.closeOnce.Do(func() { close(session.closeEntered) })
	<-session.closeRelease
	close(session.closeDone)
	return nil
}

func TestSetupTimeoutDoesNotWaitForeverForInjectedNoncooperativeClose(t *testing.T) {
	session := &setupNoncooperativeCloseSession{
		setupTestSession: newSetupTestSession(0),
		closeEntered:     make(chan struct{}),
		closeRelease:     make(chan struct{}),
		closeDone:        make(chan struct{}),
	}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	started := time.Now()
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want setup deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("noncooperative Close stalled timeout for %s", elapsed)
	}
	close(session.closeRelease)
	select {
	case <-session.closeDone:
	case <-time.After(time.Second):
		t.Fatal("injected Close goroutine did not retire after release")
	}
}

func (dialer *setupSequenceDialer) Dial(context.Context) (Session, error) {
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.sessions) == 0 {
		return nil, ErrUnavailable
	}
	session := dialer.sessions[0]
	dialer.sessions = dialer.sessions[1:]
	return session, nil
}

func TestSetupTimeoutPermitsCleanRetryWithoutReusingCandidate(t *testing.T) {
	first := newSetupTestSession(setupTestStreamManagement)
	second := newSetupTestSession(0)
	client := qaUnstartedClient(t, &setupSequenceDialer{sessions: []Session{first, second}})
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Start() = %v", err)
	}
	client.config.ReconnectOperationTimeout = time.Second
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("retry Start() = %v", err)
	}
	if firstClose, _, _, _ := first.snapshot(); firstClose != 1 {
		t.Fatalf("retired candidate close count = %d", firstClose)
	}
	if secondClose, _, _, _ := second.snapshot(); secondClose != 0 {
		t.Fatalf("fresh candidate was closed before publication: %d", secondClose)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupPhasesShareOneAbsoluteAttemptDeadline(t *testing.T) {
	session := newSetupTestSession(0)
	session.delay = 10 * time.Millisecond
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	started := time.Now()
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want absolute setup deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("phase deadlines reset the attempt budget: %s", elapsed)
	}
	closeCount, _, phases, _ := session.snapshot()
	if closeCount != 1 || len(phases) != 2 {
		t.Fatalf("candidate close=%d phases=%v, want timeout in second phase", closeCount, phases)
	}
}

func TestSetupCloseBeforeProofCannotPublishLateAuthentication(t *testing.T) {
	session := newSetupTestSession(setupTestStreamManagement)
	session.proof = []byte("candidate-only-proof")
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = time.Second
	var sinkCalls int
	client.config.ProofSink = func(context.Context, string, []byte) error {
		sinkCalls++
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- client.Start(context.Background()) }()
	<-session.entered
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("late Start() = %v, want closed", err)
	}
	if sinkCalls != 0 {
		t.Fatalf("proof sink called %d times after setup generation retired", sinkCalls)
	}
}

func TestMelliumCancellationClosesSocketAndJoinsCallback(t *testing.T) {
	candidate, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	release := bindConnectionContext(ctx, candidate)
	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := candidate.Read(buffer)
		readDone <- err
	}()
	if err := <-readDone; !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("blocked read error = %v, want closed socket", err)
	}
	release()
	cancel()
}

type setupDeadlineCaptureSession struct {
	mu              sync.Mutex
	connectDeadline time.Time
	catchDeadline   time.Time
}

func (session *setupDeadlineCaptureSession) ConnectTLS(ctx context.Context, _ string) error {
	deadline, _ := ctx.Deadline()
	session.mu.Lock()
	session.connectDeadline = deadline
	session.mu.Unlock()
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*setupDeadlineCaptureSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return "a@example.test", nil, nil
}
func (*setupDeadlineCaptureSession) BindResource(context.Context, string) (string, error) {
	return "a@example.test/mesh", nil
}
func (*setupDeadlineCaptureSession) EnableStreamManagement(context.Context, bool) error { return nil }
func (*setupDeadlineCaptureSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}
func (*setupDeadlineCaptureSession) Send(context.Context, Stanza) error { return nil }
func (*setupDeadlineCaptureSession) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}
func (*setupDeadlineCaptureSession) Resume(context.Context) (bool, error) { return false, nil }
func (session *setupDeadlineCaptureSession) CatchUp(ctx context.Context, _ int) ([]Stanza, error) {
	deadline, _ := ctx.Deadline()
	session.mu.Lock()
	session.catchDeadline = deadline
	session.mu.Unlock()
	return nil, ErrUnavailable
}
func (*setupDeadlineCaptureSession) Close(context.Context) error { return nil }

func TestSetupUsesOneAbsoluteDeadlineThroughCatchUp(t *testing.T) {
	session := &setupDeadlineCaptureSession{}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 150 * time.Millisecond
	if err := client.Start(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start() = %v, want catch-up failure", err)
	}
	session.mu.Lock()
	connectDeadline, catchDeadline := session.connectDeadline, session.catchDeadline
	session.mu.Unlock()
	if connectDeadline.IsZero() || catchDeadline.IsZero() || !connectDeadline.Equal(catchDeadline) {
		t.Fatalf("attempt deadline reset: connect=%v catch-up=%v", connectDeadline, catchDeadline)
	}
}

type setupBlockedCalibrationSession struct {
	*setupTestSession
	queryEntered chan struct{}
	queryRelease chan struct{}
	abortOnce    sync.Once
}

func (session *setupBlockedCalibrationSession) QueryServerTime(context.Context) (time.Time, error) {
	close(session.queryEntered)
	<-session.queryRelease
	return time.Now().UTC(), nil
}
func (session *setupBlockedCalibrationSession) AbortSetup(context.Context) error {
	session.abortOnce.Do(func() { close(session.queryRelease) })
	return session.setupTestSession.Close(context.Background())
}

func TestSetupDeadlineForceAbortsBlockedCalibration(t *testing.T) {
	session := &setupBlockedCalibrationSession{
		setupTestSession: newSetupTestSession(0),
		queryEntered:     make(chan struct{}),
		queryRelease:     make(chan struct{}),
	}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	result := make(chan error, 1)
	go func() { result <- client.Start(context.Background()) }()
	<-session.queryEntered
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start() = %v, want setup deadline", err)
		}
	case <-time.After(100 * time.Millisecond):
		session.abortOnce.Do(func() { close(session.queryRelease) })
		<-result
		t.Fatal("setup deadline did not force-abort calibration")
	}
}

type setupCatchUpClosedSession struct{ *setupTestSession }

func (session *setupCatchUpClosedSession) CatchUp(context.Context, int) ([]Stanza, error) {
	<-session.release
	return nil, ErrClosed
}

func TestSetupCatchUpAbortPreservesDeadlineTaxonomy(t *testing.T) {
	session := &setupCatchUpClosedSession{setupTestSession: newSetupTestSession(0)}
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want deadline instead of close wakeup", err)
	}
}

type setupOwnedMailboxSession struct {
	*setupTestSession
	mu        sync.Mutex
	mailbox   []byte
	entered   chan struct{}
	release   chan struct{}
	returning chan struct{}
	late      bool
	err       error
}

func newSetupOwnedMailboxSession() *setupOwnedMailboxSession {
	return &setupOwnedMailboxSession{
		setupTestSession: newSetupTestSession(0),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
		returning:        make(chan struct{}),
	}
}

func (session *setupOwnedMailboxSession) CatchUp(ctx context.Context, _ int) ([]Stanza, error) {
	close(session.entered)
	if session.late {
		<-ctx.Done()
	} else {
		<-session.release
	}
	data := []byte("unpublished-private-mailbox")
	session.mu.Lock()
	session.mailbox = data
	session.mu.Unlock()
	close(session.returning)
	return []Stanza{{Kind: StanzaEnvelope, Data: data}}, session.err
}

func (session *setupOwnedMailboxSession) mailboxCleared() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, value := range session.mailbox {
		if value != 0 {
			return false
		}
	}
	return true
}

func TestSetupTimeoutClearsLateMailboxBeforeReturn(t *testing.T) {
	session := newSetupOwnedMailboxSession()
	session.late = true
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = 15 * time.Millisecond
	if err := client.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() = %v, want deadline", err)
	}
	if !session.mailboxCleared() {
		t.Fatal("timed-out setup retained unpublished mailbox bytes")
	}
}

func TestSetupGenerationRetirementClearsReturnedMailbox(t *testing.T) {
	session := newSetupOwnedMailboxSession()
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = time.Second
	result := make(chan error, 1)
	go func() { result <- client.Start(context.Background()) }()
	<-session.entered
	client.mu.Lock()
	close(session.release)
	<-session.returning
	client.generation++
	client.mu.Unlock()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() = %v, want retired generation", err)
	}
	if !session.mailboxCleared() {
		t.Fatal("retired setup generation retained mailbox bytes")
	}
	_ = client.Close(context.Background())
}

func TestSetupFinishCapturesCancellationAndClearsMailbox(t *testing.T) {
	session := newSetupOwnedMailboxSession()
	client := qaUnstartedClient(t, fakeDialer{session})
	client.config.ReconnectOperationTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Start(ctx) }()
	<-session.entered
	client.mu.Lock()
	close(session.release)
	<-session.returning
	cancel()
	client.mu.Unlock()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() = %v, want atomic publication cancellation", err)
	}
	if !session.mailboxCleared() {
		t.Fatal("failed final publication retained mailbox bytes")
	}
	_ = client.Close(context.Background())
}

func TestCatchUpSessionClearsPartialResultOnError(t *testing.T) {
	session := newSetupOwnedMailboxSession()
	session.err = ErrUnavailable
	close(session.release)
	if stanzas, err := catchUpSession(session, context.Background(), 1); !errors.Is(err, ErrUnavailable) || stanzas != nil {
		t.Fatalf("catchUpSession() = %#v, %v", stanzas, err)
	}
	if !session.mailboxCleared() {
		t.Fatal("partial catch-up result survived dependency error")
	}
}
