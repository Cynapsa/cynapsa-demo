package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/pion/sctp"
)

type lifecycleCloseChannel struct {
	mu       sync.Mutex
	closed   chan struct{}
	closeN   int
	closeErr error
	block    <-chan struct{}
	once     sync.Once
}

func (*lifecycleCloseChannel) MaximumFrameBytes() int            { return 1024 }
func (*lifecycleCloseChannel) Send(context.Context, Frame) error { return nil }
func (*lifecycleCloseChannel) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (channel *lifecycleCloseChannel) Receive(ctx context.Context) (Frame, error) {
	select {
	case <-channel.closed:
		return Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}
func (channel *lifecycleCloseChannel) Close(ctx context.Context) error {
	channel.mu.Lock()
	channel.closeN++
	channel.mu.Unlock()
	if channel.block != nil {
		select {
		case <-channel.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	channel.once.Do(func() { close(channel.closed) })
	return channel.closeErr
}

type lifecycleCloseConnection struct {
	channel  DataChannel
	open     func(context.Context) (DataChannel, error)
	mu       sync.Mutex
	closeN   int
	closeErr error
	onClose  func()
}

func (connection *lifecycleCloseConnection) OpenDataChannel(ctx context.Context, _ DataChannelConfig) (DataChannel, error) {
	if connection.open != nil {
		return connection.open(ctx)
	}
	return connection.channel, nil
}
func (*lifecycleCloseConnection) ChannelBinding() ([sha256.Size]byte, bool) {
	return sha256.Sum256([]byte("close-lifecycle-binding")), true
}
func (connection *lifecycleCloseConnection) Close(context.Context) error {
	connection.mu.Lock()
	connection.closeN++
	onClose, err := connection.onClose, connection.closeErr
	connection.mu.Unlock()
	if onClose != nil {
		onClose()
	}
	return err
}

func newLifecycleCloseLink(t *testing.T, connection PeerConnection) *Link {
	t.Helper()
	link, err := NewLink(Config{
		MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1,
		TransferQueue: 1, Clock: rank1TestClock,
	}, connection, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func TestLinkCloseHasOneOwnedResultAndCallerContextsOnlyBoundJoin(t *testing.T) {
	release := make(chan struct{})
	channel := &lifecycleCloseChannel{closed: make(chan struct{}), closeErr: errors.New("close canary"), block: release}
	connection := &lifecycleCloseConnection{channel: channel}
	link := newLifecycleCloseLink(t, connection)
	if err := link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := link.Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled close=%v", err)
	}
	close(release)
	results := make(chan error, 2)
	go func() { results <- link.Close(context.Background()) }()
	go func() { results <- link.Close(context.Background()) }()
	for range 2 {
		if err := <-results; !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("joined close=%v", err)
		}
	}
	if err := link.Close(context.Background()); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("repeated close=%v", err)
	}
	channel.mu.Lock()
	channelCalls := channel.closeN
	channel.mu.Unlock()
	connection.mu.Lock()
	connectionCalls := connection.closeN
	connection.mu.Unlock()
	if channelCalls != 1 || connectionCalls != 1 {
		t.Fatalf("dependency close calls channel=%d connection=%d", channelCalls, connectionCalls)
	}
}

func TestLinkCloseOwnsChannelReturnedAfterStartCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	channel := &lifecycleCloseChannel{closed: make(chan struct{})}
	var enterOnce sync.Once
	connection := &lifecycleCloseConnection{}
	connection.open = func(context.Context) (DataChannel, error) {
		enterOnce.Do(func() { close(entered) })
		<-release
		return channel, nil
	}
	link := newLifecycleCloseLink(t, connection)
	started := make(chan error, 1)
	go func() { started <- link.Start(context.Background()) }()
	<-entered
	closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := link.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline close=%v", err)
	}
	close(release)
	if err := <-started; !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("start after close=%v", err)
	}
	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("joined close=%v", err)
	}
	channel.mu.Lock()
	channelCalls := channel.closeN
	channel.mu.Unlock()
	connection.mu.Lock()
	connectionCalls := connection.closeN
	connection.mu.Unlock()
	if channelCalls != 1 || connectionCalls != 1 {
		t.Fatalf("late dependency close calls channel=%d connection=%d", channelCalls, connectionCalls)
	}
}

func TestLinkCloseClosesOwnedConnectionBeforeBlockingChannelJoin(t *testing.T) {
	connectionClosed := make(chan struct{})
	var signalConnectionClosed sync.Once
	channel := &lifecycleCloseChannel{closed: make(chan struct{}), block: connectionClosed}
	connection := &lifecycleCloseConnection{
		channel: channel,
		onClose: func() {
			signalConnectionClosed.Do(func() { close(connectionClosed) })
		},
	}
	link := newLifecycleCloseLink(t, connection)
	if err := link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- link.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close=%v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("link close waited on channel shutdown before closing owned connection")
	}
	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("repeated close=%v", err)
	}
	channel.mu.Lock()
	channelCalls := channel.closeN
	channel.mu.Unlock()
	connection.mu.Lock()
	connectionCalls := connection.closeN
	connection.mu.Unlock()
	if channelCalls != 1 || connectionCalls != 1 {
		t.Fatalf("dependency close calls channel=%d connection=%d", channelCalls, connectionCalls)
	}
}

type lifecycleDetached struct {
	mu     sync.Mutex
	closeN int
	err    error
}

func (*lifecycleDetached) Read([]byte) (int, error)                   { return 0, io.EOF }
func (*lifecycleDetached) Write([]byte) (int, error)                  { return 0, io.ErrClosedPipe }
func (*lifecycleDetached) ReadDataChannel([]byte) (int, bool, error)  { return 0, false, io.EOF }
func (*lifecycleDetached) WriteDataChannel([]byte, bool) (int, error) { return 0, io.ErrClosedPipe }
func (detached *lifecycleDetached) Close() error {
	detached.mu.Lock()
	defer detached.mu.Unlock()
	detached.closeN++
	return detached.err
}
func (*lifecycleDetached) SetReadDeadline(time.Time) error  { return nil }
func (*lifecycleDetached) SetWriteDeadline(time.Time) error { return nil }

func TestPionDataChannelCloseJoinsOneDependencyOwnerAndRemoteClosedIsSuccess(t *testing.T) {
	for _, test := range []struct {
		name         string
		state        transport.HealthState
		closeErr     error
		terminal     bool
		readerJoined bool
		wantErr      error
	}{
		{name: "exact remote already-closed sentinel", state: transport.HealthFailed, closeErr: sctp.ErrResetPacketInStateNotExist, terminal: true, readerJoined: true},
		{name: "sentinel without terminal evidence", state: transport.HealthHealthy, closeErr: sctp.ErrResetPacketInStateNotExist, readerJoined: true, wantErr: transport.ErrClosed},
		{name: "unexpected error after remote close", state: transport.HealthClosed, closeErr: errors.New("remote close canary"), terminal: true, readerJoined: true, wantErr: transport.ErrClosed},
		{name: "unexpected dependency error", state: transport.HealthFailed, closeErr: errors.New("failed close canary"), terminal: true, readerJoined: true, wantErr: transport.ErrClosed},
	} {
		t.Run(test.name, func(t *testing.T) {
			detached := &lifecycleDetached{err: test.closeErr}
			done := make(chan struct{})
			readDone := make(chan struct{})
			if test.readerJoined {
				close(readDone)
			}
			channel := &pionDataChannel{detached: detached, done: done, readDone: readDone, closeDone: make(chan struct{}), state: test.state, terminal: test.terminal, readStarted: true}
			results := make(chan error, 8)
			for range 8 {
				go func() { results <- channel.Close(context.Background()) }()
			}
			for range 8 {
				err := <-results
				if !errors.Is(err, test.wantErr) || test.wantErr == nil && err != nil {
					t.Fatalf("close=%v want=%v", err, test.wantErr)
				}
			}
			detached.mu.Lock()
			calls := detached.closeN
			detached.mu.Unlock()
			if calls != 1 {
				t.Fatalf("dependency close calls=%d", calls)
			}
		})
	}
}

type lifecycleSentinelReleasingDetached struct {
	started sync.Once
	release sync.Once
	ready   chan struct{}
	closed  chan struct{}
	mu      sync.Mutex
	closeN  int
}

func (*lifecycleSentinelReleasingDetached) Read([]byte) (int, error)  { return 0, io.EOF }
func (*lifecycleSentinelReleasingDetached) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (detached *lifecycleSentinelReleasingDetached) ReadDataChannel([]byte) (int, bool, error) {
	detached.started.Do(func() { close(detached.ready) })
	<-detached.closed
	return 0, false, errors.New("dependency close released detached reader")
}
func (*lifecycleSentinelReleasingDetached) WriteDataChannel([]byte, bool) (int, error) {
	return 0, io.ErrClosedPipe
}
func (detached *lifecycleSentinelReleasingDetached) Close() error {
	detached.mu.Lock()
	detached.closeN++
	detached.mu.Unlock()
	detached.release.Do(func() { close(detached.closed) })
	return sctp.ErrResetPacketInStateNotExist
}
func (*lifecycleSentinelReleasingDetached) SetReadDeadline(time.Time) error  { return nil }
func (*lifecycleSentinelReleasingDetached) SetWriteDeadline(time.Time) error { return nil }

func TestPionDataChannelCloseAcceptsSentinelAfterDependencyCloseJoinsReader(t *testing.T) {
	detached := &lifecycleSentinelReleasingDetached{ready: make(chan struct{}), closed: make(chan struct{})}
	budget, err := transport.NewInboundBudget(1, transport.MinimumRank1MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	channel := &pionDataChannel{
		detached: detached, receiveMaximum: transport.MinimumRank1MessageBytes,
		leasedReceive: make(chan inboundFrame, 1), receive: make(chan Frame, 1),
		done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}),
		readStarted: true, state: transport.HealthClosed, terminal: true, clock: rank1TestClock, inboundBudget: budget,
	}
	go channel.readLoop(detached)
	select {
	case <-detached.ready:
	case <-time.After(time.Second):
		t.Fatal("detached reader did not start")
	}
	if channelIsClosed(channel.readDone) {
		t.Fatal("reader joined before dependency close")
	}

	if err := channel.Close(context.Background()); err != nil {
		t.Fatalf("close=%v", err)
	}
	if !channelIsClosed(channel.readDone) {
		t.Fatal("close succeeded without joined-reader evidence")
	}
	if err := channel.Close(context.Background()); err != nil {
		t.Fatalf("repeated close=%v", err)
	}
	detached.mu.Lock()
	closeCalls := detached.closeN
	detached.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("dependency close calls=%d", closeCalls)
	}
}

func TestPionDataChannelCloseRejectsSentinelWhenReaderCannotJoinWithoutDeadlock(t *testing.T) {
	detached := &lifecycleDetached{err: sctp.ErrResetPacketInStateNotExist}
	channel := &pionDataChannel{
		detached: detached, done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}),
		state: transport.HealthClosed, terminal: true, readStarted: true, closeJoinWait: 10 * time.Millisecond,
	}
	result := make(chan error, 1)
	go func() { result <- channel.Close(context.Background()) }()
	select {
	case err := <-result:
		if !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("close=%v want=%v", err, transport.ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("unjoinable reader deadlocked sentinel rejection")
	}
	detached.mu.Lock()
	closeCalls := detached.closeN
	detached.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("dependency close calls=%d", closeCalls)
	}
}

type lifecycleBlockingDetached struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closeN  int
}

func (*lifecycleBlockingDetached) Read([]byte) (int, error)  { return 0, io.EOF }
func (*lifecycleBlockingDetached) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (detached *lifecycleBlockingDetached) ReadDataChannel([]byte) (int, bool, error) {
	detached.once.Do(func() { close(detached.started) })
	<-detached.closed
	return 0, false, io.EOF
}
func (*lifecycleBlockingDetached) WriteDataChannel([]byte, bool) (int, error) {
	return 0, io.ErrClosedPipe
}
func (detached *lifecycleBlockingDetached) Close() error {
	detached.mu.Lock()
	detached.closeN++
	detached.mu.Unlock()
	select {
	case <-detached.closed:
	default:
		close(detached.closed)
	}
	return nil
}
func (*lifecycleBlockingDetached) SetReadDeadline(time.Time) error  { return nil }
func (*lifecycleBlockingDetached) SetWriteDeadline(time.Time) error { return nil }

func TestPionDataChannelFailureStartsAndJoinsSameCloseOwner(t *testing.T) {
	detached := &lifecycleBlockingDetached{started: make(chan struct{}), closed: make(chan struct{})}
	budget, err := transport.NewInboundBudget(1, transport.MinimumRank1MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	channel := &pionDataChannel{
		detached: detached, receiveMaximum: transport.MinimumRank1MessageBytes,
		leasedReceive: make(chan inboundFrame, 1), receive: make(chan Frame, 1),
		done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}),
		readStarted: true, state: transport.HealthHealthy, clock: rank1TestClock, inboundBudget: budget,
	}
	go channel.readLoop(detached)
	<-detached.started
	channel.fail()
	select {
	case <-channel.readDone:
	case <-time.After(time.Second):
		t.Fatal("failure did not unblock and join detached read producer")
	}
	if err := channel.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	detached.mu.Lock()
	closeCalls := detached.closeN
	detached.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("detached Close calls=%d", closeCalls)
	}
}

type lifecyclePeerAbortDetached struct {
	started chan struct{}
	abort   chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closeN  int
}

func (*lifecyclePeerAbortDetached) Read([]byte) (int, error)  { return 0, io.EOF }
func (*lifecyclePeerAbortDetached) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (detached *lifecyclePeerAbortDetached) ReadDataChannel([]byte) (int, bool, error) {
	detached.once.Do(func() { close(detached.started) })
	<-detached.abort
	return 0, false, errors.New("local peer shutdown aborted detached read")
}
func (*lifecyclePeerAbortDetached) WriteDataChannel([]byte, bool) (int, error) {
	return 0, io.ErrClosedPipe
}
func (detached *lifecyclePeerAbortDetached) Close() error {
	detached.mu.Lock()
	detached.closeN++
	detached.mu.Unlock()
	return sctp.ErrResetPacketInStateNotExist
}
func (*lifecyclePeerAbortDetached) SetReadDeadline(time.Time) error  { return nil }
func (*lifecyclePeerAbortDetached) SetWriteDeadline(time.Time) error { return nil }

func TestPionDataChannelPeerCloseReadAbortDefersChannelCleanupUntilReaderJoins(t *testing.T) {
	detached := &lifecyclePeerAbortDetached{started: make(chan struct{}), abort: make(chan struct{})}
	budget, err := transport.NewInboundBudget(1, transport.MinimumRank1MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	channel := &pionDataChannel{
		detached: detached, receiveMaximum: transport.MinimumRank1MessageBytes,
		leasedReceive: make(chan inboundFrame, 1), receive: make(chan Frame, 1),
		done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}),
		readStarted: true, state: transport.HealthHealthy, clock: rank1TestClock, inboundBudget: budget,
	}
	go channel.readLoop(detached)
	<-detached.started

	channel.beginPeerClose()
	close(detached.abort)
	select {
	case <-channel.readDone:
	case <-time.After(time.Second):
		t.Fatal("peer-close abort did not join detached read producer")
	}
	detached.mu.Lock()
	closeCallsBeforeExplicitClose := detached.closeN
	detached.mu.Unlock()
	if closeCallsBeforeExplicitClose != 0 {
		t.Fatalf("read failure started detached cleanup during peer close: calls=%d", closeCallsBeforeExplicitClose)
	}

	if err := channel.Close(context.Background()); err != nil {
		t.Fatalf("explicit channel close after peer abort=%v", err)
	}
	detached.mu.Lock()
	closeCalls := detached.closeN
	detached.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("detached Close calls=%d", closeCalls)
	}
	if observation := channel.Observe(); observation.State != transport.HealthClosed {
		t.Fatalf("channel state=%v want=%v", observation.State, transport.HealthClosed)
	}
}

func TestPionDataChannelTerminalReadAbortDoesNotReplaceClosedStateOrStartCleanup(t *testing.T) {
	detached := &lifecyclePeerAbortDetached{started: make(chan struct{}), abort: make(chan struct{})}
	budget, err := transport.NewInboundBudget(1, transport.MinimumRank1MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	channel := &pionDataChannel{
		detached: detached, receiveMaximum: transport.MinimumRank1MessageBytes,
		leasedReceive: make(chan inboundFrame, 1), receive: make(chan Frame, 1),
		done: make(chan struct{}), readDone: make(chan struct{}), closeDone: make(chan struct{}),
		readStarted: true, state: transport.HealthHealthy, clock: rank1TestClock, inboundBudget: budget,
	}
	go channel.readLoop(detached)
	<-detached.started

	// Simulate Pion's terminal callback winning the race with a later
	// non-EOF error from the detached reader.
	channel.markClosed()
	close(detached.abort)
	select {
	case <-channel.readDone:
	case <-time.After(time.Second):
		t.Fatal("terminal read abort did not join detached read producer")
	}

	detached.mu.Lock()
	closeCallsBeforeExplicitClose := detached.closeN
	detached.mu.Unlock()
	if closeCallsBeforeExplicitClose != 0 {
		t.Fatalf("late read failure started detached cleanup: calls=%d", closeCallsBeforeExplicitClose)
	}
	if observation := channel.Observe(); observation.State != transport.HealthClosed {
		t.Fatalf("late read failure changed channel state=%v want=%v", observation.State, transport.HealthClosed)
	}

	if err := channel.Close(context.Background()); err != nil {
		t.Fatalf("explicit channel close after terminal read abort=%v", err)
	}
	detached.mu.Lock()
	closeCalls := detached.closeN
	detached.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("detached Close calls=%d", closeCalls)
	}
}
