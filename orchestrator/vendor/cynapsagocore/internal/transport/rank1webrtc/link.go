// Package rank1webrtc implements the private preferred live-link adapter.
package rank1webrtc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type TransferReceiver interface {
	// HandleFrame borrows route and frame only through the callback. Receivers
	// must clone their values before retaining them or using them asynchronously.
	HandleFrame(context.Context, payload.CarrierRoute, Frame) (*payload.CompletionEvidence, error)
}

type RouteResolver interface {
	ResolveTransferRoute(string) (payload.CarrierRoute, bool)
}

type waitingRouteResolver interface {
	WaitTransferRoute(context.Context, string) (payload.CarrierRoute, bool)
}

type Config struct {
	MeshID               string
	LocalIdentity        string
	PeerID               string
	MaximumFrameBytes    int
	ReceiveCapacity      int
	TransferWorkers      int
	TransferQueue        int
	TransferQueueBytes   int
	TransferRouteTimeout time.Duration
	Random               io.Reader
	Clock                transport.Clock
}

// Link hides the concrete connectivity library behind the private transport
// interface and is permanently bound to one authenticated peer.
type Link struct {
	config     Config
	connection PeerConnection
	receiver   TransferReceiver
	routes     RouteResolver

	mu                sync.Mutex
	channel           DataChannel
	ctx               context.Context
	cancel            context.CancelFunc
	starting          bool
	startDone         chan struct{}
	started           bool
	closed            bool
	closeDone         chan struct{}
	closeErr          error
	pumpDone          chan struct{}
	pumpStarted       bool
	terminalEnvelope  transport.AuthenticatedReceived
	terminalOwned     bool
	ingressMu         sync.Mutex
	closeOnce         sync.Once
	retire            func()
	retireOnce        sync.Once
	envelopes         chan transport.AuthenticatedReceived
	jobs              []chan transferJob
	transferAdmission *transferAdmission
	complete          map[string]chan completion
	receiptMu         sync.Mutex
	receipts          map[string]*pendingLiveReceipt
	sendGate          chan struct{}
	wg                sync.WaitGroup
	healthMu          sync.Mutex
	lastProgress      time.Time
	probeNonce        []byte
	channelBinding    [sha256.Size]byte
	authorityBlocked  atomic.Bool
	authorityDone     chan struct{}
	authorityOnce     sync.Once
	authorityGate     *transport.LiveAuthorityGate
	authorityEpoch    uint64
	authorityLoss     <-chan struct{}
	inboundBudget     *transport.InboundBudget
}

type completion struct {
	evidence payload.CompletionEvidence
	err      error
}

type pendingLiveReceipt struct {
	link    *Link
	binding transport.LiveReceipt
	done    chan bool
	once    sync.Once
}

func (pending *pendingLiveReceipt) Binding() transport.LiveReceipt {
	if pending == nil {
		return transport.LiveReceipt{}
	}
	return pending.binding
}

func (pending *pendingLiveReceipt) Done() <-chan bool {
	if pending == nil {
		return nil
	}
	return pending.done
}

func (pending *pendingLiveReceipt) Close() {
	if pending == nil {
		return
	}
	if pending.link != nil {
		pending.link.removeReceipt(pending)
	}
	pending.resolve(false)
}

func (pending *pendingLiveReceipt) resolve(accepted bool) {
	pending.once.Do(func() { pending.done <- accepted; close(pending.done) })
}

func NewLink(config Config, connection PeerConnection, receiver TransferReceiver, routes RouteResolver) (*Link, error) {
	if protocol.ValidateMeshID(config.MeshID) != nil || protocol.ValidateAgentIdentity(config.LocalIdentity) != nil || protocol.ValidateAgentIdentity(config.PeerID) != nil || config.LocalIdentity == config.PeerID || config.MaximumFrameBytes < transport.MinimumRank1MessageBytes || config.MaximumFrameBytes > transport.MaximumControlFrameBytes || config.ReceiveCapacity <= 0 || config.ReceiveCapacity > transport.MaximumReceiveQueue || config.TransferWorkers <= 0 || config.TransferWorkers > 1024 || config.TransferQueue < config.TransferWorkers || config.TransferQueue > 65536 || config.TransferQueueBytes < 0 || config.TransferRouteTimeout < 0 || config.Clock == nil || config.Clock.Now().IsZero() || connection == nil {
		return nil, transport.ErrInvalidConfig
	}
	if receiver != nil && routes == nil {
		return nil, transport.ErrInvalidConfig
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.TransferRouteTimeout == 0 {
		config.TransferRouteTimeout = HealthProbeInterval
	}
	if config.TransferQueueBytes == 0 {
		config.TransferQueueBytes = defaultTransferQueueBytes(config.TransferQueue, config.MaximumFrameBytes)
	}
	if config.TransferQueueBytes < config.MaximumFrameBytes {
		return nil, transport.ErrInvalidConfig
	}
	jobs := make([]chan transferJob, config.TransferWorkers)
	for i := range jobs {
		capacity := config.TransferQueue / config.TransferWorkers
		if i < config.TransferQueue%config.TransferWorkers {
			capacity++
		}
		jobs[i] = make(chan transferJob, capacity)
	}
	owned, cancel := context.WithCancel(context.Background())
	sendGate := make(chan struct{}, 1)
	sendGate <- struct{}{}
	link := &Link{config: config, connection: connection, receiver: receiver, routes: routes, ctx: owned, cancel: cancel, closeDone: make(chan struct{}), pumpDone: make(chan struct{}), authorityDone: make(chan struct{}), envelopes: make(chan transport.AuthenticatedReceived, config.ReceiveCapacity), jobs: jobs, complete: make(map[string]chan completion), receipts: make(map[string]*pendingLiveReceipt), sendGate: sendGate}
	link.inboundBudget, _ = transport.NewInboundBudget(config.ReceiveCapacity, config.MaximumFrameBytes)
	link.transferAdmission = newTransferAdmission(link)
	return link, nil
}

func (l *Link) Kind() transport.Kind { return transport.KindLive }

func (l *Link) Start(ctx context.Context) error {
	if l == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return transport.ErrClosed
		}
		if l.started {
			l.mu.Unlock()
			return nil
		}
		if l.starting {
			done := l.startDone
			l.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		l.starting = true
		l.startDone = make(chan struct{})
		lifetime := l.ctx
		l.mu.Unlock()

		operation, cancelOperation := context.WithCancel(ctx)
		stopLifetime := context.AfterFunc(lifetime, cancelOperation)
		channel, err := createDataChannel(operation, l.connection, l.config.MaximumFrameBytes)
		stopLifetime()
		cancelOperation()
		if err != nil {
			l.mu.Lock()
			closed := l.closed
			l.finishStartLocked()
			l.mu.Unlock()
			if closed {
				return transport.ErrClosed
			}
			return normalize(err, ctx, transport.ErrUnavailable)
		}
		l.mu.Lock()
		if l.closed {
			// Close owns all dependencies after its fence. Transfer this late
			// channel before releasing the Start join; do not close it here.
			l.channel = channel
			l.finishStartLocked()
			l.mu.Unlock()
			return transport.ErrClosed
		}
		l.mu.Unlock()
		effectiveMaximum := channelMaximum(channel)
		binding, bound := l.connection.ChannelBinding()
		if effectiveMaximum < transport.MinimumRank1MessageBytes || effectiveMaximum > l.config.MaximumFrameBytes || !bound || binding == [sha256.Size]byte{} {
			_ = closeChannel(channel, ctx)
			l.mu.Lock()
			closed := l.closed
			l.finishStartLocked()
			l.mu.Unlock()
			if closed {
				return transport.ErrClosed
			}
			return transport.ErrProtocol
		}
		if l.config.Clock.Now().UTC().IsZero() {
			_ = closeChannel(channel, ctx)
			l.mu.Lock()
			l.finishStartLocked()
			l.mu.Unlock()
			return transport.ErrUnavailable
		}

		l.mu.Lock()
		if l.closed {
			// Close may win after validation. It still remains the sole owner
			// of both this channel and the peer connection.
			l.channel = channel
			l.finishStartLocked()
			l.mu.Unlock()
			return transport.ErrClosed
		}
		l.config.MaximumFrameBytes = effectiveMaximum
		l.channelBinding = binding
		l.channel, l.started = channel, true
		l.pumpStarted = true
		l.wg.Add(2 + l.config.TransferWorkers + l.transferAdmission.workers)
		go l.pump()
		go l.healthLoop()
		for i := 0; i < l.transferAdmission.workers; i++ {
			go l.transferAdmission.run()
		}
		for i := 0; i < l.config.TransferWorkers; i++ {
			go l.transferLoop(l.jobs[i])
		}
		l.finishStartLocked()
		l.mu.Unlock()
		return nil
	}
}

func (l *Link) finishStartLocked() {
	if l.starting {
		l.starting = false
		close(l.startDone)
	}
}

// setRetirement binds one registry token before the link can be installed.
// The callback must be nonblocking and remove only that exact token.
func (l *Link) setRetirement(callback func()) bool {
	if l == nil || callback == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.retire != nil {
		return false
	}
	l.retire = callback
	return true
}

func (l *Link) pump() {
	defer func() {
		l.failReceipts()
		close(l.pumpDone)
		l.wg.Done()
	}()
	for {
		owned, err := receiveChannelOwned(l.channel, l.ctx)
		if err != nil {
			clearInboundFrame(&owned)
			return
		}
		if owned.lease == nil {
			lease, budgetErr := l.inboundBudget.Acquire(l.ctx, retainedFrameBytes(owned.frame))
			if budgetErr != nil {
				clearInboundFrame(&owned)
				return
			}
			owned.lease = lease
		}
		frame := &owned.frame
		authorityEpoch := l.currentAuthorityEpoch()
		frame.LiveAuthorityEpoch = authorityEpoch
		if !owned.validated && !validFrame(*frame, l.config.MaximumFrameBytes) {
			clearInboundFrame(&owned)
			continue
		}
		if frame.Kind != FrameEnvelope && !isInboundTransferFrame(frame.Kind) && !l.authorityAdmittedAt(authorityEpoch) {
			clearInboundFrame(&owned)
			continue
		}
		switch frame.Kind {
		case FrameEnvelope:
			l.recordProgress()
			codec, err := protocol.NewCodec()
			if err != nil {
				clearInboundFrame(&owned)
				continue
			}
			envelope, err := codec.Decode(frame.Data)
			if err != nil || envelope.Sender != l.config.PeerID || envelope.Recipient != l.config.LocalIdentity || envelope.MeshID != l.config.MeshID {
				clearProtocolEnvelope(&envelope)
				clearInboundFrame(&owned)
				continue
			}
			l.mu.Lock()
			binding := l.channelBinding
			l.mu.Unlock()
			received := transport.AuthenticatedReceived{Envelope: envelope, Authentication: transport.LiveAuthentication{Peer: l.config.PeerID, MeshID: l.config.MeshID, ChannelBinding: binding}, LiveAuthorityEpoch: authorityEpoch, InboundLease: owned.lease}
			envelope = protocol.Envelope{}
			owned.lease = nil
			if !l.publishAuthenticated(&received) {
				clearInboundFrame(&owned)
				return
			}
		case FrameEnvelopeReceipt:
			l.recordProgress()
			l.deliverReceipt(frame.Receipt, frame.LiveAuthorityEpoch)
		case FrameTransferCompletion:
			l.recordProgress()
			l.deliverCompletion(*frame)
		case FrameHealthProbe:
			l.recordProgress()
			emitHealthEvidence(l.healthEvidence("health_probe_receive", nil, false))
			replyErr := l.sendFrame(l.ctx, Frame{Kind: FrameHealthReply, ProbeNonce: frame.ProbeNonce})
			emitHealthEvidence(l.healthEvidence("health_reply_send", replyErr, false))
		case FrameHealthReply:
			l.healthMu.Lock()
			matched := false
			if len(l.probeNonce) == len(frame.ProbeNonce) && string(l.probeNonce) == string(frame.ProbeNonce) {
				clear(l.probeNonce)
				l.probeNonce = nil
				matched = true
			}
			l.healthMu.Unlock()
			if matched {
				l.recordProgress()
			}
			emitHealthEvidence(l.healthEvidence("health_reply_receive", nil, matched))
		case FrameTransferManifest, FrameTransferChunk, FrameTransferFinish, FrameTransferAbort:
			l.recordProgress()
			l.transferAdmission.admit(*frame)
		}
		clearInboundFrame(&owned)
	}
}

const HealthProbeInterval = 5 * time.Second

func (l *Link) healthLoop() {
	defer l.wg.Done()
	ticker := time.NewTicker(HealthProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.sendHealthProbe()
		case <-l.ctx.Done():
			return
		}
	}
}

func (l *Link) sendHealthProbe() {
	nonce := make([]byte, 16)
	if _, err := io.ReadFull(l.config.Random, nonce); err != nil {
		clear(nonce)
		return
	}
	l.healthMu.Lock()
	clear(l.probeNonce)
	l.probeNonce = append([]byte(nil), nonce...)
	l.healthMu.Unlock()
	err := l.sendFrame(l.ctx, Frame{Kind: FrameHealthProbe, ProbeNonce: nonce})
	emitHealthEvidence(l.healthEvidence("health_probe_send", err, false))
	clear(nonce)
}

func (l *Link) healthEvidence(event string, err error, matched bool) healthEvidenceRecord {
	record := healthEvidenceRecord{Event: event, Outcome: healthEvidenceOutcome(err), Matched: matched, ProgressAgeMS: -1}
	if l == nil {
		return record
	}
	l.mu.Lock()
	channel := l.channel
	l.mu.Unlock()
	observation := observeChannel(channel)
	record.ChannelState = healthStateEvidence(observation.State)
	now := l.config.Clock.Now()
	record.ClockReady = !now.IsZero()
	record.AuthorityAdmitted = l.authorityAdmitted()
	progress := l.healthProgress()
	if !now.IsZero() && !progress.IsZero() {
		age := now.Sub(progress).Milliseconds()
		if age < 0 {
			age = 0
		}
		if age > 600_000 {
			age = 600_000
		}
		record.ProgressAgeMS = age
	}
	return record
}

func (l *Link) recordProgress() {
	l.healthMu.Lock()
	l.lastProgress = l.config.Clock.Now().UTC()
	l.healthMu.Unlock()
}

func (l *Link) healthProgress() time.Time {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()
	return l.lastProgress
}

func (l *Link) transferLoop(jobs <-chan transferJob) {
	defer func() {
		for {
			select {
			case job := <-jobs:
				clearOwnedFrame(&job.frame)
				l.transferAdmission.release(job.id, job.size)
			default:
				l.wg.Done()
				return
			}
		}
	}()
	for {
		if l.ctx.Err() != nil {
			return
		}
		select {
		case job := <-jobs:
			frame := job.frame
			if l.receiver == nil || l.authorityBlocked.Load() {
				clearOwnedFrame(&frame)
				l.transferAdmission.release(job.id, job.size)
				continue
			}
			evidence, err := callTransferReceiver(l.receiver, l.ctx, frame.Route, frame.Clone())
			if frame.Kind == FrameTransferFinish && err == nil && evidence != nil && l.authorityAdmittedAt(frame.LiveAuthorityEpoch) {
				_ = l.sendFrame(l.ctx, Frame{Kind: FrameTransferCompletion, Route: frame.Route, TransferID: frame.TransferID, Evidence: *evidence})
			}
			clearOwnedFrame(&frame)
			l.transferAdmission.release(job.id, job.size)
		case <-l.ctx.Done():
			return
		}
	}
}

func isInboundTransferFrame(kind FrameKind) bool {
	switch kind {
	case FrameTransferManifest, FrameTransferChunk, FrameTransferFinish, FrameTransferAbort:
		return true
	default:
		return false
	}
}

func transferLane(id string, count int) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		hash ^= uint32(id[i])
		hash *= 16777619
	}
	return int(hash % uint32(count))
}

func callTransferReceiver(receiver TransferReceiver, ctx context.Context, route payload.CarrierRoute, frame Frame) (evidence *payload.CompletionEvidence, err error) {
	defer clearOwnedFrame(&frame)
	defer func() {
		if recover() != nil {
			err = transport.ErrReceive
		}
	}()
	return receiver.HandleFrame(ctx, route, frame)
}

func (l *Link) Send(ctx context.Context, envelope protocol.Envelope) error {
	if l == nil || ctx == nil || !l.authorityAdmitted() || protocol.ValidateEnvelope(envelope) != nil || envelope.Sender != l.config.LocalIdentity || envelope.Recipient != l.config.PeerID || envelope.MeshID != l.config.MeshID {
		return transport.ErrInvalidEnvelope
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		return transport.ErrProtocol
	}
	encoded, err := codec.Encode(envelope)
	if err != nil {
		return transport.ErrInvalidEnvelope
	}
	defer clear(encoded)
	return l.sendFrame(ctx, Frame{Kind: FrameEnvelope, Data: encoded})
}

func (l *Link) SendTracked(ctx context.Context, envelope protocol.Envelope) (transport.PendingLiveReceipt, error) {
	if l == nil || ctx == nil || !l.authorityAdmitted() || protocol.ValidateEnvelope(envelope) != nil || envelope.Sender != l.config.LocalIdentity || envelope.Recipient != l.config.PeerID || envelope.MeshID != l.config.MeshID {
		return nil, transport.ErrInvalidEnvelope
	}
	l.mu.Lock()
	binding, ready := l.channelBinding, l.started && !l.closed
	l.mu.Unlock()
	if !ready || binding == [sha256.Size]byte{} {
		return nil, transport.ErrUnavailable
	}
	pending := &pendingLiveReceipt{link: l, binding: receiptForEnvelope(envelope, binding), done: make(chan bool, 1)}
	l.receiptMu.Lock()
	if len(l.receipts) >= l.config.ReceiveCapacity || l.receipts[envelope.MessageID] != nil || !l.authorityAdmitted() {
		l.receiptMu.Unlock()
		return nil, transport.ErrQueueFull
	}
	l.receipts[envelope.MessageID] = pending
	l.receiptMu.Unlock()
	if err := l.Send(ctx, envelope); err != nil {
		pending.Close()
		return nil, err
	}
	return pending, nil
}

func (l *Link) SendReceipt(ctx context.Context, receipt transport.LiveReceipt) error {
	if l == nil || ctx == nil || !l.authorityAdmitted() {
		return transport.ErrUnavailable
	}
	l.mu.Lock()
	binding, ready := l.channelBinding, l.started && !l.closed
	l.mu.Unlock()
	if !ready || binding == [sha256.Size]byte{} || receipt.ChannelBinding != binding ||
		receipt.Sender != l.config.PeerID || receipt.Recipient != l.config.LocalIdentity || receipt.MeshID != l.config.MeshID || !validReceipt(receipt) {
		return transport.ErrAuthentication
	}
	wire := receipt
	wire.ChannelBinding = [sha256.Size]byte{}
	return l.sendFrame(ctx, Frame{Kind: FrameEnvelopeReceipt, Receipt: wire})
}

func receiptForEnvelope(envelope protocol.Envelope, binding [sha256.Size]byte) transport.LiveReceipt {
	return transport.LiveReceipt{
		MessageID: envelope.MessageID, ConversationID: envelope.ConversationID,
		Sender: envelope.Sender, Recipient: envelope.Recipient, MeshID: envelope.MeshID,
		ChannelBinding: binding,
	}
}

func validReceipt(receipt transport.LiveReceipt) bool {
	return receipt.ChannelBinding != [sha256.Size]byte{} && validReceiptID(receipt.MessageID, "msg_", 16) &&
		protocol.ValidateConversationID(receipt.ConversationID) == nil && protocol.ValidateAgentIdentity(receipt.Sender) == nil &&
		protocol.ValidateAgentIdentity(receipt.Recipient) == nil && protocol.ValidateMeshID(receipt.MeshID) == nil
}

func validReceiptID(value, prefix string, size int) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	encoded := strings.TrimPrefix(value, prefix)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	valid := err == nil && len(raw) == size && base64.RawURLEncoding.EncodeToString(raw) == encoded
	clear(raw)
	return valid
}

func (l *Link) removeReceipt(pending *pendingLiveReceipt) {
	if l == nil || pending == nil {
		return
	}
	l.receiptMu.Lock()
	if l.receipts[pending.binding.MessageID] == pending {
		delete(l.receipts, pending.binding.MessageID)
	}
	l.receiptMu.Unlock()
}

func (l *Link) deliverReceipt(wire transport.LiveReceipt, authorityEpoch uint64) {
	if l == nil || !l.authorityAdmittedAt(authorityEpoch) {
		return
	}
	l.mu.Lock()
	binding := l.channelBinding
	l.mu.Unlock()
	wire.ChannelBinding = binding
	l.receiptMu.Lock()
	pending := l.receipts[wire.MessageID]
	if pending == nil || pending.binding != wire || !l.authorityAdmittedAt(authorityEpoch) {
		l.receiptMu.Unlock()
		return
	}
	delete(l.receipts, wire.MessageID)
	l.receiptMu.Unlock()
	pending.resolve(true)
}

func (l *Link) failReceipts() {
	if l == nil {
		return
	}
	l.receiptMu.Lock()
	pending := make([]*pendingLiveReceipt, 0, len(l.receipts))
	for id, receipt := range l.receipts {
		pending = append(pending, receipt)
		delete(l.receipts, id)
	}
	l.receiptMu.Unlock()
	for _, receipt := range pending {
		receipt.resolve(false)
	}
}

func (l *Link) ChannelBinding() ([sha256.Size]byte, bool) {
	if l == nil {
		return [sha256.Size]byte{}, false
	}
	l.mu.Lock()
	binding, ready := l.channelBinding, l.started && !l.closed
	l.mu.Unlock()
	return binding, ready && binding != [sha256.Size]byte{}
}

func (l *Link) sendFrame(ctx context.Context, frame Frame) error {
	if ctx == nil || !validFrame(frame, l.config.MaximumFrameBytes) {
		return transport.ErrProtocol
	}
	if !l.authorityAdmitted() {
		return transport.ErrUnavailable
	}
	operation, cancelOperation := context.WithCancel(ctx)
	stopLifetime := context.AfterFunc(l.ctx, cancelOperation)
	defer func() {
		stopLifetime()
		cancelOperation()
	}()
	select {
	case <-l.sendGate:
		defer func() { l.sendGate <- struct{}{} }()
	case <-operation.Done():
		if l.isClosed() {
			return transport.ErrClosed
		}
		return operation.Err()
	}
	if !l.authorityAdmitted() {
		return transport.ErrUnavailable
	}
	l.mu.Lock()
	channel, ready := l.channel, l.started && !l.closed
	l.mu.Unlock()
	if !ready {
		if l.isClosed() {
			return transport.ErrClosed
		}
		return transport.ErrUnavailable
	}
	err := sendChannel(channel, operation, frame)
	if l.isClosed() {
		return transport.ErrClosed
	}
	return normalize(err, operation, transport.ErrSendAmbiguous)
}

func (l *Link) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func validFrame(frame Frame, maximum int) bool {
	encoded, err := transport.EncodeControlFrame(frame, maximum)
	clear(encoded)
	return err == nil
}

func (l *Link) Receive(ctx context.Context) (protocol.Envelope, error) {
	received, err := l.ReceiveAuthenticated(ctx)
	return received.Envelope, err
}

func (l *Link) ReceiveAuthenticated(ctx context.Context) (transport.AuthenticatedReceived, error) {
	if l == nil || ctx == nil {
		return transport.AuthenticatedReceived{}, transport.ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return transport.AuthenticatedReceived{}, err
	}
	select {
	case received := <-l.envelopes:
		return l.transferAuthenticated(&received)
	default:
	}
	select {
	case received := <-l.envelopes:
		return l.transferAuthenticated(&received)
	case <-ctx.Done():
		select {
		case received := <-l.envelopes:
			return l.transferAuthenticated(&received)
		default:
			return transport.AuthenticatedReceived{}, ctx.Err()
		}
	case <-l.closedChan():
		if err := l.waitForPump(ctx); err != nil {
			select {
			case received := <-l.envelopes:
				return l.transferAuthenticated(&received)
			default:
			}
			return transport.AuthenticatedReceived{}, err
		}
		select {
		case received := <-l.envelopes:
			return l.transferAuthenticated(&received)
		default:
		}
		if received, ok := l.takeTerminalEnvelope(); ok {
			return received, nil
		}
		return transport.AuthenticatedReceived{}, transport.ErrClosed
	case <-l.authorityDone:
		return transport.AuthenticatedReceived{}, transport.ErrUnavailable
	}
}

func (l *Link) transferAuthenticated(received *transport.AuthenticatedReceived) (transport.AuthenticatedReceived, error) {
	return cloneAndClearAuthenticated(received), nil
}

// BlockLiveAuthority irreversibly fences this exact authenticated link after a
// complete snapshot has selected it for terminal retirement. Retained links
// move epochs only through RebindLiveAuthority and never invoke this callback.
func (l *Link) BlockLiveAuthority() {
	if l != nil {
		l.authorityBlocked.Store(true)
		l.authorityOnce.Do(func() { close(l.authorityDone) })
		l.mu.Lock()
		waiters := l.complete
		l.complete = make(map[string]chan completion)
		l.mu.Unlock()
		for _, waiter := range waiters {
			select {
			case waiter <- completion{err: transport.ErrUnavailable}:
			default:
			}
		}
		l.failReceipts()
	}
}

func (l *Link) BindLiveAuthority(gate *transport.LiveAuthorityGate, epoch uint64) bool {
	if l == nil || gate == nil || epoch == 0 || !gate.Admit(epoch) {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.starting || l.closed || l.authorityGate != nil {
		return false
	}
	l.authorityGate, l.authorityEpoch, l.authorityLoss = gate, epoch, gate.Loss(epoch)
	return true
}

// RebindLiveAuthority moves this retained or Manager-owned pending link to one
// exact blocked Manager epoch. A pending link may not have entered Start yet,
// and has no published ingress, but must still advance with authority before
// the fence is opened; otherwise either the pre-Start handoff or a slow
// ICE/DTLS/SCTP establishment can finish with a stale epoch. Already-
// authenticated input on a retained link keeps its original epoch. Removed
// links take the irreversible BlockLiveAuthority path instead.
func (l *Link) RebindLiveAuthority(gate *transport.LiveAuthorityGate, epoch uint64) bool {
	if l == nil || gate == nil || epoch == 0 || !gate.Rebindable(epoch) || l.authorityBlocked.Load() {
		return false
	}
	l.ingressMu.Lock()
	defer l.ingressMu.Unlock()
	l.mu.Lock()
	if l.closed || l.authorityGate != gate {
		l.mu.Unlock()
		return false
	}
	if l.authorityEpoch == epoch {
		l.mu.Unlock()
		return true
	}
	l.authorityEpoch, l.authorityLoss = epoch, gate.Loss(epoch)
	waiters := l.complete
	l.complete = make(map[string]chan completion)
	l.mu.Unlock()
	for _, waiter := range waiters {
		select {
		case waiter <- completion{err: transport.ErrUnavailable}:
		default:
		}
	}
	l.failReceipts()
	l.transferAdmission.clearAll()
	return true
}

func (l *Link) authorityAdmitted() bool {
	if l == nil || l.authorityBlocked.Load() {
		return false
	}
	l.mu.Lock()
	gate, epoch := l.authorityGate, l.authorityEpoch
	l.mu.Unlock()
	return gate == nil || gate.Admit(epoch)
}

func (l *Link) authorityAdmittedAt(epoch uint64) bool {
	if l == nil || l.authorityBlocked.Load() {
		return false
	}
	l.mu.Lock()
	gate, current := l.authorityGate, l.authorityEpoch
	l.mu.Unlock()
	if current != epoch {
		return false
	}
	return gate == nil || gate.Admit(epoch)
}

func (l *Link) currentAuthorityEpoch() uint64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	epoch := l.authorityEpoch
	l.mu.Unlock()
	return epoch
}

func (l *Link) authorityLost() <-chan struct{} {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	loss := l.authorityLoss
	l.mu.Unlock()
	if loss == nil {
		return l.authorityDone
	}
	return loss
}

func (l *Link) authorityBound() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	bound := l.authorityGate != nil && l.authorityEpoch != 0
	l.mu.Unlock()
	return bound
}

func (l *Link) LiveAuthorityDone() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.authorityDone
}

// LiveAuthorityCapturedIngress declares that every authenticated value placed
// in this Link's bounded receive ownership carries process-private capture
// provenance. Manager therefore never falls back to receive-call timing when
// deciding whether an old-epoch retained value crossed a publication fence.
func (*Link) LiveAuthorityCapturedIngress() {}

func (l *Link) waitForPump(ctx context.Context) error {
	l.mu.Lock()
	started, done := l.pumpStarted, l.pumpDone
	l.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneAndClearAuthenticated(received *transport.AuthenticatedReceived) transport.AuthenticatedReceived {
	owned := *received
	*received = transport.AuthenticatedReceived{}
	if owned.InboundLease != nil {
		owned.InboundLease.Release()
		owned.InboundLease = nil
	}
	return owned
}

func enqueueAuthenticatedBeforeClose(queue chan<- transport.AuthenticatedReceived, received transport.AuthenticatedReceived, closed <-chan struct{}) bool {
	select {
	case queue <- received:
		return true
	default:
	}
	select {
	case queue <- received:
		return true
	case <-closed:
		select {
		case queue <- received:
			return true
		default:
			return false
		}
	}
}

func (l *Link) publishAuthenticated(received *transport.AuthenticatedReceived) bool {
	l.ingressMu.Lock()
	defer l.ingressMu.Unlock()
	if l.authorityBlocked.Load() || l.isClosed() {
		clearAuthenticatedReceived(received)
		return true
	}
	l.mu.Lock()
	authorityGate := l.authorityGate
	l.mu.Unlock()
	// Capture succeeds for the current epoch and for the immediately previous
	// epoch only while its successor fence remains closed. A synthetic old
	// value first offered after publication therefore receives no capability.
	if received != nil {
		received.CaptureLiveAuthority(authorityGate, received.LiveAuthorityEpoch)
	}
	if enqueueAuthenticatedBeforeClose(l.envelopes, *received, l.ctx.Done()) {
		*received = transport.AuthenticatedReceived{}
		return true
	}
	l.mu.Lock()
	if l.terminalOwned {
		l.mu.Unlock()
		clearAuthenticatedReceived(received)
		return false
	}
	l.terminalEnvelope = *received
	*received = transport.AuthenticatedReceived{}
	l.terminalOwned = true
	l.mu.Unlock()
	return false
}

func (l *Link) takeTerminalEnvelope() (transport.AuthenticatedReceived, bool) {
	l.mu.Lock()
	if !l.terminalOwned {
		l.mu.Unlock()
		return transport.AuthenticatedReceived{}, false
	}
	received := l.terminalEnvelope
	l.terminalEnvelope = transport.AuthenticatedReceived{}
	l.terminalOwned = false
	l.mu.Unlock()
	return cloneAndClearAuthenticated(&received), true
}

func clearAuthenticatedReceived(received *transport.AuthenticatedReceived) {
	if received == nil {
		return
	}
	clear(received.Envelope.Payload.Inline)
	clear(received.Envelope.CredentialProof)
	if received.InboundLease != nil {
		received.InboundLease.Release()
	}
	*received = transport.AuthenticatedReceived{}
}

func clearProtocolEnvelope(envelope *protocol.Envelope) {
	if envelope == nil {
		return
	}
	clear(envelope.Payload.Inline)
	clear(envelope.CredentialProof)
	*envelope = protocol.Envelope{}
}

func (l *Link) closedChan() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ctx == nil {
		return nil
	}
	return l.ctx.Done()
}

func (l *Link) registerCompletion(transferID string) (<-chan completion, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || !l.started || len(l.complete) >= l.config.TransferQueue || l.complete[transferID] != nil {
		return nil, transport.ErrQueueFull
	}
	result := make(chan completion, 1)
	l.complete[transferID] = result
	return result, nil
}

func (l *Link) removeCompletion(transferID string) {
	l.mu.Lock()
	delete(l.complete, transferID)
	l.mu.Unlock()
}

func (l *Link) failCompletion(transferID string, err error) {
	l.mu.Lock()
	waiter := l.complete[transferID]
	delete(l.complete, transferID)
	l.mu.Unlock()
	if waiter == nil {
		return
	}
	select {
	case waiter <- completion{err: err}:
	default:
	}
}

func (l *Link) deliverCompletion(frame Frame) {
	if !l.authorityAdmittedAt(frame.LiveAuthorityEpoch) {
		return
	}
	l.mu.Lock()
	waiter := l.complete[frame.TransferID]
	l.mu.Unlock()
	if waiter == nil || !l.authorityAdmittedAt(frame.LiveAuthorityEpoch) || frame.Evidence.TransferID != frame.TransferID {
		return
	}
	select {
	case waiter <- completion{evidence: frame.Evidence}:
	default:
	}
}

func (l *Link) Close(ctx context.Context) error {
	if l == nil || ctx == nil {
		return transport.ErrInvalidConfig
	}
	l.closeOnce.Do(func() {
		l.beginClose()
	})
	return l.waitClose(ctx)
}

// beginClose establishes one Link-owned cleanup lifetime. Caller contexts
// bound only their join; they never flow into dependency closure.
func (l *Link) beginClose() {
	l.mu.Lock()
	l.closed = true
	var startDone <-chan struct{}
	if l.starting {
		startDone = l.startDone
	}
	for _, waiter := range l.complete {
		select {
		case waiter <- completion{err: transport.ErrClosed}:
		default:
		}
	}
	l.complete = make(map[string]chan completion)
	retire := l.retire
	l.mu.Unlock()
	l.failReceipts()
	l.retireOnce.Do(func() {
		if retire != nil {
			retire()
		}
	})
	go l.finishClose(startDone)
}

func (l *Link) finishClose(startDone <-chan struct{}) {
	// An in-flight Start observes this cancellation and transfers any channel
	// it obtained after the close fence to this sole cleanup owner.
	if startDone != nil {
		l.cancel()
		<-startDone
	}
	l.mu.Lock()
	channel := l.channel
	l.mu.Unlock()
	var result error
	// Some channel implementations cannot finish their own close path until the
	// owned peer connection tears down the underlying association.
	result = errors.Join(result, normalize(closeConnection(l.connection, context.Background()), nil, transport.ErrClosed))
	if channel != nil {
		result = errors.Join(result, normalize(closeChannel(channel, context.Background()), nil, transport.ErrClosed))
	}
	l.cancel()
	// Dependency close has published terminal channel state. Cancellation now
	// stops the remaining Link workers without racing stale close callbacks.
	l.wg.Wait()
	l.transferAdmission.clearAll()
	l.mu.Lock()
	l.closeErr = result
	l.mu.Unlock()
	close(l.closeDone)
}

func (l *Link) waitClose(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-l.closeDone:
		l.mu.Lock()
		result := l.closeErr
		l.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DiscardShutdownOwned is called only by Manager after Link.Close has joined
// the pump and the exact Manager receive loop has finished draining, or after
// global shutdown has canceled and joined that loop.
func (l *Link) DiscardShutdownOwned() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.closed || l.pumpStarted && !channelIsClosed(l.pumpDone) {
		l.mu.Unlock()
		return
	}
	terminal := l.terminalEnvelope
	l.terminalEnvelope = transport.AuthenticatedReceived{}
	l.terminalOwned = false
	channel := l.channel
	l.channel = nil
	l.mu.Unlock()
	clearAuthenticatedReceived(&terminal)
	for {
		select {
		case received := <-l.envelopes:
			clearAuthenticatedReceived(&received)
		default:
			if discarder, ok := channel.(transport.ShutdownOwnedDiscarder); ok {
				discarder.DiscardShutdownOwned()
			}
			return
		}
	}
}

func channelIsClosed(channel <-chan struct{}) bool {
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

func normalize(err error, ctx context.Context, fallback error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fallback
}
