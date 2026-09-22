package peer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type sendRecord struct {
	kind     transport.Kind
	envelope protocol.Envelope
}
type testSender struct {
	mu      sync.Mutex
	liveErr error
	sent    []sendRecord
}

func (s *testSender) Send(_ context.Context, k transport.Kind, e protocol.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, sendRecord{k, e.Clone()})
	if k == transport.KindLive {
		return s.liveErr
	}
	return nil
}

type testRecoverer struct {
	mu    sync.Mutex
	calls int
}

func (r *testRecoverer) Recover(context.Context, string) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return nil
}

func peerEnvelope(t *testing.T, sender, recipient string) protocol.Envelope {
	t.Helper()
	p, err := protocol.NewInlinePayload("native", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("conv"))
	e, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(h[:]), Sender: sender, Recipient: recipient, MeshID: "mesh", Mode: protocol.ModeMessage, CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond, Payload: p, CredentialProof: []byte("proof")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func newTestWorker(t *testing.T, sender Sender, recoverer Recoverer, inbound Inbound) *Worker {
	t.Helper()
	w, err := NewWorker(Config{PeerID: "b", QueueCapacity: 8, InboundWorkers: 2, HealthPolicy: DefaultHealthPolicy(), Sender: sender, Recoverer: recoverer, Inbound: inbound})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestHealthExactThresholds(t *testing.T) {
	w := newTestWorker(t, &testSender{}, nil, func(context.Context, transport.Kind, protocol.Envelope) error { return nil })
	start := time.Unix(100, 0).UTC()
	w.observeHealth(start, transport.Observation{State: transport.HealthDisconnected})
	w.observeHealth(start.Add(SuspiciousThreshold-time.Nanosecond), transport.Observation{State: transport.HealthDisconnected})
	if got := w.Snapshot().Health; got == HealthSuspicious {
		t.Fatalf("suspicious early: %v", got)
	}
	w.observeHealth(start.Add(SuspiciousThreshold), transport.Observation{State: transport.HealthDisconnected})
	if got := w.Snapshot().Health; got != HealthSuspicious {
		t.Fatalf("at suspicious: %v", got)
	}
	w.observeHealth(start.Add(DemotionThreshold), transport.Observation{State: transport.HealthDisconnected})
	if s := w.Snapshot(); s.Health != HealthDemoted || s.PreferredRank != RankDurable {
		t.Fatalf("at demotion: %#v", s)
	}
	w.stateMu.Lock()
	w.state.PreferredRank = RankLive
	w.stateMu.Unlock()
	w.observeHealth(start.Add(time.Second), transport.Observation{State: transport.HealthFailed})
	if w.Snapshot().PreferredRank != RankDurable {
		t.Fatal("hard failure did not demote immediately")
	}
}

func TestAmbiguousLiveSendReplaysExactIdentity(t *testing.T) {
	sender := &testSender{liveErr: transport.ErrSendAmbiguous}
	w := newTestWorker(t, sender, nil, func(context.Context, transport.Kind, protocol.Envelope) error { return nil })
	w.stateMu.Lock()
	w.state.PreferredRank = RankLive
	w.state.Health = HealthHealthy
	w.stateMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	e := peerEnvelope(t, "a", "b")
	if err := w.Send(ctx, e); err != nil {
		t.Fatal(err)
	}
	sender.mu.Lock()
	sent := append([]sendRecord(nil), sender.sent...)
	sender.mu.Unlock()
	if len(sent) != 2 || sent[0].kind != transport.KindLive || sent[1].kind != transport.KindDurable || sent[0].envelope.MessageID != sent[1].envelope.MessageID {
		t.Fatalf("replay = %#v", sent)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
}

func TestDurableInboundTriggersSingleRecoveryAfterAcceptance(t *testing.T) {
	sender := &testSender{}
	recoverer := &testRecoverer{}
	accepted := make(chan struct{}, 2)
	w := newTestWorker(t, sender, recoverer, func(context.Context, transport.Kind, protocol.Envelope) error { accepted <- struct{}{}; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	e := peerEnvelope(t, "b", "a")
	if err := w.ReceiveFrom(transport.KindDurable, e); err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("inbound not accepted")
	}
	deadline := time.After(time.Second)
	for {
		recoverer.mu.Lock()
		calls := recoverer.calls
		recoverer.mu.Unlock()
		if calls == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("recovery calls=%d", calls)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if s := w.Snapshot(); s.PreferredRank != RankLive || s.RecoveryInFlight {
		t.Fatalf("state=%#v", s)
	}
	cancel()
	<-done
}
