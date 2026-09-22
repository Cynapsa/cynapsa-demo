package rank2xmpp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestMelliumInboundLeaseMovesAcrossEventQueueWithoutDoubleCharge(t *testing.T) {
	budget, err := transport.NewInboundBudget(1, transport.MaximumControlFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{events: make(chan Event, 1), serveDone: make(chan serveResult, 1), generation: 1, inboundBudget: budget}
	producer := context.WithValue(t.Context(), melliumSessionGenerationKey{}, uint64(1))
	first := Event{Kind: EventStanza, Stanza: Stanza{Kind: StanzaSignal, Data: []byte("owned")}}
	if err = session.emit(producer, first); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.Count != 1 || stats.Bytes != 5 {
		t.Fatalf("queued stats = %+v", stats)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- session.emit(producer, Event{Kind: EventHandled}) }()
	select {
	case err = <-blocked:
		t.Fatalf("second event bypassed aggregate count: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	event, err := session.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-blocked:
		t.Fatalf("dequeue released ownership before its final consumer: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	clearEventOwned(&event)
	select {
	case err = <-blocked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("final ownership release did not wake producer")
	}
	queued, err := session.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	clearEventOwned(&queued)
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("released stats = %+v", stats)
	}
}

func TestMelliumInboundOversizeFailsClosedBeforeQueue(t *testing.T) {
	budget, _ := transport.NewInboundBudget(1, transport.MaximumControlFrameBytes)
	session := &melliumSession{events: make(chan Event, 1), generation: 1, inboundBudget: budget}
	data := make([]byte, transport.MaximumControlFrameBytes+1)
	err := session.emit(context.WithValue(t.Context(), melliumSessionGenerationKey{}, uint64(1)), Event{Kind: EventStanza, Stanza: Stanza{Data: data}})
	if !errors.Is(err, ErrProtocol) || len(session.events) != 0 || budget.Stats().Count != 0 {
		t.Fatalf("oversize result err=%v queued=%d stats=%+v", err, len(session.events), budget.Stats())
	}
}

func TestMailboxBatchAdmissionRejectsAggregateBeforeStaging(t *testing.T) {
	budget, _ := transport.NewInboundBudget(1, transport.MaximumControlFrameBytes)
	stanzas := []Stanza{{Data: make([]byte, transport.MaximumControlFrameBytes)}, {Data: []byte{1}}}
	if err := leaseMailbox(t.Context(), stanzas, budget); !errors.Is(err, ErrProtocol) {
		t.Fatalf("mailbox aggregate admission = %v", err)
	}
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("rejected mailbox partially admitted = %+v", stats)
	}
	clearStanzas(stanzas)
}

func TestMailboxLeaseMovesFromSourceSlotWithoutDoubleCharge(t *testing.T) {
	budget, _ := transport.NewInboundBudget(2, transport.MaximumControlFrameBytes)
	stanzas := []Stanza{{Data: []byte("first")}, {Data: []byte("second")}}
	if err := leaseMailbox(t.Context(), stanzas, budget); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.Count != 2 || stats.Bytes != 11 {
		t.Fatalf("leased mailbox stats = %+v", stats)
	}
	moved := stanzas[0]
	stanzas[0] = Stanza{}
	clearStanzas(stanzas)
	if stats := budget.Stats(); stats.Count != 1 || stats.Bytes != 5 {
		t.Fatalf("source cleanup released moved ownership = %+v", stats)
	}
	clearStanzaOwned(&moved)
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("moved ownership not released = %+v", stats)
	}
}
