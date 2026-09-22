package rank2xmpp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func controlLaneTestSession(capacity int) (*melliumSession, context.Context, *atomic.Int32) {
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: capacity, StreamManagementCapacity: capacity}, Endpoint{})
	session.generation = 7
	var fences atomic.Int32
	session.setAuthorityIngressFence(func(uint64) bool {
		fences.Add(1)
		return true
	})
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	return session, ctx, &fences
}

func TestDedicatedControlLaneIsIndependentFromPeerQueue(t *testing.T) {
	session, ctx, fences := controlLaneTestSession(1)
	if err := session.emit(ctx, Event{Kind: EventStanza, Stanza: Stanza{Kind: StanzaEnvelope, Data: []byte("peer")}}); err != nil {
		t.Fatal(err)
	}
	if err := session.emit(ctx, Event{Kind: EventHandled, HandledThrough: 4}); err != nil {
		t.Fatalf("control blocked by full peer lane: %v", err)
	}
	control, err := session.ReceiveControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer clearEventOwned(&control)
	if control.Kind != EventHandled || control.HandledThrough != 4 {
		t.Fatalf("control=%#v", control)
	}
	peer, err := session.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer clearEventOwned(&peer)
	if peer.Kind != EventStanza || string(peer.Stanza.Data) != "peer" {
		t.Fatalf("peer=%#v", peer)
	}
	if fences.Load() != 0 {
		t.Fatalf("unexpected fences=%d", fences.Load())
	}
}

func TestDedicatedControlLaneOverloadFencesAuthority(t *testing.T) {
	session, ctx, fences := controlLaneTestSession(1)
	if err := session.emit(ctx, Event{Kind: EventHandled, HandledThrough: 1}); err != nil {
		t.Fatal(err)
	}
	if err := session.emit(ctx, Event{Kind: EventHandled, HandledThrough: 2}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overload=%v", err)
	}
	if fences.Load() != 1 {
		t.Fatalf("fences=%d", fences.Load())
	}
	event, err := session.ReceiveControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clearEventOwned(&event)
}

func TestResumeBarrierReleasesPeerOnlyAfterPriorControlWork(t *testing.T) {
	session, ctx, _ := controlLaneTestSession(2)
	barrier := &resumeAuthorityBarrier{done: make(chan struct{})}
	barrier.ready.Store(true)
	session.mu.Lock()
	session.resumeBarrier = barrier
	session.resumeBarrierGeneration = 7
	session.mu.Unlock()
	if err := session.emit(ctx, Event{Kind: EventStanza, Stanza: Stanza{Kind: StanzaEnvelope, Data: []byte("replayed-peer")}}); err != nil {
		t.Fatal(err)
	}
	if err := session.emit(ctx, Event{Kind: EventMembershipChanged}); err != nil {
		t.Fatal(err)
	}
	if err := session.emit(ctx, Event{Kind: EventResumeAuthorityResult, resumeBarrier: barrier}); err != nil {
		t.Fatal(err)
	}
	peerResult := make(chan Event, 1)
	peerError := make(chan error, 1)
	go func() {
		event, err := session.Receive(context.Background())
		if err != nil {
			peerError <- err
			return
		}
		peerResult <- event
	}()
	select {
	case event := <-peerResult:
		clearEventOwned(&event)
		t.Fatal("peer escaped before replayed controls")
	case err := <-peerError:
		t.Fatalf("peer receive=%v", err)
	case <-time.After(20 * time.Millisecond):
	}
	first, err := session.ReceiveControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != EventMembershipChanged {
		t.Fatalf("first control=%v", first.Kind)
	}
	clearEventOwned(&first)
	second, err := session.ReceiveControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Kind != EventResumeAuthorityResult || second.resumeBarrier != barrier {
		t.Fatalf("second control=%#v", second)
	}
	close(second.resumeBarrier.done)
	second.resumeBarrier = nil
	clearEventOwned(&second)
	select {
	case event := <-peerResult:
		defer clearEventOwned(&event)
		if string(event.Stanza.Data) != "replayed-peer" {
			t.Fatalf("peer=%#v", event)
		}
	case err := <-peerError:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("peer remained blocked after replay barrier")
	}
}
