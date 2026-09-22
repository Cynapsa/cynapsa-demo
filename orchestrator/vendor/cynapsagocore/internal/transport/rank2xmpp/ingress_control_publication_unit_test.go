package rank2xmpp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestCurrentMembershipWakePausesBeforeCoalescedHandoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &fakeSession{}
	ingress := newIngressGeneration(3, session, "local@example.test/mesh", nil, ctx)
	var fences atomic.Uint32
	client := &Client{
		started: true, state: DurableLive, session: session, ingress: ingress, ctx: ctx,
		generation: 1, sessionEpoch: 3, authorityFence: func() { fences.Add(1) },
		authorityChanges: make(chan struct{}, 1), membershipReady: true,
	}
	client.handleIngressAuthorityChange(ingress)
	client.handleIngressAuthorityChange(ingress)
	if client.membershipReady || client.authorityPendingIngress != ingress.id || len(client.authorityChanges) != 1 {
		t.Fatalf("ready=%v pending=%d wakes=%d", client.membershipReady, client.authorityPendingIngress, len(client.authorityChanges))
	}
	if fences.Load() != 1 {
		t.Fatalf("fences=%d", fences.Load())
	}
	if err := client.ReceiveAuthorityChange(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredIngressMembershipWakeCannotPauseReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old := &fakeSession{}
	current := &fakeSession{}
	staleIngress := newIngressGeneration(1, old, "local@example.test/mesh", nil, ctx)
	currentIngress := newIngressGeneration(2, current, "local@example.test/mesh", nil, ctx)
	client := &Client{started: true, state: DurableLive, session: current, ingress: currentIngress, ctx: ctx, generation: 1, sessionEpoch: 2, authorityFence: func() {}, authorityChanges: make(chan struct{}, 1), membershipReady: true}
	client.handleIngressAuthorityChange(staleIngress)
	if !client.membershipReady || len(client.authorityChanges) != 0 {
		t.Fatal("retired ingress changed current membership authority")
	}
}

func TestQueuedRetiredIngressMembershipWakeIsSupersededNotClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		started: true, ctx: ctx, generation: 2, sessionEpoch: 2,
		authorityChanges: make(chan struct{}, 1), authorityPendingIngress: 1,
		authorityWakeQueued: true,
	}
	client.authorityChanges <- struct{}{}
	if err := client.ReceiveAuthorityChange(context.Background()); !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrClosed) {
		t.Fatalf("retired wake=%v, want superseded ErrUnavailable", err)
	}
	if client.authorityPendingIngress != 0 || client.authorityWakeQueued {
		t.Fatal("retired wake ownership was not cleared")
	}
}
