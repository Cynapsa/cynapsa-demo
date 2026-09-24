package peer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicRevokeRetryWaitsForTerminalPeerCleanup(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	registry := newTestRegistry(t, 4, 32, func(string) {
		once.Do(func() { close(entered) })
		<-release
	})
	if err := registry.ResetDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	const id = "peer@example.test/resource"
	if err := registry.AddDynamicPeer(id); err != nil {
		t.Fatal(err)
	}
	var ran, cleared atomic.Int32
	result, err := registry.AdmitOutbound(context.Background(), id, testWork(WorkOutbound, 1, &ran, &cleared))
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- registry.RemoveDynamicPeer(ctx, id) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("peer cleanup did not start")
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first revoke=%v", err)
	}
	second := make(chan error, 1)
	go func() { second <- registry.RemoveDynamicPeer(context.Background(), id) }()
	select {
	case err := <-second:
		t.Fatalf("action completed before peer cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatalf("retry revoke=%v", err)
	}
	if _, ok := registry.PeerEpoch(id); ok {
		t.Fatal("revoked peer retained epoch")
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDynamicDropRequiresExplicitReopen(t *testing.T) {
	registry := newTestRegistry(t, 4, 32, nil)
	if err := registry.ResetDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	const id = "peer@example.test/resource"
	if err := registry.AddDynamicPeer(id); err != nil {
		t.Fatal(err)
	}
	if err := registry.DropDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	if registry.AuthorityState() != AuthorityPaused {
		t.Fatal("drop reopened authority")
	}
	if err := registry.AddDynamicPeer(id); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("peer added before re-login: %v", err)
	}
	if err := registry.ResetDynamicAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddDynamicPeer(id); err != nil {
		t.Fatalf("re-login did not permit fresh peer handshake: %v", err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
