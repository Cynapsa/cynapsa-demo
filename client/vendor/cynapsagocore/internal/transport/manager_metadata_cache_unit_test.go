package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// metadataTrapReceiptAdapter proves adapter kind is captured before Manager
// ownership is published. No Manager path may call the dependency's Kind
// method while holding Manager.mu or after Start.
type metadataTrapReceiptAdapter struct {
	*lifecycleReceiptAdapter
	mu        sync.Mutex
	armed     bool
	kindCalls int
}

type reentrantAuthorityBlocker struct {
	*lifecycleReceiptAdapter
	manager *Manager
	called  chan struct{}
	once    sync.Once
}

func (adapter *reentrantAuthorityBlocker) BlockLiveAuthority() {
	_ = adapter.manager.BlockedLivePeers()
	adapter.once.Do(func() { close(adapter.called) })
}

func (adapter *metadataTrapReceiptAdapter) Kind() Kind {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.kindCalls++
	if adapter.armed {
		panic("Kind called after ownership publication")
	}
	return KindLive
}

func (adapter *metadataTrapReceiptAdapter) arm() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.armed = true
	return adapter.kindCalls
}

func (adapter *metadataTrapReceiptAdapter) callCount() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.kindCalls
}

func TestManagerCachesKindBeforePublishingAdapterOwnership(t *testing.T) {
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	adapter := &metadataTrapReceiptAdapter{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{
			fakeAdapter: base, auth: make(chan AuthenticatedReceived),
		}},
	}
	manager, err := NewManager(4, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	initialKindCalls := adapter.arm()
	if initialKindCalls != 1 {
		t.Fatalf("kind calls before publication = %d, want 1", initialKindCalls)
	}
	if _, err = manager.SendLiveTracked(context.Background(), managerEnvelope(t)); err == nil {
		t.Fatal("SendLiveTracked unexpectedly succeeded")
	}
	if kindCalls := adapter.callCount(); kindCalls != initialKindCalls {
		t.Fatalf("kind calls after publication = %d, want %d", kindCalls, initialKindCalls)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerQuarantineInvokesAuthorityBlockerWithoutManagerLock(t *testing.T) {
	manager, err := NewLiveManager(4)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := &fakeAdapter{kind: KindLive, recv: make(chan protocol.Envelope)}
	adapter := &reentrantAuthorityBlocker{
		lifecycleReceiptAdapter: &lifecycleReceiptAdapter{fakeBoundAdapter: &fakeBoundAdapter{
			fakeAdapter: base, auth: make(chan AuthenticatedReceived),
		}},
		manager: manager,
		called:  make(chan struct{}),
	}
	if err = manager.InstallLive(context.Background(), "peer", adapter); err != nil {
		t.Fatal(err)
	}
	manager.BlockLiveAuthority()
	result := make(chan []string, 1)
	go func() { result <- manager.QuarantineLiveAuthority() }()
	select {
	case peers := <-result:
		if len(peers) != 1 || peers[0] != "peer" {
			t.Fatalf("quarantined peers = %v, want [peer]", peers)
		}
	case <-time.After(time.Second):
		t.Fatal("authority blocker reentry deadlocked on Manager.mu")
	}
	select {
	case <-adapter.called:
	default:
		t.Fatal("authority blocker was not invoked")
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
