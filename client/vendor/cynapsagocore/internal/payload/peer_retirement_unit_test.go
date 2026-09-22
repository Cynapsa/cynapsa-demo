package payload

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReassemblerRetirePeerScrubsAndRequiresFreshAllow(t *testing.T) {
	canonical := testCanonical("peer-retirement")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 5)
	limits := testLimits()
	limits.ChunkBytes = 5
	limits.ReassemblyBytesPerPeer = 256
	reassembler, err := NewReassembler(limits, nil, func() time.Time { return time.Unix(950, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Accept(CarrierDirectChunks, chunks[0]); err != nil {
		t.Fatal(err)
	}
	reassembler.mu.Lock()
	retained := reassembler.entries[manifest.TransferID].data
	reassembler.mu.Unlock()
	if retired := reassembler.RetirePeer(binding.MeshID, binding.SenderID); retired != 1 {
		t.Fatalf("retired=%d", retired)
	}
	for index, value := range retained {
		if value != 0 {
			t.Fatalf("retained byte %d not scrubbed", index)
		}
	}
	if reassembler.CurrentBytes() != 0 {
		t.Fatalf("bytes=%d", reassembler.CurrentBytes())
	}
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("revoked Begin=%v", err)
	}
	reassembler.AllowPeer(binding.MeshID, binding.SenderID)
	if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); err != nil {
		t.Fatalf("fresh snapshot Begin=%v", err)
	}
	reassembler.Close()
}

func TestReassemblerRetirePeerLinearizesAgainstConcurrentBegin(t *testing.T) {
	for range 100 {
		canonical := testCanonical("peer-retirement-race")
		binding, manifest, _ := buildTestTransfer(t, canonical, canonical, 5)
		limits := testLimits()
		limits.ChunkBytes = 5
		limits.ReassemblyBytesPerPeer = 256
		reassembler, err := NewReassembler(limits, nil, func() time.Time { return time.Unix(950, 0).UTC() })
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_ = reassembler.Begin(manifest, binding, CarrierDirectChunks)
		}()
		close(start)
		reassembler.RetirePeer(binding.MeshID, binding.SenderID)
		wait.Wait()
		reassembler.mu.Lock()
		entries := len(reassembler.entries)
		reassembler.mu.Unlock()
		if entries != 0 {
			t.Fatalf("retired race retained %d entries", entries)
		}
		if err := reassembler.Begin(manifest, binding, CarrierDirectChunks); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("post-fence Begin=%v", err)
		}
		reassembler.Close()
	}
}
