package payload

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type quotaProbeCipher struct {
	decrypts int
}

type expiringCipher struct {
	started chan struct{}
}

func (*expiringCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}

func (cipher *expiringCipher) Decrypt(ctx context.Context, _ TransferBinding, _ []byte, _ string) ([]byte, error) {
	close(cipher.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*quotaProbeCipher) Encrypt(context.Context, TransferBinding, []byte) ([]byte, string, error) {
	return nil, "", ErrEncryption
}

func (cipher *quotaProbeCipher) Decrypt(context.Context, TransferBinding, []byte, string) ([]byte, error) {
	cipher.decrypts++
	return nil, ErrEncryption
}

func TestReassemblerMovesOneCanonicalBufferAndReleasesExactQuota(t *testing.T) {
	canonical := testCanonical("one retained canonical allocation")
	binding, manifest, chunks := buildTestTransfer(t, canonical, canonical, 7)
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = int64(len(canonical))
	limits.ReassemblyBytes = int64(len(canonical))
	reassembler, err := NewReassembler(limits, nil, timeAt950)
	if err != nil {
		t.Fatal(err)
	}
	if err = reassembler.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	reassembler.mu.Lock()
	owned := &reassembler.entries[manifest.TransferID].data[0]
	reassembler.mu.Unlock()
	for _, chunk := range chunks {
		if err = reassembler.Accept(CarrierMessageChunks, chunk); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
	if err != nil || evidence.Digest != binding.CanonicalDigest {
		t.Fatalf("completion=%#v err=%v", evidence, err)
	}
	if got := reassembler.CurrentBytes(); got != int64(len(canonical)) {
		t.Fatalf("retained bytes=%d", got)
	}
	reassembler.mu.Lock()
	retained := &reassembler.entries[manifest.TransferID].canonical[0]
	reassembler.mu.Unlock()
	if retained != owned {
		t.Fatal("completion cloned the full canonical buffer")
	}
	consumed, err := reassembler.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(consumed, canonical) || &consumed[0] != owned {
		t.Fatalf("consume moved=%t equal=%t err=%v", len(consumed) != 0 && &consumed[0] == owned, bytes.Equal(consumed, canonical), err)
	}
	if got := reassembler.CurrentBytes(); got != 0 {
		t.Fatalf("released bytes=%d", got)
	}
	zero(consumed)
}

func TestReassemblerReservesEncryptedOutputBeforeDecrypt(t *testing.T) {
	canonical := testCanonical("encrypted quota reservation")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	transferred, reference, err := (fakeCipher{}).Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	now := timeAt950()
	manifest, err := BuildManifest(binding, canonical, transferred, 8, reference, now.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical) - 1)
	limits.ReassemblyBytes = limits.ReassemblyBytesPerPeer
	probe := &quotaProbeCipher{}
	reassembler, err := NewReassembler(limits, probe, timeAt950)
	if err != nil {
		t.Fatal(err)
	}
	if err = reassembler.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err = ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		return reassembler.Accept(CarrierMessageChunks, chunk)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks); !errors.Is(err, ErrReassemblyQuota) {
		t.Fatalf("completion error=%v", err)
	}
	if probe.decrypts != 0 {
		t.Fatal("cipher allocated before canonical quota was reserved")
	}
	if got := reassembler.CurrentBytes(); got != int64(len(transferred)) {
		t.Fatalf("failed completion retained bytes=%d", got)
	}
	if err = reassembler.Abort(manifest.TransferID, CarrierMessageChunks); err != nil || reassembler.CurrentBytes() != 0 {
		t.Fatalf("abort err=%v bytes=%d", err, reassembler.CurrentBytes())
	}
	zero(transferred)
}

func TestReassemblerExpiryWinsFinalizeOnceWithoutRevival(t *testing.T) {
	start := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var current atomic.Int64
	current.Store(start.UnixNano())
	now := func() time.Time { return time.Unix(0, current.Load()).UTC() }
	canonical := testCanonical("expiry owns blocked finalize")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	transferred, reference, err := (fakeCipher{}).Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildManifest(binding, canonical, transferred, 8, reference, start.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	limits := testLimits()
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	limits.ReassemblyBytes = limits.ReassemblyBytesPerPeer
	cipher := &expiringCipher{started: make(chan struct{})}
	reassembler, err := NewReassembler(limits, cipher, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = reassembler.Begin(manifest, binding, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
	if err = ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		return reassembler.Accept(CarrierMessageChunks, chunk)
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, completeErr := reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
		done <- completeErr
	}()
	<-cipher.started
	current.Store(start.Add(2 * time.Minute).UnixNano())
	if expired := reassembler.Expire(now()); expired != 1 {
		t.Fatalf("first expiry count=%d", expired)
	}
	if expired := reassembler.Expire(now()); expired != 0 {
		t.Fatalf("repeat expiry count=%d", expired)
	}
	if completeErr := <-done; !errors.Is(completeErr, ErrTransferExpired) {
		t.Fatalf("expired finalize=%v", completeErr)
	}
	if got := reassembler.CurrentBytes(); got != 0 {
		t.Fatalf("expired finalize bytes=%d", got)
	}
	reassembler.mu.Lock()
	_, retained := reassembler.entries[manifest.TransferID]
	reassembler.mu.Unlock()
	if retained {
		t.Fatal("expired finalize revived transfer entry")
	}
	zero(transferred)
}

func bytesAllZeroPayload(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
