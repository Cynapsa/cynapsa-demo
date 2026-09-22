package payload

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcceptanceReassemblyCarrierTransitionsConflictsAndRetainedQuota(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	canonical := testCanonical("cross-carrier retained completion")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	cipher := fakeCipher{}
	transferred, encryptionRef, err := cipher.Encrypt(context.Background(), binding, canonical)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildManifest(binding, canonical, transferred, 7, encryptionRef, now.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []TransferChunk
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		chunks = append(chunks, TransferChunk{TransferID: chunk.TransferID, Index: chunk.Index, Offset: chunk.Offset, Bytes: clone(chunk.Bytes), Digest: clone(chunk.Digest)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	limits := testLimits()
	limits.ChunkBytes = 7
	limits.TransfersPerPeer = 1
	limits.MaximumTransfers = 1
	limits.ReassemblyBytesPerPeer = int64(len(transferred) + len(canonical))
	limits.ReassemblyBytes = int64(len(transferred) + len(canonical))
	reassembler, err := NewReassembler(limits, cipher, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer reassembler.Close()
	for _, carrier := range []CarrierKind{CarrierDirectChunks, CarrierObjectUpload, CarrierMessageChunks} {
		if err := reassembler.Begin(manifest, binding, carrier); err != nil {
			t.Fatalf("same logical transfer carrier %d admission: %v", carrier, err)
		}
	}
	reassembler.mu.Lock()
	if len(reassembler.entries) != 1 || reassembler.totalBytes != manifest.TransferredSize || reassembler.peerTransfers[binding.SenderID] != 1 {
		t.Fatalf("carrier transition double charged: entries=%d bytes=%d peer=%d", len(reassembler.entries), reassembler.totalBytes, reassembler.peerTransfers[binding.SenderID])
	}
	reassembler.mu.Unlock()

	if err := reassembler.Accept(CarrierDirectChunks, chunks[0]); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Accept(CarrierMessageChunks, chunks[0]); err != nil {
		t.Fatalf("identical cross-carrier duplicate: %v", err)
	}
	conflict := chunks[0]
	conflict.Bytes = clone(conflict.Bytes)
	conflict.Bytes[0] ^= 1
	digest := Digest(conflict.Bytes)
	conflict.Digest = clone(digest[:])
	if err := reassembler.Accept(CarrierMessageChunks, conflict); !errors.Is(err, ErrChunkConflict) {
		t.Fatalf("conflicting duplicate = %v", err)
	}
	if err := reassembler.Abort(manifest.TransferID, CarrierDirectChunks); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks[1:] {
		if err := reassembler.Accept(CarrierMessageChunks, chunk); err != nil {
			t.Fatal(err)
		}
	}
	completed, err := reassembler.Complete(context.Background(), manifest.TransferID, CarrierMessageChunks)
	if err != nil || completed.Digest != binding.CanonicalDigest {
		t.Fatalf("message-carrier completion: %#v err=%v", completed, err)
	}
	lateObject := clone(transferred)
	if err := reassembler.CompleteObject(context.Background(), manifest.TransferID, lateObject); err != nil {
		t.Fatalf("late ambiguous object completion: %v", err)
	}
	if !bytes.Equal(lateObject, make([]byte, len(lateObject))) {
		t.Fatal("late object ciphertext ownership was not cleared")
	}

	secondBinding := binding
	secondBinding.TransferID = "xfer_AQEBAQEBAQEBAQEBAQEBAQ"
	secondManifest, err := BuildManifest(secondBinding, canonical, transferred, 7, encryptionRef, now.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Begin(secondManifest, secondBinding, CarrierMessageChunks); !errors.Is(err, ErrReassemblyQuota) {
		t.Fatalf("retained completion did not hold quota: %v", err)
	}
	consumed, err := reassembler.Consume(manifest.TransferID, binding)
	if err != nil || !bytes.Equal(consumed, canonical) {
		t.Fatalf("consume: len=%d err=%v", len(consumed), err)
	}
	if err := reassembler.Begin(secondManifest, secondBinding, CarrierMessageChunks); err != nil {
		t.Fatalf("quota not released after trusted consume: %v", err)
	}
	if err := reassembler.Abort(secondManifest.TransferID, CarrierMessageChunks); err != nil {
		t.Fatal(err)
	}
}
