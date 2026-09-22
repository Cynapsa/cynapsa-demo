package payload

import (
	"bytes"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func FuzzAcceptanceCanonicalDecoder(f *testing.F) {
	serializer, err := NewSerializer(64 << 10)
	if err != nil {
		f.Fatal(err)
	}
	for _, value := range []model.Payload{
		{Value: model.NativePayload{Path: "/", Body: []byte("seed")}},
		{Value: model.HTTPRequestPayload{Method: "POST", Path: "/x", Headers: []model.Header{{Name: "x-a", Value: "1"}}, Body: []byte{0, 1}}},
		{Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK", Body: []byte("seed")}},
	} {
		encoded, encodeErr := serializer.Serialize(value)
		if encodeErr != nil {
			f.Fatal(encodeErr)
		}
		f.Add(encoded)
	}
	f.Add([]byte{})
	f.Add([]byte{0xbf, 0xff})

	f.Fuzz(func(t *testing.T, encoded []byte) {
		decoded, decodeErr := serializer.Deserialize(encoded)
		if decodeErr != nil {
			return
		}
		reencoded, encodeErr := serializer.Serialize(decoded)
		zeroModelPayload(decoded)
		defer zero(reencoded)
		if encodeErr != nil {
			t.Fatalf("accepted value cannot re-encode: %v", encodeErr)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatal("decoder accepted a non-canonical representation")
		}
	})
}

func FuzzAcceptanceManifestAndChunkDecoders(f *testing.F) {
	_, manifest, chunks := fuzzSeedTransfer(f)
	manifestBytes, _ := EncodeManifest(manifest)
	chunkBytes, _ := EncodeChunk(chunks[0])
	f.Add(manifestBytes, chunkBytes)
	f.Add([]byte{}, []byte{})

	f.Fuzz(func(t *testing.T, manifestInput, chunkInput []byte) {
		if decoded, err := DecodeManifest(manifestInput); err == nil {
			reencoded, encodeErr := EncodeManifest(decoded)
			defer zero(reencoded)
			if encodeErr != nil || !bytes.Equal(manifestInput, reencoded) {
				t.Fatalf("manifest canonical mismatch: %v", encodeErr)
			}
			zero(decoded.CanonicalDigest)
			zero(decoded.TransferredDigest)
		}
		if decoded, err := DecodeChunk(chunkInput, 1<<10); err == nil {
			reencoded, encodeErr := EncodeChunk(decoded)
			defer zero(reencoded)
			if encodeErr != nil || !bytes.Equal(chunkInput, reencoded) {
				t.Fatalf("chunk canonical mismatch: %v", encodeErr)
			}
			zero(decoded.Bytes)
			zero(decoded.Digest)
		}
	})
}

func FuzzAcceptanceManifestQuotaAndChunkArithmetic(f *testing.F) {
	binding, manifest, chunks := fuzzSeedTransfer(f)
	f.Add(int64(manifest.CanonicalSize), int64(manifest.TransferredSize), uint32(manifest.ChunkCount), int64(chunks[0].Offset), uint32(chunks[0].Index))
	f.Add(int64(-1), int64(-1), ^uint32(0), int64(-1), ^uint32(0))

	f.Fuzz(func(t *testing.T, canonicalSize, transferredSize int64, chunkCount uint32, offset int64, index uint32) {
		candidate := cloneManifest(manifest)
		candidate.CanonicalSize = canonicalSize
		candidate.TransferredSize = transferredSize
		candidate.ChunkCount = chunkCount
		_ = ValidateTransferManifest(candidate, binding, testLimits(), time.Unix(950, 0).UTC())
		zero(candidate.CanonicalDigest)
		zero(candidate.TransferredDigest)

		chunk := chunks[0]
		chunk.Offset = offset
		chunk.Index = index
		_ = ValidateTransferChunk(manifest, chunk)
	})
}

func fuzzSeedTransfer(tb testing.TB) (TransferBinding, TransferManifest, []TransferChunk) {
	tb.Helper()
	canonical := testCanonical("fuzz framing seed")
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(canonical))
	binding.CanonicalDigest = Digest(canonical)
	manifest, err := BuildManifest(binding, canonical, canonical, 8, "", time.Unix(1000, 0).UTC(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	var chunks []TransferChunk
	if err := ForEachChunk(manifest, canonical, func(chunk TransferChunk) error {
		chunks = append(chunks, TransferChunk{TransferID: chunk.TransferID, Index: chunk.Index, Offset: chunk.Offset, Bytes: clone(chunk.Bytes), Digest: clone(chunk.Digest)})
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return binding, manifest, chunks
}

func FuzzAcceptanceHandleAndObjectReferenceDecoding(f *testing.F) {
	validHandle := "payh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	validReference, _ := encodeObjectReference("xfer_AAAAAAAAAAAAAAAAAAAAAA", "https://objects.example/blob")
	f.Add(validHandle, validReference)
	f.Add("payh_../secret", "obj1_private-url-canary")
	f.Add("0", "obj1_owABAXgbeGZlcl90Y010Y010Y010Y010Y010Y01\r0Y01BAngcA000A000A000A000A000A000A000A000AA")
	f.Fuzz(func(t *testing.T, handle, reference string) {
		_ = validPayloadHandle(handle)
		transferID, rawURL, err := decodeObjectReference(reference)
		if err == nil {
			reencoded, encodeErr := encodeObjectReference(transferID, rawURL)
			if encodeErr != nil || reencoded != reference {
				t.Fatalf("object reference canonical mismatch: %v", encodeErr)
			}
		}
	})
}
