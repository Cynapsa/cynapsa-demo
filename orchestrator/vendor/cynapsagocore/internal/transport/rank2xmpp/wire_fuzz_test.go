package rank2xmpp

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"
)

func FuzzQADecodeJingle(f *testing.F) {
	valid, err := EncodeJingle(sampleJingle())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`<jingle xmlns="urn:xmpp:jingle:1"/>`))
	f.Add([]byte{0xff, 0xfe, 0xfd})
	f.Fuzz(func(t *testing.T, input []byte) {
		decoded, err := DecodeJingle(input)
		if err != nil {
			return
		}
		if _, err := EncodeJingle(decoded); err != nil {
			t.Fatalf("accepted Jingle cannot re-encode: %v", err)
		}
	})
}

func FuzzQADecodeStanzaFrame(f *testing.F) {
	valid, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}, 256, 512)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`<frame xmlns="urn:cynapsa:aztm:1" v="1">%%%%</frame>`))
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = DecodeStanzaFrame(input, "a@example.test/mesh", "b@example.test/mesh", "mesh", 256, 512)
	})
}

func FuzzQADecodeObjectReadiness(f *testing.F) {
	reference := "https://objects.example.test:8443/blob/token"
	request := objectReadinessRequest{
		AttemptID: typed("rdy_", 0x71), MessageID: typed("msg_", 0x72), TransferID: typed("xfer_", 0x73),
		ExpiresAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), Reference: reference,
		ReferenceDigest: sha256.Sum256([]byte(reference)), ManifestDigest: sha256.Sum256([]byte("manifest")),
	}
	encodedRequest, err := encodeObjectReadinessRequest(request)
	if err != nil {
		f.Fatal(err)
	}
	encodedResult, err := encodeObjectReadinessResult(objectReadinessResult{request: request, ready: true})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encodedRequest)
	f.Add(encodedResult)
	f.Add([]byte{0xff, 0x00})
	f.Fuzz(func(t *testing.T, input []byte) {
		if decoded, decodeErr := decodeObjectReadinessRequest(input); decodeErr == nil {
			reencoded, encodeErr := encodeObjectReadinessRequest(decoded)
			if encodeErr != nil || !bytes.Equal(reencoded, input) {
				t.Fatalf("accepted request cannot re-encode canonically: %v", encodeErr)
			}
		}
		if decoded, decodeErr := decodeObjectReadinessResult(input); decodeErr == nil {
			reencoded, encodeErr := encodeObjectReadinessResult(decoded)
			if encodeErr != nil || !bytes.Equal(reencoded, input) {
				t.Fatalf("accepted result cannot re-encode canonically: %v", encodeErr)
			}
		}
	})
}
