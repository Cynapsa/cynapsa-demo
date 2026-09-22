package rank2xmpp

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/fxamacker/cbor/v2"
)

func FuzzDecodeObjectPublication(f *testing.F) {
	canonical := []byte("canonical-payload")
	binding := payload.TransferBinding{
		TransferID: typed("xfer_", 0x71), MessageID: typed("msg_", 0x72), MeshID: "mesh",
		SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh",

		CanonicalSize: int64(len(canonical)), CanonicalDigest: payload.Digest(canonical),
	}
	manifest, err := payload.BuildManifest(binding, canonical, canonical, 8, "", time.Date(2030, 1, 2, 3, 5, 0, 0, time.UTC), nil)
	if err != nil {
		f.Fatal(err)
	}
	reference := testObjectReferenceForFuzz(f, manifest.TransferID, "https://objects.example.test/blob/token")
	seed, err := EncodeObjectPublication(ObjectPublication{Manifest: manifest, PrivateReference: reference})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add(append(append([]byte(nil), seed...), 0))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		publication, decodeErr := DecodeObjectPublication(encoded)
		if decodeErr != nil {
			return
		}
		reencoded, encodeErr := EncodeObjectPublication(publication)
		if encodeErr != nil || !bytes.Equal(encoded, reencoded) {
			t.Fatalf("accepted noncanonical publication: encode=%v", encodeErr)
		}
	})
}

func testObjectReferenceForFuzz(f *testing.F, transferID, rawURL string) string {
	f.Helper()
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		f.Fatal(err)
	}
	encoded, err := mode.Marshal(testObjectReferenceWire{Version: payload.TransferVersion1, TransferID: transferID, URL: rawURL})
	if err != nil {
		f.Fatal(err)
	}
	return "obj1_" + base64.RawURLEncoding.EncodeToString(encoded)
}
