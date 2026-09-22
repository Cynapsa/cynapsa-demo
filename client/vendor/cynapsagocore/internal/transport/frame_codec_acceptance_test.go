package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

func qaTypedID(prefix string, fill byte) string {
	return prefix + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 16))
}

func TestQAControlFrameRejectsNonCanonicalAndExtendedCBOR(t *testing.T) {
	valid, err := EncodeControlFrame(ControlFrame{Kind: FrameEnvelope, Data: []byte("e")}, 128)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"trailing item":          append(append([]byte(nil), valid...), 0x00),
		"indefinite map":         {0xbf, 0x00, 0x01, 0x01, 0x01, 0x02, 0x41, 'e', 0xff},
		"non-minimal version":    {0xa3, 0x00, 0x18, 0x01, 0x01, 0x01, 0x02, 0x41, 'e'},
		"duplicate kind":         {0xa4, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02, 0x41, 'e'},
		"unknown map key":        {0xa4, 0x00, 0x01, 0x01, 0x01, 0x02, 0x41, 'e', 0x07, 0x00},
		"tagged top-level value": {0xd8, 0x18, 0xa3, 0x00, 0x01, 0x01, 0x01, 0x02, 0x41, 'e'},
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeControlFrame(input, 128); !errors.Is(err, ErrProtocol) {
				t.Fatalf("DecodeControlFrame(%x) error = %v", input, err)
			}
		})
	}
}

func TestQAControlFrameCompletionBindingAndOwnership(t *testing.T) {
	digest := sha256.Sum256([]byte("canonical payload"))
	frame := ControlFrame{
		Kind:       FrameTransferCompletion,
		TransferID: qaTypedID("xfer_", 0x41),
		Evidence: payload.CompletionEvidence{
			TransferID: qaTypedID("xfer_", 0x41),
			MessageID:  qaTypedID("msg_", 0x42),
			Digest:     digest,
		},
	}
	encoded, err := EncodeControlFrame(frame, 512)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeControlFrame(encoded, 512)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TransferID != frame.TransferID || decoded.Evidence != frame.Evidence {
		t.Fatalf("decoded completion = %#v", decoded)
	}

	encoded[0] ^= 0xff
	if decoded.TransferID != frame.TransferID || decoded.Evidence != frame.Evidence {
		t.Fatal("decoded value aliases encoded input")
	}
	frame.Evidence.TransferID = qaTypedID("xfer_", 0x43)
	if _, err := EncodeControlFrame(frame, 512); !errors.Is(err, ErrProtocol) {
		t.Fatalf("mismatched completion binding error = %v", err)
	}
}

func TestControlFrameCompletionGoldenVector(t *testing.T) {
	digest := sha256.Sum256([]byte("golden"))
	frame := ControlFrame{
		Kind: FrameTransferCompletion, TransferID: qaTypedID("xfer_", 0x11),
		Evidence: payload.CompletionEvidence{
			TransferID: qaTypedID("xfer_", 0x11), MessageID: qaTypedID("msg_", 0x22),
			Digest: digest,
		},
	}
	encoded, err := EncodeControlFrame(frame, 512)
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("a50002010503781b786665725f4552455245524552455245524552455245524552455104781a6d73675f49694969496949694969496949694969496949694967055820dd56de4137951d9c92681b03416ec15f886b4482a27e3a517d32f085244cbe5d")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("golden=%x", encoded)
	}
}
