package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
)

func TestControlFrameCanonicalRoundTripAndBounds(t *testing.T) {
	frame := ControlFrame{Kind: FrameHealthProbe, ProbeNonce: bytes.Repeat([]byte{1}, 16)}
	encoded, err := EncodeControlFrame(frame, 128)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeControlFrame(encoded, 128)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != frame.Kind || !bytes.Equal(decoded.ProbeNonce, frame.ProbeNonce) {
		t.Fatalf("decoded=%#v", decoded)
	}
	if _, err := DecodeControlFrame(append(encoded, 0), 128); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing bytes=%v", err)
	}
	if _, err := EncodeControlFrame(ControlFrame{Kind: FrameHealthProbe, ProbeNonce: []byte{1}}, 128); !errors.Is(err, ErrProtocol) {
		t.Fatalf("nonce=%v", err)
	}
	if _, err := EncodeControlFrame(ControlFrame{Kind: FrameEnvelope, Data: make([]byte, 129)}, 128); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversize=%v", err)
	}
}

func TestEnvelopeReceiptCanonicalRoundTripExcludesLocalSessionBinding(t *testing.T) {
	messageRaw := bytes.Repeat([]byte{1}, 16)
	conversationRaw := bytes.Repeat([]byte{2}, sha256.Size)
	receipt := LiveReceipt{
		MessageID:      "msg_" + base64.RawURLEncoding.EncodeToString(messageRaw),
		ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(conversationRaw),
		Sender:         "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh",
	}
	encoded, err := EncodeControlFrame(ControlFrame{Kind: FrameEnvelopeReceipt, Receipt: receipt}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeControlFrame(encoded, 1024)
	if err != nil || decoded.Kind != FrameEnvelopeReceipt || decoded.Receipt != receipt || decoded.Receipt.ChannelBinding != [sha256.Size]byte{} {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
	localOnly := receipt
	localOnly.ChannelBinding = sha256.Sum256([]byte("session"))
	if _, err = EncodeControlFrame(ControlFrame{Kind: FrameEnvelopeReceipt, Receipt: localOnly}, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatalf("serialized local binding=%v", err)
	}
	invalidIdentity := receipt
	invalidIdentity.MessageID = ""
	if _, err = EncodeControlFrame(ControlFrame{Kind: FrameEnvelopeReceipt, Receipt: invalidIdentity}, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid identity=%v", err)
	}
}
