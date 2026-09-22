package transport_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestMinimumRank1MessageBytesIsDerivedFromProductionEncoders(t *testing.T) {
	payload, err := protocol.NewInlinePayload("p", nil)
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{
		Version:          protocol.Version2,
		MessageID:        "msg_" + base64.RawURLEncoding.EncodeToString(make([]byte, 16)),
		ConversationID:   "conv_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		Sender:           "a/m",
		Recipient:        "b/m",
		MeshID:           "m",
		Mode:             protocol.ModeMessage,
		CreatedAt:        time.Unix(0, 0).UTC(),
		ClockUncertainty: time.Microsecond,
		Payload:          payload,
	}
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encodedEnvelope, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelopeFrame, err := transport.EncodeControlFrame(transport.ControlFrame{Kind: transport.FrameEnvelope, Data: encodedEnvelope}, transport.MaximumControlFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []transport.FrameKind{transport.FrameHealthProbe, transport.FrameHealthReply} {
		healthFrame, healthErr := transport.EncodeControlFrame(transport.ControlFrame{Kind: kind, ProbeNonce: make([]byte, 16)}, transport.MaximumControlFrameBytes)
		if healthErr != nil {
			t.Fatal(healthErr)
		}
		if len(healthFrame) != 23 {
			t.Fatalf("mandatory health encoding changed to %d bytes; review the negotiated Rank 1 minimum", len(healthFrame))
		}
	}
	if len(envelopeFrame) != transport.MinimumRank1MessageBytes {
		t.Fatalf("smallest V2 envelope frame changed to %d bytes; review MinimumRank1MessageBytes=%d", len(envelopeFrame), transport.MinimumRank1MessageBytes)
	}
	decodedFrame, err := transport.DecodeControlFrame(envelopeFrame, transport.MinimumRank1MessageBytes)
	if err != nil {
		t.Fatalf("exact-minimum envelope frame was not accepted: %v", err)
	}
	if _, err = codec.Decode(decodedFrame.Data); err != nil {
		t.Fatalf("exact-minimum V1 envelope was not accepted: %v", err)
	}
	if _, err = transport.EncodeControlFrame(transport.ControlFrame{Kind: transport.FrameEnvelope, Data: encodedEnvelope}, transport.MinimumRank1MessageBytes-1); err == nil {
		t.Fatal("minimum-minus-one accepted the smallest V2 envelope frame")
	}
}
