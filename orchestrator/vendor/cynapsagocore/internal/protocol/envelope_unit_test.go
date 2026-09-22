package protocol

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewEnvelopeGeneratesRequestIdentityAndOwnsInput(t *testing.T) {
	payload, err := NewInlinePayload("aztm.native", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope(EnvelopeInput{
		ConversationID: testConversationID("known"),
		Sender:         "agent-a@xmpp.example/mesh",
		Recipient:      "agent-b@xmpp.example/mesh",
		MeshID:         "mesh", Mode: ModeRequest,
		CreatedAt:        time.Date(2026, 8, 11, 14, 0, 0, 123_456_789, time.FixedZone("offset", -4*60*60)),
		ExpiresAt:        time.Date(2026, 8, 11, 14, 0, 30, 123_456_789, time.FixedZone("offset", -4*60*60)),
		ClockUncertainty: 250*time.Millisecond + time.Nanosecond,
		Payload:          payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(envelope.MessageID, "msg_") || !strings.HasPrefix(envelope.CorrelationID, "cor_") {
		t.Fatalf("message=%q correlation=%q", envelope.MessageID, envelope.CorrelationID)
	}
	if envelope.CreatedAt.Location() != time.UTC || envelope.CreatedAt.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("noncanonical time %v", envelope.CreatedAt)
	}
	if envelope.ExpiresAt.Location() != time.UTC || envelope.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 || !envelope.ExpiresAt.After(envelope.CreatedAt) {
		t.Fatalf("noncanonical expiry %v", envelope.ExpiresAt)
	}
	if envelope.ClockUncertainty != 250*time.Millisecond+time.Microsecond {
		t.Fatalf("uncertainty was not rounded up: %v", envelope.ClockUncertainty)
	}
	payload.Inline[0] ^= 0xff
	if len(envelope.CredentialProof) != 0 || string(envelope.Payload.Inline) != "hello" {
		t.Fatal("envelope aliases constructor input")
	}
}

func TestNewEnvelopeAndCodecOmitCompatibilityProof(t *testing.T) {
	payload, err := NewInlinePayload("aztm.native", nil)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope(EnvelopeInput{
		MessageID:      testMessageID("known"),
		ConversationID: testConversationID("known"),
		Sender:         "agent-a@xmpp.example/mesh",
		Recipient:      "agent-b@xmpp.example/mesh",
		MeshID:         "mesh", Mode: ModeMessage,
		CreatedAt:        time.Now(),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	codec := mustCodec(t)
	if _, err := codec.EncodeForIntegrity(envelope); err != nil {
		t.Fatalf("integrity encoding: %v", err)
	}
	if _, err := codec.Encode(envelope); err != nil {
		t.Fatalf("proof-free encoding: %v", err)
	}
	envelope.CredentialProof = []byte("proof")
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatalf("compatibility proof encoding: %v", err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || len(decoded.CredentialProof) != 0 {
		t.Fatalf("compatibility proof reached wire: %#v %v", decoded.CredentialProof, err)
	}
}

func TestNewEnvelopeRejectsMissingCalibratedCreationTime(t *testing.T) {
	payload, err := NewInlinePayload("aztm.native", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewEnvelope(EnvelopeInput{
		MessageID: testMessageID("missing-created-at"), ConversationID: testConversationID("missing-created-at"),
		Sender: "agent-a", Recipient: "agent-b", MeshID: "mesh", Mode: ModeMessage,
		ClockUncertainty: time.Millisecond, Payload: payload,
	})
	if err == nil {
		t.Fatal("accepted missing calibrated creation time")
	}
}

func TestNewInlinePayloadRejectsBeforeCopyBoundary(t *testing.T) {
	if _, err := NewInlinePayload("aztm.native", make([]byte, MaxInlinePayloadBytes+1)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}
