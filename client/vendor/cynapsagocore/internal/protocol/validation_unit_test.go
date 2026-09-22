package protocol

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeAndPayloadBoundaries(t *testing.T) {
	base := testEnvelope(t)
	inlineMax, err := NewInlinePayload(strings.Repeat("p", MaxPayloadProfileBytes), make([]byte, MaxInlinePayloadBytes))
	if err != nil {
		t.Fatal(err)
	}
	base.Payload = inlineMax
	base.Sender = strings.Repeat("s", MaxAgentIdentityBytes)
	base.Recipient = strings.Repeat("r", MaxAgentIdentityBytes)
	base.MeshID = strings.Repeat("m", MaxMeshIDBytes)
	base.MessageID = testMessageID("maximum")
	base.ConversationID = testConversationID("maximum")
	base.CorrelationID = testCorrelationID("maximum")
	base.ClockUncertainty = MaxClockUncertainty
	if err := ValidateEnvelope(base); err != nil {
		t.Fatalf("maximum valid envelope rejected: %v", err)
	}

	mutations := []struct {
		name string
		fn   func(*Envelope)
	}{
		{name: "message id empty", fn: func(e *Envelope) { e.MessageID = "" }},
		{name: "message id wrong length", fn: func(e *Envelope) { e.MessageID += "x" }},
		{name: "message id padded", fn: func(e *Envelope) { e.MessageID += "=" }},
		{name: "message id wrong prefix", fn: func(e *Envelope) { e.MessageID = "cor_" + e.MessageID[4:] }},
		{name: "conversation id wrong length", fn: func(e *Envelope) { e.ConversationID = e.ConversationID[:len(e.ConversationID)-1] }},
		{name: "sender empty", fn: func(e *Envelope) { e.Sender = "" }},
		{name: "sender long", fn: func(e *Envelope) { e.Sender += "x" }},
		{name: "sender control", fn: func(e *Envelope) { e.Sender = "agent\n" }},
		{name: "mesh long", fn: func(e *Envelope) { e.MeshID += "x" }},
		{name: "creation zero", fn: func(e *Envelope) { e.CreatedAt = time.Time{} }},
		{name: "uncertainty zero", fn: func(e *Envelope) { e.ClockUncertainty = 0 }},
		{name: "uncertainty over maximum", fn: func(e *Envelope) { e.ClockUncertainty = MaxClockUncertainty + time.Microsecond }},
		{name: "uncertainty sub-microsecond", fn: func(e *Envelope) { e.ClockUncertainty = time.Nanosecond }},
		{name: "proof over compatibility bound", fn: func(e *Envelope) { e.CredentialProof = make([]byte, MaxCredentialProofBytes+1) }},
		{name: "profile long", fn: func(e *Envelope) { e.Payload.Profile += "x" }},
		{name: "inline long", fn: func(e *Envelope) {
			e.Payload.Inline = make([]byte, MaxInlinePayloadBytes+1)
			e.Payload.Size = int64(len(e.Payload.Inline))
			e.Payload.Digest = sha256.Sum256(e.Payload.Inline)
		}},
		{name: "inline size mismatch", fn: func(e *Envelope) { e.Payload.Size-- }},
		{name: "inline digest mismatch", fn: func(e *Envelope) { e.Payload.Digest[0] ^= 1 }},
		{name: "inline reference", fn: func(e *Envelope) { e.Payload.Reference = "private-ref" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := base.Clone()
			mutation.fn(&candidate)
			if err := ValidateEnvelope(candidate); err == nil {
				t.Fatal("accepted invalid envelope")
			}
		})
	}
}

func TestReferencedPayloadContract(t *testing.T) {
	digest := sha256.Sum256([]byte("large canonical payload"))
	for _, kind := range []PayloadKind{PayloadObjectReference, PayloadTransferReference} {
		payload, err := NewReferencedPayload(kind, "aztm.native", strings.Repeat("r", MaxPrivateReferenceBytes), 23, digest, strings.Repeat("e", MaxEncryptionRefBytes))
		if err != nil {
			t.Fatalf("kind %d maximum rejected: %v", kind, err)
		}
		if payload.IsMaterialized() {
			t.Fatal("reference reported materialized")
		}
		if _, err := payload.Materialize(); !errors.Is(err, ErrPayloadNotMaterialized) {
			t.Fatalf("got %v", err)
		}

		invalid := []PayloadDescriptor{
			func() PayloadDescriptor { p := payload; p.Reference += "x"; return p }(),
			func() PayloadDescriptor { p := payload; p.EncryptionRef += "x"; return p }(),
			func() PayloadDescriptor { p := payload; p.Inline = []byte{1}; return p }(),
			func() PayloadDescriptor { p := payload; p.Reference = ""; return p }(),
		}
		for _, candidate := range invalid {
			if err := ValidatePayloadDescriptor(candidate); err == nil {
				t.Fatalf("kind %d accepted invalid descriptor: %#v", kind, candidate)
			}
		}
	}

	if _, err := NewReferencedPayload(PayloadKind(99), "aztm.native", "ref", 1, digest, ""); err == nil {
		t.Fatal("accepted unknown descriptor kind")
	}
}

func TestModeMetadataRules(t *testing.T) {
	tests := []struct {
		name  string
		mode  Mode
		cor   string
		reply string
		valid bool
	}{
		{name: "message", mode: ModeMessage, valid: true},
		{name: "message correlation", mode: ModeMessage, cor: testCorrelationID("x"), valid: false},
		{name: "request", mode: ModeRequest, cor: testCorrelationID("x"), valid: true},
		{name: "request missing correlation", mode: ModeRequest, valid: false},
		{name: "request reply", mode: ModeRequest, cor: testCorrelationID("x"), reply: testMessageID("x"), valid: false},
		{name: "response", mode: ModeResponse, cor: testCorrelationID("x"), reply: testMessageID("x"), valid: true},
		{name: "response missing reply", mode: ModeResponse, cor: testCorrelationID("x"), valid: false},
		{name: "unknown", mode: Mode("future"), valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope := testEnvelope(t)
			envelope.Mode = test.mode
			envelope.CorrelationID = test.cor
			envelope.ReplyTo = test.reply
			if test.mode != ModeRequest {
				envelope.ExpiresAt = time.Time{}
			}
			err := ValidateEnvelope(envelope)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v, got %v", test.valid, err)
			}
		})
	}
}

func TestRequestExpiryIsRequiredExactAndModeBound(t *testing.T) {
	request := testEnvelope(t)
	mutations := []struct {
		name string
		edit func(*Envelope)
	}{
		{name: "missing", edit: func(envelope *Envelope) { envelope.ExpiresAt = time.Time{} }},
		{name: "equal creation", edit: func(envelope *Envelope) { envelope.ExpiresAt = envelope.CreatedAt }},
		{name: "before creation", edit: func(envelope *Envelope) { envelope.ExpiresAt = envelope.CreatedAt.Add(-time.Millisecond) }},
		{name: "sub millisecond", edit: func(envelope *Envelope) { envelope.ExpiresAt = envelope.ExpiresAt.Add(time.Nanosecond) }},
		{name: "message carries expiry", edit: func(envelope *Envelope) { envelope.Mode, envelope.CorrelationID = ModeMessage, "" }},
		{name: "response carries expiry", edit: func(envelope *Envelope) { envelope.Mode, envelope.ReplyTo = ModeResponse, testMessageID("reply") }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := request.Clone()
			mutation.edit(&candidate)
			if err := ValidateEnvelope(candidate); err == nil {
				t.Fatal("accepted invalid request expiry")
			}
		})
	}
}

func TestTypedIdentifierExactForms(t *testing.T) {
	base := testEnvelope(t)
	tests := []struct {
		name string
		set  func(*Envelope)
	}{
		{name: "message short", set: func(e *Envelope) { e.MessageID = e.MessageID[:len(e.MessageID)-1] }},
		{name: "message padded", set: func(e *Envelope) { e.MessageID += "=" }},
		{name: "message standard base64", set: func(e *Envelope) { e.MessageID = "msg_+++++++++++++++++++++A" }},
		{name: "message wrong prefix", set: func(e *Envelope) { e.MessageID = "cor_" + e.MessageID[len("msg_"):] }},
		{name: "conversation short", set: func(e *Envelope) { e.ConversationID = e.ConversationID[:len(e.ConversationID)-1] }},
		{name: "conversation only 128 bits", set: func(e *Envelope) { e.ConversationID = "conv_" + e.MessageID[len("msg_"):] }},
		{name: "conversation noncanonical tail", set: func(e *Envelope) { e.ConversationID = "conv_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" }},
		{name: "correlation short", set: func(e *Envelope) { e.CorrelationID = e.CorrelationID[:len(e.CorrelationID)-1] }},
		{name: "correlation padded", set: func(e *Envelope) { e.CorrelationID += "=" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base.Clone()
			test.set(&candidate)
			if err := ValidateEnvelope(candidate); !errors.Is(err, ErrInvalidIdentifier) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestValidationErrorsDoNotEchoPrivateValues(t *testing.T) {
	envelope := testEnvelope(t)
	canary := "PRIVATE-CREDENTIAL-CANARY"
	envelope.CredentialProof = []byte(strings.Repeat(canary, 200))
	err := ValidateEnvelope(envelope)
	if err == nil {
		t.Fatal("accepted oversized proof")
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("error exposed private value: %v", err)
	}
}
