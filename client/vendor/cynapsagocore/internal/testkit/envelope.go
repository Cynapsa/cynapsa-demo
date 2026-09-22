package testkit

import (
	"crypto/sha256"
	"encoding/base64"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

var fixtureTime = time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)

// Envelope will construct a deterministic valid private test envelope.
func Envelope() protocol.Envelope {
	payload, err := protocol.NewInlinePayload("application/octet-stream", []byte("cynapsa-integration-fixture"))
	if err != nil {
		panic(err)
	}
	conversation := sha256.Sum256([]byte("peer-a|peer-b|mesh-integration"))
	message := sha256.Sum256([]byte("message-integration-1"))
	envelope, err := protocol.NewEnvelope(protocol.EnvelopeInput{
		MessageID:      "msg_" + base64.RawURLEncoding.EncodeToString(message[:16]),
		ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(conversation[:]),
		Sender:         "peer-a@example.test/mesh-integration",
		Recipient:      "peer-b@example.test/mesh-integration",
		MeshID:         "mesh-integration", Mode: protocol.ModeMessage,
		CreatedAt:        fixtureTime,
		ClockUncertainty: time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("test-proof-not-a-secret"),
	})
	if err != nil {
		panic(err)
	}
	return envelope
}
