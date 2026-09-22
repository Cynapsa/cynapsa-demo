package rpc

import "github.com/Cynapsa/cynapsagocore/internal/protocol"

func clearEnvelope(envelope *protocol.Envelope) {
	if envelope == nil {
		return
	}
	for index := range envelope.Payload.Inline {
		envelope.Payload.Inline[index] = 0
	}
	for index := range envelope.CredentialProof {
		envelope.CredentialProof[index] = 0
	}
	*envelope = protocol.Envelope{}
}
