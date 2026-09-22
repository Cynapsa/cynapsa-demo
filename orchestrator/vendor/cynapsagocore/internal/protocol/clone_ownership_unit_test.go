package protocol

import (
	"strings"
	"testing"
	"unsafe"
)

func TestEnvelopeCloneDetachesRetainedStrings(t *testing.T) {
	backing := strings.Repeat("x", 1<<20) + "agent-a"
	alias := backing[len(backing)-7:]
	envelope := Envelope{MessageID: alias, ConversationID: alias, Sender: alias, Recipient: alias, MeshID: alias,
		Payload: PayloadDescriptor{Profile: alias, Reference: alias, EncryptionRef: alias}}
	clone := envelope.Clone()
	for name, value := range map[string]string{"message": clone.MessageID, "conversation": clone.ConversationID, "sender": clone.Sender, "recipient": clone.Recipient, "mesh": clone.MeshID, "profile": clone.Payload.Profile, "reference": clone.Payload.Reference, "encryption": clone.Payload.EncryptionRef} {
		if value != alias || unsafe.StringData(value) == unsafe.StringData(alias) {
			t.Fatalf("%s was not detached", name)
		}
	}
}
