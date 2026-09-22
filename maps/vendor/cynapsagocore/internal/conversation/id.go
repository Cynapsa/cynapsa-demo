// Package conversation owns private conversation identity and dedupe state.
package conversation

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

var conversationDomain = []byte("CYNAPSA-CONVERSATION-ID-V3\x00")

// DeriveID deterministically derives an opaque mesh-scoped identifier from
// exact canonical participant bytes. Participants are sorted bytewise so both
// peers derive the same ID. Identity parsing and server binding happen before
// this layer and are intentionally not inferred here.
func DeriveID(meshID, participantA, participantB string) (string, error) {
	if err := protocol.ValidateMeshID(meshID); err != nil {
		return "", ErrInvalidConversation
	}
	if err := protocol.ValidateAgentIdentity(participantA); err != nil {
		return "", ErrInvalidConversation
	}
	if err := protocol.ValidateAgentIdentity(participantB); err != nil {
		return "", ErrInvalidConversation
	}

	first, second := []byte(participantA), []byte(participantB)
	if bytes.Compare(first, second) > 0 {
		first, second = second, first
	}
	hash := sha256.New()
	_, _ = hash.Write(conversationDomain)
	writeLengthPrefixed(hash, []byte(meshID))
	writeLengthPrefixed(hash, first)
	writeLengthPrefixed(hash, second)
	return "conv_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil)), nil
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeLengthPrefixed(writer byteWriter, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}
