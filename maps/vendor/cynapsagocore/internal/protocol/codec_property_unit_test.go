package protocol

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"
)

func TestCodecDeterministicRoundTripProperty(t *testing.T) {
	codec := mustCodec(t)
	random := rand.New(rand.NewPCG(0x415a544d, 0x43424f52))
	for i := 0; i < 1_000; i++ {
		body := make([]byte, random.IntN(1024))
		for index := range body {
			body[index] = byte(random.Uint32())
		}
		profile := fmt.Sprintf("profile.%d", random.IntN(8))
		var payload PayloadDescriptor
		var err error
		if random.IntN(3) == 0 {
			digest := sha256.Sum256(body)
			kind := PayloadObjectReference
			if random.IntN(2) == 1 {
				kind = PayloadTransferReference
			}
			payload, err = NewReferencedPayload(kind, profile, fmt.Sprintf("private-ref-%d", i), int64(len(body)), digest, "enc-v1")
		} else {
			payload, err = NewInlinePayload(profile, body)
		}
		if err != nil {
			t.Fatal(err)
		}
		mode := []Mode{ModeMessage, ModeRequest, ModeResponse}[random.IntN(3)]
		envelope := Envelope{
			Version:        Version2,
			MessageID:      testMessageID(fmt.Sprintf("message-%d", i)),
			ConversationID: testConversationID(fmt.Sprintf("conversation-%d", random.Int64())),
			Sender:         fmt.Sprintf("agent-%d@xmpp.example/mesh", random.IntN(8)),
			Recipient:      fmt.Sprintf("agent-%d@xmpp.example/mesh", random.IntN(8)),
			MeshID:         "mesh", Mode: mode,
			CreatedAt:        time.UnixMilli(random.Int64N(4_102_444_800_000)).UTC(),
			ClockUncertainty: time.Duration(random.Int64N(1_000_000)+1) * time.Microsecond,
			Payload:          payload,
		}
		if mode == ModeRequest || mode == ModeResponse {
			envelope.CorrelationID = testCorrelationID(fmt.Sprintf("correlation-%d", i))
		}
		if mode == ModeRequest {
			envelope.ExpiresAt = envelope.CreatedAt.Add(30 * time.Second)
		}
		if mode == ModeResponse {
			envelope.ReplyTo = testMessageID(fmt.Sprintf("request-%d", i))
		}
		first, err := codec.Encode(envelope)
		if err != nil {
			t.Fatalf("iteration %d encode: %v", i, err)
		}
		decoded, err := codec.Decode(first)
		if err != nil {
			t.Fatalf("iteration %d decode: %v", i, err)
		}
		second, err := codec.Encode(decoded)
		if err != nil {
			t.Fatalf("iteration %d reencode: %v", i, err)
		}
		if !bytes.Equal(first, second) || !reflect.DeepEqual(envelope, decoded) {
			t.Fatalf("iteration %d was not stable", i)
		}
	}
}
