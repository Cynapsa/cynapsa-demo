package rank1webrtc

import (
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestInboundFrameByteChargeIncludesReceiptIdentity(t *testing.T) {
	frame := Frame{Receipt: transport.LiveReceipt{
		MessageID: "message", ConversationID: "conversation", Sender: "sender", Recipient: "recipient", MeshID: "mesh",
	}}
	want := len(frame.Receipt.MessageID) + len(frame.Receipt.ConversationID) + len(frame.Receipt.Sender) + len(frame.Receipt.Recipient) + len(frame.Receipt.MeshID)
	if got := retainedFrameBytes(frame); got != want {
		t.Fatalf("receipt byte charge = %d, want %d", got, want)
	}
}
