package transport

import (
	"crypto/sha256"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

const (
	MaximumControlFrameBytes = 2 << 20

	// MinimumRank1MessageBytes is the smallest negotiated Rank 1 message size
	// that carries both mandatory V1 traffic classes. The canonical 16-byte
	// health frame is 23 bytes; the smallest valid mesh-bound V1 envelope in
	// its control frame is 158 bytes. frame_minimum_acceptance_test.go derives
	// and locks both sizes through the production encoders.
	MinimumRank1MessageBytes = 154
)

type FrameKind uint8

const (
	FrameEnvelope FrameKind = iota + 1
	FrameTransferManifest
	FrameTransferChunk
	FrameTransferFinish
	FrameTransferCompletion
	FrameTransferAbort
	FrameHealthProbe
	FrameHealthReply
	FrameObjectReadinessRequest
	FrameObjectReadinessResult
	FrameObjectTransfer
	FrameObjectTransferFailure
	FrameObjectTransferAbort
	FrameEnvelopeReceipt
)

// LiveReceipt identifies one exact application envelope on one authenticated
// live session. ChannelBinding is local evidence and is never serialized: a
// receipt is authenticated by the exact DTLS-protected channel that carries it.
type LiveReceipt struct {
	MessageID, ConversationID, Sender, Recipient, MeshID string
	ChannelBinding                                       [sha256.Size]byte
}

// ControlFrame is carrier-neutral private framing. Route is authenticated
// out-of-band and deliberately excluded from EncodeControlFrame.
type ControlFrame struct {
	Kind       FrameKind
	Route      payload.CarrierRoute
	TransferID string
	Data       []byte
	Evidence   payload.CompletionEvidence
	ProbeNonce []byte
	Receipt    LiveReceipt
	// LiveAuthorityEpoch is local adapter provenance and is never serialized.
	LiveAuthorityEpoch uint64
}

func (f ControlFrame) Clone() ControlFrame {
	f.Data = append([]byte(nil), f.Data...)
	f.ProbeNonce = append([]byte(nil), f.ProbeNonce...)
	return f
}
