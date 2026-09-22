package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/fxamacker/cbor/v2"
)

const controlFrameVersion2 uint16 = 2

type wireControlFrameV1 struct {
	Version        uint16 `cbor:"0,keyasint"`
	Kind           uint8  `cbor:"1,keyasint"`
	Data           []byte `cbor:"2,keyasint,omitempty"`
	TransferID     string `cbor:"3,keyasint,omitempty"`
	MessageID      string `cbor:"4,keyasint,omitempty"`
	Digest         []byte `cbor:"5,keyasint,omitempty"`
	ProbeNonce     []byte `cbor:"6,keyasint,omitempty"`
	ConversationID string `cbor:"8,keyasint,omitempty"`
	Sender         string `cbor:"9,keyasint,omitempty"`
	Recipient      string `cbor:"10,keyasint,omitempty"`
	MeshID         string `cbor:"11,keyasint,omitempty"`
}

func EncodeControlFrame(frame ControlFrame, maximum int) ([]byte, error) {
	if maximum <= 0 || maximum > MaximumControlFrameBytes || !validControlFrame(frame, maximum) {
		return nil, ErrProtocol
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, ErrProtocol
	}
	wire := wireControlFrameV1{Version: controlFrameVersion2, Kind: uint8(frame.Kind), Data: append([]byte(nil), frame.Data...), TransferID: frame.TransferID, ProbeNonce: append([]byte(nil), frame.ProbeNonce...)}
	defer func() {
		clear(wire.Data)
		clear(wire.Digest)
		clear(wire.ProbeNonce)
	}()
	if frame.Kind == FrameTransferCompletion {
		wire.MessageID = frame.Evidence.MessageID
		wire.Digest = append([]byte(nil), frame.Evidence.Digest[:]...)
	}
	if frame.Kind == FrameEnvelopeReceipt {
		wire.MessageID = frame.Receipt.MessageID
		wire.ConversationID = frame.Receipt.ConversationID
		wire.Sender = frame.Receipt.Sender
		wire.Recipient = frame.Receipt.Recipient
		wire.MeshID = frame.Receipt.MeshID
	}
	encoded, err := mode.Marshal(wire)
	if err != nil || len(encoded) > maximum {
		clear(encoded)
		return nil, ErrProtocol
	}
	return encoded, nil
}

func DecodeControlFrame(encoded []byte, maximum int) (ControlFrame, error) {
	if maximum <= 0 || maximum > MaximumControlFrameBytes || len(encoded) == 0 || len(encoded) > maximum {
		return ControlFrame{}, ErrProtocol
	}
	mode, err := (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, MaxNestedLevels: 4, MaxArrayElements: 16, MaxMapPairs: 16, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden, ExtraReturnErrors: cbor.ExtraDecErrorUnknownField, UTF8: cbor.UTF8RejectInvalid}).DecMode()
	if err != nil {
		return ControlFrame{}, ErrProtocol
	}
	var wire wireControlFrameV1
	defer func() {
		clear(wire.Data)
		clear(wire.Digest)
		clear(wire.ProbeNonce)
	}()
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.Version != controlFrameVersion2 {
		return ControlFrame{}, ErrProtocol
	}
	frame := ControlFrame{Kind: FrameKind(wire.Kind), Data: append([]byte(nil), wire.Data...), TransferID: wire.TransferID, ProbeNonce: append([]byte(nil), wire.ProbeNonce...)}
	if frame.Kind == FrameTransferCompletion && len(wire.Digest) == sha256.Size {
		frame.Evidence.TransferID = wire.TransferID
		frame.Evidence.MessageID = wire.MessageID
		copy(frame.Evidence.Digest[:], wire.Digest)
	}
	if frame.Kind == FrameEnvelopeReceipt {
		frame.Receipt = LiveReceipt{
			MessageID: wire.MessageID, ConversationID: wire.ConversationID,
			Sender: wire.Sender, Recipient: wire.Recipient, MeshID: wire.MeshID,
		}
	}
	if !validControlFrame(frame, maximum) {
		clear(frame.Data)
		clear(frame.ProbeNonce)
		return ControlFrame{}, ErrProtocol
	}
	reencoded, err := EncodeControlFrame(frame, maximum)
	defer clear(reencoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		clear(frame.Data)
		clear(frame.ProbeNonce)
		return ControlFrame{}, ErrProtocol
	}
	return frame, nil
}

func validControlFrame(frame ControlFrame, maximum int) bool {
	if len(frame.Data) > maximum || frame.Kind < FrameEnvelope || frame.Kind > FrameEnvelopeReceipt {
		return false
	}
	switch frame.Kind {
	case FrameEnvelope:
		return len(frame.Data) > 0 && frame.TransferID == "" && frame.Evidence == (payload.CompletionEvidence{}) && len(frame.ProbeNonce) == 0
	case FrameTransferManifest, FrameTransferChunk:
		return validTyped(frame.TransferID, "xfer_", 16) && len(frame.Data) > 0 && frame.Evidence == (payload.CompletionEvidence{}) && len(frame.ProbeNonce) == 0
	case FrameTransferFinish, FrameTransferAbort:
		return validTyped(frame.TransferID, "xfer_", 16) && len(frame.Data) == 0 && frame.Evidence == (payload.CompletionEvidence{}) && len(frame.ProbeNonce) == 0
	case FrameTransferCompletion:
		return validTyped(frame.TransferID, "xfer_", 16) && frame.Evidence.TransferID == frame.TransferID && validTyped(frame.Evidence.MessageID, "msg_", 16) && len(frame.Data) == 0 && len(frame.ProbeNonce) == 0
	case FrameHealthProbe, FrameHealthReply:
		return len(frame.ProbeNonce) == 16 && len(frame.Data) == 0 && frame.TransferID == "" && frame.Evidence == (payload.CompletionEvidence{})
	case FrameObjectReadinessRequest, FrameObjectReadinessResult, FrameObjectTransfer, FrameObjectTransferFailure, FrameObjectTransferAbort:
		return validTyped(frame.TransferID, "xfer_", 16) && len(frame.Data) > 0 && frame.Evidence == (payload.CompletionEvidence{}) && len(frame.ProbeNonce) == 0
	case FrameEnvelopeReceipt:
		receipt := frame.Receipt
		return validTyped(receipt.MessageID, "msg_", 16) && validTyped(receipt.ConversationID, "conv_", sha256.Size) &&
			protocol.ValidateAgentIdentity(receipt.Sender) == nil && protocol.ValidateAgentIdentity(receipt.Recipient) == nil &&
			protocol.ValidateMeshID(receipt.MeshID) == nil &&
			receipt.ChannelBinding == [sha256.Size]byte{} && len(frame.Data) == 0 && frame.TransferID == "" &&
			frame.Evidence == (payload.CompletionEvidence{}) && len(frame.ProbeNonce) == 0
	}
	return false
}

func validTyped(value, prefix string, size int) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	encoded := strings.TrimPrefix(value, prefix)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	valid := err == nil && len(raw) == size && base64.RawURLEncoding.EncodeToString(raw) == encoded
	clear(raw)
	return valid
}
