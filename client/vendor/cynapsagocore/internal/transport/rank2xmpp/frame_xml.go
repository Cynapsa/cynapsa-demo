package rank2xmpp

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"io"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const AZTMNamespaceV1 = "urn:cynapsa:aztm:1"

type xmlFrame struct {
	XMLName      xml.Name     `xml:"urn:cynapsa:aztm:1 frame"`
	Version      string       `xml:"v,attr"`
	Data         string       `xml:",chardata"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

type outboundXMLFrame struct {
	XMLName xml.Name `xml:"urn:cynapsa:aztm:1 frame"`
	Version string   `xml:"v,attr"`
	Data    []byte   `xml:",chardata"`
}

type xmlUnknown struct {
	XMLName xml.Name
	Inner   string `xml:",innerxml"`
}

// EncodeStanzaFrame wraps the approved canonical CBOR frame in canonical
// unpadded standard Base64 and enforces the complete XML element budget.
func EncodeStanzaFrame(stanza Stanza, maximumFrameBytes, stanzaBudget int) ([]byte, error) {
	frame, err := stanzaToFrame(stanza)
	if err != nil {
		return nil, err
	}
	encoded, err := transport.EncodeControlFrame(frame, maximumFrameBytes)
	if err != nil {
		return nil, ErrProtocol
	}
	defer clear(encoded)
	base64Data := make([]byte, base64.RawStdEncoding.EncodedLen(len(encoded)))
	defer clear(base64Data)
	base64.RawStdEncoding.Encode(base64Data, encoded)
	value := outboundXMLFrame{Version: "1", Data: base64Data}
	output, err := xml.Marshal(value)
	outerID := ""
	if stanza.Kind == StanzaEnvelope {
		outerID = stanza.MessageID
	}
	if err != nil || completeMessageSize(stanza.From, stanza.To, outerID, output) > stanzaBudget {
		clear(output)
		return nil, ErrProtocol
	}
	return output, nil
}

// DecodeStanzaFrame rejects extra tokens/elements and reconstructs routing
// only from authenticated stanza/session context, never from frame bytes.
func DecodeStanzaFrame(input []byte, from, to, meshID string, maximumFrameBytes, stanzaBudget int) (Stanza, error) {
	if len(input) == 0 || completeMessageSize(from, to, "", input) > stanzaBudget {
		return Stanza{}, ErrProtocol
	}
	decoder := xml.NewDecoder(bytes.NewReader(input))
	decoder.Strict = true
	var value xmlFrame
	if err := decoder.Decode(&value); err != nil || value.XMLName.Space != AZTMNamespaceV1 || value.XMLName.Local != "frame" || value.Version != "1" || len(value.Unknown) != 0 || !onlyNamespaceAttr(value.UnknownAttrs, AZTMNamespaceV1) {
		return Stanza{}, ErrProtocol
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Stanza{}, ErrProtocol
		}
		if token != nil {
			return Stanza{}, ErrProtocol
		}
	}
	return decodeStanzaFrameValue(value, from, to, meshID, maximumFrameBytes)
}

// decodeStanzaFrameValue validates an already bounded XML child decoded from
// Mellium's authenticated stanza token stream. Keeping this separate avoids
// re-marshalling namespace declaration attributes into duplicate xmlns fields.
func decodeStanzaFrameValue(value xmlFrame, from, to, meshID string, maximumFrameBytes int) (Stanza, error) {
	if value.XMLName.Space != AZTMNamespaceV1 || value.XMLName.Local != "frame" || value.Version != "1" || len(value.Unknown) != 0 || !onlyNamespaceAttr(value.UnknownAttrs, AZTMNamespaceV1) {
		return Stanza{}, ErrProtocol
	}
	if strings.TrimSpace(value.Data) != value.Data {
		return Stanza{}, ErrProtocol
	}
	encoded, err := base64.RawStdEncoding.Strict().DecodeString(value.Data)
	if err != nil || base64.RawStdEncoding.EncodeToString(encoded) != value.Data {
		return Stanza{}, ErrProtocol
	}
	defer clear(encoded)
	frame, err := transport.DecodeControlFrame(encoded, maximumFrameBytes)
	if err != nil {
		return Stanza{}, ErrProtocol
	}
	return frameToStanza(&frame, from, to, meshID)
}

// completeMessageSize budgets the complete client stanza, including routing
// attributes and the wrapper added by Mellium. XML escaping is included.
func completeMessageSize(from, to, id string, child []byte) int {
	escapedFrom, fromOK := escapedXMLStringLength(from)
	escapedTo, toOK := escapedXMLStringLength(to)
	escapedID, idOK := escapedXMLStringLength(id)
	if !fromOK || !toOK || !idOK {
		return int(^uint(0) >> 1)
	}
	// <message xmlns="jabber:client" from="" to="" type="chat"></message>
	fixed := 67
	if id != "" {
		// ` id=""` plus the escaped canonical correlation value.
		fixed += 6
	}
	size, ok := checkedXMLSize(fixed, escapedFrom, escapedTo, escapedID, len(child))
	if !ok {
		return int(^uint(0) >> 1)
	}
	return size
}

func stanzaToFrame(stanza Stanza) (transport.ControlFrame, error) {
	frame := transport.ControlFrame{TransferID: stanza.TransferID, Data: stanza.Data, Evidence: stanza.Evidence}
	switch stanza.Kind {
	case StanzaEnvelope:
		frame.Kind = transport.FrameEnvelope
	case StanzaTransferManifest:
		frame.Kind = transport.FrameTransferManifest
	case StanzaTransferChunk:
		frame.Kind = transport.FrameTransferChunk
	case StanzaTransferFinish:
		frame.Kind = transport.FrameTransferFinish
	case StanzaTransferCompletion:
		frame.Kind = transport.FrameTransferCompletion
	case StanzaTransferAbort:
		frame.Kind = transport.FrameTransferAbort
	case StanzaObjectReadinessRequest:
		frame.Kind = transport.FrameObjectReadinessRequest
	case StanzaObjectReadinessResult:
		frame.Kind = transport.FrameObjectReadinessResult
	case StanzaObjectTransfer:
		frame.Kind = transport.FrameObjectTransfer
	case StanzaObjectTransferFailure:
		frame.Kind = transport.FrameObjectTransferFailure
	case StanzaObjectTransferAbort:
		frame.Kind = transport.FrameObjectTransferAbort
	default:
		return transport.ControlFrame{}, ErrProtocol
	}
	return frame, nil
}

type stanzaFrameMetadataDecoder func(transport.ControlFrame, *Stanza) error

func frameToStanza(frame *transport.ControlFrame, from, to, meshID string) (Stanza, error) {
	return frameToStanzaWithMetadata(frame, from, to, meshID, populateStanzaFrameMetadata)
}

func frameToStanzaWithMetadata(frame *transport.ControlFrame, from, to, meshID string, decodeMetadata stanzaFrameMetadataDecoder) (stanza Stanza, err error) {
	if frame == nil {
		return Stanza{}, ErrProtocol
	}
	transferred := false
	defer func() {
		clearControlFrameOwned(frame)
		if !transferred {
			clearStanzaOwned(&stanza)
		}
	}()
	stanza = Stanza{From: from, To: to, MeshID: meshID, TransferID: frame.TransferID, Evidence: frame.Evidence}
	if decodeMetadata == nil || decodeMetadata(*frame, &stanza) != nil || stanza.Data != nil {
		err = ErrProtocol
		return
	}
	// DecodeControlFrame allocated Data independently. Metadata is validated
	// first, then that allocation is moved instead of cloned a second time.
	stanza.Data = frame.Data
	frame.Data = nil
	transferred = true
	return stanza, nil
}

func populateStanzaFrameMetadata(frame transport.ControlFrame, stanza *Stanza) error {
	switch frame.Kind {
	case transport.FrameEnvelope:
		stanza.Kind = StanzaEnvelope
	case transport.FrameTransferManifest:
		stanza.Kind = StanzaTransferManifest
	case transport.FrameTransferChunk:
		stanza.Kind = StanzaTransferChunk
	case transport.FrameTransferFinish:
		stanza.Kind = StanzaTransferFinish
	case transport.FrameTransferCompletion:
		stanza.Kind = StanzaTransferCompletion
		stanza.MessageID = frame.Evidence.MessageID
	case transport.FrameTransferAbort:
		stanza.Kind = StanzaTransferAbort
	case transport.FrameObjectReadinessRequest:
		stanza.Kind = StanzaObjectReadinessRequest
		request, err := decodeObjectReadinessRequest(frame.Data)
		if err != nil {
			return ErrProtocol
		}
		stanza.AttemptID, stanza.MessageID = request.AttemptID, request.MessageID
	case transport.FrameObjectReadinessResult:
		stanza.Kind = StanzaObjectReadinessResult
		result, err := decodeObjectReadinessResult(frame.Data)
		if err != nil {
			return ErrProtocol
		}
		stanza.AttemptID, stanza.MessageID = result.request.AttemptID, result.request.MessageID
	case transport.FrameObjectTransfer:
		stanza.Kind = StanzaObjectTransfer
		publication, err := decodeObjectPublication(frame.Data)
		if err != nil {
			return ErrProtocol
		}
		defer clearObjectPublicationOwned(&publication)
		stanza.MessageID = publication.Manifest.MessageID
	case transport.FrameObjectTransferFailure:
		stanza.Kind = StanzaObjectTransferFailure
		publication, err := decodeObjectPublication(frame.Data)
		if err != nil {
			return ErrProtocol
		}
		defer clearObjectPublicationOwned(&publication)
		stanza.MessageID = publication.Manifest.MessageID
	case transport.FrameObjectTransferAbort:
		stanza.Kind = StanzaObjectTransferAbort
		publication, err := decodeObjectPublication(frame.Data)
		if err != nil {
			return ErrProtocol
		}
		defer clearObjectPublicationOwned(&publication)
		stanza.MessageID = publication.Manifest.MessageID
	default:
		return ErrProtocol
	}
	return nil
}

func clearControlFrameOwned(frame *transport.ControlFrame) {
	if frame == nil {
		return
	}
	clear(frame.Data)
	clear(frame.ProbeNonce)
	*frame = transport.ControlFrame{}
}
