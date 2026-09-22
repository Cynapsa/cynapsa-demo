package rank2xmpp

import (
	"bytes"
	"encoding/xml"
	"io"
	"net"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const (
	JingleNamespace          = "urn:xmpp:jingle:1"
	ICEUDPNamespace          = "urn:xmpp:jingle:transports:ice-udp:1"
	DTLSNamespace            = "urn:xmpp:jingle:apps:dtls:0"
	SCTPNamespace            = "urn:xmpp:jingle:transports:dtls-sctp:1"
	AZTMDataChannelNamespace = AZTMNamespaceV1
	MaximumJingleCandidates  = 256
)

type Jingle struct {
	XMLName      xml.Name      `xml:"urn:xmpp:jingle:1 jingle"`
	Action       string        `xml:"action,attr"`
	Initiator    string        `xml:"initiator,attr,omitempty"`
	Responder    string        `xml:"responder,attr,omitempty"`
	SID          string        `xml:"sid,attr"`
	Content      JingleContent `xml:"content"`
	Unknown      []xmlUnknown  `xml:",any"`
	UnknownAttrs []xml.Attr    `xml:",any,attr"`
}
type JingleContent struct {
	Creator      string                 `xml:"creator,attr"`
	Name         string                 `xml:"name,attr"`
	Description  DataChannelDescription `xml:"urn:cynapsa:aztm:1 description"`
	Transport    ICETransport           `xml:"urn:xmpp:jingle:transports:ice-udp:1 transport"`
	Unknown      []xmlUnknown           `xml:",any"`
	UnknownAttrs []xml.Attr             `xml:",any,attr"`
}
type DataChannelDescription struct {
	XMLName            xml.Name     `xml:"urn:cynapsa:aztm:1 description"`
	Media              string       `xml:"media,attr"`
	MeshID             string       `xml:"mesh,attr"`
	MaximumMessageSize uint32       `xml:"max-message-size,attr"`
	Unknown            []xmlUnknown `xml:",any"`
	UnknownAttrs       []xml.Attr   `xml:",any,attr"`
}
type ICETransport struct {
	XMLName      xml.Name        `xml:"urn:xmpp:jingle:transports:ice-udp:1 transport"`
	Ufrag        string          `xml:"ufrag,attr"`
	Password     string          `xml:"pwd,attr"`
	Candidates   []ICECandidate  `xml:"candidate"`
	Fingerprint  DTLSFingerprint `xml:"urn:xmpp:jingle:apps:dtls:0 fingerprint"`
	SCTP         SCTPMap         `xml:"urn:xmpp:jingle:transports:dtls-sctp:1 sctpmap"`
	Unknown      []xmlUnknown    `xml:",any"`
	UnknownAttrs []xml.Attr      `xml:",any,attr"`
}
type ICECandidate struct {
	Component  uint16     `xml:"component,attr"`
	Foundation string     `xml:"foundation,attr"`
	Generation uint16     `xml:"generation,attr"`
	ID         string     `xml:"id,attr"`
	IP         string     `xml:"ip,attr"`
	Network    uint16     `xml:"network,attr,omitempty"`
	Port       uint16     `xml:"port,attr"`
	Priority   uint32     `xml:"priority,attr"`
	Protocol   string     `xml:"protocol,attr"`
	Type       string     `xml:"type,attr"`
	RelAddr    string     `xml:"rel-addr,attr,omitempty"`
	RelPort    uint16     `xml:"rel-port,attr,omitempty"`
	Unknown    []xml.Attr `xml:",any,attr"`
}
type DTLSFingerprint struct {
	XMLName xml.Name   `xml:"urn:xmpp:jingle:apps:dtls:0 fingerprint"`
	Hash    string     `xml:"hash,attr"`
	Setup   string     `xml:"setup,attr"`
	Value   string     `xml:",chardata"`
	Unknown []xml.Attr `xml:",any,attr"`
}
type SCTPMap struct {
	XMLName  xml.Name   `xml:"urn:xmpp:jingle:transports:dtls-sctp:1 sctpmap"`
	Number   uint16     `xml:"number,attr"`
	Protocol string     `xml:"protocol,attr"`
	Streams  uint16     `xml:"streams,attr"`
	Unknown  []xml.Attr `xml:",any,attr"`
}

func EncodeJingle(signal Jingle) ([]byte, error) {
	if !validJingle(signal) {
		return nil, ErrProtocol
	}
	// Namespace declarations retained by a token-stream decode are already
	// represented by the typed XML names. Clear the validated declarations so
	// xml.Marshal cannot emit duplicate xmlns attributes.
	signal.UnknownAttrs = nil
	signal.Content.Description.UnknownAttrs = nil
	signal.Content.Transport.UnknownAttrs = nil
	signal.Content.Transport.Fingerprint.Unknown = nil
	signal.Content.Transport.SCTP.Unknown = nil
	encoded, err := xml.Marshal(signal)
	if err != nil || len(encoded) > MaximumSignalBytes {
		clear(encoded)
		return nil, ErrProtocol
	}
	return encoded, nil
}

func decodeJingleValue(signal Jingle) (Jingle, error) {
	if !validJingle(signal) {
		return Jingle{}, ErrProtocol
	}
	return signal, nil
}
func DecodeJingle(encoded []byte) (Jingle, error) {
	if len(encoded) == 0 || len(encoded) > MaximumSignalBytes {
		return Jingle{}, ErrProtocol
	}
	decoder := xml.NewDecoder(bytes.NewReader(encoded))
	decoder.Strict = true
	var signal Jingle
	if err := decoder.Decode(&signal); err != nil {
		return Jingle{}, ErrProtocol
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return Jingle{}, ErrProtocol
	}
	return decodeJingleValue(signal)
}
func validJingle(s Jingle) bool {
	if !onlyNamespaceAttr(s.UnknownAttrs, JingleNamespace) || len(s.Content.UnknownAttrs) > 0 || !onlyNamespaceAttr(s.Content.Description.UnknownAttrs, AZTMDataChannelNamespace) || !onlyNamespaceAttr(s.Content.Transport.UnknownAttrs, ICEUDPNamespace) {
		return false
	}
	description := s.Content.Description
	if s.XMLName.Space != JingleNamespace || s.XMLName.Local != "jingle" || len(s.Unknown) > 0 || len(s.Content.Unknown) > 0 || protocol.ValidateAgentIdentity(s.Initiator) != nil || protocol.ValidateAgentIdentity(s.Responder) != nil || s.SID == "" || len(s.SID) > 128 || s.Content.Creator != "initiator" || s.Content.Name != "data" || description.XMLName.Space != AZTMDataChannelNamespace || description.Media != "application" || protocol.ValidateMeshID(description.MeshID) != nil || !validJingleBoundIdentity(s.Initiator, description.MeshID) || !validJingleBoundIdentity(s.Responder, description.MeshID) || description.MaximumMessageSize < transport.MinimumRank1MessageBytes || description.MaximumMessageSize > transport.MaximumControlFrameBytes || len(description.Unknown) > 0 || s.Content.Transport.XMLName.Space != ICEUDPNamespace || s.Content.Transport.Ufrag == "" || len(s.Content.Transport.Ufrag) > 256 || s.Content.Transport.Password == "" || len(s.Content.Transport.Password) > 256 || len(s.Content.Transport.Candidates) > MaximumJingleCandidates || s.Content.Transport.Fingerprint.XMLName.Space != DTLSNamespace || s.Content.Transport.Fingerprint.Hash != "sha-256" || !validSetup(s.Content.Transport.Fingerprint.Setup) || !validFingerprint(s.Content.Transport.Fingerprint.Value) || !onlyNamespaceAttr(s.Content.Transport.Fingerprint.Unknown, DTLSNamespace) || s.Content.Transport.SCTP.XMLName.Space != SCTPNamespace || s.Content.Transport.SCTP.Number == 0 || s.Content.Transport.SCTP.Protocol != "webrtc-datachannel" || s.Content.Transport.SCTP.Streams == 0 || !onlyNamespaceAttr(s.Content.Transport.SCTP.Unknown, SCTPNamespace) {
		return false
	}
	if s.Action != "session-initiate" && s.Action != "session-accept" && s.Action != "transport-replace" && s.Action != "transport-accept" && s.Action != "transport-info" && s.Action != "session-terminate" {
		return false
	}
	for _, candidate := range s.Content.Transport.Candidates {
		if candidate.Component == 0 || candidate.Foundation == "" || len(candidate.Foundation) > 256 || candidate.ID == "" || len(candidate.ID) > 128 || net.ParseIP(candidate.IP) == nil || candidate.Port == 0 || candidate.Priority == 0 || candidate.Protocol != "udp" || !validCandidateType(candidate.Type) || len(candidate.Unknown) > 0 {
			return false
		}
		if candidate.RelAddr != "" && net.ParseIP(candidate.RelAddr) == nil {
			return false
		}
		if (candidate.RelAddr == "") != (candidate.RelPort == 0) {
			return false
		}
	}
	return true
}

func validJingleBoundIdentity(identity, meshID string) bool {
	return protocol.ValidateBoundSessionIdentity(identity, meshID) == nil
}

func onlyNamespaceAttr(attrs []xml.Attr, namespace string) bool {
	return len(attrs) == 0 || len(attrs) == 1 && attrs[0].Name.Space == "" && attrs[0].Name.Local == "xmlns" && attrs[0].Value == namespace
}
func validCandidateType(value string) bool {
	return value == "host" || value == "srflx" || value == "prflx" || value == "relay"
}
func validSetup(value string) bool {
	return value == "actpass" || value == "active" || value == "passive"
}
func validFingerprint(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 32 {
		return false
	}
	for _, part := range parts {
		if len(part) != 2 || !strings.Contains("0123456789ABCDEF", part[:1]) || !strings.Contains("0123456789ABCDEF", part[1:]) {
			return false
		}
	}
	return true
}
