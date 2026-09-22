package rank1webrtc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

type JingleExchange interface {
	ExchangeJingle(context.Context, string, rank2xmpp.Jingle) (rank2xmpp.Jingle, error)
	SendJingle(context.Context, string, rank2xmpp.Jingle) error
}

type JingleNegotiatorConfig struct {
	LocalIdentity       string
	PeerIdentity        string
	MeshID              string
	SID                 string
	SCTPStreams         uint16
	MaximumMessageBytes int
	Remote              *rank2xmpp.Jingle
}

// JingleNegotiator converts local Pion SDP to the approved Jingle profile and
// back inside the process. Raw SDP is never placed on XMPP.
type JingleNegotiator struct {
	config    JingleNegotiatorConfig
	exchange  JingleExchange
	initiator string
	responder string
	mu        sync.Mutex
	effective int
	binding   [sha256.Size]byte
}

func NewJingleNegotiator(config JingleNegotiatorConfig, exchange JingleExchange) (*JingleNegotiator, error) {
	if protocol.ValidateAgentIdentity(config.LocalIdentity) != nil || protocol.ValidateAgentIdentity(config.PeerIdentity) != nil || protocol.ValidateMeshID(config.MeshID) != nil || !boundToMesh(config.LocalIdentity, config.MeshID) || !boundToMesh(config.PeerIdentity, config.MeshID) || config.SID == "" || len(config.SID) > 128 || config.SCTPStreams == 0 || config.MaximumMessageBytes < transport.MinimumRank1MessageBytes || config.MaximumMessageBytes > transport.MaximumControlFrameBytes || exchange == nil {
		return nil, transport.ErrInvalidConfig
	}
	initiator, responder := config.LocalIdentity, config.PeerIdentity
	if config.Remote != nil {
		initiator, responder = config.Remote.Initiator, config.Remote.Responder
	}
	if initiator == "" || responder == "" || initiator == responder {
		return nil, transport.ErrInvalidConfig
	}
	return &JingleNegotiator{config: config, exchange: exchange, initiator: initiator, responder: responder}, nil
}

func (n *JingleNegotiator) ChannelBinding() ([sha256.Size]byte, bool) {
	if n == nil {
		return [sha256.Size]byte{}, false
	}
	n.mu.Lock()
	binding := n.binding
	n.mu.Unlock()
	return binding, binding != [sha256.Size]byte{}
}

func (n *JingleNegotiator) setBinding(offer, answer rank2xmpp.Jingle) error {
	binding, err := deriveJingleChannelBinding(offer, answer)
	if err != nil {
		return err
	}
	n.mu.Lock()
	if n.binding != [sha256.Size]byte{} && n.binding != binding {
		n.mu.Unlock()
		return transport.ErrProtocol
	}
	n.binding = binding
	n.mu.Unlock()
	return nil
}

func (n *JingleNegotiator) EffectiveMaximumFrameBytes() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.effective
}
func (n *JingleNegotiator) setRemoteMaximum(remote uint32) error {
	if remote < transport.MinimumRank1MessageBytes || remote > transport.MaximumControlFrameBytes {
		return transport.ErrProtocol
	}
	effective := n.config.MaximumMessageBytes
	if int(remote) < effective {
		effective = int(remote)
	}
	if effective < transport.MinimumRank1MessageBytes {
		return transport.ErrProtocol
	}
	n.mu.Lock()
	n.effective = effective
	n.mu.Unlock()
	return nil
}

func (n *JingleNegotiator) Negotiate(ctx context.Context, pc *webrtc.PeerConnection, initiator bool) error {
	if n == nil || ctx == nil || pc == nil {
		return transport.ErrInvalidConfig
	}
	if initiator {
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			return transport.ErrUnavailable
		}
		gathered := webrtc.GatheringCompletePromise(pc)
		if err = pc.SetLocalDescription(offer); err != nil {
			return transport.ErrUnavailable
		}
		select {
		case <-gathered:
		case <-ctx.Done():
			return ctx.Err()
		}
		local := pc.LocalDescription()
		if local == nil {
			return transport.ErrProtocol
		}
		signal, err := descriptionToJingleWithRoles(*local, "session-initiate", n.config, n.initiator, n.responder)
		if err != nil {
			return err
		}
		answer, err := n.exchange.ExchangeJingle(ctx, n.config.PeerIdentity, signal)
		if err != nil {
			return err
		}
		if answer.Action != "session-accept" || answer.SID != n.config.SID || answer.Initiator != n.config.LocalIdentity || answer.Responder != n.config.PeerIdentity {
			return transport.ErrProtocol
		}
		if !sameJingleGroup(signal, answer) {
			return transport.ErrProtocol
		}
		if err = n.setRemoteMaximum(answer.Content.Description.MaximumMessageSize); err != nil {
			return err
		}
		remote, err := jingleToDescription(answer, webrtc.SDPTypeAnswer)
		if err != nil {
			return err
		}
		if err = pc.SetRemoteDescription(remote); err != nil {
			return normalize(err, ctx, transport.ErrProtocol)
		}
		return n.setBinding(signal, answer)
	}
	if n.config.Remote == nil {
		return transport.ErrProtocol
	}
	offer := *n.config.Remote
	if offer.Action != "session-initiate" || offer.SID != n.config.SID || offer.Initiator != n.config.PeerIdentity || offer.Responder != n.config.LocalIdentity || offer.Content.Description.MeshID != n.config.MeshID {
		return transport.ErrProtocol
	}
	if err := n.setRemoteMaximum(offer.Content.Description.MaximumMessageSize); err != nil {
		return err
	}
	remote, err := jingleToDescription(offer, webrtc.SDPTypeOffer)
	if err != nil {
		return err
	}
	if err = pc.SetRemoteDescription(remote); err != nil {
		return transport.ErrProtocol
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return transport.ErrUnavailable
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(answer); err != nil {
		return transport.ErrUnavailable
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return ctx.Err()
	}
	local := pc.LocalDescription()
	if local == nil {
		return transport.ErrProtocol
	}
	signal, err := descriptionToJingleWithRoles(*local, "session-accept", n.config, n.initiator, n.responder)
	if err != nil {
		return err
	}
	if !sameJingleGroup(offer, signal) {
		return transport.ErrProtocol
	}
	if err = n.exchange.SendJingle(ctx, n.config.PeerIdentity, signal); err != nil {
		return err
	}
	return n.setBinding(offer, signal)
}

// Restart performs a complete-gathering ICE restart on the existing Jingle
// session. The authenticated data-channel binding must remain unchanged.
func (n *JingleNegotiator) Restart(ctx context.Context, pc *webrtc.PeerConnection) error {
	if n == nil || ctx == nil || pc == nil {
		return transport.ErrInvalidConfig
	}
	offer, err := pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	if err != nil {
		return transport.ErrUnavailable
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return transport.ErrUnavailable
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return ctx.Err()
	}
	local := pc.LocalDescription()
	if local == nil {
		return transport.ErrProtocol
	}
	signal, err := descriptionToJingleWithRoles(*local, "transport-replace", n.config, n.initiator, n.responder)
	if err != nil {
		return err
	}
	answer, err := n.exchange.ExchangeJingle(ctx, n.config.PeerIdentity, signal)
	if err != nil {
		return err
	}
	if answer.Action != "transport-accept" || !sameJingleGroup(signal, answer) {
		return transport.ErrProtocol
	}
	if err = n.setRemoteMaximum(answer.Content.Description.MaximumMessageSize); err != nil {
		return err
	}
	remote, err := jingleToDescription(answer, webrtc.SDPTypeAnswer)
	if err != nil {
		return err
	}
	if err = pc.SetRemoteDescription(remote); err != nil {
		return normalize(err, ctx, transport.ErrProtocol)
	}
	return n.setBinding(signal, answer)
}

// AcceptRestart applies an authenticated transport-replace offer and returns
// its transport-accept answer on the same bounded signaling path.
func (n *JingleNegotiator) AcceptRestart(ctx context.Context, pc *webrtc.PeerConnection, offer rank2xmpp.Jingle) error {
	if n == nil || ctx == nil || pc == nil || offer.Action != "transport-replace" || offer.SID != n.config.SID || offer.Initiator != n.initiator || offer.Responder != n.responder || offer.Content.Description.MeshID != n.config.MeshID {
		return transport.ErrProtocol
	}
	if err := n.setRemoteMaximum(offer.Content.Description.MaximumMessageSize); err != nil {
		return err
	}
	remote, err := jingleToDescription(offer, webrtc.SDPTypeOffer)
	if err != nil {
		return err
	}
	if err = pc.SetRemoteDescription(remote); err != nil {
		return normalize(err, ctx, transport.ErrProtocol)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return transport.ErrUnavailable
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(answer); err != nil {
		return transport.ErrUnavailable
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return ctx.Err()
	}
	local := pc.LocalDescription()
	if local == nil {
		return transport.ErrProtocol
	}
	signal, err := descriptionToJingleWithRoles(*local, "transport-accept", n.config, n.initiator, n.responder)
	if err != nil || !sameJingleGroup(offer, signal) {
		return transport.ErrProtocol
	}
	if err = n.exchange.SendJingle(ctx, n.config.PeerIdentity, signal); err != nil {
		return err
	}
	return n.setBinding(offer, signal)
}

func descriptionToJingle(description webrtc.SessionDescription, action string, config JingleNegotiatorConfig) (rank2xmpp.Jingle, error) {
	initiator, responder := config.LocalIdentity, config.PeerIdentity
	if action == "session-accept" {
		initiator, responder = config.PeerIdentity, config.LocalIdentity
	}
	return descriptionToJingleWithRoles(description, action, config, initiator, responder)
}

func descriptionToJingleWithRoles(description webrtc.SessionDescription, action string, config JingleNegotiatorConfig, initiator, responder string) (rank2xmpp.Jingle, error) {
	if ((action != "session-initiate" && action != "transport-replace") || description.Type != webrtc.SDPTypeOffer) && ((action != "session-accept" && action != "transport-accept") || description.Type != webrtc.SDPTypeAnswer) {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	var parsed sdp.SessionDescription
	if err := parsed.UnmarshalString(description.SDP); err != nil || len(parsed.MediaDescriptions) != 1 {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	media := parsed.MediaDescriptions[0]
	if media.MediaName.Media != "application" || len(media.MediaName.Formats) != 1 || media.MediaName.Formats[0] != "webrtc-datachannel" {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	ufrag, ok1 := media.Attribute("ice-ufrag")
	password, ok2 := media.Attribute("ice-pwd")
	fingerprint, ok3 := media.Attribute("fingerprint")
	if !ok3 {
		fingerprint, ok3 = parsed.Attribute("fingerprint")
	}
	setup, ok4 := media.Attribute("setup")
	portText, ok5 := media.Attribute("sctp-port")
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	fp := strings.SplitN(fingerprint, " ", 2)
	port, err := strconv.ParseUint(portText, 10, 16)
	if len(fp) != 2 || fp[0] != "sha-256" || err != nil || port == 0 {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	candidates := make([]rank2xmpp.ICECandidate, 0)
	for _, attr := range media.Attributes {
		if attr.Key == "candidate" {
			candidate, err := parseCandidate(attr.Value, ufrag)
			if err != nil {
				return rank2xmpp.Jingle{}, err
			}
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 || len(candidates) > rank2xmpp.MaximumJingleCandidates {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	signal := rank2xmpp.Jingle{XMLName: xmlName(rank2xmpp.JingleNamespace, "jingle"), Action: action, Initiator: initiator, Responder: responder, SID: config.SID, Content: rank2xmpp.JingleContent{Creator: "initiator", Name: "data", Description: rank2xmpp.DataChannelDescription{XMLName: xmlName(rank2xmpp.AZTMDataChannelNamespace, "description"), Media: "application", MeshID: config.MeshID, MaximumMessageSize: uint32(config.MaximumMessageBytes)}, Transport: rank2xmpp.ICETransport{XMLName: xmlName(rank2xmpp.ICEUDPNamespace, "transport"), Ufrag: ufrag, Password: password, Candidates: candidates, Fingerprint: rank2xmpp.DTLSFingerprint{XMLName: xmlName(rank2xmpp.DTLSNamespace, "fingerprint"), Hash: "sha-256", Setup: setup, Value: fp[1]}, SCTP: rank2xmpp.SCTPMap{XMLName: xmlName(rank2xmpp.SCTPNamespace, "sctpmap"), Number: uint16(port), Protocol: "webrtc-datachannel", Streams: config.SCTPStreams}}}}
	if _, err := rank2xmpp.EncodeJingle(signal); err != nil {
		return rank2xmpp.Jingle{}, transport.ErrProtocol
	}
	return signal, nil
}

func sameJingleGroup(left, right rank2xmpp.Jingle) bool {
	return left.SID == right.SID && left.Initiator == right.Initiator && left.Responder == right.Responder && left.Content.Description.MeshID == right.Content.Description.MeshID
}

func deriveJingleChannelBinding(offer, answer rank2xmpp.Jingle) ([sha256.Size]byte, error) {
	initial := offer.Action == "session-initiate" && answer.Action == "session-accept"
	restart := offer.Action == "transport-replace" && answer.Action == "transport-accept"
	if (!initial && !restart) || !sameJingleGroup(offer, answer) {
		return [sha256.Size]byte{}, transport.ErrProtocol
	}
	description := offer.Content.Description
	if description.MeshID == "" {
		return [sha256.Size]byte{}, transport.ErrProtocol
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("CYNAPSA-RANK1-JINGLE-BINDING-V2\x00"))
	writeBindingText(hash, description.MeshID)
	for _, value := range []string{offer.SID, offer.Initiator, offer.Responder, offer.Content.Transport.Fingerprint.Value, answer.Content.Transport.Fingerprint.Value} {
		writeBindingText(hash, value)
	}
	var binding [sha256.Size]byte
	copy(binding[:], hash.Sum(nil))
	if binding == [sha256.Size]byte{} {
		return [sha256.Size]byte{}, transport.ErrProtocol
	}
	return binding, nil
}

type bindingHash interface{ Write([]byte) (int, error) }

func writeBindingText(hash bindingHash, value string) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write([]byte(value))
}

func boundToMesh(identity, meshID string) bool {
	return protocol.ValidateBoundSessionIdentity(identity, meshID) == nil
}

func jingleToDescription(signal rank2xmpp.Jingle, typ webrtc.SDPType) (webrtc.SessionDescription, error) {
	if _, err := rank2xmpp.EncodeJingle(signal); err != nil {
		return webrtc.SessionDescription{}, transport.ErrProtocol
	}
	if (typ != webrtc.SDPTypeOffer || signal.Action != "session-initiate" && signal.Action != "transport-replace") && (typ != webrtc.SDPTypeAnswer || signal.Action != "session-accept" && signal.Action != "transport-accept") {
		return webrtc.SessionDescription{}, transport.ErrProtocol
	}
	description, err := sdp.NewJSEPSessionDescription(false)
	if err != nil {
		return webrtc.SessionDescription{}, transport.ErrUnavailable
	}
	media := &sdp.MediaDescription{MediaName: sdp.MediaName{Media: "application", Port: sdp.RangedPort{Value: 9}, Protos: []string{"UDP", "DTLS", "SCTP"}, Formats: []string{"webrtc-datachannel"}}, ConnectionInformation: &sdp.ConnectionInformation{NetworkType: "IN", AddressType: "IP4", Address: &sdp.Address{Address: "0.0.0.0"}}}
	media.WithValueAttribute("ice-ufrag", signal.Content.Transport.Ufrag).WithValueAttribute("ice-pwd", signal.Content.Transport.Password).WithValueAttribute("fingerprint", signal.Content.Transport.Fingerprint.Hash+" "+signal.Content.Transport.Fingerprint.Value).WithValueAttribute("setup", signal.Content.Transport.Fingerprint.Setup).WithValueAttribute("mid", "0").WithValueAttribute("sctp-port", strconv.Itoa(int(signal.Content.Transport.SCTP.Number)))
	media.WithValueAttribute("max-message-size", strconv.FormatUint(uint64(signal.Content.Description.MaximumMessageSize), 10))
	for _, candidate := range signal.Content.Transport.Candidates {
		media.WithValueAttribute("candidate", formatCandidate(candidate, signal.Content.Transport.Ufrag))
	}
	media.WithPropertyAttribute("end-of-candidates")
	description.WithValueAttribute("group", "BUNDLE 0").WithPropertyAttribute("extmap-allow-mixed").WithMedia(media)
	encoded, err := description.Marshal()
	if err != nil {
		return webrtc.SessionDescription{}, transport.ErrProtocol
	}
	defer clear(encoded)
	return webrtc.SessionDescription{Type: typ, SDP: string(encoded)}, nil
}

func parseCandidate(value, transportUfrag string) (rank2xmpp.ICECandidate, error) {
	fields := strings.Fields(strings.TrimPrefix(value, "candidate:"))
	if len(fields) < 8 || (len(fields)-8)%2 != 0 || strings.ToLower(fields[2]) != "udp" || fields[6] != "typ" {
		return rank2xmpp.ICECandidate{}, transport.ErrProtocol
	}
	component, e1 := strconv.ParseUint(fields[1], 10, 16)
	priority, e2 := strconv.ParseUint(fields[3], 10, 32)
	port, e3 := strconv.ParseUint(fields[5], 10, 16)
	if e1 != nil || e2 != nil || e3 != nil || net.ParseIP(fields[4]) == nil {
		return rank2xmpp.ICECandidate{}, transport.ErrProtocol
	}
	h := sha256.Sum256([]byte(value))
	candidate := rank2xmpp.ICECandidate{Component: uint16(component), Foundation: fields[0], Generation: 0, ID: "cand_" + base64.RawURLEncoding.EncodeToString(h[:16]), IP: fields[4], Port: uint16(port), Priority: uint32(priority), Protocol: "udp", Type: fields[7]}
	for i := 8; i+1 < len(fields); i += 2 {
		switch fields[i] {
		case "raddr":
			candidate.RelAddr = fields[i+1]
		case "rport":
			parsed, err := strconv.ParseUint(fields[i+1], 10, 16)
			if err != nil {
				return rank2xmpp.ICECandidate{}, transport.ErrProtocol
			}
			candidate.RelPort = uint16(parsed)
		case "generation":
			parsed, err := strconv.ParseUint(fields[i+1], 10, 16)
			if err != nil {
				return rank2xmpp.ICECandidate{}, transport.ErrProtocol
			}
			candidate.Generation = uint16(parsed)
		case "network-cost":
			if _, err := strconv.ParseUint(fields[i+1], 10, 32); err != nil {
				return rank2xmpp.ICECandidate{}, transport.ErrProtocol
			}
		case "ufrag":
			if fields[i+1] == "" || fields[i+1] != transportUfrag {
				return rank2xmpp.ICECandidate{}, transport.ErrProtocol
			}
		default:
			return rank2xmpp.ICECandidate{}, transport.ErrProtocol
		}
	}
	if (candidate.RelAddr == "") != (candidate.RelPort == 0) {
		return rank2xmpp.ICECandidate{}, transport.ErrProtocol
	}
	return candidate, nil
}

func formatCandidate(candidate rank2xmpp.ICECandidate, transportUfrag string) string {
	value := fmt.Sprintf("%s %d udp %d %s %d typ %s", candidate.Foundation, candidate.Component, candidate.Priority, candidate.IP, candidate.Port, candidate.Type)
	if candidate.RelAddr != "" {
		value += fmt.Sprintf(" raddr %s rport %d", candidate.RelAddr, candidate.RelPort)
	}
	value += fmt.Sprintf(" generation %d ufrag %s", candidate.Generation, transportUfrag)
	return value
}

func xmlName(space, local string) xml.Name { return xml.Name{Space: space, Local: local} }

var _ PionNegotiator = (*JingleNegotiator)(nil)
var _ pionRestartNegotiator = (*JingleNegotiator)(nil)
