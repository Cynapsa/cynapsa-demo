package rank2xmpp

import (
	"context"
	"encoding/xml"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// A routing denial is a server-originated message error, not a peer message.
// It reports neither application delivery nor new peer authority. In
// particular, it must not turn a stale destination into a local XMPP outage.
func (s *melliumSession) handleServerMessageError(ctx context.Context, source xml.TokenReader, outer xml.StartElement, message stanza.Message) error {
	if s == nil || ctx == nil || source == nil || message.Type != stanza.ErrorMessage {
		return ErrInvalidConfig
	}
	local, err := jid.Parse(s.username + "/" + s.boundResource())
	if err != nil || local.Localpart() == "" || local.Resourcepart() == "" {
		return ErrIdentityBinding
	}
	server := local.Domain()
	if outer.Name != (xml.Name{Space: stanza.NSClient, Local: "message"}) ||
		message.From.String() != server.String() || message.To.String() != local.String() ||
		!validServerMessageErrorAttrs(outer.Attr, server, local) {
		emitInboundAuthRejection(ctx, outer, "server_message_error_address")
		return ErrAuthentication
	}
	// The server strips the denied message's content. Reuse the bounded stanza
	// error decoder, which accepts exactly one error child and no application
	// payload. A malformed error is not a trusted routing outcome.
	condition, err := decodeJingleIQErrorCondition(source, outer)
	if err != nil {
		return err
	}
	s.mu.Lock()
	management, generation := s.management, s.generation
	s.mu.Unlock()
	contextGeneration, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	if management == nil || !ok || contextGeneration == 0 || contextGeneration != generation {
		return ErrStreamManagement
	}
	if validReadinessTyped(message.ID, "msg_", 16) {
		if err := s.emit(ctx, Event{Kind: EventRoutingFailure, MessageID: message.ID, ErrorCondition: condition}); err != nil {
			return err
		}
	}
	management.MarkHandledInbound()
	// Future telemetry can count this server denial by bounded stanza ID.
	emitRank2Evidence(rank2EvidenceRecord{Event: "server_message_error_consumed", Source: "control", Stage: "routing_denied", StanzaID: message.ID, Handled: true}, nil)
	return nil
}

func validServerMessageErrorAttrs(attrs []xml.Attr, server, local jid.JID) bool {
	seen := make(map[string]bool, 6)
	for _, attr := range attrs {
		key := attr.Name.Space + ":" + attr.Name.Local
		if seen[key] {
			return false
		}
		seen[key] = true
		switch attr.Name {
		case xml.Name{Local: "xmlns"}:
			if attr.Value != stanza.NSClient {
				return false
			}
		case xml.Name{Space: "http://www.w3.org/XML/1998/namespace", Local: "lang"}:
			if !validEntityTimeLanguage(attr.Value) {
				return false
			}
		case xml.Name{Local: "id"}:
			if attr.Value == "" || len(attr.Value) > 256 {
				return false
			}
		case xml.Name{Local: "type"}:
			if attr.Value != string(stanza.ErrorMessage) {
				return false
			}
		case xml.Name{Local: "from"}:
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != server.String() {
				return false
			}
		case xml.Name{Local: "to"}:
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != local.String() {
				return false
			}
		default:
			return false
		}
	}
	return seen[":type"] && seen[":from"] && seen[":to"]
}
