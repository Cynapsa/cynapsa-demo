package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"strings"
	"time"
	"unicode/utf8"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	authorityDiscoveryNamespace = "http://jabber.org/protocol/disco#info"
	authorityFeatureNamespace   = "urn:cynapsa:mesh-authority:1"
	authorityFeaturePrefix      = "urn:cynapsa:mesh-authority:"
	authorityDiscoveryIDPrefix  = "cynapsa-authority-disco-"
	authorityDiscoveryTimeout   = 5 * time.Second
	maximumDiscoFeatureBytes    = 2 << 10
)

// DiscoverAuthority verifies the versioned Cynapsa authority feature on one
// fresh, exact-resource, XEP-0198-enabled session before snapshot sync begins.
func (s *melliumSession) DiscoverAuthority(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return err
	}
	defer s.releaseCorrelated()

	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return ErrUnavailable
	}
	local, server := session.LocalAddr(), session.LocalAddr().Domain()
	if local.Localpart() == "" || local.Resourcepart() != s.boundResource() || server.Localpart() != "" || server.Resourcepart() != "" || server.Domainpart() == "" {
		return ErrIdentityBinding
	}

	random := rand.Text()
	if len(random) < privateIQRandomLength {
		return ErrProtocol
	}
	id := authorityDiscoveryIDPrefix + random[:privateIQRandomLength]
	record := Stanza{
		Kind:      StanzaAuthorityDiscovery,
		From:      local.String(),
		To:        server.String(),
		MeshID:    s.meshID,
		MessageID: id,
	}
	query := xml.StartElement{Name: xml.Name{Space: authorityDiscoveryNamespace, Local: "query"}}
	iq := stanza.IQ{
		XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"},
		ID:      id,
		To:      server,
		Type:    stanza.GetIQ,
	}
	response, sequence, err := s.sendTrackedIQElement(
		ctx, session, management, record, xmlstream.Wrap(nil, query), iq,
	)
	if err != nil {
		return err
	}
	if response == nil {
		return ErrProtocol
	}
	defer response.Close()

	correlated, handled, decodeErr := decodeAuthorityDiscovery(
		response, id, server, local, s.config.StanzaBudgetBytes, true,
	)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	return decodeErr
}

// decodeAuthorityDiscovery consumes one strictly correlated disco#info IQ and
// accepts exactly one feature for the supported authority protocol version.
func decodeAuthorityDiscovery(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, budget int, mellium bool) (bool, bool, error) {
	if source == nil || expectedID == "" || expectedFrom.String() == "" || expectedTo.String() == "" || budget <= 0 {
		return false, false, ErrProtocol
	}
	token, err := source.Token()
	if err != nil {
		return false, false, ErrProtocol
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return false, false, ErrProtocol
	}
	iqType, ok := validCorrelatedIQAttrs(outer.Attr, expectedID, expectedFrom, expectedTo)
	if !ok {
		return false, false, ErrProtocol
	}
	bounded, err := newStanzaBudget(source, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return true, false, ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	if iqType == stanza.ErrorIQ {
		decoded, valid := decodePrivateIQError(reader, outer, mellium)
		return true, valid, decoded
	}

	token, err = nextPrivateToken(reader)
	query, ok := token.(xml.StartElement)
	if err != nil || !ok || query.Name != (xml.Name{Space: authorityDiscoveryNamespace, Local: "query"}) {
		return true, false, ErrProtocol
	}
	if _, ok := exactPrivateAttrs(query.Attr, nil, authorityDiscoveryNamespace); !ok {
		return true, false, ErrProtocol
	}
	authorityFeatures := 0
	for {
		token, err = nextPrivateToken(reader)
		if err != nil {
			return true, false, ErrProtocol
		}
		switch value := token.(type) {
		case xml.StartElement:
			switch value.Name {
			case xml.Name{Space: authorityDiscoveryNamespace, Local: "feature"}:
				attrs, valid := exactPrivateAttrs(value.Attr, []string{"var"}, authorityDiscoveryNamespace)
				feature := attrs["var"]
				if !valid || feature == "" || len(feature) > maximumDiscoFeatureBytes || !utf8.ValidString(feature) {
					return true, false, ErrProtocol
				}
				end, endErr := nextPrivateToken(reader)
				if endErr != nil || end != value.End() {
					return true, false, ErrProtocol
				}
				if strings.HasPrefix(feature, authorityFeaturePrefix) {
					if feature != authorityFeatureNamespace {
						return true, false, ErrProtocol
					}
					authorityFeatures++
				}
			case xml.Name{Space: authorityDiscoveryNamespace, Local: "identity"}:
				if !validDiscoIdentity(value.Attr) {
					return true, false, ErrProtocol
				}
				end, endErr := nextPrivateToken(reader)
				if endErr != nil || end != value.End() {
					return true, false, ErrProtocol
				}
			case xml.Name{Space: "jabber:x:data", Local: "x"}:
				if err := skipPrivateElement(reader, value); err != nil {
					return true, false, ErrProtocol
				}
			default:
				return true, false, ErrProtocol
			}
		case xml.EndElement:
			if value != query.End() || authorityFeatures != 1 {
				return true, false, ErrProtocol
			}
			if err := consumePrivateIQEnd(reader, outer, mellium); err != nil {
				return true, false, ErrProtocol
			}
			return true, true, nil
		default:
			return true, false, ErrProtocol
		}
	}
}

func validDiscoIdentity(attrs []xml.Attr) bool {
	seen := map[string]bool{}
	for _, attr := range attrs {
		if (attr.Name.Space == "xmlns" || attr.Name.Space == "" && attr.Name.Local == "xmlns") && attr.Value == authorityDiscoveryNamespace {
			continue
		}
		key := attr.Name.Local
		if attr.Name.Space == "http://www.w3.org/XML/1998/namespace" && key == "lang" {
			key = "xml:lang"
		} else if attr.Name.Space != "" {
			return false
		}
		if seen[key] || key != "category" && key != "type" && key != "name" && key != "xml:lang" || attr.Value == "" || len(attr.Value) > maximumDiscoFeatureBytes || !utf8.ValidString(attr.Value) {
			return false
		}
		seen[key] = true
	}
	return seen["category"] && seen["type"]
}
