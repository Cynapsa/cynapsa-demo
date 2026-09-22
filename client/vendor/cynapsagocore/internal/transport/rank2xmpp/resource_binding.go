package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"io"
	"strings"

	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const resourceBindingNamespace = "urn:ietf:params:xml:ns:xmpp-bind"

const stanzaErrorNamespace = "urn:ietf:params:xml:ns:xmpp-stanzas"

var xmlLanguageName = xml.Name{Space: "http://www.w3.org/XML/1998/namespace", Local: "lang"}

// exactResourceBinding is the legacy XMPP resource-binding feature with an
// explicit client resource encoder. Mellium v0.23.0's BindResource client
// encoder emits its empty JID field where the requested resource belongs,
// causing compliant servers to assign a different resource. Keep this local
// adapter until the pinned dependency provides an equivalent corrected API.
func exactResourceBinding(resource string) xmpp.StreamFeature {
	return resourceBindingFeature(resource, rand.Text)
}

func resourceBindingFeature(resource string, requestID func() string) xmpp.StreamFeature {
	return xmpp.StreamFeature{
		Name:       xml.Name{Space: resourceBindingNamespace, Local: "bind"},
		Necessary:  xmpp.Authn,
		Prohibited: xmpp.Ready,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			if start == nil || start.Name != (xml.Name{Space: resourceBindingNamespace, Local: "bind"}) {
				return true, nil, ErrIdentityBinding
			}
			var advertised struct {
				XMLName xml.Name `xml:"urn:ietf:params:xml:ns:xmpp-bind bind"`
			}
			return true, nil, decoder.DecodeElement(&advertised, start)
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			if session == nil || resource == "" || requestID == nil || session.LocalAddr().Resourcepart() != resource {
				return 0, nil, ErrIdentityBinding
			}
			id := requestID()
			if id == "" {
				return 0, nil, ErrIdentityBinding
			}
			writer := session.TokenWriter()
			if err := writeExactBindRequest(writer, id, resource); err != nil {
				_ = writer.Close()
				return 0, nil, err
			}
			_ = writer.Close()

			reader := session.TokenReader()
			defer reader.Close()
			bound, err := readExactBindResponse(reader, id)
			if err != nil {
				return 0, nil, err
			}
			if !session.UpdateAddr(bound) {
				return 0, nil, ErrIdentityBinding
			}
			return xmpp.Ready, nil, nil
		},
	}
}

func writeExactBindRequest(writer xmlstream.TokenWriteFlusher, id, resource string) error {
	if writer == nil || id == "" || resource == "" {
		return ErrIdentityBinding
	}
	iq := xml.StartElement{
		Name: xml.Name{Space: stanza.NSClient, Local: "iq"},
		Attr: []xml.Attr{
			{Name: xml.Name{Local: "type"}, Value: string(stanza.SetIQ)},
			{Name: xml.Name{Local: "id"}, Value: id},
		},
	}
	bind := xml.StartElement{Name: xml.Name{Space: resourceBindingNamespace, Local: "bind"}}
	requested := xml.StartElement{Name: xml.Name{Space: resourceBindingNamespace, Local: "resource"}}
	for _, token := range []xml.Token{
		iq,
		bind,
		requested,
		xml.CharData(resource),
		requested.End(),
		bind.End(),
		iq.End(),
	} {
		if err := writer.EncodeToken(token); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func readExactBindResponse(reader xml.TokenReader, requestID string) (jid.JID, error) {
	if reader == nil || requestID == "" {
		return jid.JID{}, ErrIdentityBinding
	}
	decoder := xml.NewTokenDecoder(xmlstream.LimitReader(reader, 32))
	token, err := decoder.Token()
	if err != nil {
		return jid.JID{}, err
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: stanza.NSClient, Local: "iq"}) {
		return jid.JID{}, ErrIdentityBinding
	}
	responseType, err := validateExactBindIQAttributes(start.Attr, requestID)
	if err != nil {
		return jid.JID{}, err
	}
	var bound jid.JID
	seenChild := false
	for {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return jid.JID{}, tokenErr
		}
		switch value := token.(type) {
		case xml.CharData:
			if len(value) > 1024 || !xmlWhitespace(value) {
				return jid.JID{}, ErrIdentityBinding
			}
		case xml.StartElement:
			if seenChild {
				return jid.JID{}, ErrIdentityBinding
			}
			seenChild = true
			switch responseType {
			case stanza.ResultIQ:
				if value.Name != (xml.Name{Space: resourceBindingNamespace, Local: "bind"}) {
					return jid.JID{}, ErrIdentityBinding
				}
				bound, err = parseExactBindElement(decoder, value)
				if err != nil {
					return jid.JID{}, err
				}
			case stanza.ErrorIQ:
				if value.Name != (xml.Name{Space: stanza.NSClient, Local: "error"}) || parseExactBindError(decoder, value) != nil {
					return jid.JID{}, ErrIdentityBinding
				}
			default:
				return jid.JID{}, ErrIdentityBinding
			}
		case xml.EndElement:
			if value.Name != start.Name || !seenChild {
				return jid.JID{}, ErrIdentityBinding
			}
			if responseType == stanza.ErrorIQ {
				return jid.JID{}, ErrIdentityBinding
			}
			if bound.String() == "" {
				return jid.JID{}, ErrIdentityBinding
			}
			return bound, nil
		default:
			return jid.JID{}, ErrIdentityBinding
		}
	}
}

func validateExactBindIQAttributes(attributes []xml.Attr, requestID string) (stanza.IQType, error) {
	attributes, err := exactBindSemanticAttributes(attributes, stanza.NSClient)
	if err != nil {
		return "", err
	}
	seen := make(map[xml.Name]struct{}, len(attributes))
	var id string
	var responseType stanza.IQType
	for _, attribute := range attributes {
		if _, duplicate := seen[attribute.Name]; duplicate {
			return "", ErrIdentityBinding
		}
		seen[attribute.Name] = struct{}{}
		switch {
		case attribute.Name.Space == "" && attribute.Name.Local == "id":
			id = attribute.Value
		case attribute.Name.Space == "" && attribute.Name.Local == "type":
			responseType = stanza.IQType(attribute.Value)
		case attribute.Name.Space == "" && (attribute.Name.Local == "from" || attribute.Name.Local == "to"):
			if attribute.Value == "" || len(attribute.Value) > 256 {
				return "", ErrIdentityBinding
			}
			if _, err := jid.Parse(attribute.Value); err != nil {
				return "", ErrIdentityBinding
			}
		case attribute.Name == xmlLanguageName:
			if attribute.Value == "" || len(attribute.Value) > 64 {
				return "", ErrIdentityBinding
			}
		default:
			return "", ErrIdentityBinding
		}
	}
	if id != requestID || (responseType != stanza.ResultIQ && responseType != stanza.ErrorIQ) {
		return "", ErrIdentityBinding
	}
	return responseType, nil
}

func parseExactBindElement(decoder *xml.Decoder, start xml.StartElement) (jid.JID, error) {
	attributes, err := exactBindSemanticAttributes(start.Attr, resourceBindingNamespace)
	if err != nil || decoder == nil || len(attributes) != 0 {
		return jid.JID{}, ErrIdentityBinding
	}
	seenJID := false
	var bound jid.JID
	for {
		token, err := decoder.Token()
		if err != nil {
			return jid.JID{}, err
		}
		switch value := token.(type) {
		case xml.CharData:
			if len(value) > 1024 || !xmlWhitespace(value) {
				return jid.JID{}, ErrIdentityBinding
			}
		case xml.StartElement:
			if seenJID || value.Name != (xml.Name{Space: resourceBindingNamespace, Local: "jid"}) {
				return jid.JID{}, ErrIdentityBinding
			}
			seenJID = true
			bound, err = parseExactBoundJID(decoder, value)
			if err != nil {
				return jid.JID{}, err
			}
		case xml.EndElement:
			if value.Name != start.Name || !seenJID || bound.String() == "" {
				return jid.JID{}, ErrIdentityBinding
			}
			return bound, nil
		default:
			return jid.JID{}, ErrIdentityBinding
		}
	}
}

func parseExactBoundJID(decoder *xml.Decoder, start xml.StartElement) (jid.JID, error) {
	attributes, err := exactBindSemanticAttributes(start.Attr, resourceBindingNamespace)
	if err != nil || decoder == nil || len(attributes) != 0 {
		return jid.JID{}, ErrIdentityBinding
	}
	var text strings.Builder
	for {
		token, err := decoder.Token()
		if err != nil {
			return jid.JID{}, err
		}
		switch value := token.(type) {
		case xml.CharData:
			if text.Len()+len(value) > 256 {
				return jid.JID{}, ErrIdentityBinding
			}
			_, _ = text.Write(value)
		case xml.EndElement:
			if value.Name != start.Name || text.Len() == 0 {
				return jid.JID{}, ErrIdentityBinding
			}
			encoded := text.String()
			if strings.TrimSpace(encoded) != encoded {
				return jid.JID{}, ErrIdentityBinding
			}
			bound, parseErr := jid.Parse(encoded)
			if parseErr != nil || bound.String() != encoded {
				return jid.JID{}, ErrIdentityBinding
			}
			return bound, nil
		default:
			return jid.JID{}, ErrIdentityBinding
		}
	}
}

func parseExactBindError(decoder *xml.Decoder, start xml.StartElement) error {
	if decoder == nil {
		return ErrIdentityBinding
	}
	attributes, err := exactBindSemanticAttributes(start.Attr, stanza.NSClient)
	if err != nil {
		return ErrIdentityBinding
	}
	seen := make(map[xml.Name]struct{}, len(attributes))
	validType := false
	for _, attribute := range attributes {
		if _, duplicate := seen[attribute.Name]; duplicate {
			return ErrIdentityBinding
		}
		seen[attribute.Name] = struct{}{}
		switch {
		case attribute.Name.Space == "" && attribute.Name.Local == "type":
			switch attribute.Value {
			case "auth", "cancel", "continue", "modify", "wait":
				validType = true
			default:
				return ErrIdentityBinding
			}
		case attribute.Name.Space == "" && attribute.Name.Local == "by":
			if attribute.Value == "" || len(attribute.Value) > 256 {
				return ErrIdentityBinding
			}
			if _, err := jid.Parse(attribute.Value); err != nil {
				return ErrIdentityBinding
			}
		default:
			return ErrIdentityBinding
		}
	}
	if !validType {
		return ErrIdentityBinding
	}
	seenCondition := false
	seenText := false
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch value := token.(type) {
		case xml.CharData:
			if len(value) > 1024 || !xmlWhitespace(value) {
				return ErrIdentityBinding
			}
		case xml.StartElement:
			if value.Name.Space != stanzaErrorNamespace {
				return ErrIdentityBinding
			}
			if value.Name.Local == "text" {
				if seenText || parseStanzaErrorText(decoder, value) != nil {
					return ErrIdentityBinding
				}
				seenText = true
				continue
			}
			if seenCondition || !knownStanzaErrorCondition(value.Name.Local) || parseEmptyElement(decoder, value) != nil {
				return ErrIdentityBinding
			}
			seenCondition = true
		case xml.EndElement:
			if value.Name != start.Name || !seenCondition {
				return ErrIdentityBinding
			}
			return nil
		default:
			return ErrIdentityBinding
		}
	}
}

func parseStanzaErrorText(decoder *xml.Decoder, start xml.StartElement) error {
	attributes, err := exactBindSemanticAttributes(start.Attr, stanzaErrorNamespace)
	if err != nil || len(attributes) > 1 || (len(attributes) == 1 && (attributes[0].Name != xmlLanguageName || attributes[0].Value == "" || len(attributes[0].Value) > 64)) {
		return ErrIdentityBinding
	}
	for total := 0; ; {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch value := token.(type) {
		case xml.CharData:
			total += len(value)
			if total > 4096 {
				return ErrIdentityBinding
			}
		case xml.EndElement:
			if value.Name != start.Name {
				return ErrIdentityBinding
			}
			return nil
		default:
			return ErrIdentityBinding
		}
	}
}

func parseEmptyElement(decoder *xml.Decoder, start xml.StartElement) error {
	attributes, err := exactBindSemanticAttributes(start.Attr, stanzaErrorNamespace)
	if err != nil || len(attributes) != 0 {
		return ErrIdentityBinding
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch value := token.(type) {
		case xml.CharData:
			if len(value) > 1024 || !xmlWhitespace(value) {
				return ErrIdentityBinding
			}
		case xml.EndElement:
			if value.Name != start.Name {
				return ErrIdentityBinding
			}
			return nil
		default:
			return ErrIdentityBinding
		}
	}
}

func knownStanzaErrorCondition(condition string) bool {
	switch condition {
	case "bad-request", "conflict", "feature-not-implemented", "forbidden", "gone",
		"internal-server-error", "item-not-found", "jid-malformed", "not-acceptable",
		"not-allowed", "not-authorized", "policy-violation", "recipient-unavailable",
		"redirect", "registration-required", "remote-server-not-found",
		"remote-server-timeout", "resource-constraint", "service-unavailable",
		"subscription-required", "undefined-condition", "unexpected-request":
		return true
	default:
		return false
	}
}

func exactBindSemanticAttributes(attributes []xml.Attr, elementNamespace string) ([]xml.Attr, error) {
	semantic := make([]xml.Attr, 0, len(attributes))
	seenDeclarations := make(map[xml.Name]struct{})
	for _, attribute := range attributes {
		isDefault := attribute.Name.Space == "" && attribute.Name.Local == "xmlns"
		isPrefixed := attribute.Name.Space == "xmlns" && attribute.Name.Local != ""
		if !isDefault && !isPrefixed {
			semantic = append(semantic, attribute)
			continue
		}
		if _, duplicate := seenDeclarations[attribute.Name]; duplicate {
			return nil, ErrIdentityBinding
		}
		seenDeclarations[attribute.Name] = struct{}{}
		if isDefault {
			if attribute.Value != elementNamespace {
				return nil, ErrIdentityBinding
			}
			continue
		}
		switch attribute.Value {
		case stanza.NSClient, resourceBindingNamespace, stanzaErrorNamespace:
		default:
			return nil, ErrIdentityBinding
		}
	}
	return semantic, nil
}

func xmlWhitespace(value []byte) bool {
	for _, character := range value {
		switch character {
		case ' ', '\t', '\r', '\n':
		default:
			return false
		}
	}
	return true
}
