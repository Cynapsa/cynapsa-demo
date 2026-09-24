package rank2xmpp

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	privateIQRandomLength  = 26
	maximumPrivateIQBytes  = 256 << 10
	maximumPrivateIQTokens = 1_024
	privateIQTimeout       = 30 * time.Second
)

func validCorrelatedIQAttrs(attrs []xml.Attr, expectedID string, expectedFrom, expectedTo jid.JID) (stanza.IQType, bool) {
	seen := map[string]bool{}
	var iqType stanza.IQType
	for _, attr := range attrs {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" && attr.Value == stanza.NSClient {
			continue
		}
		if attr.Name.Space == "http://www.w3.org/XML/1998/namespace" && attr.Name.Local == "lang" && !seen["xml:lang"] && validEntityTimeLanguage(attr.Value) {
			seen["xml:lang"] = true
			continue
		}
		if attr.Name.Space != "" || seen[attr.Name.Local] {
			return "", false
		}
		seen[attr.Name.Local] = true
		switch attr.Name.Local {
		case "id":
			if attr.Value != expectedID {
				return "", false
			}
		case "type":
			iqType = stanza.IQType(attr.Value)
			if iqType != stanza.ResultIQ && iqType != stanza.ErrorIQ {
				return "", false
			}
		case "from":
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != expectedFrom.String() {
				return "", false
			}
		case "to":
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != expectedTo.String() {
				return "", false
			}
		default:
			return "", false
		}
	}
	return iqType, seen["id"] && seen["type"] && seen["from"] && seen["to"]
}

// validCorrelatedResultIQ validates one canonical result IQ consistently at
// both dispatch and handling boundaries. In particular, xml:lang is optional
// but, when present, must be unique and canonical according to
// validCorrelatedIQAttrs.
func validCorrelatedResultIQ(outer xml.StartElement, iq stanza.IQ, expectedFrom, expectedTo jid.JID) bool {
	if outer.Name != (xml.Name{Space: stanza.NSClient, Local: "iq"}) || iq.ID == "" ||
		iq.Type != stanza.ResultIQ || iq.From.String() != expectedFrom.String() || iq.To.String() != expectedTo.String() {
		return false
	}
	iqType, ok := validCorrelatedIQAttrs(outer.Attr, iq.ID, expectedFrom, expectedTo)
	return ok && iqType == stanza.ResultIQ
}

func decodePrivateIQError(reader xml.TokenReader, outer xml.StartElement, melliumFraming bool) (error, bool) {
	token, err := nextPrivateToken(reader)
	if err != nil {
		return fmt.Errorf("iq-error-token: %w", ErrProtocol), false
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name.Local != "error" || start.Name.Space != stanza.NSClient && start.Name.Space != "" {
		return fmt.Errorf("iq-error-start: %w", ErrProtocol), false
	}
	if !validPrivateIQErrorAttrs(start.Attr) {
		return fmt.Errorf("iq-error-attrs: %w", ErrProtocol), false
	}
	condition := ""
	for {
		token, err = nextPrivateToken(reader)
		if err != nil {
			return fmt.Errorf("iq-error-body-token: %w", ErrProtocol), false
		}
		switch typed := token.(type) {
		case xml.StartElement:
			if typed.Name.Space == "urn:ietf:params:xml:ns:xmpp-stanzas" && typed.Name.Local != "text" && condition == "" {
				condition = typed.Name.Local
			}
			if err := skipPrivateElement(reader, typed); err != nil {
				return fmt.Errorf("iq-error-child: %w", ErrProtocol), false
			}
		case xml.EndElement:
			if typed != start.End() || condition == "" || consumePrivateIQEnd(reader, outer, melliumFraming) != nil {
				return fmt.Errorf("iq-error-end-%s: %w", condition, ErrProtocol), false
			}
			switch condition {
			case "resource-constraint":
				return ErrCapacity, true
			case "service-unavailable":
				return ErrUnavailable, true
			case "bad-request", "conflict", "forbidden", "item-not-found", "not-allowed", "policy-violation":
				return fmt.Errorf("iq-error-condition-%s: %w", condition, ErrProtocol), true
			default:
				return fmt.Errorf("iq-error-condition-%s: %w", condition, ErrProtocol), false
			}
		default:
			return fmt.Errorf("iq-error-body-shape: %w", ErrProtocol), false
		}
	}
}

func validPrivateIQErrorAttrs(attrs []xml.Attr) bool {
	seenType := false
	for _, attr := range attrs {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" && attr.Value == stanza.NSClient {
			continue
		}
		if attr.Name.Space != "" || attr.Name.Local != "type" || seenType {
			return false
		}
		seenType = true
		switch attr.Value {
		case "auth", "cancel", "continue", "modify", "wait":
		default:
			return false
		}
	}
	return seenType
}

func exactPrivateAttrs(attrs []xml.Attr, names []string, namespace string) (map[string]string, bool) {
	values := make(map[string]string, len(names))
	for _, attr := range attrs {
		if (attr.Name.Space == "xmlns" || attr.Name.Space == "" && attr.Name.Local == "xmlns") && attr.Value == namespace {
			continue
		}
		if attr.Name.Space != "" || !containsString(names, attr.Name.Local) {
			return nil, false
		}
		if _, exists := values[attr.Name.Local]; exists {
			return nil, false
		}
		values[attr.Name.Local] = attr.Value
	}
	return values, len(values) == len(names)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func parseCanonicalUint(value string, maximum uint64, zero bool) (uint64, bool) {
	if value == "" || len(value) > 20 || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed > maximum || !zero && parsed == 0 {
		return 0, false
	}
	return parsed, true
}

func nextPrivateToken(reader xml.TokenReader) (xml.Token, error) {
	for {
		token, err := reader.Token()
		if err != nil {
			return nil, err
		}
		if data, ok := token.(xml.CharData); ok && len(data) == 0 {
			continue
		}
		return token, nil
	}
}

func consumePrivateIQEnd(reader xml.TokenReader, outer xml.StartElement, melliumFraming bool) error {
	token, err := nextPrivateToken(reader)
	if melliumFraming && errors.Is(err, io.EOF) && token == nil {
		return nil
	}
	if err != nil || token != outer.End() {
		return ErrProtocol
	}
	if token, err = nextPrivateToken(reader); err != io.EOF || token != nil {
		return ErrProtocol
	}
	return nil
}

func skipPrivateElement(reader xml.TokenReader, start xml.StartElement) error {
	depth := 1
	for depth > 0 {
		token, err := nextPrivateToken(reader)
		if err != nil {
			return err
		}
		switch token.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return nil
}

type privateIQTokenReader struct {
	source    xml.TokenReader
	remaining int
}

func (reader *privateIQTokenReader) Token() (xml.Token, error) {
	if reader == nil || reader.source == nil || reader.remaining <= 0 {
		return nil, ErrProtocol
	}
	reader.remaining--
	return reader.source.Token()
}
