package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"io"
	"strings"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	entityTimeNamespace      = "urn:xmpp:time"
	maximumEntityTimeBytes   = 2 << 10
	maximumEntityTimeTokens  = 32
	entityTimeIDRandomLength = 26
	entityTimeIDPrefix       = "cynapsa-time-"
)

func validEntityTimeID(id string) bool {
	if len(id) != len(entityTimeIDPrefix)+entityTimeIDRandomLength || !strings.HasPrefix(id, entityTimeIDPrefix) {
		return false
	}
	for _, char := range id[len(entityTimeIDPrefix):] {
		if char >= 'A' && char <= 'Z' || char >= '2' && char <= '7' {
			continue
		}
		return false
	}
	return true
}

// QueryServerTime performs one authenticated, correlated XEP-0202 query on an
// already-bound and XEP-0198-enabled session. The server result is accounted
// in both directions of the stream-management ledger before it is trusted.
func (s *melliumSession) QueryServerTime(ctx context.Context) (time.Time, error) {
	if s == nil || ctx == nil {
		return time.Time{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return time.Time{}, err
	}
	defer s.releaseCorrelated()

	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return time.Time{}, ErrUnavailable
	}
	local := session.LocalAddr()
	server := local.Domain()
	if local.Localpart() == "" || local.Resourcepart() == "" || server.Domainpart() == "" {
		return time.Time{}, ErrIdentityBinding
	}
	random := rand.Text()
	if len(random) < entityTimeIDRandomLength {
		return time.Time{}, ErrProtocol
	}
	id := entityTimeIDPrefix + random[:entityTimeIDRandomLength]
	record := Stanza{Kind: StanzaTimeCalibration, From: local.String(), To: server.String(), MeshID: s.meshID, MessageID: id}

	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: server, Type: stanza.GetIQ}
	query := xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xmlstream.Wrap(nil, query), iq)
	if err != nil {
		return time.Time{}, err
	}
	if response == nil {
		return time.Time{}, ErrProtocol
	}
	defer response.Close()
	serverUTC, correlated, decodeErr := decodeCorrelatedEntityTimeResponse(response, id, server, local)
	// A strictly correlated outer IQ proves that the server handled this
	// request even when its XEP-0202 payload is malformed. Commit that proof
	// before failing the calibration so the control-plane marker cannot become
	// a phantom outbound sequence on a later resume.
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return time.Time{}, confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return time.Time{}, emitErr
			}
		}
	}
	if decodeErr != nil {
		return time.Time{}, decodeErr
	}
	management.MarkHandledInbound()
	return serverUTC, nil
}

// handleLateServerTimeResult consumes a valid XEP-0202 result that raced the
// expiry of Mellium's SendIQ waiter. The request remains in the XEP-0198
// ledger, so matching that exact retained record preserves the same strict
// correlation without mistaking the ordinary result for resume authority.
func (s *melliumSession) handleLateServerTimeResult(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ, owner bareServerResultOwnership) error {
	if s == nil || ctx == nil || source == nil {
		return ErrInvalidConfig
	}
	generation, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	s.mu.Lock()
	username, resource, management := s.username, s.boundResource(), s.management
	current := ok && generation != 0 && generation == owner.generation && generation == s.generation &&
		management == owner.management && owner.kind == bareServerResultEntityTime &&
		!s.closed && !s.suspended
	s.mu.Unlock()
	local, err := jid.Parse(username + "/" + resource)
	if err != nil || !current || management == nil {
		return ErrUnavailable
	}
	server := local.Domain()
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	_, correlated, decodeErr := decodeCorrelatedEntityTimeResponse(
		xmlstream.Wrap(children, outer), iq.ID, server, local,
	)
	if !correlated {
		return ErrAuthentication
	}
	// Revalidate and mutate while holding the session lifecycle lock. This
	// linearizes the old-generation result against PrepareResume's suspended
	// edge and against Close/new-generation publication.
	s.mu.Lock()
	current = generation == s.generation && !s.closed && !s.suspended && s.management == management
	if !current {
		s.mu.Unlock()
		return ErrUnavailable
	}
	ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(owner.sequence)
	if confirmErr != nil {
		s.mu.Unlock()
		return confirmErr
	}
	if decodeErr == nil {
		management.MarkHandledInbound()
	}
	s.mu.Unlock()
	if count > 0 {
		if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
			return emitErr
		}
	}
	if decodeErr != nil {
		return decodeErr
	}
	return nil
}

func decodeEntityTimeResponse(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID) (time.Time, error) {
	value, _, err := decodeCorrelatedEntityTimeResponse(source, expectedID, expectedFrom, expectedTo)
	return value, err
}

// decodeCorrelatedEntityTimeResponse reports correlation only after the
// authenticated outer IQ has the exact expected id, type and endpoints. Once
// true, a malformed inner payload still proves outbound server handling but is
// never accepted as a clock sample or counted as handled inbound input.
func decodeCorrelatedEntityTimeResponse(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID) (time.Time, bool, error) {
	if source == nil || expectedID == "" || expectedFrom.String() == "" || expectedTo.String() == "" {
		return time.Time{}, false, ErrProtocol
	}
	token, err := source.Token()
	if err != nil {
		return time.Time{}, false, ErrProtocol
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name.Local != "iq" || (outer.Name.Space != "" && outer.Name.Space != stanza.NSClient) {
		return time.Time{}, false, ErrProtocol
	}
	if !validEntityTimeIQAttrs(outer.Attr, expectedID, expectedFrom, expectedTo) {
		return time.Time{}, false, ErrProtocol
	}
	correlated := true
	bounded, err := newStanzaBudget(source, outer, maximumEntityTimeBytes)
	if err != nil {
		return time.Time{}, correlated, ErrProtocol
	}
	reader := &entityTimeTokenReader{source: bounded, remaining: maximumEntityTimeTokens}

	timeStart, err := expectEntityTimeStart(reader, xml.Name{Space: entityTimeNamespace, Local: "time"})
	if err != nil || !onlyMatchingNamespaceDeclarations(timeStart.Attr, entityTimeNamespace) {
		return time.Time{}, correlated, ErrProtocol
	}
	tzo, err := readEntityTimeText(reader, xml.Name{Space: entityTimeNamespace, Local: "tzo"})
	if err != nil || !validEntityTimeTZD(tzo) {
		return time.Time{}, correlated, ErrProtocol
	}
	utcText, err := readEntityTimeText(reader, xml.Name{Space: entityTimeNamespace, Local: "utc"})
	if err != nil || !validMicrosecondUTC(utcText) {
		return time.Time{}, correlated, ErrProtocol
	}
	serverUTC, err := time.Parse(time.RFC3339Nano, utcText)
	if err != nil || serverUTC.Location() != time.UTC || serverUTC.Year() < 1 || serverUTC.Year() > 9999 {
		return time.Time{}, correlated, ErrProtocol
	}
	if err = expectEntityTimeEnd(reader, timeStart.End()); err != nil {
		return time.Time{}, correlated, ErrProtocol
	}
	if err = expectEntityTimeEnd(reader, outer.End()); err != nil {
		return time.Time{}, correlated, ErrProtocol
	}
	if token, err = nextEntityTimeToken(reader); err != io.EOF || token != nil {
		return time.Time{}, correlated, ErrProtocol
	}
	return serverUTC.UTC(), correlated, nil
}

func onlyMatchingNamespaceDeclarations(attrs []xml.Attr, expected string) bool {
	for _, attr := range attrs {
		if (attr.Name.Space == "xmlns" || attr.Name.Space == "" && attr.Name.Local == "xmlns") && attr.Value == expected {
			continue
		}
		return false
	}
	return true
}

func validEntityTimeIQAttrs(attrs []xml.Attr, expectedID string, expectedFrom, expectedTo jid.JID) bool {
	seen := map[string]bool{}
	for _, attr := range attrs {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" && attr.Value == stanza.NSClient {
			continue
		}
		if attr.Name.Space == "http://www.w3.org/XML/1998/namespace" && attr.Name.Local == "lang" && !seen["xml:lang"] && validEntityTimeLanguage(attr.Value) {
			seen["xml:lang"] = true
			continue
		}
		if attr.Name.Space != "" || seen[attr.Name.Local] {
			return false
		}
		seen[attr.Name.Local] = true
		switch attr.Name.Local {
		case "id":
			if attr.Value != expectedID {
				return false
			}
		case "type":
			if attr.Value != string(stanza.ResultIQ) {
				return false
			}
		case "from":
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != expectedFrom.String() {
				return false
			}
		case "to":
			parsed, err := jid.Parse(attr.Value)
			if err != nil || parsed.String() != expectedTo.String() {
				return false
			}
		default:
			return false
		}
	}
	if !seen["id"] || !seen["type"] || !seen["from"] || !seen["to"] {
		return false
	}
	return true
}

func validEntityTimeLanguage(value string) bool {
	if len(value) == 0 || len(value) > 35 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if char == '-' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return true
}

type entityTimeTokenReader struct {
	source    xml.TokenReader
	remaining int
}

func (r *entityTimeTokenReader) Token() (xml.Token, error) {
	if r == nil || r.source == nil || r.remaining <= 0 {
		return nil, ErrProtocol
	}
	r.remaining--
	return r.source.Token()
}

func expectEntityTimeStart(reader xml.TokenReader, name xml.Name) (xml.StartElement, error) {
	token, err := nextEntityTimeToken(reader)
	if err != nil {
		return xml.StartElement{}, err
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != name {
		return xml.StartElement{}, ErrProtocol
	}
	return start, nil
}

func expectEntityTimeEnd(reader xml.TokenReader, expected xml.EndElement) error {
	token, err := nextEntityTimeToken(reader)
	if err != nil {
		return err
	}
	end, ok := token.(xml.EndElement)
	if !ok || end.Name != expected.Name {
		return ErrProtocol
	}
	return nil
}

func nextEntityTimeToken(reader xml.TokenReader) (xml.Token, error) {
	for {
		token, err := reader.Token()
		if err != nil && (err != io.EOF || token == nil) {
			return token, err
		}
		chars, ok := token.(xml.CharData)
		if !ok {
			return token, nil
		}
		if strings.TrimSpace(string(chars)) != "" {
			return nil, ErrProtocol
		}
	}
}

func validMicrosecondUTC(value string) bool {
	if len(value) != len("2006-01-02T15:04:05.000000Z") || value[19] != '.' || value[len(value)-1] != 'Z' {
		return false
	}
	for index := 20; index < 26; index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func readEntityTimeText(reader xml.TokenReader, name xml.Name) (string, error) {
	start, err := expectEntityTimeStart(reader, name)
	if err != nil || len(start.Attr) != 0 {
		return "", ErrProtocol
	}
	token, err := reader.Token()
	if err != nil {
		return "", err
	}
	chars, ok := token.(xml.CharData)
	if !ok || len(chars) == 0 || len(chars) > 64 {
		return "", ErrProtocol
	}
	value := string(chars)
	if err = expectEntityTimeEnd(reader, start.End()); err != nil {
		return "", err
	}
	return value, nil
}

func validEntityTimeTZD(value string) bool {
	if value == "Z" {
		return true
	}
	if len(value) != 6 || (value[0] != '+' && value[0] != '-') || value[3] != ':' || value[1] < '0' || value[1] > '9' || value[2] < '0' || value[2] > '9' || value[4] < '0' || value[4] > '9' || value[5] < '0' || value[5] > '9' {
		return false
	}
	hour := int(value[1]-'0')*10 + int(value[2]-'0')
	minute := int(value[4]-'0')*10 + int(value[5]-'0')
	return minute < 60 && (hour < 14 || hour == 14 && minute == 0)
}
