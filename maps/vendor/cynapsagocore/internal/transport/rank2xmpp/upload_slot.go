package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/xep0363"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	uploadNamespace        = "urn:xmpp:http:upload:0"
	uploadServicePrefix    = "upload."
	uploadQueryIDPrefix    = "cynapsa-upload-"
	uploadFilenamePrefix   = "cynapsa-"
	uploadRandomLength     = 26
	uploadFilenameSuffix   = ".bin"
	maximumUploadSlotBytes = 64 << 10
)

type uploadSlotSession interface {
	RequestUploadSlot(context.Context, int64, string) (xep0363.Slot, error)
}

// RequestSlot implements xep0363.SlotRequester on the exact authenticated
// Rank 2 session. The default ejabberd XEP-0363 component is addressed at
// upload.<authenticated-domain>; no caller-provided service or URL is trusted.
func (c *Client) RequestSlot(ctx context.Context, size int64, contentType string) (xep0363.Slot, error) {
	if c == nil || ctx == nil || size <= 0 || size > payload.MaximumTransferredBytes || !validUploadText(contentType, 256) {
		return xep0363.Slot{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return xep0363.Slot{}, err
	}
	c.mu.Lock()
	session, lifetime := c.session, c.ctx
	lifetimeGeneration, sessionEpoch, membershipReady := c.generation, c.sessionEpoch, c.membershipReady
	started, closed, state := c.started, c.closed, c.state
	c.mu.Unlock()
	if closed {
		return xep0363.Slot{}, ErrClosed
	}
	if !started || lifetime == nil || state != DurableLive || session == nil || !membershipReady {
		return xep0363.Slot{}, ErrUnavailable
	}
	source, ok := session.(uploadSlotSession)
	if !ok {
		return xep0363.Slot{}, ErrUnavailable
	}
	operation, cancel := context.WithTimeout(ctx, privateIQTimeout)
	stopLifetime := context.AfterFunc(lifetime, cancel)
	defer func() {
		stopLifetime()
		cancel()
	}()
	slot, err := source.RequestUploadSlot(operation, size, contentType)
	if stateErr := c.uploadSlotSessionError(lifetimeGeneration, sessionEpoch); stateErr != nil {
		return xep0363.Slot{}, stateErr
	}
	if callerErr := ctx.Err(); callerErr != nil {
		return xep0363.Slot{}, callerErr
	}
	if operation.Err() != nil {
		return xep0363.Slot{}, ErrUnavailable
	}
	if err != nil {
		return xep0363.Slot{}, err
	}
	return cloneUploadSlot(slot), nil
}

func (c *Client) uploadSlotSessionError(lifetimeGeneration, sessionEpoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.generation != lifetimeGeneration {
		return ErrClosed
	}
	if !c.started || c.state != DurableLive || !c.membershipReady || c.session == nil || c.sessionEpoch != sessionEpoch {
		return ErrUnavailable
	}
	return nil
}

// RequestUploadSlot performs one strictly correlated XEP-0363 slot request on
// an already TLS/SASL/bind/XEP-0198 authenticated stream.
func (s *melliumSession) RequestUploadSlot(ctx context.Context, size int64, contentType string) (xep0363.Slot, error) {
	if s == nil || ctx == nil || size <= 0 || size > payload.MaximumTransferredBytes || !validUploadText(contentType, 256) {
		return xep0363.Slot{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return xep0363.Slot{}, err
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return xep0363.Slot{}, err
	}
	defer s.releaseCorrelated()

	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return xep0363.Slot{}, ErrUnavailable
	}
	local := session.LocalAddr()
	if local.Localpart() == "" || local.Resourcepart() == "" || local.Domainpart() == "" || local.Resourcepart() != s.boundResource() {
		return xep0363.Slot{}, ErrIdentityBinding
	}
	service, err := jid.Parse(uploadServicePrefix + local.Domainpart())
	if err != nil || service.Localpart() != "" || service.Resourcepart() != "" {
		return xep0363.Slot{}, ErrProtocol
	}
	random := rand.Text()
	if len(random) < uploadRandomLength {
		return xep0363.Slot{}, ErrProtocol
	}
	id := uploadQueryIDPrefix + random[:uploadRandomLength]
	filename := uploadFilenamePrefix + random[:uploadRandomLength] + uploadFilenameSuffix
	record := Stanza{Kind: StanzaUploadSlotQuery, From: local.String(), To: service.String(), MeshID: s.meshID, MessageID: id}

	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: service, Type: stanza.GetIQ}
	request := xml.StartElement{Name: xml.Name{Space: uploadNamespace, Local: "request"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "filename"}, Value: filename},
		{Name: xml.Name{Local: "size"}, Value: strconv.FormatInt(size, 10)},
		{Name: xml.Name{Local: "content-type"}, Value: contentType},
	}}
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xmlstream.Wrap(nil, request), iq)
	if err != nil {
		return xep0363.Slot{}, err
	}
	if response == nil {
		return xep0363.Slot{}, ErrProtocol
	}
	defer response.Close()
	slot, correlated, handled, decodeErr := decodeMelliumUploadSlot(response, id, service, local)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return xep0363.Slot{}, confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return xep0363.Slot{}, emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	if decodeErr != nil {
		return xep0363.Slot{}, decodeErr
	}
	return slot, nil
}

type uploadSlotXML struct {
	XMLName      xml.Name     `xml:"urn:xmpp:http:upload:0 slot"`
	Put          []uploadPut  `xml:"urn:xmpp:http:upload:0 put"`
	Get          []uploadGet  `xml:"urn:xmpp:http:upload:0 get"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

type uploadPut struct {
	URL          string         `xml:"url,attr"`
	Headers      []uploadHeader `xml:"urn:xmpp:http:upload:0 header"`
	Unknown      []xmlUnknown   `xml:",any"`
	UnknownAttrs []xml.Attr     `xml:",any,attr"`
}

type uploadGet struct {
	URL          string       `xml:"url,attr"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

type uploadHeader struct {
	Name         string       `xml:"name,attr"`
	Value        string       `xml:",chardata"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

func decodeCorrelatedUploadSlot(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID) (xep0363.Slot, bool, bool, error) {
	return decodeUploadSlot(source, expectedID, expectedFrom, expectedTo, false)
}

func decodeMelliumUploadSlot(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID) (xep0363.Slot, bool, bool, error) {
	return decodeUploadSlot(source, expectedID, expectedFrom, expectedTo, true)
}

func decodeUploadSlot(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, melliumFraming bool) (xep0363.Slot, bool, bool, error) {
	if source == nil || expectedID == "" || expectedFrom.String() == "" || expectedTo.String() == "" {
		return xep0363.Slot{}, false, false, fmt.Errorf("upload outer input: %w", ErrProtocol)
	}
	token, err := source.Token()
	if err != nil {
		return xep0363.Slot{}, false, false, fmt.Errorf("upload outer token: %w", ErrProtocol)
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return xep0363.Slot{}, false, false, fmt.Errorf("upload outer start: %w", ErrProtocol)
	}
	iqType, valid := validUploadIQAttrs(outer.Attr, expectedID, expectedFrom, expectedTo)
	if !valid {
		return xep0363.Slot{}, false, false, fmt.Errorf("upload outer attributes: %w", ErrProtocol)
	}
	correlated := true
	bounded, err := newStanzaBudget(source, outer, maximumUploadSlotBytes)
	if err != nil {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload outer budget: %w", ErrProtocol)
	}
	if iqType == string(stanza.ErrorIQ) {
		if !consumeUploadError(bounded, outer.End(), melliumFraming) {
			return xep0363.Slot{}, correlated, false, fmt.Errorf("upload error payload: %w", ErrProtocol)
		}
		return xep0363.Slot{}, correlated, true, ErrUnavailable
	}
	if iqType != string(stanza.ResultIQ) {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload IQ type: %w", ErrProtocol)
	}
	children := &uploadChildReader{source: bounded, outer: outer.Name, melliumFraming: melliumFraming}
	decoder := xml.NewTokenDecoder(children)
	token, err = decoder.Token()
	if err != nil {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload slot token: %w", ErrProtocol)
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: uploadNamespace, Local: "slot"}) {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload slot start: %w", ErrProtocol)
	}
	var wire uploadSlotXML
	if err = decoder.DecodeElement(&wire, &start); err != nil {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload slot body: %w", ErrProtocol)
	}
	if token, err = decoder.Token(); token != nil || err != io.EOF || !children.complete {
		return xep0363.Slot{}, correlated, false, fmt.Errorf("upload slot finish: %w", ErrProtocol)
	}
	slot, err := validateUploadSlotXML(wire)
	if err != nil {
		return xep0363.Slot{}, correlated, true, fmt.Errorf("upload slot value: %w", err)
	}
	return slot, correlated, true, nil
}

func validUploadIQAttrs(attrs []xml.Attr, expectedID string, expectedFrom, expectedTo jid.JID) (string, bool) {
	seen := make(map[string]bool, 4)
	values := make(map[string]string, 4)
	languageSeen := false
	for _, attr := range attrs {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" && attr.Value == stanza.NSClient {
			continue
		}
		if attr.Name.Space == "http://www.w3.org/XML/1998/namespace" && attr.Name.Local == "lang" && !languageSeen && validEntityTimeLanguage(attr.Value) {
			languageSeen = true
			continue
		}
		if attr.Name.Space != "" || seen[attr.Name.Local] {
			return "", false
		}
		seen[attr.Name.Local], values[attr.Name.Local] = true, attr.Value
	}
	if len(seen) != 4 || values["id"] != expectedID || values["type"] != string(stanza.ResultIQ) && values["type"] != string(stanza.ErrorIQ) {
		return "", false
	}
	from, fromErr := jid.Parse(values["from"])
	to, toErr := jid.Parse(values["to"])
	if fromErr != nil || toErr != nil || from.String() != expectedFrom.String() || to.String() != expectedTo.String() {
		return "", false
	}
	return values["type"], true
}

type uploadChildReader struct {
	source         xml.TokenReader
	outer          xml.Name
	melliumFraming bool
	depth          int
	complete       bool
}

func (reader *uploadChildReader) Token() (xml.Token, error) {
	if reader.complete {
		return nil, io.EOF
	}
	token, err := reader.source.Token()
	if err == io.EOF && reader.melliumFraming && reader.depth == 0 {
		reader.complete = true
		return nil, io.EOF
	}
	if err != nil {
		return token, err
	}
	switch value := token.(type) {
	case xml.StartElement:
		reader.depth++
	case xml.EndElement:
		if reader.depth == 0 {
			if value.Name != reader.outer {
				return nil, ErrProtocol
			}
			reader.complete = true
			return nil, io.EOF
		}
		reader.depth--
	}
	return token, nil
}

func consumeUploadError(source xml.TokenReader, expected xml.EndElement, melliumFraming bool) bool {
	depth := 0
	for {
		token, err := source.Token()
		if err == io.EOF {
			return melliumFraming && depth == 0
		}
		if err != nil {
			return false
		}
		switch value := token.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			if depth == 0 {
				if value.Name != expected.Name {
					return false
				}
				token, err = source.Token()
				return token == nil && err == io.EOF
			}
			depth--
		}
	}
}

func validateUploadSlotXML(wire uploadSlotXML) (xep0363.Slot, error) {
	if wire.XMLName != (xml.Name{Space: uploadNamespace, Local: "slot"}) || len(wire.Put) != 1 || len(wire.Get) != 1 || len(wire.Unknown) != 0 || !onlyNamespaceAttr(wire.UnknownAttrs, uploadNamespace) {
		return xep0363.Slot{}, ErrProtocol
	}
	put, get := wire.Put[0], wire.Get[0]
	if put.URL == "" || get.URL == "" || len(put.URL) > 16<<10 || len(get.URL) > 16<<10 || len(put.Unknown) != 0 || len(get.Unknown) != 0 || !onlyNamespaceAttr(put.UnknownAttrs, uploadNamespace) || !onlyNamespaceAttr(get.UnknownAttrs, uploadNamespace) || len(put.Headers) > 16 {
		return xep0363.Slot{}, ErrProtocol
	}
	result := xep0363.Slot{PutURL: put.URL, GetURL: get.URL, PutHeaders: make([]xep0363.Header, 0, len(put.Headers))}
	seen := make(map[string]bool, len(put.Headers))
	for _, header := range put.Headers {
		name := strings.ToLower(header.Name)
		if name == "" || seen[name] || len(header.Unknown) != 0 || !onlyNamespaceAttr(header.UnknownAttrs, uploadNamespace) || !validUploadText(header.Value, 4096) {
			return xep0363.Slot{}, ErrProtocol
		}
		seen[name] = true
		result.PutHeaders = append(result.PutHeaders, xep0363.Header{Name: name, Value: header.Value})
	}
	return result, nil
}

func validUploadText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func cloneUploadSlot(slot xep0363.Slot) xep0363.Slot {
	slot.PutHeaders = append([]xep0363.Header(nil), slot.PutHeaders...)
	return slot
}

var _ xep0363.SlotRequester = (*Client)(nil)
