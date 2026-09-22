package rank2xmpp

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"unicode/utf8"

	"mellium.im/xmlstream"
	"mellium.im/xmpp"
)

const startTLSNamespace = "urn:ietf:params:xml:ns:xmpp-tls"

// boundedStartTLS is the production STARTTLS feature. It differs from
// Mellium's stock feature only at the ownership seam returned after TLS: every
// decrypted read passes through xmlTokenLimitReader before encoding/xml can
// grow a token buffer from attacker-controlled input.
func boundedStartTLS(config *tls.Config, maximumTokenBytes int) xmpp.StreamFeature {
	tlsConfig := config
	return xmpp.StreamFeature{
		Name:       xml.Name{Local: "starttls", Space: startTLSNamespace},
		Prohibited: xmpp.Secure,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			parsed := struct {
				XMLName  xml.Name `xml:"urn:ietf:params:xml:ns:xmpp-tls starttls"`
				Required struct {
					XMLName xml.Name `xml:"urn:ietf:params:xml:ns:xmpp-tls required"`
				}
			}{}
			err := decoder.DecodeElement(&parsed, start)
			return parsed.Required.XMLName == (xml.Name{Space: startTLSNamespace, Local: "required"}), nil, err
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			if maximumTokenBytes <= 0 {
				return 0, nil, ErrInvalidConfig
			}
			connection := session.Conn()
			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			if tlsConfig == nil {
				tlsConfig = &tls.Config{ServerName: session.LocalAddr().Domain().String(), MinVersion: tls.VersionTLS12}
			}
			if session.State()&xmpp.Received == xmpp.Received {
				if _, err := fmt.Fprint(connection, `<proceed xmlns='`+startTLSNamespace+`'/>`); err != nil {
					return 0, nil, err
				}
				return xmpp.Secure, newXMLTokenBoundedConn(tls.Server(unwrapXMLTokenBoundedConn(connection), tlsConfig), maximumTokenBytes), nil
			}
			if _, err := fmt.Fprint(connection, `<starttls xmlns='`+startTLSNamespace+`'/>`); err != nil {
				return 0, nil, err
			}
			token, err := decoder.Token()
			if err != nil {
				return 0, nil, err
			}
			start, ok := token.(xml.StartElement)
			if !ok || start.Name.Space != startTLSNamespace {
				return 0, nil, fmt.Errorf("xmpp: disallowed XML during TLS negotiation")
			}
			switch start.Name.Local {
			case "proceed":
				if err = decoder.Skip(); err != nil {
					return 0, nil, err
				}
				return xmpp.Secure, newXMLTokenBoundedConn(tls.Client(unwrapXMLTokenBoundedConn(connection), tlsConfig), maximumTokenBytes), nil
			case "failure":
				if err = decoder.Skip(); err != nil {
					return 0, nil, err
				}
				return 0, nil, fmt.Errorf("xmpp: receiver indicated TLS negotiation failure")
			default:
				return 0, nil, fmt.Errorf("xmpp: unknown TLS negotiation element")
			}
		},
		List: func(_ context.Context, writer xmlstream.TokenWriter, start xml.StartElement) (bool, error) {
			if err := writer.EncodeToken(start); err != nil {
				return true, err
			}
			required := xml.StartElement{Name: xml.Name{Local: "required"}}
			if err := writer.EncodeToken(required); err != nil {
				return true, err
			}
			if err := writer.EncodeToken(required.End()); err != nil {
				return true, err
			}
			return true, writer.EncodeToken(start.End())
		},
	}
}

type xmlTokenBoundedConn struct {
	net.Conn
	reader *xmlTokenLimitReader
}

func newXMLTokenBoundedConn(connection net.Conn, maximum int) *xmlTokenBoundedConn {
	return &xmlTokenBoundedConn{Conn: connection, reader: &xmlTokenLimitReader{source: connection, maximum: maximum}}
}

func (connection *xmlTokenBoundedConn) Read(destination []byte) (int, error) {
	return connection.reader.Read(destination)
}

func (connection *xmlTokenBoundedConn) ConnectionState() tls.ConnectionState {
	if secured, ok := connection.Conn.(interface{ ConnectionState() tls.ConnectionState }); ok {
		return secured.ConnectionState()
	}
	return tls.ConnectionState{}
}

func (connection *xmlTokenBoundedConn) HandshakeContext(ctx context.Context) error {
	if secured, ok := connection.Conn.(interface{ HandshakeContext(context.Context) error }); ok {
		return secured.HandshakeContext(ctx)
	}
	return ErrProtocol
}

func unwrapXMLTokenBoundedConn(connection io.ReadWriter) net.Conn {
	if bounded, ok := connection.(*xmlTokenBoundedConn); ok {
		return bounded.Conn
	}
	return connection.(net.Conn)
}

type xmlLexicalState uint8

const (
	xmlText xmlLexicalState = iota
	xmlMarkup
	xmlComment
	xmlCDATA
	xmlProcessingInstruction
)

// xmlTokenLimitReader bounds one raw XML lexical token while streaming. It
// returns at most maximum bytes of an oversized token, then a sticky protocol
// error. Bytes past that boundary are never handed to encoding/xml.
type xmlTokenLimitReader struct {
	source        io.Reader
	maximum       int
	used          int
	elementUsed   int
	elementDepth  int
	state         xmlLexicalState
	quote         byte
	prefix        [9]byte
	prefixN       int
	tail1         byte
	tail2         byte
	markupClosing bool
	elementActive bool
	failed        bool
}

func (reader *xmlTokenLimitReader) Read(destination []byte) (int, error) {
	if reader == nil || reader.source == nil || reader.maximum <= 0 {
		return 0, ErrProtocol
	}
	if reader.failed {
		return 0, ErrProtocol
	}
	count, err := reader.source.Read(destination)
	for index, value := range destination[:count] {
		if !reader.consume(value) {
			reader.failed = true
			if index == 0 {
				return 0, ErrProtocol
			}
			return index, nil
		}
	}
	return count, err
}

func (reader *xmlTokenLimitReader) consume(value byte) bool {
	if reader.state == xmlText {
		if value == '<' {
			reader.state, reader.used, reader.prefixN = xmlMarkup, 1, 1
			reader.prefix[0] = '<'
			reader.quote, reader.tail1, reader.tail2 = 0, 0, 0
			reader.markupClosing = false
			if reader.elementDepth == 1 {
				reader.elementActive, reader.elementUsed = true, 1
			} else if reader.elementActive {
				reader.elementUsed++
			}
			return reader.used <= reader.maximum && reader.elementUsed <= reader.maximum
		}
		reader.used++
		if reader.elementActive {
			reader.elementUsed++
		}
		return reader.used <= reader.maximum && reader.elementUsed <= reader.maximum
	}
	reader.used++
	if reader.elementActive {
		reader.elementUsed++
	}
	if reader.used > reader.maximum || reader.elementUsed > reader.maximum {
		return false
	}
	if reader.state == xmlMarkup && reader.prefixN < len(reader.prefix) {
		reader.prefix[reader.prefixN] = value
		reader.prefixN++
		switch {
		case reader.prefixN == 2 && value == '?':
			reader.state = xmlProcessingInstruction
		case reader.prefixN == 2 && value == '!':
			// DTDs can contain internal subsets whose embedded delimiters make
			// lexical accounting ambiguous. XMPP/AZTM does not require DTD,
			// comments, or CDATA, so reject every declaration fail closed.
			return false
		case reader.prefixN == 2 && value == '/':
			reader.markupClosing = true
			if reader.elementDepth == 1 {
				reader.elementActive, reader.elementUsed = false, 0
			}
			reader.prefixN = len(reader.prefix)
		case reader.prefixN == 2:
			reader.prefixN = len(reader.prefix)
		}
	}
	switch reader.state {
	case xmlMarkup:
		if reader.quote != 0 {
			if value == reader.quote {
				reader.quote = 0
			}
		} else if value == '\'' || value == '"' {
			reader.quote = value
		} else if value == '>' {
			selfClosing := reader.tail1 == '/'
			reader.finishMarkup(selfClosing)
		}
	case xmlComment:
		if reader.tail2 == '-' && reader.tail1 == '-' && value == '>' {
			reader.finishToken()
		}
	case xmlCDATA:
		if reader.tail2 == ']' && reader.tail1 == ']' && value == '>' {
			reader.finishToken()
		}
	case xmlProcessingInstruction:
		if reader.tail1 == '?' && value == '>' {
			reader.finishToken()
		}
	}
	reader.tail2, reader.tail1 = reader.tail1, value
	return true
}

func (reader *xmlTokenLimitReader) finishToken() {
	reader.state, reader.used, reader.prefixN = xmlText, 0, 0
	reader.quote, reader.tail1, reader.tail2 = 0, 0, 0
}

func (reader *xmlTokenLimitReader) finishMarkup(selfClosing bool) {
	if reader.markupClosing {
		if reader.elementDepth > 0 {
			reader.elementDepth--
		}
		if reader.elementDepth == 1 {
			reader.elementActive, reader.elementUsed = false, 0
		}
	} else if !selfClosing {
		reader.elementDepth++
	} else if reader.elementDepth == 1 {
		reader.elementActive, reader.elementUsed = false, 0
	}
	reader.finishToken()
}

func (reader *xmlTokenLimitReader) resetStream() {
	if reader == nil {
		return
	}
	source, maximum := reader.source, reader.maximum
	*reader = xmlTokenLimitReader{source: source, maximum: maximum}
}

func resetXMLTokenBounds(connection io.ReadWriter) {
	if bounded, ok := connection.(*xmlTokenBoundedConn); ok {
		bounded.reader.resetStream()
	}
}

// resetXMLStreamFeature ties an XML stream restart to an authenticated XMPP
// negotiation edge. Raw element names cannot request or spoof this reset.
func resetXMLStreamFeature(feature xmpp.StreamFeature) xmpp.StreamFeature {
	negotiate := feature.Negotiate
	if negotiate == nil {
		return feature
	}
	feature.Negotiate = func(ctx context.Context, session *xmpp.Session, data interface{}) (xmpp.SessionState, io.ReadWriter, error) {
		state, replacement, err := negotiate(ctx, session, data)
		if err == nil {
			resetXMLTokenBounds(session.Conn())
		}
		return state, replacement, err
	}
	return feature
}

func escapedXMLStringLength(value string) (int, bool) {
	total := 0
	for _, character := range value {
		width := escapedXMLRuneLength(character)
		if total > int(^uint(0)>>1)-width {
			return 0, false
		}
		total += width
	}
	return total, true
}

func checkedXMLSize(values ...int) (int, bool) {
	total, maximum := 0, int(^uint(0)>>1)
	for _, value := range values {
		if value < 0 || total > maximum-value {
			return 0, false
		}
		total += value
	}
	return total, true
}

func escapedXMLBytesLength(value []byte) (int, bool) {
	total := 0
	for len(value) != 0 {
		character, width := utf8.DecodeRune(value)
		value = value[width:]
		escaped := escapedXMLRuneLength(character)
		if total > int(^uint(0)>>1)-escaped {
			return 0, false
		}
		total += escaped
	}
	return total, true
}

func escapedXMLRuneLength(character rune) int {
	switch character {
	case '"', '\'', '&', '\t', '\n', '\r':
		return 5
	case '<', '>':
		return 4
	case utf8.RuneError:
		return 3
	default:
		if !validXMLCharacter(character) {
			return 3
		}
		width := utf8.RuneLen(character)
		if width < 0 {
			return 3
		}
		return width
	}
}

func validXMLCharacter(character rune) bool {
	return character == '\t' || character == '\n' || character == '\r' ||
		character >= 0x20 && character <= 0xD7FF ||
		character >= 0xE000 && character <= 0xFFFD ||
		character >= 0x10000 && character <= 0x10FFFF
}
