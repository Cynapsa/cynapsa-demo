package rank2xmpp

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"testing"
)

type repeatedByteReader struct {
	remaining int
	read      int
	value     byte
}

func (reader *repeatedByteReader) Read(destination []byte) (int, error) {
	if reader.remaining == 0 {
		return 0, io.EOF
	}
	count := min(len(destination), reader.remaining)
	for index := range count {
		destination[index] = reader.value
	}
	reader.remaining -= count
	reader.read += count
	return count, nil
}

func TestXMLTokenLimitReaderRejectsBeforeDecoderCanRetainAttackerToken(t *testing.T) {
	const maximum = 64 << 10
	source := &repeatedByteReader{remaining: 64 << 20, value: 'x'}
	decoder := xml.NewDecoder(&xmlTokenLimitReader{source: source, maximum: maximum})
	var gotError error
	for {
		token, err := decoder.Token()
		if data, ok := token.(xml.CharData); ok && len(data) > maximum {
			t.Fatalf("decoder retained %d token bytes, maximum=%d", len(data), maximum)
		}
		if err != nil {
			gotError = err
			break
		}
	}
	if !errors.Is(gotError, ErrProtocol) {
		t.Fatalf("oversized character token error = %v", gotError)
	}
	// encoding/xml currently reads in small fixed chunks. This invariant proves
	// the bounded plaintext seam stops feeding it shortly after the configured
	// token ceiling instead of consuming the complete hostile source.
	if source.read > maximum+(8<<10) {
		t.Fatalf("decoder consumed %d bytes before rejection, maximum=%d", source.read, maximum)
	}
}

func TestXMLTokenLimitReaderCoversEveryGrowingLexicalToken(t *testing.T) {
	const maximum = 32
	inputs := []string{
		strings.Repeat("x", maximum+1),
		`<x attribute="` + strings.Repeat("x", maximum) + `">`,
		`<?target ` + strings.Repeat("x", maximum) + `?>`,
	}
	for _, input := range inputs {
		reader := &xmlTokenLimitReader{source: strings.NewReader(input), maximum: maximum}
		if _, err := io.ReadAll(reader); !errors.Is(err, ErrProtocol) {
			t.Fatalf("oversized token %q error = %v", input[:min(len(input), 16)], err)
		}
	}
	valid := `<stream><x attribute='&gt;'>text<?p i?></x></stream>`
	output, err := io.ReadAll(&xmlTokenLimitReader{source: strings.NewReader(valid), maximum: len(valid)})
	if err != nil || string(output) != valid {
		t.Fatalf("valid token stream = %q, %v", output, err)
	}
}

func TestXMLTokenLimitReaderRejectsDeclarationsAndAggregateTopLevelElement(t *testing.T) {
	for _, declaration := range []string{
		`<!DOCTYPE stream [ <!ELEMENT stream ANY> ]>`,
		`<!--comment-->`,
		`<![CDATA[value]]>`,
	} {
		if _, err := io.ReadAll(&xmlTokenLimitReader{source: strings.NewReader(declaration), maximum: 1024}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("declaration %q error = %v", declaration, err)
		}
	}
	const maximum = 64
	hostile := `<stream><error>` + strings.Repeat(`<text>12345678</text>`, 8) + `</error></stream>`
	reader := &xmlTokenLimitReader{source: strings.NewReader(hostile), maximum: maximum}
	if _, err := io.ReadAll(reader); !errors.Is(err, ErrProtocol) {
		t.Fatalf("aggregate top-level element error = %v", err)
	}
	valid := `<stream><x>` + strings.Repeat("a", maximum-8) + `</x><r/></stream>`
	if output, err := io.ReadAll(&xmlTokenLimitReader{source: strings.NewReader(valid), maximum: maximum}); err != nil || string(output) != valid {
		t.Fatalf("bounded sibling elements = %q, %v", output, err)
	}
}

func TestXMLTokenLimitReaderResetsOnlyAtTrustedNegotiationEdge(t *testing.T) {
	const maximum = 48
	reader := &xmlTokenLimitReader{source: strings.NewReader(""), maximum: maximum}
	for _, value := range []byte(`<stream:stream xmlns:stream='s'>`) {
		if !reader.consume(value) {
			t.Fatal("initial stream root rejected")
		}
	}
	reader.resetStream()
	restarted := `<stream:stream xmlns:stream='s'><x>` + strings.Repeat("a", maximum-7) + `</x><r/></stream:stream>`
	for _, value := range []byte(restarted) {
		if !reader.consume(value) {
			t.Fatal("trusted restarted stream rejected")
		}
	}

	shadowed := `<stream><stream:stream xmlns:stream='urn:attacker'></stream:stream><error>` + strings.Repeat(`<text>x</text>`, 8) + `</error></stream>`
	if _, err := io.ReadAll(&xmlTokenLimitReader{source: strings.NewReader(shadowed), maximum: maximum}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("shadowed stream aggregate error = %v", err)
	}
}

func TestEscapedXMLLengthMatchesStandardLibraryWithoutAllocation(t *testing.T) {
	values := [][]byte{
		[]byte("plain"),
		[]byte("<&>\"'\t\n\r"),
		[]byte("שלום🙂"),
		{0xff, 'x'},
		{0, 1, 0x0b},
	}
	for _, value := range values {
		var escaped bytes.Buffer
		if err := xml.EscapeText(&escaped, value); err != nil {
			t.Fatal(err)
		}
		got, ok := escapedXMLBytesLength(value)
		if !ok || got != escaped.Len() {
			t.Fatalf("escaped bytes length(%x) = %d/%t, want %d", value, got, ok, escaped.Len())
		}
		got, ok = escapedXMLStringLength(string(value))
		if !ok || got != escaped.Len() {
			t.Fatalf("escaped string length(%x) = %d/%t, want %d", value, got, ok, escaped.Len())
		}
	}
	large := bytes.Repeat([]byte("<&🙂"), 1<<18)
	if allocations := testing.AllocsPerRun(100, func() {
		if _, ok := escapedXMLBytesLength(large); !ok {
			panic("length overflow")
		}
	}); allocations != 0 {
		t.Fatalf("escaped byte accounting allocations = %v, want 0", allocations)
	}
	largeString := string(large)
	if allocations := testing.AllocsPerRun(100, func() {
		if _, ok := escapedXMLStringLength(largeString); !ok {
			panic("length overflow")
		}
	}); allocations != 0 {
		t.Fatalf("escaped string accounting allocations = %v, want 0", allocations)
	}
}

func TestMelliumHandlerDispatchRequiresExactClientNamespace(t *testing.T) {
	session := &melliumSession{config: MelliumConfig{StanzaBudgetBytes: 1024}}
	for _, local := range []string{"message", "iq"} {
		start := xml.StartElement{Name: xml.Name{Space: "urn:attacker", Local: local}}
		if err := session.handleElement(t.Context(), panicTokenReadEncoder{}, &start); err != nil {
			t.Fatalf("foreign %s handler result = %v", local, err)
		}
	}
}

type panicTokenReadEncoder struct{}

func (panicTokenReadEncoder) Token() (xml.Token, error)   { panic("foreign namespace was dispatched") }
func (panicTokenReadEncoder) EncodeToken(xml.Token) error { panic("foreign namespace was dispatched") }
func (panicTokenReadEncoder) Encode(any) error            { panic("foreign namespace was dispatched") }
func (panicTokenReadEncoder) EncodeElement(any, xml.StartElement) error {
	panic("foreign namespace was dispatched")
}
