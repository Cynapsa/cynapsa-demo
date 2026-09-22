package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func typed(prefix string, value byte) string {
	return prefix + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{value}, 16))
}

func TestStanzaFrameRoundTripAndBudget(t *testing.T) {
	stanza := Stanza{Kind: StanzaTransferChunk, From: "a/m", To: "b/m", MeshID: "m", TransferID: typed("xfer_", 1), Data: []byte("chunk")}
	encoded, err := EncodeStanzaFrame(stanza, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStanzaFrame(encoded, stanza.From, stanza.To, stanza.MeshID, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != stanza.Kind || got.TransferID != stanza.TransferID || !bytes.Equal(got.Data, stanza.Data) {
		t.Fatalf("got=%#v", got)
	}
	if _, err := EncodeStanzaFrame(stanza, 256, len(encoded)-1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("budget=%v", err)
	}
	tampered := bytes.Replace(encoded, []byte(`v="1"`), []byte(`v="2"`), 1)
	if _, err := DecodeStanzaFrame(tampered, "a/m", "b/m", "m", 256, 512); !errors.Is(err, ErrProtocol) {
		t.Fatalf("version=%v", err)
	}
	tampered = bytes.Replace(encoded, []byte(`v="1"`), []byte(`v="1" extra="bad"`), 1)
	if _, err = DecodeStanzaFrame(tampered, "a/m", "b/m", "m", 256, 512); !errors.Is(err, ErrProtocol) {
		t.Fatalf("unknown attr=%v", err)
	}
}

func TestMailboxDelaySiblingOrdersAreAcceptedButUnknownChildFails(t *testing.T) {
	record := Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}
	frame, err := EncodeStanzaFrame(record, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	delay := []byte(`<delay xmlns="urn:xmpp:delay" from="mesh.test" stamp="2026-01-01T00:00:00.123Z">Offline Storage</delay>`)
	for name, input := range map[string][]byte{
		"delay before frame": append(append([]byte(nil), delay...), frame...),
		"delay after frame":  append(append([]byte(nil), frame...), delay...),
	} {
		t.Run(name, func(t *testing.T) {
			decoded, decodeErr := decodeMessageFrame(xml.NewDecoder(bytes.NewReader(input)))
			if decodeErr != nil || decoded.Version != "1" {
				t.Fatalf("delay=%#v %v", decoded, decodeErr)
			}
		})
	}
	if _, err = decodeMessageFrame(xml.NewDecoder(bytes.NewBufferString(`<body>bad</body>`))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("unknown=%v", err)
	}
}

func TestMailboxDelayStrictRejectionMatrix(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	validDelay := `<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"></delay>`
	tests := map[string][]byte{
		"delay only":              []byte(validDelay),
		"duplicate delay":         append(append(append([]byte(nil), []byte(validDelay)...), frame...), []byte(validDelay)...),
		"duplicate frame":         append(append([]byte(nil), frame...), frame...),
		"missing stamp":           append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay"/>`)...),
		"non UTC stamp":           append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T01:00:00+01:00"/>`)...),
		"malformed stamp":         append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="yesterday"/>`)...),
		"duplicate stamp":         append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z" stamp="2026-01-01T00:00:00Z"/>`)...),
		"unknown attribute":       append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z" extra="bad"/>`)...),
		"invalid from":            append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" from="not a jid" stamp="2026-01-01T00:00:00Z"/>`)...),
		"nested delay content":    append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"><bad/></delay>`)...),
		"comment delay content":   append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"><!--bad--></delay>`)...),
		"wrong delay namespace":   append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay:wrong" stamp="2026-01-01T00:00:00Z"/>`)...),
		"unknown sibling":         append(append([]byte(nil), frame...), []byte(`<body xmlns="jabber:client">bad</body>`)...),
		"non-whitespace text":     append(append([]byte(nil), frame...), []byte(`bad`)...),
		"malformed delay element": append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z">`)...),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, decodeErr := decodeMessageFrame(xml.NewDecoder(bytes.NewReader(input))); !errors.Is(decodeErr, ErrProtocol) {
				t.Fatalf("decode error=%v", decodeErr)
			}
		})
	}
}

func TestStreamManagementAckResumeAndRejection(t *testing.T) {
	sm, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = sm.RecordSent(Stanza{Ordinal: 7}); err != nil {
		t.Fatal(err)
	}
	if err = sm.RecordSent(Stanza{Ordinal: 8}); err != nil {
		t.Fatal(err)
	}
	replay, ordinal, err := sm.ResumeAccepted(1)
	if err != nil {
		t.Fatal(err)
	}
	if ordinal != 7 || len(replay) != 1 || replay[0].Ordinal != 8 {
		t.Fatalf("ordinal=%d replay=%#v", ordinal, replay)
	}
	if _, err = sm.ApplyAck(3); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid ack=%v", err)
	}
	if got := sm.ResumeRejected(); len(got) != 1 || sm.Pending() != 0 {
		t.Fatalf("rejected=%#v", got)
	}
}

func sampleJingle() Jingle {
	return Jingle{XMLName: xml.Name{Space: JingleNamespace, Local: "jingle"}, Action: "session-initiate", Initiator: "a/m", Responder: "b/m", SID: "hsk_123", Content: JingleContent{Creator: "initiator", Name: "data", Description: DataChannelDescription{XMLName: xml.Name{Space: AZTMDataChannelNamespace, Local: "description"}, Media: "application", MeshID: "m", MaximumMessageSize: 65536}, Transport: ICETransport{XMLName: xml.Name{Space: ICEUDPNamespace, Local: "transport"}, Ufrag: "ufrag", Password: "password", Candidates: []ICECandidate{{Component: 1, Foundation: "1", ID: "candidate", IP: "127.0.0.1", Port: 9999, Priority: 1, Protocol: "udp", Type: "host"}}, Fingerprint: DTLSFingerprint{XMLName: xml.Name{Space: DTLSNamespace, Local: "fingerprint"}, Hash: "sha-256", Setup: "actpass", Value: "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"}, SCTP: SCTPMap{XMLName: xml.Name{Space: SCTPNamespace, Local: "sctpmap"}, Number: 5000, Protocol: "webrtc-datachannel", Streams: 16}}}}
}

func melliumIQChildDecoder(t *testing.T, encoded []byte, outer xml.StartElement) xml.TokenReader {
	t.Helper()
	decoder := xml.NewDecoder(bytes.NewReader(encoded))
	tokens := make([]xml.Token, 0, 16)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, xml.CopyToken(token))
	}
	tokens = append(tokens, outer.End())
	return &qaTokenReader{tokens: tokens}
}

func TestJingleStrictRoundTrip(t *testing.T) {
	signal := sampleJingle()
	encoded, err := EncodeJingle(signal)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeJingle(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SID != signal.SID || decoded.Content.Transport.Candidates[0].Protocol != "udp" {
		t.Fatalf("decoded=%#v", decoded)
	}
	tampered := bytes.Replace(encoded, []byte(`sid="hsk_123"`), []byte(`sid="hsk_123" extra="bad"`), 1)
	if _, err = DecodeJingle(tampered); !errors.Is(err, ErrProtocol) {
		t.Fatalf("unknown attr=%v", err)
	}
	signal.Content.Transport.Candidates[0].Protocol = "tcp"
	if _, err = EncodeJingle(signal); !errors.Is(err, ErrProtocol) {
		t.Fatalf("tcp=%v", err)
	}
	signal = sampleJingle()
	signal.Content.Description.MaximumMessageSize = 0
	if _, err = EncodeJingle(signal); !errors.Is(err, ErrProtocol) {
		t.Fatalf("missing max-message-size=%v", err)
	}
}

func TestJingleRoundTripAcceptsRuntimeV2AndMixedSessionResources(t *testing.T) {
	const installationA = "01234567-89ab-4def-8123-456789abcdef"
	const installationB = "11234567-89ab-4def-8123-456789abcdef"
	for name, identities := range map[string][2]string{
		"runtime-v2": {
			"a@example.test/r2." + installationA + ".AAAAAAAAAAAAAAAA",
			"b@example.test/r2." + installationB + ".BBBBBBBBBBBBBBBB",
		},
		"mixed-legacy-runtime-v2": {
			"a@example.test/m",
			"b@example.test/r2." + installationB + ".BBBBBBBBBBBBBBBB",
		},
	} {
		t.Run(name, func(t *testing.T) {
			signal := sampleJingle()
			signal.Initiator = identities[0]
			signal.Responder = identities[1]
			encoded, err := EncodeJingle(signal)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeJingle(encoded)
			if err != nil || decoded.Initiator != signal.Initiator || decoded.Responder != signal.Responder {
				t.Fatalf("decoded=%#v err=%v", decoded, err)
			}
		})
	}
}

func TestJingleMaximumMessageSizeExactBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		maximum uint32
		valid   bool
	}{
		{name: "minimum minus one", maximum: transport.MinimumRank1MessageBytes - 1},
		{name: "minimum", maximum: transport.MinimumRank1MessageBytes, valid: true},
		{name: "minimum plus one", maximum: transport.MinimumRank1MessageBytes + 1, valid: true},
		{name: "maximum", maximum: transport.MaximumControlFrameBytes, valid: true},
		{name: "maximum plus one", maximum: transport.MaximumControlFrameBytes + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signal := sampleJingle()
			signal.Content.Description.MaximumMessageSize = test.maximum
			encoded, err := EncodeJingle(signal)
			if test.valid {
				if err != nil {
					t.Fatal(err)
				}
				if _, err = DecodeJingle(encoded); err != nil {
					t.Fatalf("wire decode at valid boundary: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid boundary error = %v", err)
			}
		})
	}
	valid := sampleJingle()
	valid.Content.Description.MaximumMessageSize = transport.MinimumRank1MessageBytes
	encoded, err := EncodeJingle(valid)
	if err != nil {
		t.Fatal(err)
	}
	encoded = bytes.Replace(encoded,
		[]byte(`max-message-size="`+strconv.Itoa(transport.MinimumRank1MessageBytes)+`"`),
		[]byte(`max-message-size="`+strconv.Itoa(transport.MinimumRank1MessageBytes-1)+`"`), 1)
	if _, err = DecodeJingle(encoded); !errors.Is(err, ErrProtocol) {
		t.Fatalf("undersized peer advertisement error = %v", err)
	}
}

func TestMelliumJingleChildBoundaryDecoding(t *testing.T) {
	signal := sampleJingle()
	encoded, err := EncodeJingle(signal)
	if err != nil {
		t.Fatal(err)
	}
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}}
	children := &outerBoundaryTokenReader{source: melliumIQChildDecoder(t, encoded, outer), outer: outer.Name}
	counter, err := newStanzaBudget(children, outer, MaximumSignalBytes)
	if err != nil {
		t.Fatal(err)
	}
	decoder := xml.NewTokenDecoder(counter)
	var decoded Jingle
	if err = decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if token, tailErr := decoder.Token(); tailErr != io.EOF || token != nil || !children.done {
		t.Fatalf("boundary: token=%#v err=%v done=%v", token, tailErr, children.done)
	}
	if _, err = decodeJingleValue(decoded); err != nil {
		t.Fatalf("value: %#v: %v", decoded, err)
	}
	canonical, err := EncodeJingle(decoded)
	if err != nil {
		t.Fatalf("canonical re-encode: %v", err)
	}
	if bytes.Count(canonical, []byte(`xmlns="`+JingleNamespace+`"`)) != 1 {
		t.Fatalf("duplicate outer namespace after child decode: %s", canonical)
	}
	if _, err = DecodeJingle(canonical); err != nil {
		t.Fatalf("canonical result rejected: %v", err)
	}
}

func TestMelliumJingleResultConsumesExactOuterBoundary(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{
		username: "a@example.test", meshID: "mesh", management: management,
		events: make(chan Event, 1), generation: 7,
		issuedJingles: map[string]issuedJingle{
			"iq-result": {from: "a@example.test/mesh", to: "b@example.test/mesh"},
		},
		issuedJingleCapacity: 2,
	}
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"},
		{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "result"},
		{Name: xml.Name{Local: "id"}, Value: "iq-result"},
	}}
	var output bytes.Buffer
	tokens := &tokenEncoder{decoder: &qaTokenReader{tokens: []xml.Token{outer.End()}}, encoder: xml.NewEncoder(&output)}
	if err = session.handleElement(ctx, tokens, &outer); err != nil {
		t.Fatal(err)
	}
	if management.HandledInbound() != 1 {
		t.Fatalf("handled=%d", management.HandledInbound())
	}

	truncated := &tokenEncoder{decoder: &qaTokenReader{}, encoder: xml.NewEncoder(io.Discard)}
	if err = session.handleElement(ctx, truncated, &outer); !errors.Is(err, ErrProtocol) {
		t.Fatalf("truncated result accepted: %v", err)
	}
}

func TestJingleRequiresExactMeshBinding(t *testing.T) {
	valid := sampleJingle()
	for name, mutate := range map[string]func(*Jingle){
		"missing mesh":   func(signal *Jingle) { signal.Content.Description.MeshID = "" },
		"wrong resource": func(signal *Jingle) { signal.Responder = "b/other" },
		"bare initiator": func(signal *Jingle) { signal.Initiator = "a" },
	} {
		t.Run(name, func(t *testing.T) {
			signal := valid
			mutate(&signal)
			if _, err := EncodeJingle(signal); !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid group binding accepted: %v", err)
			}
		})
	}
}

type fakeSession struct {
	mu      sync.Mutex
	phases  []string
	events  chan Event
	sent    []Stanza
	resume  bool
	mailbox []Stanza
	closed  bool
}

func (f *fakeSession) ConnectTLS(context.Context, string) error {
	f.mu.Lock()
	f.phases = append(f.phases, "tls")
	f.mu.Unlock()
	return nil
}
func (f *fakeSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	f.mu.Lock()
	f.phases = append(f.phases, "auth")
	f.mu.Unlock()
	return "a@example.test", nil, nil
}
func (f *fakeSession) BindResource(context.Context, string) (string, error) {
	f.mu.Lock()
	f.phases = append(f.phases, "bind")
	f.mu.Unlock()
	return "a@example.test/mesh", nil
}
func (f *fakeSession) EnableStreamManagement(context.Context, bool) error {
	f.mu.Lock()
	f.phases = append(f.phases, "sm")
	f.mu.Unlock()
	return nil
}
func (f *fakeSession) DiscoverAuthority(context.Context) error {
	f.mu.Lock()
	f.phases = append(f.phases, "authority-discovery")
	f.mu.Unlock()
	return nil
}
func (f *fakeSession) QueryServerTime(context.Context) (time.Time, error) {
	f.mu.Lock()
	f.phases = append(f.phases, "time")
	f.mu.Unlock()
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), nil
}
func (f *fakeSession) Send(_ context.Context, s Stanza) error {
	f.mu.Lock()
	f.sent = append(f.sent, s.clone())
	f.mu.Unlock()
	return nil
}
func (f *fakeSession) Receive(ctx context.Context) (Event, error) {
	select {
	case e := <-f.events:
		return e, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (f *fakeSession) Resume(context.Context) (bool, error) { return f.resume, nil }
func (f *fakeSession) CatchUp(context.Context, int) ([]Stanza, error) {
	return append([]Stanza(nil), f.mailbox...), nil
}
func (f *fakeSession) Close(context.Context) error { f.closed = true; return nil }

type fakeDialer struct{ s Session }

func (d fakeDialer) Dial(context.Context) (Session, error) { return d.s, nil }

func durableEnvelope(t *testing.T) protocol.Envelope {
	t.Helper()
	p, _ := protocol.NewInlinePayload("native", []byte("x"))
	h := sha256.Sum256([]byte("conv"))
	e, err := protocol.NewEnvelope(protocol.EnvelopeInput{ConversationID: "conv_" + base64.RawURLEncoding.EncodeToString(h[:]), Sender: "a@example.test/mesh", Recipient: "b@example.test/mesh", MeshID: "mesh", Mode: protocol.ModeMessage, CreatedAt: time.Unix(1, 0).UTC(), ClockUncertainty: time.Millisecond, Payload: p, CredentialProof: []byte("proof")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestClientAuthenticationOrderAndIdentityPreservingSend(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	pending, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	client, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	phases := append([]string(nil), session.phases...)
	session.mu.Unlock()
	if want := []string{"tls", "auth", "bind", "sm", "authority-discovery", "time"}; len(phases) != len(want) || phases[0] != want[0] || phases[5] != want[5] {
		t.Fatalf("phases=%v", phases)
	}
	e := durableEnvelope(t)
	if err = client.Send(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	sent := append([]Stanza(nil), session.sent...)
	session.mu.Unlock()
	if len(sent) != 1 || sent[0].From != e.Sender || sent[0].To != e.Recipient {
		t.Fatalf("sent=%#v", sent)
	}
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := client.Observe().State; got != transport.HealthClosed {
		t.Fatalf("health=%v", got)
	}
}

func TestXMPPChunkCarrierUsesTextFramesAndCompletion(t *testing.T) {
	session := &fakeSession{events: make(chan Event)}
	pending, _ := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	client, err := NewClient(Config{Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{MaximumFrameBytes: 1024, MaximumChunkBytes: 512, StanzaBudgetBytes: 900, XMLOverheadBytes: 128, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	transferID := typed("xfer_", 2)
	messageID := typed("msg_", 2)
	route := payload.CarrierRoute{PeerID: "b@example.test/mesh", MeshID: "mesh", SenderID: "a@example.test/mesh", RecipientID: "b@example.test/mesh", MessageID: messageID}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: make([]byte, 513)}); !errors.Is(err, payload.ErrFrameTooLarge) {
		t.Fatalf("encoded boundary=%v", err)
	}
	if err = carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: []byte(base64.RawStdEncoding.EncodeToString([]byte("manifest")))}); err != nil {
		t.Fatal(err)
	}
	if err = carrier.SendChunk(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: []byte(base64.RawStdEncoding.EncodeToString([]byte("chunk")))}); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("payload"))
	done := make(chan error, 1)
	go func() { _, err := carrier.Finish(context.Background(), route, transferID); done <- err }()
	client.deliverCompletion(Stanza{Kind: StanzaTransferCompletion, From: route.PeerID, TransferID: transferID, MessageID: messageID, Evidence: payload.CompletionEvidence{TransferID: transferID, MessageID: messageID, Digest: digest}})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("finish did not complete")
	}
	session.mu.Lock()
	sent := append([]Stanza(nil), session.sent...)
	session.mu.Unlock()
	if len(sent) != 3 || sent[0].Kind != StanzaTransferManifest || sent[1].Kind != StanzaTransferChunk || sent[2].Kind != StanzaTransferFinish {
		t.Fatalf("sent=%#v", sent)
	}
}
