package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/stream"
)

func qaXMPPClient(t *testing.T) (*Client, *fakeSession) {
	t.Helper()
	session := &fakeSession{events: make(chan Event, 8)}
	pending, err := outbox.New(outbox.Config{MessageCapacity: 16, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint:                       "localhost:5222",
		Auth:                           Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity:                8,
		TransferWorkers:                1,
		TransferQueue:                  8,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     8,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   8,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client, session
}

func qaRoute() payload.CarrierRoute {
	return payload.CarrierRoute{
		PeerID:      "b@example.test/mesh",
		MeshID:      "mesh",
		SenderID:    "a@example.test/mesh",
		RecipientID: "b@example.test/mesh",
		MessageID:   typed("msg_", 0x31),
	}
}

func TestQAJingleRejectsTrailingXMLAndRawSDP(t *testing.T) {
	encoded, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	trailing := append(append([]byte(nil), encoded...), []byte(`<description xmlns="urn:cynapsa:aztm:1">v=0&#xA;m=application</description>`)...)
	if _, err := DecodeJingle(trailing); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing XML accepted: %v", err)
	}

	withRawSDP := bytes.Replace(encoded, []byte(`</description>`), []byte(`<sdp>v=0&#xA;m=application</sdp></description>`), 1)
	if _, err := DecodeJingle(withRawSDP); !errors.Is(err, ErrProtocol) {
		t.Fatalf("raw SDP child accepted: %v", err)
	}
}

func TestQAMessageFrameRejectsTrailingSiblingPayload(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	input := append(append([]byte(nil), frame...), []byte(`<body xmlns="jabber:client">smuggled sibling</body>`)...)
	if _, err := decodeMessageFrame(xml.NewDecoder(bytes.NewReader(input))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing message child accepted: %v", err)
	}
}

func TestQAMelliumMessageFramingAcceptsOnlyExactOuterBoundary(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}, 256, 1024)
	if err != nil {
		t.Fatal(err)
	}
	outer := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "message"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Local: "to"}, Value: "b@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "chat"},
	}}
	decode := func(children []byte) error {
		input := append([]byte(`<message xmlns="jabber:client" from="a@example.test/mesh" to="b@example.test/mesh" type="chat">`), children...)
		input = append(input, []byte(`</message>`)...)
		reader := xml.NewDecoder(bytes.NewReader(input))
		token, readErr := reader.Token()
		if readErr != nil {
			return readErr
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name != outer.Name {
			return ErrProtocol
		}
		_, readErr = decodeMelliumMessageFrameBounded(reader, start, 1024)
		return readErr
	}
	if err = decode(frame); err != nil {
		t.Fatalf("valid Mellium child framing rejected: %v", err)
	}
	frameDecoder := xml.NewDecoder(bytes.NewReader(frame))
	var liveTokens []xml.Token
	for {
		token, readErr := frameDecoder.Token()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		liveTokens = append(liveTokens, xml.CopyToken(token))
	}
	// Mellium's InnerElement reader can expose the authenticated outer end
	// without the namespace representation present on its consumed start.
	liveTokens = append(liveTokens, xml.EndElement{Name: xml.Name{Local: "message"}})
	liveFrame, err := decodeMelliumMessageFrameBounded(&qaTokenReader{tokens: liveTokens}, outer, 1024)
	if err != nil {
		t.Fatalf("Mellium namespace-normalized outer boundary rejected: %v", err)
	}
	if _, err = decodeStanzaFrameValue(liveFrame, "a@example.test/mesh", "b@example.test/mesh", "mesh", 256); err != nil {
		t.Fatalf("Mellium-decoded frame rejected by canonical wire decoder: %v", err)
	}
	trailing := append(append([]byte(nil), frame...), []byte(`<body xmlns="jabber:client">smuggled</body>`)...)
	if err = decode(trailing); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing sibling accepted: %v", err)
	}
	truncated := bytes.TrimSuffix(frame, []byte(`</frame>`))
	if err = decode(truncated); err == nil {
		t.Fatal("truncated frame accepted at outer boundary")
	}
	withoutOuterEnd := append([]byte(`<message xmlns="jabber:client" from="a@example.test/mesh" to="b@example.test/mesh" type="chat">`), frame...)
	reader := xml.NewDecoder(bytes.NewReader(withoutOuterEnd))
	token, err := reader.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := token.(xml.StartElement)
	if _, err = decodeMelliumMessageFrameBounded(reader, start, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatalf("missing outer end accepted: %v", err)
	}
}

func TestQAMelliumServerReplayAcceptsXEP0203DelayInEitherSiblingOrder(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("envelope")}, 256, 1024)
	if err != nil {
		t.Fatal(err)
	}
	delay := []byte(`<delay xmlns="urn:xmpp:delay" from="mesh.test" stamp="2026-09-03T17:19:29.123Z">Offline Storage</delay>`)
	decode := func(children []byte) error {
		input := append([]byte(`<message xmlns="jabber:client" from="a@example.test/mesh" to="b@example.test/mesh" type="chat">`), children...)
		input = append(input, []byte(`</message>`)...)
		reader := xml.NewDecoder(bytes.NewReader(input))
		token, readErr := reader.Token()
		if readErr != nil {
			return readErr
		}
		outer, ok := token.(xml.StartElement)
		if !ok {
			return ErrProtocol
		}
		decoded, readErr := decodeMelliumMessageFrameBounded(reader, outer, 4096)
		if readErr != nil {
			return readErr
		}
		_, readErr = decodeStanzaFrameValue(decoded, "a@example.test/mesh", "b@example.test/mesh", "mesh", 256)
		return readErr
	}
	for name, children := range map[string][]byte{
		"delay before frame": append(append([]byte(nil), delay...), frame...),
		// ejabberd appends XEP-0203 metadata after the original application
		// payload when replaying an offline stanza on a resumed stream.
		"frame before delay": append(append([]byte(nil), frame...), delay...),
	} {
		t.Run(name, func(t *testing.T) {
			if decodeErr := decode(children); decodeErr != nil {
				t.Fatalf("server replay rejected: %v", decodeErr)
			}
		})
	}
	for name, children := range map[string][]byte{
		"duplicate metadata": append(append(append([]byte(nil), delay...), frame...), delay...),
		"metadata child":     append(append([]byte(nil), frame...), []byte(`<delay xmlns="urn:xmpp:delay" stamp="2026-09-03T17:19:29Z"><extra/></delay>`)...),
		"unknown sibling":    append(append([]byte(nil), frame...), []byte(`<extra xmlns="urn:xmpp:delay"/>`)...),
	} {
		t.Run(name, func(t *testing.T) {
			if decodeErr := decode(children); !errors.Is(decodeErr, ErrProtocol) {
				t.Fatalf("invalid replay metadata accepted: %v", decodeErr)
			}
		})
	}
}

type qaTokenReader struct {
	tokens []xml.Token
	next   int
}

func (reader *qaTokenReader) Token() (xml.Token, error) {
	if reader == nil || reader.next >= len(reader.tokens) {
		return nil, io.EOF
	}
	token := reader.tokens[reader.next]
	reader.next++
	return token, nil
}

func TestQAXMPPStanzaBudgetIncludesRoutingAndMessageWrapper(t *testing.T) {
	record := Stanza{
		Kind:   StanzaEnvelope,
		From:   "a@example.test/mesh",
		To:     strings.Repeat("b", 200) + "@example.test/mesh",
		MeshID: "mesh",
		Data:   []byte("envelope"),
	}
	inner, err := EncodeStanzaFrame(record, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	// The child itself fits, but even the mandatory message wrapper plus the
	// destination attribute cannot fit in one additional byte.
	if _, err := EncodeStanzaFrame(record, 256, len(inner)+1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("complete-stanza budget ignored routing wrapper: %v", err)
	}
}

func TestQAJingleRejectsInvalidCandidateFingerprintAndNegotiatedBounds(t *testing.T) {
	tests := map[string]func(*Jingle){
		"zero max message":      func(j *Jingle) { j.Content.Description.MaximumMessageSize = 0 },
		"undersize max message": func(j *Jingle) { j.Content.Description.MaximumMessageSize = transport.MinimumRank1MessageBytes - 1 },
		"oversize max message":  func(j *Jingle) { j.Content.Description.MaximumMessageSize = 2<<20 + 1 },
		"zero streams":          func(j *Jingle) { j.Content.Transport.SCTP.Streams = 0 },
		"partial fingerprint":   func(j *Jingle) { j.Content.Transport.Fingerprint.Value = "AA:BB" },
		"tcp candidate":         func(j *Jingle) { j.Content.Transport.Candidates[0].Protocol = "tcp" },
		"missing related port":  func(j *Jingle) { j.Content.Transport.Candidates[0].RelAddr = "192.0.2.1" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := sampleJingle()
			mutate(&value)
			if _, err := EncodeJingle(value); !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid Jingle error = %v", err)
			}
		})
	}

	tooMany := sampleJingle()
	tooMany.Content.Transport.Candidates = make([]ICECandidate, MaximumJingleCandidates+1)
	if _, err := EncodeJingle(tooMany); !errors.Is(err, ErrProtocol) {
		t.Fatalf("candidate capacity error = %v", err)
	}
}

func TestQAXEP0198WraparoundDuplicateAckAndReplay(t *testing.T) {
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	// Package-local setup reaches the RFC 1982 boundary without allocating
	// 2^32 test records.
	sm.outbound = math.MaxUint32 - 1
	sm.acked = math.MaxUint32 - 1
	sm.serverAcked = math.MaxUint32 - 1
	if err := sm.RecordSent(Stanza{Ordinal: 10, Data: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := sm.RecordSent(Stanza{Ordinal: 11, Data: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	ordinal, err := sm.ApplyAck(math.MaxUint32)
	if err != nil || ordinal != 10 || sm.Pending() != 1 {
		t.Fatalf("first ack: ordinal=%d pending=%d err=%v", ordinal, sm.Pending(), err)
	}
	if ordinal, err = sm.ApplyAck(math.MaxUint32); err != nil || ordinal != 0 || sm.Pending() != 1 {
		t.Fatalf("duplicate ack: ordinal=%d pending=%d err=%v", ordinal, sm.Pending(), err)
	}
	replay, ordinal, err := sm.ResumeAccepted(0)
	if err != nil || ordinal != 11 || len(replay) != 0 || sm.Pending() != 0 {
		t.Fatalf("wrapped ack: ordinal=%d replay=%#v pending=%d err=%v", ordinal, replay, sm.Pending(), err)
	}
	if _, err := sm.ApplyAck(math.MaxUint32); !errors.Is(err, ErrProtocol) {
		t.Fatalf("backward ack error = %v", err)
	}
}

func TestQAXEP0198ResumeRejectionPreservesAllPendingPrivateTraffic(t *testing.T) {
	kinds := []StanzaKind{
		StanzaEnvelope,
		StanzaSignal,
		StanzaSignalResult,
		StanzaTransferManifest,
		StanzaTransferChunk,
		StanzaTransferFinish,
		StanzaTransferCompletion,
		StanzaTransferAbort,
		StanzaObjectReadinessRequest,
		StanzaObjectReadinessResult,
	}
	sm, err := NewStreamManagement(len(kinds), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	for i, kind := range kinds {
		if err := sm.RecordSent(Stanza{Kind: kind, AttemptID: "attempt", TransferID: typed("xfer_", byte(i+1)), Data: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	replay := sm.ResumeRejected()
	if len(replay) != len(kinds) {
		t.Fatalf("replay count = %d", len(replay))
	}
	for i, record := range replay {
		if record.Kind == StanzaEnvelope {
			if record.Kind != kinds[i] || len(record.Data) != 0 {
				t.Fatalf("replay[%d] = %#v", i, record)
			}
			continue
		}
		if record.Kind != kinds[i] || len(record.Data) != 1 || record.Data[0] != byte(i) {
			t.Fatalf("replay[%d] = %#v", i, record)
		}
	}
	if sm.Pending() != 0 {
		t.Fatalf("pending after rejection = %d", sm.Pending())
	}
}

type qaTokenReadEncoder struct {
	decoder xml.TokenReader
	encoder *xml.Encoder
}

func (q *qaTokenReadEncoder) Token() (xml.Token, error) { return q.decoder.Token() }
func (q *qaTokenReadEncoder) Encode(value interface{}) error {
	return q.encoder.Encode(value)
}
func (q *qaTokenReadEncoder) EncodeElement(value interface{}, start xml.StartElement) error {
	return q.encoder.EncodeElement(value, start)
}
func (q *qaTokenReadEncoder) EncodeToken(token xml.Token) error { return q.encoder.EncodeToken(token) }

var _ xmlstream.TokenReadEncoder = (*qaTokenReadEncoder)(nil)

type qaTerminalSignalReader struct {
	source   xml.TokenReader
	terminal chan struct{}
	once     sync.Once
}

func (r *qaTerminalSignalReader) Token() (xml.Token, error) {
	token, err := r.source.Token()
	if err != nil {
		r.once.Do(func() { close(r.terminal) })
	}
	return token, err
}

func TestQATrackedIQReleasesWriteGateWhileAwaitingResponse(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: &output}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	defer retire()
	session := &melliumSession{
		config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
		username: "a@example.test", meshID: "mesh", session: xsession, management: sm, ctx: lifetime, cancel: retire,
		events: make(chan Event, 2), writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	terminal := make(chan struct{})
	query := xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}
	payload := &qaTerminalSignalReader{source: xmlstream.Wrap(nil, query), terminal: terminal}
	server, _ := jid.Parse("example.test")
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: "qa-correlated", To: server, Type: stanza.GetIQ}
	record := Stanza{Kind: StanzaTimeCalibration, From: origin.String(), To: server.String(), MeshID: "mesh", MessageID: iq.ID}
	type queryResult struct {
		sequence uint32
		err      error
	}
	queryDone := make(chan queryResult, 1)
	go func() {
		response, sequence, queryErr := session.sendTrackedIQElement(ctx, xsession, sm, record, payload, iq)
		if response != nil {
			_ = response.Close()
		}
		queryDone <- queryResult{sequence: sequence, err: queryErr}
	}()
	select {
	case <-terminal:
	case <-time.After(time.Second):
		cancel()
		<-queryDone
		t.Fatal("Mellium did not consume the correlated IQ payload")
	}
	gateCtx, gateCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err = session.acquireWrite(gateCtx)
	gateCancel()
	var laterErr error
	if err == nil {
		laterErr = sm.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 41, MessageID: "later"})
		session.releaseWrite()
	}
	cancel()
	result := <-queryDone
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("correlated IQ wait returned %v after cancellation", result.err)
	}
	if err != nil {
		t.Fatalf("write gate remained held while correlated IQ awaited its response: %v", err)
	}
	if laterErr != nil {
		t.Fatalf("later stream-management admission failed: %v", laterErr)
	}
	if result.sequence == 0 {
		t.Fatal("tracked IQ did not return its stream-management sequence")
	}
	if _, count, confirmErr := sm.ConfirmCorrelatedHandled(result.sequence); confirmErr != nil || count != 1 {
		t.Fatalf("correlated IQ confirmation count=%d err=%v", count, confirmErr)
	}
	if pending := sm.PendingSnapshot(); len(pending) != 1 || pending[0].Kind != StanzaEnvelope || pending[0].Ordinal != 41 {
		t.Fatalf("correlated IQ confirmation consumed later traffic: %#v", pending)
	}
}

func TestQATrackedIQExactSessionLossCancelsWait(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	var output bytes.Buffer
	xsession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: &output}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	session := &melliumSession{
		config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
		username: "a@example.test", meshID: "mesh", session: xsession, management: sm, ctx: lifetime, cancel: retire,
		events: make(chan Event, 1), writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}
	caller, cancelCaller := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCaller()
	terminal := make(chan struct{})
	query := xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}
	payload := &qaTerminalSignalReader{source: xmlstream.Wrap(nil, query), terminal: terminal}
	server, _ := jid.Parse("example.test")
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: "qa-session-loss", To: server, Type: stanza.GetIQ}
	record := Stanza{Kind: StanzaTimeCalibration, From: origin.String(), To: server.String(), MeshID: "mesh", MessageID: iq.ID}
	type queryResult struct {
		response xmlstream.TokenReadCloser
		sequence uint32
		err      error
	}
	done := make(chan queryResult, 1)
	go func() {
		response, sequence, queryErr := session.sendTrackedIQElement(caller, xsession, sm, record, payload, iq)
		done <- queryResult{response: response, sequence: sequence, err: queryErr}
	}()
	select {
	case <-terminal:
	case <-time.After(time.Second):
		retire()
		result := <-done
		if result.response != nil {
			_ = result.response.Close()
		}
		t.Fatal("Mellium did not send the correlated IQ before session loss")
	}
	started := time.Now()
	retire()
	select {
	case result := <-done:
		if result.response != nil {
			_ = result.response.Close()
			t.Fatal("session loss returned an IQ response")
		}
		if !errors.Is(result.err, ErrUnavailable) {
			t.Fatalf("session loss returned %v", result.err)
		}
		if result.sequence == 0 {
			t.Fatal("sent IQ was not retained in stream management")
		}
	case <-time.After(time.Second):
		t.Fatal("session loss did not cancel the correlated IQ wait")
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("session-loss cancellation took %v", elapsed)
	}
	if err := caller.Err(); err != nil {
		t.Fatalf("session loss incorrectly canceled caller context: %v", err)
	}
	if pending := sm.PendingSnapshot(); len(pending) != 1 || pending[0].Kind != StanzaTimeCalibration || pending[0].MessageID != iq.ID {
		t.Fatalf("ambiguous control IQ was not retained for the resume fence: %#v", pending)
	}
	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	if err = session.acquireWrite(gateCtx); err != nil {
		t.Fatalf("session-loss cancellation leaked the write gate: %v", err)
	}
	session.releaseWrite()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err = session.Close(closeCtx); err != nil {
		t.Fatalf("session-loss cleanup did not release exact-session ownership: %v", err)
	}
}

func TestQAMelliumServeRetiresExactSessionLifetime(t *testing.T) {
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: io.Discard}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	session := &melliumSession{
		session: xsession, ctx: lifetime, cancel: retire, generation: 7,
		serveDone: make(chan serveResult, 1), events: make(chan Event, 1),
	}
	session.serveWG.Add(1)
	go session.serve(xsession, lifetime, 7)
	select {
	case <-lifetime.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal Mellium Serve did not retire its exact session lifetime")
	}
	done := make(chan struct{})
	go func() {
		session.serveWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal Mellium Serve did not release its ownership")
	}
}

func TestQAMelliumServeDoesNotRetireReplacementSessionLifetime(t *testing.T) {
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	staleSession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: io.Discard}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	currentSession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: io.Discard}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	staleLifetime := context.Background()
	currentLifetime, retireCurrent := context.WithCancel(context.Background())
	defer retireCurrent()
	session := &melliumSession{
		session: currentSession, ctx: currentLifetime, cancel: retireCurrent, generation: 8,
		serveDone: make(chan serveResult, 1), events: make(chan Event, 1),
	}
	session.serveWG.Add(1)
	go session.serve(staleSession, staleLifetime, 7)
	done := make(chan struct{})
	go func() {
		session.serveWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stale Mellium Serve did not release its ownership")
	}
	if err := currentLifetime.Err(); err != nil {
		t.Fatalf("stale Mellium Serve retired replacement lifetime: %v", err)
	}
}

func TestQAMelliumLocalCloseDoesNotReportGracefulWriteAfterSocketRetirement(t *testing.T) {
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	conn, peer := net.Pipe()
	defer peer.Close()
	xsession, err := xmpp.NewSession(context.Background(), location, origin, conn, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	session := newMelliumSession(MelliumConfig{ReceiveCapacity: 1}, Endpoint{})
	session.conn = conn
	session.session = xsession
	session.ctx = lifetime
	session.cancel = retire
	session.generation = 1
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err = session.Close(closeCtx); err != nil {
		t.Fatalf("local socket retirement became a provider shutdown failure: %v", err)
	}
	if lifetime.Err() == nil {
		t.Fatal("local close did not retire the exact session lifetime")
	}
}

func TestQATrackedIQCanceledAdmissionDoesNotMutateStreamManagement(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: io.Discard}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	defer retire()
	session := &melliumSession{
		config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
		username: "a@example.test", meshID: "mesh", session: xsession, management: sm, ctx: lifetime, cancel: retire,
		events: make(chan Event, 1), writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server, _ := jid.Parse("example.test")
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: "qa-canceled", To: server, Type: stanza.GetIQ}
	record := Stanza{Kind: StanzaTimeCalibration, From: origin.String(), To: server.String(), MeshID: "mesh", MessageID: iq.ID}
	response, sequence, err := session.sendTrackedIQElement(ctx, xsession, sm, record, xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}), iq)
	if response != nil || sequence != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled IQ admission returned response=%v sequence=%d err=%v", response, sequence, err)
	}
	if pending := sm.Pending(); pending != 0 {
		t.Fatalf("canceled IQ admission retained %d stream-management entries", pending)
	}
	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	if err = session.acquireWrite(gateCtx); err != nil {
		t.Fatalf("canceled IQ admission leaked the write gate: %v", err)
	}
	session.releaseWrite()
}

func TestQATrackedIQCallerCancellationWinsOverRetiredLifetime(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, qaReadWriter{Reader: strings.NewReader(""), Writer: io.Discard}, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, retire := context.WithCancel(context.Background())
	retire()
	session := &melliumSession{
		config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
		username: "a@example.test", meshID: "mesh", session: xsession, management: sm, ctx: lifetime, cancel: retire,
		events: make(chan Event, 1), writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server, _ := jid.Parse("example.test")
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: "qa-both-canceled", To: server, Type: stanza.GetIQ}
	record := Stanza{Kind: StanzaTimeCalibration, From: origin.String(), To: server.String(), MeshID: "mesh", MessageID: iq.ID}
	response, sequence, err := session.sendTrackedIQElement(ctx, xsession, sm, record, xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: entityTimeNamespace, Local: "time"}}), iq)
	if response != nil || sequence != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation lost to retired lifetime: response=%v sequence=%d err=%v", response, sequence, err)
	}
	if pending := sm.Pending(); pending != 0 {
		t.Fatalf("both-canceled IQ admission retained %d stream-management entries", pending)
	}
	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	if err = session.acquireWrite(gateCtx); err != nil {
		t.Fatalf("both-canceled IQ admission leaked the write gate: %v", err)
	}
	session.releaseWrite()
}

func TestQACorrelatedOperationGatePreservesContextCancellation(t *testing.T) {
	session := &melliumSession{correlatedGate: make(chan struct{}, 1)}
	session.correlatedGate <- struct{}{}
	if err := session.acquireCorrelated(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := session.acquireCorrelated(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended correlated operation returned %v", err)
	}
	session.releaseCorrelated()
	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), time.Second)
	defer acquireCancel()
	if err := session.acquireCorrelated(acquireCtx); err != nil {
		t.Fatalf("released correlated gate remained unavailable: %v", err)
	}
	session.releaseCorrelated()
}

func TestQAXEP0198AckRequestDoesNotIncrementHandledStanzaCount(t *testing.T) {
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{meshID: "mesh", management: sm}
	var output bytes.Buffer
	request := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "r"}}
	tokens := &qaTokenReadEncoder{decoder: &qaTokenReader{tokens: []xml.Token{request.End()}}, encoder: xml.NewEncoder(&output)}
	if err := session.handleElement(context.Background(), tokens, &request); err != nil {
		t.Fatal(err)
	}
	if _, handled, _ := sm.ResumeState(); handled != 0 {
		t.Fatalf("XEP-0198 <r/> counted as handled stanza: h=%d", handled)
	}
}

func TestQAXEP0198RejectedIQDoesNotAdvanceHandledCount(t *testing.T) {
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{username: "a@example.test", meshID: "mesh", management: sm, events: make(chan Event)}
	start := xml.StartElement{
		Name: xml.Name{Space: "jabber:client", Local: "iq"},
		Attr: []xml.Attr{
			{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"},
			{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
			{Name: xml.Name{Local: "type"}, Value: "set"},
			{Name: xml.Name{Local: "id"}, Value: "iq-1"},
		},
	}
	var output bytes.Buffer
	tokens := &qaTokenReadEncoder{
		decoder: xml.NewDecoder(bytes.NewBufferString(`<invalid xmlns="urn:invalid"/>`)),
		encoder: xml.NewEncoder(&output),
	}
	if err := session.handleElement(context.Background(), tokens, &start); err == nil {
		t.Fatal("malformed IQ unexpectedly accepted")
	}
	if _, handled, _ := sm.ResumeState(); handled != 0 {
		t.Fatalf("rejected IQ counted as handled stanza: h=%d", handled)
	}
}

func TestQAAutomaticIQResultAndEnvelopeUseOneCumulativeAckLedger(t *testing.T) {
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{username: "a@example.test", meshID: "mesh", management: sm, events: make(chan Event, 4)}
	jingle, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	iq := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "b@example.test/r2.00000000-0000-4000-8000-000000000002.BBBBBBBB"},
		{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "set"},
		{Name: xml.Name{Local: "id"}, Value: "iq-qa"},
	}}
	var output bytes.Buffer
	tokens := &qaTokenReadEncoder{decoder: melliumIQChildDecoder(t, jingle, iq), encoder: xml.NewEncoder(&output)}
	if err = session.handleElement(context.Background(), tokens, &iq); err != nil {
		t.Fatal(err)
	}
	if event := <-session.events; event.Kind != EventStanza || event.Stanza.Kind != StanzaSignal {
		t.Fatalf("incoming IQ event = %#v", event)
	}
	envelope := Stanza{Kind: StanzaEnvelope, Ordinal: 41, Data: []byte("application-envelope")}
	if err = sm.RecordSent(envelope); err != nil {
		t.Fatal(err)
	}
	ack := func(h string) Event {
		t.Helper()
		start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "a"}, Attr: []xml.Attr{{Name: xml.Name{Local: "h"}, Value: h}}}
		tokens := &qaTokenReadEncoder{decoder: &qaTokenReader{tokens: []xml.Token{start.End()}}, encoder: xml.NewEncoder(io.Discard)}
		if ackErr := session.handleElement(context.Background(), tokens, &start); ackErr != nil {
			t.Fatal(ackErr)
		}
		return <-session.events
	}
	if event := ack("1"); event.Kind != EventHandled || event.HandledThrough != 0 || event.HandledCount != 1 || sm.Pending() != 1 {
		t.Fatalf("Q acknowledgement = %#v pending=%d", event, sm.Pending())
	}
	if pending := sm.PendingSnapshot(); len(pending) != 1 || pending[0].Kind != StanzaEnvelope || pending[0].Ordinal != 41 {
		t.Fatalf("ledger after Q acknowledgement = %#v", pending)
	}
	if event := ack("2"); event.Kind != EventHandled || event.HandledThrough != 41 || event.HandledCount != 1 || sm.Pending() != 0 {
		t.Fatalf("E acknowledgement = %#v pending=%d", event, sm.Pending())
	}
}

type qaFailingTokenReadEncoder struct{ decoder xml.TokenReader }

func (q *qaFailingTokenReadEncoder) Token() (xml.Token, error) { return q.decoder.Token() }
func (*qaFailingTokenReadEncoder) Encode(interface{}) error    { return errors.New("qa output failure") }
func (*qaFailingTokenReadEncoder) EncodeElement(interface{}, xml.StartElement) error {
	return errors.New("qa output failure")
}
func (*qaFailingTokenReadEncoder) EncodeToken(xml.Token) error {
	return errors.New("qa output failure")
}

func TestQAAutomaticIQAmbiguousWriteRetainsLedgerCapacity(t *testing.T) {
	sm, err := NewStreamManagement(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{username: "a@example.test", meshID: "mesh", management: sm, events: make(chan Event, 1)}
	jingle, err := EncodeJingle(sampleJingle())
	if err != nil {
		t.Fatal(err)
	}
	iq := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"},
		{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "set"},
		{Name: xml.Name{Local: "id"}, Value: "iq-fail"},
	}}
	tokens := &qaFailingTokenReadEncoder{decoder: melliumIQChildDecoder(t, jingle, iq)}
	if err = session.handleElement(context.Background(), tokens, &iq); err == nil {
		t.Fatal("automatic IQ result write unexpectedly succeeded")
	}
	if sm.Pending() != 1 {
		t.Fatalf("ambiguous automatic result was removed from ledger: %d", sm.Pending())
	}
	if err = sm.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 51, Data: []byte("next")}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("retained ambiguous result did not consume capacity: %v", err)
	}
}

func TestQADefinitivePreWireFailureRollsBackLedgerCapacity(t *testing.T) {
	sm, _ := NewStreamManagement(1, 1<<20)
	_ = sm.Enable("resume-token", true)
	first := Stanza{Kind: StanzaEnvelope, Ordinal: 52, Data: []byte("invalid-before-wire")}
	if err := sm.RecordSent(first); err != nil {
		t.Fatal(err)
	}
	canary := errors.New("qa definitive pre-wire failure")
	if err := finishManagedSend(sm, first, &wireError{stage: wireNotStarted, cause: canary}); !errors.Is(err, canary) {
		t.Fatalf("pre-wire failure = %v", err)
	}
	if sm.Pending() != 0 {
		t.Fatalf("definitive pre-wire failure retained capacity: %d", sm.Pending())
	}
	if err := sm.RecordSent(Stanza{Kind: StanzaEnvelope, Ordinal: 53, Data: []byte("next")}); err != nil {
		t.Fatalf("rolled-back capacity was not reusable: %v", err)
	}
}

type qaFailAfterWriter struct {
	bytes.Buffer
	writes int
}

func (w *qaFailAfterWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errors.New("qa acknowledgement-request write failure")
	}
	return w.Buffer.Write(p)
}

type qaReadWriter struct {
	io.Reader
	io.Writer
}

func TestQAFullEnvelopeThenAckRequestFailureRemainsReplayable(t *testing.T) {
	sm, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	writer := &qaFailAfterWriter{}
	rw := qaReadWriter{Reader: strings.NewReader(""), Writer: writer}
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, rw, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	record := Stanza{Kind: StanzaEnvelope, From: origin.String(), To: "b@example.test/mesh", MeshID: "mesh", Ordinal: 61, MessageID: typed("msg_", 0x61), Data: []byte("opaque")}
	session := &melliumSession{
		config:  MelliumConfig{MaximumFrameBytes: 1024, StanzaBudgetBytes: 2048},
		session: xsession, management: sm, writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}
	if err = session.Send(context.Background(), record); err == nil {
		t.Fatal("post-stanza acknowledgement-request failure reported success")
	}
	if writer.writes != 2 || sm.Pending() != 1 {
		t.Fatalf("writes=%d pending=%d", writer.writes, sm.Pending())
	}
	if pending := sm.PendingSnapshot(); len(pending) != 1 || pending[0].Ordinal != record.Ordinal {
		t.Fatalf("ambiguous full stanza not replayable: %#v", pending)
	}
}

func TestQAReceiveIgnoresStaleServeCompletionAfterGenerationSwap(t *testing.T) {
	session := &melliumSession{events: make(chan Event, 1), serveDone: make(chan serveResult, 2), generation: 9}
	session.serveDone <- serveResult{generation: 8, err: errors.New("stale old connection failure")}
	session.events <- Event{Kind: EventMailboxComplete}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := session.Receive(ctx)
	if err != nil || event.Kind != EventMailboxComplete {
		t.Fatalf("stale Serve completion escaped swapped generation: event=%#v err=%v", event, err)
	}
}

func TestQASuccessfulResumeThawsSendsAndEventBackpressureUsesAttemptDeadline(t *testing.T) {
	sm, _ := NewStreamManagement(2, 1<<20)
	_ = sm.Enable("resume-token", true)
	var output bytes.Buffer
	rw := qaReadWriter{Reader: strings.NewReader(""), Writer: &output}
	location, _ := jid.Parse("example.test")
	origin, _ := jid.Parse("a@example.test/mesh")
	noop := func(context.Context, *stream.Info, *stream.Info, *xmpp.Session, interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		return xmpp.Ready, nil, nil, nil
	}
	xsession, err := xmpp.NewSession(context.Background(), location, origin, rw, xmpp.Ready, noop)
	if err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{
		config:  MelliumConfig{MaximumFrameBytes: 1024, StanzaBudgetBytes: 2048},
		session: xsession, management: sm, suspended: true, events: make(chan Event, 1), writeGate: make(chan struct{}, 1),
	}
	session.writeGate <- struct{}{}
	// This is the local state transition made only after a successful XEP-0198
	// resume. A clean failure freezes sends; successful resume must thaw them.
	session.markResumedActive()
	record := Stanza{Kind: StanzaEnvelope, From: origin.String(), To: "b@example.test/mesh", MeshID: "mesh", Ordinal: 71, MessageID: typed("msg_", 0x71), Data: []byte("after-resume")}
	if err = session.Send(context.Background(), record); err != nil {
		t.Fatalf("send remained frozen after successful resume: %v", err)
	}

	session.events <- Event{Kind: EventMailboxComplete}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = session.emit(ctx, Event{Kind: EventHandled, HandledCount: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backpressured resume event error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("backpressured resume event ignored operation deadline: %v", elapsed)
	}
}

func TestQACustodyAcceptedRequiresExactBareAuthorityAndConfirmsLedger(t *testing.T) {
	messageID := typed("msg_", 0x7c)
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	if err = sm.RecordSent(Stanza{
		Kind: StanzaEnvelope, From: "a@example.test/mesh", To: "b@example.test/mesh",
		MeshID: "mesh", Ordinal: 77, MessageID: messageID,
	}); err != nil {
		t.Fatal(err)
	}
	session := &melliumSession{
		config: MelliumConfig{StanzaBudgetBytes: 4096}, username: "a@example.test",
		meshID: "mesh", management: sm, events: make(chan Event, 2),
	}
	wire := `<message xmlns="jabber:client" from="example.test" to="a@example.test/mesh" type="headline" id="` + messageID + `"><accepted xmlns="urn:cynapsa:mesh-custody:1" message-id="` + messageID + `"/></message>`
	decoder := xml.NewDecoder(strings.NewReader(wire))
	token, err := decoder.Token()
	start, ok := token.(xml.StartElement)
	if err != nil || !ok {
		t.Fatalf("outer start = %#v err=%v", token, err)
	}
	tokens := &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(io.Discard)}
	if err = session.handleElement(context.Background(), tokens, &start); err != nil {
		t.Fatal(err)
	}
	event := <-session.events
	if event.Kind != EventCustodyAccepted || event.MessageID != messageID || sm.Pending() != 0 {
		t.Fatalf("custody event=%#v pending=%d", event, sm.Pending())
	}

	for _, forged := range []string{
		`<message xmlns="jabber:client" from="mallory@example.test/mesh" to="a@example.test/mesh" type="headline" id="` + messageID + `"><accepted xmlns="urn:cynapsa:mesh-custody:1" message-id="` + messageID + `"/></message>`,
		`<message xmlns="jabber:client" from="other.test" to="a@example.test/mesh" type="headline" id="` + messageID + `"><accepted xmlns="urn:cynapsa:mesh-custody:1" message-id="` + messageID + `"/></message>`,
		`<message xmlns="jabber:client" from="example.test" to="a@example.test/other" type="headline" id="` + messageID + `"><accepted xmlns="urn:cynapsa:mesh-custody:1" message-id="` + messageID + `"/></message>`,
	} {
		decoder = xml.NewDecoder(strings.NewReader(forged))
		token, _ = decoder.Token()
		start = token.(xml.StartElement)
		tokens = &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(io.Discard)}
		if err = session.handleElement(context.Background(), tokens, &start); err == nil {
			t.Fatalf("forged custody control accepted: %s", forged)
		}
	}
}

func TestQAEjabberdCustodyWireBeforeEarlierSMACKDoesNotRetireStream(t *testing.T) {
	messageID := "msg_xyGEBoDDtOUROy60tb96Mg"
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.Enable("ejabberd-custody-order", true); err != nil {
		t.Fatal(err)
	}
	for _, record := range []Stanza{
		{Kind: StanzaSignalResult, AttemptID: "setup"},
		{Kind: StanzaEnvelope, From: "agent-a@mesh.test/e2e-mesh", To: "agent-b@mesh.test/e2e-mesh", MeshID: "e2e-mesh", Ordinal: 7, MessageID: messageID},
	} {
		if err = sm.RecordSent(record); err != nil {
			t.Fatal(err)
		}
	}
	session := &melliumSession{
		config: MelliumConfig{StanzaBudgetBytes: 4096}, username: "agent-a@mesh.test",
		meshID: "e2e-mesh", management: sm, events: make(chan Event, 4),
	}
	handle := func(wire string) error {
		decoder := xml.NewDecoder(strings.NewReader(wire))
		token, decodeErr := decoder.Token()
		start, ok := token.(xml.StartElement)
		if decodeErr != nil || !ok {
			t.Fatalf("outer start=%#v err=%v", token, decodeErr)
		}
		return session.handleElement(context.Background(), &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(io.Discard)}, &start)
	}
	// This is the exact attribute order and self-closing child emitted by
	// xmpp:encode/1 for mod_cynapsa_mesh's #message custody receipt.
	receipt := `<message to='agent-a@mesh.test/e2e-mesh' from='mesh.test' type='headline' id='` + messageID + `' xmlns='jabber:client'><accepted xmlns='urn:cynapsa:mesh-custody:1' message-id='` + messageID + `'/></message>`
	if err = handle(receipt); err != nil {
		t.Fatal(err)
	}
	if event := <-session.events; event.Kind != EventCustodyAccepted || event.MessageID != messageID || event.HandledThrough != 7 || event.HandledCount != 2 {
		t.Fatalf("custody event=%#v", event)
	}
	// ejabberd may already have queued this earlier cumulative ACK when the
	// asynchronously routed custody receipt overtakes it on the c2s mailbox.
	if err = handle(`<a xmlns='urn:xmpp:sm:3' h='1'/>`); err != nil {
		t.Fatal(err)
	}
	if event := <-session.events; event.Kind != EventHandled || event.HandledThrough != 0 || event.HandledCount != 0 || sm.Pending() != 0 {
		t.Fatalf("earlier ack event=%#v pending=%d", event, sm.Pending())
	}
}

func TestQAXMPPEnvelopeOuterMessageIDMustMatchDecodedEnvelope(t *testing.T) {
	envelope := ingressEnvelope(t, 1, "outer-id-binding")
	codec, err := protocol.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(envelope)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	inner, err := EncodeStanzaFrame(Stanza{
		Kind: StanzaEnvelope, From: ingressRemote, To: ingressLocal,
		MeshID: ingressMesh, MessageID: envelope.MessageID, Data: encoded,
	}, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	handle := func(outerID string) (Event, error) {
		t.Helper()
		id := ""
		if outerID != "" {
			id = ` id="` + outerID + `"`
		}
		wire := `<message xmlns="jabber:client" from="` + ingressRemote + `" to="` + ingressLocal + `" type="chat"` + id + `>` + string(inner) + `</message>`
		decoder := xml.NewDecoder(strings.NewReader(wire))
		token, tokenErr := decoder.Token()
		start, ok := token.(xml.StartElement)
		if tokenErr != nil || !ok {
			t.Fatalf("outer start=%#v err=%v", token, tokenErr)
		}
		sm, smErr := NewStreamManagement(4, 1<<20)
		if smErr != nil {
			t.Fatal(smErr)
		}
		if smErr = sm.Enable("outer-message-id", true); smErr != nil {
			t.Fatal(smErr)
		}
		session := &melliumSession{
			config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
			username: "local@example.test", meshID: ingressMesh,
			management: sm, events: make(chan Event, 1),
		}
		tokens := &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(io.Discard)}
		if handleErr := session.handleElement(context.Background(), tokens, &start); handleErr != nil {
			return Event{}, handleErr
		}
		event := <-session.events
		event.Stanza.inboundAccept.decide(true)
		return event, nil
	}

	got, err := handle(envelope.MessageID)
	if err != nil || got.Kind != EventStanza || got.Stanza.MessageID != envelope.MessageID {
		t.Fatalf("exact outer binding event=%#v err=%v", got, err)
	}
	clearStanzaOwned(&got.Stanza)
	for _, outerID := range []string{"", typed("msg_", 0x7d)} {
		if got, err = handle(outerID); !errors.Is(err, ErrProtocol) {
			clearStanzaOwned(&got.Stanza)
			t.Fatalf("outer id %q accepted: event=%#v err=%v", outerID, got, err)
		}
	}
}

func TestQAXMPPNonEnvelopeFramesRequireNoOuterMessageID(t *testing.T) {
	transferID := typed("xfer_", 0x66)
	inner, err := EncodeStanzaFrame(Stanza{
		Kind: StanzaTransferFinish, From: ingressRemote, To: ingressLocal,
		MeshID: ingressMesh, TransferID: transferID,
	}, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	handle := func(outerID string) (Event, error) {
		id := ""
		if outerID != "" {
			id = ` id="` + outerID + `"`
		}
		wire := `<message xmlns="jabber:client" from="` + ingressRemote + `" to="` + ingressLocal + `" type="chat"` + id + `>` + string(inner) + `</message>`
		decoder := xml.NewDecoder(strings.NewReader(wire))
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return Event{}, tokenErr
		}
		start := token.(xml.StartElement)
		sm, _ := NewStreamManagement(4, 1<<20)
		session := &melliumSession{
			config:   MelliumConfig{MaximumFrameBytes: 1 << 20, StanzaBudgetBytes: 1 << 20},
			username: "local@example.test", meshID: ingressMesh,
			management: sm, events: make(chan Event, 1),
		}
		tokens := &qaTokenReadEncoder{decoder: decoder, encoder: xml.NewEncoder(io.Discard)}
		if handleErr := session.handleElement(context.Background(), tokens, &start); handleErr != nil {
			return Event{}, handleErr
		}
		return <-session.events, nil
	}
	got, err := handle("")
	if err != nil || got.Kind != EventStanza || got.Stanza.Kind != StanzaTransferFinish || got.Stanza.TransferID != transferID {
		t.Fatalf("id-free transfer event=%#v err=%v", got, err)
	}
	clearStanzaOwned(&got.Stanza)
	for _, outerID := range []string{"367a1cb35a378489", typed("msg_", 0x67)} {
		if got, err = handle(outerID); !errors.Is(err, ErrProtocol) {
			clearStanzaOwned(&got.Stanza)
			t.Fatalf("non-envelope outer id %q accepted: event=%#v err=%v", outerID, got, err)
		}
	}
}

func TestQAInboundBudgetIsCumulativeAcrossOuterChildAndExtraAttributes(t *testing.T) {
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "message"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"},
		{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Space: "urn:qa:attributes", Local: "padding"}, Value: `<&" cumulative`},
	}}
	child := `<aztm xmlns="urn:cynapsa:aztm:1" kind="envelope" extra="123"><data>eCZ5</data></aztm>`
	exact := xmlTokenSize(outer) + xmlTokenSize(outer.End())
	decoder := xml.NewDecoder(strings.NewReader(child))
	for {
		token, tokenErr := decoder.Token()
		if tokenErr == io.EOF {
			break
		}
		if tokenErr != nil {
			t.Fatal(tokenErr)
		}
		exact += xmlTokenSize(token)
	}
	consume := func(limit int) error {
		counter, makeErr := newStanzaBudget(xml.NewDecoder(strings.NewReader(child)), outer, limit)
		if makeErr != nil {
			return makeErr
		}
		for {
			if _, readErr := counter.Token(); readErr != nil {
				return readErr
			}
		}
	}
	if err := consume(exact); !errors.Is(err, io.EOF) {
		t.Fatalf("exact cumulative budget rejected: %v", err)
	}
	if err := consume(exact - 1); !errors.Is(err, ErrProtocol) {
		t.Fatalf("outer+child+attribute overage accepted: %v", err)
	}
}

func TestQAHandleMessageCountsUnknownOuterAttributesInActualStanzaBudget(t *testing.T) {
	frame, err := EncodeStanzaFrame(Stanza{Kind: StanzaEnvelope, Data: []byte("opaque")}, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	start := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "message"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "b@example.test/mesh"},
		{Name: xml.Name{Local: "to"}, Value: "a@example.test/mesh"},
		{Name: xml.Name{Local: "type"}, Value: "chat"},
		{Name: xml.Name{Space: "urn:qa:unknown", Local: "padding"}, Value: strings.Repeat("x", 80)},
	}}
	message, err := stanza.NewMessage(start)
	if err != nil {
		t.Fatal(err)
	}
	baseline := xmlTokenSize(message.StartElement()) + xmlTokenSize(message.StartElement().End())
	decoder := xml.NewDecoder(bytes.NewReader(frame))
	for {
		token, tokenErr := decoder.Token()
		if tokenErr == io.EOF {
			break
		}
		if tokenErr != nil {
			t.Fatal(tokenErr)
		}
		baseline += xmlTokenSize(token)
	}
	if actual := xmlTokenSize(start) + xmlTokenSize(start.End()); actual <= xmlTokenSize(message.StartElement())+xmlTokenSize(message.StartElement().End()) {
		t.Fatal("QA fixture did not add unknown-attribute cost")
	}
	sm, _ := NewStreamManagement(2, 1<<20)
	_ = sm.Enable("resume", true)
	session := &melliumSession{
		config:   MelliumConfig{MaximumFrameBytes: 1024, StanzaBudgetBytes: baseline},
		username: "a@example.test", meshID: "mesh", management: sm, events: make(chan Event, 1),
	}
	tokens := &qaTokenReadEncoder{decoder: xml.NewDecoder(bytes.NewReader(frame)), encoder: xml.NewEncoder(io.Discard)}
	if err = session.handleElement(context.Background(), tokens, &start); !errors.Is(err, ErrProtocol) {
		t.Fatalf("unknown outer attributes escaped actual budget: %v", err)
	}
}

type qaBlockingDialer struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
	session Session
}

type qaLifecycleSession struct {
	mu       sync.Mutex
	done     chan struct{}
	once     sync.Once
	phases   []string
	resource string
}

var qaAuthenticatedServerTime = time.Date(2026, 8, 13, 12, 34, 56, 123456000, time.UTC)

type qaOutboundFailureSession struct {
	*qaLifecycleSession
	mu          sync.Mutex
	resumeCalls int
}

func (s *qaOutboundFailureSession) Send(context.Context, Stanza) error {
	return errors.New("dependency send failure with secret canary")
}
func (s *qaOutboundFailureSession) Resume(context.Context) (bool, error) {
	s.mu.Lock()
	s.resumeCalls++
	s.mu.Unlock()
	return false, ErrUnavailable
}
func (s *qaOutboundFailureSession) ResumeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resumeCalls
}

type qaIdentitySession struct {
	*qaLifecycleSession
	bare string
	full string
}

func (s *qaIdentitySession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return s.bare, []byte("opaque-proof"), nil
}
func (s *qaIdentitySession) BindResource(context.Context, string) (string, error) {
	return s.full, nil
}

func newQALifecycleSession() *qaLifecycleSession {
	return &qaLifecycleSession{done: make(chan struct{})}
}

func (s *qaLifecycleSession) record(phase string) {
	s.mu.Lock()
	s.phases = append(s.phases, phase)
	s.mu.Unlock()
}

func (s *qaLifecycleSession) ConnectTLS(context.Context, string) error {
	s.record("tls")
	return nil
}
func (s *qaLifecycleSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	s.record("auth")
	return "a@example.test", nil, nil
}
func (s *qaLifecycleSession) BindResource(_ context.Context, resource string) (string, error) {
	s.record("bind")
	if resource != "mesh" {
		return "", ErrIdentityBinding
	}
	s.mu.Lock()
	s.resource = resource
	s.mu.Unlock()
	return "a@example.test/mesh", nil
}
func (s *qaLifecycleSession) EnableStreamManagement(context.Context, bool) error {
	s.record("sm")
	return nil
}
func (s *qaLifecycleSession) QueryServerTime(context.Context) (time.Time, error) {
	s.record("time")
	return qaAuthenticatedServerTime, nil
}
func (s *qaLifecycleSession) Send(context.Context, Stanza) error { return nil }
func (s *qaLifecycleSession) Receive(ctx context.Context) (Event, error) {
	select {
	case <-s.done:
		return Event{}, ErrClosed
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (s *qaLifecycleSession) Resume(context.Context) (bool, error) { return false, ErrClosed }
func (s *qaLifecycleSession) CatchUp(context.Context, int) ([]Stanza, error) {
	return nil, nil
}
func (s *qaLifecycleSession) Close(context.Context) error {
	s.once.Do(func() { close(s.done) })
	return nil
}

func (s *qaLifecycleSession) establishment() ([]string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.phases...), s.resource
}

func (d *qaBlockingDialer) Dial(ctx context.Context) (Session, error) {
	d.mu.Lock()
	d.calls++
	if d.calls == 1 {
		close(d.entered)
	}
	d.mu.Unlock()
	select {
	case <-d.release:
		return d.session, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *qaBlockingDialer) Calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func qaUnstartedClient(t *testing.T, dialer Dialer) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint:                       "localhost:5222",
		Auth:                           Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity:                4,
		TransferWorkers:                1,
		TransferQueue:                  4,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     4,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   4,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, dialer, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestQAConcurrentClientStartIsSingleFlight(t *testing.T) {
	session := newQALifecycleSession()
	dialer := &qaBlockingDialer{entered: make(chan struct{}), release: make(chan struct{}), session: session}
	client := qaUnstartedClient(t, dialer)
	results := make(chan error, 2)
	go func() { results <- client.Start(context.Background()) }()
	<-dialer.entered
	go func() { results <- client.Start(context.Background()) }()
	time.Sleep(25 * time.Millisecond)
	if calls := dialer.Calls(); calls != 1 {
		close(dialer.release)
		<-results
		<-results
		cleanup := make(chan struct{})
		go func() {
			_ = client.Close(context.Background())
			close(cleanup)
		}()
		// A defective double start loses the first owned cancellation context.
		// Wake any transfer loop still selecting on it so that it re-observes
		// the current cancelled client context and QA itself does not hang.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-cleanup:
				t.Fatalf("concurrent Start created %d sessions", calls)
			case client.jobs[0] <- transferWork{}:
			case <-time.After(time.Millisecond):
			}
		}
		t.Fatalf("concurrent Start created %d sessions", calls)
	}
	close(dialer.release)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQAClientBindsRequestedResourceBeforeAuthenticatedTimeCalibration(t *testing.T) {
	session := newQALifecycleSession()
	client := qaUnstartedClient(t, fakeDialer{session})
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	phases, resource := session.establishment()
	want := []string{"tls", "auth", "bind", "sm", "time"}
	if !slices.Equal(phases, want) || resource != "mesh" {
		t.Fatalf("establishment phases=%v resource=%q, want %v resource mesh", phases, resource, want)
	}
	identity, err := client.AuthenticatedIdentity()
	if err != nil || identity.BareIdentity != "a@example.test" || identity.BoundIdentity != "a@example.test/mesh" || identity.MeshID != "mesh" {
		t.Fatalf("authenticated identity=%#v error=%v", identity, err)
	}
	snapshot, ok := client.TimeCalibration()
	if !ok || snapshot.UTC.IsZero() || snapshot.UTC.Location() != time.UTC || snapshot.Uncertainty <= 0 || snapshot.Uncertainty > transport.MaximumClockUncertainty {
		t.Fatalf("time calibration=%#v ready=%t", snapshot, ok)
	}
}

func TestQAClientCloseWinsAgainstInFlightStart(t *testing.T) {
	session := newQALifecycleSession()
	dialer := &qaBlockingDialer{entered: make(chan struct{}), release: make(chan struct{}), session: session}
	client := qaUnstartedClient(t, dialer)
	result := make(chan error, 1)
	go func() { result <- client.Start(context.Background()) }()
	<-dialer.entered
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(dialer.release)
	err := <-result
	if !errors.Is(err, ErrClosed) {
		// Ensure a defective late-start path cannot leave QA-owned goroutines.
		client.mu.Lock()
		cancel := client.cancel
		client.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		_ = session.Close(context.Background())
		client.wg.Wait()
		t.Fatalf("Start succeeded after Close linearized: %v", err)
	}
}

func TestQAAuthenticationRejectsBareAndBoundIdentityMismatch(t *testing.T) {
	tests := []struct {
		name string
		bare string
		full string
	}{
		{name: "different authenticated bare identity", bare: "other@example.test", full: "other@example.test/mesh"},
		{name: "different bound resource", bare: "a@example.test", full: "a@example.test/other-mesh"},
		{name: "different bound bare identity", bare: "a@example.test", full: "other@example.test/mesh"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &qaIdentitySession{qaLifecycleSession: newQALifecycleSession(), bare: test.bare, full: test.full}
			client := qaUnstartedClient(t, fakeDialer{session})
			if err := client.Start(context.Background()); !errors.Is(err, ErrIdentityBinding) {
				t.Fatalf("identity mismatch error = %v", err)
			}
			select {
			case <-session.done:
			default:
				t.Fatal("rejected authentication session was not closed")
			}
		})
	}
}

func TestQAOutboundDurableFailureTriggersReconnectDemand(t *testing.T) {
	session := &qaOutboundFailureSession{qaLifecycleSession: newQALifecycleSession()}
	client := qaUnstartedClient(t, fakeDialer{session})
	if err := startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	err := client.Send(context.Background(), durableEnvelope(t))
	if err == nil {
		t.Fatal("outbound dependency failure reported success")
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for session.ResumeCalls() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls := session.ResumeCalls(); calls == 0 {
		t.Fatal("outbound failure did not trigger bounded reconnect")
	}
}

type qaReconnectStage uint8

const (
	qaStageNone qaReconnectStage = iota
	qaStageResume
	qaStageConnect
	qaStageTime
	qaStageReplay
	qaStageCatchUp
	qaStageClose
)

type qaDeadlineSession struct {
	block   qaReconnectStage
	pending []Stanza
	mu      sync.Mutex
	calls   map[qaReconnectStage]int
	bounded map[qaReconnectStage]int
}

func newQADeadlineSession(block qaReconnectStage) *qaDeadlineSession {
	return &qaDeadlineSession{block: block, calls: make(map[qaReconnectStage]int), bounded: make(map[qaReconnectStage]int)}
}

func (s *qaDeadlineSession) operation(ctx context.Context, stage qaReconnectStage) error {
	if s.block != stage {
		return nil
	}
	s.mu.Lock()
	s.calls[stage]++
	if _, ok := ctx.Deadline(); ok {
		s.bounded[stage]++
	}
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (s *qaDeadlineSession) count(stage qaReconnectStage) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[stage], s.bounded[stage]
}

func (s *qaDeadlineSession) ConnectTLS(ctx context.Context, _ string) error {
	return s.operation(ctx, qaStageConnect)
}
func (*qaDeadlineSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return "a@example.test", nil, nil
}
func (*qaDeadlineSession) BindResource(context.Context, string) (string, error) {
	return "a@example.test/mesh", nil
}
func (*qaDeadlineSession) EnableStreamManagement(context.Context, bool) error { return nil }
func (s *qaDeadlineSession) QueryServerTime(ctx context.Context) (time.Time, error) {
	if err := s.operation(ctx, qaStageTime); err != nil {
		return time.Time{}, err
	}
	return qaAuthenticatedServerTime, nil
}
func (s *qaDeadlineSession) Send(ctx context.Context, _ Stanza) error {
	return s.operation(ctx, qaStageReplay)
}
func (*qaDeadlineSession) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}
func (s *qaDeadlineSession) Resume(ctx context.Context) (bool, error) {
	if err := s.operation(ctx, qaStageResume); err != nil {
		return false, err
	}
	return false, ErrUnavailable
}
func (s *qaDeadlineSession) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneStanzas(s.pending), nil
}
func (s *qaDeadlineSession) CatchUp(ctx context.Context, _ int) ([]Stanza, error) {
	if err := s.operation(ctx, qaStageCatchUp); err != nil {
		return nil, err
	}
	return nil, nil
}
func (s *qaDeadlineSession) Close(ctx context.Context) error {
	return s.operation(ctx, qaStageClose)
}

type qaSequenceDialer struct {
	mu       sync.Mutex
	sessions []Session
	calls    int
}

func (d *qaSequenceDialer) Dial(context.Context) (Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if len(d.sessions) == 0 {
		return nil, ErrUnavailable
	}
	session := d.sessions[0]
	d.sessions = d.sessions[1:]
	return session, nil
}

func (d *qaSequenceDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type qaRetrySnapshotSession struct {
	*qaDeadlineSession
	mu            sync.Mutex
	snapshotCalls int
	bounded       int
}

func (s *qaRetrySnapshotSession) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	s.mu.Lock()
	s.snapshotCalls++
	call := s.snapshotCalls
	if _, ok := ctx.Deadline(); ok {
		s.bounded++
	}
	s.mu.Unlock()
	if call == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return []Stanza{{Kind: StanzaSignalResult, AttemptID: "authoritative-q"}}, nil
}

func (s *qaRetrySnapshotSession) snapshotCount() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotCalls, s.bounded
}

func qaReconnectClient(t *testing.T, old Session, dialer Dialer) *Client {
	t.Helper()
	client := qaUnstartedClient(t, dialer)
	client.config.ReconnectOperationTimeout = 50 * time.Millisecond
	client.ctx = context.Background()
	client.session = old
	client.identity = Authenticated{BareIdentity: "a@example.test", BoundIdentity: "a@example.test/mesh"}
	client.started = true
	client.state = DurablePending
	return client
}

func qaDemandReconnect(t *testing.T, client *Client) bool {
	t.Helper()
	client.requestReconnect()
	select {
	case <-client.reconnectDemand:
		return client.reconnect()
	case <-time.After(time.Second):
		t.Fatal("reconnect demand was not admitted")
		return false
	}
}

func TestQAReconnectOperationsHaveIndependentDeadlinesAndPermitLaterDemand(t *testing.T) {
	t.Run("resume", func(t *testing.T) {
		old := newQADeadlineSession(qaStageResume)
		client := qaReconnectClient(t, old, &qaSequenceDialer{})
		if qaDemandReconnect(t, client) || qaDemandReconnect(t, client) {
			t.Fatal("blocked resume unexpectedly recovered")
		}
		if calls, bounded := old.count(qaStageResume); calls != 2 || bounded != calls {
			t.Fatalf("resume calls=%d bounded=%d", calls, bounded)
		}
	})

	t.Run("connect", func(t *testing.T) {
		old := newQADeadlineSession(qaStageNone)
		first, second := newQADeadlineSession(qaStageConnect), newQADeadlineSession(qaStageConnect)
		client := qaReconnectClient(t, old, &qaSequenceDialer{sessions: []Session{first, second}})
		if qaDemandReconnect(t, client) || qaDemandReconnect(t, client) {
			t.Fatal("blocked connect unexpectedly recovered")
		}
		for i, session := range []*qaDeadlineSession{first, second} {
			if calls, bounded := session.count(qaStageConnect); calls != 1 || bounded != 1 {
				t.Fatalf("connect session %d calls=%d bounded=%d", i, calls, bounded)
			}
		}
	})

	t.Run("time calibration", func(t *testing.T) {
		old := newQADeadlineSession(qaStageNone)
		first, second := newQADeadlineSession(qaStageTime), newQADeadlineSession(qaStageTime)
		client := qaReconnectClient(t, old, &qaSequenceDialer{sessions: []Session{first, second}})
		if qaDemandReconnect(t, client) || qaDemandReconnect(t, client) {
			t.Fatal("blocked authenticated time query unexpectedly recovered")
		}
		for i, session := range []*qaDeadlineSession{first, second} {
			if calls, bounded := session.count(qaStageTime); calls != 1 || bounded != 1 {
				t.Fatalf("time session %d calls=%d bounded=%d", i, calls, bounded)
			}
		}
	})

	t.Run("catchup", func(t *testing.T) {
		old := newQADeadlineSession(qaStageNone)
		first, second := newQADeadlineSession(qaStageCatchUp), newQADeadlineSession(qaStageCatchUp)
		client := qaReconnectClient(t, old, &qaSequenceDialer{sessions: []Session{first, second}})
		if qaDemandReconnect(t, client) || qaDemandReconnect(t, client) {
			t.Fatal("blocked catchup unexpectedly recovered")
		}
		for i, session := range []*qaDeadlineSession{first, second} {
			if calls, bounded := session.count(qaStageCatchUp); calls != 1 || bounded != 1 {
				t.Fatalf("catchup session %d calls=%d bounded=%d", i, calls, bounded)
			}
		}
	})

	t.Run("close", func(t *testing.T) {
		old := newQADeadlineSession(qaStageClose)
		first, second := newQADeadlineSession(qaStageClose), newQADeadlineSession(qaStageNone)
		dialer := &qaSequenceDialer{sessions: []Session{first, second}}
		client := qaReconnectClient(t, old, dialer)
		if !qaDemandReconnect(t, client) {
			t.Fatal("bounded old-session close prevented the first successful reconnect")
		}
		if !qaDemandReconnect(t, client) {
			oldCalls, oldBounded := old.count(qaStageClose)
			firstCalls, firstBounded := first.count(qaStageClose)
			client.mu.Lock()
			current, state := client.session, client.state
			client.mu.Unlock()
			t.Fatalf("bounded replacement close prevented the later reconnect demand: dials=%d old-close=%d/%d first-close=%d/%d current-first=%t current-second=%t state=%d", dialer.count(), oldCalls, oldBounded, firstCalls, firstBounded, current == first, current == second, state)
		}
		for i, session := range []*qaDeadlineSession{old, first} {
			if calls, bounded := session.count(qaStageClose); calls != 1 || bounded != 1 {
				t.Fatalf("close session %d calls=%d bounded=%d", i, calls, bounded)
			}
		}
	})
}

func TestQAPendingReplayTimeoutPreventsSwapAndLaterDemandRetries(t *testing.T) {
	old := &qaRetrySnapshotSession{qaDeadlineSession: newQADeadlineSession(qaStageNone)}
	newSession := newQADeadlineSession(qaStageNone)
	dialer := &qaSequenceDialer{sessions: []Session{newSession}}
	client := qaReconnectClient(t, old, dialer)
	if qaDemandReconnect(t, client) {
		t.Fatal("reconnect swapped without an authoritative pending snapshot")
	}
	client.mu.Lock()
	current := client.session
	client.mu.Unlock()
	if current != old || dialer.count() != 0 {
		t.Fatalf("snapshot timeout changed session=%v dial calls=%d", current != old, dialer.count())
	}
	if !qaDemandReconnect(t, client) {
		t.Fatal("later reconnect demand did not retry the pending snapshot")
	}
	client.mu.Lock()
	current = client.session
	client.mu.Unlock()
	if current != newSession || dialer.count() != 1 {
		t.Fatalf("later retry did not swap session=%v dial calls=%d", current != newSession, dialer.count())
	}
	if calls, bounded := old.snapshotCount(); calls != 2 || bounded != calls {
		t.Fatalf("snapshot calls=%d bounded=%d", calls, bounded)
	}
	if calls, bounded := newSession.count(qaStageReplay); calls != 0 || bounded != 0 {
		t.Fatalf("nonblocking replay counters=%d/%d", calls, bounded)
	}
}

func TestQAXMPPCompletionMustBeBoundToExpectedPeer(t *testing.T) {
	client, _ := qaXMPPClient(t)
	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{
		MaximumFrameBytes: 1024,
		MaximumChunkBytes: 256,
		StanzaBudgetBytes: 900,
		XMLOverheadBytes:  128,
		InFlightChunks:    1,
		MaximumTransfers:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := qaRoute()
	transferID := typed("xfer_", 0x32)
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: []byte("bWFuaWZlc3Q")}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, finishErr := carrier.Finish(ctx, route, transferID)
		result <- finishErr
	}()
	digest := sha256.Sum256([]byte("payload"))
	forged := Stanza{
		Kind:       StanzaTransferCompletion,
		From:       "evil@example.test/mesh",
		To:         route.SenderID,
		MeshID:     route.MeshID,
		TransferID: transferID,
		MessageID:  route.MessageID,

		Evidence: payload.CompletionEvidence{TransferID: transferID, MessageID: route.MessageID, Digest: digest},
	}
	client.handleEvent(Event{Kind: EventStanza, Stanza: forged})
	select {
	case err := <-result:
		t.Fatalf("wrong-peer completion terminated transfer: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	legitimate := forged
	legitimate.From = route.PeerID
	client.handleEvent(Event{Kind: EventStanza, Stanza: legitimate})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("legitimate completion did not terminate transfer")
	}
}

func TestQAXMPPChunkCarrierRejectsNonCanonicalTextFrame(t *testing.T) {
	client, _ := qaXMPPClient(t)
	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{
		MaximumFrameBytes: 1024,
		MaximumChunkBytes: 256,
		StanzaBudgetBytes: 900,
		XMLOverheadBytes:  128,
		InFlightChunks:    1,
		MaximumTransfers:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = carrier.Begin(context.Background(), qaRoute(), payload.CarrierFrame{
		TransferID: typed("xfer_", 0x33),
		Encoding:   payload.FrameText,
		Data:       []byte("not canonical base64 ***"),
	})
	if err == nil {
		t.Fatal("non-canonical text frame accepted")
	}
}

func TestQAXMPPChunkFinishCancellationReleasesTransferCapacity(t *testing.T) {
	client, _ := qaXMPPClient(t)
	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{
		MaximumFrameBytes: 1024,
		MaximumChunkBytes: 256,
		StanzaBudgetBytes: 900,
		XMLOverheadBytes:  128,
		InFlightChunks:    1,
		MaximumTransfers:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := qaRoute()
	first := typed("xfer_", 0x34)
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: first, Encoding: payload.FrameText, Data: []byte(base64.RawStdEncoding.EncodeToString([]byte("manifest")))}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := carrier.Finish(ctx, route, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finish cancellation error = %v", err)
	}
	second := typed("xfer_", 0x35)
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: second, Encoding: payload.FrameText, Data: []byte(base64.RawStdEncoding.EncodeToString([]byte("manifest")))}); err != nil {
		t.Fatalf("cancelled transfer retained capacity: %v", err)
	}
	_ = carrier.Abort(context.Background(), route, second)
}

func TestQAXMPPChunkAbortUnblocksConcurrentFinish(t *testing.T) {
	client, session := qaXMPPClient(t)
	carrier, err := NewPayloadChunks(client, PayloadChunksConfig{
		MaximumFrameBytes: 1024,
		MaximumChunkBytes: 256,
		StanzaBudgetBytes: 900,
		XMLOverheadBytes:  128,
		InFlightChunks:    1,
		MaximumTransfers:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := qaRoute()
	transferID := typed("xfer_", 0x36)
	manifest := []byte(base64.RawStdEncoding.EncodeToString([]byte("manifest")))
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameText, Data: manifest}); err != nil {
		t.Fatal(err)
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finishResult := make(chan error, 1)
	go func() {
		_, finishErr := carrier.Finish(finishCtx, route, transferID)
		finishResult <- finishErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		session.mu.Lock()
		sawFinish := false
		for _, stanza := range session.sent {
			if stanza.Kind == StanzaTransferFinish && stanza.TransferID == transferID {
				sawFinish = true
				break
			}
		}
		session.mu.Unlock()
		if sawFinish {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Finish stanza was not sent")
		}
		time.Sleep(time.Millisecond)
	}
	if err := carrier.Abort(context.Background(), route, transferID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finishResult:
	case <-time.After(50 * time.Millisecond):
		cancel()
		<-finishResult
		t.Fatal("Abort did not unblock concurrent Finish")
	}
}

func TestQAAmbiguousTimeCalibrationFencesResumeAndCleanLedger(t *testing.T) {
	sm, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("qa-resume", true); err != nil {
		t.Fatal(err)
	}
	for _, stanza := range []Stanza{
		{Kind: StanzaEnvelope, Ordinal: 81},
		{Kind: StanzaTimeCalibration, MessageID: "time-ambiguous"},
		{Kind: StanzaSignal},
		{Kind: StanzaEnvelope, Ordinal: 82},
	} {
		if _, err := sm.RecordSentTracked(stanza); err != nil {
			t.Fatal(err)
		}
	}
	beforeID, beforeHandled, beforeResumable := sm.ResumeState()
	if fenced, err := sm.pendingKindAfterAck(1, StanzaTimeCalibration); err != nil || !fenced {
		t.Fatalf("pending calibration fence=%t error=%v", fenced, err)
	}
	afterID, afterHandled, afterResumable := sm.ResumeState()
	if beforeID != afterID || beforeHandled != afterHandled || beforeResumable != afterResumable {
		t.Fatalf("fence preview mutated resume state: before=(%q,%d,%t) after=(%q,%d,%t)", beforeID, beforeHandled, beforeResumable, afterID, afterHandled, afterResumable)
	}

	replay, err := applicationReplay(sm.ResumeRejected())
	if err != nil {
		t.Fatal(err)
	}
	want := []Stanza{
		{Kind: StanzaEnvelope, Ordinal: 81},
		{Kind: StanzaSignal},
		{Kind: StanzaEnvelope, Ordinal: 82},
	}
	if len(replay) != len(want) {
		t.Fatalf("clean-session replay=%#v want=%#v", replay, want)
	}
	for index := range want {
		if replay[index].Kind != want[index].Kind || replay[index].Ordinal != want[index].Ordinal {
			t.Fatalf("clean-session replay[%d]=%#v want=%#v", index, replay[index], want[index])
		}
	}
	if _, _, resumable := sm.ResumeState(); resumable || sm.Pending() != 0 {
		t.Fatalf("rejected state remained resumable=%t pending=%d", resumable, sm.Pending())
	}

	clean, err := NewStreamManagement(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := clean.Enable("qa-clean", true); err != nil {
		t.Fatal(err)
	}
	for _, stanza := range replay {
		if _, err := clean.RecordSentTracked(stanza); err != nil {
			t.Fatal(err)
		}
	}
	ordinal, count, err := clean.ApplyAckDetailed(3)
	if err != nil || ordinal != 82 || count != 3 || clean.Pending() != 0 {
		t.Fatalf("clean acknowledgement ordinal=%d count=%d pending=%d error=%v", ordinal, count, clean.Pending(), err)
	}
}

func TestQAAcknowledgedTimeCalibrationDoesNotFenceResume(t *testing.T) {
	sm, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Enable("qa-resume", true); err != nil {
		t.Fatal(err)
	}
	for _, stanza := range []Stanza{
		{Kind: StanzaEnvelope, Ordinal: 91},
		{Kind: StanzaTimeCalibration, MessageID: "time-confirmed"},
	} {
		if _, err := sm.RecordSentTracked(stanza); err != nil {
			t.Fatal(err)
		}
	}
	if fenced, err := sm.pendingKindAfterAck(2, StanzaTimeCalibration); err != nil || fenced {
		t.Fatalf("acknowledged calibration fence=%t error=%v", fenced, err)
	}
	ordinal, count, err := sm.ApplyAckDetailed(2)
	if err != nil || ordinal != 91 || count != 2 || sm.Pending() != 0 {
		t.Fatalf("acknowledgement ordinal=%d count=%d pending=%d error=%v", ordinal, count, sm.Pending(), err)
	}
	if _, _, resumable := sm.ResumeState(); !resumable || sm.Pending() != 0 {
		t.Fatalf("confirmed calibration disabled resume: resumable=%t pending=%d", resumable, sm.Pending())
	}
}
