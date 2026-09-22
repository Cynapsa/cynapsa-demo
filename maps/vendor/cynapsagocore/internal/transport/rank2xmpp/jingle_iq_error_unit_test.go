package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	jingleErrorLocal   = "native-server@mesh.test/simple-e2e"
	jingleErrorServer  = "mesh.test"
	jingleErrorAttempt = "hsk_cHCKzFl7mdgipOvKT1tPSw"
)

func jingleErrorTestSession(t *testing.T) (*melliumSession, context.Context) {
	t.Helper()
	management, err := NewStreamManagement(16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("jingle-error-resume", true); err != nil {
		t.Fatal(err)
	}
	session := newMelliumSession(MelliumConfig{
		ReceiveCapacity: 16, StanzaBudgetBytes: 1 << 20,
	}, Endpoint{})
	session.username = "native-server@mesh.test"
	session.meshID = "simple-e2e"
	session.management = management
	session.generation = 7
	ctx := context.WithValue(context.Background(), melliumSessionGenerationKey{}, uint64(7))
	return session, ctx
}

func handleJingleErrorTestXML(session *melliumSession, ctx context.Context, document string) (string, error) {
	decoder := xml.NewDecoder(strings.NewReader(document))
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		return "", ErrProtocol
	}
	var output bytes.Buffer
	encoder := xml.NewEncoder(&output)
	tokens := &qaTokenReadEncoder{decoder: decoder, encoder: encoder}
	err = session.handleElement(ctx, tokens, &start)
	if flushErr := encoder.Flush(); err == nil && flushErr != nil {
		err = flushErr
	}
	return output.String(), err
}

func canonicalJingleIQError(attemptID, from, to, body string) string {
	return `<iq xmlns="jabber:client" xml:lang="en" to="` + to + `" from="` + from + `" type="error" id="` + attemptID + `">` + body + `</iq>`
}

func canonicalJingleIQErrorBody() string {
	return `<error type="modify"><policy-violation xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><text xml:lang="en" xmlns="urn:ietf:params:xml:ns:xmpp-stanzas">Cynapsa mesh routing policy rejected the stanza</text></error>`
}

func issueJingleForTest(session *melliumSession, attemptID string) {
	session.issuedJingleCapacity = 16
	session.issuedJingles[attemptID] = issuedJingle{
		from:  jingleErrorLocal,
		to:    "native-sync-client@mesh.test/simple-e2e",
		state: issuedJingleActive,
	}
	session.issuedJingleActive++
}

func TestJingleIQErrorFailsExactExchangeAndLeavesSessionUsable(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	issueJingleForTest(session, jingleErrorAttempt)
	waiter := &jingleWaiter{
		recipient: "native-sync-client@mesh.test/simple-e2e",
		result:    make(chan jingleResult, 1),
	}
	client := &Client{
		config: Config{Auth: Authentication{MeshID: "simple-e2e"}},
		jingle: map[string]*jingleWaiter{jingleErrorAttempt: waiter},
	}

	wire := canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody())
	probe := xml.NewDecoder(strings.NewReader(wire))
	token, err := probe.Token()
	outer := token.(xml.StartElement)
	iq, iqErr := stanza.NewIQ(outer)
	server, _ := jid.Parse(jingleErrorServer)
	local, _ := jid.Parse(jingleErrorLocal)
	if iqErr != nil || iq.Type != stanza.ErrorIQ {
		t.Fatalf("IQ parse=%#v err=%v", iq, iqErr)
	}
	if kind, ok := validCorrelatedIQAttrs(outer.Attr, jingleErrorAttempt, server, local); !ok || kind != stanza.ErrorIQ {
		t.Fatalf("IQ attributes=%#v kind=%q ok=%v", outer.Attr, kind, ok)
	}
	if err = decodeJingleIQError(probe, outer); err != nil {
		t.Fatalf("IQ error body: %v", err)
	}
	if output, err := handleJingleErrorTestXML(session, ctx, wire); err != nil || output != "" {
		t.Fatalf("canonical error: output=%q err=%v", output, err)
	}
	event := <-session.events
	if event.Kind != EventJingleFailure || event.AttemptID != jingleErrorAttempt {
		t.Fatalf("failure event=%#v", event)
	}
	if !client.failJingleExchange(event.AttemptID) {
		t.Fatal("exact active exchange was not claimed")
	}
	clearEventOwned(&event)
	select {
	case result := <-waiter.result:
		if !errors.Is(result.err, ErrUnavailable) {
			t.Fatalf("exchange result=%v", result.err)
		}
	default:
		t.Fatal("exchange failure was not delivered")
	}
	if got := session.management.HandledInbound(); got != 1 {
		t.Fatalf("handled after Jingle error=%d want=1", got)
	}
	if session.suspended {
		t.Fatal("valid Jingle failure suspended the shared XMPP session")
	}

	ping := `<iq xmlns="jabber:client" from="mesh.test" to="` + jingleErrorLocal + `" type="get" id="ping-after-jingle-error"><ping xmlns="urn:xmpp:ping"/></iq>`
	output, err := handleJingleErrorTestXML(session, ctx, ping)
	if err != nil {
		t.Fatalf("post-error server ping: %v", err)
	}
	if !strings.Contains(output, `type="result"`) || !strings.Contains(output, `id="ping-after-jingle-error"`) || !strings.Contains(output, streamManagementNamespace) {
		t.Fatalf("post-error ping output=%q", output)
	}
	if got := session.management.HandledInbound(); got != 2 {
		t.Fatalf("handled after post-error ping=%d want=2", got)
	}
}

func TestUnknownJingleIQErrorIsFatalWithoutAdvancingStream(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	wire := canonicalJingleIQError("hsk_unknown", jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody())
	if _, err := handleJingleErrorTestXML(session, ctx, wire); err == nil {
		t.Fatal("unknown correlated ID was accepted")
	}
	select {
	case event := <-session.events:
		clearEventOwned(&event)
		t.Fatalf("unknown ID emitted event=%#v", event)
	default:
	}
	if got := session.management.HandledInbound(); got != 0 {
		t.Fatalf("unknown error advanced handled count to %d", got)
	}
}

func TestLateIssuedJingleIQErrorIsConsumedWithoutActiveWaiter(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	issueJingleForTest(session, jingleErrorAttempt)
	wire := canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody())
	if _, err := handleJingleErrorTestXML(session, ctx, wire); err != nil {
		t.Fatalf("late issued error=%v", err)
	}
	event := <-session.events
	client := &Client{jingle: make(map[string]*jingleWaiter)}
	if client.failJingleExchange(event.AttemptID) {
		t.Fatal("late error claimed a nonexistent waiter")
	}
	clearEventOwned(&event)
	if session.management.HandledInbound() != 1 || session.suspended {
		t.Fatalf("late error handled=%d suspended=%t", session.management.HandledInbound(), session.suspended)
	}
	issued, exists := session.issuedJingles[jingleErrorAttempt]
	if !exists || issued.state != issuedJingleTombstone || !issued.responded || session.issuedJingleActive != 0 {
		t.Fatalf("late error correlation=%#v exists=%t active=%d", issued, exists, session.issuedJingleActive)
	}
}

func TestExactJingleIQResultLeavesBoundedIdempotentTombstone(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	issueJingleForTest(session, jingleErrorAttempt)
	wire := `<iq xmlns="jabber:client" from="native-sync-client@mesh.test/simple-e2e" to="` + jingleErrorLocal + `" type="result" id="` + jingleErrorAttempt + `"></iq>`
	if _, err := handleJingleErrorTestXML(session, ctx, wire); err != nil {
		t.Fatalf("exact result=%v", err)
	}
	if session.management.HandledInbound() != 1 {
		t.Fatalf("result handled=%d", session.management.HandledInbound())
	}
	issued, exists := session.issuedJingles[jingleErrorAttempt]
	if !exists || issued.state != issuedJingleTombstone || !issued.responded || session.issuedJingleActive != 0 {
		t.Fatalf("result correlation=%#v exists=%t active=%d", issued, exists, session.issuedJingleActive)
	}
	if _, err := handleJingleErrorTestXML(session, ctx, wire); err != nil {
		t.Fatalf("exact duplicate result was not idempotent: %v", err)
	}
	if session.management.HandledInbound() != 2 {
		t.Fatalf("duplicate result handled=%d", session.management.HandledInbound())
	}
}

func TestIssuedJingleCanceledAttemptsDoNotExhaustCapacityAcrossGenerations(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	session.issuedJingleCapacity = 2
	from := jingleErrorLocal
	to := "native-sync-client@mesh.test/simple-e2e"
	var latest string
	for generation := uint64(1); generation <= 64; generation++ {
		latest = "hsk_cancelled_" + strconv.FormatUint(generation, 10)
		record := Stanza{Kind: StanzaSignal, AttemptID: latest, MeshID: "simple-e2e", From: from, To: to}
		if err := session.registerIssuedJingle(record, true); err != nil {
			t.Fatalf("generation %d register: %v", generation, err)
		}
		// This is the lifecycle transition used after success, cancellation, or
		// ambiguous publication failure. It must survive a fresh auth generation.
		if !session.quiesceIssuedJingle(latest, from, to) {
			t.Fatalf("generation %d did not quiesce", generation)
		}
		session.generation = generation
		if err := session.registerIssuedJingle(record, false); err != nil {
			t.Fatalf("generation %d replay: %v", generation, err)
		}
		if session.issuedJingleActive != 0 || len(session.issuedJingles) > session.issuedJingleCapacity || len(session.issuedJingleTombstones) > session.issuedJingleCapacity {
			t.Fatalf("generation %d active=%d ledger=%d tombstones=%d", generation, session.issuedJingleActive, len(session.issuedJingles), len(session.issuedJingleTombstones))
		}
	}

	session.generation = 64
	ctx = context.WithValue(ctx, melliumSessionGenerationKey{}, uint64(64))
	wire := canonicalJingleIQError(latest, jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody())
	if _, err := handleJingleErrorTestXML(session, ctx, wire); err != nil {
		t.Fatalf("exact delayed error after fresh auth: %v", err)
	}
	select {
	case event := <-session.events:
		if event.Kind != EventJingleFailure || event.AttemptID != latest {
			t.Fatalf("delayed event=%#v", event)
		}
		clearEventOwned(&event)
	default:
		t.Fatal("exact delayed error did not emit failure")
	}

	evicted := "hsk_cancelled_1"
	if _, err := handleJingleErrorTestXML(session, ctx, canonicalJingleIQError(evicted, jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody())); err == nil {
		t.Fatal("evicted unknown ID was accepted")
	}
}

func TestIssuedJingleAndFailureCorrelationAcceptRuntimeV2Resources(t *testing.T) {
	const mesh = "mesh"
	from := "a@example.test/r2.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAA"
	to := "b@example.test/r2.11234567-89ab-4def-8123-456789abcdef.BBBBBBBBBBBBBBBB"
	session, _ := jingleErrorTestSession(t)
	session.issuedJingleCapacity = 2
	record := Stanza{Kind: StanzaSignal, AttemptID: "hsk_runtime_v2", MeshID: mesh, From: from, To: to}
	if err := session.registerIssuedJingle(record, true); err != nil {
		t.Fatalf("register r2 Jingle: %v", err)
	}
	if !session.quiesceIssuedJingle(record.AttemptID, from, to) {
		t.Fatal("r2 Jingle correlation did not quiesce")
	}
	client := &Client{
		config: Config{Auth: Authentication{MeshID: mesh}},
		jingle: map[string]*jingleWaiter{
			record.AttemptID: {recipient: to, result: make(chan jingleResult, 1)},
		},
	}
	if !client.failJingleExchange(record.AttemptID) {
		t.Fatal("r2 Jingle failure did not claim exact waiter")
	}
}

func TestIssuedJingleSameSIDSameRouteSupportsSequentialActionsAcrossGenerations(t *testing.T) {
	session, _ := jingleErrorTestSession(t)
	session.issuedJingleCapacity = 2
	from := jingleErrorLocal
	to := "native-sync-client@mesh.test/simple-e2e"
	record := Stanza{
		Kind: StanzaSignal, AttemptID: jingleErrorAttempt,
		MeshID: "simple-e2e", From: from, To: to,
	}

	// Model session-accept completing its wire publication under the session
	// SID, followed by a later transport action reusing that same SID.
	if err := session.registerIssuedJingle(record, true); err != nil {
		t.Fatalf("session-accept register: %v", err)
	}
	if !session.quiesceIssuedJingle(record.AttemptID, from, to) {
		t.Fatal("session-accept did not quiesce")
	}
	issued, ok := session.lookupIssuedJingle(record.AttemptID)
	if !ok {
		t.Fatal("session-accept correlation was not retained")
	}
	if first, completed := session.completeIssuedJingle(record.AttemptID, issued); !first || !completed {
		t.Fatalf("session-accept completion first=%t completed=%t", first, completed)
	}

	session.generation++
	if err := session.registerIssuedJingle(record, true); err != nil {
		t.Fatalf("same-route transport action register: %v", err)
	}
	session.mu.Lock()
	reactivated := session.issuedJingles[record.AttemptID]
	active := session.issuedJingleActive
	tombstones := append([]string(nil), session.issuedJingleTombstones...)
	session.mu.Unlock()
	if reactivated.state != issuedJingleActive || reactivated.responded || active != 1 || len(tombstones) != 0 {
		t.Fatalf("reactivated=%#v active=%d tombstones=%v", reactivated, active, tombstones)
	}

	changedRoute := record
	changedRoute.To = "other@mesh.test/simple-e2e"
	if err := session.registerIssuedJingle(changedRoute, true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("same SID route mutation=%v", err)
	}
	if !session.quiesceIssuedJingle(record.AttemptID, from, to) {
		t.Fatal("transport action did not quiesce")
	}
	if err := session.registerIssuedJingle(record, false); err != nil {
		t.Fatalf("same-route XEP-0198 replay: %v", err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.issuedJingleActive != 0 || len(session.issuedJingleTombstones) != 1 || session.issuedJingleTombstones[0] != record.AttemptID {
		t.Fatalf("active=%d tombstones=%v", session.issuedJingleActive, session.issuedJingleTombstones)
	}
}

func TestIssuedJingleConcurrentCancellationIsBoundedAndRaceSafe(t *testing.T) {
	session, _ := jingleErrorTestSession(t)
	session.issuedJingleCapacity = 64
	from := jingleErrorLocal
	to := "native-sync-client@mesh.test/simple-e2e"
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			id := "hsk_concurrent_" + strconv.Itoa(index)
			record := Stanza{Kind: StanzaSignal, AttemptID: id, MeshID: "simple-e2e", From: from, To: to}
			if err := session.registerIssuedJingle(record, true); err != nil {
				t.Errorf("register %s: %v", id, err)
				return
			}
			if !session.quiesceIssuedJingle(id, from, to) {
				t.Errorf("quiesce %s", id)
			}
		}()
	}
	wait.Wait()
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.issuedJingleActive != 0 || len(session.issuedJingles) > session.issuedJingleCapacity || len(session.issuedJingleTombstones) > session.issuedJingleCapacity {
		t.Fatalf("active=%d ledger=%d tombstones=%d", session.issuedJingleActive, len(session.issuedJingles), len(session.issuedJingleTombstones))
	}
}

func TestJingleIQErrorRejectsForgedRoutesAndMalformedBodies(t *testing.T) {
	validBody := canonicalJingleIQErrorBody()
	tests := []struct {
		name string
		wire string
	}{
		{name: "peer-origin", wire: canonicalJingleIQError(jingleErrorAttempt, "mallory@mesh.test/simple-e2e", jingleErrorLocal, validBody)},
		{name: "other-domain", wire: canonicalJingleIQError(jingleErrorAttempt, "other.test", jingleErrorLocal, validBody)},
		{name: "wrong-resource", wire: canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, "native-server@mesh.test/other", validBody)},
		{name: "missing-condition", wire: canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, `<error type="modify"/>`)},
		{name: "unknown-condition", wire: canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, `<error type="modify"><made-up xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error>`)},
		{name: "extra-child", wire: canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, `<error type="modify"><policy-violation xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><extra/></error>`)},
		{name: "oversized", wire: canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, `<error type="modify"><policy-violation xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><text xmlns="urn:ietf:params:xml:ns:xmpp-stanzas">`+strings.Repeat("x", maximumJingleIQErrorText+1)+`</text></error>`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, ctx := jingleErrorTestSession(t)
			if _, err := handleJingleErrorTestXML(session, ctx, test.wire); err == nil {
				t.Fatal("invalid Jingle error was accepted")
			}
			select {
			case event := <-session.events:
				clearEventOwned(&event)
				t.Fatalf("invalid Jingle error emitted event=%#v", event)
			default:
			}
			if got := session.management.HandledInbound(); got != 0 {
				t.Fatalf("invalid Jingle error advanced handled count to %d", got)
			}
		})
	}
}

func TestJingleIQErrorDecoderRequiresCompleteOuterElement(t *testing.T) {
	session, ctx := jingleErrorTestSession(t)
	wire := strings.TrimSuffix(canonicalJingleIQError(jingleErrorAttempt, jingleErrorServer, jingleErrorLocal, canonicalJingleIQErrorBody()), `</iq>`)
	_, err := handleJingleErrorTestXML(session, ctx, wire)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("truncated outer error=%v", err)
	}
}
