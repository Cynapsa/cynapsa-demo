package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/xep0363"
	"mellium.im/xmpp/jid"
)

type melliumResponseReader struct {
	source xml.TokenReader
}

func (reader *melliumResponseReader) Token() (xml.Token, error) {
	token, err := reader.source.Token()
	if end, ok := token.(xml.EndElement); ok && end.Name.Local == "iq" {
		return nil, io.EOF
	}
	return token, err
}

func TestUploadSlotStrictCorrelatedDecode(t *testing.T) {
	from, _ := jid.Parse("upload.mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	response := `<iq xmlns='jabber:client' xml:lang='en' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://objects.example.test/put'><header name='Authorization'>Bearer token</header><header name='Cookie'>a=b</header></put><get url='https://objects.example.test/get'></get></slot></iq>`
	slot, correlated, handled, err := decodeCorrelatedUploadSlot(xml.NewDecoder(strings.NewReader(response)), "slot-id", from, to)
	if err != nil || !correlated || !handled {
		t.Fatalf("decode = (%#v,%v,%v,%v)", slot, correlated, handled, err)
	}
	if slot.PutURL != "https://objects.example.test/put" || slot.GetURL != "https://objects.example.test/get" || len(slot.PutHeaders) != 2 || slot.PutHeaders[0] != (xep0363.Header{Name: "authorization", Value: "Bearer token"}) || slot.PutHeaders[1] != (xep0363.Header{Name: "cookie", Value: "a=b"}) {
		t.Fatalf("slot = %#v", slot)
	}
}

func TestUploadSlotMelliumFramingAndErrors(t *testing.T) {
	from, _ := jid.Parse("upload.mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	response := `<iq xmlns='jabber:client' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://objects.example.test/put'></put><get url='https://objects.example.test/get'></get></slot></iq>`
	truncated := strings.TrimSuffix(response, "</iq>")
	if _, _, _, err := decodeCorrelatedUploadSlot(xml.NewDecoder(strings.NewReader(truncated)), "slot-id", from, to); err == nil {
		t.Fatal("raw XML without outer end was accepted")
	}
	slot, correlated, handled, err := decodeMelliumUploadSlot(&melliumResponseReader{source: xml.NewDecoder(strings.NewReader(response))}, "slot-id", from, to)
	if err != nil || !correlated || !handled || slot.GetURL == "" {
		t.Fatalf("Mellium decode = (%#v,%v,%v,%v)", slot, correlated, handled, err)
	}
	errorResponse := `<iq xmlns='jabber:client' id='slot-id' type='error' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><request xmlns='urn:xmpp:http:upload:0' filename='x' size='1'></request><error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'></not-allowed></error></iq>`
	_, correlated, handled, err = decodeCorrelatedUploadSlot(xml.NewDecoder(strings.NewReader(errorResponse)), "slot-id", from, to)
	if !correlated || !handled || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error decode = correlated=%v handled=%v err=%v", correlated, handled, err)
	}
}

func TestUploadSlotRejectsMalformedAndAmbiguousValues(t *testing.T) {
	from, _ := jid.Parse("upload.mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	cases := []string{
		`<iq xmlns='jabber:client' xml:lang='en' xml:lang='en' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'></put><get url='https://o/get'></get></slot></iq>`,
		`<iq xmlns='jabber:client' id='wrong' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'></put><get url='https://o/get'></get></slot></iq>`,
		`<iq xmlns='jabber:client' id='slot-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'></put><get url='https://o/get'></get></slot></iq>`,
		`<iq xmlns='jabber:client' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/one'></put><put url='https://o/two'></put><get url='https://o/get'></get></slot></iq>`,
		`<iq xmlns='jabber:client' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'><header name='Authorization'>one</header><header name='authorization'>two</header></put><get url='https://o/get'></get></slot></iq>`,
		`<iq xmlns='jabber:client' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'></put><get url='https://o/get'></get><extra></extra></slot></iq>`,
		`<iq xmlns='jabber:client' id='slot-id' type='result' from='upload.mesh.test' to='agent@mesh.test/mesh-1'><slot xmlns='urn:xmpp:http:upload:0'><put url='https://o/put'></put><get url='https://o/get'></get></slot><extra></extra></iq>`,
	}
	for index, document := range cases {
		_, _, _, err := decodeCorrelatedUploadSlot(xml.NewDecoder(strings.NewReader(document)), "slot-id", from, to)
		if err == nil {
			t.Fatalf("case %d accepted", index)
		}
	}
}

type blockingUploadSession struct {
	*fakeSession
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (session *blockingUploadSession) RequestUploadSlot(context.Context, int64, string) (xep0363.Slot, error) {
	session.once.Do(func() { close(session.started) })
	<-session.release
	return xep0363.Slot{PutURL: "https://objects.example.test/put", GetURL: "https://objects.example.test/get"}, nil
}

func TestUploadSlotResultIsFencedToSessionEpoch(t *testing.T) {
	base := &fakeSession{events: make(chan Event)}
	session := &blockingUploadSession{fakeSession: base, started: make(chan struct{}), release: make(chan struct{})}
	client := testUploadClient(t, session)
	done := make(chan error, 1)
	go func() {
		_, err := client.RequestSlot(context.Background(), 32, "application/octet-stream")
		done <- err
	}()
	<-session.started
	client.mu.Lock()
	client.sessionEpoch++
	client.mu.Unlock()
	close(session.release)
	if err := <-done; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("late slot result = %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testUploadClient(t *testing.T, session Session) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Endpoint: "localhost:5222", Auth: Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, MailboxLimit: 4,
		TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4,
		UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second,
		ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second,
	}
	client, err := NewClient(config, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	return client
}
