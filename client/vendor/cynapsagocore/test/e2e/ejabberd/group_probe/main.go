// Command group_probe is an independent raw-XMPP adversary for the disposable
// shared-group profile. It intentionally imports no Cynapsa production code.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	authorityNamespace = "urn:cynapsa:mesh-authority:1"
	probeNamespace     = "urn:cynapsa:qa:mesh-group:1"
	jingleNamespace    = "urn:xmpp:jingle:1"
	bindingNamespace   = "urn:ietf:params:xml:ns:xmpp-bind"
)

type rawEvent struct {
	kind, id, stanzaType, from, to string
	startName, endName             xml.Name
	childName                      xml.Name
	childAttrs                     []xml.Attr
}

type rawClient struct {
	session          *xmpp.Session
	conn             net.Conn
	events           chan rawEvent
	done             chan error
	resumeID         string
	frozenSMAck      atomic.Bool
	frozenSMAckCount atomic.Uint32
	smAckRequests    atomic.Uint32
	handled          atomic.Uint32
}

type authorityPeer struct {
	jid        string
	authorized bool
}

type authorityResponse struct {
	iqType               stanza.IQType
	nonce, total, cursor string
	next, condition      string
	peers                []authorityPeer
	pages                int
}

type uploadSlot struct {
	putURL string
	getURL string
}

func main() {
	if len(os.Args) < 2 {
		panic("usage: group_probe MODE ...")
	}
	switch os.Args[1] {
	case "matrix":
		if len(os.Args) != 8 {
			panic("usage: group_probe matrix ENDPOINT CA USER_A USER_B USER_C MESH")
		}
		runMatrix(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_B"), os.Args[6], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_C"), os.Args[7])
	case "authority-revalidation":
		if len(os.Args) != 9 {
			panic("usage: group_probe authority-revalidation ENDPOINT CA USER_A USER_B USER_C MESH MUTATION_SIGNAL")
		}
		runAuthorityRevalidation(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_B"), os.Args[6], os.Args[7], os.Args[8])
	case "offline-negative":
		if len(os.Args) != 7 {
			panic("usage: group_probe offline-negative ENDPOINT CA USER_A USER_B MESH")
		}
		runOfflineNegative(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5], os.Args[6])
	case "bind-denied":
		if len(os.Args) != 7 {
			panic("usage: group_probe bind-denied ENDPOINT CA USER PASSWORD_ENV MESH")
		}
		runBindDenied(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment(os.Args[5]), os.Args[6])
	case "wait-revoked":
		if len(os.Args) != 7 {
			panic("usage: group_probe wait-revoked ENDPOINT CA USER PASSWORD_ENV MESH")
		}
		runWaitRevoked(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment(os.Args[5]), os.Args[6])
	case "wait-preserved":
		if len(os.Args) != 8 && len(os.Args) != 9 {
			panic("usage: group_probe wait-preserved ENDPOINT CA USER PASSWORD_ENV MESH OTHER_MEMBER SIGNAL")
		}
		otherMember, signal := "", os.Args[7]
		if len(os.Args) == 9 {
			otherMember, signal = os.Args[7], os.Args[8]
		}
		runWaitPreserved(os.Args[2], os.Args[3], os.Args[4], requiredEnvironment(os.Args[5]), os.Args[6], otherMember, signal)
	case "send-offline":
		if len(os.Args) != 8 {
			panic("usage: group_probe send-offline ENDPOINT CA SENDER PASSWORD_ENV RECIPIENT MESH")
		}
		runSendOffline(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7])
	case "mailbox-send":
		if len(os.Args) != 9 && len(os.Args) != 10 {
			panic("usage: group_probe mailbox-send ENDPOINT CA SENDER PASSWORD_ENV RECIPIENT MESH ID [OTHER_MEMBER]")
		}
		otherMember := ""
		if len(os.Args) == 10 {
			otherMember = os.Args[9]
		}
		runMailboxSend(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7], os.Args[8], otherMember)
	case "mailbox-receive":
		if len(os.Args) != 8 {
			panic("usage: group_probe mailbox-receive ENDPOINT CA USER PASSWORD_ENV MESH ID")
		}
		runMailboxReceive(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7])
	case "mailbox-hold-sm":
		if len(os.Args) != 9 {
			panic("usage: group_probe mailbox-hold-sm ENDPOINT CA USER PASSWORD_ENV MESH READY_SIGNAL DROP_SIGNAL")
		}
		runMailboxHoldSM(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7], os.Args[8])
	case "mailbox-wait-preserved-sm":
		if len(os.Args) != 9 && len(os.Args) != 11 {
			panic("usage: group_probe mailbox-wait-preserved-sm ENDPOINT CA USER PASSWORD_ENV MESH READY_SIGNAL MUTATION_SIGNAL [FIRST_ID SECOND_ID]")
		}
		expectedIDs := os.Args[9:]
		runMailboxWaitPreservedSM(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7], os.Args[8], expectedIDs)
	case "mailbox-receive-sequence":
		if len(os.Args) != 10 {
			panic("usage: group_probe mailbox-receive-sequence ENDPOINT CA USER PASSWORD_ENV MESH FIRST_ID FIRST_ACK_SIGNAL SECOND_ID")
		}
		runMailboxReceiveSequence(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment(os.Args[5]), os.Args[6], os.Args[7], os.Args[8], os.Args[9])
	case "paged-snapshot":
		if len(os.Args) != 9 {
			panic("usage: group_probe paged-snapshot ENDPOINT CA USER OTHER_USER MESH PREFIX COUNT")
		}
		runPagedSnapshot(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5], os.Args[6], os.Args[7], os.Args[8])
	case "paged-invalidation":
		if len(os.Args) != 10 {
			panic("usage: group_probe paged-invalidation ENDPOINT CA USER OTHER_USER MESH PREFIX COUNT MUTATION_SIGNAL")
		}
		runPagedInvalidation(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5], os.Args[6], os.Args[7], os.Args[8], os.Args[9])
	case "paged-expiry":
		if len(os.Args) != 9 {
			panic("usage: group_probe paged-expiry ENDPOINT CA USER_A USER_B MESH PREFIX COUNT")
		}
		runPagedExpiry(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5],
			requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_B"), os.Args[6], os.Args[7], os.Args[8])
	case "upload-reused-id":
		if len(os.Args) != 6 {
			panic("usage: group_probe upload-reused-id ENDPOINT CA USER MESH")
		}
		runUploadReusedID(os.Args[2], os.Args[3], os.Args[4],
			requiredEnvironment("CYNAPSA_EJABBERD_PASSWORD_A"), os.Args[5])
	default:
		panic("group_probe: unknown mode")
	}
}

func runUploadReusedID(endpoint, caPath, user, password, mesh string) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	tlsConfig := loadTLS(caPath)
	client, err := openRawClient(ctx, endpoint, user, password, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("upload reused-id bind: %w", err))
	}
	defer client.close()
	wantAuthority(syncAuthority(ctx, client.session, "upload-reused-id"),
		[]string{"agent-a@mesh.test/" + mesh, "agent-b@mesh.test/" + mesh})
	const randomPart = "CCCCCCCCCCCCCCCCCCCCCCCCCC"
	const id = "cynapsa-upload-" + randomPart
	const body = "gate"
	filename := "cynapsa-" + randomPart + ".bin"
	first := requestUploadSlot(ctx, client.session, id, filename, len(body))
	putAndReadUpload(ctx, tlsConfig, first, body)
	second := requestUploadSlot(ctx, client.session, id, filename, len(body))
	if first.putURL == second.putURL || first.getURL == second.getURL {
		panic("server-generated upload paths collided")
	}
	putAndReadUpload(ctx, tlsConfig, second, body)
	fmt.Println("upload-reused-id-objects=2")
	fmt.Println("upload-http-gate=passed")
}

func requestUploadSlot(ctx context.Context, session *xmpp.Session, id, filename string, size int) uploadSlot {
	start := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:http:upload:0", Local: "request"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "filename"}, Value: filename},
		{Name: xml.Name{Local: "size"}, Value: fmt.Sprint(size)},
		{Name: xml.Name{Local: "content-type"}, Value: "application/octet-stream"},
	}}
	var payload strings.Builder
	encoder := xml.NewEncoder(&payload)
	if err := encoder.EncodeToken(start); err != nil {
		panic(err)
	}
	if err := encoder.EncodeToken(start.End()); err != nil {
		panic(err)
	}
	if err := encoder.Flush(); err != nil {
		panic(err)
	}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id,
		To: jid.MustParse("upload.mesh.test"), Type: stanza.GetIQ}
	response, err := session.SendIQElement(ctx, xml.NewDecoder(strings.NewReader(payload.String())), iq)
	if err != nil {
		panic(fmt.Errorf("request upload slot: %w", err))
	}
	if response == nil {
		panic("upload slot returned nil response")
	}
	defer response.Close()
	var slot uploadSlot
	var iqType stanza.IQType
	for {
		token, tokenErr := response.Token()
		if errors.Is(tokenErr, io.EOF) {
			break
		}
		if tokenErr != nil {
			panic(tokenErr)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch {
		case start.Name.Local == "iq":
			decoded, decodeErr := stanza.NewIQ(start)
			if decodeErr != nil {
				panic(decodeErr)
			}
			iqType = decoded.Type
		case start.Name.Space == "urn:xmpp:http:upload:0" && start.Name.Local == "put":
			slot.putURL = attrValue(start.Attr, "url")
		case start.Name.Space == "urn:xmpp:http:upload:0" && start.Name.Local == "get":
			slot.getURL = attrValue(start.Attr, "url")
		}
	}
	if iqType != stanza.ResultIQ || slot.putURL == "" || slot.getURL == "" {
		panic(fmt.Errorf("invalid upload slot response: type=%s", iqType))
	}
	return slot
}

func attrValue(attrs []xml.Attr, name string) string {
	for _, attr := range attrs {
		if attr.Name.Local == name {
			return attr.Value
		}
	}
	return ""
}

func putAndReadUpload(ctx context.Context, tlsConfig *tls.Config, slot uploadSlot, body string) {
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig.Clone()}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, slot.putURL, bytes.NewBufferString(body))
	if err != nil {
		panic(err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := httpClient.Do(request)
	if err != nil {
		panic(fmt.Errorf("upload PUT: %w", err))
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		panic(fmt.Errorf("upload PUT status = %d", response.StatusCode))
	}
	getRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, slot.getURL, nil)
	if err != nil {
		panic(err)
	}
	getResponse, err := httpClient.Do(getRequest)
	if err != nil {
		panic(fmt.Errorf("upload GET: %w", err))
	}
	got, readErr := io.ReadAll(io.LimitReader(getResponse.Body, 64))
	_ = getResponse.Body.Close()
	if readErr != nil || getResponse.StatusCode != http.StatusOK || string(got) != body {
		panic(fmt.Errorf("upload GET status=%d body=%q error=%v", getResponse.StatusCode, got, readErr))
	}
}

func runPagedInvalidation(endpoint, caPath, user, password, otherUser, mesh, prefix, countText, mutationSignal string) {
	var count int
	if _, err := fmt.Sscan(countText, &count); err != nil || count < 257 || count > 511 {
		panic("invalid paged invalidation count")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("paged invalidation bind: %w", err))
	}
	defer client.close()
	first := syncAuthorityPage(ctx, client.session, "paged-invalid-01", "0")
	if first.iqType != stanza.ResultIQ || first.next == "" || len(first.peers) != 256 {
		panic(fmt.Errorf("unexpected first immutable page: %#v", first))
	}
	fmt.Println("paged-invalidation-mutation-ready")
	waitForFile(ctx, mutationSignal)
	stale := syncAuthorityPage(ctx, client.session, "paged-invalid-01", first.next)
	if stale.iqType != stanza.ErrorIQ || stale.condition != "conflict" {
		panic(fmt.Errorf("stale snapshot continued after mutation: %#v", stale))
	}
	fmt.Println("paged-invalidation-stale=conflict")
	expected := []string{user + "/" + mesh, otherUser + "/" + mesh}
	for index := 0; index <= count; index++ {
		expected = append(expected, fmt.Sprintf("%s-%03d@mesh.test/%s", prefix, index, mesh))
	}
	fresh := syncAuthority(ctx, client.session, "paged-invalid-02")
	wantAuthority(fresh, expected)
	fmt.Printf("paged-invalidation-fresh-pages=%d members=%d\n", fresh.pages, len(expected))
}

func runPagedSnapshot(endpoint, caPath, user, password, otherUser, mesh, prefix, countText string) {
	var count int
	if _, err := fmt.Sscan(countText, &count); err != nil || count < 1 || count > 512 {
		panic("invalid paged snapshot count")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("paged snapshot bind: %w", err))
	}
	defer client.close()
	expected := []string{user + "/" + mesh, otherUser + "/" + mesh}
	for index := 0; index < count; index++ {
		expected = append(expected, fmt.Sprintf("%s-%03d@mesh.test/%s", prefix, index, mesh))
	}
	response := syncAuthority(ctx, client.session, "paged-snapshot-01")
	wantAuthority(response, expected)
	if response.pages < 2 {
		panic(fmt.Errorf("snapshot completed in %d page", response.pages))
	}
	fmt.Printf("paged-snapshot-pages=%d members=%d\n", response.pages, len(expected))
}

func runPagedExpiry(endpoint, caPath, userA, passwordA, userB, passwordB, mesh, prefix, countText string) {
	var count int
	if _, err := fmt.Sscan(countText, &count); err != nil || count < 257 || count > 512 {
		panic("invalid paged expiry count")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	tlsConfig := loadTLS(caPath)
	a, err := openRawClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("paged expiry bind A: %w", err))
	}
	defer a.close()
	b, err := openRawClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("paged expiry bind B: %w", err))
	}
	defer b.close()
	first := syncAuthorityPage(ctx, a.session, "paged-expiry-a01", "0")
	if first.iqType != stanza.ResultIQ || first.next == "" || len(first.peers) != 256 {
		panic(fmt.Errorf("unexpected stalled first page: %#v", first))
	}
	blocked := syncAuthorityPage(ctx, b.session, "paged-expiry-b00", "0")
	if blocked.iqType != stanza.ErrorIQ || blocked.condition != "resource-constraint" {
		panic(fmt.Errorf("aggregate pending bound not enforced: %#v", blocked))
	}
	select {
	case <-a.done:
		fmt.Println("paged-expiry-stalled-session=closed")
	case <-time.After(8 * time.Second):
		panic("stalled snapshot session was not closed on monotonic expiry")
	}
	expected := []string{userA + "/" + mesh, userB + "/" + mesh}
	for index := 0; index < count; index++ {
		expected = append(expected, fmt.Sprintf("%s-%03d@mesh.test/%s", prefix, index, mesh))
	}
	response := syncAuthority(ctx, b.session, "paged-expiry-b01")
	wantAuthority(response, expected)
	fmt.Printf("paged-expiry-capacity-released-pages=%d members=%d\n", response.pages, len(expected))
}

func runSendOffline(endpoint, caPath, sender, password, recipient, mesh string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, sender, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("offline sender bind: %w", err))
	}
	defer client.close()
	wantAuthority(syncAuthority(ctx, client.session, "offline-current-01"),
		[]string{sender + "/" + mesh, recipient + "/" + mesh})
	sendMessage(ctx, client.session, recipient+"/"+mesh, "offline-current-message", probeNamespace)
	time.Sleep(250 * time.Millisecond)
	fmt.Println("offline-current-message=sent")
}

const canonicalMailboxFrame = `<frame xmlns="urn:cynapsa:aztm:1" v="1">owACAQECWBljYW5vbmljYWwtb3BhcXVlLWVudG9wZQ</frame>`

func runMailboxSend(endpoint, caPath, sender, password, recipient, mesh, id, otherMember string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, sender, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("mailbox sender bind: %w", err))
	}
	defer client.close()
	expected := []string{sender + "/" + mesh, recipient + "/" + mesh}
	if otherMember != "" {
		expected = append(expected, otherMember+"/"+mesh)
	}
	wantAuthority(syncAuthority(ctx, client.session, "mailbox-send-sync"), expected)
	messageID := canonicalMailboxID(id)
	sendMessagePayload(ctx, client.session, recipient+"/"+mesh, messageID, canonicalMailboxFrame)
	receipt := wantEvent(client, messageID, "message", "headline", "mesh.test", sender+"/"+mesh, 5*time.Second)
	if receipt.childName != (xml.Name{Space: "urn:cynapsa:mesh-custody:1", Local: "accepted"}) ||
		attrValue(receipt.childAttrs, "message-id") != messageID {
		panic(fmt.Errorf("invalid custody receipt: %#v", receipt))
	}
	fmt.Printf("mailbox-sent=%s\n", id)
}

func runMailboxReceive(endpoint, caPath, user, password, mesh, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("mailbox recipient bind: %w", err))
	}
	defer client.close()
	if response := syncAuthority(ctx, client.session, "mailbox-receive-sync"); response.iqType != stanza.ResultIQ {
		panic(fmt.Errorf("mailbox recipient sync: %#v", response))
	}
	event := wantEvent(client, canonicalMailboxID(id), "message", "chat", "", user+"/"+mesh, 5*time.Second)
	if event.childName != (xml.Name{Space: "urn:cynapsa:aztm:1", Local: "frame"}) {
		panic(fmt.Errorf("mailbox frame child = %v", event.childName))
	}
	fmt.Printf("mailbox-received=%s\n", id)
}

func canonicalMailboxID(label string) string {
	digest := sha256.Sum256([]byte("cynapsa-resource-mailbox-test:" + label))
	return "msg_" + base64.RawURLEncoding.EncodeToString(digest[:16])
}

func runMailboxHoldSM(endpoint, caPath, user, password, mesh, readySignal, dropSignal string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := openRawSMClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("mailbox SM recipient bind: %w", err))
	}
	if response := syncAuthority(ctx, client.session, "mailbox-sm-sync0"); response.iqType != stanza.ResultIQ {
		panic(fmt.Errorf("mailbox SM sync: %#v", response))
	}
	if err := os.WriteFile(readySignal, nil, 0o600); err != nil {
		panic(err)
	}
	waitForFile(ctx, dropSignal)
	_ = client.conn.Close()
	select {
	case <-client.done:
	case <-time.After(3 * time.Second):
		panic("mailbox SM socket did not terminate")
	}
	fmt.Println("mailbox-sm-session-dropped")
}

// runMailboxWaitPreservedSM proves that a membership mutation fences authority
// without terminating a retained member's stream or editing its SM FIFO.
func runMailboxWaitPreservedSM(endpoint, caPath, user, password, mesh, readySignal, mutationSignal string, expectedIDs []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := openRawSMClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("mailbox invalidation recipient bind: %w", err))
	}
	if response := syncAuthority(ctx, client.session, "mailbox-invalidation-sync"); response.iqType != stanza.ResultIQ {
		panic(fmt.Errorf("mailbox invalidation sync: %#v", response))
	} else {
		client.handled.Add(uint32(response.pages))
	}
	client.freezeSMAckAtHandled()
	if err := os.WriteFile(readySignal, nil, 0o600); err != nil {
		panic(err)
	}
	waitForFile(ctx, mutationSignal)
	frames, requests := wantMembershipChangedWithMailboxFrames(client, user, mesh, expectedIDs, 5*time.Second)
	if len(expectedIDs) > 0 {
		fmt.Printf("mailbox-frozen-sm-frames=%d requests=%d\n", frames, requests)
	}
	client.close()
	fmt.Println("mailbox-resumable-stream=preserved")
}

// runMailboxReceiveSequence models the exact fresh-session boundary used by
// the deep E2E test. The first row was offered to an expired SM session and
// must drain to this replacement session. After its explicit SM ack deletes
// that row, a newly authenticated sender must still be able to commit and
// dispatch a second row to this same replacement epoch.
func runMailboxReceiveSequence(endpoint, caPath, user, password, mesh, firstID, firstAckSignal, secondID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := openRawSMClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("replacement mailbox recipient bind: %w", err))
	}
	defer client.close()
	if response := syncAuthority(ctx, client.session, "mailbox-replacement-sync"); response.iqType != stanza.ResultIQ {
		panic(fmt.Errorf("replacement mailbox recipient sync: %#v", response))
	}
	// SendIQElement consumes the terminal IQ through Mellium's request mux, so
	// the generic Serve handler does not observe it. It is nevertheless one
	// inbound stanza in the XEP-0198 handled count.
	client.handled.Add(1)
	wantMailboxFrame(client, user, mesh, firstID)
	client.ackHandled()
	if err := os.WriteFile(firstAckSignal, nil, 0o600); err != nil {
		panic(err)
	}
	wantMailboxFrame(client, user, mesh, secondID)
	client.ackHandled()
	fmt.Printf("mailbox-replacement-sequence=%s,%s\n", firstID, secondID)
}

func wantMailboxFrame(client *rawClient, user, mesh, id string) {
	event := wantEvent(client, canonicalMailboxID(id), "message", "chat", "", user+"/"+mesh, 5*time.Second)
	if event.childName != (xml.Name{Space: "urn:cynapsa:aztm:1", Local: "frame"}) {
		panic(fmt.Errorf("mailbox frame child = %v", event.childName))
	}
}

func runOfflineNegative(endpoint, caPath, userA, passwordA, userB, mesh string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := openRawClient(ctx, endpoint, userA, passwordA, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("offline-negative bind A: %w", err))
	}
	defer a.close()
	fullA := userA + "/" + mesh
	fullB := userB + "/" + mesh
	wantAuthority(syncAuthority(ctx, a.session, "offline-negative-a"), []string{fullA, fullB})
	for _, denied := range []struct {
		name, to, payload string
	}{
		{name: "bare", to: userB, payload: "<probe xmlns='" + probeNamespace + "'/>"},
		{name: "cross", to: userB + "/other", payload: "<probe xmlns='" + probeNamespace + "'/>"},
		{name: "wrapper", to: fullB, payload: "<forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' to='" + fullB + "'/></forwarded>"},
	} {
		id := "offline-denied-" + denied.name
		sendMessagePayload(ctx, a.session, denied.to, id, denied.payload)
		wantOptionalError(a, id, "message", fullA, 500*time.Millisecond)
	}
	fmt.Println("offline-denied-no-store=sent")
}

func runWaitRevoked(endpoint, caPath, user, password, mesh string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("revocation bind: %w", err))
	}
	defer client.close()
	fmt.Println("revocation-wait-ready")
	select {
	case <-client.done:
		fmt.Println("removed-session-revoked")
	case <-ctx.Done():
		panic("removed member session was not revoked")
	}
}

func runWaitPreserved(endpoint, caPath, user, password, mesh, otherMember, signal string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err != nil {
		panic(fmt.Errorf("preserved bind: %w", err))
	}
	defer client.close()
	expected := []string{user + "/" + mesh}
	if otherMember != "" {
		expected = append(expected, otherMember+"/"+mesh)
	}
	wantAuthority(syncAuthority(ctx, client.session, "preserved-before"), expected)
	fmt.Println("other-mesh-session-ready")
	waitForFile(ctx, signal)
	wantAuthority(syncAuthority(ctx, client.session, "preserved-after0"), expected)
	fmt.Println("other-mesh-session=preserved")
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("group_probe: required environment is unavailable: " + name)
	}
	return value
}

func runMatrix(endpoint, caPath, userA, passwordA, userB, passwordB, userC, passwordC, mesh string) {
	tlsConfig := loadTLS(caPath)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := openRawClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("allowed bind A: %w", err))
	}
	defer a.close()
	b, err := openRawClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("allowed bind B: %w", err))
	}
	defer b.close()
	fmt.Println("allowed-bind=true")

	if denied, err := openRawClient(ctx, endpoint, userC, passwordC, mesh, tlsConfig); err == nil {
		denied.close()
		panic("nonmember bind succeeded")
	}
	fmt.Println("nonmember-bind=denied")
	if denied, err := openRawClient(ctx, endpoint, userA, passwordA, mesh+"-other", tlsConfig); err == nil {
		denied.close()
		panic("cross-resource bind succeeded")
	}
	fmt.Println("cross-resource-bind=denied")

	fullA := userA + "/" + mesh
	fullB := userB + "/" + mesh
	sendMessage(ctx, a.session, fullB, "pre-sync-message", probeNamespace)
	wantEvent(a, "pre-sync-message", "message", "error", "mesh.test", fullA, 3*time.Second)
	wantNoEvent(b, "pre-sync-message", 350*time.Millisecond)
	fmt.Println("pre-sync-route=blocked")

	authorityA := syncAuthority(ctx, a.session, "matrix-sync-a-012345")
	wantAuthority(authorityA, []string{fullA, fullB})
	sendMessage(ctx, a.session, fullB, "recipient-unsynced-message", probeNamespace)
	wantNoEvent(b, "recipient-unsynced-message", 350*time.Millisecond)
	fmt.Println("recipient-unsynced-route=blocked")
	authorityB := syncAuthority(ctx, b.session, "matrix-sync-b-012345")
	wantAuthority(authorityB, []string{fullA, fullB})
	fmt.Println("authority-snapshot=complete")
	fmt.Printf("authority-pages=%d\n", authorityA.pages)
	fmt.Println("both-session-authority=synchronized")

	sendIQType(ctx, a.session, "mesh.test", "local-authority-time", stanza.GetIQ, "<time xmlns='urn:xmpp:time'></time>")
	wantEvent(a, "local-authority-time", "iq", "result", "mesh.test", fullA, 3*time.Second)
	fmt.Println("local-authority-time=delivered")

	sendMessage(ctx, a.session, fullB, "same-message", probeNamespace)
	messageEvent := wantEvent(b, "same-message", "message", "chat", fullA, fullB, 3*time.Second)
	fmt.Printf("message-boundary-start=%s|%s end=%s|%s\n", messageEvent.startName.Space, messageEvent.startName.Local, messageEvent.endName.Space, messageEvent.endName.Local)
	fmt.Println("same-resource-message=delivered")
	framePayload := `<frame xmlns="urn:cynapsa:aztm:1" v="1">owACAQQDeBt4ZmVyX0FBQUFBQUFBQUFBQUFBQUFBQUFBQUE</frame>`
	sendMessagePayload(ctx, a.session, fullB, "", framePayload)
	frameEvent := wantEvent(b, "", "message", "chat", fullA, fullB, 3*time.Second)
	fmt.Printf("frame-child=%s|%s attrs=%s boundary-end=%s|%s\n", frameEvent.childName.Space, frameEvent.childName.Local, formatAttrs(frameEvent.childAttrs), frameEvent.endName.Space, frameEvent.endName.Local)

	sendIQ(ctx, a.session, fullB, "same-signal", jingleNamespace)
	wantEvent(b, "same-signal", "iq", "set", fullA, fullB, 3*time.Second)
	wantEvent(a, "same-signal", "iq", "result", fullB, fullA, 3*time.Second)
	fmt.Println("same-resource-signal=delivered")

	sendIQType(ctx, a.session, fullB, "same-result", stanza.ResultIQ, "<probe xmlns='urn:cynapsa:probe:1'></probe>")
	wantEvent(b, "same-result", "iq", "result", fullA, fullB, 3*time.Second)
	sendIQType(ctx, a.session, fullB, "same-error", stanza.ErrorIQ, "<error xmlns='jabber:client' type='cancel'><policy-violation xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>")
	wantEvent(b, "same-error", "iq", "error", fullA, fullB, 3*time.Second)
	fmt.Println("same-resource-iq-terminal=delivered")

	sendPresence(ctx, a.session, fullB, "same-presence", stanza.AvailablePresence)
	wantEvent(b, "same-presence", "presence", "", fullA, fullB, 3*time.Second)
	fmt.Println("same-resource-presence=delivered")

	for _, denied := range []struct {
		name, to, kind, namespace string
	}{
		{name: "bare-message", to: userB, kind: "message", namespace: probeNamespace},
		{name: "cross-resource-message", to: userB + "/other", kind: "message", namespace: probeNamespace},
		{name: "bare-signal", to: userB, kind: "iq", namespace: jingleNamespace},
		{name: "cross-resource-signal", to: userB + "/other", kind: "iq", namespace: jingleNamespace},
		{name: "foreign-message", to: "agent-b@elsewhere.test/" + mesh, kind: "message", namespace: probeNamespace},
		{name: "foreign-service-message", to: "elsewhere.test", kind: "message", namespace: probeNamespace},
		{name: "foreign-service-iq", to: "elsewhere.test", kind: "iq", namespace: jingleNamespace},
		{name: "local-service-message", to: "mesh.test", kind: "message", namespace: probeNamespace},
		{name: "local-service-iq-unknown", to: "mesh.test", kind: "iq", namespace: probeNamespace},
	} {
		id := "denied-" + denied.name
		if denied.kind == "message" {
			sendMessage(ctx, a.session, denied.to, id, denied.namespace)
			wantEvent(a, id, "message", "error", "", fullA, 3*time.Second)
		} else {
			sendIQ(ctx, a.session, denied.to, id, denied.namespace)
			wantEvent(a, id, "iq", "error", "", fullA, 3*time.Second)
		}
		wantNoEvent(b, id, 350*time.Millisecond)
		fmt.Printf("%s=blocked\n", denied.name)
	}
	for _, denied := range []struct {
		name string
		read func() xml.TokenReader
	}{
		{name: "inherited-namespace-wrapper", read: func() xml.TokenReader {
			return xmlstream.Wrap(
				xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Local: "message"}}),
				xml.StartElement{Name: xml.Name{Local: "container"}},
			)
		}},
		{name: "prefixed-forwarded-wrapper", read: func() xml.TokenReader {
			return xmlstream.Wrap(nil, xml.StartElement{
				Name: xml.Name{Local: "f:forwarded"},
				Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns:f"}, Value: "urn:xmpp:forward:0"}},
			})
		}},
		{name: "mixed-namespace-declaration", read: func() xml.TokenReader {
			return xmlstream.Wrap(nil, xml.StartElement{
				Name: xml.Name{Local: "frame"},
				Attr: []xml.Attr{
					{Name: xml.Name{Local: "xmlns"}, Value: "urn:cynapsa:aztm:1"},
					{Name: xml.Name{Local: "xmlns:f"}, Value: "urn:xmpp:forward:0"},
				},
			})
		}},
	} {
		id := "denied-" + denied.name
		sendMessageTokenPayload(ctx, a.session, fullB, id, denied.read())
		wantNoEvent(b, id, 350*time.Millisecond)
		wantOptionalError(a, id, "message", fullA, 350*time.Millisecond)
		fmt.Printf("%s=blocked\n", denied.name)
	}
	undirectedID := "denied-undirected-presence"
	sendUndirectedPresence(ctx, a.session, undirectedID)
	wantNoEvent(b, undirectedID, 350*time.Millisecond)
	wantOptionalError(a, undirectedID, "presence", fullA, 350*time.Millisecond)
	fmt.Println("undirected-presence=blocked")

	for _, denied := range []struct {
		name, to     string
		presenceType stanza.PresenceType
	}{
		{name: "bare-presence", to: userB, presenceType: stanza.AvailablePresence},
		{name: "cross-resource-presence", to: userB + "/other", presenceType: stanza.AvailablePresence},
		{name: "bare-presence-subscribe", to: userB, presenceType: stanza.SubscribePresence},
		{name: "bare-presence-probe", to: userB, presenceType: stanza.ProbePresence},
	} {
		id := "denied-" + denied.name
		sendPresence(ctx, a.session, denied.to, id, denied.presenceType)
		wantEvent(a, id, "presence", "error", "", fullA, 3*time.Second)
		wantNoEvent(b, id, 350*time.Millisecond)
		fmt.Printf("%s=blocked\n", denied.name)
	}

	for _, denied := range []struct {
		name, payload string
	}{
		{name: "forwarded-wrapper", payload: "<forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' to='" + fullB + "'/></forwarded>"},
		{name: "carbon-wrapper", payload: "<sent xmlns='urn:xmpp:carbons:2'><forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' to='" + fullB + "'/></forwarded></sent>"},
		{name: "mam-wrapper", payload: "<result xmlns='urn:xmpp:mam:2' id='hidden'><forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' to='" + fullB + "'/></forwarded></result>"},
		{name: "nested-stanza-wrapper", payload: "<container xmlns='urn:cynapsa:probe:1'><message xmlns='jabber:client' to='" + fullB + "'/></container>"},
	} {
		id := "denied-" + denied.name
		sendMessagePayload(ctx, a.session, fullB, id, denied.payload)
		wantNoEvent(b, id, 350*time.Millisecond)
		wantOptionalError(a, id, "message", fullA, 350*time.Millisecond)
		fmt.Printf("%s=blocked\n", denied.name)
	}

}

func runAuthorityRevalidation(endpoint, caPath, userA, passwordA, userB, passwordB, userC, mesh, mutationSignal string) {
	tlsConfig := loadTLS(caPath)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	a, err := openRawSMClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("authority bind A: %w", err))
	}
	b, err := openRawSMClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig)
	if err != nil {
		a.close()
		panic(fmt.Errorf("authority bind B: %w", err))
	}
	fullA, fullB, fullC := userA+"/"+mesh, userB+"/"+mesh, userC+"/"+mesh

	sendMessage(ctx, a.session, fullB, "authority-pre-sync", probeNamespace)
	wantEvent(a, "authority-pre-sync", "message", "error", "mesh.test", fullA, 3*time.Second)
	wantNoEvent(b, "authority-pre-sync", 350*time.Millisecond)
	fmt.Println("authority-pre-sync=blocked")

	firstA := syncAuthority(ctx, a.session, "authority-first-a0")
	wantAuthority(firstA, []string{fullA, fullB, fullC})
	a.handled.Add(uint32(firstA.pages))
	sendMessage(ctx, a.session, fullB, "authority-recipient-unsynced", probeNamespace)
	wantNoEvent(b, "authority-recipient-unsynced", 350*time.Millisecond)
	firstB := syncAuthority(ctx, b.session, "authority-first-b0")
	wantAuthority(firstB, []string{fullA, fullB, fullC})
	b.handled.Add(uint32(firstB.pages))
	sendMessage(ctx, a.session, fullB, "authority-before-mutation", probeNamespace)
	wantEvent(b, "authority-before-mutation", "message", "chat", fullA, fullB, 3*time.Second)
	fmt.Println("authority-initial-snapshot=complete")
	a.ackHandled()
	b.ackHandled()

	// A transport-only outage preserves the exact logical authority epoch. A
	// successful server-validated XEP-0198 handoff must permit application
	// traffic immediately, without another snapshot request.
	aID, aHandled := dropRawClient(a, "authority clean-resume A", 5*time.Second)
	bID, bHandled := dropRawClient(b, "authority clean-resume B", 5*time.Second)
	a, err = resumeRawSMClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig, aID, aHandled)
	if err != nil {
		panic(fmt.Errorf("authority clean resume A: %w", err))
	}
	b, err = resumeRawSMClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig, bID, bHandled)
	if err != nil {
		a.close()
		panic(fmt.Errorf("authority clean resume B: %w", err))
	}
	wantResumeAuthority(a, "ready", 5*time.Second)
	wantResumeAuthority(b, "ready", 5*time.Second)
	sendMessage(ctx, a.session, fullB, "authority-after-clean-resume", probeNamespace)
	wantEvent(b, "authority-after-clean-resume", "message", "chat", fullA, fullB, 3*time.Second)
	fmt.Println("authority-clean-resume=readiness-preserved")
	a.ackHandled()
	b.ackHandled()

	// Put both retained resources in ejabberd's pending-resume state before the
	// external mutation. The server must queue the control IQ without closing
	// or editing either SM FIFO.
	aID, aHandled = dropRawClient(a, "authority mutation-resume A", 5*time.Second)
	bID, bHandled = dropRawClient(b, "authority mutation-resume B", 5*time.Second)
	fmt.Println("authority-mutation-ready")
	waitForFile(ctx, mutationSignal)

	a, err = resumeRawSMClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig, aID, aHandled)
	if err != nil {
		panic(fmt.Errorf("authority mutation resume A: %w", err))
	}
	b, err = resumeRawSMClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig, bID, bHandled)
	if err != nil {
		panic(fmt.Errorf("authority mutation resume B: %w", err))
	}
	defer func() {
		a.close()
		b.close()
	}()
	wantMembershipChanged(a, 5*time.Second)
	wantMembershipChanged(b, 5*time.Second)
	wantResumeAuthority(a, "not-ready", 5*time.Second)
	wantResumeAuthority(b, "not-ready", 5*time.Second)
	fmt.Println("authority-mutated-resume=control-replayed")

	// The successful transport resume cannot recover readiness invalidated by
	// the mutation. A strict not-ready result makes the production client
	// abandon continuity and use a fresh bind before requesting its snapshot.
	a.close()
	b.close()
	a, err = openRawSMClient(ctx, endpoint, userA, passwordA, mesh, tlsConfig)
	if err != nil {
		panic(fmt.Errorf("authority clean bind A: %w", err))
	}
	b, err = openRawSMClient(ctx, endpoint, userB, passwordB, mesh, tlsConfig)
	if err != nil {
		a.close()
		panic(fmt.Errorf("authority clean bind B: %w", err))
	}
	sendMessage(ctx, a.session, fullB, "authority-resumed-unsynced", probeNamespace)
	wantEvent(a, "authority-resumed-unsynced", "message", "error", "mesh.test", fullA, 3*time.Second)
	wantNoEvent(b, "authority-resumed-unsynced", 350*time.Millisecond)
	fmt.Println("authority-stale-snapshot=blocked")

	secondA := syncAuthority(ctx, a.session, "authority-second-a")
	wantAuthority(secondA, []string{fullA, fullB})
	a.handled.Add(uint32(secondA.pages))
	secondB := syncAuthority(ctx, b.session, "authority-second-b")
	wantAuthority(secondB, []string{fullA, fullB})
	b.handled.Add(uint32(secondB.pages))
	sendMessage(ctx, a.session, fullC, "authority-removed-peer", probeNamespace)
	wantEvent(a, "authority-removed-peer", "message", "error", "mesh.test", fullA, 3*time.Second)
	fmt.Println("authority-removed-peer=denied")
	sendMessage(ctx, a.session, fullB, "authority-after-sync", probeNamespace)
	wantEvent(b, "authority-after-sync", "message", "chat", fullA, fullB, 3*time.Second)
	fmt.Println("authority-retained-peer=delivered")
	fmt.Println("authority-revalidated-snapshot=complete")
	fmt.Println("authority-current-membership=reconciled")
	fmt.Println("authority-reconnect=fresh-sync-required")
}

func dropRawClient(client *rawClient, label string, timeout time.Duration) (string, uint32) {
	resumeID, handled := client.resumeID, client.handled.Load()
	_ = client.conn.Close()
	waitClientEnded(client, label, timeout)
	return resumeID, handled
}

func wantMembershipChanged(client *rawClient, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event := <-client.events:
			if event.kind != "iq" || event.stanzaType != string(stanza.SetIQ) ||
				event.childName != (xml.Name{Space: authorityNamespace, Local: "membership-changed"}) {
				continue
			}
			for _, attr := range event.childAttrs {
				if attr.Name.Space == "" && attr.Name.Local == "snapshot-required" && attr.Value == "true" {
					return
				}
			}
			panic(fmt.Errorf("membership control omitted snapshot-required: %#v", event))
		case err := <-client.done:
			panic(fmt.Errorf("resumed session ended before membership control: %w", err))
		case <-timer.C:
			panic("timed out waiting for replayed membership control")
		}
	}
}

func wantMembershipChangedWithMailboxFrames(client *rawClient, user, mesh string, expectedIDs []string, timeout time.Duration) (int, uint32) {
	expected := make(map[string]bool, len(expectedIDs))
	for _, id := range expectedIDs {
		expected[canonicalMailboxID(id)] = false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event := <-client.events:
			if event.kind == "message" && event.stanzaType == "chat" && event.to == user+"/"+mesh {
				if _, ok := expected[event.id]; ok {
					if event.childName != (xml.Name{Space: "urn:cynapsa:aztm:1", Local: "frame"}) {
						panic(fmt.Errorf("mailbox frame child = %v", event.childName))
					}
					expected[event.id] = true
					continue
				}
			}
			if event.kind != "iq" || event.stanzaType != string(stanza.SetIQ) ||
				event.childName != (xml.Name{Space: authorityNamespace, Local: "membership-changed"}) {
				continue
			}
			for _, attr := range event.childAttrs {
				if attr.Name.Space == "" && attr.Name.Local == "snapshot-required" && attr.Value == "true" {
					missing := make([]string, 0)
					for _, id := range expectedIDs {
						if !expected[canonicalMailboxID(id)] {
							missing = append(missing, id)
						}
					}
					if len(missing) > 0 {
						panic(fmt.Errorf("membership control arrived before mailbox frames: %v", missing))
					}
					requests := client.smAckRequests.Load()
					if len(expectedIDs) > 0 && requests == 0 {
						panic("frozen SM probe did not observe any acknowledgement request")
					}
					return len(expectedIDs), requests
				}
			}
			panic(fmt.Errorf("membership control omitted snapshot-required: %#v", event))
		case err := <-client.done:
			panic(fmt.Errorf("resumed session ended before membership control: %w", err))
		case <-timer.C:
			panic("timed out waiting for replayed membership control")
		}
	}
}

func wantResumeAuthority(client *rawClient, status string, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	markerSeen := false
	for {
		select {
		case event := <-client.events:
			if event.startName == (xml.Name{Space: "urn:xmpp:sm:3", Local: "r"}) {
				if markerSeen {
					panic("duplicate post-resume stream-management marker")
				}
				markerSeen = true
				continue
			}
			if event.kind != "iq" || event.stanzaType != string(stanza.ResultIQ) ||
				event.childName != (xml.Name{Space: authorityNamespace, Local: "resume-authority"}) {
				continue
			}
			if !markerSeen {
				// An unacknowledged result from an earlier reconnect is part of the
				// retained FIFO and is not current authority evidence.
				continue
			}
			if event.from != "mesh.test" || event.to == "" ||
				!strings.HasPrefix(event.id, "cynapsa-resume-authority-") {
				panic(fmt.Errorf("invalid resume authority owner: %#v", event))
			}
			seenStatus := ""
			for _, attr := range event.childAttrs {
				if attr.Name.Space == "" && attr.Name.Local == "status" {
					if seenStatus != "" {
						panic("duplicate resume authority status")
					}
					seenStatus = attr.Value
				}
			}
			if seenStatus != status {
				panic(fmt.Errorf("resume authority status=%q want=%q", seenStatus, status))
			}
			return
		case err := <-client.done:
			panic(fmt.Errorf("resumed session ended before authority result: %w", err))
		case <-timer.C:
			panic("timed out waiting for current resume authority result")
		}
	}
}

func waitClientEnded(client *rawClient, label string, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-client.done:
		_ = client.conn.Close()
	case <-timer.C:
		_ = client.conn.Close()
		panic(label + " did not terminate")
	}
}

func waitForFile(ctx context.Context, path string) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			panic("authority mutation signal timed out")
		case <-ticker.C:
		}
	}
}

func runBindDenied(endpoint, caPath, user, password, mesh string) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := openRawClient(ctx, endpoint, user, password, mesh, loadTLS(caPath))
	if err == nil {
		client.close()
		panic("bind unexpectedly succeeded")
	}
	fmt.Println("bind=denied")
}

func loadTLS(path string) *tls.Config {
	pem, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		panic("invalid CA")
	}
	return &tls.Config{RootCAs: roots, ServerName: "mesh.test", MinVersion: tls.VersionTLS13}
}

func openRawClient(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config) (*rawClient, error) {
	return openRawClientMode(ctx, endpoint, username, password, resource, tlsConfig, false, false)
}

func openRawSMClient(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config) (*rawClient, error) {
	return openRawClientMode(ctx, endpoint, username, password, resource, tlsConfig, true, false)
}

// openRawSMClientWithFrozenAck keeps parsing every incoming stanza and answers
// server XEP-0198 acknowledgement requests with the handled count captured
// when its receive loop starts. The live stream therefore remains responsive
// without acknowledging any later application or control stanza.
func openRawSMClientWithFrozenAck(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config) (*rawClient, error) {
	return openRawClientMode(ctx, endpoint, username, password, resource, tlsConfig, true, true)
}

func openRawClientMode(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config, enableSM, frozenSMAck bool) (*rawClient, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	origin, err := jid.Parse(username + "/" + resource)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	session, err := xmpp.NewClientSession(ctx, origin, conn,
		xmpp.StartTLS(tlsConfig.Clone()),
		xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha256),
		exactResourceBinding(resource),
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if session.LocalAddr().String() != origin.String() {
		_ = session.Close()
		_ = conn.Close()
		return nil, errors.New("server substituted requested resource")
	}
	var resumeID string
	if enableSM {
		resumeID, err = enableRawStreamManagement(session)
		if err != nil {
			_ = session.Close()
			_ = conn.Close()
			return nil, err
		}
	}
	return serveRawClient(session, conn, resumeID, 0, frozenSMAck), nil
}

func resumeRawSMClient(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config, resumeID string, handled uint32) (*rawClient, error) {
	if resumeID == "" {
		return nil, errors.New("missing resumable stream identity")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	origin, err := jid.Parse(username + "/" + resource)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	feature := rawResumeFeature(origin, resumeID, handled)
	session, err := xmpp.NewClientSession(ctx, origin, conn,
		xmpp.StartTLS(tlsConfig.Clone()),
		xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha256),
		feature,
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return serveRawClient(session, conn, resumeID, handled, false), nil
}

func rawResumeFeature(origin jid.JID, resumeID string, handled uint32) xmpp.StreamFeature {
	return xmpp.StreamFeature{
		Name:       xml.Name{Space: "urn:xmpp:sm:3", Local: "sm"},
		Necessary:  xmpp.Authn,
		Prohibited: xmpp.Ready,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			var advertised struct {
				XMLName xml.Name `xml:"urn:xmpp:sm:3 sm"`
			}
			return true, nil, decoder.DecodeElement(&advertised, start)
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			writer := session.TokenWriter()
			resume := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "resume"}, Attr: []xml.Attr{
				{Name: xml.Name{Local: "previd"}, Value: resumeID},
				{Name: xml.Name{Local: "h"}, Value: fmt.Sprint(handled)},
			}}
			for _, token := range []xml.Token{resume, resume.End()} {
				if err := writer.EncodeToken(token); err != nil {
					_ = writer.Close()
					return 0, nil, err
				}
			}
			if err := writer.Flush(); err != nil {
				_ = writer.Close()
				return 0, nil, err
			}
			_ = writer.Close()

			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			token, err := decoder.Token()
			start, ok := token.(xml.StartElement)
			if err != nil || !ok || start.Name != (xml.Name{Space: "urn:xmpp:sm:3", Local: "resumed"}) {
				return 0, nil, errors.New("stream management resume was not accepted")
			}
			previd := ""
			for _, attr := range start.Attr {
				if attr.Name.Space == "" && attr.Name.Local == "previd" {
					previd = attr.Value
				}
			}
			if previd != resumeID {
				return 0, nil, errors.New("stream management resumed the wrong logical session")
			}
			if err := decoder.Skip(); err != nil {
				return 0, nil, err
			}
			if !session.UpdateAddr(origin) {
				return 0, nil, errors.New("cannot restore resumed full JID")
			}
			return xmpp.Ready, nil, nil
		},
	}
}

func serveRawClient(session *xmpp.Session, conn net.Conn, resumeID string, handled uint32, frozenSMAck bool) *rawClient {
	client := &rawClient{
		session:  session,
		conn:     conn,
		events:   make(chan rawEvent, 64),
		done:     make(chan error, 1),
		resumeID: resumeID,
	}
	client.handled.Store(handled)
	client.frozenSMAck.Store(frozenSMAck)
	client.frozenSMAckCount.Store(handled)
	go func() {
		client.done <- session.Serve(xmpp.HandlerFunc(client.handle))
		close(client.done)
	}()
	return client
}

func enableRawStreamManagement(session *xmpp.Session) (string, error) {
	writer := session.TokenWriter()
	start := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "enable"}, Attr: []xml.Attr{{Name: xml.Name{Local: "resume"}, Value: "true"}}}
	for _, token := range []xml.Token{start, start.End()} {
		if err := writer.EncodeToken(token); err != nil {
			_ = writer.Close()
			return "", err
		}
	}
	if err := writer.Flush(); err != nil {
		_ = writer.Close()
		return "", err
	}
	_ = writer.Close()
	reader := session.TokenReader()
	defer reader.Close()
	decoder := xml.NewTokenDecoder(reader)
	token, err := decoder.Token()
	enabled, ok := token.(xml.StartElement)
	if err != nil || !ok || enabled.Name != (xml.Name{Space: "urn:xmpp:sm:3", Local: "enabled"}) {
		return "", errors.New("stream management was not enabled")
	}
	var resumeID string
	var resumable bool
	for _, attr := range enabled.Attr {
		if attr.Name.Space != "" {
			continue
		}
		switch attr.Name.Local {
		case "id":
			resumeID = attr.Value
		case "resume":
			resumable = attr.Value == "true" || attr.Value == "1"
		}
	}
	if resumeID == "" || !resumable {
		return "", errors.New("server omitted resumable stream identity")
	}
	if err := decoder.Skip(); err != nil {
		return "", err
	}
	return resumeID, nil
}

// Mellium v0.23.0's stock BindResource encoder does not place the requested
// resource in the bind request. This independent feature writes the public
// XMPP resource-binding exchange directly and then checks the server's exact
// full-JID result.
func exactResourceBinding(resource string) xmpp.StreamFeature {
	return xmpp.StreamFeature{
		Name:       xml.Name{Space: bindingNamespace, Local: "bind"},
		Necessary:  xmpp.Authn,
		Prohibited: xmpp.Ready,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			if start == nil || start.Name != (xml.Name{Space: bindingNamespace, Local: "bind"}) {
				return true, nil, errors.New("missing resource-binding feature")
			}
			return true, nil, decoder.Skip()
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			if session == nil || resource == "" || session.LocalAddr().Resourcepart() != resource {
				return 0, nil, errors.New("invalid requested resource")
			}
			requestID := "qa-bind-" + rand.Text()
			writer := session.TokenWriter()
			iq := xml.StartElement{Name: xml.Name{Space: stanza.NSClient, Local: "iq"}, Attr: []xml.Attr{
				{Name: xml.Name{Local: "type"}, Value: string(stanza.SetIQ)},
				{Name: xml.Name{Local: "id"}, Value: requestID},
			}}
			bind := xml.StartElement{Name: xml.Name{Space: bindingNamespace, Local: "bind"}}
			requested := xml.StartElement{Name: xml.Name{Space: bindingNamespace, Local: "resource"}}
			for _, token := range []xml.Token{iq, bind, requested, xml.CharData(resource), requested.End(), bind.End(), iq.End()} {
				if err := writer.EncodeToken(token); err != nil {
					_ = writer.Close()
					return 0, nil, err
				}
			}
			if err := writer.Flush(); err != nil {
				_ = writer.Close()
				return 0, nil, err
			}
			_ = writer.Close()

			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			token, err := decoder.Token()
			start, ok := token.(xml.StartElement)
			if err != nil || !ok || start.Name.Local != "iq" {
				return 0, nil, errors.New("malformed resource-binding response")
			}
			response, err := stanza.NewIQ(start)
			if err != nil || response.ID != requestID || response.Type != stanza.ResultIQ {
				return 0, nil, errors.New("resource bind rejected")
			}
			var result struct {
				Bind struct {
					JID string `xml:"jid"`
				} `xml:"bind"`
			}
			if err := decoder.DecodeElement(&result, &start); err != nil {
				return 0, nil, err
			}
			bound, err := jid.Parse(result.Bind.JID)
			if err != nil || bound.String() != session.LocalAddr().Bare().String()+"/"+resource || !session.UpdateAddr(bound) {
				return 0, nil, errors.New("server returned unexpected bound identity")
			}
			return xmpp.Ready, nil, nil
		},
	}
}

func (client *rawClient) handle(stream xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	var endName xml.Name
	var childName xml.Name
	var childAttrs []xml.Attr
	for {
		token, err := stream.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if end, ok := token.(xml.EndElement); ok {
			endName = end.Name
		}
		if child, ok := token.(xml.StartElement); ok && childName.Local == "" {
			childName = child.Name
			childAttrs = append([]xml.Attr(nil), child.Attr...)
		}
	}
	event := rawEvent{kind: start.Name.Local, startName: start.Name, endName: endName, childName: childName, childAttrs: childAttrs}
	if start.Name == (xml.Name{Space: "urn:xmpp:sm:3", Local: "r"}) {
		if childName.Local != "" || endName != start.Name {
			return errors.New("malformed stream-management request")
		}
		handled := client.handled.Load()
		if client.frozenSMAck.Load() {
			handled = client.frozenSMAckCount.Load()
		}
		ack := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "a"}, Attr: []xml.Attr{
			{Name: xml.Name{Local: "h"}, Value: fmt.Sprint(handled)},
		}}
		if err := stream.EncodeToken(ack); err != nil {
			return err
		}
		if err := stream.EncodeToken(ack.End()); err != nil {
			return err
		}
		client.smAckRequests.Add(1)
		select {
		case client.events <- event:
			return nil
		default:
			return errors.New("raw event capacity exhausted")
		}
	}
	switch start.Name.Local {
	case "message":
		message, err := stanza.NewMessage(*start)
		if err != nil {
			return err
		}
		event.id, event.stanzaType = message.ID, string(message.Type)
		event.from, event.to = message.From.String(), message.To.String()
	case "iq":
		iq, err := stanza.NewIQ(*start)
		if err != nil {
			return err
		}
		event.id, event.stanzaType = iq.ID, string(iq.Type)
		event.from, event.to = iq.From.String(), iq.To.String()
		if iq.Type == stanza.GetIQ || iq.Type == stanza.SetIQ {
			if _, err := xmlstream.Copy(stream, iq.Result(nil)); err != nil {
				return err
			}
		}
	case "presence":
		presence, err := stanza.NewPresence(*start)
		if err != nil {
			return err
		}
		event.id, event.stanzaType = presence.ID, string(presence.Type)
		event.from, event.to = presence.From.String(), presence.To.String()
	default:
		return nil
	}
	client.handled.Add(1)
	select {
	case client.events <- event:
	default:
		return errors.New("raw event capacity exhausted")
	}
	return nil
}

func (client *rawClient) freezeSMAckAtHandled() {
	client.frozenSMAckCount.Store(client.handled.Load())
	client.frozenSMAck.Store(true)
}

func (client *rawClient) ackHandled() {
	handled := client.handled.Load()
	writer := client.session.TokenWriter()
	start := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "a"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "h"}, Value: fmt.Sprint(handled)},
	}}
	for _, token := range []xml.Token{start, start.End()} {
		if err := writer.EncodeToken(token); err != nil {
			_ = writer.Close()
			panic(fmt.Errorf("write stream-management ack: %w", err))
		}
	}
	if err := writer.Flush(); err != nil {
		_ = writer.Close()
		panic(fmt.Errorf("flush stream-management ack: %w", err))
	}
	_ = writer.Close()
}

func (client *rawClient) close() {
	if client == nil {
		return
	}
	_ = client.session.SetCloseDeadline(time.Now().Add(time.Second))
	_ = client.session.Close()
	_ = client.conn.Close()
	select {
	case <-client.done:
	case <-time.After(2 * time.Second):
		panic("raw client did not terminate")
	}
}

func sendMessage(ctx context.Context, session *xmpp.Session, target, id, namespace string) {
	sendMessagePayload(ctx, session, target, id, fmt.Sprintf("<probe xmlns='%s' id='%s'></probe>", namespace, id))
}

func sendMessagePayload(ctx context.Context, session *xmpp.Session, target, id, payload string) {
	sendMessageTokenPayload(ctx, session, target, id, xml.NewDecoder(strings.NewReader(payload)))
}

func sendMessageTokenPayload(ctx context.Context, session *xmpp.Session, target, id string, payload xml.TokenReader) {
	to := jid.MustParse(target)
	message := stanza.Message{XMLName: xml.Name{Space: stanza.NSClient, Local: "message"}, ID: id, To: to, Type: stanza.ChatMessage}
	if err := session.SendElement(ctx, payload, message.StartElement()); err != nil {
		panic(err)
	}
}

func formatAttrs(attrs []xml.Attr) string {
	parts := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		parts = append(parts, attr.Name.Space+"|"+attr.Name.Local+"="+attr.Value)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func sendIQ(ctx context.Context, session *xmpp.Session, target, id, namespace string) {
	to := jid.MustParse(target)
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: to, Type: stanza.SetIQ}
	payload := fmt.Sprintf("<jingle xmlns='%s' action='session-initiate' sid='%s'></jingle>", namespace, id)
	if err := session.SendElement(ctx, xml.NewDecoder(strings.NewReader(payload)), iq.StartElement()); err != nil {
		panic(err)
	}
}

func sendIQType(ctx context.Context, session *xmpp.Session, target, id string, iqType stanza.IQType, payload string) {
	to := jid.MustParse(target)
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: to, Type: iqType}
	if err := session.SendElement(ctx, xml.NewDecoder(strings.NewReader(payload)), iq.StartElement()); err != nil {
		panic(err)
	}
}

func sendPresence(ctx context.Context, session *xmpp.Session, target, id string, presenceType stanza.PresenceType) {
	to := jid.MustParse(target)
	presence := stanza.Presence{XMLName: xml.Name{Space: stanza.NSClient, Local: "presence"}, ID: id, To: to, Type: presenceType}
	if err := session.SendElement(ctx, xml.NewDecoder(strings.NewReader("")), presence.StartElement()); err != nil {
		panic(err)
	}
}

func sendUndirectedPresence(ctx context.Context, session *xmpp.Session, id string) {
	presence := stanza.Presence{XMLName: xml.Name{Space: stanza.NSClient, Local: "presence"}, ID: id}
	if err := session.SendElement(ctx, xml.NewDecoder(strings.NewReader("")), presence.StartElement()); err != nil {
		panic(err)
	}
}

func wantEvent(client *rawClient, id, kind, stanzaType, from, to string, timeout time.Duration) rawEvent {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case event := <-client.events:
			if event.id != id {
				continue
			}
			if event.kind != kind || event.stanzaType != stanzaType || from != "" && event.from != from || to != "" && event.to != to {
				panic(fmt.Errorf("event %s mismatch: %#v", id, event))
			}
			return event
		case err := <-client.done:
			panic(fmt.Errorf("session ended waiting for %s: %w", id, err))
		case <-deadline.C:
			panic("timed out waiting for " + id)
		}
	}
}

func wantNoEvent(client *rawClient, id string, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case event := <-client.events:
			if event.id == id {
				panic(fmt.Errorf("denied stanza delivered: %#v", event))
			}
		case err := <-client.done:
			panic(fmt.Errorf("recipient session ended during negative window: %w", err))
		case <-timer.C:
			return
		}
	}
}

func wantOptionalError(client *rawClient, id, kind, to string, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case event := <-client.events:
			if event.id != id {
				continue
			}
			if event.kind != kind || event.stanzaType != "error" || event.to != to {
				panic(fmt.Errorf("blocked stanza produced non-error event: %#v", event))
			}
			return
		case err := <-client.done:
			panic(fmt.Errorf("sender session ended during rejection window: %w", err))
		case <-timer.C:
			return
		}
	}
}

func syncAuthority(ctx context.Context, session *xmpp.Session, nonce string) authorityResponse {
	combined := authorityResponse{iqType: stanza.ResultIQ, nonce: nonce, cursor: "0"}
	cursor := "0"
	for {
		page := syncAuthorityPage(ctx, session, nonce, cursor)
		if page.iqType != stanza.ResultIQ || page.nonce != nonce || page.cursor != cursor || page.total == "" {
			panic(fmt.Errorf("authority page mismatch: %#v", page))
		}
		if combined.total == "" {
			combined.total = page.total
		} else if combined.total != page.total {
			panic(fmt.Errorf("authority total changed: %s to %s", combined.total, page.total))
		}
		combined.peers = append(combined.peers, page.peers...)
		combined.pages++
		if page.next == "" {
			return combined
		}
		cursor = page.next
	}
}

func syncAuthorityPage(ctx context.Context, session *xmpp.Session, nonce, cursor string) authorityResponse {
	start := xml.StartElement{Name: xml.Name{Space: authorityNamespace, Local: "sync"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "nonce"}, Value: nonce},
		{Name: xml.Name{Local: "cursor"}, Value: cursor},
	}}
	var payload strings.Builder
	encoder := xml.NewEncoder(&payload)
	if err := encoder.EncodeToken(start); err != nil {
		panic(err)
	}
	if err := encoder.EncodeToken(start.End()); err != nil {
		panic(err)
	}
	if err := encoder.Flush(); err != nil {
		panic(err)
	}
	id := "qa-authority-" + nonce
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: jid.MustParse("mesh.test"), Type: stanza.GetIQ}
	response, err := session.SendIQElement(ctx, xml.NewDecoder(strings.NewReader(payload.String())), iq)
	if err != nil {
		panic(fmt.Errorf("authority sync %s: %w", nonce, err))
	}
	if response == nil {
		panic("authority sync returned nil response")
	}
	defer response.Close()
	return decodeAuthorityResponse(response)
}

func decodeAuthorityResponse(reader xml.TokenReader) authorityResponse {
	var response authorityResponse
	for {
		token, err := reader.Token()
		if errors.Is(err, io.EOF) {
			return response
		}
		if err != nil {
			panic(err)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch {
		case start.Name.Local == "iq":
			iq, err := stanza.NewIQ(start)
			if err != nil {
				panic(err)
			}
			response.iqType = iq.Type
		case start.Name == (xml.Name{Space: authorityNamespace, Local: "synchronized"}):
			for _, attr := range start.Attr {
				switch attr.Name.Local {
				case "nonce":
					response.nonce = attr.Value
				case "total":
					response.total = attr.Value
				case "cursor":
					response.cursor = attr.Value
				case "next":
					response.next = attr.Value
				}
			}
		case start.Name == (xml.Name{Space: authorityNamespace, Local: "member"}):
			var peer authorityPeer
			for _, attr := range start.Attr {
				switch attr.Name.Local {
				case "jid":
					peer.jid = attr.Value
				}
			}
			peer.authorized = true
			response.peers = append(response.peers, peer)
		case start.Name.Space == "urn:ietf:params:xml:ns:xmpp-stanzas" && start.Name.Local != "text":
			response.condition = start.Name.Local
		}
	}
}

func wantAuthority(response authorityResponse, expected []string) {
	if response.iqType != stanza.ResultIQ || response.total != fmt.Sprint(len(expected)) || response.cursor != "0" || response.next != "" || response.pages < 1 {
		panic(fmt.Errorf("unexpected authority response for %d expected members: %#v", len(expected), response))
	}
	actual := make([]string, 0, len(response.peers))
	for _, peer := range response.peers {
		if !peer.authorized || peer.jid == "" {
			panic(fmt.Errorf("invalid authority member: %#v", peer))
		}
		actual = append(actual, peer.jid)
	}
	sort.Strings(actual)
	want := append([]string(nil), expected...)
	sort.Strings(want)
	if strings.Join(actual, ",") != strings.Join(want, ",") {
		panic(fmt.Errorf("authority members = %v, want %v", actual, want))
	}
}
