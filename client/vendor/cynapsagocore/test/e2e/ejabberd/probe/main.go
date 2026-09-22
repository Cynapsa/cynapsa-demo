// Command probe proves that the disposable server completes the production
// rank-2 establishment sequence before an integration or E2E test begins.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	timeNamespace  = "urn:xmpp:time"
	discoNamespace = "http://jabber.org/protocol/disco#info"
)

var tzoPattern = regexp.MustCompile(`^[+-][0-9]{2}:[0-9]{2}$`)

func main() {
	if len(os.Args) != 6 {
		panic("usage: probe MODE ENDPOINT CA USER RESOURCE")
	}
	password := os.Getenv("CYNAPSA_EJABBERD_PASSWORD_A")
	if password == "" {
		panic("probe: password environment is missing")
	}
	ca, err := os.ReadFile(os.Args[3])
	if err != nil {
		panic(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		panic("probe: invalid CA")
	}
	tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
	if os.Args[1] == "time" {
		timeControl(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig)
		return
	}
	if os.Args[1] == "control" {
		control(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig)
		return
	}
	if os.Args[1] == "client" {
		productionClient(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig, "")
		return
	}
	if os.Args[1] == "slot" {
		productionClient(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig, "slot")
		return
	}
	if os.Args[1] == "external" {
		productionClient(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig, "external")
		return
	}
	if os.Args[1] == "resume" {
		resumeRejectedControl(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig)
		return
	}
	if os.Args[1] == "application" {
		applicationWire(os.Args[2], os.Args[4], password, os.Args[5], tlsConfig)
		return
	}
	if os.Args[1] != "adapter" {
		panic("probe: mode must be adapter, application, client, control, external, resume, slot, or time")
	}
	dialer, err := rank2xmpp.NewMelliumDialer(os.Args[2], rank2xmpp.MelliumConfig{
		TLSConfig:                    tlsConfig,
		SASLMechanisms:               []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256},
		ReceiveCapacity:              4,
		StreamManagementCapacity:     4,
		StreamManagementByteCapacity: 2 << 20,
		MaximumFrameBytes:            256 << 10,
		StanzaBudgetBytes:            512 << 10,
	})
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := dialer.Dial(ctx)
	if err != nil {
		panic(err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	if err := session.ConnectTLS(ctx, os.Args[2]); err != nil {
		panic(err)
	}
	fmt.Println("adapter-phase=socket-open")
	bare, _, err := session.Authenticate(ctx, os.Args[4], []byte(password))
	if err != nil {
		panic(err)
	}
	fmt.Println("adapter-phase=credentials-recorded")
	full, err := session.BindResource(ctx, os.Args[5])
	if err != nil {
		panic(err)
	}
	if bare != os.Args[4] || full != fmt.Sprintf("%s/%s", bare, os.Args[5]) {
		panic("probe: server returned unexpected bound identity")
	}
	fmt.Println("adapter-phase=resource-recorded")
	if err := session.EnableStreamManagement(ctx, true); err != nil {
		fmt.Printf("adapter-phase=combined-negotiation-failed typed_sm=%t typed_auth=%t typed_bind=%t typed_unavailable=%t\n",
			errors.Is(err, rank2xmpp.ErrStreamManagement), errors.Is(err, rank2xmpp.ErrAuthentication),
			errors.Is(err, rank2xmpp.ErrIdentityBinding), errors.Is(err, rank2xmpp.ErrUnavailable))
		panic(err)
	}
	fmt.Println("adapter-phase=stream-management-enabled")
	serverUTC, err := queryServerTime(session, ctx)
	if err != nil {
		fmt.Printf("adapter-phase=time-query-failed typed_protocol=%t typed_unavailable=%t\n",
			errors.Is(err, rank2xmpp.ErrProtocol), errors.Is(err, rank2xmpp.ErrUnavailable))
		panic(err)
	}
	fmt.Println("adapter-phase=time-query-complete")
	fmt.Printf("adapter-time-utc=%s\n", serverUTC.UTC().Format(time.RFC3339Nano))
}

func applicationWire(endpoint, sender, senderPassword, resource string, tlsConfig *tls.Config) {
	recipient := os.Getenv("CYNAPSA_EJABBERD_AGENT_B")
	recipientPassword := os.Getenv("CYNAPSA_EJABBERD_PASSWORD_B")
	if recipient == "" || recipientPassword == "" {
		panic("probe: recipient credentials are missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	senderSession := openRank2Session(ctx, endpoint, sender, senderPassword, resource, tlsConfig)
	defer senderSession.Close(context.Background())
	recipientSession := openRank2Session(ctx, endpoint, recipient, recipientPassword, resource, tlsConfig)
	defer recipientSession.Close(context.Background())

	record := rank2xmpp.Stanza{
		Kind: rank2xmpp.StanzaEnvelope, From: sender + "/" + resource,
		To: recipient + "/" + resource, MeshID: resource, Data: []byte("canonical-opaque-envelope"),
	}
	frame, err := rank2xmpp.EncodeStanzaFrame(record, transport.MaximumControlFrameBytes, 512<<10)
	if err != nil {
		panic(err)
	}
	fmt.Printf("application-route=%s->%s mesh=%s\n", record.From, record.To, record.MeshID)
	fmt.Printf("application-frame-namespace=%s frame-bytes=%d\n", rank2xmpp.AZTMNamespaceV1, len(frame))
	fmt.Printf("application-frame-xml=%s\n", frame)
	if _, err = rank2xmpp.DecodeStanzaFrame(frame, record.From, record.To, record.MeshID, transport.MaximumControlFrameBytes, 512<<10); err != nil {
		panic(fmt.Sprintf("probe: local frame round trip: %v", err))
	}
	fmt.Println("application-local-frame-decode=true")
	if err = senderSession.Send(ctx, record); err != nil {
		panic(fmt.Sprintf("probe: sender wire: %v", err))
	}
	event, err := recipientSession.Receive(ctx)
	if err != nil {
		fmt.Printf("application-receive-error protocol=%t authentication=%t sm=%t unavailable=%t type=%T\n",
			errors.Is(err, rank2xmpp.ErrProtocol), errors.Is(err, rank2xmpp.ErrAuthentication),
			errors.Is(err, rank2xmpp.ErrStreamManagement), errors.Is(err, rank2xmpp.ErrUnavailable), err)
		panic(err)
	}
	fmt.Printf("application-received kind=%d from=%s to=%s mesh=%s bytes=%d\n",
		event.Stanza.Kind, event.Stanza.From, event.Stanza.To, event.Stanza.MeshID, len(event.Stanza.Data))
}

func openRank2Session(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config) rank2xmpp.Session {
	dialer, err := rank2xmpp.NewMelliumDialer(endpoint, rank2xmpp.MelliumConfig{
		TLSConfig: tlsConfig.Clone(), SASLMechanisms: []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256},
		ReceiveCapacity: 4, StreamManagementCapacity: 4,
		StreamManagementByteCapacity: 2 << 20,
		MaximumFrameBytes:            transport.MaximumControlFrameBytes, StanzaBudgetBytes: 512 << 10,
	})
	if err != nil {
		panic(err)
	}
	session, err := dialer.Dial(ctx)
	if err != nil {
		panic(err)
	}
	if err = session.ConnectTLS(ctx, endpoint); err != nil {
		panic(err)
	}
	bare, _, err := session.Authenticate(ctx, username, []byte(password))
	if err != nil || bare != username {
		panic(fmt.Sprintf("probe: authenticate %s: bare=%s err=%v", username, bare, err))
	}
	full, err := session.BindResource(ctx, resource)
	if err != nil || full != username+"/"+resource {
		panic(fmt.Sprintf("probe: bind %s: full=%s err=%v", username, full, err))
	}
	if err = session.EnableStreamManagement(ctx, true); err != nil {
		panic(err)
	}
	return session
}

type serverTimeQuerier interface {
	QueryServerTime(context.Context) (time.Time, error)
}

func queryServerTime(session rank2xmpp.Session, ctx context.Context) (time.Time, error) {
	querier, ok := session.(serverTimeQuerier)
	if !ok {
		return time.Time{}, fmt.Errorf("probe: production session omitted QueryServerTime")
	}
	return querier.QueryServerTime(ctx)
}

func productionClient(endpoint, username, password, resource string, tlsConfig *tls.Config, feature string) {
	dialer, err := rank2xmpp.NewMelliumDialer(endpoint, rank2xmpp.MelliumConfig{
		TLSConfig:                    tlsConfig,
		SASLMechanisms:               []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256},
		ReceiveCapacity:              8,
		StreamManagementCapacity:     8,
		StreamManagementByteCapacity: 4 << 20,
		MaximumFrameBytes:            transport.MaximumControlFrameBytes,
		StanzaBudgetBytes:            512 << 10,
	})
	if err != nil {
		panic(err)
	}
	clock := transport.NewCalibratedClock(nil)
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: clock.Now})
	if err != nil {
		panic(err)
	}
	client, err := rank2xmpp.NewClient(rank2xmpp.Config{
		Endpoint:        endpoint,
		Auth:            rank2xmpp.Authentication{Username: username, Password: []byte(password), MeshID: resource},
		ReceiveCapacity: 8, TransferWorkers: 1, TransferQueue: 8, MailboxLimit: 8,
		TransferByteCapacity: 4 << 20, UnresolvedTransferCapacity: 8,
		UnresolvedTransferByteCapacity: 4 << 20, UnresolvedTransferLifetime: 5 * time.Second,
		ReconnectAttempts: 1, ReconnectInitial: 10 * time.Millisecond, ReconnectMaximum: 10 * time.Millisecond,
		ReconnectOperationTimeout: 5 * time.Second, Clock: clock,
	}, dialer, pending, nil, nil)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Start(ctx); err != nil {
		fmt.Printf("client-phase=start-failed typed_auth=%t typed_bind=%t typed_sm=%t typed_unavailable=%t\n",
			errors.Is(err, rank2xmpp.ErrAuthentication), errors.Is(err, rank2xmpp.ErrIdentityBinding),
			errors.Is(err, rank2xmpp.ErrStreamManagement), errors.Is(err, rank2xmpp.ErrUnavailable))
		panic(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.Close(closeCtx); err != nil && !errors.Is(err, rank2xmpp.ErrClosed) {
			panic(err)
		}
	}()
	fmt.Println("client-phase=start-complete")
	snapshot, ready := client.TimeCalibration()
	if !ready || snapshot.UTC.IsZero() || snapshot.UTC.Location() != time.UTC ||
		snapshot.Uncertainty <= 0 || snapshot.Uncertainty > transport.MaximumClockUncertainty {
		panic("probe: production client returned invalid time calibration")
	}
	if feature == "slot" {
		membership, membershipErr := client.SyncAuthority(ctx)
		if membershipErr != nil || client.AcknowledgeAuthoritySnapshot(membership) != nil {
			panic(fmt.Sprintf("probe: current-membership sync before slot failed: %v", membershipErr))
		}
		slot, slotErr := client.RequestSlot(ctx, 32, "application/octet-stream")
		if slotErr != nil || slot.PutURL == "" || slot.GetURL == "" {
			panic(fmt.Sprintf("probe: production slot request failed: state=%d unavailable=%t protocol=%t error=%v", client.DurableState(), errors.Is(slotErr, rank2xmpp.ErrUnavailable), errors.Is(slotErr, rank2xmpp.ErrProtocol), slotErr))
		}
		putScheme, putHost := sanitizedSlotAuthority(slot.PutURL)
		getScheme, getHost := sanitizedSlotAuthority(slot.GetURL)
		headerNames := make([]string, 0, len(slot.PutHeaders))
		for _, header := range slot.PutHeaders {
			headerNames = append(headerNames, strings.ToLower(header.Name))
		}
		sort.Strings(headerNames)
		fmt.Printf("client-phase=upload-slot-complete put_scheme=%s put_host=%s get_scheme=%s get_host=%s headers=%d header_names=%s\n",
			putScheme, putHost, getScheme, getHost, len(headerNames), strings.Join(headerNames, ","))
	}
	if feature == "external" {
		membership, membershipErr := client.SyncAuthority(ctx)
		if membershipErr != nil || client.AcknowledgeAuthoritySnapshot(membership) != nil {
			panic(fmt.Sprintf("probe: current-membership sync before external discovery failed: %v", membershipErr))
		}
		profile, discoveryErr := client.DiscoverExternalServices(ctx)
		if discoveryErr != nil {
			panic(fmt.Sprintf("probe: external discovery failed: unavailable=%t protocol=%t error=%v", errors.Is(discoveryErr, rank2xmpp.ErrUnavailable), errors.Is(discoveryErr, rank2xmpp.ErrProtocol), discoveryErr))
		}
		defer clearExternalProbeProfile(&profile)
		credential := externalProbeCredential{}
		for _, service := range profile.Services {
			if service.Type == "turn" && service.Restricted && service.Transport == "udp" {
				credential = externalProbeCredential{Host: service.Host, Port: service.Port, Username: service.Username, Password: string(service.Password), ExpiresAt: service.ExpiresAt}
				break
			}
		}
		if credential.Host == "" || credential.Username == "" || credential.Password == "" || credential.ExpiresAt.IsZero() || !snapshot.UTC.Before(credential.ExpiresAt) {
			panic("probe: external discovery omitted usable restricted TURN credentials")
		}
		output := os.Getenv("CYNAPSA_EXTDISCO_CREDENTIAL_FILE")
		if output == "" {
			panic("probe: external credential output is missing")
		}
		file, openErr := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if openErr != nil {
			panic(openErr)
		}
		encodeErr := json.NewEncoder(file).Encode(credential)
		closeErr := file.Close()
		credential.Password = ""
		credential.Username = ""
		if encodeErr != nil || closeErr != nil {
			panic("probe: external credential output failed")
		}
		fmt.Printf("client-phase=external-services-complete services=%d restricted-turn=true expiry=true\n", len(profile.Services))
	}
	fmt.Println("client-time-ready=true")
	fmt.Printf("client-time-utc=%s\n", snapshot.UTC.Format(time.RFC3339Nano))
	fmt.Printf("client-time-uncertainty-ns=%d\n", snapshot.Uncertainty.Nanoseconds())
}

func sanitizedSlotAuthority(raw string) (string, string) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		panic("probe: production slot returned invalid URL authority")
	}
	return parsed.Scheme, parsed.Host
}

type externalProbeCredential struct {
	Host      string    `json:"host"`
	Port      uint16    `json:"port"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	ExpiresAt time.Time `json:"expires_at"`
}

func clearExternalProbeProfile(profile *rank2xmpp.ExternalServiceProfile) {
	profile.Clear()
}

type authenticatedControl struct {
	session *xmpp.Session
	conn    net.Conn
	target  jid.JID
	bound   jid.JID
}

func openAuthenticatedControl(ctx context.Context, endpoint, username, password, resource string, tlsConfig *tls.Config) authenticatedControl {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		panic(err)
	}
	origin := jid.MustParse(username + "/" + resource)
	tlsConfig = tlsConfig.Clone()
	tlsConfig.ServerName = "127.0.0.1"
	session, err := xmpp.NewClientSession(ctx, origin, conn,
		xmpp.StartTLS(tlsConfig), xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha256), xmpp.BindResource())
	if err != nil {
		_ = conn.Close()
		panic(err)
	}
	return authenticatedControl{session: session, conn: conn, target: jid.MustParse("mesh.test"), bound: session.LocalAddr()}
}

func control(endpoint, username, password, resource string, tlsConfig *tls.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := jid.MustParse(username + "/" + resource)
	control := openAuthenticatedControl(ctx, endpoint, username, password, resource, tlsConfig)
	session := control.session
	defer session.Close()
	fmt.Println("control-phase=tls-sasl-bind-complete")
	bound := session.LocalAddr()
	fmt.Printf("control-bound-match=%t local-match=%t domain-match=%t resource-match=%t\n",
		bound.String() == origin.String(), bound.Localpart() == origin.Localpart(),
		bound.Domainpart() == origin.Domainpart(), bound.Resourcepart() == origin.Resourcepart())
	fmt.Printf("control-resource-length=%d expected-resource-length=%d\n", len(bound.Resourcepart()), len(origin.Resourcepart()))
	resumeID, err := enableControlStreamManagement(session)
	if err != nil {
		panic(err)
	}
	fmt.Printf("control-resume-id-present=%t\n", resumeID != "")
	fmt.Println("control-phase=stream-management-enabled")
}

func enableControlStreamManagement(session *xmpp.Session) (string, error) {
	w := session.TokenWriter()
	start := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "enable"}, Attr: []xml.Attr{{Name: xml.Name{Local: "resume"}, Value: "true"}}}
	if err := w.EncodeToken(start); err != nil {
		return "", err
	}
	if err := w.EncodeToken(start.End()); err != nil {
		return "", err
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	_ = w.Close()
	r := session.TokenReader()
	defer r.Close()
	decoder := xml.NewTokenDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	enabled, ok := token.(xml.StartElement)
	if !ok || enabled.Name != (xml.Name{Space: "urn:xmpp:sm:3", Local: "enabled"}) {
		return "", fmt.Errorf("control: unexpected SM response name=%v", enabled.Name)
	}
	resumeID, err := enabledResumeID(enabled)
	if err != nil || decoder.Skip() != nil {
		return "", fmt.Errorf("control: invalid resumable SM response")
	}
	return resumeID, nil
}

func enabledResumeID(enabled xml.StartElement) (string, error) {
	var resumeID string
	var resumable bool
	for _, attribute := range enabled.Attr {
		if attribute.Name.Space != "" {
			continue
		}
		switch attribute.Name.Local {
		case "id":
			resumeID = attribute.Value
		case "resume":
			resumable = attribute.Value == "true" || attribute.Value == "1"
		}
	}
	if resumeID == "" || !resumable {
		return "", fmt.Errorf("control: server omitted resumable stream identity")
	}
	return resumeID, nil
}

var errExpectedResumeRejection = errors.New("probe: expected expired resume rejection")

func resumeRejectedControl(endpoint, username, password, resource string, tlsConfig *tls.Config) {
	if os.Getenv("CYNAPSA_EJABBERD_PROFILE") != "resume-rejected" {
		panic("probe: resume mode requires resume-rejected profile")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := openAuthenticatedControl(ctx, endpoint, username, password, resource, tlsConfig)
	resumeID, err := enableControlStreamManagement(first.session)
	if err != nil {
		panic(err)
	}
	// An abrupt TCP close leaves the resumable stream at the server. The profile's
	// one-second retention then makes the later resume request deterministically stale.
	if err := first.conn.Close(); err != nil {
		panic(err)
	}
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		panic(ctx.Err())
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		panic(err)
	}
	origin := jid.MustParse(username + "/" + resource)
	configured := tlsConfig.Clone()
	configured.ServerName = "127.0.0.1"
	rejected := false
	feature := xmpp.StreamFeature{
		Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "sm"}, Necessary: xmpp.Authn, Prohibited: xmpp.Ready,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			var advertised struct {
				XMLName xml.Name `xml:"urn:xmpp:sm:3 sm"`
			}
			return false, nil, decoder.DecodeElement(&advertised, start)
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			writer := session.TokenWriter()
			resume := xml.StartElement{Name: xml.Name{Space: "urn:xmpp:sm:3", Local: "resume"}, Attr: []xml.Attr{{Name: xml.Name{Local: "previd"}, Value: resumeID}, {Name: xml.Name{Local: "h"}, Value: "0"}}}
			err := writer.EncodeToken(resume)
			if err == nil {
				err = writer.EncodeToken(resume.End())
			}
			if err == nil {
				err = writer.Flush()
			}
			_ = writer.Close()
			if err != nil {
				return 0, nil, err
			}
			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			token, err := decoder.Token()
			response, ok := token.(xml.StartElement)
			if err != nil || !ok || response.Name.Space != "urn:xmpp:sm:3" || response.Name.Local != "failed" {
				return 0, nil, fmt.Errorf("probe: expired resume was not rejected")
			}
			rejected = true
			_ = decoder.Skip()
			return 0, nil, errExpectedResumeRejection
		},
	}
	resumed, err := xmpp.NewClientSession(ctx, origin, conn,
		xmpp.StartTLS(configured), xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha256), feature)
	if resumed != nil {
		_ = resumed.Close()
	}
	_ = conn.Close()
	// Mellium treats this advertised feature as optional and may continue feature
	// negotiation after our observer returns the sentinel. The authenticated
	// server's exact <failed/> response is the authoritative assertion.
	if !rejected {
		panic(fmt.Sprintf("probe: expected expired resume rejection, rejected=%t err=%v", rejected, err))
	}
	fmt.Println("resume-expired-rejected=true")
}

type timeResult struct {
	UTC    time.Time
	RawUTC string
	TZO    string
}

func timeControl(endpoint, username, password, resource string, tlsConfig *tls.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rejection, err := proveUnauthenticatedTimeRejected(ctx, endpoint, tlsConfig)
	if err != nil {
		panic(err)
	}
	fmt.Printf("time-unauthenticated-rejected=true mode=%s\n", rejection)
	control := openAuthenticatedControl(ctx, endpoint, username, password, resource, tlsConfig)
	defer control.session.Close()
	if err := control.conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		panic(err)
	}
	if err := queryDiscovery(control.session, control.target, control.bound); err != nil {
		panic(err)
	}
	fmt.Println("time-disco-feature=urn:xmpp:time")
	requestID := "cynapsa-time-1"
	started := time.Now().UTC()
	result, err := queryTime(control.session, control.target, control.bound, requestID, true)
	finished := time.Now().UTC()
	if err != nil {
		panic(err)
	}
	roundTrip := finished.Sub(started)
	midpoint := started.Add(roundTrip / 2)
	offset := result.UTC.Sub(midpoint)
	fmt.Println("time-authenticated=true")
	fmt.Println("time-response-id-match=true")
	fmt.Println("time-utc-format=strict-rfc3339-utc")
	fmt.Printf("time-utc=%s\n", result.RawUTC)
	fmt.Printf("time-utc-fraction-digits=%d\n", utcFractionDigits(result.RawUTC))
	fmt.Printf("time-tzo=%s\n", result.TZO)
	fmt.Printf("time-roundtrip-ns=%d\n", roundTrip.Nanoseconds())
	fmt.Printf("time-midpoint-offset-ns=%d\n", offset.Nanoseconds())
	fmt.Printf("time-roundtrip-ms=%d\n", roundTrip.Milliseconds())
	fmt.Printf("time-midpoint-offset-ms=%d\n", offset.Milliseconds())
}

func utcFractionDigits(value string) int {
	dot := strings.LastIndexByte(value, '.')
	if dot < 0 {
		return 0
	}
	return len(value) - dot - len(".Z")
}

func proveUnauthenticatedTimeRejected(ctx context.Context, endpoint string, tlsConfig *tls.Config) (string, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(4 * time.Second)); err != nil {
		return "", err
	}
	openStream := `<stream:stream to="mesh.test" version="1.0" xmlns="jabber:client" xmlns:stream="http://etherx.jabber.org/streams">`
	if _, err := io.WriteString(conn, openStream); err != nil {
		return "", err
	}
	decoder := xml.NewDecoder(conn)
	start, err := scanStart(decoder, xml.Name{Space: "urn:ietf:params:xml:ns:xmpp-tls", Local: "starttls"})
	if err != nil {
		return "", err
	}
	if err := decoder.Skip(); err != nil {
		return "", err
	}
	_ = start
	if _, err := io.WriteString(conn, `<starttls xmlns="urn:ietf:params:xml:ns:xmpp-tls"/>`); err != nil {
		return "", err
	}
	proceed, err := scanStart(decoder, xml.Name{Space: "urn:ietf:params:xml:ns:xmpp-tls", Local: "proceed"})
	if err != nil {
		return "", err
	}
	if err := decoder.Skip(); err != nil {
		return "", err
	}
	_ = proceed
	configured := tlsConfig.Clone()
	configured.ServerName = "127.0.0.1"
	tlsConn := tls.Client(conn, configured)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return "", err
	}
	if _, err := io.WriteString(tlsConn, openStream); err != nil {
		return "", err
	}
	decoder = xml.NewDecoder(tlsConn)
	features, err := scanStart(decoder, xml.Name{Space: "http://etherx.jabber.org/streams", Local: "features"})
	if err != nil {
		return "", err
	}
	if err := decoder.Skip(); err != nil {
		return "", err
	}
	_ = features
	unauthenticatedIQ := `<iq xmlns="jabber:client" type="get" to="mesh.test" id="unauth-time-1"><time xmlns="urn:xmpp:time"/></iq>`
	if _, err := io.WriteString(tlsConn, unauthenticatedIQ); err != nil {
		return "", err
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", fmt.Errorf("time probe: unauthenticated stream closed without explicit rejection")
			}
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				return "", fmt.Errorf("time probe: unauthenticated request timed out without explicit rejection")
			}
			return "", err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "iq":
			iq, parseErr := stanza.NewIQ(start)
			if parseErr != nil {
				return "", parseErr
			}
			if iq.ID != "unauth-time-1" {
				return "", fmt.Errorf("time probe: unauthenticated response ID mismatch")
			}
			if iq.Type == stanza.ResultIQ {
				return "", fmt.Errorf("time probe: unauthenticated time request succeeded")
			}
			if iq.Type == stanza.ErrorIQ {
				return "iq-error", nil
			}
			return "", fmt.Errorf("time probe: unexpected unauthenticated IQ type")
		case "error", "failure", "not-authorized":
			return "protocol-error", nil
		case "time":
			return "", fmt.Errorf("time probe: unauthenticated time payload returned")
		}
	}
}

func scanStart(decoder *xml.Decoder, wanted xml.Name) (xml.StartElement, error) {
	for {
		token, err := decoder.Token()
		if err != nil {
			return xml.StartElement{}, err
		}
		if start, ok := token.(xml.StartElement); ok && start.Name == wanted {
			return start, nil
		}
	}
}

func queryDiscovery(session *xmpp.Session, target, bound jid.JID) error {
	const requestID = "cynapsa-disco-1"
	payload := xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: discoNamespace, Local: "query"}})
	response, err := sendIQ(session, target, requestID, payload)
	if err != nil {
		return err
	}
	defer response.Close()
	decoder := xml.NewTokenDecoder(response)
	start, iq, err := readIQStart(decoder, requestID, target, bound, true)
	if err != nil {
		return err
	}
	if start.Name != (xml.Name{Space: discoNamespace, Local: "query"}) {
		return fmt.Errorf("time probe: unexpected discovery payload")
	}
	var body struct {
		Features []struct {
			Var string `xml:"var,attr"`
		} `xml:"feature"`
	}
	if err := decoder.DecodeElement(&body, &start); err != nil {
		return fmt.Errorf("time probe: malformed discovery response")
	}
	if err := finishIQ(decoder); err != nil {
		return err
	}
	_ = iq
	for _, feature := range body.Features {
		if feature.Var == timeNamespace {
			return nil
		}
	}
	return fmt.Errorf("time probe: server discovery omitted time feature")
}

func queryTime(session *xmpp.Session, target, bound jid.JID, requestID string, authenticated bool) (timeResult, error) {
	payload := xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: timeNamespace, Local: "time"}})
	response, err := sendIQ(session, target, requestID, payload)
	if err != nil {
		return timeResult{}, err
	}
	defer response.Close()
	return parseTimeResponse(xml.NewTokenDecoder(response), requestID, target, bound, authenticated)
}

func sendIQ(session *xmpp.Session, target jid.JID, requestID string, payload xml.TokenReader) (xmlstream.TokenReadCloser, error) {
	request := stanza.IQ{ID: requestID, To: target, Type: stanza.GetIQ}
	w := session.TokenWriter()
	if _, err := xmlstream.Copy(w, request.Wrap(payload)); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Flush(); err != nil {
		_ = w.Close()
		return nil, err
	}
	_ = w.Close()
	return session.TokenReader(), nil
}

func parseTimeResponse(decoder *xml.Decoder, requestID string, target, bound jid.JID, authenticated bool) (timeResult, error) {
	if !authenticated {
		return timeResult{}, fmt.Errorf("time probe: unauthenticated response rejected")
	}
	start, _, err := readIQStart(decoder, requestID, target, bound, authenticated)
	if err != nil {
		return timeResult{}, err
	}
	if start.Name != (xml.Name{Space: timeNamespace, Local: "time"}) {
		return timeResult{}, fmt.Errorf("time probe: unexpected response payload")
	}
	tzo, rawUTC, err := decodeStrictTimePayload(decoder, start)
	if err != nil {
		return timeResult{}, err
	}
	if err := finishIQ(decoder); err != nil {
		return timeResult{}, err
	}
	utc, err := strictUTC(rawUTC)
	if err != nil || !validTZO(tzo) {
		return timeResult{}, fmt.Errorf("time probe: malformed UTC or TZO")
	}
	return timeResult{UTC: utc, RawUTC: rawUTC, TZO: tzo}, nil
}

func decodeStrictTimePayload(decoder *xml.Decoder, start xml.StartElement) (string, string, error) {
	for _, attribute := range start.Attr {
		if attribute.Name.Space != "xmlns" && attribute.Name.Local != "xmlns" {
			return "", "", fmt.Errorf("time probe: unexpected time attributes")
		}
	}
	var tzo, utc string
	var tzoCount, utcCount int
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", "", fmt.Errorf("time probe: malformed response")
		}
		switch value := token.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(value)) != "" {
				return "", "", fmt.Errorf("time probe: unexpected text")
			}
		case xml.StartElement:
			if len(value.Attr) != 0 {
				return "", "", fmt.Errorf("time probe: unexpected field attributes")
			}
			var field string
			if err := decoder.DecodeElement(&field, &value); err != nil {
				return "", "", fmt.Errorf("time probe: malformed field")
			}
			switch value.Name {
			case xml.Name{Space: timeNamespace, Local: "tzo"}:
				tzoCount++
				tzo = field
			case xml.Name{Space: timeNamespace, Local: "utc"}:
				utcCount++
				utc = field
			default:
				return "", "", fmt.Errorf("time probe: unexpected time field")
			}
		case xml.EndElement:
			if value.Name != start.Name || tzoCount != 1 || utcCount != 1 {
				return "", "", fmt.Errorf("time probe: missing or duplicate field")
			}
			return tzo, utc, nil
		default:
			return "", "", fmt.Errorf("time probe: unexpected token")
		}
	}
}

func finishIQ(decoder *xml.Decoder) error {
	token, err := decoder.Token()
	end, ok := token.(xml.EndElement)
	if err != nil || !ok || end.Name.Local != "iq" {
		return fmt.Errorf("time probe: malformed IQ termination")
	}
	return nil
}

func readIQStart(decoder *xml.Decoder, requestID string, target, bound jid.JID, authenticated bool) (xml.StartElement, stanza.IQ, error) {
	if !authenticated {
		return xml.StartElement{}, stanza.IQ{}, fmt.Errorf("time probe: unauthenticated IQ rejected")
	}
	token, err := decoder.Token()
	if err != nil {
		return xml.StartElement{}, stanza.IQ{}, fmt.Errorf("time probe: missing IQ response")
	}
	iqStart, ok := token.(xml.StartElement)
	if !ok || iqStart.Name.Local != "iq" {
		return xml.StartElement{}, stanza.IQ{}, fmt.Errorf("time probe: malformed IQ response")
	}
	iq, err := stanza.NewIQ(iqStart)
	if err != nil || iq.Type != stanza.ResultIQ || iq.ID != requestID || !iq.From.Equal(target) || !iq.To.Equal(bound) {
		return xml.StartElement{}, stanza.IQ{}, fmt.Errorf("time probe: mismatched IQ response")
	}
	token, err = decoder.Token()
	start, ok := token.(xml.StartElement)
	if err != nil || !ok {
		return xml.StartElement{}, stanza.IQ{}, fmt.Errorf("time probe: missing IQ payload")
	}
	return start, iq, nil
}

func strictUTC(value string) (time.Time, error) {
	if !strings.HasSuffix(value, "Z") {
		return time.Time{}, fmt.Errorf("time probe: UTC value is not Z-normalized")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.Location() != time.UTC {
		return time.Time{}, fmt.Errorf("time probe: invalid UTC value")
	}
	return parsed, nil
}

func validTZO(value string) bool {
	if !tzoPattern.MatchString(value) {
		return false
	}
	hours, errHours := strconv.Atoi(value[1:3])
	minutes, errMinutes := strconv.Atoi(value[4:6])
	return errHours == nil && errMinutes == nil && minutes < 60 && hours <= 14 && (hours != 14 || minutes == 0)
}
