package rank1webrtc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/pion/webrtc/v4"
)

func TestPionTURNRouteValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		urls  []string
		want  int
		valid bool
	}{
		{"all transports", []string{"turn:turn.test:3478?transport=udp", "turn:turn.test:3478?transport=tcp", "turns:turn.test:5349?transport=tcp"}, 2, true},
		{"default TLS port", []string{"turns:turn.test"}, 1, true},
		{"duplicate canonical route", []string{"turn:turn.test:3478?transport=tcp", "turn:TURN.test:03478?transport=tcp"}, 1, true},
		{"ambiguous security", []string{"turn:turn.test:443?transport=tcp", "turns:turn.test:443?transport=tcp"}, 0, false},
		{"userinfo", []string{"turn:secret@turn.test:3478?transport=tcp"}, 0, false},
		{"escaped content", []string{"turn:turn.test%2einvalid:3478?transport=tcp"}, 0, false},
		{"duplicate transport", []string{"turn:turn.test:3478?transport=tcp&transport=udp"}, 0, false},
		{"fragment", []string{"turns:turn.test:443#secret"}, 0, false},
		{"bad port", []string{"turns:turn.test:65536"}, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := pionTURNRoutes([]webrtc.ICEServer{{URLs: test.urls, Username: "test-user", Credential: "test-secret"}})
			if (err == nil) != test.valid || len(got) != test.want {
				t.Fatalf("routes=%d error=%v", len(got), err)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("route error exposed source material")
			}
		})
	}
	routes, err := pionTURNRoutes([]webrtc.ICEServer{{URLs: []string{"turn:turn.test:5349?transport=tcp", "turns:turn.test:3478?transport=tcp"}}})
	if err != nil || routes["turn.test:5349"].tls || !routes["turn.test:3478"].tls {
		t.Fatal("TLS selection must follow the URI scheme")
	}
}

func TestPionOpenRejectsUnownedTURNRouteBeforeNegotiation(t *testing.T) {
	negotiator := &blockingPionNegotiator{entered: make(chan struct{}, 1)}
	connection, err := NewPionPeerConnection(PionConfig{
		Initiator:          true,
		ICEServers:         []webrtc.ICEServer{{URLs: []string{"turn:secret@turn.test:3478?transport=tcp"}}},
		ICETransportPolicy: webrtc.ICETransportPolicyAll,
		ReceiveCapacity:    1,
		Clock:              transport.ClockFunc(func() time.Time { return time.Now().UTC() }),
	}, negotiator)
	if err != nil {
		t.Fatal(err)
	}
	if connection.turnDialer == nil {
		t.Fatal("constructor omitted the TURN setup dialer")
	}
	defer connection.Close(context.Background())
	_, err = connection.OpenDataChannel(context.Background(), DataChannelConfig{Label: "aztm", MaximumFrameBytes: 1024})
	if !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("invalid TURN route error=%v", err)
	}
	select {
	case <-negotiator.entered:
		t.Fatal("negotiation started before TURN route validation")
	default:
	}
}

func TestPionTURNStreamSetupIsBounded(t *testing.T) {
	for _, cancelOperation := range []bool{false, true} {
		t.Run(map[bool]string{false: "setup timeout", true: "operation cancellation"}[cancelOperation], func(t *testing.T) {
			d := newPionTURNDialer()
			d.setupTimeout = 50 * time.Millisecond
			entered := make(chan struct{})
			d.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finish := d.begin(ctx, map[string]pionTURNRoute{"turn.test:3478": {}})
			defer finish()
			result := make(chan error, 1)
			go func() {
				_, err := d.Dial("tcp4", "turn.test:3478")
				result <- err
			}()
			<-entered
			if cancelOperation {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, errPionTURNSetup) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("TURN stream setup exceeded its operation bound")
			}
		})
	}
}

func testTURNStreamCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "turn.test"},
		DNSNames:     []string{"turn.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, roots
}

func TestPionTURNSVerificationAndSocketHandoff(t *testing.T) {
	for _, test := range []struct {
		name       string
		serverName string
		trusted    bool
		wantOK     bool
	}{
		{"verified", "turn.test", true, true},
		{"wrong hostname", "wrong.test", true, false},
		{"untrusted certificate", "turn.test", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cert, roots := testTURNStreamCertificate(t)
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				raw, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				defer raw.Close()
				_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
				secured := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
				if secured.Handshake() != nil {
					return
				}
				_, _ = io.Copy(secured, secured)
			}()

			d := newPionTURNDialer()
			d.setupTimeout = 200 * time.Millisecond
			if test.trusted {
				d.roots = roots
			} else {
				d.roots = x509.NewCertPool()
			}
			d.dialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
			}
			operation, cancel := context.WithCancel(context.Background())
			finish := d.begin(operation, map[string]pionTURNRoute{"turn.test:443": {tls: true, serverName: test.serverName}})
			connection, err := d.Dial("tcp4", "turn.test:443")
			if !test.wantOK {
				finish()
				cancel()
				if !errors.Is(err, errPionTURNSetup) {
					t.Fatalf("unverified TLS error=%v", err)
				}
				<-serverDone
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			cancel()
			finish()
			time.Sleep(250 * time.Millisecond)
			if _, err = connection.Write([]byte("still-live")); err != nil {
				t.Fatal("setup context or deadline killed the healthy TURN socket")
			}
			got := make([]byte, len("still-live"))
			if _, err = io.ReadFull(connection, got); err != nil || string(got) != "still-live" {
				t.Fatalf("healthy TURN socket failed after handoff: %v", err)
			}
			_ = connection.Close()
			<-serverDone
		})
	}
}

func TestPionTURNRestartReplacesOperationAndRoutes(t *testing.T) {
	d := newPionTURNDialer()
	d.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}
	initial, cancel := context.WithCancel(context.Background())
	finishInitial := d.begin(initial, map[string]pionTURNRoute{"old.test:3478": {}})
	connection, err := d.Dial("tcp4", "old.test:3478")
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	cancel()
	finishInitial()

	finishRestart := d.begin(context.Background(), map[string]pionTURNRoute{"new.test:3478": {}})
	defer finishRestart()
	finishInitial()
	if _, err = d.Dial("tcp4", "old.test:3478"); err == nil {
		t.Fatal("restart retained an obsolete TURN route")
	}
	connection, err = d.Dial("tcp4", "new.test:3478")
	if err != nil {
		t.Fatal("initial cancellation poisoned the restart operation")
	}
	_ = connection.Close()
	if _, err = d.Dial("udp4", "new.test:3478"); err == nil {
		t.Fatal("stream hook accepted UDP")
	}
}

func TestPinnedPionTURNProxyHookPreservesOriginalURIHost(t *testing.T) {
	for _, scheme := range []string{"turn", "turns"} {
		t.Run(scheme, func(t *testing.T) {
			servers := []webrtc.ICEServer{{
				URLs:       []string{scheme + ":turn.test:443?transport=tcp"},
				Username:   "test-user",
				Credential: "test-password",
			}}
			routes, err := pionTURNRoutes(servers)
			if err != nil {
				t.Fatal(err)
			}
			d := newPionTURNDialer()
			calls := make(chan string, 4)
			d.dialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				calls <- network + "|" + address
				return nil, errPionTURNSetup
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			finish := d.begin(ctx, routes)
			defer finish()
			settings := webrtc.SettingEngine{}
			settings.SetICEProxyDialer(d)
			settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})
			api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
			peer, err := api.NewPeerConnection(webrtc.Configuration{
				ICEServers:         servers,
				ICETransportPolicy: webrtc.ICETransportPolicyRelay,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			if _, err = peer.CreateDataChannel("probe", nil); err != nil {
				t.Fatal(err)
			}
			offer, err := peer.CreateOffer(nil)
			if err != nil {
				t.Fatal(err)
			}
			gathered := webrtc.GatheringCompletePromise(peer)
			if err = peer.SetLocalDescription(offer); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gathered:
			case <-ctx.Done():
				t.Fatal("pinned Pion did not finish gathering after bounded stream failure")
			}
			select {
			case got := <-calls:
				if got != "tcp4|turn.test:443" {
					t.Fatalf("proxy target=%q", got)
				}
			default:
				t.Fatal("pinned Pion bypassed the bounded TURN dialer")
			}
		})
	}
}
