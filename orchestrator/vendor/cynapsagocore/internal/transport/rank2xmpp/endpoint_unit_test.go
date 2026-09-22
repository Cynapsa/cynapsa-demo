package rank2xmpp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mellium.im/sasl"
)

func TestParseEndpointCanonicalHostPortAndTLSName(t *testing.T) {
	tests := []struct {
		input      string
		dial       string
		serverName string
	}{
		{input: "Mesh-01.Example.TEST:5222", dial: "mesh-01.example.test:5222", serverName: "mesh-01.example.test"},
		{input: "localhost:00080", dial: "localhost:80", serverName: "localhost"},
		{input: "mesh.example:1", dial: "mesh.example:1", serverName: "mesh.example"},
		{input: "mesh.example:65535", dial: "mesh.example:65535", serverName: "mesh.example"},
		{input: "192.0.2.10:443", dial: "192.0.2.10:443", serverName: "192.0.2.10"},
		{input: "[2001:0DB8:0:0:0:0:0:1]:5222", dial: "[2001:db8::1]:5222", serverName: "2001:db8::1"},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			endpoint, err := ParseEndpoint(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if endpoint.DialAddress() != test.dial || endpoint.TLSServerName() != test.serverName {
				t.Fatalf("endpoint = {%q, %q}", endpoint.DialAddress(), endpoint.TLSServerName())
			}
		})
	}
}

func TestParseEndpointRejectsImplicitAmbiguousAndNonASCIIForms(t *testing.T) {
	invalid := []string{
		"", "example.test", "example.test:", ":5222", "example.test:xmpp-client",
		"example.test:0", "example.test:65536", "example.test:-1", "example.test:+5222",
		"xmpp://example.test:5222", "user@example.test:5222", "example.test:5222/path",
		"example.test:5222?query", "example.test:5222#fragment", " example.test:5222",
		"example.test:5222 ", "example.test.:5222", "m\u00e9sh.example:5222", "bad_name:5222",
		"-bad.example:5222", "bad-.example:5222", "bad..example:5222",
		"[example.test]:5222", "[192.0.2.1]:5222", "2001:db8::1:5222", "[fe80::1%en0]:5222",
	}
	for _, input := range invalid {
		t.Run(input, func(t *testing.T) {
			if endpoint, err := ParseEndpoint(input); !errors.Is(err, ErrInvalidConfig) || endpoint != (Endpoint{}) {
				t.Fatalf("endpoint = %#v, %v", endpoint, err)
			}
		})
	}
}

func validMelliumConfig(tlsConfig *tls.Config) MelliumConfig {
	return MelliumConfig{
		TLSConfig:                    tlsConfig,
		SASLMechanisms:               []sasl.Mechanism{sasl.ScramSha256},
		ReceiveCapacity:              1,
		StreamManagementCapacity:     1,
		StreamManagementByteCapacity: 1 << 20,
		MaximumFrameBytes:            1024,
		StanzaBudgetBytes:            2048,
	}
}

func TestNewMelliumDialerDerivesAndClonesTLSName(t *testing.T) {
	original := &tls.Config{MinVersion: tls.VersionTLS13}
	dialer, err := NewMelliumDialer("Mesh.Example:5222", validMelliumConfig(original))
	if err != nil {
		t.Fatal(err)
	}
	if original.ServerName != "" {
		t.Fatalf("caller TLS config mutated: %q", original.ServerName)
	}
	if dialer.config.TLSConfig == original || dialer.config.TLSConfig.ServerName != "mesh.example" {
		t.Fatalf("derived TLS config = %#v", dialer.config.TLSConfig)
	}
	if dialer.endpoint.DialAddress() != "mesh.example:5222" {
		t.Fatalf("dial endpoint = %q", dialer.endpoint.DialAddress())
	}
	original.MinVersion = tls.VersionTLS10
	if dialer.config.TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Fatal("dialer retained caller TLS config")
	}
}

func TestNewMelliumDialerRejectsTLSDowngradeAndNameConflict(t *testing.T) {
	tests := []struct {
		name   string
		config *tls.Config
	}{
		{name: "nil config", config: nil},
		{name: "verification disabled", config: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec -- rejection test
		{name: "conflicting name", config: &tls.Config{ServerName: "other.example"}},
		{name: "trailing dot conflict", config: &tls.Config{ServerName: "mesh.example."}},
		{name: "TLS 1.2 maximum", config: &tls.Config{MaxVersion: tls.VersionTLS12}},
		{name: "unsupported maximum", config: &tls.Config{MaxVersion: tls.VersionTLS13 + 1}},
		{name: "maximum uint16", config: &tls.Config{MaxVersion: ^uint16(0)}},
		{name: "unsupported minimum", config: &tls.Config{MinVersion: tls.VersionTLS13 + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewMelliumDialer("mesh.example:5222", validMelliumConfig(test.config)); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if _, err := NewMelliumDialer("mesh.example:5222", validMelliumConfig(&tls.Config{ServerName: "MESH.EXAMPLE"})); err != nil {
		t.Fatalf("equivalent DNS name rejected: %v", err)
	}
	if _, err := NewMelliumDialer("[2001:db8::1]:5222", validMelliumConfig(&tls.Config{ServerName: "2001:0db8::1"})); err != nil {
		t.Fatalf("equivalent IP name rejected: %v", err)
	}
}

func TestXMPPTLSConfigMinMaxCrossProductAndCallerClone(t *testing.T) {
	endpoint, err := ParseEndpoint("mesh.example:5222")
	if err != nil {
		t.Fatal(err)
	}
	minimums := []uint16{0, tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13, tls.VersionTLS13 + 1, ^uint16(0)}
	maximums := []uint16{0, tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13, tls.VersionTLS13 + 1, ^uint16(0)}
	for _, minimum := range minimums {
		for _, maximum := range maximums {
			t.Run(fmt.Sprintf("min_%04x_max_%04x", minimum, maximum), func(t *testing.T) {
				caller := &tls.Config{MinVersion: minimum, MaxVersion: maximum}
				config, err := tlsConfigForEndpoint(endpoint, caller)
				wantAccepted := minimum <= tls.VersionTLS13 && (maximum == 0 || maximum == tls.VersionTLS13)
				if !wantAccepted {
					if !errors.Is(err, ErrInvalidConfig) || config != nil {
						t.Fatalf("config=%#v err=%v", config, err)
					}
					if caller.MinVersion != minimum || caller.MaxVersion != maximum || caller.ServerName != "" {
						t.Fatalf("rejected caller mutated: %#v", caller)
					}
					return
				}
				if err != nil || config == nil || config == caller || config.MinVersion != tls.VersionTLS13 || config.MaxVersion != maximum || config.ServerName != endpoint.TLSServerName() {
					t.Fatalf("config=%#v err=%v", config, err)
				}
				caller.MinVersion = 0
				caller.MaxVersion = tls.VersionTLS12
				caller.ServerName = "caller-mutated.example"
				if config.MinVersion != tls.VersionTLS13 || config.MaxVersion != maximum || config.ServerName != endpoint.TLSServerName() {
					t.Fatalf("derived config aliases caller: %#v", config)
				}
			})
		}
	}
}

func TestXMPPTLSFinalSeamAcceptsTLS13AndRejectsTLS12(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   uint16
		clientMax uint16
		accept    bool
	}{
		{name: "TLS 1.3 unbounded maximum", version: tls.VersionTLS13, accept: true},
		{name: "TLS 1.3 exact maximum", version: tls.VersionTLS13, clientMax: tls.VersionTLS13, accept: true},
		{name: "TLS 1.2 only", version: tls.VersionTLS12},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			server.TLS = &tls.Config{MinVersion: test.version, MaxVersion: test.version}
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			defer server.Close()
			endpoint, err := ParseEndpoint(strings.TrimPrefix(server.URL, "https://"))
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			caller := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, MaxVersion: test.clientMax}
			config, err := tlsConfigForEndpoint(endpoint, caller)
			if err != nil {
				t.Fatal(err)
			}
			if caller.MinVersion != tls.VersionTLS12 || caller.MaxVersion != test.clientMax || config.MinVersion != tls.VersionTLS13 || config.MaxVersion != test.clientMax || config.RootCAs != roots || config.ServerName != endpoint.TLSServerName() || config.InsecureSkipVerify {
				t.Fatalf("TLS config caller=%#v derived=%#v", caller, config)
			}
			connection, dialErr := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", endpoint.DialAddress(), config)
			if connection != nil {
				defer connection.Close()
			}
			if test.accept {
				if dialErr != nil || connection.ConnectionState().Version != tls.VersionTLS13 {
					t.Fatalf("TLS 1.3 dial = (%#v, %v)", connection, dialErr)
				}
			} else if dialErr == nil {
				t.Fatal("TLS 1.2-only endpoint was accepted")
			}
		})
	}
}

func TestMelliumSessionRejectsEndpointSubstitutionBeforeDial(t *testing.T) {
	dialer, err := NewMelliumDialer("mesh.example:5222", validMelliumConfig(&tls.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*melliumSession)
	if err = session.ConnectTLS(context.Background(), "other.example:5222"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("substitution error = %v", err)
	}
	if session.conn != nil {
		t.Fatal("endpoint substitution attempted a network connection")
	}
}

func TestNewClientStoresOnlyCanonicalEndpoint(t *testing.T) {
	client := newIdentityClientForEndpoint(t, "Mesh.Example:05222", &identityDialer{sessions: []Session{newIdentitySession("proof")}})
	if client.config.Endpoint != "mesh.example:5222" {
		t.Fatalf("client endpoint = %q", client.config.Endpoint)
	}
}

func TestNewClientRejectsInvalidEndpoint(t *testing.T) {
	for _, endpoint := range []string{"mesh.example", "xmpp://mesh.example:5222", "[fe80::1%en0]:5222"} {
		t.Run(endpoint, func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Fatal("invalid endpoint panicked")
				}
			}()
			// The shared helper fails the test on construction errors, so construct
			// directly here to assert the closed error vocabulary.
			client := newIdentityClient(t, &identityDialer{sessions: []Session{newIdentitySession("proof")}})
			config := client.config
			config.Endpoint = endpoint
			if _, err := NewClient(config, client.dialer, client.outbox, nil, nil); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
