package rank1webrtc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/stun/v3"
	"github.com/pion/webrtc/v4"
)

// A blocked optional TURN stream must finish before the outer negotiation
// deadline so usable relay candidates can complete non-trickle gathering.
const pionTURNSetupTimeout = 5 * time.Second

var errPionTURNSetup = errors.New("TURN stream setup unavailable")

type pionTURNRoute struct {
	tls        bool
	serverName string
}

// Only validated endpoint and scheme information is retained here.
// Credentials remain exclusively in the existing Core-to-Pion boundary.
func pionTURNRoutes(servers []webrtc.ICEServer) (map[string]pionTURNRoute, error) {
	if len(servers) > maximumPrivateServers {
		return nil, errPionTURNSetup
	}
	routes := make(map[string]pionTURNRoute)
	for _, server := range servers {
		if len(server.URLs) > maximumPrivateURLs {
			return nil, errPionTURNSetup
		}
		for _, raw := range server.URLs {
			if raw == "" || len(raw) > maximumPrivateFieldBytes || strings.ContainsAny(raw, "@%\r\n\t ") {
				return nil, errPionTURNSetup
			}
			parsed, err := url.Parse(raw)
			if err != nil || parsed.User != nil || parsed.Fragment != "" || parsed.Path != "" {
				return nil, errPionTURNSetup
			}
			query, err := url.ParseQuery(parsed.RawQuery)
			if err != nil || len(query) > 1 {
				return nil, errPionTURNSetup
			}
			for key, values := range query {
				if key != "transport" || len(values) != 1 || (values[0] != "udp" && values[0] != "tcp") {
					return nil, errPionTURNSetup
				}
			}
			uri, err := stun.ParseURI(raw)
			if err != nil || uri.Port < 1 || uri.Port > 65535 {
				return nil, errPionTURNSetup
			}
			if (uri.Scheme != stun.SchemeTypeTURN && uri.Scheme != stun.SchemeTypeTURNS) || uri.Proto != stun.ProtoTypeTCP {
				continue
			}
			host := strings.ToLower(uri.Host)
			address := net.JoinHostPort(host, strconv.Itoa(uri.Port))
			route := pionTURNRoute{tls: uri.Scheme == stun.SchemeTypeTURNS, serverName: host}
			if existing, found := routes[address]; found && existing != route {
				return nil, errPionTURNSetup
			}
			routes[address] = route
		}
	}
	return routes, nil
}

// Pion's proxy hook receives the original URI host and port before DNS
// resolution, but omits its scheme and skips Pion's native TLS branch. This
// private dialer owns both bounded TCP setup and verified TLS setup for TURNS.
type pionTURNDialer struct {
	mu           sync.Mutex
	operation    context.Context
	generation   uint64
	routes       map[string]pionTURNRoute
	dialContext  func(context.Context, string, string) (net.Conn, error)
	setupTimeout time.Duration
	roots        *x509.CertPool // nil uses system roots; synthetic roots are test-only.
}

func newPionTURNDialer() *pionTURNDialer {
	return &pionTURNDialer{dialContext: (&net.Dialer{}).DialContext, setupTimeout: pionTURNSetupTimeout}
}

// Open and restart are serialized by the adapter. Every operation installs a
// fresh context; successful sockets never retain its cancellation or deadline.
// A nil route argument retains the last validated route table for ICE restart.
func (d *pionTURNDialer) begin(ctx context.Context, routes map[string]pionTURNRoute) func() {
	operation, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	d.generation++
	generation := d.generation
	d.operation = operation
	if routes != nil {
		d.routes = routes
	}
	d.mu.Unlock()
	return func() {
		cancel()
		d.mu.Lock()
		if d.generation == generation {
			d.operation = nil
		}
		d.mu.Unlock()
	}
}

func (d *pionTURNDialer) Dial(network, address string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errPionTURNSetup
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errPionTURNSetup
	}
	d.mu.Lock()
	operation := d.operation
	route, allowed := d.routes[net.JoinHostPort(strings.ToLower(host), port)]
	d.mu.Unlock()
	if operation == nil || !allowed {
		return nil, errPionTURNSetup
	}
	setup, cancel := context.WithTimeout(operation, d.setupTimeout)
	defer cancel()
	if setup.Err() != nil {
		return nil, errPionTURNSetup
	}
	connection, err := d.dialContext(setup, network, address)
	if err != nil {
		return nil, errPionTURNSetup
	}
	if setup.Err() != nil {
		_ = connection.Close()
		return nil, errPionTURNSetup
	}
	if route.tls {
		secured := tls.Client(connection, &tls.Config{ServerName: route.serverName, RootCAs: d.roots, MinVersion: tls.VersionTLS12})
		if err = secured.HandshakeContext(setup); err != nil {
			_ = connection.Close()
			return nil, errPionTURNSetup
		}
		connection = secured
	}
	if setup.Err() != nil {
		_ = connection.Close()
		return nil, errPionTURNSetup
	}
	// DialContext and HandshakeContext only govern setup. Do not leave socket
	// deadlines or callbacks that close a healthy allocation after handoff.
	return connection, nil
}
