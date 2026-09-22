package xep0363

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSClientFreezesTLS13AndPreservesVerification(t *testing.T) {
	server := newTLSServer(t, tls.VersionTLS13, func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.TLS.Version != tls.VersionTLS13 {
			t.Errorf("negotiated TLS version = %#v", r.TLS)
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer server.Close()
	caller := &tls.Config{RootCAs: rootsForServer(t, server), MaxVersion: tls.VersionTLS13}
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{}, TLSConfig: caller})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if caller.MinVersion != 0 || caller.MaxVersion != tls.VersionTLS13 || caller.ServerName != "" || caller.InsecureSkipVerify {
		t.Fatalf("caller TLS config mutated: %#v", caller)
	}
	transport := client.http.Transport.(*http.Transport)
	if transport.TLSClientConfig == caller || transport.TLSClientConfig.MinVersion != tls.VersionTLS13 || transport.TLSClientConfig.MaxVersion != tls.VersionTLS13 || transport.TLSClientConfig.RootCAs != caller.RootCAs || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("hardened TLS config = %#v", transport.TLSClientConfig)
	}
	if err = client.ProbeDownloadReference(context.Background(), server.URL+"/object"); err != nil {
		t.Fatalf("TLS 1.3 probe = %v", err)
	}
}

func TestHTTPSClientRejectsTLS12OnlyService(t *testing.T) {
	server := newTLSServer(t, tls.VersionTLS12, func(http.ResponseWriter, *http.Request) {})
	defer server.Close()
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{}, TLSConfig: &tls.Config{RootCAs: rootsForServer(t, server)}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err = client.ProbeDownloadReference(context.Background(), server.URL+"/object"); !errors.Is(err, ErrTransfer) {
		t.Fatalf("TLS 1.2-only probe error = %v", err)
	}
}

func TestHTTPSClientRejectsInsecureOrUnsupportedTLSConfig(t *testing.T) {
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	tests := []*tls.Config{
		{InsecureSkipVerify: true}, //nolint:gosec -- rejection test
		{ServerName: "substituted.example"},
		{MaxVersion: tls.VersionTLS12},
		{MaxVersion: tls.VersionTLS13 + 1},
		{MaxVersion: ^uint16(0)},
		{MinVersion: tls.VersionTLS13 + 1},
	}
	for _, config := range tests {
		if _, err := NewClient(Config{Policy: policy, Slots: staticSlots{}, TLSConfig: config}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %#v error = %v", config, err)
		}
	}
	for _, minimum := range []uint16{0, tls.VersionTLS12, tls.VersionTLS13} {
		config, err := hardenedTLSConfig(&tls.Config{MinVersion: minimum})
		if err != nil || config.MinVersion != tls.VersionTLS13 {
			t.Fatalf("minimum %x = (%#v, %v)", minimum, config, err)
		}
	}
}

func TestHardenedTLSConfigMinMaxCrossProductAndCallerClone(t *testing.T) {
	minimums := []uint16{0, tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13, tls.VersionTLS13 + 1, ^uint16(0)}
	maximums := []uint16{0, tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13, tls.VersionTLS13 + 1, ^uint16(0)}
	for _, minimum := range minimums {
		for _, maximum := range maximums {
			t.Run(fmt.Sprintf("min_%04x_max_%04x", minimum, maximum), func(t *testing.T) {
				caller := &tls.Config{MinVersion: minimum, MaxVersion: maximum}
				config, err := hardenedTLSConfig(caller)
				wantAccepted := minimum <= tls.VersionTLS13 && (maximum == 0 || maximum == tls.VersionTLS13)
				if !wantAccepted {
					if !errors.Is(err, ErrInvalidConfig) || config != nil {
						t.Fatalf("config=%#v err=%v", config, err)
					}
					if caller.MinVersion != minimum || caller.MaxVersion != maximum {
						t.Fatalf("rejected caller mutated: %#v", caller)
					}
					return
				}
				if err != nil || config == nil || config == caller || config.MinVersion != tls.VersionTLS13 || config.MaxVersion != maximum {
					t.Fatalf("config=%#v err=%v", config, err)
				}
				caller.MinVersion = 0
				caller.MaxVersion = tls.VersionTLS12
				if config.MinVersion != tls.VersionTLS13 || config.MaxVersion != maximum {
					t.Fatalf("derived config aliases caller: %#v", config)
				}
			})
		}
	}
}

type controlledRoundTripper struct {
	entered chan *http.Request
	release chan struct{}
	err     error
}

func (transport *controlledRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.entered <- request
	<-transport.release
	if transport.err != nil {
		return nil, transport.err
	}
	return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
}

func TestPreparedUploadTerminallyOwnsAndClearsSlotSecrets(t *testing.T) {
	const authorization = "Bearer terminal-secret-canary"
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	headers := []Header{{Name: "authorization", Value: authorization}, {Name: "cookie", Value: "session=secret"}, {Name: "expires", Value: "soon"}}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: "https://objects.example/put-secret", GetURL: "https://objects.example/get-secret", PutHeaders: headers}}})
	if err != nil {
		t.Fatal(err)
	}
	transport := &controlledRoundTripper{entered: make(chan *http.Request, 1), release: make(chan struct{})}
	client.http = &http.Client{Transport: transport}
	preparedValue, err := client.PrepareUpload(context.Background(), int64(len("payload")))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedValue.(*preparedUpload)
	headers[0].Value = "caller-mutated"
	if prepared.slot.PutHeaders[0].Value != authorization {
		t.Fatal("prepared slot aliases requester header storage")
	}
	reference := prepared.DownloadReference()
	commitResult := make(chan error, 1)
	go func() { commitResult <- prepared.Commit(context.Background(), []byte("payload")) }()
	request := <-transport.entered
	if request.Header.Get("Authorization") != authorization {
		t.Fatalf("authorization header = %q", request.Header.Get("Authorization"))
	}
	prepared.mu.Lock()
	if prepared.slot.PutURL != "" || prepared.slot.GetURL != "" || prepared.slot.PutHeaders != nil {
		t.Fatalf("prepared upload retained slot during commit: %#v", prepared.slot)
	}
	prepared.mu.Unlock()
	abortDone := make(chan struct{})
	go func() { prepared.Abort(); close(abortDone) }()
	select {
	case <-abortDone:
		t.Fatal("Abort returned before the commit owner retired")
	case <-time.After(10 * time.Millisecond):
	}
	close(transport.release)
	<-abortDone
	if err = <-commitResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("commit after abort = %v", err)
	}
	if reference != "https://objects.example/get-secret" {
		t.Fatalf("caller-owned reference changed: %q", reference)
	}
	if headers[0].Value != "caller-mutated" {
		t.Fatalf("terminal cleanup mutated caller-owned header storage: %#v", headers)
	}
	if len(request.Header) != 0 || request.URL.String() != "" || request.Host != "" || request.Body != nil || request.GetBody != nil {
		t.Fatalf("terminal request retained private material: url=%q headers=%v host=%q", request.URL, request.Header, request.Host)
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.state != 2 || prepared.cancel != nil || !slotIsClear(prepared.slot) {
		t.Fatalf("terminal upload state = %#v", prepared)
	}
}

func TestPreparedUploadClearsSecretsOnAbortValidationAndDependencyError(t *testing.T) {
	const canary = "PRIVATE_SLOT_AUTHORIZATION_CANARY"
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: "https://objects.example/put", GetURL: "https://objects.example/get", PutHeaders: []Header{{Name: "authorization", Value: canary}}}}})
	if err != nil {
		t.Fatal(err)
	}
	abortedValue, err := client.PrepareUpload(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	aborted := abortedValue.(*preparedUpload)
	reference := aborted.DownloadReference()
	aborted.Abort()
	if reference == "" || aborted.DownloadReference() != "" || !slotIsClear(aborted.slot) {
		t.Fatalf("abort ownership = reference %q slot %#v", reference, aborted.slot)
	}
	cancelledValue, err := client.PrepareUpload(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := cancelledValue.(*preparedUpload)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err = cancelled.Commit(cancelledContext, []byte("payload")); !errors.Is(err, context.Canceled) || !slotIsClear(cancelled.slot) || cancelled.state != 2 {
		t.Fatalf("cancelled commit = %v, slot=%#v state=%d", err, cancelled.slot, cancelled.state)
	}
	invalidValue, err := client.PrepareUpload(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	invalid := invalidValue.(*preparedUpload)
	if err = invalid.Commit(context.Background(), []byte("short")); !errors.Is(err, ErrSize) || !slotIsClear(invalid.slot) || invalid.state != 2 {
		t.Fatalf("invalid commit = %v, slot=%#v state=%d", err, invalid.slot, invalid.state)
	}
	failingValue, err := client.PrepareUpload(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	failing := failingValue.(*preparedUpload)
	transport := &controlledRoundTripper{entered: make(chan *http.Request, 1), release: make(chan struct{}), err: errors.New(canary)}
	client.http = &http.Client{Transport: transport}
	result := make(chan error, 1)
	go func() { result <- failing.Commit(context.Background(), []byte("payload")) }()
	request := <-transport.entered
	close(transport.release)
	err = <-result
	if !errors.Is(err, ErrTransfer) || strings.Contains(err.Error(), canary) || !slotIsClear(failing.slot) || len(request.Header) != 0 || request.URL.String() != "" {
		t.Fatalf("dependency error=%v slot=%#v request=%#v", err, failing.slot, request)
	}
}

func newTLSServer(t *testing.T, version uint16, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: version, MaxVersion: version}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	return server
}

func rootsForServer(t *testing.T, server *httptest.Server) *x509.CertPool {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	return roots
}

func slotIsClear(slot Slot) bool {
	return slot.PutURL == "" && slot.GetURL == "" && slot.PutHeaders == nil
}

var _ http.RoundTripper = (*controlledRoundTripper)(nil)
