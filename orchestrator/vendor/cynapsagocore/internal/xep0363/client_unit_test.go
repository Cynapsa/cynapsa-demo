package xep0363

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type staticSlots struct {
	slot Slot
	err  error
}

func (s staticSlots) RequestSlot(context.Context, int64, string) (Slot, error) { return s.slot, s.err }

func testPolicy(t *testing.T, rawURL string) Policy {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, MaximumRedirects: 1, AllowedHosts: map[string]bool{u.Hostname(): true}, AllowedPorts: map[uint16]bool{uint16(port): true}, AllowPrivateHosts: map[string]bool{u.Hostname(): true}, AllowHTTPHosts: map[string]bool{u.Hostname(): true}}
}

func TestClientUploadDownloadBounded(t *testing.T) {
	var mu sync.Mutex
	var stored []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			value, _ := io.ReadAll(r.Body)
			mu.Lock()
			stored = append([]byte(nil), value...)
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			mu.Lock()
			value := append([]byte(nil), stored...)
			mu.Unlock()
			w.Header().Set("Content-Length", strconv.Itoa(len(value)))
			_, _ = w.Write(value)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{slot: Slot{PutURL: server.URL + "/blob", GetURL: server.URL + "/blob"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	input := []byte("ciphertext")
	reference, err := client.Upload(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] ^= 1
	output, err := client.Download(context.Background(), reference)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "ciphertext" {
		t.Fatalf("output %q", output)
	}
}

func TestPreparedUploadDoesNotWriteBeforeRecipientReadiness(t *testing.T) {
	var mu sync.Mutex
	putCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		putCount++
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{slot: Slot{PutURL: server.URL + "/put-token", GetURL: server.URL + "/get-token"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	prepared, err := client.PrepareUpload(context.Background(), int64(len("ciphertext")))
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.DownloadReference(); got != server.URL+"/get-token" {
		t.Fatalf("download reference = %q", got)
	}
	mu.Lock()
	before := putCount
	mu.Unlock()
	if before != 0 {
		t.Fatalf("prepare uploaded %d objects", before)
	}
	if err := prepared.Commit(context.Background(), []byte("ciphertext")); err != nil {
		t.Fatalf("commit = %v", err)
	}
	mu.Lock()
	after := putCount
	mu.Unlock()
	if after != 1 {
		t.Fatalf("commit uploads = %d", after)
	}
	if err := prepared.Commit(context.Background(), []byte("ciphertext")); !errors.Is(err, ErrRejected) {
		t.Fatalf("second commit = %v", err)
	}

	aborted, err := client.PrepareUpload(context.Background(), int64(len("ciphertext")))
	if err != nil {
		t.Fatal(err)
	}
	aborted.Abort()
	if err := aborted.Commit(context.Background(), []byte("ciphertext")); !errors.Is(err, ErrRejected) {
		t.Fatalf("commit after abort = %v", err)
	}
	cancelled, err := client.PrepareUpload(context.Background(), int64(len("ciphertext")))
	if err != nil {
		t.Fatal(err)
	}
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cancelled.Commit(cancelledContext, []byte("ciphertext")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled commit = %v", err)
	}
	mu.Lock()
	final := putCount
	mu.Unlock()
	if final != 1 {
		t.Fatalf("aborted slot uploaded: %d", final)
	}
}

func TestProbeDownloadReferenceTreatsBoundedHTTPResponseAsReachable(t *testing.T) {
	var mu sync.Mutex
	methods := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{slot: Slot{PutURL: server.URL + "/put", GetURL: server.URL + "/get"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.ProbeDownloadReference(context.Background(), server.URL+"/get"); err != nil {
		t.Fatalf("404 reachability probe = %v", err)
	}
	mu.Lock()
	got := append([]string(nil), methods...)
	mu.Unlock()
	if len(got) != 1 || got[0] != http.MethodHead {
		t.Fatalf("probe methods = %v", got)
	}
	if err := client.ProbeDownloadReference(context.Background(), "http://example.invalid:22/object"); err == nil {
		t.Fatal("unsafe authority was probed")
	}
}

func TestClientRejectsUnsafeURLsHeadersRedirectsAndSizes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
			return
		}
		if r.URL.Path == "/large" {
			w.Header().Set("Content-Length", "1000")
			return
		}
		_, _ = w.Write([]byte("x"))
	}))
	defer server.Close()
	policy := testPolicy(t, server.URL)
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: server.URL + "/redirect", GetURL: server.URL + "/target"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Upload(context.Background(), []byte("x")); !errors.Is(err, ErrTransfer) {
		t.Fatalf("put redirect %v", err)
	}
	if _, err := client.Download(context.Background(), server.URL+"/large"); !errors.Is(err, ErrSize) {
		t.Fatalf("large %v", err)
	}
	unsafe := []string{"file:///tmp/x", "https://user:pass@example.com/x", "https://127.0.0.1/x", "https://[::1]/x", "https://example.com:22/x", "https://example.com/x#fragment"}
	for _, raw := range unsafe {
		if err := client.validateURL(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	client.slots = staticSlots{slot: Slot{PutURL: server.URL, GetURL: server.URL, PutHeaders: []Header{{Name: "X-Secret", Value: "canary"}}}}
	if _, err := client.RequestSlot(context.Background(), 1, "application/octet-stream"); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("header %v", err)
	}
}

type fakeResolver struct {
	values []net.IPAddr
	err    error
}

func (f fakeResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return f.values, f.err
}

func TestClientRejectsMixedDNSAnswers(t *testing.T) {
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	resolver := fakeResolver{values: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("127.0.0.1")}}}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: "https://objects.example/a", GetURL: "https://objects.example/a"}}, Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.resolveAndValidate(context.Background(), "objects.example"); !errors.Is(err, ErrUnsafeAddress) {
		t.Fatalf("mixed DNS %v", err)
	}
}

func TestClientRejectsExcessiveDNSAnswers(t *testing.T) {
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	values := make([]net.IPAddr, 17)
	for i := range values {
		values[i] = net.IPAddr{IP: net.ParseIP("8.8.8.8")}
	}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: "https://objects.example/a", GetURL: "https://objects.example/a"}}, Resolver: fakeResolver{values: values}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.resolveAndValidate(context.Background(), "objects.example"); !errors.Is(err, ErrDNS) {
		t.Fatalf("answers %v", err)
	}
}

func TestResolverNormalizesWrappedDependencyCancellation(t *testing.T) {
	policy := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedHosts: map[string]bool{"objects.example": true}, AllowedPorts: map[uint16]bool{443: true}}
	resolver := fakeResolver{err: errors.Join(errors.New("PRIVATE_DNS_CANARY"), context.Canceled)}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{}, Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.resolveAndValidate(context.Background(), "objects.example")
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("resolver error %q", err)
	}
}

func TestClientTimeoutAndChangingLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			time.Sleep(100 * time.Millisecond)
			return
		}
		if r.URL.Path == "/partial-stall" {
			_, _ = w.Write([]byte("partial"))
			if flush, ok := w.(http.Flusher); ok {
				flush.Flush()
			}
			time.Sleep(100 * time.Millisecond)
			return
		}
		w.Header().Set("Content-Length", "5")
		_, _ = w.Write([]byte("abc"))
	}))
	defer server.Close()
	policy := testPolicy(t, server.URL)
	policy.RequestTimeout = 20 * time.Millisecond
	client, _ := NewClient(Config{Policy: policy, Slots: staticSlots{slot: Slot{PutURL: server.URL, GetURL: server.URL}}})
	defer client.Close()
	if _, err := client.Download(context.Background(), server.URL+"/stall"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("timeout %v", err)
	}
	if _, err := client.Download(context.Background(), server.URL+"/partial-stall"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("partial timeout %v", err)
	}
	callerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := client.Download(callerCtx, server.URL+"/stall"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline %v", err)
	}
	if _, err := client.Download(context.Background(), server.URL+"/short"); err == nil {
		t.Fatal("accepted truncated response")
	}
}

func TestClientHostPolicyIsFailClosedForEveryScheme(t *testing.T) {
	base := Policy{MaximumBytes: 64, MaximumURLBytes: 4096, MaximumResponseHeaderBytes: 4096, RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, IdleConnTimeout: time.Second, AllowedPorts: map[uint16]bool{80: true, 443: true}}
	bypass := base
	bypass.AllowHTTPHosts = map[string]bool{"bypass.example": true}
	if _, err := NewClient(Config{Policy: bypass, Slots: staticSlots{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("HTTP-only allowlist bypass: %v", err)
	}
	base.AllowedHosts = map[string]bool{"objects.example": true}
	client, err := NewClient(Config{Policy: base, Slots: staticSlots{slot: Slot{PutURL: "https://objects.example/a", GetURL: "https://objects.example/a"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, raw := range []string{"https://bypass.example/a", "http://bypass.example/a"} {
		if err := client.validateURL(raw); !errors.Is(err, ErrInvalidReference) {
			t.Fatalf("accepted unlisted host %s: %v", raw, err)
		}
	}
}

func TestRequestSlotNormalizesWrappedDependencyDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	client, err := NewClient(Config{Policy: testPolicy(t, server.URL), Slots: staticSlots{err: errors.Join(errors.New("PRIVATE_SLOT_URL_PASSWORD_TOKEN_CANARY"), context.DeadlineExceeded)}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.RequestSlot(context.Background(), 16, "application/octet-stream")
	if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("slot error %q", err)
	}
}

func TestHTTPContextClassificationRemovesDependencyCanary(t *testing.T) {
	for _, contextClass := range []error{context.Canceled, context.DeadlineExceeded} {
		err := classifyHTTPError(context.Background(), errors.Join(errors.New("PRIVATE_HTTP_CANARY"), contextClass))
		if !errors.Is(err, contextClass) || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("classified error %q", err)
		}
	}
}
