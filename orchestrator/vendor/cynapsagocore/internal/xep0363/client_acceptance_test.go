package xep0363

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestAcceptanceClientAdversarialStreamingRedirectAndSecretRedaction(t *testing.T) {
	var crossURL string
	cross := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("cross-authority"))
	}))
	defer cross.Close()
	crossURL = cross.URL

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/exact":
			_, _ = w.Write(bytes.Repeat([]byte{'x'}, 64))
		case "/chunk-over":
			w.(http.Flusher).Flush()
			_, _ = w.Write(bytes.Repeat([]byte{'x'}, 65))
		case "/truncate":
			w.Header().Set("Content-Length", "10")
			_, _ = w.Write([]byte("abc"))
		case "/stall":
			_, _ = w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/redirect-ok":
			http.Redirect(w, r, "/exact", http.StatusTemporaryRedirect)
		case "/redirect-cross":
			http.Redirect(w, r, crossURL+"/object-secret-canary", http.StatusTemporaryRedirect)
		case "/redirect-loop":
			http.Redirect(w, r, "/redirect-loop", http.StatusTemporaryRedirect)
		case "/upload-redirect":
			http.Redirect(w, r, "/exact", http.StatusTemporaryRedirect)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	policy := testPolicy(t, server.URL)
	policy.RequestTimeout = 40 * time.Millisecond
	policy.MaximumRedirects = 1
	slots := staticSlots{slot: Slot{
		PutURL:     server.URL + "/upload-redirect",
		GetURL:     server.URL + "/exact",
		PutHeaders: []Header{{Name: "authorization", Value: "Bearer upload-secret-canary"}},
	}}
	client, err := NewClient(Config{Policy: policy, Slots: slots})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, err := client.Download(context.Background(), server.URL+"/redirect-ok")
	if err != nil || len(value) != 64 {
		t.Fatalf("same-authority redirect: len=%d err=%v", len(value), err)
	}
	tests := map[string]struct {
		path string
		want error
	}{
		"actual stream over limit": {"/chunk-over", ErrSize},
		"truncated declared body":  {"/truncate", ErrTransfer},
		"stalled body":             {"/stall", ErrTimeout},
		"cross authority redirect": {"/redirect-cross", ErrTransfer},
		"redirect hop overflow":    {"/redirect-loop", ErrTransfer},
	}
	for name, test := range tests {
		_, gotErr := client.Download(context.Background(), server.URL+test.path)
		if !errors.Is(gotErr, test.want) {
			t.Errorf("%s: error=%v want=%v", name, gotErr, test.want)
		}
		if gotErr != nil && (strings.Contains(gotErr.Error(), "object-secret-canary") || strings.Contains(gotErr.Error(), "upload-secret-canary") || strings.Contains(gotErr.Error(), server.URL)) {
			t.Errorf("%s leaked private material: %v", name, gotErr)
		}
	}
	if _, err := client.Upload(context.Background(), []byte("ciphertext")); !errors.Is(err, ErrTransfer) {
		t.Fatalf("upload redirect accepted/classified unexpectedly: %v", err)
	}
}

func TestAcceptanceSlotDeadlinePreservesClassWithoutPrivateCanary(t *testing.T) {
	const canary = "https://user:password@private.invalid/slot?token=secret"
	policy := Policy{
		MaximumBytes:               64,
		MaximumURLBytes:            4096,
		MaximumResponseHeaderBytes: 4096,
		RequestTimeout:             time.Second,
		DialTimeout:                time.Second,
		TLSHandshakeTimeout:        time.Second,
		ResponseHeaderTimeout:      time.Second,
		IdleConnTimeout:            time.Second,
		AllowedHosts:               map[string]bool{"objects.example": true},
		AllowedPorts:               map[uint16]bool{443: true},
	}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{err: fmt.Errorf("slot service %s: %w", canary, context.DeadlineExceeded)}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.RequestSlot(context.Background(), 1, "application/octet-stream")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline class lost: %v", err)
	}
	if strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "token=secret") {
		t.Fatalf("private slot dependency error leaked: %v", err)
	}
}

func TestAcceptanceClientRejectsReservedAndPrivateAddressFamilies(t *testing.T) {
	policy := Policy{
		MaximumBytes:               64,
		MaximumURLBytes:            4096,
		MaximumResponseHeaderBytes: 4096,
		RequestTimeout:             time.Second,
		DialTimeout:                time.Second,
		TLSHandshakeTimeout:        time.Second,
		ResponseHeaderTimeout:      time.Second,
		IdleConnTimeout:            time.Second,
		AllowedHosts:               map[string]bool{"objects.example": true},
		AllowedPorts:               map[uint16]bool{443: true},
	}
	client, err := NewClient(Config{Policy: policy, Slots: staticSlots{}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	blocked := []string{
		"0.0.0.0", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.1.1",
		"172.16.0.1", "192.0.0.1", "192.0.2.1", "192.168.0.1", "198.18.0.1",
		"198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "255.255.255.255",
		"::", "::1", "100::1", "2001:db8::1", "fc00::1", "fe80::1", "ff00::1",
	}
	for _, raw := range blocked {
		if err := client.validateIP("objects.example", netip.MustParseAddr(raw).Unmap()); !errors.Is(err, ErrUnsafeAddress) {
			t.Errorf("address %s error=%v, want unsafe", raw, err)
		}
	}
	if err := client.validateIP("objects.example", netip.MustParseAddr("8.8.8.8")); err != nil {
		t.Fatalf("global unicast rejected: %v", err)
	}
	client.policy.AllowPrivateHosts = map[string]bool{"objects.example": true}
	if err := client.validateIP("objects.example", netip.MustParseAddr("127.0.0.1")); err != nil {
		t.Fatalf("exact explicit private-host grant rejected: %v", err)
	}
	if err := client.validateIP("other.example", netip.MustParseAddr("127.0.0.1")); !errors.Is(err, ErrUnsafeAddress) {
		t.Fatalf("private grant escaped exact host: %v", err)
	}
}
