package xep0363

import (
	"testing"
	"time"
)

func FuzzAcceptanceURLAndHeaderValidation(f *testing.F) {
	client := &Client{policy: Policy{
		MaximumURLBytes:   4096,
		AllowedHosts:      map[string]bool{"objects.example": true, "127.0.0.1": true},
		AllowedPorts:      map[uint16]bool{443: true, 8443: true, 80: true},
		AllowPrivateHosts: map[string]bool{"127.0.0.1": true},
		AllowHTTPHosts:    map[string]bool{"127.0.0.1": true},
	}}
	for _, seed := range []string{
		"https://objects.example/blob",
		"https://objects.example:8443/blob?x=1",
		"http://127.0.0.1/blob",
		"https://user:secret@objects.example/blob",
		"file:///etc/passwd",
		"https://127.0.0.1/blob#fragment",
		"https://[::1]/blob",
	} {
		f.Add(seed, "authorization", "Bearer canary")
	}

	f.Fuzz(func(t *testing.T, rawURL, headerName, headerValue string) {
		_ = client.validateURL(rawURL)
		_ = validateHeaders([]Header{{Name: headerName, Value: headerValue}})
	})
}

func FuzzAcceptancePolicyLimits(f *testing.F) {
	f.Add(int64(64), 4096, int64(4096), int64(time.Second), 1)
	f.Add(int64(-1), -1, int64(-1), int64(-1), -1)
	f.Fuzz(func(t *testing.T, maximumBytes int64, maximumURLBytes int, maximumHeaders int64, timeoutNanos int64, redirects int) {
		policy := Policy{
			MaximumBytes:               maximumBytes,
			MaximumURLBytes:            maximumURLBytes,
			MaximumResponseHeaderBytes: maximumHeaders,
			RequestTimeout:             time.Duration(timeoutNanos),
			DialTimeout:                time.Duration(timeoutNanos),
			TLSHandshakeTimeout:        time.Duration(timeoutNanos),
			ResponseHeaderTimeout:      time.Duration(timeoutNanos),
			IdleConnTimeout:            time.Duration(timeoutNanos),
			MaximumRedirects:           redirects,
			AllowedHosts:               map[string]bool{"objects.example": true},
			AllowedPorts:               map[uint16]bool{443: true},
		}
		_ = policy.validate()
	})
}
