// Package xep0363 implements bounded private object transfer. V1 permits
// transport-protected plaintext; optional end-to-end encryption is layered by
// the carrier-neutral payload package.
package xep0363

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

var (
	ErrInvalidConfig    = errors.New("xep0363: invalid configuration")
	ErrInvalidSlot      = errors.New("xep0363: invalid upload slot")
	ErrSlotUnavailable  = errors.New("xep0363: slot unavailable")
	ErrInvalidReference = errors.New("xep0363: invalid object reference")
	ErrUnsafeAddress    = errors.New("xep0363: unsafe network address")
	ErrDNS              = errors.New("xep0363: name resolution failed")
	ErrTimeout          = errors.New("xep0363: transfer timed out")
	ErrRejected         = errors.New("xep0363: object service rejected transfer")
	ErrSize             = errors.New("xep0363: object size mismatch")
	ErrTransfer         = errors.New("xep0363: object transfer failed")
)

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type Policy struct {
	MaximumBytes               int64
	MaximumURLBytes            int
	MaximumResponseHeaderBytes int64
	RequestTimeout             time.Duration
	DialTimeout                time.Duration
	TLSHandshakeTimeout        time.Duration
	ResponseHeaderTimeout      time.Duration
	IdleConnTimeout            time.Duration
	MaximumRedirects           int
	AllowedHosts               map[string]bool
	AllowedPorts               map[uint16]bool
	AllowPrivateHosts          map[string]bool
	AllowHTTPHosts             map[string]bool
}

func (p Policy) validate() error {
	if p.MaximumBytes <= 0 || p.MaximumBytes > payload.MaximumTransferredBytes || p.MaximumURLBytes <= 0 || p.MaximumURLBytes > 16<<10 || p.MaximumResponseHeaderBytes <= 0 || p.MaximumResponseHeaderBytes > 1<<20 || p.RequestTimeout <= 0 || p.DialTimeout <= 0 || p.TLSHandshakeTimeout <= 0 || p.ResponseHeaderTimeout <= 0 || p.IdleConnTimeout <= 0 || p.MaximumRedirects < 0 || p.MaximumRedirects > 8 || len(p.AllowedHosts) > 256 || len(p.AllowHTTPHosts) > 256 || len(p.AllowPrivateHosts) > 256 || len(p.AllowedPorts) > 256 || !hasAllowedHost(p) || !hasAllowedPort(p.AllowedPorts) || !grantsWithinAllowed(p) {
		return ErrInvalidConfig
	}
	return nil
}

func grantsWithinAllowed(p Policy) bool {
	for host, allowed := range p.AllowHTTPHosts {
		canonical := strings.ToLower(strings.TrimSuffix(host, "."))
		if allowed && !p.AllowedHosts[host] && !p.AllowedHosts[canonical] {
			return false
		}
	}
	for host, allowed := range p.AllowPrivateHosts {
		canonical := strings.ToLower(strings.TrimSuffix(host, "."))
		if allowed && !p.AllowedHosts[host] && !p.AllowedHosts[canonical] {
			return false
		}
	}
	return true
}

func hasAllowedHost(p Policy) bool {
	for _, allowed := range p.AllowedHosts {
		if allowed {
			return true
		}
	}
	return false
}
func hasAllowedPort(values map[uint16]bool) bool {
	for port, allowed := range values {
		if port != 0 && allowed {
			return true
		}
	}
	return false
}

type Config struct {
	Policy    Policy
	Slots     SlotRequester
	Resolver  Resolver
	Dialer    *net.Dialer
	TLSConfig *tls.Config
}

// Client negotiates slots and transfers private object bytes.
type Client struct {
	policy   Policy
	slots    SlotRequester
	resolver Resolver
	dialer   *net.Dialer
	http     *http.Client
}

func NewClient(config Config) (*Client, error) {
	if err := config.Policy.validate(); err != nil || config.Slots == nil {
		return nil, ErrInvalidConfig
	}
	tlsConfig, err := hardenedTLSConfig(config.TLSConfig)
	if err != nil {
		return nil, err
	}
	if config.Resolver == nil {
		config.Resolver = net.DefaultResolver
	}
	if config.Dialer == nil {
		config.Dialer = &net.Dialer{Timeout: config.Policy.DialTimeout, KeepAlive: 30 * time.Second}
	}
	c := &Client{policy: clonePolicy(config.Policy), slots: config.Slots, resolver: config.Resolver, dialer: config.Dialer}
	transport := &http.Transport{Proxy: nil, DialContext: c.dialContext, TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, DisableCompression: true, MaxIdleConns: 16, MaxIdleConnsPerHost: 4, IdleConnTimeout: c.policy.IdleConnTimeout, TLSHandshakeTimeout: c.policy.TLSHandshakeTimeout, ResponseHeaderTimeout: c.policy.ResponseHeaderTimeout, MaxResponseHeaderBytes: c.policy.MaximumResponseHeaderBytes}
	c.http = &http.Client{Transport: transport, CheckRedirect: c.checkRedirect}
	return c, nil
}

func hardenedTLSConfig(config *tls.Config) (*tls.Config, error) {
	if config == nil {
		return &tls.Config{MinVersion: tls.VersionTLS13}, nil
	}
	if config.InsecureSkipVerify || config.ServerName != "" || config.MinVersion > tls.VersionTLS13 || config.MaxVersion != 0 && config.MaxVersion != tls.VersionTLS13 {
		return nil, ErrInvalidConfig
	}
	clone := config.Clone()
	if clone.MinVersion < tls.VersionTLS13 {
		clone.MinVersion = tls.VersionTLS13
	}
	return clone, nil
}

func (c *Client) Available(ctx context.Context) (bool, error) {
	if c == nil || ctx == nil {
		return false, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) Upload(ctx context.Context, transferred []byte) (string, error) {
	if c == nil || ctx == nil || len(transferred) == 0 || int64(len(transferred)) > c.policy.MaximumBytes {
		return "", ErrSize
	}
	prepared, err := c.PrepareUpload(ctx, int64(len(transferred)))
	if err != nil {
		return "", err
	}
	defer prepared.Abort()
	reference := prepared.DownloadReference()
	if err := prepared.Commit(ctx, transferred); err != nil {
		return "", err
	}
	return reference, nil
}

// PrepareUpload obtains and validates one XEP-0363 slot without sending any
// object bytes. The caller can therefore complete the authenticated recipient
// reachability exchange against DownloadReference before Commit performs PUT.
func (c *Client) PrepareUpload(ctx context.Context, size int64) (payload.PreparedObjectUpload, error) {
	if c == nil || ctx == nil || size <= 0 || size > c.policy.MaximumBytes {
		return nil, ErrSize
	}
	slot, err := c.RequestSlot(ctx, size, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	return &preparedUpload{client: c, slot: slot, size: size}, nil
}

type preparedUpload struct {
	client *Client
	slot   Slot
	size   int64

	mu     sync.Mutex
	state  uint8
	cancel context.CancelFunc
	done   chan struct{}
}

func (upload *preparedUpload) DownloadReference() string {
	if upload == nil {
		return ""
	}
	upload.mu.Lock()
	reference := strings.Clone(upload.slot.GetURL)
	upload.mu.Unlock()
	return reference
}

func (upload *preparedUpload) Commit(ctx context.Context, transferred []byte) error {
	if upload == nil || upload.client == nil {
		return ErrSize
	}
	upload.mu.Lock()
	if upload.state != 0 {
		upload.mu.Unlock()
		return ErrRejected
	}
	upload.state = 1
	slot := upload.slot
	upload.slot = Slot{}
	if ctx == nil || int64(len(transferred)) != upload.size {
		upload.state = 2
		upload.mu.Unlock()
		clearSlot(&slot)
		return ErrSize
	}
	operation, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	upload.cancel = cancel
	upload.done = done
	upload.mu.Unlock()

	defer func() {
		cancel()
		clearSlot(&slot)
		upload.mu.Lock()
		upload.cancel = nil
		upload.state = 2
		close(done)
		upload.mu.Unlock()
	}()
	_, err := upload.client.uploadSlot(operation, slot, transferred)
	return err
}

func (upload *preparedUpload) Abort() {
	if upload == nil {
		return
	}
	upload.mu.Lock()
	switch upload.state {
	case 0:
		upload.state = 2
		slot := upload.slot
		upload.slot = Slot{}
		upload.mu.Unlock()
		clearSlot(&slot)
		return
	case 1:
		cancel, done := upload.cancel, upload.done
		upload.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
		return
	}
	upload.mu.Unlock()
}

func (c *Client) uploadSlot(ctx context.Context, slot Slot, transferred []byte) (string, error) {
	operation, cancel, policyDeadline := c.requestOperation(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(operation, http.MethodPut, slot.PutURL, bytes.NewReader(transferred))
	if err != nil {
		return "", ErrInvalidSlot
	}
	defer clearHTTPRequest(req)
	req.ContentLength = int64(len(transferred))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	for _, header := range slot.PutHeaders {
		req.Header.Add(header.Name, header.Value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		closeErrorResponse(resp)
		return "", classifyHTTPRequestError(ctx, operation, err, policyDeadline)
	}
	defer resp.Body.Close()
	defer clearHTTPResponseRequest(resp)
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", ErrRejected
	}
	return slot.GetURL, nil
}

func clearHTTPRequest(request *http.Request) {
	if request == nil {
		return
	}
	for name, values := range request.Header {
		for index := range values {
			values[index] = ""
		}
		clear(values)
		delete(request.Header, name)
	}
	request.URL = &url.URL{}
	request.Host = ""
	request.Body = nil
	request.GetBody = nil
	request.Cancel = nil
	request.Response = nil
	request.TLS = nil
}

func clearHTTPResponseRequest(response *http.Response) {
	if response == nil {
		return
	}
	clearHTTPRequest(response.Request)
	response.Request = nil
}

func (c *Client) Download(ctx context.Context, reference string) ([]byte, error) {
	if c == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := c.validateURL(reference); err != nil {
		return nil, err
	}
	operation, cancel, policyDeadline := c.requestOperation(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(operation, http.MethodGet, reference, nil)
	if err != nil {
		return nil, ErrInvalidReference
	}
	defer clearHTTPRequest(req)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.http.Do(req)
	if err != nil {
		closeErrorResponse(resp)
		return nil, classifyHTTPRequestError(ctx, operation, err, policyDeadline)
	}
	defer resp.Body.Close()
	defer clearHTTPResponseRequest(resp)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, ErrRejected
	}
	declared := resp.ContentLength
	if declared > c.policy.MaximumBytes {
		return nil, ErrSize
	}
	reader := io.LimitReader(resp.Body, c.policy.MaximumBytes+1)
	value, err := io.ReadAll(reader)
	if err != nil {
		zero(value)
		if contextErr := dependencyContextError(operation, err); contextErr != nil {
			if errors.Is(contextErr, context.DeadlineExceeded) && policyDeadline && ctx.Err() == nil {
				return nil, errors.Join(ErrTimeout, context.DeadlineExceeded)
			}
			return nil, contextErr
		}
		return nil, ErrTransfer
	}
	if int64(len(value)) > c.policy.MaximumBytes || declared >= 0 && int64(len(value)) != declared {
		zero(value)
		return nil, ErrSize
	}
	return value, nil
}

// ProbeDownloadReference proves that this client can reach the exact validated
// download endpoint before the sender uploads payload bytes. Any bounded HTTP
// response proves reachability; status is deliberately not treated as object
// availability because a prepared object may not exist until after the probe.
func (c *Client) ProbeDownloadReference(ctx context.Context, reference string) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err := c.validateURL(reference); err != nil {
		return err
	}
	operation, cancel, policyDeadline := c.requestOperation(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(operation, http.MethodHead, reference, nil)
	if err != nil {
		return ErrInvalidReference
	}
	defer clearHTTPRequest(req)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.http.Do(req)
	if err != nil {
		closeErrorResponse(resp)
		return classifyHTTPRequestError(ctx, operation, err, policyDeadline)
	}
	defer clearHTTPResponseRequest(resp)
	if err := resp.Body.Close(); err != nil {
		return ErrTransfer
	}
	return nil
}

func (c *Client) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > c.policy.MaximumRedirects {
		return ErrInvalidReference
	}
	if len(via) > 0 && via[0].Method != http.MethodGet {
		return ErrInvalidReference
	}
	if err := c.validateURL(req.URL.String()); err != nil {
		return err
	}
	if len(via) > 0 {
		previous := via[len(via)-1].URL
		if !sameAuthority(previous, req.URL) {
			return ErrInvalidReference
		}
	}
	return nil
}

func (c *Client) Close() {
	if c != nil && c.http != nil {
		c.http.CloseIdleConnections()
	}
}

func closeErrorResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	clearHTTPResponseRequest(response)
}

func sameAuthority(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Hostname(), b.Hostname()) && effectivePort(a) == effectivePort(b)
}
func effectivePort(u *url.URL) string {
	if u.Port() != "" {
		return u.Port()
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}
func classifyHTTPRequestError(caller, operation context.Context, err error, policyDeadline bool) error {
	if caller != nil && caller.Err() != nil {
		return caller.Err()
	}
	if contextErr := dependencyContextError(operation, err); contextErr != nil {
		if errors.Is(contextErr, context.DeadlineExceeded) && policyDeadline {
			return errors.Join(ErrTimeout, context.DeadlineExceeded)
		}
		return contextErr
	}
	var value net.Error
	if errors.As(err, &value) && value.Timeout() {
		return ErrTimeout
	}
	return ErrTransfer
}

func classifyHTTPError(ctx context.Context, err error) error {
	return classifyHTTPRequestError(ctx, ctx, err, false)
}

func (c *Client) requestOperation(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	deadline := time.Now().Add(c.policy.RequestTimeout)
	if caller, ok := ctx.Deadline(); ok && !deadline.Before(caller) {
		operation, cancel := context.WithCancel(ctx)
		return operation, cancel, false
	}
	operation, cancel := context.WithDeadline(ctx, deadline)
	return operation, cancel, true
}

func dependencyContextError(ctx context.Context, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}
func clonePolicy(p Policy) Policy {
	p.AllowedHosts = cloneSet(p.AllowedHosts)
	p.AllowedPorts = clonePortSet(p.AllowedPorts)
	p.AllowPrivateHosts = cloneSet(p.AllowPrivateHosts)
	p.AllowHTTPHosts = cloneSet(p.AllowHTTPHosts)
	return p
}
func cloneSet(value map[string]bool) map[string]bool {
	out := make(map[string]bool, len(value))
	for k, v := range value {
		out[strings.ToLower(strings.TrimSuffix(k, "."))] = v
	}
	return out
}
func clonePortSet(value map[uint16]bool) map[uint16]bool {
	out := make(map[uint16]bool, len(value))
	for k, v := range value {
		out[k] = v
	}
	return out
}
func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

var _ payload.PreparedObjectStore = (*Client)(nil)
