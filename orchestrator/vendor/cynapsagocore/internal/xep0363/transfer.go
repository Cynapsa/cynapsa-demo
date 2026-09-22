package xep0363

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

var reservedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"), netip.MustParsePrefix("::1/128"), netip.MustParsePrefix("100::/64"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2001:10::/28"), netip.MustParsePrefix("fc00::/7"), netip.MustParsePrefix("fe80::/10"), netip.MustParsePrefix("ff00::/8"),
}

func (c *Client) validateURL(raw string) error {
	if c == nil || len(raw) == 0 || len(raw) > c.policy.MaximumURLBytes {
		return ErrInvalidReference
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Host == "" || u.Fragment != "" || u.Opaque != "" {
		return ErrInvalidReference
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" || !asciiHostname(host) || !c.policy.AllowedHosts[host] {
		return ErrInvalidReference
	}
	port := u.Port()
	if u.Scheme == "https" {
		if port == "" {
			port = "443"
		}
	} else if u.Scheme == "http" && c.policy.AllowHTTPHosts[host] {
		if port == "" {
			port = "80"
		}
	} else {
		return ErrInvalidReference
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 || !c.policy.AllowedPorts[uint16(portNumber)] {
		return ErrInvalidReference
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if err := c.validateIP(host, ip.Unmap()); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) validateIP(host string, ip netip.Addr) error {
	if c.policy.AllowPrivateHosts[host] {
		return nil
	}
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return ErrUnsafeAddress
	}
	for _, prefix := range reservedPrefixes {
		if prefix.Contains(ip) {
			return ErrUnsafeAddress
		}
	}
	return nil
}

func (c *Client) resolveAndValidate(ctx context.Context, host string) ([]net.IPAddr, error) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	values, err := c.resolver.LookupIPAddr(ctx, host)
	if err != nil || len(values) == 0 || len(values) > 16 {
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return nil, contextErr
		}
		return nil, ErrDNS
	}
	for _, value := range values {
		address, ok := netip.AddrFromSlice(value.IP)
		if !ok || c.validateIP(host, address.Unmap()) != nil {
			return nil, ErrUnsafeAddress
		}
	}
	return values, nil
}

func (c *Client) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, ErrInvalidReference
	}
	values, err := c.resolveAndValidate(ctx, host)
	if err != nil {
		return nil, err
	}
	var failures []error
	for _, value := range values {
		conn, err := c.dialer.DialContext(ctx, network, net.JoinHostPort(value.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return nil, contextErr
		}
		failures = append(failures, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	_ = failures
	return nil, ErrDNS
}

func canonicalHeader(value string) string { return strings.ToLower(strings.TrimSpace(value)) }
func containsUnsafe(value string) bool    { return strings.ContainsAny(value, "\x00\r\n") }
func asciiHostname(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c > 0x7f || !(c == '.' || c == '-' || c == ':' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}
