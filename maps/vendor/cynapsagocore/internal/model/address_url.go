package model

import (
	"net/url"
	"strconv"
	"strings"
)

const MaximumAddressURLBytes = 8192

// AddressURL is the canonical process-private representation of one public
// HTTP address. Public mapping results retain the separately stored spelling.
type AddressURL struct {
	lookupKey string
	path      string
	query     string
}

func (address AddressURL) LookupKey() string { return address.lookupKey }
func (address AddressURL) Path() string      { return address.path }
func (address AddressURL) Query() string     { return address.query }

func ParseAddressOrigin(input string) (AddressURL, error) {
	return parseAddressURL(input, true)
}

func ParseAddressURL(input string) (AddressURL, error) {
	return parseAddressURL(input, false)
}

func parseAddressURL(input string, originOnly bool) (AddressURL, error) {
	if input == "" || len(input) > MaximumAddressURLBytes || strings.TrimSpace(input) != input {
		return AddressURL{}, ErrInvalidAddressURL
	}
	parsed, err := url.Parse(input)
	if err != nil || parsed.User != nil || parsed.Opaque != "" || parsed.Fragment != "" || parsed.Host == "" || (originOnly && (parsed.Path != "" || parsed.RawQuery != "")) {
		return AddressURL{}, ErrInvalidAddressURL
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return AddressURL{}, ErrInvalidAddressURL
	}
	hostname := parsed.Hostname()
	if !validASCIIDNSHostname(hostname) || strings.HasSuffix(parsed.Host, ":") || strings.ContainsAny(parsed.Host, "[]") {
		return AddressURL{}, ErrInvalidAddressURL
	}
	portText := parsed.Port()
	port := uint64(0)
	if portText != "" {
		if !decimalAddressPort(portText) {
			return AddressURL{}, ErrInvalidAddressURL
		}
		port, err = strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return AddressURL{}, ErrInvalidAddressURL
		}
	}
	hostname = strings.ToLower(hostname)
	key := scheme + "://" + hostname
	if port != 0 && !((scheme == "http" && port == 80) || (scheme == "https" && port == 443)) {
		key += ":" + strconv.FormatUint(port, 10)
	}
	path := parsed.EscapedPath()
	if path == "" {
		path = "/"
	}
	return AddressURL{lookupKey: key, path: path, query: parsed.RawQuery}, nil
}

func validASCIIDNSHostname(hostname string) bool {
	if hostname == "" || len(hostname) > 253 || strings.HasSuffix(hostname, ".") {
		return false
	}
	for _, label := range strings.Split(hostname, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := range label {
			character := label[index]
			if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func decimalAddressPort(port string) bool {
	for index := range port {
		if port[index] < '0' || port[index] > '9' {
			return false
		}
	}
	return port != ""
}
