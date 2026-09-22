package model

import (
	"net"
	"strconv"
	"strings"
)

const MaximumMeshEndpointBytes = 4096

// MeshEndpoint is the canonical process-private representation of the V1
// application endpoint contract. Its zero value is invalid.
type MeshEndpoint struct {
	dialAddress string
	tlsName     string
}

func (endpoint MeshEndpoint) DialAddress() string { return endpoint.dialAddress }
func (endpoint MeshEndpoint) TLSName() string     { return endpoint.tlsName }

// ParseMeshEndpoint is the single transport-neutral V1 grammar. It performs
// no network lookup and never supplies a default port.
func ParseMeshEndpoint(input string) (MeshEndpoint, error) {
	if input == "" || len(input) > MaximumMeshEndpointBytes || strings.TrimSpace(input) != input || !meshEndpointASCII(input) || strings.ContainsAny(input, "@/?#") || strings.Contains(input, "://") {
		return MeshEndpoint{}, ErrInvalidMeshEndpoint
	}
	host, portText, err := net.SplitHostPort(input)
	if err != nil || host == "" || portText == "" || strings.Contains(host, "%") || !meshEndpointDecimal(portText) {
		return MeshEndpoint{}, ErrInvalidMeshEndpoint
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return MeshEndpoint{}, ErrInvalidMeshEndpoint
	}
	canonicalHost, ok := canonicalMeshEndpointHost(host, strings.HasPrefix(input, "["))
	if !ok {
		return MeshEndpoint{}, ErrInvalidMeshEndpoint
	}
	return MeshEndpoint{dialAddress: net.JoinHostPort(canonicalHost, strconv.FormatUint(port, 10)), tlsName: canonicalHost}, nil
}

func canonicalMeshEndpointHost(host string, bracketed bool) (string, bool) {
	if strings.HasSuffix(host, ".") {
		return "", false
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			if bracketed {
				return "", false
			}
			return v4.String(), true
		}
		if !bracketed {
			return "", false
		}
		return ip.String(), true
	}
	if bracketed || len(host) > 253 {
		return "", false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", false
		}
		for index := 0; index < len(label); index++ {
			value := label[index]
			if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '-') {
				return "", false
			}
		}
	}
	return strings.ToLower(host), true
}

func meshEndpointASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func meshEndpointDecimal(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return value != ""
}
