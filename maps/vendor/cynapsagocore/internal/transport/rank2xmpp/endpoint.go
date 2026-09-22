package rank2xmpp

import (
	"crypto/tls"
	"net"
	"strings"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Endpoint preserves Pod 4's private endpoint API while delegating the V1
// grammar and canonicalization to the boundary-neutral model contract.
type Endpoint struct{ value model.MeshEndpoint }

func ParseEndpoint(input string) (Endpoint, error) {
	value, err := model.ParseMeshEndpoint(input)
	if err != nil {
		return Endpoint{}, ErrInvalidConfig
	}
	return Endpoint{value: value}, nil
}

func (endpoint Endpoint) DialAddress() string   { return endpoint.value.DialAddress() }
func (endpoint Endpoint) TLSServerName() string { return endpoint.value.TLSName() }

func tlsConfigForEndpoint(endpoint Endpoint, configured *tls.Config) (*tls.Config, error) {
	if endpoint.DialAddress() == "" || endpoint.TLSServerName() == "" || configured == nil || configured.InsecureSkipVerify || configured.MinVersion > tls.VersionTLS13 || configured.MaxVersion != 0 && configured.MaxVersion != tls.VersionTLS13 {
		return nil, ErrInvalidConfig
	}
	clone := configured.Clone()
	if clone.MinVersion < tls.VersionTLS13 {
		clone.MinVersion = tls.VersionTLS13
	}
	if clone.ServerName != "" && !sameServerName(clone.ServerName, endpoint.TLSServerName()) {
		return nil, ErrInvalidConfig
	}
	clone.ServerName = endpoint.TLSServerName()
	return clone, nil
}

func sameServerName(configured, expected string) bool {
	configuredIP := net.ParseIP(configured)
	expectedIP := net.ParseIP(expected)
	if configuredIP != nil || expectedIP != nil {
		return configuredIP != nil && expectedIP != nil && configuredIP.Equal(expectedIP)
	}
	return strings.EqualFold(configured, expected)
}
