package rank1webrtc

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

// ExternalServiceDiscovery is the narrow authenticated Rank2 capability used
// to resolve private ICE configuration for the current exact server session.
type ExternalServiceDiscovery interface {
	DiscoverExternalServices(context.Context) (rank2xmpp.ExternalServiceProfile, error)
}

type externalServiceConfigurationSource struct {
	discovery ExternalServiceDiscovery
	clock     transport.Clock
	policy    webrtc.ICETransportPolicy
}

func NewExternalServiceConfigurationSource(discovery ExternalServiceDiscovery, clock transport.Clock, policy webrtc.ICETransportPolicy) (AuthenticatedICEConfigurationSource, error) {
	if discovery == nil || clock == nil || clock.Now().IsZero() || (policy != webrtc.ICETransportPolicyAll && policy != webrtc.ICETransportPolicyRelay) {
		return nil, transport.ErrInvalidConfig
	}
	return &externalServiceConfigurationSource{discovery: discovery, clock: clock, policy: policy}, nil
}

func DeploymentICETransportPolicy() (webrtc.ICETransportPolicy, error) {
	return webrtc.ICETransportPolicyAll, nil
}

func (source *externalServiceConfigurationSource) ResolveICEConfiguration(ctx context.Context) (ICEConfiguration, error) {
	if source == nil || source.discovery == nil || source.clock == nil || ctx == nil {
		return ICEConfiguration{}, transport.ErrUnavailable
	}
	profile, err := source.discovery.DiscoverExternalServices(ctx)
	if err != nil {
		profile.Clear()
		return ICEConfiguration{}, err
	}
	defer clearExternalServiceProfile(&profile)
	return mapExternalServiceProfile(profile, source.clock.Now().UTC(), source.policy)
}

func mapExternalServiceProfile(profile rank2xmpp.ExternalServiceProfile, now time.Time, policy webrtc.ICETransportPolicy) (ICEConfiguration, error) {
	if now.IsZero() || now.Location() != time.UTC || profile.Services == nil || !profile.ExpiresAt.IsZero() && !now.Before(profile.ExpiresAt) || (policy != webrtc.ICETransportPolicyAll && policy != webrtc.ICETransportPolicyRelay) {
		return ICEConfiguration{}, transport.ErrUnavailable
	}
	servers := make([]ICEServer, 0, len(profile.Services))
	var earliest time.Time
	hasTURN := false
	for _, service := range profile.Services {
		if service.Type != "stun" && service.Type != "stuns" && service.Type != "turn" && service.Type != "turns" || service.Host == "" || service.Port == 0 || service.Transport != "udp" && service.Transport != "tcp" || (service.Type == "stuns" || service.Type == "turns") && service.Transport != "tcp" {
			clearICEServers(servers)
			return ICEConfiguration{}, transport.ErrUnavailable
		}
		if service.Type == "stun" || service.Type == "stuns" {
			if service.Restricted || service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero() {
				clearICEServers(servers)
				return ICEConfiguration{}, transport.ErrUnavailable
			}
		} else if service.Restricted {
			hasTURN = true
			if service.Username == "" || len(service.Password) == 0 || service.ExpiresAt.IsZero() || service.ExpiresAt.Location() != time.UTC || !now.Before(service.ExpiresAt) {
				clearICEServers(servers)
				return ICEConfiguration{}, transport.ErrUnavailable
			}
		} else {
			hasTURN = true
			if service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero() {
				clearICEServers(servers)
				return ICEConfiguration{}, transport.ErrUnavailable
			}
		}
		if !service.ExpiresAt.IsZero() && (earliest.IsZero() || service.ExpiresAt.Before(earliest)) {
			earliest = service.ExpiresAt
		}
		hostPort := net.JoinHostPort(service.Host, strconv.Itoa(int(service.Port)))
		url := service.Type + ":" + hostPort
		if service.Type == "turn" || service.Type == "turns" {
			url += "?transport=" + service.Transport
		}
		server := ICEServer{URLs: []string{url}}
		if service.Restricted {
			server.Username = service.Username
			server.Credential = append([]byte(nil), service.Password...)
		}
		servers = append(servers, server)
	}
	if policy == webrtc.ICETransportPolicyRelay && !hasTURN {
		clearICEServers(servers)
		return ICEConfiguration{}, transport.ErrUnavailable
	}
	config := ICEConfiguration{Servers: servers, Policy: policy, ExpiresAt: profile.ExpiresAt}
	if !profile.ExpiresAt.Equal(earliest) || !validICEConfiguration(config, now) {
		clearICEServers(servers)
		return ICEConfiguration{}, transport.ErrUnavailable
	}
	return config, nil
}

func clearExternalServiceProfile(profile *rank2xmpp.ExternalServiceProfile) {
	profile.Clear()
}

var _ AuthenticatedICEConfigurationSource = (*externalServiceConfigurationSource)(nil)
