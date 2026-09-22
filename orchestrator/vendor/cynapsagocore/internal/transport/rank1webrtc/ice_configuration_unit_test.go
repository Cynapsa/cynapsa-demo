package rank1webrtc

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

func TestAuthenticatedExternalServicesMapToPrivateICEConfiguration(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	profile := rank2xmpp.ExternalServiceProfile{ExpiresAt: expires, Services: []rank2xmpp.ExternalService{
		{Type: "stun", Host: "2001:db8::1", Port: 3478, Transport: "udp"},
		{Type: "turn", Host: "turn.test", Port: 3478, Transport: "tcp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires},
	}}
	config, err := mapExternalServiceProfile(profile, now, webrtc.ICETransportPolicyAll)
	if err != nil {
		t.Fatal(err)
	}
	if config.Policy != webrtc.ICETransportPolicyAll || !config.ExpiresAt.Equal(expires) || len(config.Servers) != 2 || config.Servers[0].URLs[0] != "stun:[2001:db8::1]:3478" || config.Servers[1].URLs[0] != "turn:turn.test:3478?transport=tcp" || config.Servers[1].Username != "agent" || !bytes.Equal(config.Servers[1].Credential, []byte("secret")) {
		t.Fatalf("config=%#v", config)
	}
	if _, err = mapExternalServiceProfile(profile, expires, webrtc.ICETransportPolicyAll); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("expired mapping=%v", err)
	}
	empty, err := mapExternalServiceProfile(rank2xmpp.ExternalServiceProfile{Services: []rank2xmpp.ExternalService{}}, now, webrtc.ICETransportPolicyAll)
	if err != nil || len(empty.Servers) != 0 || empty.Policy != webrtc.ICETransportPolicyAll || !validICEConfiguration(empty, now) {
		t.Fatalf("empty authenticated mapping=%#v, %v", empty, err)
	}
	if _, err = mapExternalServiceProfile(rank2xmpp.ExternalServiceProfile{}, now, webrtc.ICETransportPolicyAll); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("nil/unresolved mapping=%v", err)
	}
}

func TestDeploymentICETransportPolicyIsAlwaysAll(t *testing.T) {
	for _, environmentValue := range []string{"", "all", "relay", "invalid"} {
		t.Run("environment "+environmentValue, func(t *testing.T) {
			t.Setenv("CYNAPSA_ICE_TRANSPORT_POLICY", environmentValue)
			policy, err := DeploymentICETransportPolicy()
			if err != nil || policy != webrtc.ICETransportPolicyAll {
				t.Fatalf("policy=%s err=%v", policy.String(), err)
			}
		})
	}
}

func TestAuthenticatedICEConfigurationStrictBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	valid := ICEConfiguration{Servers: []ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: []byte("secret")}}, Policy: webrtc.ICETransportPolicyAll, ExpiresAt: now.Add(time.Minute)}
	if !validICEConfiguration(valid, now) {
		t.Fatal("rejected valid authenticated ICE configuration")
	}
	for name, mutate := range map[string]func(*ICEConfiguration){
		"empty relay": func(config *ICEConfiguration) { config.Servers = nil; config.Policy = webrtc.ICETransportPolicyRelay },
		"relay without TURN": func(config *ICEConfiguration) {
			config.Servers[0] = ICEServer{URLs: []string{"stun:stun.test:3478"}}
			config.Policy = webrtc.ICETransportPolicyRelay
		},
		"expired":       func(config *ICEConfiguration) { config.ExpiresAt = now },
		"bad policy":    func(config *ICEConfiguration) { config.Policy = webrtc.ICETransportPolicyNoHost },
		"bad scheme":    func(config *ICEConfiguration) { config.Servers[0].URLs[0] = "https://turn.test:3478" },
		"bad transport": func(config *ICEConfiguration) { config.Servers[0].URLs[0] = "turn:turn.test:3478?transport=sctp" },
		"invalid auth":  func(config *ICEConfiguration) { config.Servers[0].Credential = []byte{0xff} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.Servers = cloneICEServers(valid.Servers)
			mutate(&candidate)
			if validICEConfiguration(candidate, now) {
				t.Fatal("accepted invalid authenticated ICE configuration")
			}
		})
	}
}

func TestAuthenticatedExternalServicesRejectMalformedPrivateProfile(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	valid := rank2xmpp.ExternalServiceProfile{Services: []rank2xmpp.ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}
	for name, mutate := range map[string]func(*rank2xmpp.ExternalServiceProfile){
		"missing credential": func(profile *rank2xmpp.ExternalServiceProfile) { profile.Services[0].Password = nil },
		"stale expiry": func(profile *rank2xmpp.ExternalServiceProfile) {
			profile.Services[0].ExpiresAt = now
			profile.ExpiresAt = now
		},
		"extended profile":  func(profile *rank2xmpp.ExternalServiceProfile) { profile.ExpiresAt = expires.Add(time.Minute) },
		"invalid transport": func(profile *rank2xmpp.ExternalServiceProfile) { profile.Services[0].Transport = "sctp" },
		"unrestricted auth": func(profile *rank2xmpp.ExternalServiceProfile) { profile.Services[0].Restricted = false },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.Services = append([]rank2xmpp.ExternalService(nil), valid.Services...)
			mutate(&candidate)
			if _, err := mapExternalServiceProfile(candidate, now, webrtc.ICETransportPolicyAll); !errors.Is(err, transport.ErrUnavailable) {
				t.Fatalf("malformed profile accepted: %v", err)
			}
		})
	}
}

func TestPionNetworkTypesDeclareTCPOnlyForReliableRelayTransport(t *testing.T) {
	udp := pionNetworkTypes([]webrtc.ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}}})
	if len(udp) != 2 || udp[0] != webrtc.NetworkTypeUDP4 || udp[1] != webrtc.NetworkTypeUDP6 {
		t.Fatalf("udp network types=%v", udp)
	}
	tcp := pionNetworkTypes([]webrtc.ICEServer{{URLs: []string{"turn:turn.test:3478?transport=tcp"}}})
	if len(tcp) != 4 || tcp[2] != webrtc.NetworkTypeTCP4 || tcp[3] != webrtc.NetworkTypeTCP6 {
		t.Fatalf("tcp network types=%v", tcp)
	}
	tls := pionNetworkTypes([]webrtc.ICEServer{{URLs: []string{"turns:turn.test:5349?transport=tcp"}}})
	if len(tls) != 4 || tls[2] != webrtc.NetworkTypeTCP4 || tls[3] != webrtc.NetworkTypeTCP6 {
		t.Fatalf("tls network types=%v", tls)
	}
}
