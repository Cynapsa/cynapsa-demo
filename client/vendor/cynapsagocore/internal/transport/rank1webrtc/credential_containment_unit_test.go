package rank1webrtc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/pion/webrtc/v4"
)

type credentialErrorDiscovery struct {
	profile rank2xmpp.ExternalServiceProfile
	err     error
}

func (discovery credentialErrorDiscovery) DiscoverExternalServices(context.Context) (rank2xmpp.ExternalServiceProfile, error) {
	return discovery.profile, discovery.err
}

func TestPrivateICECredentialOwnershipAndClear(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	profilePassword := []byte("profile-secret")
	profile := rank2xmpp.ExternalServiceProfile{Services: []rank2xmpp.ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: profilePassword, ExpiresAt: expires}}, ExpiresAt: expires}
	config, err := mapExternalServiceProfile(profile, now, webrtc.ICETransportPolicyAll)
	if err != nil {
		t.Fatal(err)
	}
	configPassword := config.Servers[0].Credential
	if !bytes.Equal(configPassword, profilePassword) || &configPassword[0] == &profilePassword[0] {
		t.Fatal("ICE mapping aliased profile password storage")
	}
	profile.Clear()
	if !allZeroBytes(profilePassword) || !bytes.Equal(configPassword, []byte("profile-secret")) {
		t.Fatalf("profile clear=%x config=%q", profilePassword, configPassword)
	}
	clearICEServers(config.Servers)
	if !allZeroBytes(configPassword) {
		t.Fatalf("ICE clear retained password=%x", configPassword)
	}
}

func TestPionConfigurationClonesShareOneImmutableCredentialBacking(t *testing.T) {
	ownedPassword := []byte("one-boundary-secret")
	converted := pionICEServers([]ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: ownedPassword}})
	cloned := clonePionICEServers(converted)
	left, leftOK := converted[0].Credential.(string)
	right, rightOK := cloned[0].Credential.(string)
	if !leftOK || !rightOK || left != right || unsafe.StringData(left) != unsafe.StringData(right) {
		t.Fatal("Pion configuration clone duplicated credential backing")
	}
	clearPionICEServers(converted)
	clearPionICEServers(cloned)
	if !bytes.Equal(ownedPassword, []byte("one-boundary-secret")) {
		t.Fatal("Pion boundary mutated caller-owned password")
	}
}

func TestCredentialCleanupRunsAcrossPanic(t *testing.T) {
	password := []byte("panic-secret")
	servers := []ICEServer{{URLs: []string{"turn:turn.test:3478?transport=udp"}, Username: "agent", Credential: password}}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		defer clearICEServers(servers)
		pion := pionICEServers(servers)
		defer clearPionICEServers(pion)
		panic("injected Pion boundary panic")
	}()
	if !allZeroBytes(password) {
		t.Fatalf("panic retained Core-owned password=%x", password)
	}
}

func TestDiscoveryErrorClearsUnexpectedReturnedProfile(t *testing.T) {
	password := []byte("error-profile-secret")
	source, err := NewExternalServiceConfigurationSource(credentialErrorDiscovery{
		profile: rank2xmpp.ExternalServiceProfile{Services: []rank2xmpp.ExternalService{{Password: password}}},
		err:     transport.ErrUnavailable,
	}, transport.ClockFunc(func() time.Time { return time.Now().UTC() }), webrtc.ICETransportPolicyAll)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.ResolveICEConfiguration(context.Background()); !errors.Is(err, transport.ErrUnavailable) {
		t.Fatalf("ResolveICEConfiguration error=%v", err)
	}
	if !allZeroBytes(password) {
		t.Fatalf("discovery error retained password=%x", password)
	}
}

func TestPionReplacementRetainsTransferredLiveConfiguration(t *testing.T) {
	api := webrtc.NewAPI()
	initial := pionICEServers([]ICEServer{{URLs: []string{"turn:old.test:3478?transport=udp"}, Username: "old-user", Credential: []byte("old-secret")}})
	peer, err := newPionPeerWithConfiguration(api, initial, webrtc.ICETransportPolicyAll)
	clearPionICEServers(initial)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	refreshed := pionICEServers([]ICEServer{{URLs: []string{"turn:new.test:3478?transport=udp"}, Username: "new-user", Credential: []byte("new-secret")}})
	if err = setPionPeerConfiguration(peer, refreshed, webrtc.ICETransportPolicyAll); err != nil {
		t.Fatal(err)
	}
	clearPionICEServers(refreshed)

	configuration := peer.GetConfiguration()
	if len(configuration.ICEServers) != 1 || len(configuration.ICEServers[0].URLs) != 1 || configuration.ICEServers[0].URLs[0] != "turn:new.test:3478?transport=udp" || configuration.ICEServers[0].Username != "new-user" || configuration.ICEServers[0].Credential != "new-secret" {
		t.Fatalf("live Pion configuration was cleared after transfer: %#v", configuration.ICEServers)
	}
}
