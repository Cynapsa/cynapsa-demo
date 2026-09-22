package doccontract

import (
	"os"
	"strings"
	"testing"
)

func TestTURNCredentialContainmentContract(t *testing.T) {
	external, err := os.ReadFile("../../internal/transport/rank2xmpp/external_services.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Password              []byte",
		"clear(profile.Services[i].Password)",
		"profile, queryErr := queryExternalServiceSession",
		"profile.clear()",
		"value.Password = \"\"",
		"completeExternalServiceCredential",
		"externalServiceSlicesOverlap",
	} {
		if !strings.Contains(string(external), required) {
			t.Fatalf("external service ownership missing %q", required)
		}
	}

	pion, err := os.ReadFile("../../internal/transport/rank1webrtc/pion_adapter.go")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := os.ReadFile("../../internal/transport/rank1webrtc/peer_connection.go")
	if err != nil {
		t.Fatal(err)
	}
	rank1Source := string(peer) + string(pion)
	for _, required := range []string{
		"Credential []byte",
		"Credential:     string(servers[i].Credential)",
		"cloned[i].Credential = servers[i].Credential",
		"clearPionICEServers",
		"Pion shallow-retains the handed ICEServers slice on success",
		"if !transferred",
	} {
		if !strings.Contains(rank1Source, required) {
			t.Fatalf("Pion credential boundary missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"strings.Clone(credential)",
		"strings.Clone(servers[i].Credential",
	} {
		if strings.Contains(string(pion), forbidden) {
			t.Fatalf("Pion credential boundary duplicates immutable secret via %q", forbidden)
		}
	}
}
