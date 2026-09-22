package sdkboundary

import (
	"context"
	"errors"
	"strconv"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestAuthenticationCanonicalizesMeshEndpointBeforeAdmission(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	command := v1.AuthConnectCommand{
		CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"},
		Auth:        v1.AuthInput{MeshEndpoint: "Mesh.Example.TEST:05222", Username: "agent@example.test", Password: "secret", MeshID: "mesh-one"},
	}
	mapped, err := adapter.DecodeCommand(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	mapped = takeFrozenCommandForTest(t, mapped)
	args, ok := mapped.Args.(model.AuthArgs)
	if !ok || args.MeshEndpoint != "mesh.example.test:5222" {
		t.Fatalf("mapped args = %#v", mapped.Args)
	}
}

func TestAuthenticationRejectsInvalidMeshEndpointBeforeAdmission(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"mesh.example.test", "xmpp://mesh.example.test:5222", "user@mesh.example.test:5222", "[fe80::1%en0]:5222"} {
		command := v1.AuthConnectCommand{
			CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"},
			Auth:        v1.AuthInput{MeshEndpoint: endpoint, Username: "agent@example.test", Password: "secret", MeshID: "mesh-one"},
		}
		if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("typed endpoint %q error = %v", endpoint, err)
		}
		input := `{"abi_version":1,"command_id":"command","command_name":"auth.connect","sdk_session_id":"session","args":{"mesh_endpoint":` + strconv.Quote(endpoint) + `,"username":"agent@example.test","password":"secret","mesh_id":"mesh-one","agent_instance_id":""}}`
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("ABI endpoint %q error = %v", endpoint, err)
		}
	}
}
