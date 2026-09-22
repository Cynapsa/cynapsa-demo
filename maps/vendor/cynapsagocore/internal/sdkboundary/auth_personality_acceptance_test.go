package sdkboundary

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestAuthenticationCommandsOwnCredentialsAndSelectDistinctPersonalities(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	authJSON := `{"mesh_endpoint":"mesh.example.test:5222","username":"agent@example.test","password":"do-not-echo","mesh_id":"mesh-one","agent_instance_id":"instance-one"}`
	tests := []struct {
		name v1.CommandName
		kind any
	}{
		{name: v1.CommandAuthLogin, kind: v1.AuthLoginCommand{}},
		{name: v1.CommandAuthConnect, kind: v1.AuthConnectCommand{}},
	}
	for _, test := range tests {
		input := `{"abi_version":1,"command_id":"command","command_name":"` + string(test.name) + `","sdk_session_id":"session","args":` + authJSON + `}`
		public, err := adapter.DecodeABICommand([]byte(input))
		if err != nil {
			t.Fatalf("decode %s: %v", test.name, err)
		}
		if reflect.TypeOf(public) != reflect.TypeOf(test.kind) || public.Name() != test.name {
			t.Fatalf("%s decoded as %T (%s)", test.name, public, public.Name())
		}
		private, err := adapter.DecodeCommand(context.Background(), public)
		if err != nil {
			t.Fatalf("map %s: %v", test.name, err)
		}
		private = takeFrozenCommandForTest(t, private)
		args, ok := private.Args.(model.AuthArgs)
		if !ok || args.MeshEndpoint != "mesh.example.test:5222" || args.Username != "agent@example.test" || string(args.Password) != "do-not-echo" || args.MeshID != "mesh-one" || args.AgentInstanceID != "instance-one" {
			t.Fatalf("%s private args = %#v", test.name, private.Args)
		}
	}

	encoded, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command", OK: true, Result: v1.AuthResult{AgentID: "agent@example.test", MeshID: "mesh-one", AgentInstanceID: "instance-one", Personality: v1.SDKPersonalityNative}})
	if err != nil {
		t.Fatalf("encode auth result: %v", err)
	}
	if strings.Contains(string(encoded), "do-not-echo") || strings.Contains(string(encoded), "password") || strings.Contains(string(encoded), "mesh.example.test") {
		t.Fatalf("authentication input escaped in result: %s", encoded)
	}
}

func TestAuthenticationRejectsMalformedMeshEndpointThroughTypedAndABIBoundaries(t *testing.T) {
	t.Parallel()
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	malformed := []string{
		"",
		"mesh.example.test",
		"mesh.example.test:",
		":5222",
		"mesh.example.test:0",
		"mesh.example.test:65536",
		"mesh.example.test:+5222",
		"mesh.example.test:port",
		"xmpp://mesh.example.test:5222",
		"agent@mesh.example.test:5222",
		"mesh.example.test:5222/path",
		"mesh.example.test:5222?query",
		"mesh.example.test:5222#fragment",
		" mesh.example.test:5222",
		"mesh.example.test:5222 ",
		"mesh.example test:5222",
		"mesh.example.test:\n5222",
		"mésh.example.test:5222",
		"mesh.example.test.:5222",
		"mesh..example.test:5222",
		"-mesh.example.test:5222",
		"mesh-.example.test:5222",
		"mesh_example.test:5222",
		strings.Repeat("a", 64) + ".example.test:5222",
		strings.Repeat("a.", 127) + "a:5222",
		"[mesh.example.test]:5222",
		"[192.0.2.1]:5222",
		"2001:db8::1:5222",
		"[fe80::1%en0]:5222",
		"[2001:db8::1]5222",
	}
	commandNames := []v1.CommandName{v1.CommandAuthLogin, v1.CommandAuthConnect}

	for _, commandName := range commandNames {
		for _, endpoint := range malformed {
			t.Run(string(commandName)+"/"+strconv.Quote(endpoint), func(t *testing.T) {
				base := v1.CommandBase{CommandID: "command", SDKSessionID: "session"}
				auth := v1.AuthInput{
					MeshEndpoint:    endpoint,
					Username:        "agent@example.test",
					Password:        "secret",
					MeshID:          "mesh-one",
					AgentInstanceID: "instance-one",
				}
				var typed v1.Command
				if commandName == v1.CommandAuthLogin {
					typed = v1.AuthLoginCommand{CommandBase: base, Auth: auth}
				} else {
					typed = v1.AuthConnectCommand{CommandBase: base, Auth: auth}
				}
				if _, err := adapter.DecodeCommand(context.Background(), typed); !errors.Is(err, ErrMalformedInput) {
					t.Fatalf("typed endpoint %q error = %v", endpoint, err)
				}

				wire := `{"abi_version":1,"command_id":"command","command_name":` + strconv.Quote(string(commandName)) + `,"sdk_session_id":"session","args":{"mesh_endpoint":` + strconv.Quote(endpoint) + `,"username":"agent@example.test","password":"secret","mesh_id":"mesh-one","agent_instance_id":"instance-one"}}`
				if _, err := adapter.DecodeABICommand([]byte(wire)); !errors.Is(err, ErrMalformedInput) {
					t.Fatalf("ABI endpoint %q error = %v", endpoint, err)
				}
			})
		}
	}
}

func TestFrozenCatalogHasNoCredentialMutationOrPersonalityToggle(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	for _, forbidden := range []string{"mesh.credentials.put", "mesh.credentials.remove", "http.bridge.enable", "http.bridge.disable"} {
		for _, command := range adapter.PublicCapabilities().Commands {
			if string(command) == forbidden {
				t.Errorf("removed command %q remains in capability catalog", forbidden)
			}
		}
		input := `{"abi_version":1,"command_id":"command","command_name":"` + forbidden + `","sdk_session_id":"session","args":{}}`
		if _, err := adapter.DecodeABICommand([]byte(input)); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("removed command %q error = %v", forbidden, err)
		}
	}
}

func TestAddressMappingCannotOverrideAuthenticatedMesh(t *testing.T) {
	adapter, err := New()
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	valid := `{"abi_version":1,"command_id":"command","command_name":"address.map.put","sdk_session_id":"session","args":{"virtual_origin":"https://agent.example.test","recipient":"agent@example.test"}}`
	command, err := adapter.DecodeABICommand([]byte(valid))
	if err != nil {
		t.Fatalf("valid mapping: %v", err)
	}
	private, err := adapter.DecodeCommand(context.Background(), command)
	if err != nil {
		t.Fatalf("map valid mapping: %v", err)
	}
	private = takeFrozenCommandForTest(t, private)
	mapping := private.Args.(model.AddressPutArgs).Mapping
	data, err := json.Marshal(mapping)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "mesh") {
		t.Fatalf("address mapping contains mesh selector: %s", data)
	}

	withMesh := strings.Replace(valid, `"recipient":"agent@example.test"`, `"recipient":"agent@example.test","mesh_id":"mesh-two"`, 1)
	if _, err := adapter.DecodeABICommand([]byte(withMesh)); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("per-mapping mesh selector error = %v", err)
	}
}
