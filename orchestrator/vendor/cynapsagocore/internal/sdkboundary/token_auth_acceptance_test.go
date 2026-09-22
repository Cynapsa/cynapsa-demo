package sdkboundary

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const boundaryEnrollmentToken = "cpsa_e1.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func validTokenAuthArgs() string {
	return `{"token":"` + boundaryEnrollmentToken + `","mesh_id":"mesh-one"}`
}

func tokenAuthEnvelope(name v1.CommandName, args string) []byte {
	return []byte(`{"abi_version":1,"command_id":"command","command_name":` + strconv.Quote(string(name)) + `,"sdk_session_id":"session","args":` + args + `}`)
}

func TestTokenAuthenticationTypedAndABIPathsOwnBareSecret(t *testing.T) {
	adapter, _ := New()
	for _, name := range []v1.CommandName{v1.CommandAuthTokenLogin, v1.CommandAuthTokenConnect} {
		input := tokenAuthEnvelope(name, validTokenAuthArgs())
		public, err := adapter.DecodeABICommand(input)
		if err != nil || public.Name() != name {
			t.Fatalf("public token decode: %v", err)
		}
		auth := v1.AuthTokenInput{Token: boundaryEnrollmentToken, MeshID: "mesh-one"}
		var pointer v1.Command = &v1.AuthTokenLoginCommand{CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"}, Auth: auth}
		if name == v1.CommandAuthTokenConnect {
			pointer = &v1.AuthTokenConnectCommand{CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"}, Auth: auth}
		}
		for _, path := range []string{"typed", "pointer", "abi"} {
			var frozen model.Command
			switch path {
			case "typed":
				frozen, err = adapter.DecodeCommand(context.Background(), public)
			case "pointer":
				frozen, err = adapter.DecodeCommand(context.Background(), pointer)
			case "abi":
				frozen, err = adapter.DecodeABIInternalCommand(input)
			}
			if err != nil {
				t.Fatalf("%s decode: %v", path, err)
			}
			internal := takeFrozenCommandForTest(t, frozen)
			args, ok := internal.Args.(model.TokenAuthArgs)
			if !ok || string(args.Token) != boundaryEnrollmentToken[5:] || args.MeshID != string(auth.MeshID) || args.ProfileID != enrollment.DefaultProfileID {
				t.Fatalf("%s token fields mismatch", path)
			}
			model.ClearCommand(&internal)
			if !bytes.Equal(args.Token, make([]byte, len(args.Token))) {
				t.Fatalf("%s retained credential bytes", path)
			}
		}
	}
}

func TestTokenAuthenticationRejectsPrefixShapeUnknownAndDuplicateWithoutEcho(t *testing.T) {
	adapter, _ := New()
	valid := validTokenAuthArgs()
	tests := map[string]string{
		"missing":      strings.Replace(valid, `"token":"`+boundaryEnrollmentToken+`",`, "", 1),
		"empty":        strings.Replace(valid, boundaryEnrollmentToken, "", 1),
		"aztm-prefix":  strings.Replace(valid, "cpsa_", "aztm_", 1),
		"other-prefix": strings.Replace(valid, "cpsa_", "anything_", 1),
		"bare":         strings.Replace(valid, boundaryEnrollmentToken, boundaryEnrollmentToken[5:], 1),
		"uppercase-id": strings.Replace(valid, "89ab", "89AB", 1),
		"duplicate":    strings.Replace(valid, `"token":`, `"token":"do-not-echo","token":`, 1),
		"unknown":      strings.Replace(valid, `"token":`, `"mesh_endpoint":"do-not-echo","token":`, 1),
		"oversized":    strings.Replace(valid, boundaryEnrollmentToken, boundaryEnrollmentToken+"x", 1),
	}
	for _, name := range []v1.CommandName{v1.CommandAuthTokenLogin, v1.CommandAuthTokenConnect} {
		for label, args := range tests {
			input := tokenAuthEnvelope(name, args)
			_, publicErr := adapter.DecodeABICommand(input)
			_, internalErr := adapter.DecodeABIInternalCommand(input)
			for _, err := range []error{publicErr, internalErr} {
				if err == nil {
					t.Fatalf("%s/%s accepted malformed input", name, label)
				}
				if strings.Contains(err.Error(), "do-not-echo") || strings.Contains(err.Error(), "cpsa_") {
					t.Fatal("error leaked token data")
				}
				if label == "oversized" && !errors.Is(err, ErrInputTooLarge) {
					t.Fatalf("oversized error = %v", err)
				}
			}
		}
	}
}

func TestTokenSecretIsRemovedBeforeGenericDecode(t *testing.T) {
	raw := tokenAuthEnvelope(v1.CommandAuthTokenLogin, validTokenAuthArgs())
	sanitized, secret, found, err := redactABISecretField(raw, "token")
	defer clear(sanitized)
	defer clear(secret)
	if err != nil || !found || string(secret) != boundaryEnrollmentToken {
		t.Fatalf("secret extraction: %v", err)
	}
	if bytes.Contains(sanitized, []byte("cpsa_")) {
		t.Fatal("sanitized JSON contains credential")
	}
}

func TestTokenAuthenticationMeshIDMatchesManagementBoundary(t *testing.T) {
	adapter, _ := New()
	atLimit := strings.Repeat("m", 255)
	command := v1.AuthTokenLoginCommand{
		CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"},
		Auth:        v1.AuthTokenInput{Token: boundaryEnrollmentToken, MeshID: v1.MeshID(atLimit)},
	}
	internal, err := adapter.DecodeCommand(context.Background(), command)
	if err != nil {
		t.Fatalf("255-byte mesh rejected: %v", err)
	}
	model.ClearCommand(&internal)
	command.Auth.MeshID = v1.MeshID(atLimit + "m")
	if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("256-byte mesh error = %v", err)
	}
}

func TestInstallationAuthenticationRequiresProfileAndMesh(t *testing.T) {
	adapter, _ := New()
	for _, command := range []v1.Command{
		v1.AuthInstallationLoginCommand{CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"}, Auth: v1.AuthInstallationInput{MeshID: "mesh-one"}},
		v1.AuthInstallationConnectCommand{CommandBase: v1.CommandBase{CommandID: "command", SDKSessionID: "session"}, Auth: v1.AuthInstallationInput{ProfileID: "replica-a"}},
	} {
		if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrMalformedInput) {
			t.Fatalf("%s malformed installation context error=%v", command.Name(), err)
		}
	}
}
