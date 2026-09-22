package sdkboundary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func exactApplicationPath() string {
	return "/" + strings.Repeat("a", maxPathLength-1)
}

func testCommandBase() v1.CommandBase {
	return v1.CommandBase{CommandID: "command-1", SDKSessionID: "session-1"}
}

func TestApplicationPathBoundaryOnPublicCommands(t *testing.T) {
	adapter, _ := New()
	accepted := exactApplicationPath()
	rejected := accepted + "a"

	variants := []struct {
		name    string
		payload func(string) v1.Payload
	}{
		{"native", func(path string) v1.Payload { return v1.Payload{Value: v1.NativePayload{Path: path}} }},
		{"http request", func(path string) v1.Payload {
			return v1.Payload{Value: v1.HTTPRequestPayload{Method: "POST", Path: path}}
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			command := func(path string) v1.MessageSendCommand {
				return v1.MessageSendCommand{CommandBase: testCommandBase(), To: "agent", Payload: variant.payload(path)}
			}
			if _, err := adapter.DecodeCommand(context.Background(), command(accepted)); err != nil {
				t.Fatalf("2,048-byte path rejected: %v", err)
			}
			if _, err := adapter.DecodeCommand(context.Background(), command(rejected)); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("2,049-byte path error=%v, want input too large", err)
			}
		})
	}

	for _, path := range []string{"relative", "/orders?x=1", "/orders#fragment"} {
		command := v1.HandlerRegisterCommand{CommandBase: testCommandBase(), Path: path}
		if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("handler path %q error=%v, want malformed", path, err)
		}
	}
	if _, err := adapter.DecodeCommand(context.Background(), v1.HandlerRegisterCommand{CommandBase: testCommandBase(), Path: accepted}); err != nil {
		t.Fatalf("handler 2,048-byte path rejected: %v", err)
	}
	if _, err := adapter.DecodeCommand(context.Background(), v1.HandlerRegisterCommand{CommandBase: testCommandBase(), Path: rejected}); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("handler 2,049-byte path error=%v, want input too large", err)
	}
}

func TestApplicationPathBoundaryOnPrivatePayloadProjection(t *testing.T) {
	adapter, _ := New()
	accepted := exactApplicationPath()
	rejected := accepted + "a"
	variants := []struct {
		name    string
		payload func(string) model.Payload
	}{
		{"native", func(path string) model.Payload { return model.Payload{Value: model.NativePayload{Path: path}} }},
		{"http request", func(path string) model.Payload {
			return model.Payload{Value: model.HTTPRequestPayload{Method: "POST", Path: path}}
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			if _, err := adapter.MapPayload(variant.payload(accepted)); err != nil {
				t.Fatalf("2,048-byte path rejected: %v", err)
			}
			if _, err := adapter.MapPayload(variant.payload(rejected)); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("2,049-byte path error=%v, want input too large", err)
			}
		})
	}

	attackerBody := make([]byte, maxInlinePayloadBytes-1)
	attackerHeaders := make([]model.Header, maxHeaderCount)
	for name, oversized := range map[string]model.Payload{
		"native": {Value: model.NativePayload{Path: rejected, Body: attackerBody}},
		"http":   {Value: model.HTTPRequestPayload{Method: "POST", Path: rejected, Headers: attackerHeaders, Body: attackerBody}},
	} {
		if allocations := testing.AllocsPerRun(20, func() {
			if _, err := adapter.MapPayload(oversized); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("oversized path error=%v", err)
			}
		}); allocations != 0 {
			t.Fatalf("private %s oversized-path rejection allocated %.0f objects before copy rejection", name, allocations)
		}
	}
}

func TestApplicationPathBoundaryOnStrictABICommands(t *testing.T) {
	adapter, _ := New()
	accepted := exactApplicationPath()
	rejected := accepted + "a"

	variants := []struct {
		name    string
		payload func(string) string
	}{
		{"native", func(path string) string {
			return fmt.Sprintf(`{"native":{"content_type":"","path":%q,"body":""}}`, path)
		}},
		{"http request", func(path string) string {
			return fmt.Sprintf(`{"http_request":{"method":"POST","path":%q,"query":"","headers":[],"body":""}}`, path)
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			command := func(path string) []byte {
				return []byte(fmt.Sprintf(`{"abi_version":1,"command_id":"command-1","command_name":"message.send","sdk_session_id":"session-1","args":{"to":"agent","payload":%s}}`, variant.payload(path)))
			}
			if _, err := adapter.DecodeABICommand(command(accepted)); err != nil {
				t.Fatalf("2,048-byte path rejected: %v", err)
			}
			if _, err := adapter.DecodeABICommand(command(rejected)); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("2,049-byte path error=%v, want input too large", err)
			}
		})
	}

	attackerBody := make([]byte, maxInlinePayloadBytes-1)
	attackerHeaders := make([]headerWire, maxHeaderCount)
	for name, wire := range map[string]payloadWire{
		"native": {Native: &nativePayloadWire{Path: rejected, Body: attackerBody}},
		"http":   {HTTPRequest: &httpRequestPayloadWire{Method: "POST", Path: rejected, Headers: attackerHeaders, Body: attackerBody}},
	} {
		if allocations := testing.AllocsPerRun(20, func() {
			if _, err := publicPayload(wire); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("wire oversized path error=%v", err)
			}
		}); allocations != 0 {
			t.Fatalf("wire %s oversized-path rejection allocated %.0f objects before copy rejection", name, allocations)
		}
	}
}

func TestCanonicalHTTPMetadataLedgerLimits(t *testing.T) {
	maxMethod := strings.Repeat("M", 32)
	maxQuery := strings.Repeat("q", maxPublicString)
	maxName := strings.Repeat("n", maxIdentifierLength)
	maxValue := strings.Repeat("v", maxPublicString)
	maxText := strings.Repeat("t", maxIdentifierLength)

	if err := validateHTTPMethod(maxMethod); err != nil {
		t.Fatalf("32-byte method rejected: %v", err)
	}
	if err := validateHTTPMethod(maxMethod + "M"); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("33-byte method error=%v", err)
	}
	if err := validateContentType(maxText); err != nil {
		t.Fatalf("512-byte content type rejected: %v", err)
	}
	if err := validateContentType(maxText + "t"); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("513-byte content type error=%v", err)
	}
	if err := validateReason(maxText); err != nil {
		t.Fatalf("512-byte reason rejected: %v", err)
	}
	if err := validateReason(maxText + "t"); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("513-byte reason error=%v", err)
	}
	for name, value := range map[string]string{
		"content type CRLF":  "text/plain\r\nprivate: x",
		"content type UTF-8": string([]byte{0xff}),
	} {
		if err := validateContentType(value); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("%s error=%v", name, err)
		}
	}
	for name, value := range map[string]string{
		"reason CRLF":  "OK\r\nprivate",
		"reason UTF-8": string([]byte{0xff}),
	} {
		if err := validateReason(value); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("%s error=%v", name, err)
		}
	}

	request := v1.HTTPRequestPayload{Method: "POST", Path: "/", Query: maxQuery, Headers: []v1.Header{{Name: maxName, Value: maxValue}}}
	if err := validatePublicPayload(v1.Payload{Value: request}); err != nil {
		t.Fatalf("exact request metadata limits rejected: %v", err)
	}
	request.Query += "q"
	if err := validatePublicPayload(v1.Payload{Value: request}); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("8,193-byte query error=%v", err)
	}
	request.Query = ""
	request.Headers[0].Name += "n"
	if err := validatePublicPayload(v1.Payload{Value: request}); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("513-byte header name error=%v", err)
	}
	request.Headers[0] = v1.Header{Name: "x", Value: maxValue + "v"}
	if err := validatePublicPayload(v1.Payload{Value: request}); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("8,193-byte header value error=%v", err)
	}
	request.Headers[0] = v1.Header{Name: "x", Value: "value\nprivate"}
	if err := validatePublicPayload(v1.Payload{Value: request}); !errors.Is(err, ErrMalformedInput) {
		t.Errorf("header CRLF error=%v", err)
	}
	request.Headers = make([]v1.Header, maxHeaderCount)
	for i := range request.Headers {
		request.Headers[i] = v1.Header{Name: "x"}
	}
	if err := validatePublicPayload(v1.Payload{Value: request}); err != nil {
		t.Fatalf("4,096 headers rejected: %v", err)
	}
	request.Headers = append(request.Headers, v1.Header{Name: "x"})
	if err := validatePublicPayload(v1.Payload{Value: request}); !errors.Is(err, ErrInputTooLarge) {
		t.Errorf("4,097 headers error=%v", err)
	}

	adapter, _ := New()
	for name, payload := range map[string]model.Payload{
		"private native content type": {Value: model.NativePayload{Path: "/", ContentType: string([]byte{0xff})}},
		"private HTTP reason":         {Value: model.HTTPResponsePayload{StatusCode: 200, Reason: "OK\r\nprivate"}},
	} {
		if _, err := adapter.MapPayload(payload); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("%s error=%v", name, err)
		}
	}
}

func TestPolicyApplicationPathsPreserveOnlyTheEmptyWildcard(t *testing.T) {
	adapter, _ := New()
	base := testCommandBase()
	for _, path := range []string{"", exactApplicationPath()} {
		command := v1.PolicySetCommand{CommandBase: base, Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path}}}
		if _, err := adapter.DecodeCommand(context.Background(), command); err != nil {
			t.Errorf("valid policy path %q rejected: %v", path, err)
		}
	}
	for _, path := range []string{"relative", "/orders?x=1", "/orders#fragment"} {
		command := v1.PolicySetCommand{CommandBase: base, Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: path}}}
		if _, err := adapter.DecodeCommand(context.Background(), command); !errors.Is(err, ErrMalformedInput) {
			t.Errorf("policy path %q error=%v, want malformed", path, err)
		}
	}
}

func TestAddressResolveValidatesTheEmittedApplicationPath(t *testing.T) {
	adapter, _ := New()
	accepted := exactApplicationPath()
	rejected := accepted + "a"
	command := func(path string) v1.AddressResolveCommand {
		return v1.AddressResolveCommand{CommandBase: testCommandBase(), URL: "https://agent.example" + path + "?x=1"}
	}
	if _, err := adapter.DecodeCommand(context.Background(), command(accepted)); err != nil {
		t.Fatalf("direct 2,048-byte resolved path rejected: %v", err)
	}
	if _, err := adapter.DecodeCommand(context.Background(), command(rejected)); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("direct 2,049-byte resolved path error=%v, want input too large", err)
	}
	if _, err := adapter.DecodeCommand(context.Background(), v1.AddressResolveCommand{CommandBase: testCommandBase(), URL: "https://agent.example"}); err != nil {
		t.Fatalf("origin-only URL must emit root path: %v", err)
	}

	abiCommand := func(path string) []byte {
		return []byte(fmt.Sprintf(`{"abi_version":1,"command_id":"command-1","command_name":"address.resolve","sdk_session_id":"session-1","args":{"url":%q}}`, "https://agent.example"+path+"?x=1"))
	}
	if _, err := adapter.DecodeABICommand(abiCommand(accepted)); err != nil {
		t.Fatalf("ABI 2,048-byte resolved path rejected: %v", err)
	}
	if _, err := adapter.DecodeABICommand(abiCommand(rejected)); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("ABI 2,049-byte resolved path error=%v, want input too large", err)
	}
}

func TestPolicyRuleLimitIsSharedAcrossEveryBoundaryDirection(t *testing.T) {
	adapter, _ := New()
	publicRules := make([]v1.PolicyRule, maxPolicyRules)
	privateRules := make([]model.PolicyRule, maxPolicyRules)
	wireRules := make([]policyRuleWire, maxPolicyRules)
	for index := 0; index < maxPolicyRules; index++ {
		publicRules[index] = v1.PolicyRule{Action: v1.PolicyActionAllow}
		privateRules[index] = model.PolicyRule{Action: string(v1.PolicyActionAllow)}
		wireRules[index] = policyRuleWire{Action: string(v1.PolicyActionAllow)}
	}

	direct := v1.PolicySetCommand{CommandBase: testCommandBase(), Rules: publicRules}
	if _, err := adapter.DecodeCommand(context.Background(), direct); err != nil {
		t.Fatalf("direct 1,024 rules rejected: %v", err)
	}
	privateResult := model.Result{CommandID: "command-1", Value: model.PolicyResult{Rules: privateRules}}
	if _, err := adapter.MapCompletion(privateResult); err != nil {
		t.Fatalf("private 1,024-rule result rejected: %v", err)
	}
	publicResult := v1.Completion{CommandID: "command-1", OK: true, Result: v1.PolicyResult{Rules: publicRules}}
	if _, err := adapter.EncodeABICompletion(publicResult); err != nil {
		t.Fatalf("public 1,024-rule result rejected: %v", err)
	}
	if err := validateWirePolicyRules(wireRules); err != nil {
		t.Fatalf("wire 1,024 rules rejected: %v", err)
	}

	publicOver := append(publicRules, v1.PolicyRule{Action: v1.PolicyActionAllow})
	privateOver := append(privateRules, model.PolicyRule{Action: string(v1.PolicyActionAllow)})
	wireOver := append(wireRules, policyRuleWire{Action: string(v1.PolicyActionAllow)})
	if _, err := adapter.DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: testCommandBase(), Rules: publicOver}); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("direct 1,025-rule error=%v", err)
	}
	if _, err := adapter.MapCompletion(model.Result{CommandID: "command-1", Value: model.PolicyResult{Rules: privateOver}}); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("private 1,025-rule result error=%v", err)
	}
	if _, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command-1", OK: true, Result: v1.PolicyResult{Rules: publicOver}}); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("public 1,025-rule result error=%v", err)
	}
	if err := validateWirePolicyRules(wireOver); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("wire 1,025-rule error=%v", err)
	}

	for name, reject := range map[string]func() error{
		"direct command": func() error {
			_, err := adapter.DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: testCommandBase(), Rules: publicOver})
			return err
		},
		"private result": func() error {
			_, err := adapter.MapCompletion(model.Result{CommandID: "command-1", Value: model.PolicyResult{Rules: privateOver}})
			return err
		},
		"public result": func() error {
			_, err := adapter.EncodeABICompletion(v1.Completion{CommandID: "command-1", OK: true, Result: v1.PolicyResult{Rules: publicOver}})
			return err
		},
		"wire command": func() error { return validateWirePolicyRules(wireOver) },
	} {
		if allocations := testing.AllocsPerRun(20, func() {
			if err := reject(); !errors.Is(err, ErrInputTooLarge) {
				t.Fatalf("%s over-limit error=%v", name, err)
			}
		}); allocations > 1 {
			t.Fatalf("%s copied before 1,025-rule rejection: %.0f allocations", name, allocations)
		}
	}

	for _, count := range []int{maxPolicyRules, maxPolicyRules + 1} {
		rules := wireRules
		if count > maxPolicyRules {
			rules = wireOver
		}
		args, err := stableMarshal(policySetWire{Rules: rules})
		if err != nil {
			t.Fatal(err)
		}
		input, err := stableMarshal(abiCommandWire{ABIVersion: v1.CurrentSchemaVersion, CommandID: "command-1", CommandName: string(v1.CommandPolicySet), SessionID: "session-1", Args: args})
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.DecodeABICommand(input)
		if count == maxPolicyRules && err != nil {
			t.Fatalf("strict ABI 1,024 rules rejected: %v", err)
		}
		if count > maxPolicyRules && !errors.Is(err, ErrInputTooLarge) {
			t.Fatalf("strict ABI 1,025-rule error=%v", err)
		}
	}
}
