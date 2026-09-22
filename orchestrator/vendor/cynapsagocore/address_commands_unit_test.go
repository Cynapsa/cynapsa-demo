package cynapsagocore

import (
	"context"
	"reflect"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestPublicAddressCommandLifecycle(t *testing.T) {
	core, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdownAndDrain(t, core)
		if err := core.Destroy(); err != nil {
			t.Fatal(err)
		}
	}()

	completePublicCommand(t, core, "put-z", v1.AddressMapPutCommand{
		CommandBase: v1.CommandBase{CommandID: "put-z", SDKSessionID: "session"},
		Mapping:     v1.AddressMapping{VirtualOrigin: "HTTPS://Z.Example:443", Recipient: "agent-z"},
	})
	completePublicCommand(t, core, "put-a", v1.AddressMapPutCommand{
		CommandBase: v1.CommandBase{CommandID: "put-a", SDKSessionID: "session"},
		Mapping:     v1.AddressMapping{VirtualOrigin: "http://a.example:8080", Recipient: "agent-a"},
	})

	listed := completePublicCommand(t, core, "list", v1.AddressMapListCommand{CommandBase: v1.CommandBase{CommandID: "list", SDKSessionID: "session"}})
	wantMappings := []v1.AddressMapping{
		{VirtualOrigin: "http://a.example:8080", Recipient: "agent-a"},
		{VirtualOrigin: "HTTPS://Z.Example:443", Recipient: "agent-z"},
	}
	listResult, ok := listed.Result.(v1.AddressMappingsResult)
	if !ok || !reflect.DeepEqual(listResult.Mappings, wantMappings) {
		t.Fatalf("list result = %#v", listed.Result)
	}

	resolved := completePublicCommand(t, core, "resolve", v1.AddressResolveCommand{
		CommandBase: v1.CommandBase{CommandID: "resolve", SDKSessionID: "session"},
		URL:         "http://a.example:8080/orders%2Fnew?x=1&x=2",
	})
	resolution, ok := resolved.Result.(v1.AddressResolution)
	if !ok || resolution != (v1.AddressResolution{Recipient: "agent-a", Path: "/orders%2Fnew", Query: "x=1&x=2"}) {
		t.Fatalf("resolution = %#v", resolved.Result)
	}

	completePublicCommand(t, core, "remove", v1.AddressMapRemoveCommand{
		CommandBase:   v1.CommandBase{CommandID: "remove", SDKSessionID: "session"},
		VirtualOrigin: "http://a.example:8080",
	})
	completePublicCommand(t, core, "remove-again", v1.AddressMapRemoveCommand{
		CommandBase:   v1.CommandBase{CommandID: "remove-again", SDKSessionID: "session"},
		VirtualOrigin: "HTTP://A.EXAMPLE:8080",
	})
	missing := completePublicCommand(t, core, "missing", v1.AddressResolveCommand{
		CommandBase: v1.CommandBase{CommandID: "missing", SDKSessionID: "session"},
		URL:         "http://a.example:8080/orders",
	})
	if missing.OK || missing.Error == nil || missing.Error.Code != v1.ErrorCodeCommand {
		t.Fatalf("missing completion = %#v", missing)
	}
}

func completePublicCommand(t *testing.T, core *Core, commandID v1.CommandID, command v1.Command) v1.Completion {
	t.Helper()
	admission, err := core.Submit(context.Background(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("submit %s = %#v, %v", command.Name(), admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || completion.CommandID != commandID {
		t.Fatalf("complete %s = %#v, %v", command.Name(), completion, err)
	}
	if command.Name() != v1.CommandAddressResolve || completion.CommandID != "missing" {
		if !completion.OK || completion.Error != nil {
			t.Fatalf("complete %s = %#v", command.Name(), completion)
		}
	}
	return completion
}
