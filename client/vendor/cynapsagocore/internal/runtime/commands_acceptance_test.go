package runtime_test

import (
	"reflect"
	"sort"
	"sync"
	"testing"

	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

var frozenRuntimeCommandCatalog = []string{
	"address.map.list",
	"address.map.put",
	"address.map.remove",
	"address.resolve",
	"auth.agent_id",
	"auth.connect",
	"auth.installation_connect",
	"auth.installation_login",
	"auth.login",
	"auth.logout",
	"auth.token_connect",
	"auth.token_login",
	"command.cancel",
	"command.channel.clear",
	"command.channel.register",
	"conversation.close",
	"conversation.list",
	"conversation.status",
	"core.capabilities",
	"core.init",
	"core.shutdown",
	"core.status",
	"delivery.accept",
	"delivery.drop",
	"delivery.next",
	"delivery.pause",
	"delivery.queue.status",
	"delivery.resume",
	"delivery.retry",
	"diagnostics.connectivity_status",
	"diagnostics.logs.subscribe",
	"diagnostics.peer_status",
	"diagnostics.snapshot",
	"event.sink.bind",
	"event.sink.clear",
	"event.sink.register",
	"handler.register",
	"handler.unregister",
	"mesh.list",
	"mesh.membership.refresh",
	"message.reply",
	"message.request",
	"message.send",
	"payload.cancel",
	"payload.close",
	"payload.finish",
	"payload.open",
	"payload.read",
	"payload.release",
	"payload.retain",
	"payload.write_chunk",
	"policy.get",
	"policy.set",
	"policy.test",
	"session.config.get",
	"session.config.update",
}

func TestCommandCatalogAcceptanceFrozenNamesAndOrder(t *testing.T) {
	got := coreruntime.CommandCatalog()
	if !reflect.DeepEqual(got, frozenRuntimeCommandCatalog) {
		t.Fatalf("CommandCatalog() = %q, want exact frozen catalog %q", got, frozenRuntimeCommandCatalog)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("CommandCatalog() is not in lexical order: %q", got)
	}
	for index := 1; index < len(got); index++ {
		if got[index-1] == got[index] {
			t.Fatalf("CommandCatalog() contains duplicate command %q", got[index])
		}
	}
}

func TestCommandCatalogAcceptanceExcludesRemovedCommands(t *testing.T) {
	catalog := coreruntime.CommandCatalog()
	removed := []string{
		"http.bridge.disable",
		"http.bridge.enable",
		"mesh.credentials.get",
		"mesh.credentials.put",
		"mesh.credentials.remove",
		"session.personality.set",
	}
	for _, name := range removed {
		index := sort.SearchStrings(catalog, name)
		if index < len(catalog) && catalog[index] == name {
			t.Errorf("removed command %q remains in CommandCatalog", name)
		}
	}
}

func TestCommandCatalogAcceptanceCallerOwnsEveryReturnedElement(t *testing.T) {
	untouched := coreruntime.CommandCatalog()
	for index := range frozenRuntimeCommandCatalog {
		mutated := coreruntime.CommandCatalog()
		mutated[index] = "caller.mutated"
		mutated = append(mutated[:index], mutated[index+1:]...)

		if got := coreruntime.CommandCatalog(); !reflect.DeepEqual(got, frozenRuntimeCommandCatalog) {
			t.Fatalf("mutation at index %d changed a later catalog: %q", index, got)
		}
		if !reflect.DeepEqual(untouched, frozenRuntimeCommandCatalog) {
			t.Fatalf("mutation at index %d changed an earlier catalog: %q", index, untouched)
		}
	}
}

func TestCommandCatalogAcceptanceRepeatedAndConcurrentIsolation(t *testing.T) {
	const (
		callers    = 32
		iterations = 250
	)

	start := make(chan struct{})
	failures := make(chan string, callers)
	var callersDone sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		caller := caller
		callersDone.Add(1)
		go func() {
			defer callersDone.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				catalog := coreruntime.CommandCatalog()
				if !reflect.DeepEqual(catalog, frozenRuntimeCommandCatalog) {
					failures <- "concurrent call returned a changed catalog"
					return
				}
				catalog[(caller+iteration)%len(catalog)] = "caller.mutated"
			}
		}()
	}

	close(start)
	callersDone.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if got := coreruntime.CommandCatalog(); !reflect.DeepEqual(got, frozenRuntimeCommandCatalog) {
		t.Fatalf("concurrent mutations changed the catalog: %q", got)
	}
}
