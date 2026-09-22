package runtime

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var expectedCommandCatalog = []string{
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

func TestCommandCatalogExhaustivelyMatchesRuntimeValidation(t *testing.T) {
	catalog := CommandCatalog()
	if !reflect.DeepEqual(catalog, expectedCommandCatalog) {
		t.Fatalf("CommandCatalog() = %q, want %q", catalog, expectedCommandCatalog)
	}
	if len(catalog) != len(allowedCommands) {
		t.Fatalf("catalog length = %d, allowlist length = %d", len(catalog), len(allowedCommands))
	}

	handlers := make(map[string]Handler, len(catalog))
	for _, name := range catalog {
		if !knownCommand(name) {
			t.Errorf("catalog command %q is rejected by knownCommand", name)
		}
		handlers[name] = func(context.Context, Services, model.Command) (model.Result, error) {
			return model.Result{Value: model.EmptyResult{}}, nil
		}
	}
	validated, _, _, err := validateDependencies(Dependencies{Handlers: handlers})
	if err != nil {
		t.Fatalf("validateDependencies(all catalog handlers): %v", err)
	}
	if len(validated) != len(catalog) {
		t.Fatalf("validated handlers = %d, want %d", len(validated), len(catalog))
	}

	for name := range allowedCommands {
		if _, found := handlers[name]; !found {
			t.Errorf("known command %q is absent from CommandCatalog", name)
		}
	}
	if knownCommand("private.command") {
		t.Fatal("knownCommand accepted a command outside the frozen allowlist")
	}
	if _, _, _, err := validateDependencies(Dependencies{Handlers: map[string]Handler{"private.command": testHandler}}); !errors.Is(err, ErrUnknownHandler) {
		t.Fatalf("validateDependencies(private command) error = %v, want ErrUnknownHandler", err)
	}
}

func TestCommandCatalogIsSortedStableAndMutationIsolated(t *testing.T) {
	first := CommandCatalog()
	second := CommandCatalog()
	if !sort.StringsAreSorted(first) {
		t.Fatalf("CommandCatalog() is not sorted: %q", first)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("successive catalogs differ: %q and %q", first, second)
	}
	if len(first) == 0 {
		t.Fatal("CommandCatalog() is empty")
	}

	first[0] = "private.command"
	first[len(first)-1] = "private.command"
	if got := CommandCatalog(); !reflect.DeepEqual(got, expectedCommandCatalog) {
		t.Fatalf("caller mutation changed catalog: %q", got)
	}
	if second[0] != expectedCommandCatalog[0] || second[len(second)-1] != expectedCommandCatalog[len(expectedCommandCatalog)-1] {
		t.Fatalf("catalog calls share backing storage: %q", second)
	}
}

func TestCommandCatalogConcurrentCallsAreIndependent(t *testing.T) {
	const goroutines = 64
	const iterations = 1_000

	start := make(chan struct{})
	errorsCh := make(chan string, goroutines)
	var group sync.WaitGroup
	for worker := range goroutines {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for iteration := range iterations {
				catalog := CommandCatalog()
				if !reflect.DeepEqual(catalog, expectedCommandCatalog) {
					errorsCh <- "catalog changed during concurrent access"
					return
				}
				catalog[(worker+iteration)%len(catalog)] = "private.command"
			}
		}()
	}
	close(start)
	group.Wait()
	close(errorsCh)
	for failure := range errorsCh {
		t.Error(failure)
	}
	if got := CommandCatalog(); !reflect.DeepEqual(got, expectedCommandCatalog) {
		t.Fatalf("concurrent caller mutation changed catalog: %q", got)
	}
}

func TestCommandCatalogExcludesPrivateVocabulary(t *testing.T) {
	catalog := CommandCatalog()
	for _, forbidden := range []string{
		"http.bridge.disable",
		"http.bridge.enable",
		"mesh.credentials.put",
		"mesh.credentials.remove",
		"private.command",
	} {
		if index := sort.SearchStrings(catalog, forbidden); index < len(catalog) && catalog[index] == forbidden {
			t.Errorf("private or removed command %q leaked into catalog", forbidden)
		}
	}
}
