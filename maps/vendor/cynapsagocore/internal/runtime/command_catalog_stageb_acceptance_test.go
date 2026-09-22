package runtime_test

import (
	"reflect"
	"sort"
	"sync"
	"testing"

	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

func TestStageBAcceptanceCommandCatalogParitySurvivesCallerMutation(t *testing.T) {
	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	capabilities := adapter.PublicCapabilities()
	want := make([]string, len(capabilities.Commands))
	for index, command := range capabilities.Commands {
		want[index] = string(command)
	}
	sort.Strings(want)
	if got := coreruntime.CommandCatalog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime catalog = %q, public capabilities = %q", got, want)
	}

	const callers = 24
	start := make(chan struct{})
	failures := make(chan []string, callers)
	var group sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		caller := caller
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			catalog := coreruntime.CommandCatalog()
			if !reflect.DeepEqual(catalog, want) {
				failures <- catalog
				return
			}
			catalog[caller%len(catalog)] = "caller.mutation"
		}()
	}
	close(start)
	group.Wait()
	close(failures)
	for got := range failures {
		t.Errorf("concurrent catalog = %q, want %q", got, want)
	}
	if got := coreruntime.CommandCatalog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("caller mutation changed runtime catalog: %q", got)
	}
	capabilitiesAgain := adapter.PublicCapabilities()
	if !reflect.DeepEqual(capabilitiesAgain, capabilities) {
		t.Fatalf("catalog mutation changed public capabilities: before=%+v after=%+v", capabilities, capabilitiesAgain)
	}
}
