package runtime_test

import (
	"reflect"
	"testing"

	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

func FuzzCommandCatalogCallerMutationIsolation(f *testing.F) {
	f.Add(uint64(0), "caller.mutated")
	f.Add(uint64(51), "")
	f.Add(^uint64(0), "mesh.credentials.put")

	f.Fuzz(func(t *testing.T, selector uint64, replacement string) {
		catalog := coreruntime.CommandCatalog()
		if len(catalog) != len(frozenRuntimeCommandCatalog) {
			t.Fatalf("catalog length = %d, want %d", len(catalog), len(frozenRuntimeCommandCatalog))
		}
		catalog[int(selector%uint64(len(catalog)))] = replacement

		if got := coreruntime.CommandCatalog(); !reflect.DeepEqual(got, frozenRuntimeCommandCatalog) {
			t.Fatalf("caller mutation changed later catalog: %q", got)
		}
	})
}
