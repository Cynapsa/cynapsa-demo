package runtime_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
)

// This external-package test deliberately imports both sides of the boundary.
// Keeping it outside package runtime avoids the production import cycle while
// proving the two independently maintained closed catalogs cannot drift.
func TestCommandCatalogMatchesPublicCapabilities(t *testing.T) {
	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatalf("sdkboundary.New(): %v", err)
	}
	capabilities := adapter.PublicCapabilities()
	public := make([]string, len(capabilities.Commands))
	for index, name := range capabilities.Commands {
		public[index] = string(name)
	}
	sort.Strings(public)
	private := runtime.CommandCatalog()
	if !reflect.DeepEqual(private, public) {
		t.Fatalf("runtime catalog = %q, public capabilities = %q", private, public)
	}

	for _, forbidden := range []string{
		"http.bridge.disable",
		"http.bridge.enable",
		"mesh.credentials.put",
		"mesh.credentials.remove",
		"private.command",
	} {
		if index := sort.SearchStrings(public, forbidden); index < len(public) && public[index] == forbidden {
			t.Errorf("private or removed command %q leaked into both catalogs", forbidden)
		}
	}
}
