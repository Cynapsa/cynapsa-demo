package cynapsagocore

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestAddressStoreCRUDResolveAndStableList(t *testing.T) {
	store, err := newAddressStore(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "HTTPS://Z.Example:443", Recipient: "agent-z"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "http://a.example:8080", Recipient: "agent-a"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "https://z.example", Recipient: "agent-new"}); err != nil {
		t.Fatalf("replace at capacity: %v", err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "https://full.example", Recipient: "agent-full"}); !errors.Is(err, errAddressStoreCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	want := []model.AddressMapping{
		{VirtualOrigin: "http://a.example:8080", Recipient: "agent-a"},
		{VirtualOrigin: "https://z.example", Recipient: "agent-new"},
	}
	if got := store.List(); !reflect.DeepEqual(got, want) {
		t.Fatalf("list = %#v", got)
	}
	resolution, err := store.Resolve("http://a.example:8080/orders%2Fnew?x=1&x=2")
	if err != nil || resolution != (model.AddressResolution{Recipient: "agent-a", Path: "/orders%2Fnew", Query: "x=1&x=2"}) {
		t.Fatalf("resolution = %#v, %v", resolution, err)
	}
	resolution, err = store.Resolve("https://z.example")
	if err != nil || resolution.Path != "/" {
		t.Fatalf("origin-only resolution = %#v, %v", resolution, err)
	}
	if err := store.Remove("HTTPS://Z.EXAMPLE:443"); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove("https://z.example"); err != nil {
		t.Fatalf("idempotent remove = %v", err)
	}
	if _, err := store.Resolve("https://z.example/path"); !errors.Is(err, errAddressStoreMissing) {
		t.Fatalf("removed resolve = %v", err)
	}
}

func TestAddressStoreRejectsPrivateInvalidInput(t *testing.T) {
	store, err := newAddressStore(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, mapping := range []model.AddressMapping{
		{},
		{VirtualOrigin: "https://example/path", Recipient: "agent"},
		{VirtualOrigin: "ftp://example", Recipient: "agent"},
		{VirtualOrigin: "https://example", Recipient: ""},
		{VirtualOrigin: "https://example.:443", Recipient: "agent"},
		{VirtualOrigin: "https://mésh.example:443", Recipient: "agent"},
		{VirtualOrigin: "https://user@example:443", Recipient: "agent"},
	} {
		if err := store.Put(mapping); !errors.Is(err, errAddressStoreInvalid) {
			t.Errorf("Put(%#v) = %v", mapping, err)
		}
	}
}

func TestAddressStoreEquivalentOriginsShareOneCanonicalKey(t *testing.T) {
	store, err := newAddressStore(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "HTTP://Agent.Example:80", Recipient: "agent-a"}); err != nil {
		t.Fatal(err)
	}
	if resolution, err := store.Resolve("http://agent.example/orders"); err != nil || resolution.Recipient != "agent-a" {
		t.Fatalf("default-port resolution = %#v, %v", resolution, err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "http://AGENT.example", Recipient: "agent-b"}); err != nil {
		t.Fatalf("equivalent replacement at capacity: %v", err)
	}
	if got := store.List(); !reflect.DeepEqual(got, []model.AddressMapping{{VirtualOrigin: "http://AGENT.example", Recipient: "agent-b"}}) {
		t.Fatalf("public spelling = %#v", got)
	}
	if err := store.Remove("http://agent.EXAMPLE:080"); err != nil {
		t.Fatal(err)
	}
	if got := store.List(); len(got) != 0 {
		t.Fatalf("items after equivalent remove = %#v", got)
	}
}

func TestAddressStoreConcurrentReplacementAndResolution(t *testing.T) {
	store, err := newAddressStore(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(model.AddressMapping{VirtualOrigin: "https://agent.example", Recipient: "agent-0"}); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for index := 0; index < 32; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			recipient := "agent-a"
			if index%2 == 0 {
				recipient = "agent-b"
			}
			if err := store.Put(model.AddressMapping{VirtualOrigin: "https://agent.example", Recipient: recipient}); err != nil {
				t.Errorf("replace: %v", err)
			}
			if _, err := store.Resolve("https://agent.example/path"); err != nil {
				t.Errorf("resolve: %v", err)
			}
		}(index)
	}
	workers.Wait()
	if got := store.List(); len(got) != 1 || (got[0].Recipient != "agent-a" && got[0].Recipient != "agent-b") {
		t.Fatalf("list = %#v", got)
	}
}
