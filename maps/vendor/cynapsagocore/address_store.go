package cynapsagocore

import (
	"errors"
	"sort"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var (
	errAddressStoreInvalid  = errors.New("address store: invalid input")
	errAddressStoreCapacity = errors.New("address store: capacity reached")
	errAddressStoreMissing  = errors.New("address store: mapping unavailable")
)

// addressStore owns one process-local HTTP-origin-to-agent mapping table. The
// SDK boundary validates syntax before admission; this store repeats the
// structural checks needed to remain fail-closed when exercised privately.
type addressStore struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]model.AddressMapping
}

func newAddressStore(capacity int) (*addressStore, error) {
	if capacity <= 0 {
		return nil, errAddressStoreInvalid
	}
	return &addressStore{capacity: capacity, items: make(map[string]model.AddressMapping)}, nil
}

func (store *addressStore) Put(mapping model.AddressMapping) error {
	address, err := model.ParseAddressOrigin(mapping.VirtualOrigin)
	if store == nil || err != nil || mapping.Recipient == "" {
		return errAddressStoreInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.items[address.LookupKey()]; !exists && len(store.items) >= store.capacity {
		return errAddressStoreCapacity
	}
	store.items[address.LookupKey()] = mapping
	return nil
}

func (store *addressStore) Remove(origin string) error {
	address, err := model.ParseAddressOrigin(origin)
	if store == nil || err != nil {
		return errAddressStoreInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.items, address.LookupKey())
	return nil
}

func (store *addressStore) List() []model.AddressMapping {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	origins := make([]string, 0, len(store.items))
	for key := range store.items {
		origins = append(origins, key)
	}
	sort.Strings(origins)
	result := make([]model.AddressMapping, len(origins))
	for index, key := range origins {
		result[index] = store.items[key]
	}
	store.mu.RUnlock()
	return result
}

func (store *addressStore) Resolve(raw string) (model.AddressResolution, error) {
	if store == nil {
		return model.AddressResolution{}, errAddressStoreInvalid
	}
	address, err := model.ParseAddressURL(raw)
	if err != nil {
		return model.AddressResolution{}, errAddressStoreInvalid
	}
	store.mu.RLock()
	mapping, exists := store.items[address.LookupKey()]
	store.mu.RUnlock()
	if !exists {
		return model.AddressResolution{}, errAddressStoreMissing
	}
	return model.AddressResolution{Recipient: mapping.Recipient, Path: address.Path(), Query: address.Query()}, nil
}
