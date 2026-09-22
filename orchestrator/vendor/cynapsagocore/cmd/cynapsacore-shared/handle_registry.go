package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
)

var (
	errInvalidABIHandle = errors.New("cynapsacore native ABI: invalid handle")
	errInvalidABIInput  = errors.New("cynapsacore native ABI: invalid input")
	errABIClosing       = errors.New("cynapsacore native ABI: shutdown in progress")
	errHandleAllocation = errors.New("cynapsacore native ABI: handle allocation failed")
)

const (
	maxHandleGenerationAttempts = 128
	maxProcessHandleIssuance    = 1_048_576

	// Returned ABI data is bounded independently of the 134,217,696-byte canonical
	// payload size. A payload read returns at most one 1 MiB chunk, and all
	// ordinary completion, event, status, and error documents fit well below
	// this ceiling. Pathological unpaginated list results fail closed here.
	maxABIBufferBytes     uint64 = 64 << 20
	maxLiveABIBufferBytes        = 4 * maxABIBufferBytes

	handleClassShift          = 62
	handlePayloadMask  uint64 = (uint64(1) << handleClassShift) - 1
	coreHandleTag      uint64 = uint64(handleClassCore) << handleClassShift
	bufferHandleTag    uint64 = uint64(handleClassBuffer) << handleClassShift
	emergencyBufferTag uint64 = uint64(handleClassEmergencyBuffer) << handleClassShift

	bufferSlotBits               = 12
	maxLiveABIBufferCount        = 1 << bufferSlotBits
	bufferSlotMask        uint64 = (uint64(1) << bufferSlotBits) - 1
	bufferGenerationBits         = handleClassShift - bufferSlotBits
	maxBufferGeneration   uint64 = (uint64(1) << bufferGenerationBits) - 1

	// This exact class/value is reserved for the one process-owned immutable
	// capacity-error document. It never identifies a reusable buffer slot.
	emergencyQueueFullHandle uint64 = emergencyBufferTag | 1
	emergencyQueueFullJSON          = `{"abi_version":1,"error":{"code":"queue_full","message":"The local queue is full","retryable":false,"stage":"sdk","local_or_remote":"local"}}`
)

type handleClass uint8

const (
	handleClassCore handleClass = iota + 1
	handleClassBuffer
	handleClassEmergencyBuffer
)

func handleClassOf(handle uint64) handleClass {
	return handleClass(handle >> handleClassShift)
}

func coreHandle(raw uint64) uint64 {
	return coreHandleTag | raw&handlePayloadMask
}

func encodeBufferHandle(slot int, generation uint64) uint64 {
	return bufferHandleTag | generation<<bufferSlotBits | uint64(slot)
}

func decodeBufferHandle(handle uint64) (int, uint64, bool) {
	if handle == 0 || handleClassOf(handle) != handleClassBuffer {
		return 0, 0, false
	}
	payload := handle & handlePayloadMask
	generation := payload >> bufferSlotBits
	if generation == 0 {
		return 0, 0, false
	}
	return int(payload & bufferSlotMask), generation, true
}

func emergencyQueueFullDescriptor() BufferDescriptor {
	return BufferDescriptor{BufferHandle: emergencyQueueFullHandle, ByteLength: uint64(len(emergencyQueueFullJSON))}
}

type numericHandleEntry struct {
	class handleClass
	value any
}

type coreReservation struct{}

// borrowCore atomically checks the numeric handle class and acquires one
// in-flight operation lease before registry removal can begin.
func (r *numericHandleRegistry) borrowCore(handle uint64) (*abiCoreRecord, error) {
	if handleClassOf(handle) != handleClassCore {
		return nil, errInvalidABIHandle
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.live[handle]
	if !ok || entry.class != handleClassCore {
		return nil, errInvalidABIHandle
	}
	record, ok := entry.value.(*abiCoreRecord)
	if !ok || !record.acquire() {
		return nil, errInvalidABIHandle
	}
	return record, nil
}

func (r *numericHandleRegistry) reserveCore() (uint64, error) {
	return r.insert(handleClassCore, coreReservation{})
}

func (r *numericHandleRegistry) publishCore(handle uint64, record *abiCoreRecord) error {
	if record == nil || handleClassOf(handle) != handleClassCore {
		return errInvalidABIHandle
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.live[handle]
	if !ok || entry.class != handleClassCore {
		return errInvalidABIHandle
	}
	if _, reserved := entry.value.(coreReservation); !reserved {
		return errInvalidABIHandle
	}
	entry.value = record
	r.live[handle] = entry
	return nil
}

func (r *numericHandleRegistry) abandonCoreReservation(handle uint64) error {
	if handleClassOf(handle) != handleClassCore {
		return errInvalidABIHandle
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.live[handle]
	if !ok || entry.class != handleClassCore {
		return errInvalidABIHandle
	}
	if _, reserved := entry.value.(coreReservation); !reserved {
		return errInvalidABIHandle
	}
	if r.issued == 0 {
		return errInvalidABIHandle
	}
	delete(r.live, handle)
	r.issued--
	return nil
}

// retireCore removes new handle admission and begins record quiescence in one
// registry critical section. Existing leases may finish and are awaited by the
// caller after the registry lock is released.
func (r *numericHandleRegistry) retireCore(handle uint64) (*abiCoreRecord, bool, error) {
	if handleClassOf(handle) != handleClassCore {
		return nil, false, errInvalidABIHandle
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.live[handle]; ok && entry.class == handleClassCore {
		record, valid := entry.value.(*abiCoreRecord)
		if !valid {
			return nil, false, errInvalidABIHandle
		}
		record.beginDestroy()
		delete(r.live, handle)
		r.retired[handle] = handleClassCore
		r.retiredCores[handle] = record
		return record, true, nil
	}
	if retiredClass, retired := r.retired[handle]; retired && retiredClass == handleClassCore {
		return r.retiredCores[handle], false, nil
	}
	return nil, false, errInvalidABIHandle
}

func (r *numericHandleRegistry) completeCoreRetirement(handle uint64, record *abiCoreRecord) {
	r.mu.Lock()
	if r.retiredCores[handle] == record {
		delete(r.retiredCores, handle)
	}
	r.mu.Unlock()
}

// numericHandleRegistry owns core handles only. The explicit class tag keeps
// its random namespace disjoint from reusable generation-bound buffer slots.
type numericHandleRegistry struct {
	mu           sync.RWMutex
	live         map[uint64]numericHandleEntry
	retired      map[uint64]handleClass
	retiredCores map[uint64]*abiCoreRecord
	issued       uint64
	next         func() (uint64, error)
}

var processNumericHandles = newNumericHandleRegistry()

func newNumericHandleRegistry() *numericHandleRegistry {
	return &numericHandleRegistry{live: make(map[uint64]numericHandleEntry), retired: make(map[uint64]handleClass), retiredCores: make(map[uint64]*abiCoreRecord), next: randomUint64}
}

func (r *numericHandleRegistry) insert(class handleClass, value any) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if class != handleClassCore || r.issued >= maxProcessHandleIssuance {
		return 0, errHandleAllocation
	}
	for attempt := 0; attempt < maxHandleGenerationAttempts; attempt++ {
		raw, err := r.next()
		if err != nil {
			return 0, errHandleAllocation
		}
		handle := coreHandle(raw)
		if handle&handlePayloadMask == 0 {
			continue
		}
		if _, exists := r.live[handle]; exists {
			continue
		}
		if _, retired := r.retired[handle]; retired {
			continue
		}
		r.live[handle] = numericHandleEntry{class: class, value: value}
		r.issued++
		return handle, nil
	}
	return 0, errHandleAllocation
}

func (r *numericHandleRegistry) get(handle uint64, class handleClass) (any, error) {
	if class != handleClassCore || handleClassOf(handle) != handleClassCore {
		return nil, errInvalidABIHandle
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.live[handle]
	if !ok || entry.class != class {
		return nil, errInvalidABIHandle
	}
	return entry.value, nil
}

func (r *numericHandleRegistry) retire(handle uint64, class handleClass, repeatedRetireOK bool) (any, bool, error) {
	if class != handleClassCore || handleClassOf(handle) != handleClassCore {
		return nil, false, errInvalidABIHandle
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.live[handle]; ok && entry.class == class {
		delete(r.live, handle)
		r.retired[handle] = class
		return entry.value, true, nil
	}
	if retiredClass, retired := r.retired[handle]; retired && retiredClass == class && repeatedRetireOK {
		return nil, false, nil
	}
	return nil, false, errInvalidABIHandle
}

func randomUint64() (uint64, error) {
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data[:]), nil
}

type bufferSlot struct {
	generation uint64
	reserved   bool
	live       bool
	data       []byte
}

type bufferLimits struct {
	count int
	bytes uint64
	item  uint64
}

type bufferStore struct {
	mu              sync.RWMutex
	slots           []bufferSlot
	liveCount       int
	liveBytes       uint64
	limits          bufferLimits
	clone           func([]byte) ([]byte, error)
	capacityChanged chan struct{}
}

func newBufferStore() *bufferStore {
	return newBufferStoreWithLimits(bufferLimits{count: maxLiveABIBufferCount, bytes: maxLiveABIBufferBytes, item: maxABIBufferBytes})
}

func newBufferStoreForTest(_ *numericHandleRegistry) *bufferStore {
	return newBufferStoreWithLimits(bufferLimits{count: maxLiveABIBufferCount, bytes: maxLiveABIBufferBytes, item: maxABIBufferBytes})
}

func newBufferStoreWithLimits(limits bufferLimits) *bufferStore {
	store := &bufferStore{slots: make([]bufferSlot, limits.count), limits: limits, capacityChanged: make(chan struct{})}
	store.clone = func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
	return store
}

func (s *bufferStore) allocate(data []byte) (descriptor BufferDescriptor, err error) {
	descriptor, _, err = s.allocateOrWait(data)
	return descriptor, err
}

// allocateOrWait distinguishes temporary process-wide capacity pressure from
// deterministic item/clone/generation failure. The returned generation signal
// is captured under the same lock as the failed reservation, so waiting after
// authoritative queue rollback cannot miss a concurrent FreeBuffer.
func (s *bufferStore) allocateOrWait(data []byte) (descriptor BufferDescriptor, capacity <-chan struct{}, err error) {
	size := uint64(len(data))
	slot, generation, capacity, err := s.reserveOrWait(size)
	if err != nil {
		return BufferDescriptor{}, capacity, err
	}
	published := false
	var owned []byte
	defer func() {
		if !published {
			clear(owned)
			s.rollback(slot, generation, size)
		}
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
	}()
	owned, err = s.clone(data)
	if err != nil || uint64(len(owned)) != size {
		return BufferDescriptor{}, nil, errHandleAllocation
	}
	s.mu.Lock()
	record := &s.slots[slot]
	if !record.reserved || record.live || record.generation != generation {
		s.mu.Unlock()
		return BufferDescriptor{}, nil, errHandleAllocation
	}
	record.data = owned
	record.reserved = false
	record.live = true
	s.mu.Unlock()
	published = true
	return BufferDescriptor{BufferHandle: encodeBufferHandle(slot, generation), ByteLength: size}, nil, nil
}

func safeAllocateOrWait(store *bufferStore, data []byte) (descriptor BufferDescriptor, capacity <-chan struct{}, err error) {
	defer func() {
		if recover() != nil {
			descriptor = BufferDescriptor{}
			capacity = nil
			err = errHandleAllocation
		}
	}()
	return store.allocateOrWait(data)
}

func (s *bufferStore) reserve(size uint64) (int, uint64, error) {
	slot, generation, _, err := s.reserveOrWait(size)
	return slot, generation, err
}

func (s *bufferStore) reserveOrWait(size uint64) (int, uint64, <-chan struct{}, error) {
	if err := validateABIBufferSize(size); err != nil {
		return 0, 0, nil, err
	}
	if size > s.limits.item {
		return 0, 0, nil, errHandleAllocation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveCount >= s.limits.count || size > s.limits.bytes-s.liveBytes {
		if s.capacityCanRecoverLocked(size) {
			return 0, 0, s.capacityChanged, errHandleAllocation
		}
		return 0, 0, nil, errHandleAllocation
	}
	for index := range s.slots {
		record := &s.slots[index]
		if record.live || record.reserved || record.generation == maxBufferGeneration {
			continue
		}
		record.generation++
		record.reserved = true
		s.liveCount++
		s.liveBytes += size
		return index, record.generation, nil, nil
	}
	if s.capacityCanRecoverLocked(size) {
		return 0, 0, s.capacityChanged, errHandleAllocation
	}
	return 0, 0, nil, errHandleAllocation
}

func (s *bufferStore) capacityCanRecoverLocked(size uint64) bool {
	freeUsable := false
	futureUsable := false
	for index := range s.slots {
		record := &s.slots[index]
		if !record.live && !record.reserved && record.generation < maxBufferGeneration {
			freeUsable = true
		}
		if (record.live || record.reserved) && record.generation < maxBufferGeneration {
			futureUsable = true
		}
	}
	if s.liveCount >= s.limits.count {
		return futureUsable
	}
	if size > s.limits.bytes-s.liveBytes {
		return (freeUsable || futureUsable) && s.liveBytes != 0
	}
	return futureUsable
}

func (s *bufferStore) rollback(slot int, generation, size uint64) {
	s.mu.Lock()
	record := &s.slots[slot]
	if record.reserved && !record.live && record.generation == generation {
		record.reserved = false
		record.generation--
		s.liveCount--
		s.liveBytes -= size
		s.notifyCapacityLocked()
	}
	s.mu.Unlock()
}

func (s *bufferStore) notifyCapacityLocked() {
	close(s.capacityChanged)
	s.capacityChanged = make(chan struct{})
}

func validateABIBufferSize(size uint64) error {
	if size > maxABIBufferBytes {
		return errHandleAllocation
	}
	return nil
}

func (s *bufferStore) read(handle, offset uint64, destination []byte) (uint64, error) {
	if handle == emergencyQueueFullHandle {
		if offset > uint64(len(emergencyQueueFullJSON)) {
			return 0, errInvalidABIHandle
		}
		return uint64(copy(destination, emergencyQueueFullJSON[offset:])), nil
	}
	slot, generation, ok := decodeBufferHandle(handle)
	if !ok || slot >= len(s.slots) {
		return 0, errInvalidABIHandle
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record := &s.slots[slot]
	if !record.live || record.reserved || record.generation != generation || offset > uint64(len(record.data)) {
		return 0, errInvalidABIHandle
	}
	return uint64(copy(destination, record.data[offset:])), nil
}

func (s *bufferStore) free(handle uint64) error {
	if handle == emergencyQueueFullHandle {
		return nil
	}
	slot, generation, ok := decodeBufferHandle(handle)
	if !ok || slot >= len(s.slots) {
		return errInvalidABIHandle
	}
	s.mu.Lock()
	record := &s.slots[slot]
	if !record.live || record.reserved || record.generation != generation {
		s.mu.Unlock()
		return errInvalidABIHandle
	}
	data := record.data
	record.data = nil
	record.live = false
	s.liveCount--
	s.liveBytes -= uint64(len(data))
	clear(data)
	s.notifyCapacityLocked()
	s.mu.Unlock()
	return nil
}
