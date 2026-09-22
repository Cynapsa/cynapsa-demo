// Package payload owns private local payload handles and materialization.
package payload

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"math"
	"strings"
	"sync"
)

const (
	payloadHandlePrefix  = "payh_"
	payloadHandleBytes   = 32
	maximumHandleRetries = 8
	handleChunkBytes     = 64 << 10
)

type HandleState uint8

const (
	HandleOpen HandleState = iota + 1
	HandleWriting
	// HandleFinishing is an internal transient state while strict canonical
	// validation runs. It is never projected outside the core.
	HandleFinishing
	HandleCompleted
	HandleCancelled
	HandleReleased
)

type HandleLimits struct {
	MaximumHandles int
	// MaximumBytes bounds all persistent allocated chunk and completed
	// canonical storage. Finish workspaces are accounted separately and are
	// bounded by the same value, so total peak storage cannot exceed twice it.
	MaximumBytes      int64
	MaximumPerHandle  int64
	MaximumWriteBytes int
	MaximumReadBytes  int
}

func (l HandleLimits) validate() error {
	if l.MaximumHandles <= 0 || l.MaximumHandles > 1<<20 || l.MaximumBytes <= 0 || l.MaximumBytes > MaximumCanonicalBytes || l.MaximumPerHandle <= 0 || l.MaximumPerHandle > l.MaximumBytes || l.MaximumWriteBytes <= 0 || l.MaximumWriteBytes > 1<<20 || l.MaximumReadBytes <= 0 || l.MaximumReadBytes > 1<<20 {
		return ErrInvalidLimits
	}
	return nil
}

type handleChunk struct {
	data []byte
	used int
}

type handleEntry struct {
	state          HandleState
	chunks         []handleChunk
	chunkBytes     int64
	canonicalBytes int64
	data           []byte
	refs           uint64
	digest         [32]byte
}

// HandleStore owns opaque process-local payload content. Registry membership,
// not token contents, supplies core scoping.
type HandleStore struct {
	mu         sync.RWMutex
	limits     HandleLimits
	random     io.Reader
	entries    map[string]*handleEntry
	totalBytes int64
	// finishingBytes accounts exact temporary canonical buffers while strict
	// validation runs. Those buffers never consume the persistent quota twice.
	finishingBytes int64
	validate       func([]byte) error
	closed         bool
	active         sync.WaitGroup
}

func NewHandleStore(limits HandleLimits, random io.Reader) (*HandleStore, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if random == nil {
		random = rand.Reader
	}
	serializer, err := NewSerializer(limits.MaximumPerHandle)
	if err != nil {
		return nil, err
	}
	return &HandleStore{
		limits: limits, random: random, entries: make(map[string]*handleEntry),
		validate: serializer.Validate,
	}, nil
}

// Open allocates a bounded opaque local payload handle.
func (s *HandleStore) Open() (string, error) {
	if s == nil {
		return "", ErrInvalidHandle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrInvalidHandleState
	}
	if len(s.entries) >= s.limits.MaximumHandles {
		return "", ErrHandleCapacity
	}
	for attempt := 0; attempt < maximumHandleRetries; attempt++ {
		raw := make([]byte, payloadHandleBytes)
		if _, err := io.ReadFull(s.random, raw); err != nil {
			zero(raw)
			return "", ErrInvalidHandle
		}
		handle := payloadHandlePrefix + base64.RawURLEncoding.EncodeToString(raw)
		zero(raw)
		if _, exists := s.entries[handle]; exists {
			continue
		}
		s.entries[handle] = &handleEntry{state: HandleOpen, refs: 1}
		return handle, nil
	}
	return "", ErrHandleCapacity
}

// Write appends one bounded raw chunk without changing state on rejection.
func (s *HandleStore) Write(handle string, chunk []byte) error {
	if s == nil || !validPayloadHandle(handle) {
		return ErrInvalidHandle
	}
	if len(chunk) > s.limits.MaximumWriteBytes {
		return ErrPayloadTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return ErrInvalidHandle
	}
	if entry.state != HandleOpen && entry.state != HandleWriting {
		return ErrInvalidHandleState
	}
	if len(chunk) == 0 {
		return nil
	}
	newSize, err := checkedAdd(entry.canonicalBytes, len(chunk))
	if err != nil || newSize > s.limits.MaximumPerHandle {
		return ErrPayloadTooLarge
	}

	available := 0
	if len(entry.chunks) > 0 {
		tail := entry.chunks[len(entry.chunks)-1]
		available = len(tail.data) - tail.used
	}
	required := len(chunk) - available
	if required < 0 {
		required = 0
	}
	plannedBytes := int64(0)
	plannedChunks := 0
	allocated := entry.chunkBytes
	for required > 0 {
		remaining := s.limits.MaximumPerHandle - allocated - plannedBytes
		if remaining <= 0 {
			return ErrPayloadTooLarge
		}
		blockBytes := int64(handleChunkBytes)
		if remaining < blockBytes {
			blockBytes = remaining
		}
		plannedBytes += blockBytes
		plannedChunks++
		required -= int(blockBytes)
	}
	newTotal := s.totalBytes + plannedBytes
	if newTotal < s.totalBytes || newTotal > s.limits.MaximumBytes {
		return ErrHandleCapacity
	}

	newChunks := make([]handleChunk, plannedChunks)
	remainingAllocation := plannedBytes
	for index := range newChunks {
		blockBytes := int64(handleChunkBytes)
		if remainingAllocation < blockBytes {
			blockBytes = remainingAllocation
		}
		newChunks[index].data = make([]byte, int(blockBytes))
		remainingAllocation -= blockBytes
	}
	entry.chunks = append(entry.chunks, newChunks...)
	input := chunk
	for index := range entry.chunks {
		part := &entry.chunks[index]
		if part.used == len(part.data) {
			continue
		}
		copied := copy(part.data[part.used:], input)
		part.used += copied
		input = input[copied:]
		if len(input) == 0 {
			break
		}
	}
	if len(input) != 0 {
		panic("payload: internal chunk planning mismatch")
	}
	entry.chunkBytes += plannedBytes
	entry.canonicalBytes = newSize
	entry.state = HandleWriting
	s.totalBytes = newTotal
	return nil
}

// Finish seals a handle into an immutable complete canonical snapshot.
func (s *HandleStore) Finish(handle string) error {
	if s == nil || !validPayloadHandle(handle) {
		return ErrInvalidHandle
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		s.mu.Unlock()
		return ErrInvalidHandle
	}
	if entry.state != HandleOpen && entry.state != HandleWriting {
		s.mu.Unlock()
		return ErrInvalidHandleState
	}
	previousState := entry.state
	canonicalBytes := entry.canonicalBytes
	workspaceBytes := s.finishingBytes + canonicalBytes
	if workspaceBytes < s.finishingBytes || workspaceBytes > s.limits.MaximumBytes {
		s.mu.Unlock()
		return ErrHandleCapacity
	}
	entry.state = HandleFinishing
	s.finishingBytes = workspaceBytes
	s.active.Add(1)
	s.mu.Unlock()

	var canonical []byte
	committed := false
	defer func() {
		recovered := recover()
		if !committed {
			zero(canonical)
			s.mu.Lock()
			s.finishingBytes -= canonicalBytes
			if entry.state == HandleFinishing {
				entry.state = previousState
			}
			s.mu.Unlock()
		}
		s.active.Done()
		if recovered != nil {
			panic(recovered)
		}
	}()

	canonical = make([]byte, int(canonicalBytes))
	offset := 0
	for _, part := range entry.chunks {
		offset += copy(canonical[offset:], part.data[:part.used])
	}
	if int64(offset) != canonicalBytes {
		panic("payload: internal canonical materialization mismatch")
	}
	if validationErr := s.validate(canonical); validationErr != nil {
		return validationErr
	}
	digest := Digest(canonical)

	s.mu.Lock()
	zeroChunks(entry.chunks)
	s.totalBytes += canonicalBytes - entry.chunkBytes
	s.finishingBytes -= canonicalBytes
	entry.chunks = nil
	entry.chunkBytes = 0
	entry.data = canonical
	entry.digest = digest
	entry.state = HandleCompleted
	committed = true
	s.mu.Unlock()
	return nil
}

// Cancel abandons an unfinished handle and zeroes its buffered bytes. It is
// idempotent for cancelled and completed live handles. Cancelling a completed
// handle does not change its immutable bytes or ownership count.
func (s *HandleStore) Cancel(handle string) error {
	if s == nil || !validPayloadHandle(handle) {
		return ErrInvalidHandle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return ErrInvalidHandle
	}
	if entry.state == HandleCancelled {
		return nil
	}
	if entry.state == HandleCompleted {
		return nil
	}
	if entry.state != HandleOpen && entry.state != HandleWriting {
		return ErrInvalidHandleState
	}
	s.totalBytes -= entry.chunkBytes
	zeroChunks(entry.chunks)
	entry.chunks = nil
	entry.chunkBytes = 0
	entry.canonicalBytes = 0
	entry.state = HandleCancelled
	return nil
}

// Size reports the exact canonical byte count for a completed handle without
// copying or materializing its immutable contents.
func (s *HandleStore) Size(handle string) (int64, error) {
	if s == nil || !validPayloadHandle(handle) {
		return 0, ErrInvalidHandle
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return 0, ErrInvalidHandle
	}
	if entry.state != HandleCompleted {
		return 0, ErrInvalidHandleState
	}
	return int64(len(entry.data)), nil
}

// Read returns a bounded ownership-safe range from a completed handle.
func (s *HandleStore) Read(handle string, offset int64, limit int) ([]byte, bool, error) {
	if s == nil || !validPayloadHandle(handle) {
		return nil, false, ErrInvalidHandle
	}
	if offset < 0 || limit <= 0 || limit > s.limits.MaximumReadBytes {
		return nil, false, ErrInvalidLimits
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return nil, false, ErrInvalidHandle
	}
	if entry.state != HandleCompleted {
		return nil, false, ErrInvalidHandleState
	}
	if offset > int64(len(entry.data)) {
		return nil, false, ErrInvalidLimits
	}
	end := offset + int64(limit)
	if end < offset || end > int64(len(entry.data)) {
		end = int64(len(entry.data))
	}
	return clone(entry.data[offset:end]), end == int64(len(entry.data)), nil
}

func (s *HandleStore) Snapshot(handle string) ([]byte, [32]byte, error) {
	if s == nil || !validPayloadHandle(handle) {
		return nil, [32]byte{}, ErrInvalidHandle
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, [32]byte{}, ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return nil, [32]byte{}, ErrInvalidHandle
	}
	if entry.state != HandleCompleted {
		return nil, [32]byte{}, ErrInvalidHandleState
	}
	return clone(entry.data), entry.digest, nil
}

// Retain increments explicit local ownership without allowing overflow.
func (s *HandleStore) Retain(handle string) error {
	if s == nil || !validPayloadHandle(handle) {
		return ErrInvalidHandle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return ErrInvalidHandle
	}
	if entry.state != HandleCompleted {
		return ErrInvalidHandleState
	}
	if entry.refs == math.MaxUint64 {
		return ErrReferenceOverflow
	}
	entry.refs++
	return nil
}

// Release decrements ownership and destroys content at the final reference.
func (s *HandleStore) Release(handle string) error {
	if s == nil || !validPayloadHandle(handle) {
		return ErrInvalidHandle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrInvalidHandleState
	}
	entry, ok := s.entries[handle]
	if !ok {
		return ErrInvalidHandle
	}
	if entry.refs == 0 {
		return ErrInvalidHandleState
	}
	if entry.state == HandleFinishing {
		return ErrInvalidHandleState
	}
	entry.refs--
	if entry.refs != 0 {
		return nil
	}
	s.totalBytes -= entry.chunkBytes + int64(len(entry.data))
	zeroChunks(entry.chunks)
	zero(entry.data)
	entry.chunks = nil
	entry.chunkBytes = 0
	entry.canonicalBytes = 0
	entry.data = nil
	entry.state = HandleReleased
	delete(s.entries, handle)
	return nil
}

func (s *HandleStore) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.active.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for handle, entry := range s.entries {
		zeroChunks(entry.chunks)
		zero(entry.data)
		entry.chunks = nil
		entry.chunkBytes = 0
		entry.canonicalBytes = 0
		entry.data = nil
		entry.state = HandleReleased
		delete(s.entries, handle)
	}
	s.totalBytes = 0
	s.finishingBytes = 0
}

func validPayloadHandle(handle string) bool {
	if !strings.HasPrefix(handle, payloadHandlePrefix) {
		return false
	}
	encoded := strings.TrimPrefix(handle, payloadHandlePrefix)
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	valid := err == nil && len(decoded) == payloadHandleBytes && base64.RawURLEncoding.EncodeToString(decoded) == encoded
	zero(decoded)
	return valid
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

func zeroChunks(chunks []handleChunk) {
	for index := range chunks {
		zero(chunks[index].data)
		chunks[index].data = nil
		chunks[index].used = 0
	}
}
