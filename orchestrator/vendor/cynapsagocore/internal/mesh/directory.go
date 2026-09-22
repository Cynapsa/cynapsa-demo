package mesh

import (
	"sync"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

type DirectoryConfig struct {
	Capacity int
	MeshID   string
	Now      func() time.Time
}

type DirectorySnapshot struct {
	MeshID     string
	ObservedAt time.Time
	ExpiresAt  time.Time
	Identities []Identity
}

// Directory atomically publishes immutable trusted identity snapshots.
type Directory struct {
	mu       sync.RWMutex
	capacity int
	meshID   string
	now      func() time.Time
	expires  time.Time
	entries  map[string]Identity
}

func NewDirectory(config DirectoryConfig) (*Directory, error) {
	if config.Capacity < 1 || config.Capacity > MaxDirectoryEntries {
		return nil, ErrDirectoryCapacity
	}
	if protocol.ValidateMeshID(config.MeshID) != nil {
		return nil, ErrInvalidConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Directory{capacity: config.Capacity, meshID: config.MeshID, now: now, entries: make(map[string]Identity)}, nil
}

func (directory *Directory) Replace(snapshot DirectorySnapshot) error {
	if directory == nil || snapshot.MeshID != directory.meshID || snapshot.ObservedAt.IsZero() || !snapshot.ExpiresAt.After(snapshot.ObservedAt) || len(snapshot.Identities) > directory.capacity {
		return ErrSnapshotInvalid
	}
	next := make(map[string]Identity, len(snapshot.Identities))
	for _, identity := range snapshot.Identities {
		validated, err := NewIdentity(identity.AgentID, identity.Internal)
		if err != nil {
			return ErrSnapshotInvalid
		}
		if _, duplicate := next[validated.AgentID]; duplicate {
			return ErrSnapshotInvalid
		}
		next[validated.AgentID] = validated
	}
	directory.mu.Lock()
	now := directory.now()
	if snapshot.ObservedAt.After(now) || !now.Before(snapshot.ExpiresAt) {
		directory.mu.Unlock()
		return ErrSnapshotInvalid
	}
	directory.entries = next
	directory.expires = snapshot.ExpiresAt
	directory.mu.Unlock()
	return nil
}

func (directory *Directory) Lookup(agentID string) (Identity, error) {
	if directory == nil {
		return Identity{}, ErrIdentityUnknown
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if directory.expires.IsZero() || !directory.now().Before(directory.expires) {
		return Identity{}, ErrSnapshotStale
	}
	identity, ok := directory.entries[agentID]
	if !ok {
		return Identity{}, ErrIdentityUnknown
	}
	return identity, nil
}
