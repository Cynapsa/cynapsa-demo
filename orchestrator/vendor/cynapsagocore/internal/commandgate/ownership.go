package commandgate

import (
	"sync"
	"sync/atomic"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const (
	// MaximumCommandBytes is above the exact largest SDK command (the policy
	// rule catalog) while rejecting unbounded direct internal submissions.
	MaximumCommandBytes int64 = model.MaximumFrozenCommandBytes
	// MaximumGateBytes is the process-wide hard ceiling for all admitted
	// command snapshots, independent of the entry-count limit.
	MaximumGateBytes int64 = 256 << 20
)

type ownedCommand struct {
	mu       sync.Mutex
	command  model.Command
	bytes    int64
	gate     *Gate
	refs     uint32
	terminal bool
	released bool
}

func newOwnedCommand(gate *Gate, command model.Command, bytes int64) *ownedCommand {
	return &ownedCommand{gate: gate, command: command, bytes: bytes}
}

func (owned *ownedCommand) acquireDispatch() (model.Command, bool) {
	if owned == nil {
		return model.Command{}, false
	}
	owned.mu.Lock()
	defer owned.mu.Unlock()
	if owned.terminal || owned.released {
		return model.Command{}, false
	}
	owned.refs++
	return owned.command, true
}

func (owned *ownedCommand) releaseDispatch() {
	if owned == nil {
		return
	}
	var command model.Command
	var gate *Gate
	var bytes int64
	var releaseBytes bool
	owned.mu.Lock()
	if owned.refs > 0 {
		owned.refs--
	}
	if owned.terminal && owned.refs == 0 && !owned.released {
		command, gate, bytes, releaseBytes = owned.detachLocked()
	}
	owned.mu.Unlock()
	owned.release(command, gate, bytes, releaseBytes)
}

func (owned *ownedCommand) terminalize() {
	if owned == nil {
		return
	}
	var command model.Command
	var gate *Gate
	var bytes int64
	var releaseBytes bool
	owned.mu.Lock()
	owned.terminal = true
	if owned.refs == 0 && !owned.released {
		command, gate, bytes, releaseBytes = owned.detachLocked()
	}
	owned.mu.Unlock()
	owned.release(command, gate, bytes, releaseBytes)
}

func (owned *ownedCommand) detachLocked() (model.Command, *Gate, int64, bool) {
	command := owned.command
	gate := owned.gate
	bytes := owned.bytes
	owned.command = model.Command{}
	owned.gate = nil
	owned.bytes = 0
	owned.released = true
	return command, gate, bytes, true
}

func (owned *ownedCommand) release(command model.Command, gate *Gate, bytes int64, releaseBytes bool) {
	if !releaseBytes {
		return
	}
	model.ClearCommand(&command)
	if gate != nil {
		gate.ownedBytes.Add(-bytes)
	}
}

func derivedByteCapacity(capacity int) (int64, bool) {
	if capacity <= 0 {
		return 0, false
	}
	if int64(capacity) > MaximumGateBytes/MaximumCommandBytes {
		return MaximumGateBytes, true
	}
	value := int64(capacity) * MaximumCommandBytes
	if value <= 0 || value > MaximumGateBytes {
		return 0, false
	}
	return value, true
}

func reserveOwnedBytes(counter *atomic.Int64, maximum, bytes int64) bool {
	if counter == nil || maximum <= 0 || bytes <= 0 || bytes > MaximumCommandBytes || bytes > maximum {
		return false
	}
	for {
		current := counter.Load()
		if current < 0 || current > maximum-bytes {
			return false
		}
		if counter.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}
