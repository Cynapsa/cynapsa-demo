package commandgate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func FuzzRegistryTransitionBounds(f *testing.F) {
	f.Add("seed", []byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add("", []byte{7, 7, 7, 7, 7})
	f.Add("collision", []byte{0, 0, 1, 2, 3, 4, 5, 6, 0, 2})

	f.Fuzz(func(t *testing.T, rawID string, operations []byte) {
		const capacity = 4
		registry := NewRegistry(capacity)
		if len(rawID) > 64 {
			rawID = rawID[:64]
		}
		if len(operations) > 256 {
			operations = operations[:256]
		}

		type authority struct {
			handle     CommandHandle
			completion CompletionCapability
		}
		authorities := make(map[string]authority, capacity)
		for index, operation := range operations {
			commandID := fmt.Sprintf("%s-%d", rawID, operation%7)
			current := authorities[commandID]
			switch operation % 8 {
			case 0:
				handle, err := registry.Register(commandID)
				if err == nil {
					authorities[commandID] = authority{handle: handle}
				} else if !expectedRegistryFuzzError(err) {
					t.Fatalf("step %d Register(%q) error = %v", index, commandID, err)
				}
			case 1:
				_, capability, err := registry.markDispatched(commandID)
				if err == nil {
					current.completion = capability
					authorities[commandID] = current
				} else if !expectedRegistryFuzzError(err) {
					t.Fatalf("step %d markDispatched(%q) error = %v", index, commandID, err)
				}
			case 2:
				err := registry.complete(current.completion, model.Result{CommandID: commandID})
				if err != nil && !expectedRegistryFuzzError(err) {
					t.Fatalf("step %d complete(%q) error = %v", index, commandID, err)
				}
			case 3:
				_, err := registry.cancelCommand(current.handle, ErrCommandCancelled)
				if err != nil && !expectedRegistryFuzzError(err) {
					t.Fatalf("step %d cancelCommand(%q) error = %v", index, commandID, err)
				}
			case 4:
				registry.consumeTerminal(commandID)
			case 5:
				// Exercise a capability from another logical admission. It must
				// never terminalize this command.
				foreign := authorities[fmt.Sprintf("%s-%d", rawID, (operation+1)%7)]
				err := registry.complete(foreign.completion, model.Result{CommandID: commandID})
				if err != nil && !expectedRegistryFuzzError(err) {
					t.Fatalf("step %d foreign complete(%q) error = %v", index, commandID, err)
				}
			case 6:
				registry.removeAdmitted(commandID)
			case 7:
				if operation&0x80 != 0 {
					registry.reset()
					authorities = make(map[string]authority, capacity)
				}
			}

			registry.mu.Lock()
			if len(registry.entries) > capacity || len(registry.activeHandles) > capacity || len(registry.terminalOrder) > capacity || len(registry.terminalSet) > capacity || len(registry.terminalHandles) > capacity {
				t.Fatalf("step %d exceeded bound: entries=%d active=%d order=%d IDs=%d handles=%d", index, len(registry.entries), len(registry.activeHandles), len(registry.terminalOrder), len(registry.terminalSet), len(registry.terminalHandles))
			}
			for commandKey, entry := range registry.entries {
				if entry.ctx == nil || entry.cancel == nil || entry.handle == "" || entry.completion == nil || entry.completion.commandKey != commandKey {
					t.Fatalf("step %d invalid live entry %x: %+v", index, commandKey, entry)
				}
				if got, ok := registry.activeHandles[entry.handle]; !ok || got != commandKey {
					t.Fatalf("step %d missing handle binding for %x", index, commandKey)
				}
			}
			registry.mu.Unlock()
		}
		registry.reset()
	})
}

func expectedRegistryFuzzError(err error) bool {
	return errors.Is(err, ErrEmptyCommandID) ||
		errors.Is(err, ErrEmptyCommandHandle) ||
		errors.Is(err, ErrDuplicateCommandID) ||
		errors.Is(err, ErrAlreadyTerminal) ||
		errors.Is(err, ErrRegistryFull) ||
		errors.Is(err, ErrCommandNotFound) ||
		errors.Is(err, ErrInvalidCompletionCapability) ||
		errors.Is(err, ErrStaleCompletionCapability) ||
		errors.Is(err, ErrAlreadyDispatched) ||
		errors.Is(err, ErrInvalidRegistryState) ||
		errors.Is(err, ErrCommandHandleGeneration) ||
		errors.Is(err, ErrCommandHandleExhausted) ||
		errors.Is(err, ErrNilContext) ||
		errors.Is(err, context.Canceled)
}
