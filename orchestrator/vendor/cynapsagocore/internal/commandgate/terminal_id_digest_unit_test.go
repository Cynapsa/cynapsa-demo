package commandgate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var benchmarkCommandKey commandIDKey

func TestCommandKeyStreamingMatchesHMACWithoutIDSizedAllocation(t *testing.T) {
	registry := NewRegistry(1)
	for index := range registry.terminalKey {
		registry.terminalKey[index] = byte(index + 1)
	}
	registry.handleScopeReady = true
	for _, input := range []string{"", "short", strings.Repeat("large-identifier-", 1<<16)} {
		mac := hmac.New(sha256.New, registry.terminalKey[:])
		_, _ = mac.Write([]byte(input))
		var expected commandIDKey
		copy(expected[:], mac.Sum(nil))
		if got := registry.commandKeyLocked(input); got != expected {
			t.Fatalf("streamed digest mismatch for %d-byte ID", len(input))
		}
	}

	bytesPerOp := func(input string) int64 {
		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkCommandKey = registry.commandKeyLocked(input)
			}
		})
		return result.AllocedBytesPerOp()
	}
	shortBytes := bytesPerOp("short")
	largeBytes := bytesPerOp(strings.Repeat("x", 1<<20))
	t.Logf("temporary allocation bytes/op: short=%d large=%d", shortBytes, largeBytes)
	if delta := largeBytes - shortBytes; delta > 8<<10 {
		t.Fatalf("1 MiB ID added %d temporary bytes/op (short=%d large=%d), want fixed scratch only", delta, shortBytes, largeBytes)
	}
}

func TestTerminalRegistryRetainsOnlyFixedKeyAfterOwnerRelease(t *testing.T) {
	gate, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{strings.Repeat("a", 1<<20), strings.Repeat("b", 1<<20)}
	for index, id := range ids {
		if _, err := gate.Submit(context.Background(), model.Command{ID: id, Name: "test", Args: model.EmptyArgs{}}); err != nil {
			t.Fatal(err)
		}
		dispatch, err := gate.NextDispatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := gate.Complete(dispatch.Completion, model.Result{CommandID: id, Value: model.EmptyResult{}}); err != nil {
			t.Fatal(err)
		}
		if got := gate.Stats().OwnedBytes; got != 0 {
			t.Fatalf("iteration %d terminal owner bytes = %d", index, got)
		}
		gate.registry.mu.Lock()
		key := gate.registry.commandKeyLocked(id)
		entry := gate.registry.entries[key]
		if entry == nil || entry.commandID != "" {
			gate.registry.mu.Unlock()
			t.Fatalf("iteration %d live terminal retained identifier", index)
		}
		gate.registry.mu.Unlock()
		if _, err := gate.NextCompletion(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	gate.registry.mu.Lock()
	if len(gate.registry.terminalSet) != len(ids) || len(gate.registry.terminalOrder) != len(ids) {
		gate.registry.mu.Unlock()
		t.Fatalf("terminal history sizes = (%d, %d)", len(gate.registry.terminalSet), len(gate.registry.terminalOrder))
	}
	for _, record := range gate.registry.terminalOrder {
		if record.commandKey == (commandIDKey{}) {
			gate.registry.mu.Unlock()
			t.Fatal("terminal history retained an empty digest")
		}
	}
	gate.registry.mu.Unlock()
}

func TestTerminalDigestPreservesHistoryEvictionAndIDReuse(t *testing.T) {
	registry := NewRegistry(2)
	terminalize := func(id string) {
		handle, err := registry.Register(id)
		if err != nil {
			t.Fatalf("Register(%q): %v", id, err)
		}
		_, capability, err := registry.markDispatched(id)
		if err != nil {
			t.Fatalf("markDispatched(%q): %v", id, err)
		}
		if err := registry.complete(capability, model.Result{CommandID: id, Value: model.EmptyResult{}}); err != nil {
			t.Fatalf("complete(%q): %v", id, err)
		}
		if !registry.consumeTerminal(id) {
			t.Fatalf("consumeTerminal(%q) failed", id)
		}
		if err := registry.cancelCommandForTest(handle); !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("terminal handle %q = %v", id, err)
		}
	}
	terminalize("alpha")
	terminalize("beta")
	if _, err := registry.Register("alpha"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("retained alpha Register = %v", err)
	}
	terminalize("gamma")
	if _, err := registry.Register("alpha"); err != nil {
		t.Fatalf("evicted alpha was not reusable: %v", err)
	}

	other := NewRegistry(1)
	if _, err := other.Register("beta"); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	first := registry.commandKeyLocked("beta")
	registry.mu.Unlock()
	other.mu.Lock()
	second := other.commandKeyLocked("beta")
	other.mu.Unlock()
	if first == second {
		t.Fatal("independent registries reused the same process-random digest key")
	}
}

func (r *Registry) cancelCommandForTest(handle CommandHandle) error {
	_, err := r.cancelCommand(handle, ErrCommandCancelled)
	return err
}
