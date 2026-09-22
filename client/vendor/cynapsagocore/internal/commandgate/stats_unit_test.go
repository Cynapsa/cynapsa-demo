package commandgate

import (
	"context"
	"errors"
	"testing"
)

func TestStatsAccountsForBoundedResources(t *testing.T) {
	gate := newTestGate(t, 2)
	mustSubmit(t, gate, "one")
	mustSubmit(t, gate, "two")
	stats := gate.Stats()
	if stats.Capacity != 2 || stats.CommandQueueDepth != 2 || stats.CompletionQueueDepth != 0 || stats.RegistryEntries != 2 || stats.Closing || stats.Closed {
		t.Fatalf("admitted Stats() = %+v", stats)
	}

	dispatch := mustDispatch(t, gate, "one")
	if err := gate.Complete(dispatch.Completion, testEmptyResult("one")); err != nil {
		t.Fatal(err)
	}
	stats = gate.Stats()
	if stats.CommandQueueDepth != 1 || stats.CompletionQueueDepth != 1 || stats.RegistryEntries != 2 {
		t.Fatalf("completed Stats() = %+v", stats)
	}
	mustCompletion(t, gate)
	stats = gate.Stats()
	if stats.CompletionQueueDepth != 0 || stats.RegistryEntries != 1 {
		t.Fatalf("consumed Stats() = %+v", stats)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = gate.Shutdown(ctx)
	<-gate.finalizeDone
	stats = gate.Stats()
	if !stats.Closing || !stats.Closed || stats.CommandQueueDepth != 0 || stats.CompletionQueueDepth != 0 || stats.RegistryEntries != 0 {
		t.Fatalf("closed Stats() = %+v", stats)
	}
}

func TestBeginShutdownStopsAdmissionWithoutWaitingForDrain(t *testing.T) {
	gate := newTestGate(t, 1)
	mustSubmit(t, gate, "pending")
	gate.BeginShutdown()
	if stats := gate.Stats(); !stats.Closing || stats.Closed || stats.CompletionQueueDepth != 1 {
		t.Fatalf("BeginShutdown Stats() = %+v", stats)
	}
	if _, err := gate.Submit(context.Background(), testCommand("late")); !errors.Is(err, ErrGateClosing) {
		t.Fatalf("Submit() after BeginShutdown error = %v", err)
	}
	completion := mustCompletion(t, gate)
	assertFailure(t, completion, codeShutdown, ErrGateClosing)
	if err := gate.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
