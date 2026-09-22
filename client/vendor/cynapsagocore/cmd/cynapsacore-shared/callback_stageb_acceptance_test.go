package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStageBAcceptanceCallbackClearLinearizesAndJoinsAllInflightCalls(t *testing.T) {
	const callbacks = 128
	state := newCallbackState()
	release := make(chan struct{})
	started := make(chan struct{}, callbacks)
	var calls sync.WaitGroup
	for range callbacks {
		calls.Add(1)
		go func() {
			defer calls.Done()
			if !state.begin() {
				t.Error("callback rejected before clear linearized")
				return
			}
			started <- struct{}{}
			<-release
			state.end()
		}()
	}
	for range callbacks {
		<-started
	}

	clearDone := make(chan error, 1)
	go func() { clearDone <- state.clear(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for state.begin() {
		state.end()
		if time.Now().After(deadline) {
			t.Fatal("callback admission did not close")
		}
	}
	select {
	case err := <-clearDone:
		t.Fatalf("clear returned before in-flight callbacks ended: %v", err)
	default:
	}
	close(release)
	calls.Wait()
	if err := <-clearDone; err != nil {
		t.Fatalf("clear: %v", err)
	}
	if state.begin() {
		state.end()
		t.Fatal("callback admitted after clear completed")
	}
	if err := state.clear(context.Background()); err != nil {
		t.Fatalf("repeated clear: %v", err)
	}
}
