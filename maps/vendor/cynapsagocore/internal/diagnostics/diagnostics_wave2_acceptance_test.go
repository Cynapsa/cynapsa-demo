package diagnostics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAcceptanceConcurrentDiagnosticsRemainBoundedAndOwned(t *testing.T) {
	metrics := NewMetrics()
	const (
		writers    = 32
		readers    = 32
		iterations = 500
	)
	private := make(map[string]any, maxDiagnosticFields*4)
	for index := 0; index < maxDiagnosticFields*4; index++ {
		private[fmt.Sprintf("field-%04d", index)] = index
	}
	private["password"] = "must-not-escape"

	start := make(chan struct{})
	var snapshots atomic.Int64
	var group sync.WaitGroup
	group.Add(writers + readers)
	for range writers {
		go func() {
			defer group.Done()
			<-start
			for range iterations {
				if err := metrics.Add(MetricCommandsComplete, 1); err != nil {
					t.Errorf("Add() error = %v", err)
					return
				}
			}
		}()
	}
	for reader := range readers {
		reader := reader
		go func() {
			defer group.Done()
			<-start
			for iteration := range iterations {
				snapshot := Capture(Observation{
					CommandQueueDepth:  (reader + iteration) % 64,
					EventQueueCapacity: 64,
					Private:            private,
				}, metrics)
				if len(snapshot.Metrics) > len(allowedMetrics) {
					t.Errorf("metric cardinality = %d", len(snapshot.Metrics))
					return
				}
				if len(snapshot.Private) > maxDiagnosticFields+1 || snapshot.Private["_truncated"] != true {
					t.Errorf("private snapshot not bounded: %d fields", len(snapshot.Private))
					return
				}
				if secret, retained := snapshot.Private["password"]; retained && secret != redactedValue {
					t.Errorf("password escaped: %#v", secret)
					return
				}
				snapshot.Metrics[MetricCommandsComplete] = -1
				snapshots.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()

	if got, want := metrics.Snapshot()[MetricCommandsComplete], float64(writers*iterations); got != want {
		t.Fatalf("commands complete = %v, want %v", got, want)
	}
	if got, want := snapshots.Load(), int64(readers*iterations); got != want {
		t.Fatalf("snapshots = %d, want %d", got, want)
	}
}
