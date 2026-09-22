package diagnostics

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestEventAcceptsOnlyMatchingClosedVariant(t *testing.T) {
	now := time.Unix(1, 0)
	event, err := Event("evt-1", "core.error", now, model.CoreErrorEvent{Err: &model.Error{Code: "failed"}})
	if err != nil || event.ID != "evt-1" || event.Name != "core.error" || !event.CreatedAt.Equal(now) {
		t.Fatalf("Event() = (%+v, %v)", event, err)
	}
	if _, err := Event("evt-2", "message.received", now, model.CoreErrorEvent{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("mismatched Event() error = %v", err)
	}
	if _, err := Event("", "core.error", now, model.CoreErrorEvent{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("empty Event() error = %v", err)
	}
	for _, name := range []string{"delivery.ordering_blocked", "delivery.unknown"} {
		if _, err := Event("evt-retired", name, now, model.MessageStateEvent{MessageID: "message", ConversationID: "conversation", State: "unknown"}); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("retired event %q error = %v", name, err)
		}
	}
}

func TestMetricsAreAllowlistedFiniteAndSnapshotOwned(t *testing.T) {
	metrics := NewMetrics()
	if err := metrics.Record(MetricActiveWorkers, 2); err != nil {
		t.Fatal(err)
	}
	if err := metrics.Add(MetricActiveWorkers, 1); err != nil {
		t.Fatal(err)
	}
	if err := metrics.Record(Metric("peer-derived-label"), 1); !errors.Is(err, ErrUnknownMetric) {
		t.Fatalf("unknown metric error = %v", err)
	}
	if err := metrics.Record(MetricWorkerPanics, math.NaN()); !errors.Is(err, ErrInvalidMetric) {
		t.Fatalf("NaN metric error = %v", err)
	}
	snapshot := metrics.Snapshot()
	if snapshot[MetricActiveWorkers] != 3 {
		t.Fatalf("metric = %v, want 3", snapshot[MetricActiveWorkers])
	}
	snapshot[MetricActiveWorkers] = 99
	if metrics.Snapshot()[MetricActiveWorkers] != 3 {
		t.Fatal("snapshot mutation changed metric owner")
	}
}

func TestRedactRemovesSecretsAndBoundsContent(t *testing.T) {
	long := strings.Repeat("x", maxDiagnosticString+10)
	input := map[string]any{
		"safe":              long,
		"password":          "secret",
		"database_password": "also secret",
		"nested": map[string]any{
			"Authorization": "bearer secret",
			"count":         3,
		},
		"bytes": []byte("payload"),
		"err":   errors.New("private dependency text"),
	}
	result := Redact(input)
	if result["password"] != redactedValue || result["database_password"] != redactedValue || result["bytes"] != redactedValue || result["err"] != redactedValue {
		t.Fatalf("redacted result = %#v", result)
	}
	if len(result["safe"].(string)) != maxDiagnosticString {
		t.Fatalf("safe string length = %d", len(result["safe"].(string)))
	}
	nested := result["nested"].(map[string]any)
	if nested["Authorization"] != redactedValue || nested["count"] != 3 {
		t.Fatalf("nested redaction = %#v", nested)
	}
	if input["password"] != "secret" {
		t.Fatal("Redact mutated caller input")
	}
}

func TestCaptureCopiesMetricsAndRedactsPrivateState(t *testing.T) {
	metrics := NewMetrics()
	if err := metrics.Record(MetricPendingTimers, 4); err != nil {
		t.Fatal(err)
	}
	snapshot := Capture(Observation{EventQueueDepth: 2, Private: map[string]any{"token": "secret"}}, metrics)
	if snapshot.EventQueueDepth != 2 || snapshot.Metrics[MetricPendingTimers] != 4 || snapshot.Private["token"] != redactedValue {
		t.Fatalf("Capture() = %#v", snapshot)
	}
}

func TestRedactBoundsWideMapsDeterministically(t *testing.T) {
	input := make(map[string]any, maxDiagnosticFields+100)
	for index := maxDiagnosticFields + 99; index >= 0; index-- {
		input[fmt.Sprintf("field-%03d", index)] = index
	}
	first := Redact(input)
	second := Redact(input)
	if len(first) != maxDiagnosticFields+1 || first["_truncated"] != true {
		t.Fatalf("bounded result has %d fields: %#v", len(first), first)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("bounded redaction is not deterministic")
	}
	if _, retained := first["field-000"]; !retained {
		t.Fatal("deterministic lowest key was not retained")
	}
	if _, retained := first[fmt.Sprintf("field-%03d", maxDiagnosticFields+99)]; retained {
		t.Fatal("key outside the deterministic bound was retained")
	}
}

func TestMetricsAndSnapshotsAreConcurrentSafe(t *testing.T) {
	metrics := NewMetrics()
	const (
		workers    = 32
		increments = 100
	)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range increments {
				if err := metrics.Add(MetricCommandsComplete, 1); err != nil {
					t.Errorf("Add() error = %v", err)
					return
				}
				_ = Capture(Observation{CommandQueueDepth: 1}, metrics)
			}
		}()
	}
	group.Wait()
	if got := metrics.Snapshot()[MetricCommandsComplete]; got != workers*increments {
		t.Fatalf("commands complete = %v, want %d", got, workers*increments)
	}
}
