package rpc

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestCorrelationUsesFrozenProtocolForm(t *testing.T) {
	id, err := NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.NewInlinePayload("aztm.native", nil)
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{
		Version:          protocol.Version2,
		MessageID:        "msg_AAAAAAAAAAAAAAAAAAAAAA",
		ConversationID:   "conv_qSCA93DIAmzFF9PzMSz5IgM3KyUppjil_FrmXT_JeM4",
		Sender:           "sender",
		Recipient:        "recipient",
		MeshID:           "mesh",
		Mode:             protocol.ModeRequest,
		ExpiresAt:        time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC),
		CorrelationID:    id,
		CreatedAt:        time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ClockUncertainty: 250 * time.Millisecond,
		Payload:          payload,
		CredentialProof:  []byte("proof"),
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		t.Fatalf("generated correlation is not protocol-valid: %v", err)
	}
}

func TestRequestHandleConcurrentUniqueness(t *testing.T) {
	const count = 2_048
	values := make(chan string, count)
	var wait sync.WaitGroup
	for range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, err := NewRequestHandle()
			if err != nil {
				t.Errorf("NewRequestHandle: %v", err)
				return
			}
			values <- handle
		}()
	}
	wait.Wait()
	close(values)
	seen := make(map[string]struct{}, count)
	for handle := range values {
		if _, duplicate := seen[handle]; duplicate {
			t.Fatalf("duplicate request handle %q", handle)
		}
		seen[handle] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("generated %d handles, want %d", len(seen), count)
	}
}

func TestRequestHandleExactFormAndCollisionRetry(t *testing.T) {
	first := bytes.Repeat([]byte{1}, requestHandleEntropyBytes)
	second := bytes.Repeat([]byte{2}, requestHandleEntropyBytes)
	seen := ""
	handle, err := (HandleGenerator{
		Reader:      bytes.NewReader(append(first, second...)),
		MaxAttempts: 2,
		Exists: func(candidate string) bool {
			if seen == "" {
				seen = candidate
			}
			return candidate == seen
		},
	}).Generate()
	if err != nil {
		t.Fatal(err)
	}
	if handle == seen || len(handle) != len(requestHandlePrefix)+43 {
		t.Fatalf("unexpected handle %q", handle)
	}
	if err := ValidateRequestHandle(handle); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHandleRejectsMalformedAndGeneratorBounds(t *testing.T) {
	valid, err := (HandleGenerator{Reader: bytes.NewReader(make([]byte, requestHandleEntropyBytes))}).Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, handle := range []string{
		"",
		"payh_" + valid[len(requestHandlePrefix):],
		valid[:len(valid)-1],
		valid + "=",
		requestHandlePrefix + "++++++++++++++++++++++++++++++++++++++++++A",
	} {
		if err := ValidateRequestHandle(handle); !errors.Is(err, ErrInvalidHandle) {
			t.Fatalf("handle %q: got %v", handle, err)
		}
	}
	if _, err := (HandleGenerator{MaxAttempts: requestHandleAttempts + 1}).Generate(); !errors.Is(err, ErrHandleCollision) {
		t.Fatalf("attempt cap: got %v", err)
	}
	if _, err := (HandleGenerator{Reader: bytes.NewReader(nil)}).Generate(); !errors.Is(err, ErrHandleEntropy) {
		t.Fatalf("entropy: got %v", err)
	}
}
