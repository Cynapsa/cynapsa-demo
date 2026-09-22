package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestIDGeneratorCollisionRetry(t *testing.T) {
	first := bytes.Repeat([]byte{1}, identifierEntropyBytes)
	second := bytes.Repeat([]byte{2}, identifierEntropyBytes)
	reader := bytes.NewReader(append(first, second...))
	seenFirst := ""
	generator := IDGenerator{
		Reader:      reader,
		MaxAttempts: 2,
		Exists: func(id string) bool {
			if seenFirst == "" {
				seenFirst = id
			}
			return id == seenFirst
		},
	}
	id, err := generator.Generate("msg_")
	if err != nil {
		t.Fatal(err)
	}
	if id == seenFirst || len(id) != len("msg_")+22 {
		t.Fatalf("unexpected retry result %q", id)
	}
}

func TestIDGeneratorFailureBoundaries(t *testing.T) {
	if _, err := (IDGenerator{Reader: bytes.NewReader(nil)}).Generate("msg_"); !errors.Is(err, ErrIdentifierEntropy) {
		t.Fatalf("entropy: got %v", err)
	}
	if _, err := (IDGenerator{Reader: bytes.NewReader(make([]byte, identifierEntropyBytes)), MaxAttempts: 1, Exists: func(string) bool { return true }}).Generate("msg_"); !errors.Is(err, ErrIdentifierCollision) {
		t.Fatalf("collision: got %v", err)
	}
	if _, err := (IDGenerator{MaxAttempts: defaultCollisionTries + 1}).Generate("msg_"); !errors.Is(err, ErrIdentifierCollision) {
		t.Fatalf("attempt cap: got %v", err)
	}
	if _, err := (IDGenerator{}).Generate("custom_"); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("prefix: got %v", err)
	}
}
