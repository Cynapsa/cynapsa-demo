package rank2xmpp

import (
	"context"
	"errors"
	"testing"
)

type pendingReplayCall func(context.Context) ([]Stanza, error)

func (call pendingReplayCall) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	return call(ctx)
}

func TestPendingForReplaySessionClearsResultReturnedWithError(t *testing.T) {
	dependencyErr := errors.New("pending replay failed")
	secret := []byte("returned replay secret")
	result, err := pendingForReplaySession(pendingReplayCall(func(context.Context) ([]Stanza, error) {
		return []Stanza{{Kind: StanzaSignal, Data: secret}}, dependencyErr
	}), context.Background())
	if !errors.Is(err, dependencyErr) || result != nil {
		t.Fatalf("pendingForReplaySession() = %#v, %v", result, err)
	}
	if !allZero(secret) {
		t.Fatalf("error retained dependency result=%x", secret)
	}
}

func TestPendingForReplaySessionContainsPanic(t *testing.T) {
	result, err := pendingForReplaySession(pendingReplayCall(func(context.Context) ([]Stanza, error) {
		panic("injected pending replay panic")
	}), context.Background())
	if !errors.Is(err, ErrUnavailable) || result != nil {
		t.Fatalf("pendingForReplaySession() = %#v, %v", result, err)
	}
}

func TestPendingForReplaySessionPreservesSuccessfulOwnership(t *testing.T) {
	secret := []byte("successful replay secret")
	result, err := pendingForReplaySession(pendingReplayCall(func(context.Context) ([]Stanza, error) {
		return []Stanza{{Kind: StanzaSignal, Data: secret}}, nil
	}), context.Background())
	if err != nil || len(result) != 1 || len(result[0].Data) == 0 || &result[0].Data[0] != &secret[0] {
		t.Fatalf("pendingForReplaySession() = %#v, %v", result, err)
	}
	if string(secret) != "successful replay secret" {
		t.Fatalf("success mutated dependency result=%q", secret)
	}
	clearStanzas(result)
	if !allZero(secret) {
		t.Fatalf("successful ownership did not transfer=%x", secret)
	}
}

func TestPendingForReplaySessionPrefersExactContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	secret := []byte("canceled replay secret")
	result, err := pendingForReplaySession(pendingReplayCall(func(context.Context) ([]Stanza, error) {
		cancel()
		return []Stanza{{Kind: StanzaSignal, Data: secret}}, ErrUnavailable
	}), ctx)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("pendingForReplaySession() = %#v, %v", result, err)
	}
	if !allZero(secret) {
		t.Fatalf("context error retained dependency result=%x", secret)
	}
}
