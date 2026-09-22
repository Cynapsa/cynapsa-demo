package rank2xmpp

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestFrozenApplicationReplayRejectsAliasBeforeSelectiveClear(t *testing.T) {
	shared := []byte("aliased-application-replay")
	source := []Stanza{
		{Kind: StanzaTimeCalibration, MessageID: "discard", Data: shared},
		{Kind: StanzaSignal, AttemptID: "retain", Data: shared},
	}
	replay, err := applicationReplay(source)
	if !errors.Is(err, ErrStreamManagement) || len(replay) != 0 {
		t.Fatalf("alias result=%#v error=%v", replay, err)
	}
	for index := range source {
		if !reflect.DeepEqual(source[index], Stanza{}) {
			t.Fatalf("source[%d] retained ownership: %#v", index, source[index])
		}
	}
	assertZeroBytes(t, shared)
}

func TestFrozenCanceledPendingForReplayCannotDetach(t *testing.T) {
	for attempt := 0; attempt < 1000; attempt++ {
		management, err := NewStreamManagement(1, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = management.Enable("resume", true); err != nil {
			t.Fatal(err)
		}
		secret := []byte("cancel-owned-replay")
		if err = management.RecordSent(Stanza{Kind: StanzaSignal, AttemptID: "cancel", Data: secret}); err != nil {
			t.Fatal(err)
		}
		session := &melliumSession{management: management}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		replay, snapshotErr := session.PendingForReplay(ctx)
		if snapshotErr == nil {
			clearStanzas(replay)
			t.Fatalf("attempt %d: canceled snapshot detached ledger and returned success", attempt)
		}
		if !errors.Is(snapshotErr, context.Canceled) {
			t.Fatalf("attempt %d: error=%v, want context.Canceled", attempt, snapshotErr)
		}
		if management.Pending() != 1 {
			t.Fatalf("attempt %d: canceled snapshot mutated ledger", attempt)
		}
		management.Close()
	}
}
