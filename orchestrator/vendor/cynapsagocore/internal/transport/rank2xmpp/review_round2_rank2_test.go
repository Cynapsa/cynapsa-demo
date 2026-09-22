package rank2xmpp

import (
	"context"
	"testing"
)

func TestReviewPendingForReplayAfterCloseDoesNotPanic(t *testing.T) {
	session := &melliumSession{}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("PendingForReplay panicked after terminal Close: %v", recovered)
		}
	}()
	if _, err := session.PendingForReplay(context.Background()); err == nil {
		t.Fatal("PendingForReplay succeeded after terminal Close")
	}
}

func TestReviewPendingForReplayClearsDetachedSource(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	secret := []byte("detached-replay-secret")
	session := &melliumSession{
		management: management,
		rejected:   []Stanza{{Kind: StanzaSignal, From: "a", To: "b", MeshID: "mesh", AttemptID: "attempt", Data: secret}},
	}
	replay, err := session.PendingForReplay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clearStanzas(replay)
	for i, value := range secret {
		if value != 0 {
			t.Fatalf("detached replay source remained non-zero at byte %d: %q", i, secret)
		}
	}
}

func BenchmarkReviewMergeReplayOneMiB(b *testing.B) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		primary := []Stanza{{Kind: StanzaSignal, From: "a", To: "b", MeshID: "mesh", AttemptID: "attempt", Data: data}}
		merged, err := mergeReplay(primary, nil)
		if err != nil {
			b.Fatal(err)
		}
		clearStanzas(merged)
	}
}
