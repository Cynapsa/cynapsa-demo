package rank2xmpp

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestMergeReplayMovesOwnershipAndClearsDuplicate(t *testing.T) {
	firstData := []byte("first-private-replay")
	duplicateData := append([]byte(nil), firstData...)
	secondData := []byte("second-private-replay")
	first := Stanza{Kind: StanzaSignal, From: "a", To: "b", MeshID: "mesh", AttemptID: "one", Data: firstData}
	duplicate := first
	duplicate.Data = duplicateData
	second := Stanza{Kind: StanzaSignalResult, From: "b", To: "a", MeshID: "mesh", AttemptID: "two", Data: secondData}
	primary := []Stanza{first}
	extra := []Stanza{duplicate, second}

	merged, err := mergeReplay(primary, extra)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 2 || merged[0].AttemptID != "one" || merged[1].AttemptID != "two" {
		t.Fatalf("merged order = %#v", merged)
	}
	if !reflect.DeepEqual(primary[0], Stanza{}) || !reflect.DeepEqual(extra[0], Stanza{}) || !reflect.DeepEqual(extra[1], Stanza{}) {
		t.Fatalf("sources retained ownership: primary=%#v extra=%#v", primary, extra)
	}
	assertZeroBytes(t, duplicateData)
	if string(firstData) != "first-private-replay" || string(secondData) != "second-private-replay" {
		t.Fatal("moved payload was cleared before result release")
	}
	clearStanzas(merged)
	assertZeroBytes(t, firstData)
	assertZeroBytes(t, secondData)
}

func TestMergeReplayDigestCollisionUsesExactComparison(t *testing.T) {
	firstData := []byte("first")
	secondData := []byte("second")
	duplicateData := []byte("first")
	first := Stanza{Kind: StanzaSignal, AttemptID: "same", Data: firstData}
	second := Stanza{Kind: StanzaSignal, AttemptID: "same", Data: secondData}
	duplicate := first
	duplicate.Data = duplicateData
	constantDigest := func(Stanza) [32]byte { return [32]byte{1} }

	merged, err := mergeReplayDigest([]Stanza{first}, []Stanza{second, duplicate}, constantDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 2 || string(merged[0].Data) != "first" || string(merged[1].Data) != "second" {
		t.Fatalf("collision changed exact dedupe: %#v", merged)
	}
	assertZeroBytes(t, duplicateData)
	clearStanzas(merged)
	assertZeroBytes(t, firstData)
	assertZeroBytes(t, secondData)
}

func TestMergeReplayRejectsAliasedOwnershipBeforeMove(t *testing.T) {
	shared := []byte("aliased-private-replay")
	first := []Stanza{{Kind: StanzaSignal, AttemptID: "one", Data: shared}}
	second := []Stanza{{Kind: StanzaSignal, AttemptID: "two", Data: shared[1:]}}
	merged, err := mergeReplay(first, second)
	if !errors.Is(err, ErrStreamManagement) || len(merged) != 0 {
		t.Fatalf("merged=%#v err=%v", merged, err)
	}
	if !reflect.DeepEqual(first[0], Stanza{}) || !reflect.DeepEqual(second[0], Stanza{}) {
		t.Fatalf("aliased sources not released: first=%#v second=%#v", first, second)
	}
	assertZeroBytes(t, shared)
}

func TestFilterReplayGenerationRejectsAliasBeforeSelectiveClear(t *testing.T) {
	shared := []byte("aliased-generation-replay")
	source := []Stanza{
		{Kind: StanzaTransferChunk, MeshID: "mesh", Data: shared},
		{Kind: StanzaTransferChunk, MeshID: "mesh", Data: shared},
	}
	filtered, err := filterReplayMesh(source, "mesh")
	if !errors.Is(err, ErrStreamManagement) || len(filtered) != 0 {
		t.Fatalf("filtered=%#v err=%v", filtered, err)
	}
	for index := range source {
		if !reflect.DeepEqual(source[index], Stanza{}) {
			t.Fatalf("source[%d] retained ownership: %#v", index, source[index])
		}
	}
	assertZeroBytes(t, shared)
}

func TestApplicationReplayRejectsDuplicateInboundLeaseOwnership(t *testing.T) {
	budget, err := transport.NewInboundBudget(2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.Acquire(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	source := []Stanza{
		{Kind: StanzaTimeCalibration, inboundLease: lease},
		{Kind: StanzaSignal, inboundLease: lease},
	}
	replay, err := applicationReplay(source)
	if !errors.Is(err, ErrStreamManagement) || len(replay) != 0 {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	if stats := budget.Stats(); stats.Count != 0 || stats.Bytes != 0 {
		t.Fatalf("duplicate lease was not released exactly once: %#v", stats)
	}
}

func TestMergeReplayEnforcesAggregateRecordBudget(t *testing.T) {
	oversized := make([]Stanza, maximumReplayStanzas+1)
	for i := range oversized {
		oversized[i].Kind = StanzaSignal
	}
	merged, err := mergeReplay(oversized, nil)
	if !errors.Is(err, ErrQueueFull) || len(merged) != 0 {
		t.Fatalf("merged=%d err=%v", len(merged), err)
	}
	if !reflect.DeepEqual(oversized[0], Stanza{}) || !reflect.DeepEqual(oversized[len(oversized)-1], Stanza{}) {
		t.Fatal("over-budget input was not released")
	}
}

func TestMergeReplayDoesNotAllocatePayloadSizedKey(t *testing.T) {
	payload := make([]byte, 1<<20)
	allocations := testing.AllocsPerRun(10, func() {
		primary := []Stanza{{Kind: StanzaSignal, AttemptID: "allocation", Data: payload}}
		merged, err := mergeReplay(primary, nil)
		if err != nil {
			panic(err)
		}
		clearStanzas(merged)
	})
	if allocations > 64 {
		t.Fatalf("one-record merge allocations = %.1f, want <= 64 fixed-size allocations", allocations)
	}
}

func TestPendingForReplaySnapshotAdmissionLinearizesWithClose(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	secret := []byte("admitted-before-close")
	session := &melliumSession{
		management: management,
		rejected:   []Stanza{{Kind: StanzaSignal, AttemptID: "admitted", Data: secret}},
	}
	management.mu.Lock()
	pendingResult := make(chan []Stanza, 1)
	pendingErr := make(chan error, 1)
	go func() {
		replay, snapshotErr := session.PendingForReplay(context.Background())
		pendingResult <- replay
		pendingErr <- snapshotErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if !session.mu.TryLock() {
			break
		}
		session.mu.Unlock()
		if time.Now().After(deadline) {
			management.mu.Unlock()
			t.Fatal("PendingForReplay did not reach the snapshot linearization point")
		}
		runtime.Gosched()
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- session.Close(context.Background()) }()
	management.mu.Unlock()

	replay := <-pendingResult
	if err = <-pendingErr; err != nil {
		t.Fatalf("admitted snapshot failed: %v", err)
	}
	if len(replay) != 1 || replay[0].AttemptID != "admitted" {
		t.Fatalf("admitted snapshot = %#v", replay)
	}
	if err = <-closeErr; err != nil {
		t.Fatal(err)
	}
	clearStanzas(replay)
	assertZeroBytes(t, secret)
	if _, err = session.PendingForReplay(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close snapshot error = %v, want ErrClosed", err)
	}
}

func TestPendingForReplayMovesStreamLedgerOwnership(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	caller := []byte("ledger-private-replay")
	if err = management.RecordSent(Stanza{Kind: StanzaSignal, AttemptID: "ledger", Data: caller}); err != nil {
		t.Fatal(err)
	}
	management.mu.Lock()
	owned := &management.pending[0].stanza.Data[0]
	management.mu.Unlock()
	session := &melliumSession{management: management}
	replay, err := session.PendingForReplay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 1 || &replay[0].Data[0] != owned {
		t.Fatal("PendingForReplay cloned rather than moved the ledger payload")
	}
	if management.Pending() != 0 || management.PendingBytes() != 0 {
		t.Fatalf("ledger retained moved ownership: records=%d bytes=%d", management.Pending(), management.PendingBytes())
	}
	if string(caller) != "ledger-private-replay" {
		t.Fatal("RecordSent mutated caller-owned bytes")
	}
	clearStanzas(replay)
}

func TestPendingForReplayValidatesRejectedAndLedgerAsOneGraph(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(Stanza{Kind: StanzaSignal, AttemptID: "ledger", Data: []byte("cross-source-alias")}); err != nil {
		t.Fatal(err)
	}
	management.mu.Lock()
	shared := management.pending[0].stanza.Data
	management.mu.Unlock()
	session := &melliumSession{
		management: management,
		rejected:   []Stanza{{Kind: StanzaTimeCalibration, MessageID: "discard", Data: shared}},
	}
	replay, err := session.PendingForReplay(context.Background())
	if !errors.Is(err, ErrStreamManagement) || len(replay) != 0 {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	assertZeroBytes(t, shared)
	if management.Pending() != 0 || len(session.rejected) != 0 {
		t.Fatalf("failed-closed graph retained owners: pending=%d rejected=%#v", management.Pending(), session.rejected)
	}
}

func TestCanceledPendingForReplayPreservesAllSessionState(t *testing.T) {
	management, err := NewStreamManagement(2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume", true); err != nil {
		t.Fatal(err)
	}
	ledgerCaller := []byte("ledger-state")
	if err = management.RecordSent(Stanza{Kind: StanzaSignal, AttemptID: "ledger", Data: ledgerCaller}); err != nil {
		t.Fatal(err)
	}
	rejectedData := []byte("rejected-state")
	session := &melliumSession{
		management: management,
		rejected:   []Stanza{{Kind: StanzaSignal, AttemptID: "rejected", Data: rejectedData}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if replay, snapshotErr := session.PendingForReplay(ctx); !errors.Is(snapshotErr, context.Canceled) || len(replay) != 0 {
		clearStanzas(replay)
		t.Fatalf("replay=%#v err=%v", replay, snapshotErr)
	}
	session.mu.Lock()
	suspended := session.suspended
	rejected := session.rejected
	session.mu.Unlock()
	if suspended || len(rejected) != 1 || rejected[0].AttemptID != "rejected" || string(rejectedData) != "rejected-state" {
		t.Fatalf("cancellation mutated session: suspended=%t rejected=%#v", suspended, rejected)
	}
	if management.Pending() != 1 {
		t.Fatalf("cancellation detached ledger: pending=%d", management.Pending())
	}
	clearStanzas(rejected)
	management.Close()
}

type replayOwnershipSession struct {
	*qaDeadlineSession
	mu       sync.Mutex
	returned [][]byte
}

func (s *replayOwnershipSession) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := []byte("same-reconnect-record")
	s.mu.Lock()
	s.returned = append(s.returned, data)
	s.mu.Unlock()
	return []Stanza{{Kind: StanzaSignal, From: "a", To: "b", MeshID: "mesh", AttemptID: "attempt", Data: data}}, nil
}

func TestReconnectAttemptsMoveStableReplayAndClearSnapshotDuplicates(t *testing.T) {
	old := &replayOwnershipSession{qaDeadlineSession: newQADeadlineSession(qaStageNone)}
	client := qaReconnectClient(t, old, &qaSequenceDialer{})
	seed := []byte("same-reconnect-record")
	client.replay = []Stanza{{Kind: StanzaSignal, From: "a", To: "b", MeshID: "mesh", AttemptID: "attempt", Data: seed}}

	for attempt := 0; attempt < 2; attempt++ {
		if client.reconnect() {
			t.Fatalf("attempt %d unexpectedly recovered", attempt)
		}
		client.mu.Lock()
		if len(client.replay) != 1 || len(client.replay[0].Data) == 0 || &client.replay[0].Data[0] != &seed[0] {
			client.mu.Unlock()
			t.Fatalf("attempt %d replaced the retained payload owner", attempt)
		}
		client.mu.Unlock()
		old.mu.Lock()
		returned := append([][]byte(nil), old.returned...)
		old.mu.Unlock()
		if len(returned) != attempt+1 {
			t.Fatalf("attempt %d snapshots=%d", attempt, len(returned))
		}
		assertZeroBytes(t, returned[attempt])
	}
	client.mu.Lock()
	clearStanzas(client.replay)
	client.replay = nil
	client.mu.Unlock()
	assertZeroBytes(t, seed)
}

func assertZeroBytes(t *testing.T, value []byte) {
	t.Helper()
	for index, octet := range value {
		if octet != 0 {
			t.Fatalf("byte %d remained non-zero: %q", index, value)
		}
	}
}
