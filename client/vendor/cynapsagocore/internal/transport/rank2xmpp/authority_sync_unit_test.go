package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"mellium.im/xmpp/jid"
)

func TestAuthoritySnapshotRequiresCanonicalCompleteCurrentMembership(t *testing.T) {
	valid := AuthoritySnapshot{Members: []string{"a@example.test/mesh", "local@example.test/mesh", "z@example.test/mesh"}, nonce: "0123456789abcdef"}
	if err := validateAuthoritySnapshot(valid, "mesh", "local@example.test/mesh", "example.test"); err != nil {
		t.Fatalf("valid snapshot: %v", err)
	}
	mixed := AuthoritySnapshot{Members: []string{
		"a@example.test/r2.00000000-0000-4000-8000-000000000001.AAAAAAAA",
		"local@example.test/mesh",
	}, nonce: "0123456789abcdef"}
	if err := validateAuthoritySnapshot(mixed, "mesh", "local@example.test/mesh", "example.test"); err != nil {
		t.Fatalf("legacy local with r2 peer snapshot: %v", err)
	}
	for _, members := range [][]string{
		{},
		{"a@example.test/mesh"},
		{"local@example.test/mesh", "a@example.test/mesh"},
		{"local@example.test/mesh", "local@example.test/mesh"},
		{"local@example.test/mesh", "a@foreign.test/mesh"},
	} {
		if err := validateAuthoritySnapshot(AuthoritySnapshot{Members: members, nonce: "0123456789abcdef"}, "mesh", "local@example.test/mesh", "example.test"); !errors.Is(err, ErrProtocol) {
			t.Fatalf("members=%q error=%v", members, err)
		}
	}
}

func TestAuthorityPageUsesNonceCursorAndCompleteMemberVectorOnly(t *testing.T) {
	from := jid.MustParse("mesh.test")
	to := jid.MustParse("agent@mesh.test/mesh")
	valid := "<iq xmlns='jabber:client' id='authority-id' type='result' from='mesh.test' to='agent@mesh.test/mesh'>" +
		"<synchronized xmlns='urn:cynapsa:mesh-authority:1' nonce='0123456789abcdef' total='2' cursor='0' next='1'>" +
		"<member jid='agent@mesh.test/mesh'></member></synchronized></iq>"
	page, correlated, handled, err := decodeAuthorityPage(xml.NewDecoder(strings.NewReader(valid)), "authority-id", from, to, "0123456789abcdef", 0, maximumPrivateIQBytes, false)
	if err != nil || !correlated || !handled || page.Total != 2 || page.Cursor != 0 || page.Next != 1 || len(page.Members) != 1 {
		t.Fatalf("page=%#v correlated=%v handled=%v err=%v", page, correlated, handled, err)
	}
	mutants := []string{
		strings.Replace(valid, " nonce='0123456789abcdef'", " nonce='other-session-nonce'", 1),
		strings.Replace(valid, " total='2'", " generation='7' total='2'", 1),
		strings.Replace(valid, "<member jid='agent@mesh.test/mesh'></member>", "<peer jid='agent@mesh.test/mesh' authorized='true'></peer>", 1),
		strings.Replace(valid, " next='1'", " next=''", 1),
	}
	for _, document := range mutants {
		if _, correlated, handled, err := decodeAuthorityPage(xml.NewDecoder(strings.NewReader(document)), "authority-id", from, to, "0123456789abcdef", 0, maximumPrivateIQBytes, false); !correlated || handled || !errors.Is(err, ErrProtocol) {
			t.Fatalf("mutant correlated=%v handled=%v err=%v", correlated, handled, err)
		}
	}
}

func TestMembershipWakeCarriesNoMembershipEvidence(t *testing.T) {
	outer := xml.StartElement{Name: xml.Name{Space: "jabber:client", Local: "iq"}}
	valid := `<membership-changed xmlns="urn:cynapsa:mesh-authority:1" snapshot-required="true"><removed jid="peer@example.test/mesh"></removed></membership-changed>`
	if err := decodeMembershipChanged(melliumIQChildDecoder(t, []byte(valid), outer), outer, 1<<20); err != nil {
		t.Fatalf("valid wake: %v", err)
	}
	for _, invalid := range []string{
		`<membership-changed xmlns="urn:cynapsa:mesh-authority:1" snapshot-required="false"></membership-changed>`,
		`<membership-changed xmlns="urn:cynapsa:mesh-authority:1" snapshot-required="true" generation="7"></membership-changed>`,
		`<authority-changed xmlns="urn:cynapsa:mesh-authority:1" generation="7"></authority-changed>`,
	} {
		if err := decodeMembershipChanged(melliumIQChildDecoder(t, []byte(invalid), outer), outer, 1<<20); !errors.Is(err, ErrProtocol) {
			t.Fatalf("invalid wake error=%v", err)
		}
	}
}

func TestAuthoritySnapshotAckIsExactSessionAndOpensDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := &fakeSession{}
	retired := &fakeSession{}
	client := &Client{started: true, state: DurableLive, session: current, ctx: ctx, generation: 1, sessionEpoch: 4, authorityGeneration: 7, inbound: make(chan AuthenticatedInbound, 1), membershipReadySignal: make(chan struct{}), authorityPendingNonce: "current-nonce"}
	stale := AuthoritySnapshot{Members: []string{"local@example.test/mesh"}, session: retired, sessionEpoch: 3, authorityGeneration: 6, nonce: "old-nonce"}
	if err := client.AcknowledgeAuthoritySnapshot(stale); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale ack=%v", err)
	}
	currentSnapshot := AuthoritySnapshot{Members: []string{"local@example.test/mesh"}, session: current, sessionEpoch: 4, authorityGeneration: 7, nonce: "current-nonce"}
	if err := client.AcknowledgeAuthoritySnapshot(currentSnapshot); err != nil {
		t.Fatalf("current ack=%v", err)
	}
	if !client.membershipReady {
		t.Fatal("terminal install ack did not open delivery")
	}
}

func TestAuthoritySnapshotAckRequiresCurrentLocalPublicationCapability(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		ctx, cancel := context.WithCancel(context.Background())
		session := &fakeSession{}
		client := &Client{
			started: true, state: DurableLive, session: session, ctx: ctx,
			generation: 1, sessionEpoch: 4, authorityGeneration: 7,
			membershipReadySignal: make(chan struct{}), authorityPendingNonce: "current-nonce",
		}
		snapshot := AuthoritySnapshot{Members: []string{"local@example.test/mesh"}, session: session, sessionEpoch: 4, authorityGeneration: 7, nonce: "current-nonce"}
		checks := 0
		err := client.AcknowledgeAuthoritySnapshotIf(snapshot, func() bool {
			checks++
			return false
		})
		if !errors.Is(err, ErrUnavailable) || checks != 1 {
			t.Fatalf("iteration %d guarded ack=%v checks=%d", iteration, err, checks)
		}
		if client.membershipReady || client.authorityPendingNonce != "current-nonce" {
			t.Fatalf("iteration %d stale guard opened Rank2: ready=%t nonce=%q", iteration, client.membershipReady, client.authorityPendingNonce)
		}
		cancel()
	}
}

func TestReceiveWaitsForTerminalMembershipInstallAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &fakeSession{}
	client := &Client{started: true, state: DurableLive, session: session, ctx: ctx, generation: 1, sessionEpoch: 1, authorityGeneration: 1, inbound: make(chan AuthenticatedInbound, 1), membershipReadySignal: make(chan struct{}), authorityPendingNonce: "sync-nonce"}
	client.inbound <- AuthenticatedInbound{AuthenticatedSender: "peer@example.test/mesh"}
	done := make(chan error, 1)
	go func() {
		_, err := client.ReceiveAuthenticated(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("application delivery escaped synchronization gate: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := client.AcknowledgeAuthoritySnapshot(AuthoritySnapshot{session: session, sessionEpoch: 1, authorityGeneration: 1, nonce: "sync-nonce"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not resume after terminal install ack")
	}
}

type blockingAuthoritySession struct {
	fakeSession
	entered chan struct{}
	release chan struct{}
}

func (session *blockingAuthoritySession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	close(session.entered)
	select {
	case <-session.release:
		return AuthoritySnapshot{Members: []string{"local@example.test/mesh", "peer@example.test/mesh"}}, nil
	case <-ctx.Done():
		return AuthoritySnapshot{}, ctx.Err()
	}
}

func TestAuthorityFetchCannotRepublishAfterSameSessionGenerationFence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &blockingAuthoritySession{entered: make(chan struct{}), release: make(chan struct{})}
	client := &Client{
		config:  Config{Auth: Authentication{Username: "local@example.test", MeshID: "mesh"}},
		started: true, state: DurableLive, session: session, ctx: ctx, generation: 1,
		sessionEpoch: 4, authorityGeneration: 9, membershipReady: true,
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.SyncAuthority(context.Background())
		done <- err
	}()
	select {
	case <-session.entered:
	case <-time.After(time.Second):
		t.Fatal("authority fetch did not enter server I/O")
	}
	client.mu.Lock()
	client.authoritySnapshot = AuthoritySnapshot{}
	client.pauseMembershipLocked("")
	client.mu.Unlock()
	close(session.release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("stale fetch error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale authority fetch did not return")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.authorityGeneration != 10 || client.membershipReady || client.authorityPendingNonce != "" || len(client.authoritySnapshot.Members) != 0 {
		t.Fatalf("stale fetch republished state: generation=%d ready=%t nonce=%q snapshot=%#v", client.authorityGeneration, client.membershipReady, client.authorityPendingNonce, client.authoritySnapshot)
	}
}

func TestAuthoritySnapshotGenerationIsInvalidatedByExplicitPauseAndDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &fakeSession{}
	client := &Client{started: true, state: DurableLive, session: session, ctx: ctx, generation: 1, sessionEpoch: 1, authorityGeneration: 3, membershipReady: false, authorityPendingNonce: "pending"}
	snapshot := AuthoritySnapshot{session: session, sessionEpoch: 1, authorityGeneration: 3, nonce: "pending"}
	client.mu.Lock()
	client.pauseMembershipLocked("")
	client.mu.Unlock()
	if err := client.AcknowledgeAuthoritySnapshot(snapshot); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("explicit pause stale ack = %v", err)
	}
	client.mu.Lock()
	before := client.authorityGeneration
	_ = client.setStateLocked(DurablePending)
	after := client.authorityGeneration
	client.mu.Unlock()
	if after != before+1 {
		t.Fatalf("disconnect generation = %d, want %d", after, before+1)
	}
}

func TestAuthorityGenerationExhaustionFailsClosedWithoutWrapOrRepublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &fakeSession{}
	client := &Client{
		config:  Config{Auth: Authentication{Username: "a@example.test", MeshID: "mesh"}},
		started: true, state: DurableLive, session: session, ctx: ctx,
		generation: 1, sessionEpoch: 4, authorityGeneration: math.MaxUint64,
		membershipReady: true, authorityPendingNonce: "published",
		authoritySnapshot: AuthoritySnapshot{
			Members: []string{"a@example.test/mesh"}, session: session,
			sessionEpoch: 4, authorityGeneration: math.MaxUint64, nonce: "published",
		},
	}
	stale := client.authoritySnapshot.clone()

	client.mu.Lock()
	if client.pauseMembershipLocked("") {
		t.Fatal("exhausted authority generation was accepted")
	}
	client.mu.Unlock()
	for i := 0; i < 3; i++ {
		client.mu.Lock()
		if client.pauseMembershipLocked("") {
			client.mu.Unlock()
			t.Fatalf("repeated exhausted pause %d was accepted", i)
		}
		client.mu.Unlock()
	}

	client.mu.Lock()
	generation := client.authorityGeneration
	exhausted := client.authorityExhausted
	ready := client.membershipReady
	pending := client.authorityPendingNonce
	cached := client.authoritySnapshot.clone()
	client.mu.Unlock()
	if generation != math.MaxUint64 || !exhausted || ready || pending != "" || len(cached.Members) != 0 {
		t.Fatalf("exhausted authority reopened or wrapped: generation=%d exhausted=%t ready=%t pending=%q cached=%#v", generation, exhausted, ready, pending, cached)
	}
	if err := client.AcknowledgeAuthoritySnapshot(stale); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-exhaustion snapshot acknowledgement = %v, want ErrUnavailable", err)
	}
	if _, err := client.SyncAuthority(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exhausted authority synchronization = %v, want ErrUnavailable", err)
	}
	if err := client.refreshAuthorityForSession(context.Background(), session); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exhausted session refresh = %v, want ErrUnavailable", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.authorityGeneration != math.MaxUint64 || !client.authorityExhausted || client.membershipReady || client.authorityPendingNonce != "" || len(client.authoritySnapshot.Members) != 0 {
		t.Fatalf("refresh republished exhausted authority: generation=%d exhausted=%t ready=%t pending=%q cached=%#v", client.authorityGeneration, client.authorityExhausted, client.membershipReady, client.authorityPendingNonce, client.authoritySnapshot)
	}
}

func TestQuarantineReceiveTransfersBeforeMembershipAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &fakeSession{}
	client := &Client{started: true, state: DurableLive, session: session, ctx: ctx, generation: 1, sessionEpoch: 1, inbound: make(chan AuthenticatedInbound, 1), membershipReadySignal: make(chan struct{}), authorityPendingNonce: "sync-nonce"}
	client.inbound <- AuthenticatedInbound{AuthenticatedSender: "peer@example.test/mesh"}
	inbound, err := client.ReceiveAuthenticatedForQuarantine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if inbound.AuthenticatedSender != "peer@example.test/mesh" || client.membershipReady {
		t.Fatalf("inbound=%#v ready=%v", inbound, client.membershipReady)
	}
}
