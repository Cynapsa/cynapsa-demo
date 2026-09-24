package rank2xmpp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestDynamicRejectedReloginIsTerminalAndPurgesTransport(t *testing.T) {
	first := newIdentitySession("first-proof")
	rejected := newIdentitySession("rejected-proof")
	rejected.authErr = errors.New("private server rejection detail")
	unused := newIdentitySession("must-not-retry")
	dialer := &identityDialer{sessions: []Session{first, rejected, unused}}
	client := newIdentityClient(t, dialer)
	client.config.DynamicPeerAuthority = true
	client.config.ReconnectAttempts = 3
	var fenced atomic.Int32
	if err := client.SetAuthorityFence(func() { fenced.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	secret := []byte("retained-replay-secret")
	client.mu.Lock()
	client.replay = []Stanza{{Kind: StanzaSignal, From: envelope.Sender, To: envelope.Recipient, MeshID: envelope.MeshID, AttemptID: "attempt", Data: secret}}
	client.mu.Unlock()
	if client.reconnect() {
		t.Fatal("server-rejected re-login recovered")
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(client.TerminalFailure(), ErrAuthentication) || !client.isClosed() {
		t.Fatalf("terminal=%v closed=%v", client.TerminalFailure(), client.isClosed())
	}
	if fenced.Load() == 0 {
		t.Fatal("terminal rejection did not fence peer authority")
	}
	if got := client.Observe().State; got != transport.HealthClosed {
		t.Fatalf("health=%v", got)
	}
	if messages, bytes := client.outbox.Usage(); messages != 0 || bytes != 0 {
		t.Fatalf("outbox retained messages=%d bytes=%d", messages, bytes)
	}
	for _, value := range secret {
		if value != 0 {
			t.Fatal("replay bytes were not cleared")
		}
	}
	if len(client.replay) != 0 || len(client.config.Auth.Password) != 0 {
		t.Fatal("terminal transport retained replay or credential")
	}
	if err := client.Send(context.Background(), envelope); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("post-rejection send=%v", err)
	}
	if _, err := client.ReceiveAuthenticated(context.Background()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("post-rejection receive=%v", err)
	}
	if client.reconnect() {
		t.Fatal("terminal client retried after rejection")
	}
	dialer.mu.Lock()
	remaining := len(dialer.sessions)
	dialer.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("post-rejection clean dial attempts continued: remaining=%d", remaining)
	}
}

func TestLegacyRejectedReloginKeepsPreviousRetryBehavior(t *testing.T) {
	first := newIdentitySession("first-proof")
	rejected := newIdentitySession("rejected-proof")
	rejected.authErr = errors.New("private server rejection detail")
	accepted := newIdentitySession("accepted-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first, rejected, accepted}})
	client.config.ReconnectAttempts = 2
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if !client.reconnect() || client.TerminalFailure() != nil {
		t.Fatalf("legacy reconnect=%v terminal=%v", client.DurableState(), client.TerminalFailure())
	}
}

func TestDynamicTransientCredentialReadRetainsOutbox(t *testing.T) {
	first := newIdentitySession("first-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first}})
	client.config.DynamicPeerAuthority = true
	var available atomic.Bool
	available.Store(true)
	client.config.CredentialNow = func() time.Time { return time.Now().UTC() }
	client.config.CredentialSource = func(context.Context) (Credential, error) {
		if !available.Load() {
			return Credential{}, ErrUnavailable
		}
		return Credential{Password: []byte("renewed-secret"), UsableUntil: time.Now().UTC().Add(time.Minute)}, nil
	}
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	available.Store(false)
	if client.reconnect() {
		t.Fatal("transient credential read unexpectedly recovered")
	}
	if client.TerminalFailure() != nil || client.isClosed() {
		t.Fatalf("transient read became terminal: %v", client.TerminalFailure())
	}
	if count, _ := client.outbox.Usage(); count != 1 {
		t.Fatalf("transient read erased outbox: %d", count)
	}
}

func TestDynamicBackgroundReconnectRetriesAfterCredentialReturns(t *testing.T) {
	first := newIdentitySession("first-proof")
	replacement := newIdentitySession("replacement-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first, replacement}})
	client.config.DynamicPeerAuthority = true
	client.config.ReconnectInitial = 10 * time.Millisecond
	client.config.ReconnectAttempts = 1
	var available atomic.Bool
	available.Store(true)
	client.config.CredentialNow = func() time.Time { return time.Now().UTC() }
	client.config.CredentialSource = func(context.Context) (Credential, error) {
		if !available.Load() {
			return Credential{}, ErrUnavailable
		}
		return Credential{Password: []byte("renewed-secret"), UsableUntil: time.Now().UTC().Add(time.Minute)}, nil
	}
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope); err != nil {
		t.Fatal(err)
	}
	available.Store(false)
	client.setState(DurablePending)
	client.requestReconnect()
	time.Sleep(30 * time.Millisecond)
	if client.TerminalFailure() != nil || client.isClosed() {
		t.Fatal("credential read gap terminated client")
	}
	available.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for client.DurableState() != DurableLive {
		if time.Now().After(deadline) {
			t.Fatal("background reconnect did not retry after credential returned")
		}
		time.Sleep(time.Millisecond)
	}
	if count, _ := client.outbox.Usage(); count != 1 {
		t.Fatalf("recovery erased queued work: %d", count)
	}
}

func TestDynamicCleanRebindReleasesOldRank2OwnershipOnlyAfterPublication(t *testing.T) {
	first := newIdentitySession("first-proof")
	replacement := newIdentitySession("replacement-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first, replacement}})
	client.config.DynamicPeerAuthority = true
	client.config.ReconnectAttempts = 1
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	envelope := durableEnvelope(t)
	reservation, err := client.outbox.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.outbox.ClaimRank2(envelope.MessageID, 1); err != nil {
		t.Fatal(err)
	}
	client.outbox.Release(reservation)
	if !client.reconnect() {
		t.Fatal("dynamic clean rebind failed")
	}
	if client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("old exact-resource attempt remained Rank2-owned")
	}
	if count, _ := client.outbox.Usage(); count != 1 {
		t.Fatalf("old attempt was not preserved in outbox: %d", count)
	}
	ids := client.outbox.EligibleMessageIDs(1)
	if len(ids) != 1 || ids[0] != envelope.MessageID {
		t.Fatalf("old attempt not available for exact revalidation: %v", ids)
	}
}

func TestFailedDynamicCleanRebindKeepsOldRank2Ownership(t *testing.T) {
	first := newIdentitySession("first-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first}})
	client.config.DynamicPeerAuthority = true
	client.config.ReconnectAttempts = 1
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	envelope := durableEnvelope(t)
	reservation, err := client.outbox.EnqueueReserved(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.outbox.ClaimRank2(envelope.MessageID, 1); err != nil {
		t.Fatal(err)
	}
	client.outbox.Release(reservation)
	if client.reconnect() {
		t.Fatal("unexpected replacement without a dial candidate")
	}
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("failed clean bind released old-session claim prematurely")
	}
}

func TestQueuedClaimRacingDynamicRebindReturnsToGeneralOutbox(t *testing.T) {
	first := newIdentitySession("first-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first}})
	client.config.DynamicPeerAuthority = true
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	envelope := durableEnvelope(t)
	if _, err := client.outbox.Enqueue(envelope.Clone()); err != nil {
		t.Fatal(err)
	}
	reservation, err := client.outbox.ReserveEligible(envelope.MessageID)
	if err != nil || reservation.MessageID == "" {
		t.Fatalf("reserve = %#v, %v", reservation, err)
	}
	if err := client.sendMu.LockContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan SendOwnership, 1)
	go func() { done <- client.SendQueuedOwned(t.Context(), envelope) }()
	deadline := time.Now().Add(time.Second)
	for !client.outbox.OwnsRank2(envelope.MessageID) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !client.outbox.OwnsRank2(envelope.MessageID) {
		client.sendMu.Unlock()
		t.Fatal("queued sender did not claim before replacement")
	}
	client.mu.Lock()
	oldIngress := client.ingress
	client.sessionEpoch++
	client.session = newIdentitySession("replacement-proof")
	client.ingress = newIngressGeneration(client.sessionEpoch, client.session, client.identity.BoundIdentity, nil, client.ctx)
	client.mu.Unlock()
	oldIngress.cancel()
	client.sendMu.Unlock()
	select {
	case ownership := <-done:
		if ownership != AcceptedOwned {
			t.Fatalf("queued ownership = %v", ownership)
		}
	case <-time.After(time.Second):
		t.Fatal("queued sender remained blocked")
	}
	if !client.outbox.Release(reservation) {
		t.Fatal("borrowed payload was not released")
	}
	if client.outbox.OwnsRank2(envelope.MessageID) {
		t.Fatal("racing claim stranded in Rank2 ownership")
	}
	if ids := client.outbox.EligibleMessageIDs(1); len(ids) != 1 || ids[0] != envelope.MessageID {
		t.Fatalf("racing claim not schedulable: %v", ids)
	}
}
