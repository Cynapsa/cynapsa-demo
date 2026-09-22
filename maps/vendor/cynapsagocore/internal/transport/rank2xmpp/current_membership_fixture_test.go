package rank2xmpp

import (
	"context"
	"errors"
	"testing"
)

// currentMembershipFixture returns the complete authority view used by the
// successful injected sessions in this package. A one-member mesh is complete,
// canonical, and already sorted; fixtures with a different bound identity pass
// that exact identity explicitly.
func currentMembershipFixture(ctx context.Context, local string) (AuthoritySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, err
	}
	return AuthoritySnapshot{Members: []string{local}}, nil
}

// startCurrentMembershipFixture preserves the production two-phase contract in
// tests: Start authenticates and downloads the complete snapshot, then the
// fixture acting as the membership owner installs/acknowledges that exact
// session capability before application traffic is exercised.
func startCurrentMembershipFixture(ctx context.Context, client *Client) error {
	if err := client.Start(ctx); err != nil {
		return err
	}
	return acknowledgeCurrentMembershipFixture(ctx, client)
}

func acknowledgeCurrentMembershipFixture(ctx context.Context, client *Client) error {
	snapshot, err := client.SyncAuthority(ctx)
	if err != nil {
		return err
	}
	return client.AcknowledgeAuthoritySnapshot(snapshot)
}

func (*identitySession) DiscoverAuthority(context.Context) error             { return nil }
func (*ingressSession) DiscoverAuthority(context.Context) error              { return nil }
func (*failingPhaseSession) DiscoverAuthority(context.Context) error         { return nil }
func (*externalServiceTestSession) DiscoverAuthority(context.Context) error  { return nil }
func (*lifecycleSession) DiscoverAuthority(context.Context) error            { return nil }
func (*setupDeadlineCaptureSession) DiscoverAuthority(context.Context) error { return nil }
func (*qaLifecycleSession) DiscoverAuthority(context.Context) error          { return nil }
func (*qaIdentitySession) DiscoverAuthority(context.Context) error           { return nil }
func (*qaDeadlineSession) DiscoverAuthority(context.Context) error           { return nil }

func (s *identitySession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, s.bound)
}

func (*ingressSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, ingressLocal)
}

func (*failingPhaseSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (*fakeSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (s *externalServiceTestSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "agent@mesh.test/mesh-1")
}

func (*lifecycleSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (*setupTestSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (*setupDeadlineCaptureSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (*qaLifecycleSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (s *qaIdentitySession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, s.full)
}

func (*qaDeadlineSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

type unavailableCurrentMembershipSession struct{ *fakeSession }

func (*unavailableCurrentMembershipSession) SyncAuthority(context.Context) (AuthoritySnapshot, error) {
	return AuthoritySnapshot{}, ErrUnavailable
}

func TestStartFailsClosedWhenCurrentMembershipIsUnavailable(t *testing.T) {
	session := &unavailableCurrentMembershipSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := qaUnstartedClient(t, fakeDialer{session})
	if err := client.Start(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start error = %v, want unavailable", err)
	}
	client.mu.Lock()
	started, published := client.started, client.session != nil
	client.mu.Unlock()
	if started || published {
		t.Fatalf("failed authority synchronization published session: started=%t session=%t", started, published)
	}
}
