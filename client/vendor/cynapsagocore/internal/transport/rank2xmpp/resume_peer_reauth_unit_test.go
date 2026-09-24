package rank2xmpp

import (
	"context"
	"errors"
	"testing"
)

type replayPeerAuthoritySession struct {
	*orderedAuthorityResumeSession
	peers      []string
	authorize  error
	authorized []string
}

func (s *replayPeerAuthoritySession) PreparedReplayPeers(context.Context) ([]string, error) {
	return append([]string(nil), s.peers...), nil
}

func (s *replayPeerAuthoritySession) ResolveAuthorizedExactPeer(_ context.Context, full string) (AuthorizedPeer, error) {
	s.record("authorize")
	if s.replayCalls.Load() != 0 {
		return AuthorizedPeer{}, ErrProtocol
	}
	s.authorized = append(s.authorized, full)
	if s.authorize != nil {
		return AuthorizedPeer{}, s.authorize
	}
	return AuthorizedPeer{FullJID: full, InstallationID: "install", SessionGeneration: "generation"}, nil
}

func TestDynamicResumeReauthorizesExactPeerBeforeApplicationReplay(t *testing.T) {
	for _, test := range []struct {
		name       string
		authorize  error
		wantReplay int32
	}{
		{name: "accepted", wantReplay: 1},
		{name: "rejected", authorize: ErrAuthentication},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := &replayPeerAuthoritySession{
				orderedAuthorityResumeSession: &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()},
				peers:                         []string{"b@example.test/r2.install.nonce"},
				authorize:                     test.authorize,
			}
			client := recoveryTestClient(t, session)
			client.config.DynamicPeerAuthority = true
			client.authoritySuspended = true
			client.authorityResume = func(context.Context) (bool, error) {
				session.record("authority")
				return true, nil
			}
			client.mu.Lock()
			owner, generation := client.stateTransitionOwnerLocked(), client.generation
			client.mu.Unlock()
			err := client.completeCommittedResume(context.Background(), session, session, owner, generation)
			if test.authorize != nil && !errors.Is(err, test.authorize) || test.authorize == nil && err != nil {
				t.Fatalf("resume completion = %v", err)
			}
			if got := session.replayCalls.Load(); got != test.wantReplay {
				t.Fatalf("replay before/after authorization = %d, want %d", got, test.wantReplay)
			}
			if len(session.authorized) != 1 || session.authorized[0] != session.peers[0] {
				t.Fatalf("authorized exact peers = %v", session.authorized)
			}
			session.mu.Lock()
			steps := append([]string(nil), session.steps...)
			session.mu.Unlock()
			want := []string{"barrier", "calibrate", "authority", "authorize"}
			if test.authorize == nil {
				want = append(want, "replay")
			}
			if len(steps) != len(want) {
				t.Fatalf("resume steps = %v, want %v", steps, want)
			}
			for index := range want {
				if steps[index] != want[index] {
					t.Fatalf("resume steps = %v, want %v", steps, want)
				}
			}
		})
	}
}

func TestDynamicResumeWithoutPeerMetadataFailsClosed(t *testing.T) {
	session := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()}
	client := recoveryTestClient(t, session)
	client.config.DynamicPeerAuthority = true
	client.mu.Lock()
	owner, generation := client.stateTransitionOwnerLocked(), client.generation
	client.mu.Unlock()
	if err := client.completeCommittedResume(context.Background(), session, session, owner, generation); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing peer metadata = %v", err)
	}
	if session.replayCalls.Load() != 0 {
		t.Fatal("unverifiable prepared replay reached wire")
	}
}
