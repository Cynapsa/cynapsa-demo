package rank2xmpp

import (
	"context"
	"errors"
	"testing"
	"time"
)

type expiryResumeSession struct {
	*fakeSession
	resumeCalls int
}

func (session *expiryResumeSession) Resume(context.Context) (bool, error) {
	session.resumeCalls++
	return true, nil
}

type expiryDialer struct{ calls int }

func (dialer *expiryDialer) Dial(context.Context) (Session, error) {
	dialer.calls++
	return &fakeSession{events: make(chan Event)}, nil
}

func TestExpiredCredentialNeverReachesInitialConnectOrResume(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	current := now
	dialer := &expiryDialer{}
	client := &Client{config: Config{
		CredentialUsableUntil: now.Add(time.Minute),
		CredentialNow:         func() time.Time { return current },
	}, dialer: dialer}
	current = now.Add(time.Minute)
	if _, err := client.connect(context.Background(), 1); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("expired initial connect error=%v", err)
	}
	if dialer.calls != 0 {
		t.Fatal("expired credential reached the initial dial path")
	}
	session := &expiryResumeSession{fakeSession: &fakeSession{events: make(chan Event)}}
	if resumed, prepared, err := client.prepareResume(session, context.Background()); resumed || prepared != nil || !errors.Is(err, ErrAuthentication) {
		t.Fatalf("expired resume=(%v,%v,%v)", resumed, prepared, err)
	}
	if session.resumeCalls != 0 {
		t.Fatal("expired credential reached session resume")
	}
}

func TestExpiredCredentialNeverReachesMelliumSASLOrPrepareResume(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	current := now.Add(time.Minute)
	session := newMelliumSession(MelliumConfig{
		ReceiveCapacity: 1, CredentialUsableUntil: now.Add(time.Minute),
		CredentialNow: func() time.Time { return current },
	}, Endpoint{})
	if err := session.EnableStreamManagement(context.Background(), true); !errors.Is(err, ErrStreamManagement) {
		t.Fatalf("expired SASL path error=%v", err)
	}
	if resumed, err := session.PrepareResume(context.Background()); resumed || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expired mellium resume=(%v,%v)", resumed, err)
	}
}

func TestCredentialSourceSupersedesStaleCopiedJWTForReplacementConnections(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	current := now
	client := &Client{config: Config{
		Auth: Authentication{Password: []byte("stale")}, CredentialUsableUntil: now.Add(time.Minute),
		CredentialNow: func() time.Time { return current },
		CredentialSource: func(context.Context) (Credential, error) {
			return Credential{Password: []byte("fresh"), UsableUntil: now.Add(time.Hour)}, nil
		},
	}}
	current = now.Add(2 * time.Minute)
	credential, err := client.currentCredential(context.Background())
	if err != nil || string(credential.Password) != "fresh" || !credential.UsableUntil.Equal(now.Add(time.Hour)) {
		t.Fatalf("credential=%q until=%v err=%v", credential.Password, credential.UsableUntil, err)
	}
	clear(credential.Password)
}

func TestDistinctSessionResourceKeepsMeshAsAuthorityContext(t *testing.T) {
	client := &Client{config: Config{Auth: Authentication{
		Username: "agent@example.test", MeshID: "mesh-one", SessionResource: "r2.installation.session",
	}}}
	if err := client.VerifyBoundIdentity("agent@example.test", "mesh-one", "agent@example.test/r2.installation.session"); err != nil {
		t.Fatalf("server-issued resource rejected: %v", err)
	}
	if err := client.VerifyBoundIdentity("agent@example.test", "mesh-one", "agent@example.test/mesh-one"); !errors.Is(err, ErrIdentityBinding) {
		t.Fatal("legacy mesh resource replaced an explicit server-issued resource")
	}
	legacy := &Client{config: Config{Auth: Authentication{Username: "agent@example.test", MeshID: "mesh-one"}}}
	if err := legacy.VerifyBoundIdentity("agent@example.test", "mesh-one", "agent@example.test/mesh-one"); err != nil {
		t.Fatalf("legacy mesh resource was not preserved: %v", err)
	}
}
