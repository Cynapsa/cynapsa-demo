package rank2xmpp

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
)

type identitySession struct {
	bare        string
	bound       string
	proof       []byte
	authErr     error
	issuedMu    sync.Mutex
	issuedProof []byte
	done        chan struct{}
	once        sync.Once
}

func newIdentitySession(proof string) *identitySession {
	return &identitySession{
		bare:  "a@example.test",
		bound: "a@example.test/mesh",
		proof: []byte(proof),
		done:  make(chan struct{}),
	}
}

func (s *identitySession) ConnectTLS(context.Context, string) error { return nil }
func (s *identitySession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	issued := append([]byte(nil), s.proof...)
	s.issuedMu.Lock()
	s.issuedProof = issued
	s.issuedMu.Unlock()
	return s.bare, issued, s.authErr
}
func (s *identitySession) BindResource(context.Context, string) (string, error) {
	return s.bound, nil
}
func (s *identitySession) EnableStreamManagement(context.Context, bool) error { return nil }
func (s *identitySession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), nil
}
func (s *identitySession) Send(context.Context, Stanza) error { return nil }
func (s *identitySession) Receive(ctx context.Context) (Event, error) {
	select {
	case <-s.done:
		return Event{}, ErrClosed
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (s *identitySession) Resume(context.Context) (bool, error) { return false, nil }
func (s *identitySession) CatchUp(context.Context, int) ([]Stanza, error) {
	return nil, nil
}
func (s *identitySession) Close(context.Context) error {
	s.once.Do(func() { close(s.done) })
	return nil
}

func (s *identitySession) proofWasCleared() bool {
	s.issuedMu.Lock()
	defer s.issuedMu.Unlock()
	if len(s.issuedProof) == 0 {
		return false
	}
	for _, value := range s.issuedProof {
		if value != 0 {
			return false
		}
	}
	return true
}

type identityDialer struct {
	mu       sync.Mutex
	sessions []Session
}

func (d *identityDialer) Dial(context.Context) (Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.sessions) == 0 {
		return nil, ErrUnavailable
	}
	session := d.sessions[0]
	d.sessions = d.sessions[1:]
	return session, nil
}

type gatedIdentityDialer struct {
	entered chan struct{}
	release chan struct{}
	session Session
	once    sync.Once
}

func (d *gatedIdentityDialer) Dial(ctx context.Context) (Session, error) {
	d.once.Do(func() { close(d.entered) })
	select {
	case <-d.release:
		return d.session, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newIdentityClient(t *testing.T, dialer Dialer) *Client {
	return newIdentityClientForEndpoint(t, "localhost:5222", dialer)
}

func newIdentityClientForEndpoint(t *testing.T, endpoint string, dialer Dialer) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint:                       endpoint,
		Auth:                           Authentication{Username: "a@example.test", Password: []byte("secret"), MeshID: "mesh"},
		ReceiveCapacity:                4,
		TransferWorkers:                1,
		TransferQueue:                  4,
		TransferByteCapacity:           1 << 20,
		UnresolvedTransferCapacity:     4,
		UnresolvedTransferByteCapacity: 1 << 20,
		UnresolvedTransferLifetime:     time.Second,
		MailboxLimit:                   4,
		ReconnectAttempts:              1,
		ReconnectInitial:               time.Millisecond,
		ReconnectMaximum:               time.Millisecond,
		ReconnectOperationTimeout:      time.Second,
	}, dialer, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestAuthenticatedIdentityLifecycleAndIsolation(t *testing.T) {
	session := newIdentitySession("private-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})

	if identity, err := client.AuthenticatedIdentity(); !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
		t.Fatalf("pre-start identity = %#v, %v", identity, err)
	}
	if identity, err := (*Client)(nil).AuthenticatedIdentity(); !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
		t.Fatalf("nil-client identity = %#v, %v", identity, err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	identity, err := client.AuthenticatedIdentity()
	if err != nil {
		t.Fatal(err)
	}
	want := AuthenticatedIdentity{BareIdentity: "a@example.test", BoundIdentity: "a@example.test/mesh", MeshID: "mesh"}
	if identity != want {
		t.Fatalf("identity = %#v, want %#v", identity, want)
	}
	identity.BareIdentity = "mutated@example.test"
	identity.BoundIdentity = "mutated@example.test/other"
	identity.MeshID = "other"
	second, err := client.AuthenticatedIdentity()
	if err != nil || second != want {
		t.Fatalf("identity after caller mutation = %#v, %v", second, err)
	}

	typeOfIdentity := reflect.TypeOf(second)
	if typeOfIdentity.NumField() != 3 {
		t.Fatalf("authenticated identity exposes %d fields", typeOfIdentity.NumField())
	}
	for _, forbidden := range []string{"Proof", "Password", "Session", "Connection"} {
		if _, exposed := typeOfIdentity.FieldByName(forbidden); exposed {
			t.Fatalf("authenticated identity exposes %s", forbidden)
		}
	}

	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if identity, err = client.AuthenticatedIdentity(); !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
		t.Fatalf("post-close identity = %#v, %v", identity, err)
	}
}

func TestAuthenticatedIdentityRejectsFailedBinding(t *testing.T) {
	session := newIdentitySession("private-proof")
	session.bound = "a@example.test/other-mesh"
	client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})
	if err := client.Start(context.Background()); !errors.Is(err, ErrIdentityBinding) {
		t.Fatalf("start error = %v", err)
	}
	if identity, err := client.AuthenticatedIdentity(); !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
		t.Fatalf("failed-auth identity = %#v, %v", identity, err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedIdentityConcurrentStart(t *testing.T) {
	dialer := &gatedIdentityDialer{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		session: newIdentitySession("private-proof"),
	}
	client := newIdentityClient(t, dialer)
	started := make(chan error, 1)
	go func() { started <- client.Start(context.Background()) }()
	<-dialer.entered

	for i := 0; i < 1_000; i++ {
		identity, err := client.AuthenticatedIdentity()
		if !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
			t.Fatalf("in-flight-start identity = %#v, %v", identity, err)
		}
	}
	close(dialer.release)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if identity, err := client.AuthenticatedIdentity(); err != nil || identity.BoundIdentity != "a@example.test/mesh" {
		t.Fatalf("committed identity = %#v, %v", identity, err)
	}
}

func TestAuthenticatedIdentityTracksCommittedReconnectGeneration(t *testing.T) {
	first := newIdentitySession("generation-one")
	second := newIdentitySession("generation-two")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first, second}})
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())

	if !client.reconnect() {
		t.Fatal("reconnect failed")
	}
	identity, err := client.AuthenticatedIdentity()
	if err != nil {
		t.Fatal(err)
	}
	want := AuthenticatedIdentity{BareIdentity: "a@example.test", BoundIdentity: "a@example.test/mesh", MeshID: "mesh"}
	if identity != want {
		t.Fatalf("reconnected identity = %#v, want %#v", identity, want)
	}
	client.mu.Lock()
	proofLength := len(client.identity.Proof)
	current := client.session
	client.mu.Unlock()
	if proofLength != 0 || current != second || !first.proofWasCleared() || !second.proofWasCleared() {
		t.Fatalf("reconnect retained identity proof: length=%d current=%T firstCleared=%t secondCleared=%t", proofLength, current, first.proofWasCleared(), second.proofWasCleared())
	}
}

func TestIdentityProofIsZeroedAfterSinkConsumptionAndNeverStored(t *testing.T) {
	session := newIdentitySession("sink-private-proof")
	var consumed []byte
	client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})
	client.config.ProofSink = func(_ context.Context, meshID string, proof []byte) error {
		if meshID != "mesh" || string(proof) != "sink-private-proof" {
			return errors.New("unexpected proof")
		}
		consumed = proof
		return nil
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	client.mu.Lock()
	stored := len(client.identity.Proof)
	client.mu.Unlock()
	if stored != 0 || !session.proofWasCleared() || !allZero(consumed) {
		t.Fatalf("proof lifetime exceeded sink call: stored=%d issuedCleared=%t sinkCleared=%t", stored, session.proofWasCleared(), allZero(consumed))
	}
}

func TestIdentityProofIsZeroedWhenSinkPanics(t *testing.T) {
	session := newIdentitySession("panic-private-proof")
	var consumed []byte
	client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})
	client.config.ProofSink = func(_ context.Context, _ string, proof []byte) error {
		consumed = proof
		panic("private sink panic")
	}
	if err := client.Start(context.Background()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("start error = %v, want authentication", err)
	}
	if !session.proofWasCleared() || !allZero(consumed) {
		t.Fatalf("panic retained proof: issuedCleared=%t sinkCleared=%t", session.proofWasCleared(), allZero(consumed))
	}
	_ = client.Close(context.Background())
}

func TestIdentityProofIsZeroedWhenUnusedAndOnAuthenticationFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		authErr error
		wantErr error
	}{
		{name: "unused"},
		{name: "failed authentication", authErr: errors.New("private dependency detail"), wantErr: ErrAuthentication},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newIdentitySession("short-lived-proof")
			session.authErr = test.authErr
			client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})
			err := client.Start(context.Background())
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("start error = %v, want %v", err, test.wantErr)
			}
			if !session.proofWasCleared() {
				t.Fatal("authentication proof was not zeroed")
			}
			_ = client.Close(context.Background())
		})
	}
}

func TestStoredIdentityProofIsDefensivelyZeroedOnReplacementAndClose(t *testing.T) {
	first := newIdentitySession("first")
	second := newIdentitySession("second")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{first, second}})
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	legacy := []byte("legacy-proof")
	client.mu.Lock()
	client.identity.Proof = legacy
	client.mu.Unlock()
	if !client.reconnect() {
		t.Fatal("reconnect failed")
	}
	if !allZero(legacy) {
		t.Fatal("replaced identity proof was not zeroed")
	}
	current := []byte("current-proof")
	client.mu.Lock()
	client.identity.Proof = current
	client.mu.Unlock()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !allZero(current) {
		t.Fatal("closed identity proof was not zeroed")
	}
}

func allZero(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

func TestAuthenticatedIdentityConcurrentClose(t *testing.T) {
	session := newIdentitySession("private-proof")
	client := newIdentityClient(t, &identityDialer{sessions: []Session{session}})
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const readers = 32
	stop := make(chan struct{})
	failures := make(chan error, readers)
	var readersDone sync.WaitGroup
	for i := 0; i < readers; i++ {
		readersDone.Add(1)
		go func() {
			defer readersDone.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				identity, err := client.AuthenticatedIdentity()
				if err != nil && !errors.Is(err, ErrUnavailable) {
					failures <- err
					return
				}
				if err == nil && (identity.BareIdentity != "a@example.test" || identity.BoundIdentity != "a@example.test/mesh" || identity.MeshID != "mesh") {
					failures <- errors.New("partial authenticated identity")
					return
				}
			}
		}()
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(stop)
	readersDone.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if identity, err := client.AuthenticatedIdentity(); !errors.Is(err, ErrUnavailable) || identity != (AuthenticatedIdentity{}) {
		t.Fatalf("identity after concurrent close = %#v, %v", identity, err)
	}
}
