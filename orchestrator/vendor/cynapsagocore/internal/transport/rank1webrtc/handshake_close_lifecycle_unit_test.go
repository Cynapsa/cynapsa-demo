package rank1webrtc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/handshake"
)

type lifecycleBlockingAuthority struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (authority *lifecycleBlockingAuthority) AuthorizeRank1(context.Context, string) error {
	authority.once.Do(func() { close(authority.entered) })
	<-authority.release
	return nil
}

func TestHandshakeCloseDeadlineJoinsOneLifecycleCleanup(t *testing.T) {
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	negotiator := &HandshakeNegotiator{
		sessions: make(map[string]*restartSession), reserved: make(map[string]restartReservation),
		lifetime: lifetime, cancel: cancelLifetime,
	}
	release := make(chan struct{})
	negotiator.wg.Add(1)
	go func() {
		defer negotiator.wg.Done()
		<-release
	}()
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	if err := negotiator.Close(first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close = %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- negotiator.Close(context.Background()) }()
	select {
	case err := <-joined:
		t.Fatalf("later Close returned before worker join: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-joined; err != nil {
		t.Fatalf("joined Close = %v", err)
	}
	if err := negotiator.Close(context.Background()); err != nil {
		t.Fatalf("terminal Close = %v", err)
	}
}

func TestHandshakeCloseJoinsAdmittedEstablish(t *testing.T) {
	assembly, _ := groupBAssembly(t, 1)
	servers := make([]ICEServer, 5000)
	for index := range servers {
		servers[index] = ICEServer{URLs: []string{"turn:example.test"}, Username: "private", Credential: []byte("secret")}
	}
	assembly.config.ICEServers = servers
	password := assembly.config.ICEServers[0].Credential
	authority := &lifecycleBlockingAuthority{entered: make(chan struct{}), release: make(chan struct{})}
	assembly.config.Authority = authority
	attempt := groupBAttempt(t, "peer/mesh", 1)
	establishResult := make(chan error, 1)
	go func() { establishResult <- assembly.Establish(context.Background(), attempt) }()
	select {
	case <-authority.entered:
	case <-time.After(time.Second):
		t.Fatal("Establish authority call was not entered")
	}
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(authority.release) }) }()

	first, cancelFirst := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFirst()
	if err := assembly.Close(first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close during Establish = %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- assembly.Close(context.Background()) }()
	select {
	case err := <-joined:
		t.Fatalf("Close completed before Establish joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(authority.release) })
	if err := <-establishResult; !errors.Is(err, handshake.ErrClosed) {
		t.Fatalf("Establish after Close = %v", err)
	}
	if err := <-joined; err != nil {
		t.Fatalf("joined Close = %v", err)
	}
	if !allZeroBytes(password) {
		t.Fatalf("Close retained TURN password=%x", password)
	}
}

func allZeroBytes(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
