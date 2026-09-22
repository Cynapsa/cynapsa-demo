package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

type serializedRecoverySession struct {
	*lifecycleSession
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	resumes   atomic.Int32
	active    atomic.Int32
	maximum   atomic.Int32
	queries   atomic.Int32
}

type orderedAuthorityResumeSession struct {
	*lifecycleSession
	mu             sync.Mutex
	steps          []string
	barrier        error
	barrierEntered chan struct{}
	barrierRelease chan struct{}
	barrierOnce    sync.Once
	replayCalls    atomic.Int32
	syncCalls      atomic.Int32
	discoveryCalls atomic.Int32
}

type boundaryCommittedResumeSession struct {
	*orderedAuthorityResumeSession
}

func (s *boundaryCommittedResumeSession) PrepareResume(ctx context.Context) (bool, error) {
	<-ctx.Done()
	s.record("prepare")
	return true, nil
}

type countingUnavailableDialer struct {
	calls atomic.Int32
}

func (d *countingUnavailableDialer) Dial(context.Context) (Session, error) {
	d.calls.Add(1)
	return nil, ErrUnavailable
}

func (s *orderedAuthorityResumeSession) record(step string) {
	s.mu.Lock()
	s.steps = append(s.steps, step)
	s.mu.Unlock()
}

func (s *orderedAuthorityResumeSession) PrepareResume(context.Context) (bool, error) {
	s.record("prepare")
	return true, nil
}

func (s *orderedAuthorityResumeSession) ReplayPrepared(context.Context, func(Stanza) bool, func(Stanza) (preparedReplayStanza, bool)) error {
	s.replayCalls.Add(1)
	s.record("replay")
	return nil
}

func (s *orderedAuthorityResumeSession) WaitResumeAuthorityResult(ctx context.Context) error {
	s.record("barrier")
	if s.barrierEntered != nil {
		s.barrierOnce.Do(func() { close(s.barrierEntered) })
	}
	if s.barrierRelease != nil {
		select {
		case <-s.barrierRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.barrier
}

func (s *orderedAuthorityResumeSession) QueryServerTime(context.Context) (time.Time, error) {
	s.record("calibrate")
	return time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), nil
}

func (s *orderedAuthorityResumeSession) SyncAuthority(ctx context.Context) (AuthoritySnapshot, error) {
	s.syncCalls.Add(1)
	return currentMembershipFixture(ctx, "a@example.test/mesh")
}

func (s *orderedAuthorityResumeSession) DiscoverAuthority(context.Context) error {
	s.discoveryCalls.Add(1)
	return nil
}

func TestPublicResumeReplaysControlBarrierBeforeRestoringCachedAuthority(t *testing.T) {
	session := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityResume = func(context.Context) (bool, error) {
		session.record("authority")
		return true, nil
	}
	resumed, err := client.Resume(context.Background())
	if err != nil || !resumed {
		t.Fatalf("Resume=(%t,%v)", resumed, err)
	}
	session.mu.Lock()
	steps := append([]string(nil), session.steps...)
	session.mu.Unlock()
	want := []string{"prepare", "barrier", "replay", "calibrate", "authority"}
	if len(steps) != len(want) {
		t.Fatalf("steps=%v", steps)
	}
	for index := range want {
		if steps[index] != want[index] {
			t.Fatalf("steps=%v", steps)
		}
	}
	if calls := session.syncCalls.Load(); calls != 0 {
		t.Fatalf("successful resume fetched a fresh snapshot: calls=%d", calls)
	}
	if calls := session.discoveryCalls.Load(); calls != 0 {
		t.Fatalf("successful resume repeated authority discovery: calls=%d", calls)
	}
	if client.authoritySuspended || client.state != DurableLive {
		t.Fatalf("suspended=%t state=%v", client.authoritySuspended, client.state)
	}
}

func TestPublicResumeProtocolBarrierFailureHardFencesRetainedData(t *testing.T) {
	session := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession(), barrier: ErrProtocol}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	client.retainedDataAuthority = true
	var fences, restores atomic.Int32
	client.authorityFence = func() { fences.Add(1) }
	client.authorityResume = func(context.Context) (bool, error) {
		restores.Add(1)
		return true, nil
	}
	resumed, err := client.Resume(context.Background())
	if resumed || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Resume=(%t,%v)", resumed, err)
	}
	if fences.Load() != 1 || restores.Load() != 0 || client.membershipReady || client.authoritySuspended || client.retainedDataAuthority {
		t.Fatalf("fences=%d restores=%d ready=%t suspended=%t", fences.Load(), restores.Load(), client.membershipReady, client.authoritySuspended)
	}
	select {
	case request := <-client.reconnectDemand:
		client.mu.Lock()
		current := client.stateTransitionOwnerCurrentLocked(request.owner)
		client.mu.Unlock()
		if !current {
			t.Fatal("clean-bind recovery was enqueued with the pre-transition owner")
		}
	default:
		t.Fatal("failed authority continuity did not request clean-bind recovery")
	}
}

func TestPublicResumeNetworkDeadlinePreservesRetainedData(t *testing.T) {
	session := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession(), barrier: context.DeadlineExceeded}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	client.retainedDataAuthority = true
	var fences atomic.Int32
	client.authorityFence = func() { fences.Add(1) }
	resumed, err := client.Resume(context.Background())
	if resumed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resume=(%t,%v)", resumed, err)
	}
	if fences.Load() != 0 || client.membershipReady || client.authoritySuspended || !client.retainedDataAuthority {
		t.Fatalf("fences=%d ready=%t suspended=%t retained=%t", fences.Load(), client.membershipReady, client.authoritySuspended, client.retainedDataAuthority)
	}
}

func TestLaterAuthenticatedRejectionFencesPreviouslyRetainedData(t *testing.T) {
	client := recoveryTestClient(t, newLifecycleSession())
	client.membershipReady = true
	client.authoritySuspended = true
	client.retainedDataAuthority = true
	var fences atomic.Int32
	client.authorityFence = func() { fences.Add(1) }
	first, ok := client.captureStateTransitionOwner()
	if !ok || !client.hardInvalidateAuthorityOwned(first, true) {
		t.Fatal("network failure did not retain Rank1 data")
	}
	if fences.Load() != 0 || !client.retainedDataAuthority {
		t.Fatalf("first failure fences=%d retained=%t", fences.Load(), client.retainedDataAuthority)
	}
	if err := client.setRecoveryStateOwned(first, client.generation, DurablePending); err != nil {
		t.Fatal(err)
	}
	second, ok := client.captureStateTransitionOwner()
	if !ok || !client.hardInvalidateAuthorityOwned(second, false) {
		t.Fatal("authenticated rejection did not own current retry")
	}
	if fences.Load() != 1 || client.retainedDataAuthority {
		t.Fatalf("second failure fences=%d retained=%t", fences.Load(), client.retainedDataAuthority)
	}
}

func TestPublicResumeCannotReplayBeforeAuthorityResult(t *testing.T) {
	session := &orderedAuthorityResumeSession{
		lifecycleSession: newLifecycleSession(),
		barrierEntered:   make(chan struct{}),
		barrierRelease:   make(chan struct{}),
	}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityResume = func(context.Context) (bool, error) { return true, nil }
	result := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		result <- err
	}()
	<-session.barrierEntered
	if calls := session.replayCalls.Load(); calls != 0 {
		t.Fatalf("application replay escaped before authority result: calls=%d", calls)
	}
	close(session.barrierRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if calls := session.replayCalls.Load(); calls != 1 {
		t.Fatalf("replay calls=%d", calls)
	}
}

func TestBackgroundReconnectUsesAuthorityResultWithoutFreshSnapshot(t *testing.T) {
	session := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityResume = func(context.Context) (bool, error) { return true, nil }
	if !client.reconnect() {
		t.Fatal("background resume failed")
	}
	if calls := session.syncCalls.Load(); calls != 0 {
		t.Fatalf("successful background resume fetched snapshot: calls=%d", calls)
	}
	if calls := session.replayCalls.Load(); calls != 1 {
		t.Fatalf("background replay calls=%d", calls)
	}
}

func TestBackgroundResumeGetsFreshPostCommitBudgetWithoutCleanFallback(t *testing.T) {
	session := &boundaryCommittedResumeSession{
		orderedAuthorityResumeSession: &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()},
	}
	client := recoveryTestClient(t, session)
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityResume = func(context.Context) (bool, error) {
		session.record("authority")
		return true, nil
	}
	dialer := &countingUnavailableDialer{}
	client.dialer = dialer
	if !client.reconnect() {
		t.Fatal("committed resume fell through to clean reconnect")
	}
	if calls := dialer.calls.Load(); calls != 0 {
		t.Fatalf("committed resume performed fresh dial: calls=%d", calls)
	}
	if session.discoveryCalls.Load() != 0 || session.syncCalls.Load() != 0 {
		t.Fatalf("committed resume rediscovered authority: discovery=%d snapshot=%d", session.discoveryCalls.Load(), session.syncCalls.Load())
	}
	session.mu.Lock()
	steps := append([]string(nil), session.steps...)
	session.mu.Unlock()
	want := []string{"prepare", "barrier", "replay", "calibrate", "authority"}
	if len(steps) != len(want) {
		t.Fatalf("steps=%v", steps)
	}
	for index := range want {
		if steps[index] != want[index] {
			t.Fatalf("steps=%v", steps)
		}
	}
}

func TestPublicResumeGetsFreshPostCommitBudget(t *testing.T) {
	session := &boundaryCommittedResumeSession{
		orderedAuthorityResumeSession: &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()},
	}
	client := recoveryTestClient(t, session)
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityResume = func(context.Context) (bool, error) {
		session.record("authority")
		return true, nil
	}
	resumed, err := client.Resume(context.Background())
	if err != nil || !resumed {
		t.Fatalf("Resume=(%t,%v)", resumed, err)
	}
	session.mu.Lock()
	steps := append([]string(nil), session.steps...)
	session.mu.Unlock()
	want := []string{"prepare", "barrier", "replay", "calibrate", "authority"}
	if len(steps) != len(want) {
		t.Fatalf("steps=%v", steps)
	}
	for index := range want {
		if steps[index] != want[index] {
			t.Fatalf("steps=%v", steps)
		}
	}
}

func TestPrepareResumeUsesFreshSessionLifetimeAndReplaysPendingSessionControls(t *testing.T) {
	certificateServer := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificateServer.StartTLS()
	certificate := certificateServer.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certificateServer.Certificate())
	certificateServer.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	externalID := "cynapsa-extdisco-UG43NPMZB66XVFNBKNZOUCNOVU"
	externalQuery, err := encodeExternalServiceQuery("services", ExternalService{})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(externalQuery)
	serverDone := make(chan error, 1)
	probeParser := make(chan struct{})
	postcommitContinued := make(chan struct{})
	releaseServer := make(chan struct{})
	var releaseServerOnce sync.Once
	defer releaseServerOnce.Do(func() { close(releaseServer) })
	replacementAccepted := make(chan struct{}, 1)
	acceptedConnections := atomic.Int32{}
	bindAttempts := atomic.Int32{}
	serverStep := atomic.Int32{}
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		acceptedConnections.Add(1)
		serverStep.Store(1)
		go func() {
			replacement, replacementErr := listener.Accept()
			if replacementErr != nil {
				return
			}
			acceptedConnections.Add(1)
			_ = replacement.Close()
			replacementAccepted <- struct{}{}
		}()
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		serverTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}
		serverSession, receiveErr := xmpp.ReceiveClientSession(context.Background(), jid.MustParse("example.test"), connection,
			xmpp.StartTLS(serverTLS),
			xmpp.SASLServer(func(*sasl.Negotiator) bool { return true }, sasl.Plain),
			xmpp.BindCustom(func(identity jid.JID, resource string) (jid.JID, error) {
				bindAttempts.Add(1)
				return jid.JID{}, fmt.Errorf("unexpected resource bind for %s/%s after accepted resume", identity, resource)
			}),
			wireResumeServerFeature("resume-token", 1, 1),
		)
		if receiveErr != nil {
			serverDone <- receiveErr
			return
		}
		serverStep.Store(2)
		defer serverSession.Close()
		if writeErr := writeSMRequest(serverSession); writeErr != nil {
			serverDone <- writeErr
			return
		}
		serverStep.Store(3)
		local := jid.MustParse("agent@example.test/mesh")
		result := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: resumeAuthorityIDPrefix + "7", From: jid.MustParse("example.test"), To: local, Type: stanza.ResultIQ}
		payload := []byte(`<resume-authority xmlns="urn:cynapsa:mesh-authority:1" status="ready"/>`)
		if sendErr := serverSession.SendElement(context.Background(), xml.NewDecoder(bytes.NewReader(payload)), result.StartElement()); sendErr != nil {
			serverDone <- sendErr
			return
		}
		serverStep.Store(4)
		if readErr := readSMAck(serverSession); readErr != nil {
			serverDone <- readErr
			return
		}
		if readErr := readExactPingResultReplay(serverSession, "idle-ping-before-resume", "agent@example.test/mesh", "example.test"); readErr != nil {
			serverDone <- readErr
			return
		}
		if readErr := readSMRequest(serverSession); readErr != nil {
			serverDone <- readErr
			return
		}
		if writeErr := writeSMAck(serverSession, 2); writeErr != nil {
			serverDone <- writeErr
			return
		}
		if readErr := readExactExternalServiceQueryReplay(serverSession, externalID); readErr != nil {
			serverDone <- readErr
			return
		}
		if readErr := readSMRequest(serverSession); readErr != nil {
			serverDone <- readErr
			return
		}
		externalResult := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: externalID, From: jid.MustParse("example.test"), To: local, Type: stanza.ResultIQ}
		externalPayload := []byte(`<services xmlns="urn:xmpp:extdisco:2"><service host="stun.test" port="3478" transport="udp" type="stun"/></services>`)
		if sendErr := serverSession.SendElement(context.Background(), xml.NewDecoder(bytes.NewReader(externalPayload)), externalResult.StartElement()); sendErr != nil {
			serverDone <- sendErr
			return
		}
		if writeErr := writeSMAck(serverSession, 3); writeErr != nil {
			serverDone <- writeErr
			return
		}
		select {
		case <-probeParser:
		case <-time.After(2 * time.Second):
			serverDone <- errors.New("parser probe was not requested")
			return
		}
		if writeErr := writeSMRequest(serverSession); writeErr != nil {
			serverDone <- writeErr
			return
		}
		if readErr := readSMAck(serverSession); readErr != nil {
			serverDone <- readErr
			return
		}
		close(postcommitContinued)
		<-releaseServer
		serverDone <- nil
	}()

	endpoint, err := ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	management, err := NewStreamManagement(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = management.Enable("resume-token", true); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(Stanza{Kind: StanzaEnvelope, From: "agent@example.test/mesh", To: "peer@example.test/mesh", MeshID: "mesh", Ordinal: 41, MessageID: "msg_0123456789abcdef"}); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(Stanza{Kind: StanzaSessionPingResult, From: "agent@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "idle-ping-before-resume"}); err != nil {
		t.Fatal(err)
	}
	if err = management.RecordSent(Stanza{Kind: StanzaExternalServiceQuery, From: "agent@example.test/mesh", To: "example.test", MeshID: "mesh", AttemptID: "services", MessageID: externalID, Data: externalQuery}); err != nil {
		t.Fatal(err)
	}
	management.MarkHandledInbound()

	operation, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	session := newMelliumSession(MelliumConfig{
		TLSConfig:                    &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13},
		SASLMechanisms:               []sasl.Mechanism{sasl.Plain},
		ReceiveCapacity:              4,
		StreamManagementCapacity:     4,
		StreamManagementByteCapacity: 1 << 20,
		MaximumFrameBytes:            4096,
		StanzaBudgetBytes:            4096,
	}, endpoint)
	session.username = "agent@example.test"
	session.password = []byte("secret")
	session.meshID = "mesh"
	session.management = management
	session.generation = 6
	session.setAuthorityIngressFence(func(uint64) bool { return true })
	session.afterResumeAccepted = func() {
		cancelOperation()
		<-operation.Done()
	}

	resumed, err := session.PrepareResume(operation)
	if err != nil || !resumed {
		select {
		case serverErr := <-serverDone:
			t.Fatalf("PrepareResume=(%t,%v), old operation=%v, server=%v", resumed, err, operation.Err(), serverErr)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("PrepareResume=(%t,%v), old operation=%v", resumed, err, operation.Err())
		}
	}
	if !errors.Is(operation.Err(), context.Canceled) {
		t.Fatalf("pre-commit operation did not expire at acceptance: %v", operation.Err())
	}

	receive, cancelReceive := context.WithTimeout(context.Background(), 2*time.Second)
	handled, err := session.ReceiveControl(receive)
	cancelReceive()
	if err != nil {
		t.Fatal(err)
	}
	defer clearEventOwned(&handled)
	if handled.Kind != EventHandled || handled.HandledThrough != 41 || handled.HandledCount != 1 || handled.sessionGeneration != 7 {
		t.Fatalf("handled event=%+v", handled)
	}

	receive, cancelReceive = context.WithTimeout(context.Background(), 2*time.Second)
	authority, err := session.ReceiveControl(receive)
	cancelReceive()
	if err != nil {
		select {
		case serve := <-session.serveDone:
			t.Fatalf("authority receive=%v serve=%v generation=%d server-step=%d", err, serve.err, serve.generation, serverStep.Load())
		case serverErr := <-serverDone:
			t.Fatalf("authority receive=%v server=%v server-step=%d", err, serverErr, serverStep.Load())
		default:
			t.Fatalf("authority receive=%v server-step=%d", err, serverStep.Load())
		}
	}
	if authority.Kind != EventResumeAuthorityResult || authority.resumeBarrier == nil || authority.sessionGeneration != 7 {
		clearEventOwned(&authority)
		t.Fatalf("authority event=%+v", authority)
	}
	close(authority.resumeBarrier.done)
	clearEventOwned(&authority)
	barrierCtx, cancelBarrier := context.WithTimeout(context.Background(), time.Second)
	if err = session.WaitResumeAuthorityResult(barrierCtx); err != nil {
		cancelBarrier()
		t.Fatalf("authority barrier: %v", err)
	}
	cancelBarrier()
	replayCtx, cancelReplay := context.WithTimeout(context.Background(), time.Second)
	if err = session.ReplayPrepared(replayCtx, func(Stanza) bool { return true }, func(record Stanza) (preparedReplayStanza, bool) {
		return preparedReplayStanza{Stanza: record}, true
	}); err != nil {
		cancelReplay()
		t.Fatalf("replay pending ping result: %v", err)
	}
	cancelReplay()
	duplicateReplayCtx, cancelDuplicateReplay := context.WithTimeout(context.Background(), time.Second)
	duplicateReplayErr := session.ReplayPrepared(duplicateReplayCtx, func(Stanza) bool { return true }, func(record Stanza) (preparedReplayStanza, bool) {
		return preparedReplayStanza{Stanza: record}, true
	})
	cancelDuplicateReplay()
	if !errors.Is(duplicateReplayErr, ErrUnavailable) {
		t.Fatalf("duplicate ping replay error=%v", duplicateReplayErr)
	}
	for index, expectedCount := range []uint32{1, 1, 0} {
		ackCtx, cancelAck := context.WithTimeout(context.Background(), time.Second)
		handledEvent, handledErr := session.ReceiveControl(ackCtx)
		cancelAck()
		if handledErr != nil {
			t.Fatalf("replayed control acknowledgement %d: %v", index, handledErr)
		}
		if handledEvent.Kind != EventHandled || handledEvent.HandledThrough != 0 || handledEvent.HandledCount != expectedCount || handledEvent.sessionGeneration != 7 {
			clearEventOwned(&handledEvent)
			t.Fatalf("replayed control handled event %d=%+v", index, handledEvent)
		}
		clearEventOwned(&handledEvent)
	}

	probe, cancelProbe := context.WithTimeout(context.Background(), 20*time.Millisecond)
	duplicate, duplicateErr := session.ReceiveControl(probe)
	cancelProbe()
	if !errors.Is(duplicateErr, context.DeadlineExceeded) {
		clearEventOwned(&duplicate)
		t.Fatalf("handled publication was not exactly once: event=%+v error=%v", duplicate, duplicateErr)
	}
	close(probeParser)
	select {
	case <-postcommitContinued:
	case err = <-serverDone:
		t.Fatalf("resumed parser did not continue through postcommit: %v", err)
	case <-time.After(time.Second):
		t.Fatal("resumed parser did not continue through postcommit")
	}
	deadline := time.Now().Add(time.Second)
	for management.Pending() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pending := management.PendingSnapshot(); len(pending) != 0 {
		t.Fatalf("ping replay added or retained ledger records: %#v", pending)
	}
	session.mu.Lock()
	generation, suspended, active := session.generation, session.suspended, session.session != nil && session.ctx != nil && session.ctx.Err() == nil
	session.mu.Unlock()
	if generation != 7 || suspended || !active {
		t.Fatalf("resumed session state: generation=%d suspended=%t active=%t", generation, suspended, active)
	}
	if got := acceptedConnections.Load(); got != 1 {
		t.Fatalf("replacement connections=%d", got)
	}
	if got := bindAttempts.Load(); got != 0 {
		t.Fatalf("bind requests after accepted resume=%d", got)
	}
	select {
	case <-replacementAccepted:
		t.Fatal("resume failure requested a clean replacement connection")
	case <-time.After(20 * time.Millisecond):
	}
	releaseServerOnce.Do(func() { close(releaseServer) })
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if closeErr := session.Close(closeCtx); closeErr != nil {
		t.Fatal(closeErr)
	}
}

func wireResumeServerFeature(resumeID string, handled, expectedClientHandled uint32) xmpp.StreamFeature {
	return xmpp.StreamFeature{
		Name:       xml.Name{Space: streamManagementNamespace, Local: "resume"},
		Necessary:  xmpp.Authn,
		Prohibited: xmpp.Ready,
		List: func(_ context.Context, writer xmlstream.TokenWriter, _ xml.StartElement) (bool, error) {
			start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "sm"}}
			if err := writer.EncodeToken(start); err != nil {
				return false, err
			}
			return true, writer.EncodeToken(start.End())
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			token, err := decoder.Token()
			if err != nil {
				return 0, nil, err
			}
			start, ok := token.(xml.StartElement)
			if !ok || start.Name != (xml.Name{Space: streamManagementNamespace, Local: "resume"}) {
				return 0, nil, fmt.Errorf("unexpected resume token %T %#v", token, token)
			}
			var request struct {
				XMLName  xml.Name `xml:"urn:xmpp:sm:3 resume"`
				Previous string   `xml:"previd,attr"`
				Handled  uint32   `xml:"h,attr"`
			}
			if err = decoder.DecodeElement(&request, &start); err != nil {
				return 0, nil, err
			}
			if request.Previous != resumeID {
				return 0, nil, fmt.Errorf("resume previd=%q", request.Previous)
			}
			if request.Handled != expectedClientHandled {
				return 0, nil, fmt.Errorf("resume client h=%d want %d", request.Handled, expectedClientHandled)
			}
			writer := session.TokenWriter()
			defer writer.Close()
			response := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "resumed"}, Attr: []xml.Attr{
				{Name: xml.Name{Local: "h"}, Value: fmt.Sprint(handled)},
				{Name: xml.Name{Local: "previd"}, Value: resumeID},
			}}
			if err = writer.EncodeToken(response); err == nil {
				err = writer.EncodeToken(response.End())
			}
			if err == nil {
				err = writer.Flush()
			}
			return xmpp.Ready, nil, err
		},
	}
}

func writeSMRequest(session *xmpp.Session) error {
	writer := session.TokenWriter()
	defer writer.Close()
	if err := encodeSMRequest(writer); err != nil {
		return err
	}
	return writer.Flush()
}

func readSMAck(session *xmpp.Session) error {
	reader := session.TokenReader()
	defer reader.Close()
	decoder := xml.NewTokenDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: streamManagementNamespace, Local: "a"}) {
		return fmt.Errorf("unexpected SM ack token %T %#v", token, token)
	}
	return decoder.Skip()
}

func readExactPingResultReplay(session *xmpp.Session, id, from, to string) error {
	reader := session.TokenReader()
	defer reader.Close()
	decoder := xml.NewTokenDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		return fmt.Errorf("unexpected ping replay token %T %#v", token, token)
	}
	iq, err := stanza.NewIQ(start)
	if err != nil {
		return err
	}
	if iq.Type != stanza.ResultIQ || iq.ID != id || iq.From.String() != from || iq.To.String() != to {
		return fmt.Errorf("unexpected ping replay %#v", iq)
	}
	if len(start.Attr) != 5 {
		return fmt.Errorf("unexpected ping replay attributes %#v", start.Attr)
	}
	seen := make(map[string]bool, 5)
	for _, attribute := range start.Attr {
		if attribute.Name.Space == "" && attribute.Name.Local == "xmlns" && attribute.Value == stanza.NSClient {
			seen["xmlns"] = true
			continue
		}
		if attribute.Name.Space != "" {
			return fmt.Errorf("unexpected ping replay attribute %#v", attribute)
		}
		switch attribute.Name.Local {
		case "id", "from", "to", "type":
			if seen[attribute.Name.Local] {
				return fmt.Errorf("duplicate ping replay attribute %#v", attribute)
			}
			seen[attribute.Name.Local] = true
		default:
			return fmt.Errorf("unexpected ping replay attribute %#v", attribute)
		}
	}
	if len(seen) != 5 {
		return fmt.Errorf("missing ping replay attributes %#v", start.Attr)
	}
	token, err = decoder.Token()
	if err != nil || token != start.End() {
		return fmt.Errorf("unexpected ping replay payload token=%#v err=%v", token, err)
	}
	return nil
}

func readExactExternalServiceQueryReplay(session *xmpp.Session, id string) error {
	reader := session.TokenReader()
	defer reader.Close()
	decoder := xml.NewTokenDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	start, ok := token.(xml.StartElement)
	if !ok {
		return fmt.Errorf("unexpected external-service replay token %T %#v", token, token)
	}
	iq, err := stanza.NewIQ(start)
	if err != nil {
		return err
	}
	if iq.Type != stanza.GetIQ || iq.ID != id || iq.To.String() != "example.test" || iq.From.String() != "" {
		return fmt.Errorf("unexpected external-service replay %#v", iq)
	}
	var wire struct {
		XMLName xml.Name
		Query   struct {
			XMLName xml.Name
			Text    string       `xml:",chardata"`
			Unknown []xmlUnknown `xml:",any"`
		} `xml:"urn:xmpp:extdisco:2 services"`
	}
	if err = decoder.DecodeElement(&wire, &start); err != nil {
		return err
	}
	if wire.XMLName != (xml.Name{Space: stanza.NSClient, Local: "iq"}) ||
		wire.Query.XMLName != (xml.Name{Space: externalServiceNamespace, Local: "services"}) ||
		strings.TrimSpace(wire.Query.Text) != "" || len(wire.Query.Unknown) != 0 {
		return fmt.Errorf("unexpected external-service replay body %#v", wire)
	}
	return nil
}

func readSMRequest(session *xmpp.Session) error {
	reader := session.TokenReader()
	defer reader.Close()
	decoder := xml.NewTokenDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: streamManagementNamespace, Local: "r"}) {
		return fmt.Errorf("unexpected SM request token %T %#v", token, token)
	}
	return decodeSMRequest(decoder, start)
}

func writeSMAck(session *xmpp.Session, handled uint32) error {
	writer := session.TokenWriter()
	defer writer.Close()
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "a"}, Attr: []xml.Attr{{Name: xml.Name{Local: "h"}, Value: fmt.Sprint(handled)}}}
	if err := writer.EncodeToken(start); err != nil {
		return err
	}
	if err := writer.EncodeToken(start.End()); err != nil {
		return err
	}
	return writer.Flush()
}

func TestPublicCommittedResumePostCommitWaitIsBoundedAndRequestsFallback(t *testing.T) {
	session := &orderedAuthorityResumeSession{
		lifecycleSession: newLifecycleSession(),
		barrierEntered:   make(chan struct{}),
		barrierRelease:   make(chan struct{}),
	}
	client := recoveryTestClient(t, session)
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	client.membershipReady = true
	client.authoritySuspended = true
	started := time.Now()
	resumed, err := client.Resume(context.Background())
	if resumed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resume=(%t,%v)", resumed, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("post-commit wait was unbounded: %s", elapsed)
	}
	if session.replayCalls.Load() != 0 {
		t.Fatalf("replay escaped timed-out authority barrier: calls=%d", session.replayCalls.Load())
	}
	select {
	case request := <-client.reconnectDemand:
		client.mu.Lock()
		current := client.stateTransitionOwnerCurrentLocked(request.owner)
		client.mu.Unlock()
		if !current {
			t.Fatal("fallback request did not own the Pending publication")
		}
	default:
		t.Fatal("post-commit timeout did not request clean fallback")
	}
}

func TestBackgroundNotReadyForcesCleanBindAndFreshSnapshot(t *testing.T) {
	old := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession(), barrier: ErrUnavailable}
	fresh := &orderedAuthorityResumeSession{lifecycleSession: newLifecycleSession()}
	client := correctionClient(t, &orderedDialer{sessions: []Session{fresh}})
	client.ctx = context.Background()
	client.session = old
	client.identity = Authenticated{BareIdentity: "a@example.test", BoundIdentity: "a@example.test/mesh"}
	client.started = true
	client.state = DurablePending
	client.membershipReady = true
	client.authoritySuspended = true
	client.authorityFence = func() {}
	client.authorityResume = func(context.Context) (bool, error) { return true, nil }
	if !client.reconnect() {
		t.Fatal("clean-bind recovery failed")
	}
	if calls := fresh.syncCalls.Load(); calls != 1 {
		t.Fatalf("fresh snapshot calls=%d", calls)
	}
	if calls := fresh.discoveryCalls.Load(); calls != 1 {
		t.Fatalf("fresh authority discovery calls=%d", calls)
	}
	client.mu.Lock()
	current, ready, snapshotNonce := client.session, client.membershipReady, client.authoritySnapshot.nonce
	client.mu.Unlock()
	if current != fresh || ready || snapshotNonce == "" {
		t.Fatalf("current=%T ready=%t snapshot=%q", current, ready, snapshotNonce)
	}
	_ = client.Close(context.Background())
}

func TestPublicResumeMembershipMutationRaceCannotReopenAuthority(t *testing.T) {
	session := &orderedAuthorityResumeSession{
		lifecycleSession: newLifecycleSession(),
		barrierEntered:   make(chan struct{}),
		barrierRelease:   make(chan struct{}),
	}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	var restores atomic.Int32
	client.authorityResume = func(context.Context) (bool, error) {
		restores.Add(1)
		return true, nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		result <- err
	}()
	<-session.barrierEntered
	client.mu.Lock()
	client.membershipReady = false
	client.authoritySuspended = false
	client.authoritySnapshot = AuthoritySnapshot{}
	client.mu.Unlock()
	close(session.barrierRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	ready := client.membershipReady
	client.mu.Unlock()
	if ready || restores.Load() != 0 {
		t.Fatalf("mutation was reopened: ready=%t restores=%d", ready, restores.Load())
	}
}

func TestBackgroundResumeMembershipMutationRaceCannotReopenAuthority(t *testing.T) {
	session := &orderedAuthorityResumeSession{
		lifecycleSession: newLifecycleSession(),
		barrierEntered:   make(chan struct{}),
		barrierRelease:   make(chan struct{}),
	}
	client := recoveryTestClient(t, session)
	client.membershipReady = true
	client.authoritySuspended = true
	var restores atomic.Int32
	client.authorityResume = func(context.Context) (bool, error) {
		restores.Add(1)
		return true, nil
	}
	result := make(chan bool, 1)
	go func() { result <- client.reconnect() }()
	<-session.barrierEntered
	client.mu.Lock()
	client.membershipReady = false
	client.authoritySuspended = false
	client.authoritySnapshot = AuthoritySnapshot{}
	client.mu.Unlock()
	close(session.barrierRelease)
	if !<-result {
		t.Fatal("transport resume failed after authority mutation")
	}
	client.mu.Lock()
	ready := client.membershipReady
	client.mu.Unlock()
	if ready || restores.Load() != 0 {
		t.Fatalf("mutation was reopened: ready=%t restores=%d", ready, restores.Load())
	}
}

func newSerializedRecoverySession() *serializedRecoverySession {
	return &serializedRecoverySession{lifecycleSession: newLifecycleSession(), entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *serializedRecoverySession) Resume(ctx context.Context) (bool, error) {
	s.resumes.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maximum.Load()
		if active <= maximum || s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	s.enterOnce.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *serializedRecoverySession) QueryServerTime(context.Context) (time.Time, error) {
	s.queries.Add(1)
	return time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC), nil
}

func TestConcurrentResumeWaitIsContextCancellableAndDoesNotCrossFence(t *testing.T) {
	session := newSerializedRecoverySession()
	client := recoveryTestClient(t, session)
	first := make(chan error, 1)
	go func() {
		resumed, err := client.Resume(context.Background())
		if err == nil && !resumed {
			err = errors.New("first resume did not resume")
		}
		first <- err
	}()
	<-session.entered

	waitCtx, cancelWait := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, err := client.Resume(waitCtx)
		second <- err
	}()
	cancelWait()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting Resume error = %v", err)
	}
	if got := session.resumes.Load(); got != 1 {
		t.Fatalf("dependency Resume calls while first compound operation active = %d", got)
	}
	close(session.release)
	if err := <-first; err != nil {
		t.Fatalf("first Resume = %v", err)
	}
	if session.maximum.Load() != 1 || session.queries.Load() != 1 {
		t.Fatalf("maximum concurrent resumes=%d queries=%d", session.maximum.Load(), session.queries.Load())
	}
}

func TestPublicResumeAndBackgroundReconnectShareRecoveryOwnership(t *testing.T) {
	session := newSerializedRecoverySession()
	client := recoveryTestClient(t, session)
	reconnected := make(chan bool, 1)
	go func() { reconnected <- client.reconnect() }()
	<-session.entered

	waitCtx, cancelWait := context.WithCancel(context.Background())
	public := make(chan error, 1)
	go func() {
		_, err := client.Resume(waitCtx)
		public <- err
	}()
	cancelWait()
	if err := <-public; !errors.Is(err, context.Canceled) {
		t.Fatalf("public Resume while reconnect owns recovery = %v", err)
	}
	if got := session.resumes.Load(); got != 1 {
		t.Fatalf("public Resume crossed background reconnect: calls=%d", got)
	}
	close(session.release)
	if ok := <-reconnected; !ok {
		t.Fatal("background reconnect failed")
	}
	if session.maximum.Load() != 1 || session.queries.Load() != 1 {
		t.Fatalf("maximum concurrent resumes=%d queries=%d", session.maximum.Load(), session.queries.Load())
	}
}

type deadlineRecoverySession struct {
	*lifecycleSession
	sawDeadline atomic.Bool
}

func (s *deadlineRecoverySession) Resume(ctx context.Context) (bool, error) {
	if _, ok := ctx.Deadline(); ok {
		s.sawDeadline.Store(true)
	}
	<-ctx.Done()
	return false, ctx.Err()
}

func TestPublicResumeUsesConfiguredOperationDeadline(t *testing.T) {
	session := &deadlineRecoverySession{lifecycleSession: newLifecycleSession()}
	client := recoveryTestClient(t, session)
	client.config.ReconnectOperationTimeout = 10 * time.Millisecond
	started := time.Now()
	_, err := client.Resume(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resume error = %v", err)
	}
	if !session.sawDeadline.Load() {
		t.Fatal("dependency Resume received no deadline")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("configured Resume deadline took %s", elapsed)
	}
}

type lateResumeSession struct {
	*lifecycleSession
	entered chan struct{}
	release chan struct{}
}

func (s *lateResumeSession) Resume(context.Context) (bool, error) {
	close(s.entered)
	<-s.release
	return true, nil
}

func (s *lateResumeSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC), nil
}

func TestPublicResumeCannotPublishLiveAfterClose(t *testing.T) {
	session := &lateResumeSession{
		lifecycleSession: newLifecycleSession(),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	client := recoveryTestClient(t, session)
	result := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		result <- err
	}()
	<-session.entered
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	close(session.release)
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("late Resume error = %v, want ErrClosed", err)
	}
	if got := client.DurableState(); got == DurableLive {
		t.Fatalf("late Resume published state %v after Close", got)
	}
}

type lateRejectedResumeSession struct {
	*lifecycleSession
	entered chan struct{}
	release chan struct{}
}

func (s *lateRejectedResumeSession) Resume(context.Context) (bool, error) {
	close(s.entered)
	<-s.release
	return false, nil
}

func TestCancelledResumeCannotReturnLateBenignRejection(t *testing.T) {
	session := &lateRejectedResumeSession{lifecycleSession: newLifecycleSession(), entered: make(chan struct{}), release: make(chan struct{})}
	client := recoveryTestClient(t, session)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Resume(ctx)
		result <- err
	}()
	<-session.entered
	cancel()
	close(session.release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("late rejected Resume error = %v, want context.Canceled", err)
	}
}

func TestClosedResumeCannotReturnLateBenignRejection(t *testing.T) {
	session := &lateRejectedResumeSession{lifecycleSession: newLifecycleSession(), entered: make(chan struct{}), release: make(chan struct{})}
	client := recoveryTestClient(t, session)
	result := make(chan error, 1)
	go func() {
		_, err := client.Resume(context.Background())
		result <- err
	}()
	<-session.entered
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(session.release)
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("late rejected Resume error = %v, want ErrClosed", err)
	}
}

func recoveryTestClient(t *testing.T, session Session) *Client {
	t.Helper()
	client := correctionClient(t, fakeDialer{session})
	client.ctx = context.Background()
	client.session = session
	client.identity = Authenticated{BareIdentity: "a@example.test", BoundIdentity: "a@example.test/mesh"}
	client.started = true
	client.state = DurablePending
	return client
}
