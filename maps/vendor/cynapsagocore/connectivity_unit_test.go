package cynapsagocore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type builtInTestDialer struct {
	session rank2xmpp.Session
	err     error
}

func (dialer builtInTestDialer) Dial(context.Context) (rank2xmpp.Session, error) {
	return dialer.session, dialer.err
}

type builtInTestSession struct {
	mu        sync.Mutex
	username  string
	syncCalls int
	peerCalls int
	closed    chan struct{}
	once      sync.Once
}

func newBuiltInTestSession() *builtInTestSession {
	return &builtInTestSession{closed: make(chan struct{})}
}

func (*builtInTestSession) ConnectTLS(_ context.Context, endpoint string) error {
	if endpoint != "mesh.example.test:5222" {
		return rank2xmpp.ErrInvalidConfig
	}
	return nil
}

func (session *builtInTestSession) Authenticate(_ context.Context, username string, password []byte) (string, []byte, error) {
	if username != "agent@example.test" || string(password) != "secret" {
		return "", nil, rank2xmpp.ErrAuthentication
	}
	session.mu.Lock()
	session.username = username
	session.mu.Unlock()
	return username, nil, nil
}

func (session *builtInTestSession) BindResource(_ context.Context, resource string) (string, error) {
	session.mu.Lock()
	username := session.username
	session.mu.Unlock()
	if resource != "mesh-one" || username == "" {
		return "", rank2xmpp.ErrIdentityBinding
	}
	return username + "/" + resource, nil
}

func (*builtInTestSession) EnableStreamManagement(_ context.Context, resumable bool) error {
	if !resumable {
		return rank2xmpp.ErrStreamManagement
	}
	return nil
}

// DiscoverAuthority is the explicit test seam for the mandatory fresh-session
// capability negotiation. Production sessions must prove this over XEP-0030;
// this injected fixture opts in deliberately instead of bypassing the phase.
func (*builtInTestSession) DiscoverAuthority(context.Context) error { return nil }

func (*builtInTestSession) QueryServerTime(context.Context) (time.Time, error) {
	return time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC), nil
}

func (session *builtInTestSession) SyncAuthority(context.Context) (rank2xmpp.AuthoritySnapshot, error) {
	session.mu.Lock()
	session.syncCalls++
	session.mu.Unlock()
	return rank2xmpp.AuthoritySnapshot{Members: []string{"agent@example.test/mesh-one", "peer@example.test/mesh-one"}}, nil
}

func (session *builtInTestSession) ResolveAuthorizedPeer(_ context.Context, bare string) (rank2xmpp.AuthorizedPeer, error) {
	session.mu.Lock()
	session.peerCalls++
	session.mu.Unlock()
	if bare != "peer@example.test" {
		return rank2xmpp.AuthorizedPeer{}, rank2xmpp.ErrUnavailable
	}
	return rank2xmpp.AuthorizedPeer{FullJID: "peer@example.test/r2.install-peer.nonce-1", InstallationID: "install-peer", SessionGeneration: "session-1"}, nil
}

func (*builtInTestSession) Send(context.Context, rank2xmpp.Stanza) error { return nil }

func (session *builtInTestSession) Receive(ctx context.Context) (rank2xmpp.Event, error) {
	select {
	case <-ctx.Done():
		return rank2xmpp.Event{}, ctx.Err()
	case <-session.closed:
		return rank2xmpp.Event{}, rank2xmpp.ErrClosed
	}
}

func (*builtInTestSession) Resume(context.Context) (bool, error) { return false, nil }
func (*builtInTestSession) CatchUp(context.Context, int) ([]rank2xmpp.Stanza, error) {
	return nil, nil
}
func (session *builtInTestSession) Close(context.Context) error {
	session.once.Do(func() { close(session.closed) })
	return nil
}

func TestBuiltInConnectivityAuthenticatesBothSDKPersonalities(t *testing.T) {
	for _, test := range []struct {
		name        string
		personality v1.SDKPersonality
		command     func(v1.CommandBase, v1.AuthInput) v1.Command
	}{
		{name: "login", personality: v1.SDKPersonalityHTTPBridge, command: func(base v1.CommandBase, auth v1.AuthInput) v1.Command {
			return v1.AuthLoginCommand{CommandBase: base, Auth: auth}
		}},
		{name: "connect", personality: v1.SDKPersonalityNative, command: func(base v1.CommandBase, auth v1.AuthInput) v1.Command {
			return v1.AuthConnectCommand{CommandBase: base, Auth: auth}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newBuiltInTestSession()
			factory := newBuiltInConnectivityFactory(func(endpoint string, _ sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
				if parsed, err := rank2xmpp.ParseEndpoint(endpoint); err != nil || parsed.DialAddress() != "mesh.example.test:5222" || parsed.TLSServerName() != "mesh.example.test" {
					t.Fatalf("endpoint = %#v, %v", parsed, err)
				}
				return builtInTestDialer{session: session}, nil
			})
			core := newTestCoreWithConnectivity(t, factory)
			base := v1.CommandBase{CommandID: v1.CommandID("auth-" + test.name), SDKSessionID: v1.SDKSessionID("session-" + test.name)}
			auth := v1.AuthInput{MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test", Password: "secret", MeshID: "mesh-one", AgentInstanceID: "instance-one"}
			admission, err := core.Submit(context.Background(), test.command(base, auth))
			if err != nil || !admission.Accepted {
				t.Fatalf("Submit = %#v, %v", admission, err)
			}
			completion, err := core.NextCompletion(context.Background())
			if err != nil || !completion.OK {
				t.Fatalf("NextCompletion = %#v, %v", completion, err)
			}
			result, ok := completion.Result.(v1.AuthResult)
			if !ok || result.AgentID != auth.Username || result.MeshID != auth.MeshID || result.AgentInstanceID != auth.AgentInstanceID || result.Personality != test.personality {
				t.Fatalf("auth result = %#v", completion.Result)
			}
			status, err := core.Status(context.Background())
			if err != nil || status.Lifecycle != v1.LifecycleReady || status.AgentID != auth.Username || status.MeshID != auth.MeshID || status.Personality != test.personality {
				t.Fatalf("Status = %#v, %v", status, err)
			}

			// Login publishes only the local identity. The first outbound send
			// performs its own server-authorized peer handshake.
			session.mu.Lock()
			if session.syncCalls != 0 || session.peerCalls != 0 {
				t.Fatalf("login queried group/peer before first contact: sync=%d peer=%d", session.syncCalls, session.peerCalls)
			}
			session.mu.Unlock()
			admission, err = core.Submit(context.Background(), v1.MeshListCommand{CommandBase: v1.CommandBase{CommandID: v1.CommandID("mesh-list-" + test.name), SDKSessionID: base.SDKSessionID}})
			if err != nil || !admission.Accepted {
				t.Fatalf("mesh list admission = %#v, %v", admission, err)
			}
			completion, err = core.NextCompletion(context.Background())
			meshResult, ok := completion.Result.(v1.MeshListResult)
			if err != nil || !completion.OK || !ok || len(meshResult.Meshes) != 1 || meshResult.Meshes[0].MeshID != v1.MeshID(auth.MeshID) || !meshResult.Meshes[0].Active {
				t.Fatalf("mesh list = %#v, %v", completion, err)
			}

			policyCommand := v1.PolicySetCommand{
				CommandBase: v1.CommandBase{CommandID: v1.CommandID("policy-" + test.name), SDKSessionID: base.SDKSessionID},
				Rules:       []v1.PolicyRule{{Action: v1.PolicyActionAllow, Path: "/messages", AgentID: "peer@example.test"}},
			}
			admission, err = core.Submit(context.Background(), policyCommand)
			if err != nil || !admission.Accepted {
				t.Fatalf("policy admission = %#v, %v", admission, err)
			}
			completion, err = core.NextCompletion(context.Background())
			if err != nil || !completion.OK {
				t.Fatalf("policy completion = %#v, %v", completion, err)
			}

			admission, err = core.Submit(context.Background(), v1.MessageSendCommand{
				CommandBase: v1.CommandBase{CommandID: v1.CommandID("send-" + test.name), SDKSessionID: base.SDKSessionID},
				To:          "peer@example.test",
				Payload:     v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: "/messages", Body: []byte("hello")}},
			})
			if err != nil || !admission.Accepted {
				t.Fatalf("message admission = %#v, %v", admission, err)
			}
			completion, err = core.NextCompletion(context.Background())
			sendResult, ok := completion.Result.(v1.SendResult)
			if err != nil || !completion.OK || !ok || !sendResult.Accepted || sendResult.MessageID == "" || sendResult.ConversationID == "" {
				session.mu.Lock()
				syncCalls, peerCalls := session.syncCalls, session.peerCalls
				session.mu.Unlock()
				t.Fatalf("message completion = %#v error=%+v, %v sync=%d peer=%d", completion, completion.Error, err, syncCalls, peerCalls)
			}
			session.mu.Lock()
			// Rank1 establishment may race this first Rank2 send and performs
			// its own fresh server handshake by contract.
			if session.syncCalls != 0 || session.peerCalls < 1 || session.peerCalls > 2 {
				t.Fatalf("first send handshake count: sync=%d peer=%d", session.syncCalls, session.peerCalls)
			}
			session.mu.Unlock()
			shutdownAndDrain(t, core)
			if err := core.Destroy(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newTestCoreWithConnectivity(t *testing.T, connectivity sessionkernel.ConnectivityFactory) *Core {
	t.Helper()
	boundary, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	core, err := newCoreWithConnectivity(Config{QueueLimit: 4, PayloadLimit: 1 << 20}, boundary, connectivity)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return core
}

func unavailableBuiltInFactory() *builtInConnectivityFactory {
	return newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		return builtInTestDialer{err: rank2xmpp.ErrUnavailable}, nil
	})
}

func TestBuiltInConnectivityClassifiesClosedProviderErrors(t *testing.T) {
	for _, test := range []struct {
		err  error
		code sessionkernel.ProviderErrorCode
	}{
		{context.Canceled, sessionkernel.ProviderCancelled},
		{context.DeadlineExceeded, sessionkernel.ProviderDeadline},
		{rank2xmpp.ErrAuthentication, sessionkernel.ProviderAuthenticationRejected},
		{rank2xmpp.ErrIdentityBinding, sessionkernel.ProviderAuthenticationRejected},
		{rank2xmpp.ErrQueueFull, sessionkernel.ProviderCapacity},
		{rank2xmpp.ErrUnavailable, sessionkernel.ProviderUnavailable},
		{errors.New("private"), sessionkernel.ProviderInternal},
	} {
		if failure := classifyConnectivityError(nil, test.err); failure == nil || failure.Code != test.code {
			t.Errorf("classify(%v) = %#v, want %v", test.err, failure, test.code)
		}
	}
}

func TestBuiltInConnectivityFailedStartRetiresCredentialBearingClient(t *testing.T) {
	factory := unavailableBuiltInFactory()
	profile, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	service, failure := factory.Create(context.Background(), sessionkernel.BuildContext{
		Auth:    sessionkernel.Authentication{MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test", Password: []byte("secret"), MeshID: "mesh-one"},
		Profile: profile, Clock: testConnectivityClock{}, Random: zeroConnectivityReader{},
	})
	if failure != nil || service == nil {
		t.Fatalf("Create = %#v, %#v", service, failure)
	}
	if failure = service.Start(context.Background()); failure == nil || failure.Code != sessionkernel.ProviderUnavailable {
		t.Fatalf("Start failure = %#v", failure)
	}
	concrete := service.(*builtInConnectivity)
	if observation := concrete.client.Observe(); observation.State != transport.HealthClosed {
		t.Fatalf("failed client health = %v, want closed", observation.State)
	}
	if _, err := concrete.client.AuthenticatedIdentity(); !errors.Is(err, rank2xmpp.ErrUnavailable) {
		t.Fatalf("failed client identity = %v", err)
	}
}

func TestBuiltInConnectivityUsesOneCalibratedClockAndConcreteOperations(t *testing.T) {
	factory := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		return builtInTestDialer{session: newBuiltInTestSession()}, nil
	})
	profile, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	created, failure := factory.Create(context.Background(), sessionkernel.BuildContext{
		Auth: sessionkernel.Authentication{
			MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test",
			Password: []byte("secret"), MeshID: "mesh-one",
		},
		Profile: profile, Clock: testConnectivityClock{}, Random: zeroConnectivityReader{},
	})
	if failure != nil || created == nil {
		t.Fatalf("Create = %#v, %#v", created, failure)
	}
	service := created.(*builtInConnectivity)
	if service.clock.source == nil || service.clock.source != service.client.Clock() {
		t.Fatal("durable client and messaging adapter do not share one calibrated clock")
	}
	if reading := service.clock.Snapshot(); !reading.UTC.IsZero() || reading.Uncertainty != 0 {
		t.Fatalf("uncalibrated reading = %#v", reading)
	}
	if failure = service.Start(context.Background()); failure != nil {
		t.Fatalf("Start = %#v", failure)
	}
	reading := service.clock.Snapshot()
	transportReading, ok := service.client.TimeCalibration()
	elapsed := transportReading.UTC.Sub(reading.UTC)
	if !ok || elapsed < 0 || elapsed > time.Second || reading.Uncertainty != transportReading.Uncertainty {
		t.Fatalf("shared reading = %#v, transport=%#v, ok=%t", reading, transportReading, ok)
	}
	operations := service.Operations()
	if operations.Logout == nil || operations.DiagnosticsNetwork == nil {
		t.Fatalf("concrete operations missing: %#v", operations)
	}
	if operations.MessageSend != nil || operations.DiagnosticsPeer != nil {
		t.Fatal("uncomposed application operation was exposed")
	}
	status, failure := operations.DiagnosticsNetwork(context.Background(), nil, model.EmptyArgs{})
	if failure != nil || status.State != "available" {
		t.Fatalf("healthy diagnostics = %#v, %#v", status, failure)
	}
	if _, failure = operations.Logout(context.Background(), nil, model.EmptyArgs{}); failure != nil {
		t.Fatalf("Logout = %#v", failure)
	}
	status, failure = operations.DiagnosticsNetwork(context.Background(), nil, model.EmptyArgs{})
	if failure != nil || status.State != "unavailable" {
		t.Fatalf("closed diagnostics = %#v, %#v", status, failure)
	}
}

func TestBuiltInConnectivityDiagnosticsAreApplicationNormalized(t *testing.T) {
	for _, test := range []struct {
		input transport.HealthState
		want  string
	}{
		{transport.HealthUnknown, "unknown"},
		{transport.HealthConnecting, "degraded"},
		{transport.HealthHealthy, "available"},
		{transport.HealthDisconnected, "degraded"},
		{transport.HealthFailed, "unavailable"},
		{transport.HealthClosed, "unavailable"},
		{transport.HealthState(255), "unknown"},
	} {
		if got := applicationConnectivityState(test.input); got != test.want {
			t.Errorf("state %d = %q, want %q", test.input, got, test.want)
		}
	}
}

type testConnectivityClock struct{}

func (testConnectivityClock) Now() time.Time { return time.Unix(1, 0) }
func (testConnectivityClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}
func (testConnectivityClock) NewTimer(duration time.Duration) coreclock.Timer {
	return coreclock.Real{}.NewTimer(duration)
}

type zeroConnectivityReader struct{}

func (zeroConnectivityReader) Read(target []byte) (int, error) {
	clear(target)
	return len(target), nil
}
