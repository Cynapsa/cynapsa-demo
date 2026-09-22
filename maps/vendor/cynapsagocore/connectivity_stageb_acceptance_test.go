package cynapsagocore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type qaStageBEndpointDialer struct {
	session rank2xmpp.Session
	err     error
}

func (dialer qaStageBEndpointDialer) Dial(context.Context) (rank2xmpp.Session, error) {
	return dialer.session, dialer.err
}

type qaStageBEndpointSession struct {
	mu       sync.Mutex
	failAt   string
	username string
	phases   []string
	group    rank2xmpp.AuthoritySnapshot
	groupErr error
	sent     []rank2xmpp.Stanza
	events   chan rank2xmpp.Event
	closed   chan struct{}
	once     sync.Once
}

func newQAStageBEndpointSession(failAt string) *qaStageBEndpointSession {
	return &qaStageBEndpointSession{
		failAt: failAt,
		group: rank2xmpp.AuthoritySnapshot{Members: []string{
			"agent@example.test/mesh-one", "peer@example.test/mesh-one",
		}},
		events: make(chan rank2xmpp.Event, 8),
		closed: make(chan struct{}),
	}
}

func (session *qaStageBEndpointSession) phase(name string) error {
	session.mu.Lock()
	session.phases = append(session.phases, name)
	fail := session.failAt == name
	session.mu.Unlock()
	if !fail {
		return nil
	}
	switch name {
	case "sasl":
		return rank2xmpp.ErrAuthentication
	case "bind":
		return rank2xmpp.ErrIdentityBinding
	case "sm":
		return rank2xmpp.ErrStreamManagement
	default:
		return rank2xmpp.ErrUnavailable
	}
}

func (session *qaStageBEndpointSession) ConnectTLS(_ context.Context, endpoint string) error {
	if endpoint != "mesh.example.test:5222" {
		return rank2xmpp.ErrInvalidConfig
	}
	return session.phase("tls")
}

func (session *qaStageBEndpointSession) Authenticate(_ context.Context, username string, password []byte) (string, []byte, error) {
	if username != "agent@example.test" || string(password) != "secret" {
		return "", nil, rank2xmpp.ErrAuthentication
	}
	if err := session.phase("sasl"); err != nil {
		return "", nil, err
	}
	session.mu.Lock()
	session.username = username
	session.mu.Unlock()
	return username, nil, nil
}

func (session *qaStageBEndpointSession) BindResource(_ context.Context, resource string) (string, error) {
	if err := session.phase("bind"); err != nil {
		return "", err
	}
	session.mu.Lock()
	username := session.username
	session.mu.Unlock()
	if username == "" || resource != "mesh-one" {
		return "", rank2xmpp.ErrIdentityBinding
	}
	return username + "/" + resource, nil
}

func (session *qaStageBEndpointSession) EnableStreamManagement(_ context.Context, resumable bool) error {
	if !resumable {
		return rank2xmpp.ErrStreamManagement
	}
	return session.phase("sm")
}

func (session *qaStageBEndpointSession) DiscoverAuthority(context.Context) error {
	return session.phase("discovery")
}

func (session *qaStageBEndpointSession) QueryServerTime(context.Context) (time.Time, error) {
	if err := session.phase("time"); err != nil {
		return time.Time{}, err
	}
	return time.Date(2026, 8, 13, 12, 0, 0, 123456000, time.UTC), nil
}

func (session *qaStageBEndpointSession) SyncAuthority(context.Context) (rank2xmpp.AuthoritySnapshot, error) {
	if err := session.phase("topology"); err != nil {
		return rank2xmpp.AuthoritySnapshot{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.groupErr != nil {
		return rank2xmpp.AuthoritySnapshot{}, session.groupErr
	}
	snapshot := session.group
	snapshot.Members = append([]string(nil), snapshot.Members...)
	return snapshot, nil
}

func (session *qaStageBEndpointSession) Send(_ context.Context, stanza rank2xmpp.Stanza) error {
	stanza.Data = append([]byte(nil), stanza.Data...)
	session.mu.Lock()
	session.sent = append(session.sent, stanza)
	session.mu.Unlock()
	return nil
}

func (session *qaStageBEndpointSession) Receive(ctx context.Context) (rank2xmpp.Event, error) {
	select {
	case <-ctx.Done():
		return rank2xmpp.Event{}, ctx.Err()
	case event := <-session.events:
		event.Stanza.Data = append([]byte(nil), event.Stanza.Data...)
		return event, nil
	case <-session.closed:
		return rank2xmpp.Event{}, rank2xmpp.ErrClosed
	}
}

func (*qaStageBEndpointSession) Resume(context.Context) (bool, error) { return false, nil }
func (*qaStageBEndpointSession) CatchUp(context.Context, int) ([]rank2xmpp.Stanza, error) {
	return nil, nil
}
func (session *qaStageBEndpointSession) Close(context.Context) error {
	session.once.Do(func() { close(session.closed) })
	return nil
}

func (session *qaStageBEndpointSession) observedPhases() []string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return append([]string(nil), session.phases...)
}

func qaStageBAuthCommand(name v1.CommandName, id string) v1.Command {
	base := v1.CommandBase{CommandID: v1.CommandID(id), SDKSessionID: "qa-session"}
	auth := v1.AuthInput{
		MeshEndpoint:    "mesh.example.test:5222",
		Username:        "agent@example.test",
		Password:        "secret",
		MeshID:          "mesh-one",
		AgentInstanceID: "instance-one",
	}
	if name == v1.CommandAuthLogin {
		return v1.AuthLoginCommand{CommandBase: base, Auth: auth}
	}
	return v1.AuthConnectCommand{CommandBase: base, Auth: auth}
}

func qaStageBCore(t *testing.T, dialer rank2xmpp.Dialer) *Core {
	t.Helper()
	boundary, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	factory := newBuiltInConnectivityFactory(func(endpoint string, _ sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		if endpoint != "mesh.example.test:5222" {
			return nil, rank2xmpp.ErrInvalidConfig
		}
		return dialer, nil
	})
	core, err := newCoreWithConnectivity(Config{QueueLimit: 4, PayloadLimit: 1 << 20}, boundary, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return core
}

func qaStageBCloseCore(t *testing.T, core *Core) {
	t.Helper()
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
}

func TestStageBAcceptanceBuiltInConnectivityReachesBothAuthenticationPersonalities(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        v1.CommandName
		personality v1.SDKPersonality
	}{
		{name: v1.CommandAuthLogin, personality: v1.SDKPersonalityHTTPBridge},
		{name: v1.CommandAuthConnect, personality: v1.SDKPersonalityNative},
	}
	for _, test := range tests {
		t.Run(string(test.name), func(t *testing.T) {
			session := newQAStageBEndpointSession("")
			core := qaStageBCore(t, qaStageBEndpointDialer{session: session})
			admission, err := core.Submit(context.Background(), qaStageBAuthCommand(test.name, "qa-positive-"+string(test.name)))
			if err != nil || !admission.Accepted {
				t.Fatalf("Submit = %#v, %v", admission, err)
			}
			completion, err := core.NextCompletion(context.Background())
			if err != nil || !completion.OK || completion.Error != nil {
				t.Fatalf("NextCompletion = %#v, %v", completion, err)
			}
			result, ok := completion.Result.(v1.AuthResult)
			if !ok || result.AgentID != "agent@example.test" || result.MeshID != "mesh-one" || result.AgentInstanceID != "instance-one" || result.Personality != test.personality {
				t.Fatalf("auth result = %#v", completion.Result)
			}
			status, err := core.Status(context.Background())
			if err != nil || status.Lifecycle != v1.LifecycleReady || status.Connectivity != v1.ConnectivityAvailable || status.Personality != test.personality || status.AgentID != "agent@example.test" || status.MeshID != "mesh-one" || status.MeshEndpoint != "mesh.example.test:5222" {
				t.Fatalf("Status = %#v, %v", status, err)
			}
			if phases := session.observedPhases(); !reflect.DeepEqual(phases, []string{"tls", "sasl", "bind", "sm", "discovery", "time", "topology"}) {
				t.Fatalf("establishment phases = %v", phases)
			}
			qaStageBCloseCore(t, core)
		})
	}
}

func TestStageBAcceptanceBuiltInConnectivityClassifiesEveryEstablishmentFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		phase      string
		wantCode   v1.ErrorCode
		wantStage  v1.ErrorStage
		retryable  bool
		wantPhases []string
	}{
		{phase: "network", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true},
		{phase: "tls", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true, wantPhases: []string{"tls"}},
		{phase: "sasl", wantCode: v1.ErrorCodeAuthenticationFailed, wantStage: v1.ErrorStageAuth, wantPhases: []string{"tls", "sasl"}},
		{phase: "bind", wantCode: v1.ErrorCodeAuthenticationFailed, wantStage: v1.ErrorStageAuth, wantPhases: []string{"tls", "sasl", "bind"}},
		{phase: "sm", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true, wantPhases: []string{"tls", "sasl", "bind", "sm"}},
		{phase: "discovery", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true, wantPhases: []string{"tls", "sasl", "bind", "sm", "discovery"}},
		{phase: "time", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true, wantPhases: []string{"tls", "sasl", "bind", "sm", "discovery", "time"}},
		{phase: "topology", wantCode: v1.ErrorCodeConnectivityUnavailable, wantStage: v1.ErrorStageAuth, retryable: true, wantPhases: []string{"tls", "sasl", "bind", "sm", "discovery", "time", "topology"}},
	}
	for _, commandName := range []v1.CommandName{v1.CommandAuthLogin, v1.CommandAuthConnect} {
		for _, test := range tests {
			t.Run(string(commandName)+"/"+test.phase, func(t *testing.T) {
				session := newQAStageBEndpointSession(test.phase)
				dialer := qaStageBEndpointDialer{session: session}
				if test.phase == "network" {
					dialer = qaStageBEndpointDialer{err: rank2xmpp.ErrUnavailable}
				}
				core := qaStageBCore(t, dialer)
				admission, err := core.Submit(context.Background(), qaStageBAuthCommand(commandName, "qa-failure-"+string(commandName)+"-"+test.phase))
				if err != nil || !admission.Accepted {
					t.Fatalf("Submit = %#v, %v", admission, err)
				}
				completion, err := core.NextCompletion(context.Background())
				if err != nil || completion.OK || completion.Result != nil || completion.Error == nil {
					t.Fatalf("NextCompletion = %#v, %v", completion, err)
				}
				if completion.Error.Code != test.wantCode || completion.Error.Stage != test.wantStage || completion.Error.Retryable != test.retryable {
					t.Fatalf("failure = %#v, want code=%q stage=%q retryable=%t", completion.Error, test.wantCode, test.wantStage, test.retryable)
				}
				if errors.Is(completion.Error, rank2xmpp.ErrUnavailable) || errors.Is(completion.Error, rank2xmpp.ErrAuthentication) || errors.Is(completion.Error, rank2xmpp.ErrIdentityBinding) || errors.Is(completion.Error, rank2xmpp.ErrStreamManagement) {
					t.Fatalf("private dependency error escaped: %#v", completion.Error)
				}
				if phases := session.observedPhases(); !reflect.DeepEqual(phases, test.wantPhases) {
					t.Fatalf("failure phases = %v, want %v", phases, test.wantPhases)
				}
				status, statusErr := core.Status(context.Background())
				if statusErr != nil || status.Lifecycle != v1.LifecycleCreated || status.Personality != v1.SDKPersonalityUnset || status.AgentID != "" || status.MeshID != "" {
					t.Fatalf("failed authentication retained state: %#v, %v", status, statusErr)
				}
				qaStageBCloseCore(t, core)
			})
		}
	}
}
