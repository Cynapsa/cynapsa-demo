package sessionkernel_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

func TestStageBProfileFreezesApprovedDefaultsSecurityAndBounds(t *testing.T) {
	profile, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: 7})
	if err != nil {
		t.Fatal(err)
	}
	if profile.ReconnectAttempts != 8 || profile.ReconnectInitial != 250*time.Millisecond || profile.ReconnectMaximum != 30*time.Second || profile.ReconnectOperationTimeout != 30*time.Second || profile.StanzaBudgetBytes != 4<<20 || profile.TransferWorkers != 4 || profile.QueueCapacity != 7 || !profile.UseSystemTLSRoots || !profile.VerifyEndpointHostname {
		t.Fatalf("operational profile = %+v", profile)
	}
	for _, invalid := range []int{0, -1, sessionkernel.MaximumQueueCapacity + 1} {
		if _, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: invalid}); !errors.Is(err, sessionkernel.ErrInvalidConfig) {
			t.Errorf("queue_limit=%d error = %v", invalid, err)
		}
	}
	wantBackoff := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
	for index, want := range wantBackoff {
		got, err := profile.BackoffCeiling(index + 1)
		if err != nil || got != want {
			t.Errorf("BackoffCeiling(%d) = (%v, %v), want %v", index+1, got, err, want)
		}
	}
	for _, invalid := range []int{0, 9} {
		if _, err := profile.BackoffCeiling(invalid); !errors.Is(err, sessionkernel.ErrInvalidConfig) {
			t.Errorf("BackoffCeiling(%d) error = %v", invalid, err)
		}
	}

	mechanisms := profile.SASLMechanisms()
	wantMechanisms := []string{"SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"}
	gotMechanisms := make([]string, len(mechanisms))
	for i := range mechanisms {
		gotMechanisms[i] = mechanisms[i].Name
	}
	if !reflect.DeepEqual(gotMechanisms, wantMechanisms) {
		t.Fatalf("SASL mechanisms = %v, want %v", gotMechanisms, wantMechanisms)
	}
	for _, forbidden := range []string{"PLAIN", "LOGIN", "ANONYMOUS"} {
		if strings.Contains(strings.Join(gotMechanisms, ","), forbidden) {
			t.Fatalf("plaintext or unauthenticated SASL mechanism enabled: %s", forbidden)
		}
	}
	tlsConfig, err := profile.TLSConfig("mesh.example.test")
	if err != nil || tlsConfig.RootCAs == nil || tlsConfig.ServerName != "mesh.example.test" || tlsConfig.InsecureSkipVerify {
		t.Fatalf("TLSConfig() = (%+v, %v)", tlsConfig, err)
	}
	if _, err := profile.TLSConfig(""); !errors.Is(err, sessionkernel.ErrInvalidConfig) {
		t.Fatalf("empty TLS hostname error = %v", err)
	}

	now := time.Unix(1000, 0)
	deadline, err := profile.OperationDeadline(context.Background(), now)
	if err != nil || !deadline.Equal(now.Add(30*time.Second)) {
		t.Fatalf("default deadline = (%v, %v)", deadline, err)
	}
	caller, cancel := context.WithDeadline(context.Background(), now.Add(2*time.Second))
	defer cancel()
	deadline, err = profile.OperationDeadline(caller, now)
	if err != nil || !deadline.Equal(now.Add(2*time.Second)) {
		t.Fatalf("caller deadline = (%v, %v)", deadline, err)
	}

	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], 10)
	if got, err := sessionkernel.CryptoJitter(bytes.NewReader(encoded[:]), 10); err != nil || got != 10 {
		t.Fatalf("CryptoJitter upper edge = (%v, %v)", got, err)
	}
	if got, err := sessionkernel.CryptoJitter(bytes.NewReader(make([]byte, 8)), 10); err != nil || got != 0 {
		t.Fatalf("CryptoJitter lower edge = (%v, %v)", got, err)
	}
}

func TestStageBAuthenticationBindsExactIdentityClearsPasswordAndNeverReturnsBoundIdentity(t *testing.T) {
	var captured sessionkernel.BuildContext
	service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{create: func(_ context.Context, build sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
		captured = build
		return service, nil
	}}})
	result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
	if err != nil || result.Err != nil {
		t.Fatalf("authentication = (%+v, %v)", result, err)
	}
	if captured.Auth.Username != "agent@example.test" || captured.Auth.MeshID != "mesh-one" || captured.Auth.MeshEndpoint != "mesh.example.test:5222" {
		t.Fatalf("provider auth projection = %+v", captured.Auth)
	}
	for index, value := range captured.Auth.Password {
		if value != 0 {
			t.Fatalf("provider password copy byte %d retained value %d", index, value)
		}
	}
	value, ok := result.Value.(model.AuthResult)
	if !ok || value.AgentID != "agent@example.test" || value.MeshID != "mesh-one" || value.Personality != string(sessionkernel.PersonalityNative) {
		t.Fatalf("AuthResult = %+v", result)
	}
	privateIdentity := "agent@example.test/mesh-one"
	if strings.Contains(fmt.Sprintf("%+v", result), privateIdentity) || strings.Contains(fmt.Sprintf("%+v", controller.Snapshot()), privateIdentity) {
		t.Fatalf("private bound identity escaped result or snapshot: result=%+v snapshot=%+v", result, controller.Snapshot())
	}
	agentResult, _ := dependencies.Handlers["auth.agent_id"](context.Background(), qaServices{}, model.Command{ID: "agent", Name: "auth.agent_id", Args: model.EmptyArgs{}})
	if strings.Contains(fmt.Sprintf("%+v", agentResult), privateIdentity) {
		t.Fatalf("private bound identity escaped auth.agent_id: %+v", agentResult)
	}
}

func TestStageBAuthenticationRejectsEveryBareMeshAndBoundIdentityMismatch(t *testing.T) {
	identities := []sessionkernel.AuthenticatedIdentity{
		{AgentID: "other@example.test", BoundIdentity: "other@example.test/mesh-one", MeshID: "mesh-one"},
		{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-two", MeshID: "mesh-two"},
		{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-two", MeshID: "mesh-one"},
		{AgentID: "agent@example.test/mesh-one", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"},
		{AgentID: "agent@example.test", BoundIdentity: "agent@example.test", MeshID: "mesh-one"},
	}
	for _, identity := range identities {
		t.Run(fmt.Sprintf("%q", identity), func(t *testing.T) {
			service := &qaConnectivity{identity: identity}
			controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
			result, err := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
			if err != nil || result.Err == nil || result.Err.Code != "authentication_failed" || result.Value != nil {
				t.Fatalf("identity %+v = (%+v, %v)", identity, result, err)
			}
			if controller.Snapshot().Authenticated || service.stopCount() != 1 {
				t.Fatalf("rejected identity retained graph: snapshot=%+v stops=%d", controller.Snapshot(), service.stopCount())
			}
		})
	}
}

func TestStageBOnePersonalityWinsConcurrentLoginConnectRace(t *testing.T) {
	var creates atomic.Int64
	service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{create: func(context.Context, sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
		creates.Add(1)
		return service, nil
	}}})
	const contenders = 64
	start := make(chan struct{})
	results := make(chan model.Result, contenders)
	var group sync.WaitGroup
	for index := 0; index < contenders; index++ {
		name := "auth.connect"
		if index%2 == 0 {
			name = "auth.login"
		}
		group.Add(1)
		go func(name string) {
			defer group.Done()
			<-start
			result, _ := dependencies.Handlers[name](context.Background(), qaServices{}, qaAuthCommand(name))
			results <- result
		}(name)
	}
	close(start)
	group.Wait()
	close(results)
	successes := 0
	winner := sessionkernel.PersonalityUnset
	for result := range results {
		if result.Err == nil {
			successes++
			winner = sessionkernel.Personality(result.Value.(model.AuthResult).Personality)
		}
	}
	if successes != 1 || creates.Load() != 1 {
		t.Fatalf("race successes=%d provider creates=%d, want exactly one", successes, creates.Load())
	}
	if snapshot := controller.Snapshot(); !snapshot.Authenticated || snapshot.Personality != winner {
		t.Fatalf("winner=%q snapshot=%+v", winner, snapshot)
	}
}

func TestStageBGraphRollbackAndShutdownAreReverseOrderedAndPanicContained(t *testing.T) {
	t.Run("commit_failure_reverse_rollback", func(t *testing.T) {
		trace := &qaTrace{}
		connectivity := &qaConnectivity{
			identity: sessionkernel.AuthenticatedIdentity{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"},
			onStart:  func() { trace.add("start connectivity") }, onStop: func() { trace.add("stop connectivity") },
		}
		objects := &qaObjectService{onStart: func() { trace.add("start objects") }, onStop: func() { trace.add("stop objects") }}
		_, dependencies := qaFactory(t, sessionkernel.Dependencies{
			Connectivity: qaConnectivityFactory{service: connectivity}, IdentityKeys: qaIdentityKeys{}, Objects: &qaObjectFactory{service: objects},
		})
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), qaServices{commit: func(context.Context, coreruntime.AuthenticatedPublisher) error {
			return errors.New("private commit rejection")
		}}, qaAuthCommand("auth.connect"))
		if result.Err == nil {
			t.Fatalf("commit failure result = %+v", result)
		}
		if got := trace.String(); got != "[start connectivity start objects stop objects stop connectivity]" {
			t.Fatalf("rollback order = %s", got)
		}
	})

	t.Run("shutdown_panic_contained_exactly_once", func(t *testing.T) {
		service := &qaConnectivity{stopPanic: true, identity: sessionkernel.AuthenticatedIdentity{
			AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
		}}
		controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
		result, _ := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
		if result.Err != nil {
			t.Fatalf("authentication = %+v", result)
		}
		if err := controller.Shutdown(context.Background()); !errors.Is(err, sessionkernel.ErrComponentStopFailed) {
			t.Fatalf("Shutdown() error = %v", err)
		}
		if got := service.stopCount(); got != 1 {
			t.Fatalf("panicking Shutdown calls = %d, want 1", got)
		}
	})
}

func TestStageBObjectProviderGatesOffloadIndependentlyOfOptionalKeys(t *testing.T) {
	for _, test := range []struct {
		name        string
		withKeys    bool
		withObjects bool
		wantCalls   int
		wantOffload bool
	}{
		{"neither", false, false, 0, false},
		{"objects_without_keys", false, true, 1, true},
		{"keys_without_objects", true, false, 0, false},
		{"both", true, true, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
				AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
			}}
			objectFactory := &qaObjectFactory{service: &qaObjectService{}}
			dependencies := sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}}
			if test.withKeys {
				dependencies.IdentityKeys = qaIdentityKeys{}
			}
			if test.withObjects {
				dependencies.Objects = objectFactory
			}
			controller, handlers := qaFactory(t, dependencies)
			result, err := handlers.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
			if err != nil || result.Err != nil {
				t.Fatalf("authentication = (%+v, %v)", result, err)
			}
			if calls := objectFactory.callCount(); calls != test.wantCalls {
				t.Fatalf("object factory calls = %d, want %d", calls, test.wantCalls)
			}
			if offload := controller.Snapshot().OffloadAvailable; offload != test.wantOffload {
				t.Fatalf("OffloadAvailable = %v, want %v", offload, test.wantOffload)
			}
		})
	}
}

func TestStageBNoncooperativeFactoryAndCleanupRespectCallerBounds(t *testing.T) {
	t.Run("factory", func(t *testing.T) {
		release := make(chan struct{})
		entered := make(chan struct{})
		stopped := make(chan struct{})
		var stopOnce sync.Once
		service := &qaConnectivity{
			identity: sessionkernel.AuthenticatedIdentity{AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one"},
			onStop:   func() { stopOnce.Do(func() { close(stopped) }) },
		}
		_, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{create: func(context.Context, sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
			close(entered)
			<-release
			return service, nil
		}}})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		started := time.Now()
		result, err := dependencies.Handlers["auth.connect"](ctx, qaServices{}, qaAuthCommand("auth.connect"))
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("noncooperative factory held caller for %v", elapsed)
		}
		if err != nil || result.Err == nil || result.Err.Code != "connectivity_unavailable" || !result.Err.Retryable {
			t.Fatalf("noncooperative factory result = (%+v, %v)", result, err)
		}
		<-entered
		close(release)
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("late provider service was not cleaned")
		}
	})

	t.Run("unpublished_cleanup", func(t *testing.T) {
		release := make(chan struct{})
		service := &qaConnectivity{stopBlock: release}
		_, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{
			service: service, failure: &sessionkernel.ProviderError{Code: sessionkernel.ProviderUnavailable},
		}})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		started := time.Now()
		result, err := dependencies.Handlers["auth.connect"](ctx, qaServices{}, qaAuthCommand("auth.connect"))
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("noncooperative cleanup held caller for %v", elapsed)
		}
		if err != nil || result.Err == nil || result.Err.Code != "connectivity_unavailable" || !result.Err.Retryable || service.stopCount() != 1 {
			t.Fatalf("noncooperative cleanup result = (%+v, %v), stops=%d", result, err, service.stopCount())
		}
		close(release)
	})
}

func TestStageBConcurrentShutdownJoinsOneCleanupWithoutResourceDuplication(t *testing.T) {
	service := &qaConnectivity{identity: sessionkernel.AuthenticatedIdentity{
		AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
	}}
	controller, dependencies := qaFactory(t, sessionkernel.Dependencies{Connectivity: qaConnectivityFactory{service: service}})
	result, _ := dependencies.Handlers["auth.connect"](context.Background(), qaServices{}, qaAuthCommand("auth.connect"))
	if result.Err != nil {
		t.Fatalf("authentication = %+v", result)
	}
	const callers = 64
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsSeen <- controller.Shutdown(context.Background())
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("concurrent Shutdown() error = %v", err)
		}
	}
	if got := service.stopCount(); got != 1 {
		t.Fatalf("service Shutdown calls = %d, want 1", got)
	}
}
