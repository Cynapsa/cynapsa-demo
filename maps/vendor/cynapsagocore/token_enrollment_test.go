package cynapsagocore

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

const (
	enrollmentPublicToken = "cpsa_e1.01234567-89ab-4def-8123-456789abcdef.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	enrollmentJWT         = "e30.eyJleHAiOjQxMDI0NDQ4MDB9.sig"
	enrollmentWrapper     = "aztm_private-wrapper"
)

type captureEnrollment struct {
	mu      sync.Mutex
	request enrollment.Request
	block   bool
	entered chan struct{}
}

type enrollmentOnlyConnectivity struct {
	delegate sessionkernel.ConnectivityFactory
}

func (factory enrollmentOnlyConnectivity) Create(ctx context.Context, build sessionkernel.BuildContext) (sessionkernel.ConnectivityService, *sessionkernel.ProviderError) {
	return factory.delegate.Create(ctx, build)
}

func (provider *captureEnrollment) Enroll(ctx context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	provider.request = enrollment.Request{
		Token: append([]byte(nil), request.Token...), MeshID: request.MeshID,
		InstallationID:     request.InstallationID,
		InstallationSecret: append([]byte(nil), request.InstallationSecret...),
	}
	provider.mu.Unlock()
	if provider.entered != nil {
		close(provider.entered)
	}
	if provider.block {
		<-ctx.Done()
		return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureCancelled}
	}
	return enrollment.Bundle{
		Version: "e1", InstallationID: request.InstallationID,
		AgentID: "22222222-2222-4222-a222-222222222222", MeshID: request.MeshID,
		SessionResource: "installation-" + request.InstallationID,
		MeshEndpoint:    "mesh.example.test:5222", Username: "agent@example.test",
		Server: "mesh.example.test", AccessToken: []byte(enrollmentJWT),
		ExpiresIn: 86400, Wrapper: []byte(enrollmentWrapper), Display: "Friendly Agent",
	}, nil
}

func (provider *captureEnrollment) snapshot() enrollment.Request {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return enrollment.Request{
		Token: append([]byte(nil), provider.request.Token...), MeshID: provider.request.MeshID,
		InstallationID:     provider.request.InstallationID,
		InstallationSecret: append([]byte(nil), provider.request.InstallationSecret...),
	}
}

type enrollmentTokenSession struct {
	*builtInTestSession
	resource string
}

func (session *enrollmentTokenSession) Authenticate(ctx context.Context, username string, credential []byte) (string, []byte, error) {
	if string(credential) != enrollmentJWT {
		return "", nil, rank2xmpp.ErrAuthentication
	}
	return session.builtInTestSession.Authenticate(ctx, username, []byte("secret"))
}

func (session *enrollmentTokenSession) BindResource(_ context.Context, resource string) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.username == "" {
		return "", rank2xmpp.ErrIdentityBinding
	}
	session.resource = resource
	return session.username + "/" + resource, nil
}

func (session *enrollmentTokenSession) SyncAuthority(context.Context) (rank2xmpp.AuthoritySnapshot, error) {
	session.mu.Lock()
	resource := session.resource
	session.mu.Unlock()
	return rank2xmpp.AuthoritySnapshot{Members: []string{"agent@example.test/" + resource, "peer@example.test/" + resource}}, nil
}

type retryingEnrollment struct {
	mu            sync.Mutex
	failures      []enrollment.FailureCode
	requests      []enrollment.Request
	installations map[string][]byte
}

type durableEnrollmentProvider struct {
	mu          sync.Mutex
	enrollCalls int
	renewCalls  int
	unavailable bool
}

type v2EnrollmentProvider struct {
	mu          sync.Mutex
	enrollCalls int
	renewCalls  int
	unavailable bool
}

func (provider *v2EnrollmentProvider) Enroll(context.Context, enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureRejected}
}

func (provider *v2EnrollmentProvider) EnrollV2(_ context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	provider.enrollCalls++
	unavailable := provider.unavailable
	provider.mu.Unlock()
	if unavailable {
		return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureUnavailable}
	}
	return successfulV2EnrollmentBundle(request), nil
}

func (provider *v2EnrollmentProvider) RenewV2(_ context.Context, request enrollment.RenewRequest) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	provider.renewCalls++
	unavailable := provider.unavailable
	provider.mu.Unlock()
	if unavailable {
		return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureUnavailable}
	}
	return successfulV2EnrollmentBundle(enrollment.Request{MeshID: request.MeshID, InstallationID: request.InstallationID, InstallationSecret: request.InstallationSecret}), nil
}

func (provider *v2EnrollmentProvider) counts() (int, int) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.enrollCalls, provider.renewCalls
}

func (provider *durableEnrollmentProvider) Enroll(_ context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	provider.enrollCalls++
	unavailable := provider.unavailable
	provider.mu.Unlock()
	if unavailable {
		return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureUnavailable}
	}
	return successfulEnrollmentBundle(request), nil
}

func (provider *durableEnrollmentProvider) Renew(_ context.Context, request enrollment.RenewRequest) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	provider.renewCalls++
	unavailable := provider.unavailable
	provider.mu.Unlock()
	if unavailable {
		return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureUnavailable}
	}
	return successfulEnrollmentBundle(enrollment.Request{
		MeshID: request.MeshID, InstallationID: request.InstallationID,
		InstallationSecret: request.InstallationSecret,
	}), nil
}

func (provider *durableEnrollmentProvider) counts() (int, int) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.enrollCalls, provider.renewCalls
}

func (provider *retryingEnrollment) Enroll(_ context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.requests = append(provider.requests, enrollment.Request{
		Token: append([]byte(nil), request.Token...), MeshID: request.MeshID,
		InstallationID:     request.InstallationID,
		InstallationSecret: append([]byte(nil), request.InstallationSecret...),
	})
	if provider.installations == nil {
		provider.installations = make(map[string][]byte)
	}
	if committedSecret, exists := provider.installations[request.InstallationID]; exists {
		if !bytes.Equal(committedSecret, request.InstallationSecret) {
			return enrollment.Bundle{}, &enrollment.Failure{Code: enrollment.FailureInvalidResponse}
		}
	} else {
		provider.installations[request.InstallationID] = append([]byte(nil), request.InstallationSecret...)
	}
	if len(provider.failures) >= len(provider.requests) {
		return enrollment.Bundle{}, &enrollment.Failure{Code: provider.failures[len(provider.requests)-1]}
	}
	return successfulEnrollmentBundle(request), nil
}

func (provider *retryingEnrollment) clear() {
	provider.mu.Lock()
	for index := range provider.requests {
		provider.requests[index].Clear()
	}
	provider.requests = nil
	for identifier, secret := range provider.installations {
		clear(secret)
		delete(provider.installations, identifier)
	}
	provider.mu.Unlock()
}

func successfulEnrollmentBundle(request enrollment.Request) enrollment.Bundle {
	return enrollment.Bundle{
		Version: "e1", InstallationID: request.InstallationID,
		AgentID: "22222222-2222-4222-a222-222222222222", MeshID: request.MeshID,
		SessionResource: "installation-" + request.InstallationID,
		MeshEndpoint:    "mesh.example.test:5222", Username: "agent@example.test",
		Server: "mesh.example.test", AccessToken: []byte(enrollmentJWT),
		ExpiresIn: 86400, Wrapper: []byte(enrollmentWrapper), Display: "Friendly Agent",
	}
}

func successfulV2EnrollmentBundle(request enrollment.Request) enrollment.Bundle {
	return enrollment.Bundle{
		Version: "e2", OrganizationID: "33333333-3333-4333-a333-333333333333",
		InstallationID: request.InstallationID, AgentID: "22222222-2222-4222-a222-222222222222", AgentJID: "agent@example.test",
		MeshID: request.MeshID, SessionResource: "r2." + request.InstallationID + ".session",
		MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test", Server: "mesh.example.test",
		AccessToken: []byte(enrollmentJWT), ExpiresIn: 36 * 60 * 60, Wrapper: []byte(enrollmentWrapper), Display: "Friendly Agent",
		InstallationEpoch: 1, AttachmentRevision: 1, TokenID: "token-1", PolicyRevision: 1,
		SessionExpiryMode: "continue", OfflineColdStartTargetSeconds: 24 * 60 * 60,
	}
}

func TestV2ProfilesSupportSameTokenAcrossMeshesReplicasAndTokenFreeRestart(t *testing.T) {
	stateRoot := t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CYNAPSA_STATE_DIRECTORY", stateRoot)
	connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		return builtInTestDialer{session: &enrollmentTokenSession{builtInTestSession: newBuiltInTestSession()}}, nil
	})
	isolation := enrollmentOnlyConnectivity{delegate: connectivity}
	provider := &v2EnrollmentProvider{}
	first := submitProfileTokenLogin(t, newEnrollmentStateTestCore(t, isolation, provider, enrollment.NewProductionStateStore()), "replica-a", "mesh-one", "v2-a")
	if first.ProfileID != "replica-a" || first.SessionExpiryMode != "continue" || first.PolicyRevision != 1 ||
		first.OfflineColdStartTargetSeconds != 24*60*60 || !first.OfflineTargetSatisfied || first.PreparationStatus != "ready" ||
		first.CredentialExpiresAt.IsZero() || first.OfflineStartDeadline.IsZero() {
		t.Fatalf("readiness result=%#v", first)
	}
	second := submitProfileTokenLogin(t, newEnrollmentStateTestCore(t, isolation, provider, enrollment.NewProductionStateStore()), "replica-b", "mesh-one", "v2-b")
	if first.AgentInstanceID == second.AgentInstanceID {
		t.Fatal("two profiles reused one installation identity")
	}
	thirdCore := newEnrollmentStateTestCore(t, isolation, provider, enrollment.NewProductionStateStore())
	third := submitProfileTokenLogin(t, thirdCore, "replica-a", "mesh-two", "v2-c")
	if third.AgentInstanceID != first.AgentInstanceID {
		t.Fatal("one profile changed installation across meshes")
	}
	if enroll, renew := provider.counts(); enroll != 3 || renew != 0 {
		t.Fatalf("provider calls enroll=%d renew=%d", enroll, renew)
	}
	offline := &v2EnrollmentProvider{unavailable: true}
	restart := newEnrollmentStateTestCore(t, isolation, offline, enrollment.NewProductionStateStore())
	result := submitInstallationLogin(t, restart, "replica-a", "mesh-two", "v2-restart")
	if result.AgentInstanceID != first.AgentInstanceID {
		t.Fatal("token-free restart changed installation")
	}
	if enroll, renew := offline.counts(); enroll != 0 || renew != 0 {
		t.Fatalf("valid cache contacted provider: enroll=%d renew=%d", enroll, renew)
	}
	shutdownAndDrain(t, restart)
	_ = restart.Destroy()
}

func submitProfileTokenLogin(t *testing.T, core *Core, profileID, meshID, commandID string) v1.AuthResult {
	t.Helper()
	defer func() { shutdownAndDrain(t, core); _ = core.Destroy() }()
	admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{CommandBase: v1.CommandBase{CommandID: v1.CommandID(commandID), SDKSessionID: "profile-session"}, Auth: v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: v1.MeshID(meshID), ProfileID: profileID}})
	if err != nil || !admission.Accepted {
		t.Fatalf("admission=%#v err=%v", admission, err)
	}
	completion, err := core.NextCompletion(t.Context())
	result, ok := completion.Result.(v1.AuthResult)
	if err != nil || !completion.OK || !ok {
		t.Fatalf("completion=%#v public_error=%+v err=%v", completion, completion.Error, err)
	}
	return result
}

func submitInstallationLogin(t *testing.T, core *Core, profileID, meshID, commandID string) v1.AuthResult {
	t.Helper()
	admission, err := core.Submit(t.Context(), v1.AuthInstallationLoginCommand{CommandBase: v1.CommandBase{CommandID: v1.CommandID(commandID), SDKSessionID: "profile-session"}, Auth: v1.AuthInstallationInput{ProfileID: profileID, MeshID: v1.MeshID(meshID)}})
	if err != nil || !admission.Accepted {
		t.Fatalf("admission=%#v err=%v", admission, err)
	}
	completion, err := core.NextCompletion(t.Context())
	result, ok := completion.Result.(v1.AuthResult)
	if err != nil || !completion.OK || !ok {
		t.Fatalf("completion=%#v err=%v", completion, err)
	}
	return result
}

func TestTokenEnrollmentMapsPrivateBundleAndPublishesOnlyOpaqueIdentity(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "login", true: "connect"}[native], func(t *testing.T) {
			provider := &captureEnrollment{}
			var tokenMode bool
			connectivity := newBuiltInConnectivityFactory(func(endpoint string, profile sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
				tokenMode = profile.TokenAuthentication
				mechanisms := profile.SASLMechanisms()
				if endpoint != "mesh.example.test:5222" || len(mechanisms) != 1 || mechanisms[0].Name != "PLAIN" {
					t.Fatalf("token connectivity profile = %q %#v", endpoint, mechanisms)
				}
				return builtInTestDialer{session: &enrollmentTokenSession{builtInTestSession: newBuiltInTestSession()}}, nil
			})
			core := newEnrollmentTestCore(t, connectivity, provider)
			base := v1.CommandBase{CommandID: "token-auth", SDKSessionID: "token-session"}
			auth := v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"}
			var command v1.Command = v1.AuthTokenLoginCommand{CommandBase: base, Auth: auth}
			wantPersonality := v1.SDKPersonalityHTTPBridge
			if native {
				command = v1.AuthTokenConnectCommand{CommandBase: base, Auth: auth}
				wantPersonality = v1.SDKPersonalityNative
			}
			admission, err := core.Submit(t.Context(), command)
			if err != nil || !admission.Accepted {
				t.Fatalf("admission = %#v, %v", admission, err)
			}
			completion, err := core.NextCompletion(t.Context())
			if err != nil || !completion.OK {
				t.Fatalf("completion = %#v, %v", completion, err)
			}
			request := provider.snapshot()
			defer request.Clear()
			if string(request.Token) != enrollmentPublicToken[5:] || len(request.Token) != 83 ||
				request.MeshID != "mesh-one" || !enrollment.ValidCanonicalUUID(request.InstallationID) ||
				len(request.InstallationSecret) != 43 {
				t.Fatalf("enrollment request shape is invalid")
			}
			result, ok := completion.Result.(v1.AuthResult)
			if !ok || result.AgentID != "agent@example.test" || result.MeshID != "mesh-one" ||
				result.AgentInstanceID != request.InstallationID || result.Personality != wantPersonality || !tokenMode {
				t.Fatalf("auth result = %#v", completion.Result)
			}
			status, err := core.Status(t.Context())
			if err != nil || status.MeshEndpoint != "" {
				t.Fatalf("token status exposed endpoint: %#v, %v", status, err)
			}
			encoded, err := json.Marshal([]any{completion, status})
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range [][]byte{[]byte(enrollmentPublicToken), []byte(enrollmentJWT), []byte(enrollmentWrapper), []byte("mesh.example.test:5222"), []byte("Friendly Agent")} {
				if bytes.Contains(encoded, secret) {
					t.Fatalf("public completion/status exposed private value")
				}
			}
			shutdownAndDrain(t, core)
			if err := core.Destroy(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenEnrollmentCancellationNeverStartsConnectivity(t *testing.T) {
	provider := &captureEnrollment{block: true, entered: make(chan struct{})}
	called := false
	connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		called = true
		return nil, rank2xmpp.ErrUnavailable
	})
	core := newEnrollmentTestCore(t, connectivity, provider)
	command := v1.AuthTokenConnectCommand{
		CommandBase: v1.CommandBase{CommandID: "cancel-token", SDKSessionID: "token-session"},
		Auth:        v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"},
	}
	admission, err := core.Submit(t.Context(), command)
	if err != nil || !admission.Accepted {
		t.Fatalf("admission = %#v, %v", admission, err)
	}
	<-provider.entered
	if err := core.Cancel(t.Context(), admission.CommandHandle); err != nil {
		t.Fatal(err)
	}
	completion, err := core.NextCompletion(t.Context())
	if err != nil || completion.OK || completion.Error == nil || called {
		t.Fatalf("cancel completion = %#v, %v; connectivity=%v", completion, err, called)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestTokenEnrollmentRetriesOneLogicalInstallationAfterCommittedTransientFailure(t *testing.T) {
	for _, transient := range []enrollment.FailureCode{enrollment.FailureUnavailable, enrollment.FailureDeadline} {
		t.Run(map[enrollment.FailureCode]string{
			enrollment.FailureUnavailable: "unavailable",
			enrollment.FailureDeadline:    "provider-deadline-with-command-budget",
		}[transient], func(t *testing.T) {
			provider := &retryingEnrollment{failures: []enrollment.FailureCode{transient}}
			defer provider.clear()
			connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
				return builtInTestDialer{session: &enrollmentTokenSession{builtInTestSession: newBuiltInTestSession()}}, nil
			})
			core := newEnrollmentTestCore(t, connectivity, provider)
			admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{
				CommandBase: v1.CommandBase{CommandID: "retry-token", SDKSessionID: "token-session"},
				Auth:        v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"},
			})
			if err != nil || !admission.Accepted {
				t.Fatalf("admission = %#v, %v", admission, err)
			}
			completion, err := core.NextCompletion(t.Context())
			if err != nil || !completion.OK {
				t.Fatalf("completion = %#v, %v", completion, err)
			}
			provider.mu.Lock()
			requestCount := len(provider.requests)
			if requestCount != 2 {
				provider.mu.Unlock()
				t.Fatalf("enrollment calls = %d, want 2", requestCount)
			}
			first, second := provider.requests[0], provider.requests[1]
			sameLogicalInstallation := first.InstallationID == second.InstallationID &&
				bytes.Equal(first.InstallationSecret, second.InstallationSecret) &&
				bytes.Equal(first.Token, second.Token) && first.MeshID == second.MeshID
			logicalInstallations := len(provider.installations)
			logicalInstallationID := first.InstallationID
			provider.mu.Unlock()
			if !sameLogicalInstallation {
				t.Fatal("transient retry created a second logical installation")
			}
			if logicalInstallations != 1 {
				t.Fatalf("logical installations = %d, want 1", logicalInstallations)
			}
			result, ok := completion.Result.(v1.AuthResult)
			if !ok || result.AgentInstanceID != logicalInstallationID {
				t.Fatalf("result = %#v", completion.Result)
			}
			shutdownAndDrain(t, core)
			if err := core.Destroy(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenEnrollmentNeverRetriesRejectedOrInvalidResponse(t *testing.T) {
	for _, test := range []struct {
		name     string
		failures []enrollment.FailureCode
	}{
		{name: "rejected", failures: []enrollment.FailureCode{enrollment.FailureRejected}},
		{name: "invalid-response", failures: []enrollment.FailureCode{enrollment.FailureInvalidResponse}},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &retryingEnrollment{failures: test.failures}
			defer provider.clear()
			called := false
			connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
				called = true
				return nil, rank2xmpp.ErrUnavailable
			})
			core := newEnrollmentTestCore(t, connectivity, provider)
			admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{
				CommandBase: v1.CommandBase{CommandID: "no-retry-token", SDKSessionID: "token-session"},
				Auth:        v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"},
			})
			if err != nil || !admission.Accepted {
				t.Fatal("command was not admitted")
			}
			completion, err := core.NextCompletion(t.Context())
			if err != nil || completion.OK || completion.Error == nil || called {
				t.Fatalf("completion = %#v, %v; connectivity=%v", completion, err, called)
			}
			provider.mu.Lock()
			calls := len(provider.requests)
			provider.mu.Unlock()
			if calls != 1 {
				t.Fatalf("enrollment calls = %d, want 1", calls)
			}
			shutdownAndDrain(t, core)
			if err := core.Destroy(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenEnrollmentTransientRetriesAreBounded(t *testing.T) {
	provider := &retryingEnrollment{failures: []enrollment.FailureCode{
		enrollment.FailureUnavailable,
		enrollment.FailureUnavailable,
		enrollment.FailureUnavailable,
		enrollment.FailureUnavailable,
	}}
	defer provider.clear()
	called := false
	connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		called = true
		return nil, rank2xmpp.ErrUnavailable
	})
	core := newEnrollmentTestCore(t, connectivity, provider)
	admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{
		CommandBase: v1.CommandBase{CommandID: "bounded-retry-token", SDKSessionID: "token-session"},
		Auth:        v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"},
	})
	if err != nil || !admission.Accepted {
		t.Fatal("command was not admitted")
	}
	completion, err := core.NextCompletion(t.Context())
	if err != nil || completion.OK || completion.Error == nil ||
		completion.Error.Code != v1.ErrorCodeConnectivityUnavailable || called {
		t.Fatalf("completion = %#v, %v; connectivity=%v", completion, err, called)
	}
	provider.mu.Lock()
	calls := len(provider.requests)
	provider.mu.Unlock()
	if calls != maxEnrollmentAttemptsForTest {
		t.Fatalf("enrollment calls = %d, want %d", calls, maxEnrollmentAttemptsForTest)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestTokenEnrollmentRestartsFromDurableCacheWhileEnrollmentIsOffline(t *testing.T) {
	stateRoot := t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CYNAPSA_STATE_DIRECTORY", stateRoot)
	connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		return builtInTestDialer{session: &enrollmentTokenSession{builtInTestSession: newBuiltInTestSession()}}, nil
	})
	firstProvider := &durableEnrollmentProvider{}
	first := newEnrollmentStateTestCore(t, connectivity, firstProvider, enrollment.NewProductionStateStore())
	firstResult := submitTokenLogin(t, first, "durable-first")
	shutdownAndDrain(t, first)
	if err := first.Destroy(); err != nil {
		t.Fatal(err)
	}
	if enroll, renew := firstProvider.counts(); enroll != 1 || renew != 0 {
		t.Fatalf("initial calls enroll=%d renew=%d", enroll, renew)
	}

	offlineProvider := &durableEnrollmentProvider{unavailable: true}
	second := newEnrollmentStateTestCore(t, connectivity, offlineProvider, enrollment.NewProductionStateStore())
	secondResult := submitTokenLogin(t, second, "durable-second")
	if secondResult.AgentInstanceID != firstResult.AgentInstanceID {
		t.Fatalf("installation changed across restart: %q != %q", secondResult.AgentInstanceID, firstResult.AgentInstanceID)
	}
	if enroll, renew := offlineProvider.counts(); enroll != 0 || renew != 0 {
		t.Fatalf("offline cache called provider: enroll=%d renew=%d", enroll, renew)
	}
	shutdownAndDrain(t, second)
	if err := second.Destroy(); err != nil {
		t.Fatal(err)
	}
	stateStore := enrollment.NewProductionStateStore()
	profile, found, err := stateStore.(enrollment.ProfileStore).LoadProfile(t.Context(), enrollment.DefaultProfileID)
	if err != nil || !found || len(profile.Credentials) != 1 {
		t.Fatalf("durable profile after close found=%v err=%v", found, err)
	}
	profile.Clear()
	if legacy, found, err := stateStore.Load(t.Context(), []byte(enrollmentPublicToken[5:]), "mesh-one"); err != nil || found {
		legacy.Clear()
		t.Fatalf("legacy token-wrapped state remained after migration found=%v err=%v", found, err)
	}
}

func TestTokenEnrollmentNearExpiryRenewsSameInstallation(t *testing.T) {
	stateRoot := t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CYNAPSA_STATE_DIRECTORY", stateRoot)
	token := []byte(enrollmentPublicToken[5:])
	secret := []byte("ERERERERERERERERERERERERERERERERERERERERERE")
	request := enrollment.Request{
		Token: token, MeshID: "mesh-one", InstallationID: "11111111-1111-4111-9111-111111111111",
		InstallationSecret: secret,
	}
	bundle := successfulEnrollmentBundle(request)
	receivedAt := time.Now().UTC().Add(-24*time.Hour + 30*time.Second).Truncate(time.Second)
	usableUntil, err := enrollment.UsableUntil(bundle.AccessToken, receivedAt, bundle.ExpiresIn)
	if err != nil {
		t.Fatal(err)
	}
	state := enrollment.State{
		Version: 1, MeshID: request.MeshID, InstallationID: request.InstallationID,
		InstallationSecret: secret, Bundle: &bundle, ReceivedAt: receivedAt, UsableUntil: usableUntil,
	}
	if err := enrollment.NewProductionStateStore().Save(t.Context(), token, state); err != nil {
		t.Fatal(err)
	}
	state.Clear()
	provider := &durableEnrollmentProvider{}
	connectivity := newBuiltInConnectivityFactory(func(string, sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		return builtInTestDialer{session: &enrollmentTokenSession{builtInTestSession: newBuiltInTestSession()}}, nil
	})
	core := newEnrollmentStateTestCore(t, connectivity, provider, enrollment.NewProductionStateStore())
	result := submitTokenLogin(t, core, "durable-renew")
	if result.AgentInstanceID != request.InstallationID {
		t.Fatalf("renewed installation=%q", result.AgentInstanceID)
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, renew := provider.counts()
		if renew == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if enroll, renew := provider.counts(); enroll != 0 || renew != 1 {
		t.Fatalf("renew calls enroll=%d renew=%d", enroll, renew)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func submitTokenLogin(t *testing.T, core *Core, commandID string) v1.AuthResult {
	t.Helper()
	admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{
		CommandBase: v1.CommandBase{CommandID: v1.CommandID(commandID), SDKSessionID: v1.SDKSessionID(commandID + "-session")},
		Auth:        v1.AuthTokenInput{Token: enrollmentPublicToken, MeshID: "mesh-one"},
	})
	if err != nil || !admission.Accepted {
		t.Fatalf("admission=%#v err=%v", admission, err)
	}
	completion, err := core.NextCompletion(t.Context())
	result, ok := completion.Result.(v1.AuthResult)
	if err != nil || !completion.OK || !ok {
		t.Fatalf("completion=%#v err=%v", completion, err)
	}
	return result
}

func newEnrollmentStateTestCore(t *testing.T, connectivity sessionkernel.ConnectivityFactory, provider enrollment.Provider, state enrollment.StateStore) *Core {
	t.Helper()
	boundary, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	core, err := newCoreWithConnectivityEnrollmentAndState(Config{QueueLimit: 4, PayloadLimit: 1 << 20}, boundary, connectivity, provider, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return core
}

const maxEnrollmentAttemptsForTest = 3

func newEnrollmentTestCore(t *testing.T, connectivity sessionkernel.ConnectivityFactory, provider enrollment.Provider) *Core {
	t.Helper()
	boundary, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	core, err := newCoreWithConnectivityAndEnrollment(Config{QueueLimit: 4, PayloadLimit: 1 << 20}, boundary, connectivity, provider)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return core
}
