package cynapsagocore

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

type capturingHTTPEnrollment struct {
	client *enrollment.Client
	mu     sync.Mutex
	bundle enrollment.Bundle
}

func (provider *capturingHTTPEnrollment) Enroll(ctx context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure) {
	bundle, failure := provider.client.Enroll(ctx, request)
	if failure == nil {
		provider.mu.Lock()
		provider.bundle.Clear()
		provider.bundle = enrollment.Bundle{
			Version: bundle.Version, InstallationID: bundle.InstallationID,
			AgentID: bundle.AgentID, MeshID: bundle.MeshID,
			SessionResource: bundle.SessionResource, MeshEndpoint: bundle.MeshEndpoint,
			Username: bundle.Username, Server: bundle.Server,
			AccessToken: append([]byte(nil), bundle.AccessToken...),
			ExpiresIn:   bundle.ExpiresIn, Wrapper: append([]byte(nil), bundle.Wrapper...),
			Display: bundle.Display,
		}
		provider.mu.Unlock()
	}
	return bundle, failure
}

func (provider *capturingHTTPEnrollment) clear() {
	provider.mu.Lock()
	provider.bundle.Clear()
	provider.mu.Unlock()
}

type capturingEnrollmentSession struct {
	*builtInTestSession
	mu         sync.Mutex
	endpoint   string
	username   string
	credential []byte
	meshID     string
}

func (session *capturingEnrollmentSession) ConnectTLS(_ context.Context, endpoint string) error {
	if endpoint == "" {
		return rank2xmpp.ErrInvalidConfig
	}
	session.mu.Lock()
	session.endpoint = endpoint
	session.mu.Unlock()
	return nil
}

func (session *capturingEnrollmentSession) Authenticate(_ context.Context, username string, credential []byte) (string, []byte, error) {
	session.mu.Lock()
	session.username = username
	session.credential = append(session.credential[:0], credential...)
	session.mu.Unlock()
	return username, nil, nil
}

func (session *capturingEnrollmentSession) BindResource(_ context.Context, resource string) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if resource != session.meshID || session.username == "" {
		return "", rank2xmpp.ErrIdentityBinding
	}
	return session.username + "/" + resource, nil
}

func (session *capturingEnrollmentSession) SyncAuthority(context.Context) (rank2xmpp.AuthoritySnapshot, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return rank2xmpp.AuthoritySnapshot{Members: []string{session.username + "/" + session.meshID}}, nil
}

func (session *capturingEnrollmentSession) clear() {
	session.mu.Lock()
	clear(session.credential)
	session.credential = nil
	session.mu.Unlock()
}

// TestEnrollmentHTTPGatewayToCorePreEjabberd is opt-in cross-repository
// acceptance. It exercises the actual HTTP gateway and management API, while
// replacing only ejabberd with a credential-capturing connectivity session.
func TestEnrollmentHTTPGatewayToCorePreEjabberd(t *testing.T) {
	endpoint := os.Getenv("CYNAPSA_ENROLLMENT_TEST_URL")
	token := os.Getenv("CYNAPSA_ENROLLMENT_TEST_TOKEN")
	if token == "" {
		if path := os.Getenv("CYNAPSA_ENROLLMENT_TEST_TOKEN_FILE"); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("unable to read enrollment token file")
			}
			token = strings.TrimSpace(string(data))
			clear(data)
		}
	}
	meshID := os.Getenv("CYNAPSA_ENROLLMENT_TEST_MESH_ID")
	if endpoint == "" || token == "" || meshID == "" {
		t.Skip("set CYNAPSA_ENROLLMENT_TEST_URL, CYNAPSA_ENROLLMENT_TEST_MESH_ID, and token or token file")
	}
	client, err := enrollment.NewTestClient(endpoint, nil)
	if err != nil {
		t.Fatal("invalid enrollment test endpoint")
	}
	provider := &capturingHTTPEnrollment{client: client}
	defer provider.clear()
	session := &capturingEnrollmentSession{builtInTestSession: newBuiltInTestSession(), meshID: meshID}
	defer session.clear()
	connectivity := newBuiltInConnectivityFactory(func(_ string, profile sessionkernel.OperationalProfile) (rank2xmpp.Dialer, error) {
		mechanisms := profile.SASLMechanisms()
		if !profile.TokenAuthentication || len(mechanisms) != 1 || mechanisms[0].Name != "PLAIN" {
			t.Fatal("token login did not select the TLS-protected PLAIN profile")
		}
		return builtInTestDialer{session: session}, nil
	})
	core := newEnrollmentTestCore(t, connectivity, provider)
	admission, err := core.Submit(t.Context(), v1.AuthTokenLoginCommand{
		CommandBase: v1.CommandBase{CommandID: "http-enrollment", SDKSessionID: "http-enrollment-session"},
		Auth:        v1.AuthTokenInput{Token: token, MeshID: v1.MeshID(meshID)},
	})
	if err != nil || !admission.Accepted {
		t.Fatal("enrollment command was not admitted")
	}
	completion, err := core.NextCompletion(t.Context())
	if err != nil || !completion.OK {
		t.Fatal("enrollment command did not complete")
	}
	provider.mu.Lock()
	bundle := provider.bundle
	session.mu.Lock()
	privateMapped := bundle.Username == session.username &&
		bundle.MeshEndpoint == session.endpoint &&
		bytes.Equal(bundle.AccessToken, session.credential) &&
		len(bundle.AccessToken) > 0 && len(bundle.Wrapper) > 5 &&
		bytes.HasPrefix(bundle.Wrapper, []byte("aztm_"))
	session.mu.Unlock()
	provider.mu.Unlock()
	if !privateMapped {
		t.Fatal("enrollment credentials were not mapped into private connectivity")
	}
	result, ok := completion.Result.(v1.AuthResult)
	if !ok || string(result.AgentID) != bundle.Username || string(result.MeshID) != meshID ||
		result.AgentInstanceID != bundle.InstallationID {
		t.Fatal("public authentication identity does not match enrolled installation")
	}
	public, err := jsonPublicAuthentication(completion, core)
	if err != nil || bytes.Contains(public, bundle.AccessToken) || bytes.Contains(public, bundle.Wrapper) ||
		bytes.Contains(public, []byte(bundle.MeshEndpoint)) {
		t.Fatal("public authentication output exposed private enrollment data")
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func jsonPublicAuthentication(completion v1.Completion, core *Core) ([]byte, error) {
	status, err := core.Status(context.Background())
	if err != nil {
		return nil, err
	}
	return json.Marshal([]any{completion, status})
}
