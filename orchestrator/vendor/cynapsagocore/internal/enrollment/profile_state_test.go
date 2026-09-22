package enrollment

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func profileTestBundle(now time.Time, installationID, meshID string) Bundle {
	return Bundle{
		Version: "e2", OrganizationID: "33333333-3333-4333-a333-333333333333",
		InstallationID: installationID, AgentID: "22222222-2222-4222-a222-222222222222",
		AgentJID: "agent@example.test", MeshID: meshID, SessionResource: "r2." + installationID + ".session",
		MeshEndpoint: "mesh.example.test:5222", Username: "agent@example.test", Server: "mesh.example.test",
		AccessToken: stateTestJWT(now.Add(36 * time.Hour)), ExpiresIn: 36 * 60 * 60,
		InstallationEpoch: 1, AttachmentRevision: 1, TokenID: "token-1", PolicyRevision: 1,
		SessionExpiryMode: "continue", OfflineColdStartTargetSeconds: 24 * 60 * 60,
	}
}

func TestProfileStateRoundTripUsesIndependentKeyAndKeepsMeshCaches(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	now := time.Now().UTC().Truncate(time.Second)
	installationID, secret := "11111111-1111-4111-9111-111111111111", []byte("ERERERERERERERERERERERERERERERERERERERERERE")
	profile := Profile{Version: ProfileStateVersion, ProfileID: "replica-a", InstallationID: installationID,
		InstallationSecret: secret, AgentID: "22222222-2222-4222-a222-222222222222", Credentials: map[string]Credential{}}
	for _, meshID := range []string{"mesh-one", "mesh-two"} {
		bundle := profileTestBundle(now, installationID, meshID)
		profile.Credentials[meshID] = Credential{Bundle: bundle, ReceivedAt: now, UsableUntil: now.Add(36 * time.Hour)}
	}
	store := NewProductionStateStore().(ProfileStore)
	if err := store.SaveProfile(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := NewProductionStateStore().(ProfileStore).LoadProfile(context.Background(), profile.ProfileID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	defer loaded.Clear()
	if len(loaded.Credentials) != 2 || loaded.InstallationID != installationID || !bytes.Equal(loaded.InstallationSecret, secret) {
		t.Fatal("installation profile or mesh caches were not preserved")
	}
	path, _ := profilePath(root, profile.ProfileID)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{secret, []byte(installationID), []byte("mesh-one"), profile.Credentials["mesh-one"].Bundle.AccessToken, []byte("cpsa_")} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatal("profile ciphertext exposed private state")
		}
	}
	if err := os.Remove(filepath.Join(root, profileKeyFile)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadProfile(context.Background(), profile.ProfileID); err == nil || found {
		t.Fatal("profile opened without its independently persisted key")
	}
}

func TestClientV2EnrollSendsCanonicalPublicTokenAndMapsFrozenFields(t *testing.T) {
	request := validRequestFixture()
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.URL.Path != "/v2/enroll" {
			t.Errorf("path=%q", httpRequest.URL.Path)
		}
		data, err := io.ReadAll(httpRequest.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var input requestWire
		if err := json.Unmarshal(data, &input); err != nil {
			t.Error(err)
			return
		}
		if input.Token != publicTokenPrefix+string(request.Token) {
			t.Error("v2 enrollment did not receive canonical cpsa token")
		}
		bundle := profileTestBundle(time.Now().UTC(), request.InstallationID, request.MeshID)
		bundle.Wrapper, bundle.Display = []byte("aztm_wrapper-value"), "Friendly Agent"
		wire := responseWire{
			Version: bundle.Version, OrganizationID: bundle.OrganizationID, InstallationID: bundle.InstallationID,
			AgentID: bundle.AgentID, AgentJID: bundle.AgentJID, MeshID: bundle.MeshID, SessionResource: bundle.SessionResource,
			MeshEndpoint: bundle.MeshEndpoint, Username: bundle.Username, Server: bundle.Server, AccessToken: string(bundle.AccessToken),
			ExpiresIn: bundle.ExpiresIn, Wrapper: string(bundle.Wrapper), Display: bundle.Display, InstallationEpoch: bundle.InstallationEpoch,
			AttachmentRevision: bundle.AttachmentRevision, TokenID: bundle.TokenID, PolicyRevision: bundle.PolicyRevision,
			SessionExpiryMode: bundle.SessionExpiryMode, OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(response).Encode(wire)
	}))
	defer server.Close()
	client, err := NewTestClient(server.URL+"/v2/enroll", server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	bundle, failure := client.EnrollV2(context.Background(), request)
	if failure != nil || bundle.Version != "e2" || bundle.AgentJID != bundle.Username || bundle.SessionExpiryMode != "continue" {
		t.Fatalf("bundle=%#v failure=%v", bundle, failure)
	}
	bundle.Clear()
}
