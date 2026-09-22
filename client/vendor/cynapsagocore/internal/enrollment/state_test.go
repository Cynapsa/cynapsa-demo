package enrollment

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func stateTestJWT(exp time.Time) []byte {
	payloadJSON, _ := json.Marshal(map[string]any{"exp": exp.Unix(), "scope": "mesh"})
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	return []byte("e30." + payload + ".sig")
}

func stateTestFixture(now time.Time) ([]byte, State) {
	token := []byte(testToken[5:])
	secret := []byte("ERERERERERERERERERERERERERERERERERERERERERE")
	bundle := Bundle{
		Version: "e1", InstallationID: "11111111-1111-4111-9111-111111111111",
		AgentID: "22222222-2222-4222-a222-222222222222", MeshID: "mesh-one",
		SessionResource: "installation-11111111", MeshEndpoint: "mesh.example.test:5222",
		Username: "agent@example.test", Server: "mesh.example.test",
		AccessToken: stateTestJWT(now.Add(2 * time.Hour)), ExpiresIn: 3600,
		Wrapper: []byte("aztm_wrapper-value"), Display: "Friendly Agent",
	}
	usable, _ := UsableUntil(bundle.AccessToken, now, bundle.ExpiresIn)
	return token, State{
		Version: 1, MeshID: "mesh-one", InstallationID: bundle.InstallationID,
		InstallationSecret: secret, Bundle: &bundle, ReceivedAt: now, UsableUntil: usable,
	}
}

func TestFileStateRoundTripIsEncryptedPrivateAndMinimal(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	now := time.Now().UTC().Truncate(time.Second)
	token, state := stateTestFixture(now)
	defer state.Clear()
	store := NewProductionStateStore()
	if err := store.Save(context.Background(), token, state); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "*"+stateFileSuffix))
	if err != nil || len(files) != 1 {
		t.Fatalf("state files=%v err=%v", files, err)
	}
	info, err := os.Lstat(files[0])
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("state mode=%v err=%v", info.Mode(), err)
	}
	encoded, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{token, state.InstallationSecret, state.Bundle.AccessToken, state.Bundle.Wrapper, []byte(state.Bundle.Display), []byte(state.MeshID)} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatal("state file exposed encrypted or unnecessary material")
		}
	}
	loaded, found, err := NewProductionStateStore().Load(context.Background(), token, state.MeshID)
	if err != nil || !found {
		t.Fatalf("load found=%v err=%v", found, err)
	}
	defer loaded.Clear()
	if loaded.InstallationID != state.InstallationID || !bytes.Equal(loaded.InstallationSecret, state.InstallationSecret) ||
		loaded.Bundle == nil || !bytes.Equal(loaded.Bundle.AccessToken, state.Bundle.AccessToken) ||
		len(loaded.Bundle.Wrapper) != 0 || loaded.Bundle.Display != "" || !loaded.UsableUntil.Equal(state.UsableUntil) {
		t.Fatal("loaded state did not preserve the runtime credential tuple")
	}
}

func TestFileStateWrongTokenCorruptionAndUnsafePathsFailClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	token, state := stateTestFixture(time.Now().UTC().Truncate(time.Second))
	defer state.Clear()
	store := NewProductionStateStore()
	if err := store.Save(context.Background(), token, state); err != nil {
		t.Fatal(err)
	}
	wrong := append([]byte(nil), token...)
	wrong[40] = 'B'
	if loaded, found, err := store.Load(context.Background(), wrong, state.MeshID); err != nil || found {
		loaded.Clear()
		t.Fatalf("wrong token found=%v err=%v", found, err)
	}
	files, _ := filepath.Glob(filepath.Join(root, "*"+stateFileSuffix))
	encoded, _ := os.ReadFile(files[0])
	encoded[len(encoded)-1] ^= 1
	if err := os.WriteFile(files[0], encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if loaded, found, err := store.Load(context.Background(), token, state.MeshID); err == nil || found {
		loaded.Clear()
		t.Fatalf("corrupt state found=%v err=%v", found, err)
	}
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), token, state); err == nil {
		t.Fatal("unsafe directory permissions were accepted")
	}
}

func TestFileStatePersistsPendingInstallationBeforeCredential(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	token, ready := stateTestFixture(time.Now().UTC().Truncate(time.Second))
	pending := ready
	pending.Bundle = nil
	pending.ReceivedAt = time.Time{}
	pending.UsableUntil = time.Time{}
	defer ready.Clear()
	store := NewProductionStateStore()
	if err := store.Save(context.Background(), token, pending); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := NewProductionStateStore().Load(context.Background(), token, pending.MeshID)
	if err != nil || !found {
		t.Fatalf("pending load found=%v err=%v", found, err)
	}
	defer loaded.Clear()
	if loaded.Bundle != nil || loaded.InstallationID != pending.InstallationID || !bytes.Equal(loaded.InstallationSecret, pending.InstallationSecret) {
		t.Fatal("pending installation tuple was not retained")
	}
}

func TestUsableUntilUsesEarlierJWTExpiryAndRejectsMalformedClaims(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	jwtExpiry := now.Add(10 * time.Minute)
	usable, err := UsableUntil(stateTestJWT(jwtExpiry), now, 3600)
	if err != nil || !usable.Equal(jwtExpiry) {
		t.Fatalf("usable=%v err=%v", usable, err)
	}
	for _, token := range [][]byte{
		[]byte("header.payload.signature"),
		[]byte("e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":1,"exp":2}`)) + ".sig"),
		[]byte("e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":"2"}`)) + ".sig"),
	} {
		if _, err := UsableUntil(token, now, 60); err == nil {
			t.Fatal("malformed JWT expiry was accepted")
		}
	}
}
