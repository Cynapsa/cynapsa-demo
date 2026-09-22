package enrollment

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ProfileStateVersion = 2
	DefaultProfileID    = "default"
	maximumProfileID    = 128
	maximumMeshCaches   = 256
	profileKeyFile      = ".auth-v2.key"
	profileLockFile     = ".auth-v2.lock"
	profileFilePrefix   = "profile-v2-"
)

var profileMagic = [8]byte{'C', 'P', 'S', 'A', 'P', 'R', '0', '2'}

// Credential is one mesh-scoped runtime credential cache. Installation
// identity is deliberately stored once at Profile level.
type Credential struct {
	Bundle      Bundle
	ReceivedAt  time.Time
	UsableUntil time.Time
}

func (credential Credential) Clone() Credential {
	credential.Bundle = cloneBundle(credential.Bundle)
	return credential
}

func (credential *Credential) Clear() {
	if credential == nil {
		return
	}
	credential.Bundle.Clear()
	*credential = Credential{}
}

// Profile is one independently recoverable installation. ProfileID is a
// caller-selected local namespace, not a server identity or authorization.
// Bootstrap enrollment tokens are never retained in this record.
type Profile struct {
	Version            int
	ProfileID          string
	InstallationID     string
	InstallationSecret []byte
	AgentID            string
	Credentials        map[string]Credential
}

func (profile Profile) Clone() Profile {
	clone := profile
	clone.InstallationSecret = append([]byte(nil), profile.InstallationSecret...)
	clone.Credentials = make(map[string]Credential, len(profile.Credentials))
	for meshID, credential := range profile.Credentials {
		clone.Credentials[strings.Clone(meshID)] = credential.Clone()
	}
	return clone
}

func (profile *Profile) Clear() {
	if profile == nil {
		return
	}
	clear(profile.InstallationSecret)
	for meshID, credential := range profile.Credentials {
		credential.Clear()
		delete(profile.Credentials, meshID)
	}
	*profile = Profile{}
}

// ProfileStore is the v2 installation-centric storage extension. StateStore
// retains its v1 methods solely for safe in-place migration.
type ProfileStore interface {
	LoadProfile(context.Context, string) (Profile, bool, error)
	SaveProfile(context.Context, Profile) error
	DeleteLegacy(context.Context, []byte, string) error
}

func ValidProfileID(value string) bool {
	if value == "" || len(value) > maximumProfileID || !validOpaque(value, maximumProfileID) {
		return false
	}
	return value != "." && value != ".." && !strings.ContainsAny(value, "/\\")
}

func validateProfile(profile Profile) error {
	if profile.Version != ProfileStateVersion || !ValidProfileID(profile.ProfileID) ||
		!ValidCanonicalUUID(profile.InstallationID) || !validCanonicalSecret(profile.InstallationSecret) ||
		len(profile.Credentials) > maximumMeshCaches {
		return ErrState
	}
	if profile.AgentID != "" && !ValidCanonicalUUID(profile.AgentID) {
		return ErrState
	}
	for meshID, credential := range profile.Credentials {
		if credential.Bundle.MeshID != meshID || credential.Bundle.InstallationID != profile.InstallationID ||
			(profile.AgentID != "" && credential.Bundle.AgentID != profile.AgentID) ||
			!validateCachedBundle(meshID, profile.InstallationID, credential.Bundle) ||
			credential.ReceivedAt.IsZero() || credential.UsableUntil.IsZero() ||
			credential.ReceivedAt.Location() != time.UTC || credential.UsableUntil.Location() != time.UTC {
			return ErrState
		}
		want, err := UsableUntil(credential.Bundle.AccessToken, credential.ReceivedAt, credential.Bundle.ExpiresIn)
		if err != nil || !want.Equal(credential.UsableUntil) {
			return ErrState
		}
	}
	return nil
}

type profileWire struct {
	Version            int                              `json:"version"`
	ProfileID          string                           `json:"profile_id"`
	InstallationID     string                           `json:"installation_id"`
	InstallationSecret []byte                           `json:"installation_secret"`
	AgentID            string                           `json:"agent_id,omitempty"`
	Credentials        map[string]profileCredentialWire `json:"credentials"`
}

type profileCredentialWire struct {
	Bundle      cachedBundleWire `json:"bundle"`
	ReceivedAt  string           `json:"received_at"`
	UsableUntil string           `json:"usable_until"`
}

func encodeProfile(profile Profile) ([]byte, error) {
	wire := profileWire{
		Version: profile.Version, ProfileID: profile.ProfileID, InstallationID: profile.InstallationID,
		InstallationSecret: profile.InstallationSecret, AgentID: profile.AgentID,
		Credentials: make(map[string]profileCredentialWire, len(profile.Credentials)),
	}
	for meshID, credential := range profile.Credentials {
		bundle := credential.Bundle
		wire.Credentials[meshID] = profileCredentialWire{Bundle: cachedBundleWire{
			Version: bundle.Version, OrganizationID: bundle.OrganizationID, InstallationID: bundle.InstallationID, AgentID: bundle.AgentID, AgentJID: bundle.AgentJID,
			MeshID: bundle.MeshID, SessionResource: bundle.SessionResource, MeshEndpoint: bundle.MeshEndpoint,
			Username: bundle.Username, Server: bundle.Server, AccessToken: bundle.AccessToken, ExpiresIn: bundle.ExpiresIn,
			InstallationEpoch: bundle.InstallationEpoch, AttachmentRevision: bundle.AttachmentRevision, TokenID: bundle.TokenID,
			PolicyRevision: bundle.PolicyRevision, SessionExpiryMode: bundle.SessionExpiryMode,
			OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
		}, ReceivedAt: credential.ReceivedAt.Format(time.RFC3339Nano), UsableUntil: credential.UsableUntil.Format(time.RFC3339Nano)}
	}
	return json.Marshal(wire)
}

func decodeProfile(data []byte) (Profile, error) {
	if len(data) == 0 || len(data) > maximumStateBytes || rejectDuplicateJSONKeys(data) != nil {
		return Profile{}, ErrState
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire profileWire
	if err := decoder.Decode(&wire); err != nil {
		return Profile{}, ErrState
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Profile{}, ErrState
	}
	profile := Profile{Version: wire.Version, ProfileID: wire.ProfileID, InstallationID: wire.InstallationID,
		InstallationSecret: wire.InstallationSecret, AgentID: wire.AgentID, Credentials: make(map[string]Credential, len(wire.Credentials))}
	for meshID, value := range wire.Credentials {
		receivedAt, err := time.Parse(time.RFC3339Nano, value.ReceivedAt)
		if err != nil {
			profile.Clear()
			return Profile{}, ErrState
		}
		usableUntil, err := time.Parse(time.RFC3339Nano, value.UsableUntil)
		if err != nil {
			profile.Clear()
			return Profile{}, ErrState
		}
		bundle := value.Bundle
		profile.Credentials[meshID] = Credential{Bundle: Bundle{
			Version: bundle.Version, OrganizationID: bundle.OrganizationID, InstallationID: bundle.InstallationID, AgentID: bundle.AgentID, AgentJID: bundle.AgentJID,
			MeshID: bundle.MeshID, SessionResource: bundle.SessionResource, MeshEndpoint: bundle.MeshEndpoint,
			Username: bundle.Username, Server: bundle.Server, AccessToken: bundle.AccessToken, ExpiresIn: bundle.ExpiresIn,
			InstallationEpoch: bundle.InstallationEpoch, AttachmentRevision: bundle.AttachmentRevision, TokenID: bundle.TokenID,
			PolicyRevision: bundle.PolicyRevision, SessionExpiryMode: bundle.SessionExpiryMode,
			OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
		}, ReceivedAt: receivedAt.UTC(), UsableUntil: usableUntil.UTC()}
	}
	if validateProfile(profile) != nil {
		profile.Clear()
		return Profile{}, ErrState
	}
	return profile, nil
}

func profileFingerprint(profileID string) ([sha256.Size]byte, error) {
	if !ValidProfileID(profileID) {
		return [sha256.Size]byte{}, ErrState
	}
	return sha256.Sum256([]byte("cynapsa-core/profile-state/v2\x00" + profileID)), nil
}

func profilePath(root, profileID string) (string, error) {
	fingerprint, err := profileFingerprint(profileID)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, profileFilePrefix+hex.EncodeToString(fingerprint[:])+stateFileSuffix), nil
}

func (store *memoryStateStore) LoadProfile(ctx context.Context, profileID string) (Profile, bool, error) {
	if ctx == nil || ctx.Err() != nil || !ValidProfileID(profileID) {
		return Profile{}, false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	profile, ok := store.profiles[profileID]
	return profile.Clone(), ok, nil
}

func (store *memoryStateStore) SaveProfile(ctx context.Context, profile Profile) error {
	if ctx == nil || ctx.Err() != nil || validateProfile(profile) != nil {
		return ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if old, ok := store.profiles[profile.ProfileID]; ok {
		old.Clear()
	}
	store.profiles[profile.ProfileID] = profile.Clone()
	return nil
}

func (store *memoryStateStore) DeleteLegacy(ctx context.Context, token []byte, meshID string) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrState
	}
	key, err := stateFingerprint(token, meshID)
	if err != nil {
		return ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if old, ok := store.records[key]; ok {
		old.Clear()
		delete(store.records, key)
	}
	return nil
}

func (store *fileStateStore) LoadProfile(ctx context.Context, profileID string) (Profile, bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil {
		return Profile{}, false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return Profile{}, false, ErrState
	}
	path, err := profilePath(root, profileID)
	if err != nil {
		return Profile{}, false, ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer clear(key)
	data, found, err := readPrivateFile(path)
	if err != nil || !found {
		return Profile{}, found, err
	}
	defer clear(data)
	plaintext, err := openProfile(key, profileID, data)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer clear(plaintext)
	profile, err := decodeProfile(plaintext)
	if err != nil || profile.ProfileID != profileID {
		profile.Clear()
		return Profile{}, false, ErrState
	}
	return profile, true, nil
}

func (store *fileStateStore) SaveProfile(ctx context.Context, profile Profile) error {
	if store == nil || ctx == nil || ctx.Err() != nil || validateProfile(profile) != nil {
		return ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return ErrState
	}
	unlock, err := lockProfileRoot(root)
	if err != nil {
		return ErrState
	}
	defer unlock()
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return ErrState
	}
	defer clear(key)
	plaintext, err := encodeProfile(profile)
	if err != nil {
		return ErrState
	}
	defer clear(plaintext)
	sealed, err := sealProfile(store.random, key, profile.ProfileID, plaintext)
	if err != nil || len(sealed) > maximumStateBytes {
		clear(sealed)
		return ErrState
	}
	defer clear(sealed)
	path, _ := profilePath(root, profile.ProfileID)
	return atomicPrivateWrite(ctx, root, path, sealed, store.random)
}

func (store *fileStateStore) DeleteLegacy(ctx context.Context, token []byte, meshID string) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return ErrState
	}
	fingerprint, err := stateFingerprint(token, meshID)
	if err != nil {
		return ErrState
	}
	path := filepath.Join(root, hex.EncodeToString(fingerprint[:])+stateFileSuffix)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrState
	}
	directory, err := os.Open(root)
	if err != nil {
		return ErrState
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return ErrState
	}
	return nil
}

func readPrivateFile(path string) ([]byte, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, false, ErrState
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, false, ErrState
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maximumStateBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) > maximumStateBytes {
		clear(data)
		return nil, false, ErrState
	}
	return data, true, nil
}

func loadOrCreateProfileKey(root string, randomSource io.Reader) ([]byte, error) {
	path := filepath.Join(root, profileKeyFile)
	if data, found, err := readPrivateFile(path); err != nil || found {
		if err != nil || len(data) != sha256.Size {
			clear(data)
			return nil, ErrState
		}
		return data, nil
	}
	key := make([]byte, sha256.Size)
	if randomSource == nil {
		randomSource = rand.Reader
	}
	if _, err := io.ReadFull(randomSource, key); err != nil {
		clear(key)
		return nil, ErrState
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		clear(key)
		return loadOrCreateProfileKey(root, randomSource)
	}
	if err != nil {
		clear(key)
		return nil, ErrState
	}
	if _, err = file.Write(key); err != nil || file.Sync() != nil || file.Close() != nil {
		_ = os.Remove(path)
		clear(key)
		return nil, ErrState
	}
	return key, nil
}

func atomicPrivateWrite(ctx context.Context, root, destination string, data []byte, randomSource io.Reader) error {
	if ctx.Err() != nil {
		return ErrState
	}
	var suffix [8]byte
	if _, err := io.ReadFull(randomSource, suffix[:]); err != nil {
		return ErrState
	}
	temporary := destination + ".tmp-" + hex.EncodeToString(suffix[:])
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ErrState
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = os.Remove(temporary)
		}
	}()
	if _, err = file.Write(data); err != nil || ctx.Err() != nil || file.Sync() != nil || file.Close() != nil {
		return ErrState
	}
	if err = os.Rename(temporary, destination); err != nil {
		return ErrState
	}
	directory, err := os.Open(root)
	if err != nil {
		return ErrState
	}
	err = directory.Sync()
	closeErr := directory.Close()
	if err != nil || closeErr != nil {
		return ErrState
	}
	committed = true
	return nil
}

func sealProfile(randomSource io.Reader, key []byte, profileID string, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil || randomSource == nil {
		return nil, ErrState
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrState
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(randomSource, nonce); err != nil {
		return nil, ErrState
	}
	header := append(append([]byte(nil), profileMagic[:]...), byte(ProfileStateVersion))
	fingerprint, _ := profileFingerprint(profileID)
	aad := append(append([]byte(nil), header...), fingerprint[:]...)
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	output := append(append(header, nonce...), sealed...)
	clear(nonce)
	clear(sealed)
	clear(aad)
	return output, nil
}

func openProfile(key []byte, profileID string, encoded []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrState
	}
	gcm, err := cipher.NewGCM(block)
	headerSize := len(profileMagic) + 1
	if err != nil || len(encoded) < headerSize+gcm.NonceSize()+gcm.Overhead() ||
		!bytes.Equal(encoded[:len(profileMagic)], profileMagic[:]) || encoded[len(profileMagic)] != ProfileStateVersion {
		return nil, ErrState
	}
	fingerprint, _ := profileFingerprint(profileID)
	aad := append(append([]byte(nil), encoded[:headerSize]...), fingerprint[:]...)
	plaintext, err := gcm.Open(nil, encoded[headerSize:headerSize+gcm.NonceSize()], encoded[headerSize+gcm.NonceSize():], aad)
	clear(aad)
	if err != nil {
		clear(plaintext)
		return nil, ErrState
	}
	return plaintext, nil
}

func cloneBundle(bundle Bundle) Bundle {
	bundle.AccessToken = append([]byte(nil), bundle.AccessToken...)
	bundle.Wrapper = append([]byte(nil), bundle.Wrapper...)
	return bundle
}
