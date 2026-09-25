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
	forceLockFilePrefix = ".auth-v2-force-"
	forcePendingPrefix  = "cynapsa-internal/force-pending/v1/"
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
	// LoadOrCreateProfile returns the stored profile and whether candidate was
	// created. A stored profile is never overwritten, even if it differs.
	LoadOrCreateProfile(context.Context, Profile) (profile Profile, created bool, err error)
	// CompareAndSwapProfile replaces profileID only when its current
	// InstallationID equals expectedInstallationID. An empty expected ID
	// matches absence. On error the write may have taken effect; reload before
	// retrying with any newly generated installation identity or secret.
	CompareAndSwapProfile(context.Context, string, string, Profile) (swapped bool, err error)
	// SwapProfileWithPrevious performs the same CAS and returns an owned copy
	// of the profile it displaced. An absent-profile match returns a zero
	// previous profile. After a matched read, a write/readback error returns
	// that exact previous snapshot with swapped=false; the write outcome is
	// unknown, so callers must inspect current state before rollback. Errors
	// before the matched read (validation, lock, key, load) return zero previous.
	SwapProfileWithPrevious(context.Context, string, string, Profile) (previous Profile, swapped bool, err error)
	// CompareAndDeleteProfile deletes profileID only when its current
	// InstallationID matches expectedInstallationID. An empty expected ID
	// matches absence (and is a no-op). On error, reload to resolve outcome.
	CompareAndDeleteProfile(context.Context, string, string) (deleted bool, err error)
	// TryLockForceProfile acquires a nonblocking, per-profile operation lease.
	// If acquired, release must be called when force enrollment finishes;
	// repeated calls are safe. A contended lease returns (nil, false, nil). The lease is
	// independent of the profile writer lock and may span network I/O.
	TryLockForceProfile(context.Context, string) (release func(), acquired bool, err error)
	DeleteLegacy(context.Context, []byte, string) error
}

func ValidProfileID(value string) bool {
	if value == "" || len(value) > maximumProfileID || !validOpaque(value, maximumProfileID) {
		return false
	}
	return value != "." && value != ".." && !strings.ContainsAny(value, "/\\")
}

// ForcePendingProfileID derives the private stored-profile slot for a public
// profile ID. The result is deterministic but never a valid public profile ID.
// Its only accepted grammar is forcePendingPrefix plus 64 lowercase hex digits.
func ForcePendingProfileID(publicProfileID string) (string, error) {
	if !ValidProfileID(publicProfileID) {
		return "", ErrState
	}
	digest := sha256.Sum256([]byte("cynapsa-core/force-pending-profile/v1\x00" + publicProfileID))
	return forcePendingPrefix + hex.EncodeToString(digest[:]), nil
}

func validStoredProfileID(value string) bool {
	if ValidProfileID(value) {
		return true
	}
	if len(value) != len(forcePendingPrefix)+2*sha256.Size || len(value) > maximumProfileID ||
		!strings.HasPrefix(value, forcePendingPrefix) {
		return false
	}
	for i := len(forcePendingPrefix); i < len(value); i++ {
		if !(value[i] >= '0' && value[i] <= '9') && !(value[i] >= 'a' && value[i] <= 'f') {
			return false
		}
	}
	return true
}

func validateProfile(profile Profile) error {
	if profile.Version != ProfileStateVersion || !validStoredProfileID(profile.ProfileID) ||
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
	if !validStoredProfileID(profileID) {
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
	if ctx == nil || ctx.Err() != nil || !validStoredProfileID(profileID) {
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

func (store *memoryStateStore) LoadOrCreateProfile(ctx context.Context, candidate Profile) (Profile, bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || validateProfile(candidate) != nil {
		return Profile{}, false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if ctx.Err() != nil {
		return Profile{}, false, ErrState
	}
	if existing, ok := store.profiles[candidate.ProfileID]; ok {
		return existing.Clone(), false, nil
	}
	stored := candidate.Clone()
	store.profiles[candidate.ProfileID] = stored
	return stored.Clone(), true, nil
}

func (store *memoryStateStore) CompareAndSwapProfile(ctx context.Context, profileID, expectedInstallationID string, replacement Profile) (bool, error) {
	previous, swapped, err := store.SwapProfileWithPrevious(ctx, profileID, expectedInstallationID, replacement)
	previous.Clear()
	return swapped, err
}

func (store *memoryStateStore) SwapProfileWithPrevious(ctx context.Context, profileID, expectedInstallationID string, replacement Profile) (Profile, bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !validProfileSwap(profileID, expectedInstallationID, replacement) {
		return Profile{}, false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if ctx.Err() != nil {
		return Profile{}, false, ErrState
	}
	current, found := store.profiles[profileID]
	if !profileInstallationMatches(current, found, expectedInstallationID) {
		return Profile{}, false, nil
	}
	var previous Profile
	if found {
		previous = current.Clone()
	}
	store.profiles[profileID] = replacement.Clone()
	current.Clear()
	return previous, true, nil
}

func (store *memoryStateStore) CompareAndDeleteProfile(ctx context.Context, profileID, expectedInstallationID string) (bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !validProfileExpectation(profileID, expectedInstallationID) {
		return false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if ctx.Err() != nil {
		return false, ErrState
	}
	current, found := store.profiles[profileID]
	if !found || !profileInstallationMatches(current, true, expectedInstallationID) {
		return false, nil
	}
	delete(store.profiles, profileID)
	current.Clear()
	return true, nil
}

func validProfileExpectation(profileID, expectedInstallationID string) bool {
	return validStoredProfileID(profileID) && (expectedInstallationID == "" || ValidCanonicalUUID(expectedInstallationID))
}

func validProfileSwap(profileID, expectedInstallationID string, replacement Profile) bool {
	return validProfileExpectation(profileID, expectedInstallationID) &&
		replacement.ProfileID == profileID && validateProfile(replacement) == nil
}

func profileInstallationMatches(current Profile, found bool, expectedInstallationID string) bool {
	if expectedInstallationID == "" {
		return !found
	}
	return found && current.InstallationID == expectedInstallationID
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
	if !validStoredProfileID(profileID) {
		return Profile{}, false, ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer clear(key)
	return readStoredProfile(root, key, profileID)
}

func readStoredProfile(root string, key []byte, profileID string) (Profile, bool, error) {
	path, err := profilePath(root, profileID)
	if err != nil {
		return Profile{}, false, ErrState
	}
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
	if ctx.Err() != nil {
		return ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return ErrState
	}
	defer clear(key)
	return store.writeProfile(ctx, root, key, profile)
}

// writeProfile requires the profile writer lock. A failed write can still have
// renamed the new file before a directory sync failed, so callers must not
// infer that the old profile remains on disk from an error.
func (store *fileStateStore) writeProfile(ctx context.Context, root string, key []byte, profile Profile) error {
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
	write := atomicPrivateWrite
	if store.profileAtomicWrite != nil {
		write = store.profileAtomicWrite
	}
	return write(ctx, root, path, sealed, store.random)
}

func (store *fileStateStore) LoadOrCreateProfile(ctx context.Context, candidate Profile) (Profile, bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || validateProfile(candidate) != nil {
		return Profile{}, false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return Profile{}, false, ErrState
	}
	unlock, err := lockProfileRoot(root)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer unlock()
	if ctx.Err() != nil {
		return Profile{}, false, ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer clear(key)
	existing, found, err := readStoredProfile(root, key, candidate.ProfileID)
	if err != nil || found {
		return existing, false, err
	}
	if err := store.writeProfile(ctx, root, key, candidate); err != nil {
		// Rename may have succeeded before directory sync reported failure.
		// A matching readback plus a successful sync resolves that outcome.
		if !verifiedProfileWrite(root, key, candidate) {
			return Profile{}, false, ErrState
		}
	}
	stored, found, err := readStoredProfile(root, key, candidate.ProfileID)
	if err != nil || !found || !profilesEqual(stored, candidate) {
		stored.Clear()
		return Profile{}, false, ErrState
	}
	return stored, true, nil
}

func (store *fileStateStore) CompareAndSwapProfile(ctx context.Context, profileID, expectedInstallationID string, replacement Profile) (bool, error) {
	previous, swapped, err := store.SwapProfileWithPrevious(ctx, profileID, expectedInstallationID, replacement)
	previous.Clear()
	return swapped, err
}

func (store *fileStateStore) SwapProfileWithPrevious(ctx context.Context, profileID, expectedInstallationID string, replacement Profile) (Profile, bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !validProfileSwap(profileID, expectedInstallationID, replacement) {
		return Profile{}, false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return Profile{}, false, ErrState
	}
	unlock, err := lockProfileRoot(root)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer unlock()
	if ctx.Err() != nil {
		return Profile{}, false, ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer clear(key)
	current, found, err := readStoredProfile(root, key, profileID)
	if err != nil {
		return Profile{}, false, ErrState
	}
	defer current.Clear()
	if !profileInstallationMatches(current, found, expectedInstallationID) {
		return Profile{}, false, nil
	}
	var previous Profile
	if found {
		previous = current.Clone()
	}
	if err := store.writeProfile(ctx, root, key, replacement); err != nil {
		if !verifiedProfileWrite(root, key, replacement) {
			return previous, false, ErrState
		}
	}
	stored, found, err := readStoredProfile(root, key, profileID)
	if err != nil || !found || !profilesEqual(stored, replacement) {
		stored.Clear()
		return previous, false, ErrState
	}
	stored.Clear()
	return previous, true, nil
}

func (store *fileStateStore) CompareAndDeleteProfile(ctx context.Context, profileID, expectedInstallationID string) (bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !validProfileExpectation(profileID, expectedInstallationID) {
		return false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return false, ErrState
	}
	unlock, err := lockProfileRoot(root)
	if err != nil {
		return false, ErrState
	}
	defer unlock()
	if ctx.Err() != nil {
		return false, ErrState
	}
	key, err := loadOrCreateProfileKey(root, store.random)
	if err != nil {
		return false, ErrState
	}
	defer clear(key)
	current, found, err := readStoredProfile(root, key, profileID)
	if err != nil {
		return false, ErrState
	}
	defer current.Clear()
	if !found || !profileInstallationMatches(current, true, expectedInstallationID) {
		return false, nil
	}
	path, _ := profilePath(root, profileID)
	if err := os.Remove(path); err != nil {
		return false, ErrState
	}
	// Once removed, a failed directory sync is also an uncertain outcome.
	// Retry after verifying absence while still holding the writer lock.
	if !verifiedProfileDelete(root, key, profileID) {
		return false, ErrState
	}
	return true, nil
}

func verifiedProfileWrite(root string, key []byte, expected Profile) bool {
	stored, found, err := readStoredProfile(root, key, expected.ProfileID)
	defer stored.Clear()
	return err == nil && found && profilesEqual(stored, expected) && syncProfileRoot(root) == nil
}

func verifiedProfileDelete(root string, key []byte, profileID string) bool {
	stored, found, err := readStoredProfile(root, key, profileID)
	defer stored.Clear()
	return err == nil && !found && syncProfileRoot(root) == nil
}

func syncProfileRoot(root string) error {
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

func profilesEqual(a, b Profile) bool {
	aBytes, aErr := encodeProfile(a)
	bBytes, bErr := encodeProfile(b)
	defer clear(aBytes)
	defer clear(bBytes)
	return aErr == nil && bErr == nil && bytes.Equal(aBytes, bBytes)
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
