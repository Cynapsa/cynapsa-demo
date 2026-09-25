package enrollment

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	stateEnvironment       = "CYNAPSA_STATE_DIRECTORY"
	stateDirectoryName     = "Cynapsa/Core/auth-v1"
	stateVersion           = 1
	maximumStateBytes      = 64 << 10
	stateFileSuffix        = ".state"
	stateKeyDomain         = "cynapsa-core/enrollment-state/aes-256-gcm/v1"
	stateFingerprintDomain = "cynapsa-core/enrollment-state/fingerprint/v1"
)

var (
	stateMagic = [8]byte{'C', 'P', 'S', 'A', 'S', 'T', '0', '1'}
	ErrState   = errors.New("enrollment state unavailable")
)

// State is the process-private durable installation and credential record.
// Token and wrapper input are never stored outside the encrypted payload.
type State struct {
	Version            int
	MeshID             string
	InstallationID     string
	InstallationSecret []byte
	Bundle             *Bundle
	ReceivedAt         time.Time
	UsableUntil        time.Time
}

func (state State) Clone() State {
	clone := state
	clone.InstallationSecret = append([]byte(nil), state.InstallationSecret...)
	if state.Bundle != nil {
		bundle := *state.Bundle
		bundle.AccessToken = append([]byte(nil), state.Bundle.AccessToken...)
		bundle.Wrapper = append([]byte(nil), state.Bundle.Wrapper...)
		clone.Bundle = &bundle
	}
	return clone
}

func (state *State) Clear() {
	if state == nil {
		return
	}
	clear(state.InstallationSecret)
	if state.Bundle != nil {
		state.Bundle.Clear()
	}
	*state = State{}
}

type StateStore interface {
	Load(context.Context, []byte, string) (State, bool, error)
	Save(context.Context, []byte, State) error
}

type memoryStateStore struct {
	mu          sync.Mutex
	records     map[[sha256.Size]byte]State
	profiles    map[string]Profile
	forceLeases map[string]struct{}
}

func NewMemoryStateStore() StateStore {
	return &memoryStateStore{records: make(map[[sha256.Size]byte]State), profiles: make(map[string]Profile), forceLeases: make(map[string]struct{})}
}

func (store *memoryStateStore) Load(ctx context.Context, token []byte, meshID string) (State, bool, error) {
	if ctx == nil || ctx.Err() != nil {
		return State{}, false, ErrState
	}
	key, err := stateFingerprint(token, meshID)
	if err != nil {
		return State{}, false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, ok := store.records[key]
	return state.Clone(), ok, nil
}

func (store *memoryStateStore) Save(ctx context.Context, token []byte, state State) error {
	if ctx == nil || ctx.Err() != nil || validateState(token, state) != nil {
		return ErrState
	}
	key, err := stateFingerprint(token, state.MeshID)
	if err != nil {
		return ErrState
	}
	store.mu.Lock()
	if old, ok := store.records[key]; ok {
		old.Clear()
	}
	store.records[key] = state.Clone()
	store.mu.Unlock()
	return nil
}

type fileStateStore struct {
	random io.Reader
	// profileAtomicWrite is a package-private I/O seam for post-rename fault tests.
	// Production stores leave it nil and use atomicPrivateWrite.
	profileAtomicWrite func(context.Context, string, string, []byte, io.Reader) error
}

// NewProductionStateStore resolves its root lazily so Core construction stays
// free of authentication state and I/O. TokenLogin fails closed if the root
// cannot be made private and durable.
func NewProductionStateStore() StateStore { return &fileStateStore{random: rand.Reader} }

type stateWire struct {
	Version            int               `json:"version"`
	MeshID             string            `json:"mesh_id"`
	InstallationID     string            `json:"installation_id"`
	InstallationSecret []byte            `json:"installation_secret"`
	Bundle             *cachedBundleWire `json:"bundle,omitempty"`
	ReceivedAt         string            `json:"received_at,omitempty"`
	UsableUntil        string            `json:"usable_until,omitempty"`
}

// cachedBundleWire excludes the enrollment wrapper and display metadata.
// Neither is needed after response validation, and the wrapper may duplicate
// credential material.
type cachedBundleWire struct {
	Version                       string `json:"version"`
	OrganizationID                string `json:"organization_id,omitempty"`
	InstallationID                string `json:"installation_id"`
	AgentID                       string `json:"agent_id"`
	AgentJID                      string `json:"agent_jid,omitempty"`
	MeshID                        string `json:"mesh_id"`
	SessionResource               string `json:"session_resource"`
	MeshEndpoint                  string `json:"mesh_endpoint"`
	Username                      string `json:"username"`
	Server                        string `json:"server"`
	AccessToken                   []byte `json:"access_token"`
	ExpiresIn                     int64  `json:"expires_in"`
	InstallationEpoch             int64  `json:"installation_epoch,omitempty"`
	AttachmentRevision            int64  `json:"attachment_revision,omitempty"`
	TokenID                       string `json:"token_id,omitempty"`
	PolicyRevision                int64  `json:"policy_revision,omitempty"`
	SessionExpiryMode             string `json:"session_expiry_mode,omitempty"`
	OfflineColdStartTargetSeconds int64  `json:"offline_cold_start_target_seconds,omitempty"`
}

func (store *fileStateStore) Load(ctx context.Context, token []byte, meshID string) (State, bool, error) {
	if ctx == nil || ctx.Err() != nil {
		return State{}, false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return State{}, false, ErrState
	}
	fingerprint, err := stateFingerprint(token, meshID)
	if err != nil {
		return State{}, false, ErrState
	}
	path := filepath.Join(root, hex.EncodeToString(fingerprint[:])+stateFileSuffix)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return State{}, false, ErrState
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return State{}, false, ErrState
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maximumStateBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) > maximumStateBytes {
		clear(data)
		return State{}, false, ErrState
	}
	defer clear(data)
	plaintext, err := openState(token, meshID, data)
	if err != nil {
		return State{}, false, ErrState
	}
	defer clear(plaintext)
	state, err := decodeState(plaintext)
	if err != nil || validateState(token, state) != nil || state.MeshID != meshID {
		state.Clear()
		return State{}, false, ErrState
	}
	return state, true, nil
}

func (store *fileStateStore) Save(ctx context.Context, token []byte, state State) error {
	if store == nil || ctx == nil || ctx.Err() != nil || validateState(token, state) != nil {
		return ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return ErrState
	}
	fingerprint, err := stateFingerprint(token, state.MeshID)
	if err != nil {
		return ErrState
	}
	destination := filepath.Join(root, hex.EncodeToString(fingerprint[:])+stateFileSuffix)
	if info, statErr := os.Lstat(destination); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			return ErrState
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return ErrState
	}
	plaintext, err := encodeState(state)
	if err != nil {
		return ErrState
	}
	defer clear(plaintext)
	sealed, err := sealState(store.random, token, state.MeshID, plaintext)
	if err != nil || len(sealed) > maximumStateBytes {
		clear(sealed)
		return ErrState
	}
	defer clear(sealed)
	var suffix [8]byte
	if _, err = io.ReadFull(store.random, suffix[:]); err != nil {
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
	if _, err = file.Write(sealed); err != nil || ctx.Err() != nil || file.Sync() != nil || file.Close() != nil {
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

func secureStateRoot() (string, error) {
	root := os.Getenv(stateEnvironment)
	if root == "" {
		base, err := os.UserConfigDir()
		if err != nil || base == "" {
			return "", ErrState
		}
		root = filepath.Join(base, filepath.FromSlash(stateDirectoryName))
	} else if !filepath.IsAbs(root) {
		return "", ErrState
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", ErrState
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", ErrState
	}
	return root, nil
}

func validateState(token []byte, state State) error {
	request := Request{Token: token, MeshID: state.MeshID, InstallationID: state.InstallationID, InstallationSecret: state.InstallationSecret}
	if state.Version != stateVersion || !validRequest(request) {
		return ErrState
	}
	if state.Bundle == nil {
		if !state.ReceivedAt.IsZero() || !state.UsableUntil.IsZero() {
			return ErrState
		}
		return nil
	}
	validBundle := ValidateBundle(request, *state.Bundle)
	if len(state.Bundle.Wrapper) == 0 {
		validBundle = validateCachedBundle(state.MeshID, state.InstallationID, *state.Bundle)
	}
	if !validBundle || state.ReceivedAt.IsZero() || state.UsableUntil.IsZero() ||
		state.ReceivedAt.Location() != time.UTC || state.UsableUntil.Location() != time.UTC {
		return ErrState
	}
	want, err := UsableUntil(state.Bundle.AccessToken, state.ReceivedAt, state.Bundle.ExpiresIn)
	if err != nil || !want.Equal(state.UsableUntil) {
		return ErrState
	}
	return nil
}

func validateCachedBundle(meshID, installationID string, bundle Bundle) bool {
	if len(bundle.Wrapper) != 0 || bundle.Display != "" {
		return false
	}
	copy := bundle
	copy.Wrapper = []byte("aztm_cached")
	valid := validateBundleExpected(meshID, installationID, copy)
	clear(copy.Wrapper)
	return valid
}

func encodeState(state State) ([]byte, error) {
	var cached *cachedBundleWire
	if state.Bundle != nil {
		cached = &cachedBundleWire{
			Version: state.Bundle.Version, OrganizationID: state.Bundle.OrganizationID, InstallationID: state.Bundle.InstallationID,
			AgentID: state.Bundle.AgentID, AgentJID: state.Bundle.AgentJID, MeshID: state.Bundle.MeshID,
			SessionResource: state.Bundle.SessionResource, MeshEndpoint: state.Bundle.MeshEndpoint,
			Username: state.Bundle.Username, Server: state.Bundle.Server,
			AccessToken: state.Bundle.AccessToken, ExpiresIn: state.Bundle.ExpiresIn,
			InstallationEpoch: state.Bundle.InstallationEpoch, AttachmentRevision: state.Bundle.AttachmentRevision,
			TokenID: state.Bundle.TokenID, PolicyRevision: state.Bundle.PolicyRevision, SessionExpiryMode: state.Bundle.SessionExpiryMode,
			OfflineColdStartTargetSeconds: state.Bundle.OfflineColdStartTargetSeconds,
		}
	}
	wire := stateWire{Version: state.Version, MeshID: state.MeshID, InstallationID: state.InstallationID,
		InstallationSecret: state.InstallationSecret, Bundle: cached}
	if !state.ReceivedAt.IsZero() {
		wire.ReceivedAt = state.ReceivedAt.Format(time.RFC3339Nano)
		wire.UsableUntil = state.UsableUntil.Format(time.RFC3339Nano)
	}
	return json.Marshal(wire)
}

func decodeState(data []byte) (State, error) {
	if len(data) == 0 || len(data) > maximumStateBytes || rejectDuplicateJSONKeys(data) != nil {
		return State{}, ErrState
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire stateWire
	if err := decoder.Decode(&wire); err != nil {
		return State{}, ErrState
	}
	if _, err := decoder.Token(); err != io.EOF {
		return State{}, ErrState
	}
	state := State{Version: wire.Version, MeshID: wire.MeshID, InstallationID: wire.InstallationID,
		InstallationSecret: wire.InstallationSecret}
	if wire.Bundle != nil {
		state.Bundle = &Bundle{
			Version: wire.Bundle.Version, OrganizationID: wire.Bundle.OrganizationID, InstallationID: wire.Bundle.InstallationID,
			AgentID: wire.Bundle.AgentID, AgentJID: wire.Bundle.AgentJID, MeshID: wire.Bundle.MeshID,
			SessionResource: wire.Bundle.SessionResource, MeshEndpoint: wire.Bundle.MeshEndpoint,
			Username: wire.Bundle.Username, Server: wire.Bundle.Server,
			AccessToken: wire.Bundle.AccessToken, ExpiresIn: wire.Bundle.ExpiresIn,
			InstallationEpoch: wire.Bundle.InstallationEpoch, AttachmentRevision: wire.Bundle.AttachmentRevision,
			TokenID: wire.Bundle.TokenID, PolicyRevision: wire.Bundle.PolicyRevision, SessionExpiryMode: wire.Bundle.SessionExpiryMode,
			OfflineColdStartTargetSeconds: wire.Bundle.OfflineColdStartTargetSeconds,
		}
	}
	var err error
	if wire.ReceivedAt != "" {
		state.ReceivedAt, err = time.Parse(time.RFC3339Nano, wire.ReceivedAt)
		if err == nil {
			state.UsableUntil, err = time.Parse(time.RFC3339Nano, wire.UsableUntil)
		}
		state.ReceivedAt = state.ReceivedAt.UTC()
		state.UsableUntil = state.UsableUntil.UTC()
	} else if wire.UsableUntil != "" {
		err = ErrState
	}
	if err != nil {
		state.Clear()
		return State{}, ErrState
	}
	return state, nil
}

func stateFingerprint(token []byte, meshID string) ([sha256.Size]byte, error) {
	if !ValidTokenBody(token) || meshID == "" || len(meshID) > 255 {
		return [sha256.Size]byte{}, ErrState
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(stateFingerprintDomain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(token)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(meshID))
	var output [sha256.Size]byte
	copy(output[:], hash.Sum(nil))
	return output, nil
}

func stateKey(token []byte) ([sha256.Size]byte, error) {
	if !ValidTokenBody(token) {
		return [sha256.Size]byte{}, ErrState
	}
	mac := hmac.New(sha256.New, token)
	_, _ = mac.Write([]byte(stateKeyDomain))
	var key [sha256.Size]byte
	copy(key[:], mac.Sum(nil))
	return key, nil
}

func sealState(random io.Reader, token []byte, meshID string, plaintext []byte) ([]byte, error) {
	key, err := stateKey(token)
	if err != nil || random == nil {
		return nil, ErrState
	}
	defer clear(key[:])
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, ErrState
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrState
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(random, nonce); err != nil {
		clear(nonce)
		return nil, ErrState
	}
	fingerprint, _ := stateFingerprint(token, meshID)
	header := append(append([]byte(nil), stateMagic[:]...), byte(stateVersion))
	aad := append(append([]byte(nil), header...), fingerprint[:]...)
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	output := make([]byte, 0, len(header)+len(nonce)+len(sealed))
	output = append(output, header...)
	output = append(output, nonce...)
	output = append(output, sealed...)
	clear(nonce)
	clear(sealed)
	clear(aad)
	return output, nil
}

func openState(token []byte, meshID string, encoded []byte) ([]byte, error) {
	key, err := stateKey(token)
	if err != nil {
		return nil, ErrState
	}
	defer clear(key[:])
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, ErrState
	}
	gcm, err := cipher.NewGCM(block)
	headerSize := len(stateMagic) + 1
	if err != nil || len(encoded) < headerSize+gcm.NonceSize()+gcm.Overhead() || !bytes.Equal(encoded[:len(stateMagic)], stateMagic[:]) || encoded[len(stateMagic)] != stateVersion {
		return nil, ErrState
	}
	fingerprint, _ := stateFingerprint(token, meshID)
	aad := append(append([]byte(nil), encoded[:headerSize]...), fingerprint[:]...)
	plaintext, err := gcm.Open(nil, encoded[headerSize:headerSize+gcm.NonceSize()], encoded[headerSize+gcm.NonceSize():], aad)
	clear(aad)
	if err != nil {
		clear(plaintext)
		return nil, ErrState
	}
	return plaintext, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrState
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrState
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrState
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrState
			}
			if _, exists := seen[key]; exists {
				return ErrState
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return ErrState
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return ErrState
		}
	default:
		return ErrState
	}
	return nil
}
