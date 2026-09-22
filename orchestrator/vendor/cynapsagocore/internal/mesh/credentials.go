package mesh

import (
	"fmt"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const MaxCredentialBytes = 4 << 10

type CredentialInput struct {
	MeshID   string
	Username string
	Password []byte
}

func (input CredentialInput) String() string {
	return fmt.Sprintf("mesh.CredentialInput{MeshID:%q, Username:%q, Password:[REDACTED]}", input.MeshID, input.Username)
}

func (input CredentialInput) GoString() string { return input.String() }

type sessionCredential struct {
	meshID   string
	username string
	password []byte
}

// CredentialStore owns exactly one process/login-session credential. Password
// bytes are usable only through WithPassword and never become message proof.
type CredentialStore struct {
	mu     sync.Mutex
	active *sessionCredential
}

func NewCredentialStore() *CredentialStore { return &CredentialStore{} }

func (store *CredentialStore) String() string {
	if store == nil {
		return "mesh.CredentialStore{configured:false}"
	}
	store.mu.Lock()
	configured := store.active != nil
	store.mu.Unlock()
	return fmt.Sprintf("mesh.CredentialStore{configured:%t}", configured)
}

func (store *CredentialStore) GoString() string { return store.String() }

// Replace atomically installs one scoped session credential and clears the old
// private byte storage. Input bytes are copied.
func (store *CredentialStore) Replace(input CredentialInput) error {
	if store == nil || protocol.ValidateMeshID(input.MeshID) != nil || protocol.ValidateAgentIdentity(input.Username) != nil || len(input.Password) == 0 || len(input.Password) > MaxCredentialBytes {
		return ErrInvalidCredential
	}
	next := &sessionCredential{meshID: input.MeshID, username: input.Username, password: append([]byte(nil), input.Password...)}
	store.mu.Lock()
	old := store.active
	store.active = next
	store.mu.Unlock()
	clearCredential(old)
	return nil
}

// WithPassword supplies a temporary owned copy to a scoped authentication
// callback and clears that copy on return. No getter returns reusable material.
func (store *CredentialStore) WithPassword(meshID string, authenticate func(username string, password []byte) error) error {
	if store == nil || authenticate == nil {
		return ErrInvalidCredential
	}
	store.mu.Lock()
	if store.active == nil || store.active.meshID != meshID {
		store.mu.Unlock()
		return ErrCredentialMissing
	}
	username := store.active.username
	password := append([]byte(nil), store.active.password...)
	store.mu.Unlock()
	defer clear(password)
	if err := authenticate(username, password); err != nil {
		return ErrInvalidCredential
	}
	return nil
}

func (store *CredentialStore) Remove(meshID string) error {
	if store == nil {
		return ErrCredentialMissing
	}
	store.mu.Lock()
	if store.active == nil || store.active.meshID != meshID {
		store.mu.Unlock()
		return ErrCredentialMissing
	}
	old := store.active
	store.active = nil
	store.mu.Unlock()
	clearCredential(old)
	return nil
}

func clearCredential(value *sessionCredential) {
	if value == nil {
		return
	}
	clear(value.password)
	value.username = ""
	value.meshID = ""
}
