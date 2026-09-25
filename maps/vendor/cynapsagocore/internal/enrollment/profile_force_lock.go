package enrollment

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
)

func forceProfileLockPath(root, profileID string) (string, error) {
	if !ValidProfileID(profileID) {
		return "", ErrState
	}
	fingerprint, err := profileFingerprint(profileID)
	if err != nil {
		return "", ErrState
	}
	return filepath.Join(root, forceLockFilePrefix+hex.EncodeToString(fingerprint[:])+".lock"), nil
}

func (store *memoryStateStore) TryLockForceProfile(ctx context.Context, profileID string) (func(), bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !ValidProfileID(profileID) {
		return nil, false, ErrState
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if ctx.Err() != nil {
		return nil, false, ErrState
	}
	if _, held := store.forceLeases[profileID]; held {
		return nil, false, nil
	}
	if store.forceLeases == nil {
		store.forceLeases = make(map[string]struct{})
	}
	store.forceLeases[profileID] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			store.mu.Lock()
			delete(store.forceLeases, profileID)
			store.mu.Unlock()
		})
	}, true, nil
}

func (store *fileStateStore) TryLockForceProfile(ctx context.Context, profileID string) (func(), bool, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || !ValidProfileID(profileID) {
		return nil, false, ErrState
	}
	root, err := secureStateRoot()
	if err != nil {
		return nil, false, ErrState
	}
	path, err := forceProfileLockPath(root, profileID)
	if err != nil {
		return nil, false, ErrState
	}
	// This is deliberately separate from profileLockFile: the lease may span
	// enrollment network I/O without blocking profile reads or writes.
	release, acquired, err := tryLockForceProfileFile(path)
	if err != nil || !acquired {
		return nil, acquired, err
	}
	if ctx.Err() != nil {
		release()
		return nil, false, ErrState
	}
	var once sync.Once
	return func() { once.Do(release) }, true, nil
}

// validateForceLockFile checks the opened inode against the path. Lock files
// must never be removed on release: another process may still hold that inode.
func validateForceLockFile(path string, file *os.File) error {
	opened, openErr := file.Stat()
	linked, linkErr := os.Lstat(path)
	if openErr != nil || linkErr != nil || !opened.Mode().IsRegular() || !linked.Mode().IsRegular() ||
		opened.Mode().Perm() != 0o600 || linked.Mode().Perm() != 0o600 || !os.SameFile(opened, linked) {
		return ErrState
	}
	return nil
}
