//go:build windows

package enrollment

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func lockProfileRoot(root string) (func(), error) {
	file, err := os.OpenFile(filepath.Join(root, profileLockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, ErrState
	}
	overlapped := new(windows.Overlapped)
	if err = windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, overlapped); err != nil {
		_ = file.Close()
		return nil, ErrState
	}
	return func() {
		_ = windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped)
		_ = file.Close()
	}, nil
}
