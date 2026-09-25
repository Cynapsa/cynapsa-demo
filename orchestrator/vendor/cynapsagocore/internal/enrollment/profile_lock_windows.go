//go:build windows

package enrollment

import (
	"errors"
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

func tryLockForceProfileFile(path string) (func(), bool, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, false, ErrState
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, false, ErrState
	}
	file := os.NewFile(uintptr(handle), path)
	if err := validateForceLockFile(path, file); err != nil {
		_ = file.Close()
		return nil, false, ErrState
	}
	overlapped := new(windows.Overlapped)
	err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if err != nil {
		_ = file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, false, nil
		}
		return nil, false, ErrState
	}
	if err := validateForceLockFile(path, file); err != nil {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = file.Close()
		return nil, false, ErrState
	}
	return func() {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = file.Close()
	}, true, nil
}
