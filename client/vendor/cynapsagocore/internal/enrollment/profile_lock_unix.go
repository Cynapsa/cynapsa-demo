//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package enrollment

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockProfileRoot(root string) (func(), error) {
	file, err := os.OpenFile(filepath.Join(root, profileLockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil || unix.Flock(int(file.Fd()), unix.LOCK_EX) != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, ErrState
	}
	return func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}

func tryLockForceProfileFile(path string) (func(), bool, error) {
	// O_NOFOLLOW prevents an existing symlink from redirecting the lease to
	// another inode. The file is intentionally retained after unlocking.
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, false, ErrState
	}
	file := os.NewFile(uintptr(fd), path)
	if err := validateForceLockFile(path, file); err != nil {
		_ = file.Close()
		return nil, false, ErrState
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, ErrState
	}
	if err := validateForceLockFile(path, file); err != nil {
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = file.Close()
		return nil, false, ErrState
	}
	return func() {
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = file.Close()
	}, true, nil
}
