//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package enrollment

import (
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
