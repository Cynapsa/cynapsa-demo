//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package enrollment

// Unsupported platforms fail closed because v2 persistence requires an
// inter-process writer lock, not merely an in-process mutex.
func lockProfileRoot(string) (func(), error) { return nil, ErrState }

func tryLockForceProfileFile(string) (func(), bool, error) { return nil, false, ErrState }
