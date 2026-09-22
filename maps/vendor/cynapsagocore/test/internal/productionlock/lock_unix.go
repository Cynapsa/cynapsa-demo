//go:build darwin || linux

// Package productionlock serializes repository tests that own real Docker
// service environments. Package-level Go test parallelism must not make two
// qualification suites compete for one host Docker daemon.
package productionlock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Acquire waits boundedly for the host-wide Cynapsa production-test lock.
// The kernel releases the lock automatically if a test process exits.
func Acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("production test lock: nil context")
	}
	file, err := os.OpenFile(filepath.Join(os.TempDir(), "cynapsa-production-docker.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			var released bool
			return func() {
				if released {
					return
				}
				released = true
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return nil, ctx.Err()
		}
	}
}
