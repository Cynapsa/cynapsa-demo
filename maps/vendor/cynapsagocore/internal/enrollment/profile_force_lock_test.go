package enrollment

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func privateProfileTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	return root
}

func assertForceLock(t *testing.T, store ProfileStore, profileID string, want bool) func() {
	t.Helper()
	release, acquired, err := store.TryLockForceProfile(context.Background(), profileID)
	if err != nil || acquired != want || (acquired && release == nil) || (!acquired && release != nil) {
		t.Fatalf("TryLockForceProfile(%q): acquired=%v err=%v, want=%v", profileID, acquired, err, want)
	}
	return release
}

func TestMemoryForceProfileLeaseIsPerProfile(t *testing.T) {
	store := NewMemoryStateStore().(ProfileStore)
	releaseA := assertForceLock(t, store, "profile-a", true)
	assertForceLock(t, store, "profile-a", false)
	releaseB := assertForceLock(t, store, "profile-b", true)
	if _, _, err := store.TryLockForceProfile(nil, "profile-c"); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, _, err := store.TryLockForceProfile(context.Background(), "../bad"); err == nil {
		t.Fatal("invalid profile ID accepted")
	}
	releaseA()
	releaseA() // release is safe to call more than once
	releaseA2 := assertForceLock(t, store, "profile-a", true)
	assertForceLock(t, store, "profile-b", false)
	releaseA2()
	releaseB()
	assertForceLock(t, store, "profile-b", true)()
}

func TestFileForceProfileLeaseAcrossInstances(t *testing.T) {
	root := privateProfileTestRoot(t)
	first := NewProductionStateStore().(ProfileStore)
	second := NewProductionStateStore().(ProfileStore)
	releaseA := assertForceLock(t, first, "profile-a", true)
	start := time.Now()
	assertForceLock(t, second, "profile-a", false)
	if time.Since(start) > time.Second {
		t.Fatal("contended lease did not fail promptly")
	}
	releaseB := assertForceLock(t, second, "profile-b", true)
	if _, _, err := first.TryLockForceProfile(context.Background(), "../bad"); err == nil {
		t.Fatal("invalid profile ID accepted")
	}
	if err := first.SaveProfile(context.Background(), atomicTestProfile(profileIDOne)); err != nil {
		t.Fatalf("profile writer lock was blocked by operation lease: %v", err)
	}
	path, err := forceProfileLockPath(root, "profile-a")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("force lock file is not private regular file: err=%v", err)
	}
	releaseA()
	releaseA()
	releaseA2 := assertForceLock(t, second, "profile-a", true)
	assertForceLock(t, first, "profile-a", false)
	releaseA2()
	releaseB()
	assertForceLock(t, first, "profile-b", true)()
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("release removed lock file")
	}
}

func TestFileForceProfileLeaseRejectsUnsafeLockFile(t *testing.T) {
	root := privateProfileTestRoot(t)
	store := NewProductionStateStore().(ProfileStore)
	path, err := forceProfileLockPath(root, "unsafe")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := store.TryLockForceProfile(context.Background(), "unsafe"); err == nil || acquired {
		t.Fatal("world-readable lock file accepted")
	}
	if runtime.GOOS == "windows" {
		return
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := store.TryLockForceProfile(context.Background(), "unsafe"); err == nil || acquired {
		t.Fatal("symlink lock file accepted")
	}
}

func TestFileForceProfileLeaseAcrossProcesses(t *testing.T) {
	if profileID := os.Getenv("CYNAPSA_FORCE_LOCK_CHILD_PROFILE"); profileID != "" {
		store := NewProductionStateStore().(ProfileStore)
		release, acquired, err := store.TryLockForceProfile(context.Background(), profileID)
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately exit without release when acquired: the OS must release
		// the lock with the process handle.
		_ = release
		fmt.Print(acquired)
		os.Exit(0)
	}
	privateProfileTestRoot(t)
	store := NewProductionStateStore().(ProfileStore)
	release := assertForceLock(t, store, "same", true)
	child := func(profileID string) string {
		t.Helper()
		command := exec.Command(os.Args[0], "-test.run=^TestFileForceProfileLeaseAcrossProcesses$")
		command.Env = append(os.Environ(), "CYNAPSA_FORCE_LOCK_CHILD_PROFILE="+profileID)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("force-lock child: %v: %s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	if got := child("same"); got != "false" {
		t.Fatalf("held profile: child acquired=%q", got)
	}
	if got := child("different"); got != "true" {
		t.Fatalf("different profile: child acquired=%q", got)
	}
	release()
	if got := child("same"); got != "true" {
		t.Fatalf("released profile: child acquired=%q", got)
	}
	assertForceLock(t, store, "same", true)()
}
