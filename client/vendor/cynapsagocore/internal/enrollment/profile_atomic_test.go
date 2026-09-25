package enrollment

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

const (
	profileIDOne   = "11111111-1111-4111-9111-111111111111"
	profileIDTwo   = "22222222-2222-4222-a222-222222222222"
	profileIDThree = "33333333-3333-4333-a333-333333333333"
	profileSecret  = "ERERERERERERERERERERERERERERERERERERERERERE"
)

func atomicTestProfile(id string) Profile {
	return Profile{Version: ProfileStateVersion, ProfileID: "active", InstallationID: id,
		InstallationSecret: []byte(profileSecret)}
}

func TestProfileAtomicPrimitives(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) ProfileStore
	}{
		{"memory", func(*testing.T) ProfileStore { return NewMemoryStateStore().(ProfileStore) }},
		{"file", func(t *testing.T) ProfileStore {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv(stateEnvironment, root)
			return NewProductionStateStore().(ProfileStore)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)
			first := atomicTestProfile(profileIDOne)
			second := atomicTestProfile(profileIDTwo)
			third := atomicTestProfile(profileIDThree)

			got, created, err := store.LoadOrCreateProfile(ctx, first)
			if err != nil || !created || got.InstallationID != first.InstallationID {
				t.Fatalf("initial load-or-create: created=%v err=%v", created, err)
			}
			clear(got.InstallationSecret)
			got.Clear()
			got, created, err = store.LoadOrCreateProfile(ctx, second)
			if err != nil || created || got.InstallationID != first.InstallationID || !bytes.Equal(got.InstallationSecret, first.InstallationSecret) {
				t.Fatalf("existing profile: created=%v err=%v", created, err)
			}
			got.Clear()

			if swapped, err := store.CompareAndSwapProfile(ctx, "active", "", second); err != nil || swapped {
				t.Fatalf("absent expectation must fail against existing: swapped=%v err=%v", swapped, err)
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, "active", profileIDThree, second); err != nil || swapped {
				t.Fatalf("stale expectation must fail: swapped=%v err=%v", swapped, err)
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, "active", profileIDOne, second); err != nil || !swapped {
				t.Fatalf("matching swap: swapped=%v err=%v", swapped, err)
			}
			clear(second.InstallationSecret)
			second.InstallationSecret = []byte(profileSecret)
			got, found, err := store.LoadProfile(ctx, "active")
			if err != nil || !found || !bytes.Equal(got.InstallationSecret, second.InstallationSecret) {
				t.Fatalf("swap retained caller-owned secret: found=%v err=%v", found, err)
			}
			got.Clear()
			if swapped, err := store.CompareAndSwapProfile(ctx, "active", profileIDOne, third); err != nil || swapped {
				t.Fatalf("old identity must not swap twice: swapped=%v err=%v", swapped, err)
			}
			if deleted, err := store.CompareAndDeleteProfile(ctx, "active", profileIDOne); err != nil || deleted {
				t.Fatalf("stale delete: deleted=%v err=%v", deleted, err)
			}
			if deleted, err := store.CompareAndDeleteProfile(ctx, "active", profileIDTwo); err != nil || !deleted {
				t.Fatalf("matching delete: deleted=%v err=%v", deleted, err)
			}
			if got, found, err := store.LoadProfile(ctx, "active"); err != nil || found {
				got.Clear()
				t.Fatalf("deleted profile still present: found=%v err=%v", found, err)
			}
			if deleted, err := store.CompareAndDeleteProfile(ctx, "active", ""); err != nil || deleted {
				t.Fatalf("delete of absent profile: deleted=%v err=%v", deleted, err)
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, "active", "", third); err != nil || !swapped {
				t.Fatalf("absent expectation must create: swapped=%v err=%v", swapped, err)
			}
			got, found, err = store.LoadProfile(ctx, "active")
			if err != nil || !found || got.InstallationID != profileIDThree {
				t.Fatalf("final profile: found=%v err=%v", found, err)
			}
			got.Clear()
		})
	}
}

func TestProfileAtomicRejectsInvalidAndCancelledInput(t *testing.T) {
	for _, factory := range []func(*testing.T) ProfileStore{
		func(*testing.T) ProfileStore { return NewMemoryStateStore().(ProfileStore) },
		func(t *testing.T) ProfileStore {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv(stateEnvironment, root)
			return NewProductionStateStore().(ProfileStore)
		},
	} {
		t.Run(fmt.Sprintf("store-%p", factory), func(t *testing.T) {
			store := factory(t)
			ctx := context.Background()
			candidate := atomicTestProfile(profileIDOne)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, _, err := store.LoadOrCreateProfile(cancelled, candidate); err == nil {
				t.Fatal("cancelled load-or-create succeeded")
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, "other", "", candidate); err == nil || swapped {
				t.Fatal("mismatched replacement profile ID accepted")
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, "active", "not-a-uuid", candidate); err == nil || swapped {
				t.Fatal("invalid expected installation ID accepted")
			}
			if deleted, err := store.CompareAndDeleteProfile(cancelled, "active", ""); err == nil || deleted {
				t.Fatal("cancelled delete succeeded")
			}
			if got, found, err := store.LoadProfile(ctx, "active"); err != nil || found {
				got.Clear()
				t.Fatalf("invalid operations changed state: found=%v err=%v", found, err)
			}
		})
	}
}

func TestFileProfileAtomicRejectsCorruptExistingProfile(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	store := NewProductionStateStore().(ProfileStore)
	ctx := context.Background()
	first := atomicTestProfile(profileIDOne)
	if err := store.SaveProfile(ctx, first); err != nil {
		t.Fatal(err)
	}
	path, err := profilePath(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, created, err := store.LoadOrCreateProfile(ctx, atomicTestProfile(profileIDTwo)); err == nil || created {
		got.Clear()
		t.Fatal("load-or-create overwrote corrupt profile")
	}
	if swapped, err := store.CompareAndSwapProfile(ctx, "active", profileIDOne, atomicTestProfile(profileIDTwo)); err == nil || swapped {
		t.Fatal("compare-and-swap overwrote corrupt profile")
	}
	if deleted, err := store.CompareAndDeleteProfile(ctx, "active", profileIDOne); err == nil || deleted {
		t.Fatal("compare-and-delete removed corrupt profile")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, []byte("corrupt")) {
		t.Fatal("corrupt profile unexpectedly changed")
	}
}

func TestFileProfileVerifiedReadbackResolvesVisibleOutcome(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	store := NewProductionStateStore().(ProfileStore)
	profile := atomicTestProfile(profileIDOne)
	if err := store.SaveProfile(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	key, err := loadOrCreateProfileKey(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	if !verifiedProfileWrite(root, key, profile) {
		t.Fatal("matching readback was not verified")
	}
	if verifiedProfileWrite(root, key, atomicTestProfile(profileIDTwo)) {
		t.Fatal("different installation was incorrectly verified")
	}
	if verifiedProfileDelete(root, key, "active") {
		t.Fatal("existing profile was incorrectly verified as deleted")
	}
	if deleted, err := store.CompareAndDeleteProfile(context.Background(), "active", profileIDOne); err != nil || !deleted {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if !verifiedProfileDelete(root, key, "active") {
		t.Fatal("absent readback was not verified")
	}
}

func TestFileProfileAtomicAcrossProcesses(t *testing.T) {
	if operation := os.Getenv("CYNAPSA_PROFILE_ATOMIC_CHILD"); operation != "" {
		store := NewProductionStateStore().(ProfileStore)
		ctx := context.Background()
		candidate := atomicTestProfile(os.Getenv("CYNAPSA_PROFILE_ATOMIC_ID"))
		switch operation {
		case "create":
			profile, created, err := store.LoadOrCreateProfile(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			profile.Clear()
			fmt.Print(created)
		case "swap":
			swapped, err := store.CompareAndSwapProfile(ctx, "active", profileIDOne, candidate)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Print(swapped)
		default:
			t.Fatalf("unknown child operation %q", operation)
		}
		os.Exit(0)
	}

	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(stateEnvironment, root)
	for _, operation := range []string{"create", "swap"} {
		if operation == "swap" {
			store := NewProductionStateStore().(ProfileStore)
			if err := store.SaveProfile(context.Background(), atomicTestProfile(profileIDOne)); err != nil {
				t.Fatal(err)
			}
		}
		var group sync.WaitGroup
		results := make(chan string, 2)
		for _, id := range []string{profileIDTwo, profileIDThree} {
			group.Add(1)
			go func(id string) {
				defer group.Done()
				command := exec.Command(os.Args[0], "-test.run=^TestFileProfileAtomicAcrossProcesses$")
				command.Env = append(os.Environ(), "CYNAPSA_PROFILE_ATOMIC_CHILD="+operation, "CYNAPSA_PROFILE_ATOMIC_ID="+id)
				output, err := command.CombinedOutput()
				if err != nil {
					results <- fmt.Sprintf("error: %v: %s", err, output)
					return
				}
				results <- strings.TrimSpace(string(output))
			}(id)
		}
		group.Wait()
		close(results)
		wins := 0
		for result := range results {
			switch result {
			case "true":
				wins++
			case "false":
			default:
				t.Fatalf("%s child result: %q", operation, result)
			}
		}
		if wins != 1 {
			t.Fatalf("%s had %d winners, want one", operation, wins)
		}
		profile, found, err := NewProductionStateStore().(ProfileStore).LoadProfile(context.Background(), "active")
		if err != nil || !found {
			t.Fatalf("%s final read: found=%v err=%v", operation, found, err)
		}
		if operation == "swap" && profile.InstallationID == profileIDOne {
			t.Fatal("swap did not persist winner")
		}
		profile.Clear()
	}
}
