package enrollment

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestSwapProfileWithPreviousReturnsLatestDisplacedProfile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stores func(*testing.T) (ProfileStore, ProfileStore)
	}{
		{"memory", func(*testing.T) (ProfileStore, ProfileStore) {
			store := NewMemoryStateStore().(ProfileStore)
			return store, store
		}},
		{"file", func(t *testing.T) (ProfileStore, ProfileStore) {
			privateProfileTestRoot(t)
			return NewProductionStateStore().(ProfileStore), NewProductionStateStore().(ProfileStore)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, updater := tc.stores(t)
			initial := atomicTestProfile(profileIDOne)
			if err := store.SaveProfile(ctx, initial); err != nil {
				t.Fatal(err)
			}
			stale, found, err := store.LoadProfile(ctx, "active")
			if err != nil || !found {
				t.Fatalf("stale read: found=%v err=%v", found, err)
			}
			defer stale.Clear()

			now := time.Now().UTC().Truncate(time.Second)
			updated := initial.Clone()
			updated.AgentID = profileIDTwo
			updated.Credentials = map[string]Credential{
				"mesh-one": {Bundle: profileTestBundle(now, profileIDOne, "mesh-one"),
					ReceivedAt: now, UsableUntil: now.Add(36 * time.Hour)},
			}
			expectedPrevious := updated.Clone()
			defer expectedPrevious.Clear()
			if profilesEqual(stale, expectedPrevious) {
				t.Fatal("test update did not differ from stale snapshot")
			}
			// The updater can commit under the same installation ID after the
			// caller's old snapshot, but before its force-publication CAS.
			updateDone := make(chan error, 1)
			go func() { updateDone <- updater.SaveProfile(ctx, updated) }()
			if err := <-updateDone; err != nil {
				t.Fatal(err)
			}
			updated.Clear()

			replacement := atomicTestProfile(profileIDThree)
			previous, swapped, err := store.SwapProfileWithPrevious(ctx, "active", profileIDOne, replacement)
			if err != nil || !swapped || !profilesEqual(previous, expectedPrevious) {
				previous.Clear()
				t.Fatalf("swap did not return latest displaced profile: swapped=%v err=%v", swapped, err)
			}
			if profilesEqual(previous, stale) {
				previous.Clear()
				t.Fatal("swap returned stale caller snapshot")
			}
			got, found, err := store.LoadProfile(ctx, "active")
			if err != nil || !found || !profilesEqual(got, replacement) {
				got.Clear()
				previous.Clear()
				t.Fatalf("replacement not persisted: found=%v err=%v", found, err)
			}
			got.Clear()

			// Returned secrets and credentials are owned by the caller.
			previous.Clear()
			if !validCanonicalSecret(expectedPrevious.InstallationSecret) ||
				len(expectedPrevious.Credentials["mesh-one"].Bundle.AccessToken) == 0 {
				t.Fatal("returned previous profile aliased caller's update")
			}
		})
	}
}

func TestSwapProfileWithPreviousAbsentMismatchAndError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) ProfileStore
	}{
		{"memory", func(*testing.T) ProfileStore { return NewMemoryStateStore().(ProfileStore) }},
		{"file", func(t *testing.T) ProfileStore {
			privateProfileTestRoot(t)
			return NewProductionStateStore().(ProfileStore)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)
			first := atomicTestProfile(profileIDOne)
			second := atomicTestProfile(profileIDTwo)
			previous, swapped, err := store.SwapProfileWithPrevious(ctx, "active", profileIDOne, first)
			if err != nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("nonempty expectation matched absence: swapped=%v err=%v", swapped, err)
			}
			previous, swapped, err = store.SwapProfileWithPrevious(ctx, "active", "", first)
			if err != nil || !swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("absent swap: swapped=%v err=%v", swapped, err)
			}
			previous, swapped, err = store.SwapProfileWithPrevious(ctx, "active", "", second)
			if err != nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("absent expectation matched existing: swapped=%v err=%v", swapped, err)
			}
			previous, swapped, err = store.SwapProfileWithPrevious(ctx, "active", profileIDThree, second)
			if err != nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("stale expectation matched existing: swapped=%v err=%v", swapped, err)
			}
			previous, swapped, err = store.SwapProfileWithPrevious(ctx, "wrong-profile", profileIDOne, second)
			if err == nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("invalid replacement returned previous: swapped=%v err=%v", swapped, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			previous, swapped, err = store.SwapProfileWithPrevious(cancelled, "active", profileIDOne, second)
			if err == nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
				t.Fatalf("cancelled swap returned previous: swapped=%v err=%v", swapped, err)
			}
		})
	}
}

func TestFileSwapProfileWithPreviousFailedWriteReturnsMatchedPrevious(t *testing.T) {
	privateProfileTestRoot(t)
	store := NewProductionStateStore().(ProfileStore)
	first := atomicTestProfile(profileIDOne)
	if err := store.SaveProfile(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	// The key already exists. The reader supplies the nonce but not the
	// temporary-file suffix, so the write fails before rename.
	failing := &fileStateStore{random: bytes.NewReader(make([]byte, 12))}
	previous, swapped, err := failing.SwapProfileWithPrevious(context.Background(), "active", profileIDOne, atomicTestProfile(profileIDTwo))
	if err == nil || swapped || !profilesEqual(previous, first) {
		t.Fatalf("failed write did not return matched previous: swapped=%v err=%v", swapped, err)
	}
	previous.Clear()
	got, found, err := store.LoadProfile(context.Background(), "active")
	if err != nil || !found || !profilesEqual(got, first) {
		t.Fatalf("failed write changed profile: found=%v err=%v", found, err)
	}
	got.Clear()
}

func TestFileSwapProfileWithPreviousPostRenameReadback(t *testing.T) {
	for _, tc := range []struct {
		name          string
		breakReadback bool
		wantSwapped   bool
		wantError     bool
	}{
		{name: "resolved", breakReadback: false, wantSwapped: true},
		{name: "unresolved", breakReadback: true, wantSwapped: false, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			privateProfileTestRoot(t)
			initial := atomicTestProfile(profileIDOne)
			replacement := atomicTestProfile(profileIDTwo)
			base := NewProductionStateStore().(ProfileStore)
			if err := base.SaveProfile(context.Background(), initial); err != nil {
				t.Fatal(err)
			}
			store := &fileStateStore{random: rand.Reader, profileAtomicWrite: func(ctx context.Context, root, path string, data []byte, random io.Reader) error {
				if err := atomicPrivateWrite(ctx, root, path, data, random); err != nil {
					return err
				}
				if tc.breakReadback {
					if err := os.Chmod(path, 0o644); err != nil {
						return err
					}
				}
				return ErrState // simulate a reported error after rename
			}}
			previous, swapped, err := store.SwapProfileWithPrevious(context.Background(), "active", profileIDOne, replacement)
			if swapped != tc.wantSwapped || (err != nil) != tc.wantError || !profilesEqual(previous, initial) {
				previous.Clear()
				t.Fatalf("post-rename result: swapped=%v err=%v", swapped, err)
			}
			previous.Clear()
			if tc.breakReadback {
				root, err := secureStateRoot()
				if err != nil {
					t.Fatal(err)
				}
				path, err := profilePath(root, "active")
				if err != nil || os.Chmod(path, 0o600) != nil {
					t.Fatal("could not restore test profile permissions")
				}
			}
			got, found, err := base.LoadProfile(context.Background(), "active")
			if err != nil || !found || !profilesEqual(got, replacement) {
				t.Fatalf("post-rename state: found=%v err=%v", found, err)
			}
			got.Clear()
		})
	}
}

func TestFileSwapProfileWithPreviousLoadErrorReturnsZero(t *testing.T) {
	root := privateProfileTestRoot(t)
	store := NewProductionStateStore().(ProfileStore)
	if err := store.SaveProfile(context.Background(), atomicTestProfile(profileIDOne)); err != nil {
		t.Fatal(err)
	}
	path, err := profilePath(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous, swapped, err := store.SwapProfileWithPrevious(context.Background(), "active", profileIDOne, atomicTestProfile(profileIDTwo))
	if err == nil || swapped || !reflect.DeepEqual(previous, Profile{}) {
		t.Fatalf("load error returned previous: swapped=%v err=%v", swapped, err)
	}
}

func TestFileSwapProfileWithPreviousCapturesCrossProcessUpdate(t *testing.T) {
	if os.Getenv("CYNAPSA_SWAP_PREVIOUS_CHILD") == "1" {
		now := time.Now().UTC().Truncate(time.Second)
		updated := atomicTestProfile(profileIDOne)
		updated.AgentID = profileIDTwo
		updated.Credentials = map[string]Credential{
			"mesh-one": {Bundle: profileTestBundle(now, profileIDOne, "mesh-one"),
				ReceivedAt: now, UsableUntil: now.Add(36 * time.Hour)},
		}
		if err := NewProductionStateStore().(ProfileStore).SaveProfile(context.Background(), updated); err != nil {
			t.Fatal(err)
		}
		return
	}
	privateProfileTestRoot(t)
	store := NewProductionStateStore().(ProfileStore)
	ctx := context.Background()
	if err := store.SaveProfile(ctx, atomicTestProfile(profileIDOne)); err != nil {
		t.Fatal(err)
	}
	stale, found, err := store.LoadProfile(ctx, "active")
	if err != nil || !found {
		t.Fatalf("initial load: found=%v err=%v", found, err)
	}
	defer stale.Clear()
	child := exec.Command(os.Args[0], "-test.run=^TestFileSwapProfileWithPreviousCapturesCrossProcessUpdate$")
	child.Env = append(os.Environ(), "CYNAPSA_SWAP_PREVIOUS_CHILD=1")
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("updater process failed: %v: %s", err, output)
	}
	expectedPrevious, found, err := store.LoadProfile(ctx, "active")
	if err != nil || !found || profilesEqual(expectedPrevious, stale) {
		t.Fatalf("cross-process update not observed: found=%v err=%v", found, err)
	}
	defer expectedPrevious.Clear()
	previous, swapped, err := store.SwapProfileWithPrevious(ctx, "active", profileIDOne, atomicTestProfile(profileIDThree))
	if err != nil || !swapped || !profilesEqual(previous, expectedPrevious) {
		previous.Clear()
		t.Fatalf("CAS did not return cross-process update: swapped=%v err=%v", swapped, err)
	}
	previous.Clear()
}
