package enrollment

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestForcePendingProfileIDHasClosedPrivateGrammar(t *testing.T) {
	pending, err := ForcePendingProfileID("public-profile")
	if err != nil {
		t.Fatal(err)
	}
	again, err := ForcePendingProfileID("public-profile")
	if err != nil || pending != again {
		t.Fatal("force-pending ID was not deterministic")
	}
	other, err := ForcePendingProfileID("other-profile")
	if err != nil || pending == other {
		t.Fatal("different public profiles shared a private slot")
	}
	if len(pending) > maximumProfileID || !strings.HasPrefix(pending, forcePendingPrefix) ||
		ValidProfileID(pending) || !validStoredProfileID(pending) {
		t.Fatalf("invalid private force-pending grammar: %q", pending)
	}
	for _, publicID := range []string{"", "../active", "a/b", pending} {
		if value, err := ForcePendingProfileID(publicID); err == nil || value != "" {
			t.Fatalf("invalid public input accepted: %q", publicID)
		}
	}
	longPublicID := strings.Repeat("a", maximumProfileID)
	longPending, err := ForcePendingProfileID(longPublicID)
	if err != nil || len(longPending) > maximumProfileID || !validStoredProfileID(longPending) {
		t.Fatal("maximum-length public ID did not yield a valid private slot")
	}

	for _, value := range []string{
		"a/b", "../active", "active/../other", forcePendingPrefix,
		pending + "/extra", pending + "0", pending[:len(pending)-1],
		strings.Replace(pending, forcePendingPrefix, "cynapsa-internal/force-pending/v2/", 1),
		forcePendingPrefix + strings.Repeat("A", 64),
		forcePendingPrefix + strings.Repeat("g", 64),
	} {
		if validStoredProfileID(value) {
			t.Fatalf("noncanonical slash ID accepted for storage: %q", value)
		}
		if _, err := profileFingerprint(value); err == nil {
			t.Fatalf("noncanonical slash ID accepted for fingerprinting: %q", value)
		}
	}
}

func TestPrivateForcePendingProfileStorageAndPublicLeaseSeparation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) (ProfileStore, string)
	}{
		{"memory", func(*testing.T) (ProfileStore, string) {
			return NewMemoryStateStore().(ProfileStore), ""
		}},
		{"file", func(t *testing.T) (ProfileStore, string) {
			root := privateProfileTestRoot(t)
			return NewProductionStateStore().(ProfileStore), root
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, root := tc.store(t)
			ctx := context.Background()
			pending, err := ForcePendingProfileID("public-profile")
			if err != nil {
				t.Fatal(err)
			}
			if release, acquired, err := store.TryLockForceProfile(ctx, pending); err == nil || acquired || release != nil {
				t.Fatal("private stored ID was accepted as a public force-operation lease")
			}
			if root != "" {
				path, err := profilePath(root, pending)
				if err != nil || filepath.Dir(path) != root || strings.Contains(filepath.Base(path), "/") ||
					strings.Contains(path, pending) {
					t.Fatalf("private profile did not map to a hashed basename: path=%q err=%v", path, err)
				}
				if _, err := forceProfileLockPath(root, pending); err == nil {
					t.Fatal("private ID was accepted for force-operation lock path")
				}
			}

			first := atomicTestProfile(profileIDOne)
			first.ProfileID = pending
			loaded, created, err := store.LoadOrCreateProfile(ctx, first)
			if err != nil || !created || !profilesEqual(loaded, first) {
				t.Fatalf("private load-or-create: created=%v err=%v", created, err)
			}
			loaded.Clear()
			loaded, found, err := store.LoadProfile(ctx, pending)
			if err != nil || !found || !profilesEqual(loaded, first) {
				t.Fatalf("private load: found=%v err=%v", found, err)
			}
			loaded.Clear()
			updated := first.Clone()
			updated.AgentID = profileIDTwo
			if err := store.SaveProfile(ctx, updated); err != nil {
				t.Fatal(err)
			}
			second := atomicTestProfile(profileIDTwo)
			second.ProfileID = pending
			previous, swapped, err := store.SwapProfileWithPrevious(ctx, pending, profileIDOne, second)
			if err != nil || !swapped || !profilesEqual(previous, updated) {
				previous.Clear()
				t.Fatalf("private swap-with-previous: swapped=%v err=%v", swapped, err)
			}
			previous.Clear()
			third := atomicTestProfile(profileIDThree)
			third.ProfileID = pending
			if swapped, err := store.CompareAndSwapProfile(ctx, pending, profileIDTwo, third); err != nil || !swapped {
				t.Fatalf("private compare-and-swap: swapped=%v err=%v", swapped, err)
			}
			if deleted, err := store.CompareAndDeleteProfile(ctx, pending, profileIDThree); err != nil || !deleted {
				t.Fatalf("private compare-and-delete: deleted=%v err=%v", deleted, err)
			}
			if _, found, err := store.LoadProfile(ctx, pending); err != nil || found {
				t.Fatalf("private delete readback: found=%v err=%v", found, err)
			}
			if swapped, err := store.CompareAndSwapProfile(ctx, pending, "", first); err != nil || !swapped {
				t.Fatalf("private absent CAS: swapped=%v err=%v", swapped, err)
			}
		})
	}
}

func TestOtherSlashProfileIDsFailStorageOperations(t *testing.T) {
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
			store := tc.store(t)
			ctx := context.Background()
			for _, id := range []string{"a/b", "../active", "active/../other", forcePendingPrefix + strings.Repeat("G", 64)} {
				candidate := atomicTestProfile(profileIDOne)
				candidate.ProfileID = id
				if err := store.SaveProfile(ctx, candidate); err == nil {
					t.Fatalf("SaveProfile accepted %q", id)
				}
				if _, _, err := store.LoadProfile(ctx, id); err == nil {
					t.Fatalf("LoadProfile accepted %q", id)
				}
				if _, _, err := store.LoadOrCreateProfile(ctx, candidate); err == nil {
					t.Fatalf("LoadOrCreateProfile accepted %q", id)
				}
				if _, err := store.CompareAndSwapProfile(ctx, id, "", candidate); err == nil {
					t.Fatalf("CompareAndSwapProfile accepted %q", id)
				}
				if _, _, err := store.SwapProfileWithPrevious(ctx, id, "", candidate); err == nil {
					t.Fatalf("SwapProfileWithPrevious accepted %q", id)
				}
				if _, err := store.CompareAndDeleteProfile(ctx, id, ""); err == nil {
					t.Fatalf("CompareAndDeleteProfile accepted %q", id)
				}
			}
		})
	}
}
