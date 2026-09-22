package payload

import (
	"bytes"
	"errors"
	"testing"
)

func FuzzAcceptanceStageBHandleStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{2, 0, 1, 3, 4, 4, 5})
	f.Add([]byte{1, 1, 0, 2, 4})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 256 {
			operations = operations[:256]
		}
		canonical := stageBHandleCanonical(t, []byte("fuzzed handle state"))
		store := stageBHandleStore(t, 2, 1<<20)
		handle, err := store.Open()
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Write(handle, canonical); err != nil {
			t.Fatal(err)
		}
		for _, rawOperation := range operations {
			switch rawOperation % 8 {
			case 0:
				size, err := store.Size(handle)
				if err == nil && size != int64(len(canonical)) {
					t.Fatalf("successful size = %d; want %d", size, len(canonical))
				}
				if err != nil && !stageBExpectedRaceError(err) {
					t.Fatalf("size error = %v", err)
				}
			case 1:
				if err := store.Cancel(handle); !stageBExpectedRaceError(err) {
					t.Fatalf("cancel error = %v", err)
				}
			case 2:
				if err := store.Finish(handle); err != nil &&
					!errors.Is(err, ErrInvalidHandle) &&
					!errors.Is(err, ErrInvalidHandleState) {
					t.Fatalf("finish error = %v", err)
				}
			case 3:
				if err := store.Retain(handle); !stageBExpectedRaceError(err) {
					t.Fatalf("retain error = %v", err)
				}
			case 4:
				if err := store.Release(handle); !stageBExpectedRaceError(err) {
					t.Fatalf("release error = %v", err)
				}
			case 5:
				store.Close()
			case 6:
				value, digest, err := store.Snapshot(handle)
				if err == nil {
					if !bytes.Equal(value, canonical) || digest != Digest(canonical) {
						t.Fatal("successful snapshot returned mutated completed content")
					}
					zero(value)
				} else if !stageBExpectedRaceError(err) {
					t.Fatalf("snapshot error = %v", err)
				}
			case 7:
				if err := store.Write(handle, nil); err != nil &&
					!errors.Is(err, ErrInvalidHandle) &&
					!errors.Is(err, ErrInvalidHandleState) {
					t.Fatalf("write error = %v", err)
				}
			}
		}
		store.Close()
	})
}
