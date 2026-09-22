package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
)

func FuzzStageBAuthenticatedCommitTerminality(f *testing.F) {
	for mode := byte(0); mode < 9; mode++ {
		f.Add(mode)
	}

	f.Fuzz(func(t *testing.T, mode byte) {
		mode %= 9
		r := testRuntime(t, 16, Dependencies{Clock: coreclock.NewFake(time.Unix(1_000, 0))})
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		if mode == 6 {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			ctx = cancelled
		}
		commitCalls := 0
		err := r.CommitAuthenticated(ctx, func(commitReady func() error) error {
			switch mode {
			case 0:
				return nil
			case 1:
				return errors.New("publisher failure")
			case 2:
				panic("publisher panic")
			case 3:
				commitCalls++
				return commitReady()
			case 4:
				commitCalls++
				if err := commitReady(); err != nil {
					return err
				}
				commitCalls++
				return commitReady()
			case 5:
				commitCalls++
				if err := commitReady(); err != nil {
					return err
				}
				return errors.New("ignored after terminal commit")
			case 7:
				commitCalls++
				if err := commitReady(); err != nil {
					return err
				}
				panic("ignored after terminal commit")
			case 8:
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				_ = cancelled
				return nil
			default:
				t.Fatal("publisher called for pre-cancelled context")
				return nil
			}
		})

		committed := mode == 3 || mode == 4 || mode == 5 || mode == 7
		if committed {
			if err != nil || r.Status().Lifecycle != stateReady || commitCalls == 0 {
				t.Fatalf("mode %d terminal commit: err=%v status=%+v calls=%d", mode, err, r.Status(), commitCalls)
			}
		} else {
			if err == nil || r.Status().Lifecycle != stateCreated || r.events.Stats().Depth != 0 {
				t.Fatalf("mode %d failed commit changed state: err=%v status=%+v events=%+v", mode, err, r.Status(), r.events.Stats())
			}
		}
		forceShutdown(t, r)
	})
}
