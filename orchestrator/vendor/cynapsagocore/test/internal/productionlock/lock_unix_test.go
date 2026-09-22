//go:build darwin || linux

package productionlock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireSerializesAndReleases(t *testing.T) {
	first, err := Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err = Acquire(blocked); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		first()
		t.Fatalf("contended Acquire()=%v", err)
	}
	cancel()
	first()
	second, err := Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second()
}
