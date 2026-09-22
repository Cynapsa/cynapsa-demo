package sdkboundary

import (
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func takeFrozenCommandForTest(t testing.TB, wrapper model.Command) model.Command {
	t.Helper()
	command, _, ok := model.TakeFrozenCommand(&wrapper)
	if !ok {
		t.Fatal("expected opaque frozen command ownership")
	}
	t.Cleanup(func() { model.ClearCommand(&command) })
	return command
}
