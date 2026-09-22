package cynapsagocore

import (
	"context"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestPublicCoreShutdownCommandCompletesBeforeTerminalCutoff(t *testing.T) {
	core, err := New(Config{QueueLimit: 4, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	admission, err := core.Submit(context.Background(), v1.CoreShutdownCommand{CommandBase: v1.CommandBase{
		CommandID: "shutdown-command", SDKSessionID: "session",
	}})
	if err != nil || !admission.Accepted {
		t.Fatalf("shutdown admission = %+v, %v", admission, err)
	}
	completion, err := core.NextCompletion(context.Background())
	if err != nil || !completion.OK || completion.CommandID != "shutdown-command" || completion.Result != (v1.EmptyResult{}) {
		t.Fatalf("shutdown completion = %+v, %v", completion, err)
	}

	if _, err = core.Submit(context.Background(), v1.CoreStatusCommand{CommandBase: v1.CommandBase{
		CommandID: "late", SDKSessionID: "session",
	}}); publicCode(err) != v1.ErrorCodeShutdownInProgress {
		t.Fatalf("late admission = %v", err)
	}
	if _, err = core.PayloadOpen(context.Background()); publicCode(err) != v1.ErrorCodeShutdownInProgress {
		t.Fatalf("direct payload admission = %v", err)
	}

	shutdownAndDrain(t, core)
	if err = core.Destroy(); err != nil {
		t.Fatal(err)
	}
}
