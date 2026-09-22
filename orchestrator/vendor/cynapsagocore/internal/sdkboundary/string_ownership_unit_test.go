package sdkboundary

import (
	"context"
	"strings"
	"testing"
	"unsafe"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

func TestDecodeCommandDetachesAcceptedStringBacking(t *testing.T) {
	commandBacking := "command-1" + strings.Repeat("x", 1<<20)
	sessionBacking := "session-1" + strings.Repeat("y", 1<<20)
	commandID := commandBacking[:len("command-1")]
	sessionID := sessionBacking[:len("session-1")]
	decoded, err := new(Adapter).DecodeCommand(context.Background(), v1.CoreStatusCommand{CommandBase: v1.CommandBase{CommandID: v1.CommandID(commandID), SDKSessionID: v1.SDKSessionID(sessionID)}})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _, ok := model.TakeFrozenCommand(&decoded)
	if !ok {
		t.Fatal("decoded command did not carry frozen ownership")
	}
	defer model.ClearCommand(&canonical)
	if unsafe.StringData(canonical.ID) == unsafe.StringData(commandID) || unsafe.StringData(canonical.SessionID) == unsafe.StringData(sessionID) {
		t.Fatal("decoded command retained caller string backing")
	}
}

func TestLargestValidPolicyCommandFitsFrozenHardLimit(t *testing.T) {
	rules := make([]v1.PolicyRule, maxPolicyRules)
	for index := range rules {
		rules[index] = v1.PolicyRule{Action: v1.PolicyActionAllow, Path: "/" + strings.Repeat("p", maxPathLength-1), AgentID: v1.AgentID(strings.Repeat("a", maxIdentifierLength))}
	}
	decoded, err := new(Adapter).DecodeCommand(context.Background(), v1.PolicySetCommand{CommandBase: v1.CommandBase{CommandID: v1.CommandID(strings.Repeat("c", maxIdentifierLength)), SDKSessionID: v1.SDKSessionID(strings.Repeat("s", maxIdentifierLength))}, Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	defer model.ClearCommand(&decoded)
	bytes, ok := model.FrozenCommandBytes(decoded)
	if !ok || bytes > model.MaximumFrozenCommandBytes {
		t.Fatalf("largest policy charge = %d, frozen=%v, hard maximum=%d", bytes, ok, model.MaximumFrozenCommandBytes)
	}
}
