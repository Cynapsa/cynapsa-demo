package coturn_test

import "testing"

func TestLogOutputContainsAcceptsPatternFromFinalTimedOutPoll(t *testing.T) {
	result := commandResult{output: "prefix\n{\"event\":\"ready\"}\n", timedOut: true, exitCode: -1}
	if !logOutputContains(result, `"event":"ready"`) {
		t.Fatal("final timed-out docker logs output lost a present readiness marker")
	}
}

func TestLogOutputContainsRejectsMissingOrEmptyPattern(t *testing.T) {
	result := commandResult{output: "unrelated output"}
	if logOutputContains(result, `"event":"ready"`) {
		t.Fatal("missing readiness marker accepted")
	}
	if logOutputContains(result, "") {
		t.Fatal("empty readiness marker accepted")
	}
}
