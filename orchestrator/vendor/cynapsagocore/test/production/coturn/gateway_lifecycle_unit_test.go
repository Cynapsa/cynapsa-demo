package coturn_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type gatewayCommandStep struct {
	want   []string
	result commandResult
}

type gatewayCommandScript struct {
	t     *testing.T
	steps []gatewayCommandStep
	calls [][]string
}

func (script *gatewayCommandScript) run(_ time.Duration, arguments ...string) commandResult {
	script.t.Helper()
	script.calls = append(script.calls, append([]string(nil), arguments...))
	if len(script.steps) == 0 {
		script.t.Fatalf("unexpected Docker command: %v", arguments)
	}
	step := script.steps[0]
	script.steps = script.steps[1:]
	if len(arguments) < len(step.want) {
		script.t.Fatalf("Docker command %v is shorter than expected prefix %v", arguments, step.want)
	}
	for index := range step.want {
		if arguments[index] != step.want[index] {
			script.t.Fatalf("Docker command %v does not match expected prefix %v", arguments, step.want)
		}
	}
	return step.result
}

func (script *gatewayCommandScript) requireConsumed(t *testing.T) {
	t.Helper()
	if len(script.steps) != 0 {
		t.Fatalf("%d scripted Docker commands were not executed", len(script.steps))
	}
}

func TestGatewayLifecycleCreatesAttachesAndVerifiesBeforeStart(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	id := strings.Repeat("a", 64)
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"create", "--label", gatewayOwnerLabel + "=" + specification.owner}, result: commandResult{output: id + "\n"}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, id}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, false)}},
		{want: []string{"network", "connect", "--ip", specification.serviceAddress, specification.serviceNetwork, id}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, id}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, true)}},
		{want: []string{"start", id}, result: commandResult{output: id + "\n"}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, id}, result: commandResult{output: testGatewayInspection(t, id, specification, "running", true, true)}},
	}}

	got, err := createAttachStartGateway(specification, []string{"--name", specification.name, pinnedGatewayImage}, script.run, noGatewayRetry)
	if err != nil {
		t.Fatal(err)
	}
	if got != id {
		t.Fatalf("gateway id=%q, want %q", got, id)
	}
	script.requireConsumed(t)
	if gotOrder := gatewayCommandOrder(script.calls); gotOrder != "create,inspect,network-connect,inspect,start,inspect" {
		t.Fatalf("gateway command order=%q", gotOrder)
	}
}

func TestGatewayLifecycleRejectsStartBeforeWANMutant(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	id := strings.Repeat("b", 64)
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"create"}, result: commandResult{output: id}},
		{want: []string{"inspect"}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, false)}},
		{want: []string{"network", "connect"}},
		// Mutant: Docker reports connect success but inspection still has only
		// the private attachment. Starting now recreates the original race.
		{want: []string{"inspect"}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, false)}},
	}}

	if _, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry); err == nil || !strings.Contains(err.Error(), "WAN attachment verification") {
		t.Fatalf("start-before-WAN mutant error=%v", err)
	}
	script.requireConsumed(t)
	for _, call := range script.calls {
		if len(call) != 0 && call[0] == "start" {
			t.Fatal("gateway was started before its WAN attachment was verified")
		}
	}
}

func TestGatewayLifecycleRejectsWrongOrRunningRecoveryContainers(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	id := strings.Repeat("c", 64)
	for _, test := range []struct {
		name       string
		inspection string
		want       string
	}{
		{
			name:       "wrong owner",
			inspection: testGatewayInspectionWithOwner(t, id, specification, "different-owner", "created", false, false),
			want:       "ownership label mismatch",
		},
		{
			name:       "already running",
			inspection: testGatewayInspection(t, id, specification, "running", true, false),
			want:       "running before WAN verification",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
				{want: []string{"create"}, result: commandResult{exitCode: -1, timedOut: true}},
				{want: []string{"inspect", "-f", gatewayInspectFormat, specification.name}, result: commandResult{output: test.inspection}},
			}}
			got, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry)
			if err == nil || got != "" || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("recovery mutant error=%v, want %q", err, test.want)
			}
			stopNATSideWithRunner(natSide{owner: specification.owner, gatewayName: specification.name}, script.run)
			script.requireConsumed(t)
			if containsRemoval(script.calls) {
				t.Fatal("rejected recovery container was removed by mutable name")
			}
		})
	}
}

func TestGatewayLifecycleRejectsWrongExactIDAfterSuccessfulCreate(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	wantID := strings.Repeat("d", 64)
	wrongID := strings.Repeat("e", 64)
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"create"}, result: commandResult{output: wantID}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, wantID}, result: commandResult{output: testGatewayInspection(t, wrongID, specification, "created", false, false)}},
	}}
	if _, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry); err == nil || !strings.Contains(err.Error(), "wrong gateway container identity") {
		t.Fatalf("wrong-ID mutant error=%v", err)
	}
	script.requireConsumed(t)
}

func TestGatewayLifecycleNeverDeletesPreexistingSameNameContainer(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"create"}, result: commandResult{exitCode: 1}},
	}}
	got, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry)
	if err == nil || got != "" || !strings.Contains(err.Error(), "create failed") {
		t.Fatalf("same-name container collision id=%q error=%v", got, err)
	}
	stopNATSideWithRunner(natSide{owner: specification.owner, gatewayName: specification.name}, script.run)
	script.requireConsumed(t)
	if containsRemoval(script.calls) {
		t.Fatal("preexisting same-name container was removed")
	}
}

func TestGatewayLifecycleRecoversTimedOutConnectAndStartByExactState(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	id := strings.Repeat("f", 64)
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"create"}, result: commandResult{exitCode: -1, timedOut: true}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, specification.name}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, false)}},
		{want: []string{"network", "connect"}, result: commandResult{exitCode: -1, timedOut: true}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, id}, result: commandResult{output: testGatewayInspection(t, id, specification, "created", false, true)}},
		{want: []string{"start", id}, result: commandResult{exitCode: -1, timedOut: true}},
		{want: []string{"inspect", "-f", gatewayInspectFormat, id}, result: commandResult{output: testGatewayInspection(t, id, specification, "running", true, true)}},
	}}
	got, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry)
	if err != nil || got != id {
		t.Fatalf("timed-out lifecycle recovery id=%q error=%v", got, err)
	}
	script.requireConsumed(t)
}

func TestOwnedNetworkCreatesAndRecoversOnlyExactOwner(t *testing.T) {
	name := "cynapsa-nat-a-test"
	owner := "run-owner"
	id := strings.Repeat("1", 64)

	t.Run("successful create", func(t *testing.T) {
		script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
			{want: []string{"network", "create", "--label", gatewayOwnerLabel + "=" + owner, "--driver", "bridge", name}, result: commandResult{output: id}},
			{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, id}, result: commandResult{output: testOwnedNetworkInspection(id, name, owner)}},
		}}
		got, err := createOwnedNetwork(name, owner, script.run, noGatewayRetry)
		if err != nil || got != id {
			t.Fatalf("owned network id=%q error=%v", got, err)
		}
		script.requireConsumed(t)
	})

	t.Run("timeout recovery preserves owned id for cleanup", func(t *testing.T) {
		script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
			{want: []string{"network", "create"}, result: commandResult{exitCode: -1, timedOut: true}},
			{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, name}, result: commandResult{output: testOwnedNetworkInspection(id, name, owner)}},
		}}
		got, err := createOwnedNetwork(name, owner, script.run, noGatewayRetry)
		if err != nil || got != id {
			t.Fatalf("timed-out network recovery id=%q error=%v", got, err)
		}
		script.requireConsumed(t)

		cleanup := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
			{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, id}, result: commandResult{output: testOwnedNetworkInspection(id, name, owner)}},
			{want: []string{"network", "rm", id}},
		}}
		stopNATSideWithRunner(natSide{owner: owner, network: name, networkID: got}, cleanup.run)
		cleanup.requireConsumed(t)
	})

	t.Run("timeout recovery rejects wrong-owner collision", func(t *testing.T) {
		script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
			{want: []string{"network", "create"}, result: commandResult{exitCode: -1, timedOut: true}},
			{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, name}, result: commandResult{output: testOwnedNetworkInspection(id, name, "foreign-owner")}},
		}}
		got, err := createOwnedNetwork(name, owner, script.run, noGatewayRetry)
		if err == nil || got != "" || !strings.Contains(err.Error(), "ownership label mismatch") {
			t.Fatalf("wrong-owner network recovery id=%q error=%v", got, err)
		}
		script.requireConsumed(t)
		if containsRemoval(script.calls) {
			t.Fatal("wrong-owner network collision was removed")
		}
	})

	t.Run("preexisting same-name network is never adopted or removed", func(t *testing.T) {
		script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
			{want: []string{"network", "create"}, result: commandResult{exitCode: 1}},
		}}
		got, err := createOwnedNetwork(name, owner, script.run, noGatewayRetry)
		if err == nil || got != "" || !strings.Contains(err.Error(), "create failed") {
			t.Fatalf("same-name network collision id=%q error=%v", got, err)
		}
		stopNATSideWithRunner(natSide{owner: owner, network: name}, script.run)
		script.requireConsumed(t)
		if containsRemoval(script.calls) {
			t.Fatal("preexisting same-name network was removed")
		}
	})
}

func TestNATCleanupNeverDeletesWrongOwnerOrUsesNameFallback(t *testing.T) {
	specification := testGatewayLifecycleSpec()
	containerID := strings.Repeat("2", 64)
	networkID := strings.Repeat("3", 64)
	side := natSide{
		owner:       specification.owner,
		gatewayName: specification.name,
		gatewayID:   containerID,
		network:     specification.privateNetwork,
		networkID:   networkID,
	}
	script := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
		{want: []string{"inspect", "-f", gatewayInspectFormat, containerID}, result: commandResult{output: testGatewayInspectionWithOwner(t, containerID, specification, "foreign-owner", "created", false, false)}},
		{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, networkID}, result: commandResult{output: testOwnedNetworkInspection(networkID, specification.privateNetwork, "foreign-owner")}},
	}}
	stopNATSideWithRunner(side, script.run)
	script.requireConsumed(t)
	if containsRemoval(script.calls) {
		t.Fatal("cleanup deleted a wrong-owner same-name resource")
	}

	nameOnly := &gatewayCommandScript{t: t}
	stopNATSideWithRunner(natSide{
		owner: specification.owner, gatewayName: specification.name,
		network: specification.privateNetwork,
	}, nameOnly.run)
	if len(nameOnly.calls) != 0 {
		t.Fatalf("cleanup used an unverified mutable name: %v", nameOnly.calls)
	}
}

func TestNATCleanupRetainsVerifiedIDAcrossAttachAndStartFailures(t *testing.T) {
	for _, test := range []struct {
		name          string
		lifecycle     []gatewayCommandStep
		cleanupStatus string
		attached      bool
		wantError     string
	}{
		{
			name: "attach failure",
			lifecycle: []gatewayCommandStep{
				{want: []string{"network", "connect"}, result: commandResult{exitCode: 1}},
			},
			cleanupStatus: "created",
			wantError:     "WAN attachment failed",
		},
		{
			name: "start failure",
			lifecycle: []gatewayCommandStep{
				{want: []string{"network", "connect"}},
				{want: []string{"inspect"}, result: commandResult{}},
				{want: []string{"start"}, result: commandResult{exitCode: 1}},
			},
			cleanupStatus: "created",
			attached:      true,
			wantError:     "start failed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			specification := testGatewayLifecycleSpec()
			containerID := strings.Repeat("4", 64)
			networkID := strings.Repeat("5", 64)
			steps := []gatewayCommandStep{
				{want: []string{"create"}, result: commandResult{output: containerID}},
				{want: []string{"inspect"}, result: commandResult{output: testGatewayInspection(t, containerID, specification, "created", false, false)}},
			}
			for _, step := range test.lifecycle {
				if len(step.want) > 0 && step.want[0] == "inspect" && step.result.output == "" {
					step.result.output = testGatewayInspection(t, containerID, specification, "created", false, true)
				}
				steps = append(steps, step)
			}
			script := &gatewayCommandScript{t: t, steps: steps}
			got, err := createAttachStartGateway(specification, nil, script.run, noGatewayRetry)
			if err == nil || got != containerID || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("failed lifecycle retained id=%q error=%v", got, err)
			}
			script.requireConsumed(t)

			cleanup := &gatewayCommandScript{t: t, steps: []gatewayCommandStep{
				{want: []string{"inspect", "-f", gatewayInspectFormat, containerID}, result: commandResult{output: testGatewayInspection(t, containerID, specification, test.cleanupStatus, false, test.attached)}},
				{want: []string{"rm", "-f", containerID}},
				{want: []string{"network", "inspect", "-f", ownedNetworkInspectFormat, networkID}, result: commandResult{output: testOwnedNetworkInspection(networkID, specification.privateNetwork, specification.owner)}},
				{want: []string{"network", "rm", networkID}},
			}}
			stopNATSideWithRunner(natSide{
				owner: specification.owner, gatewayName: specification.name, gatewayID: got,
				network: specification.privateNetwork, networkID: networkID,
			}, cleanup.run)
			cleanup.requireConsumed(t)
		})
	}
}

func testGatewayLifecycleSpec() gatewayLifecycleSpec {
	return gatewayLifecycleSpec{
		name: "cynapsa-gateway-a-test", owner: "cynapsa-gateway-a-test",
		privateNetwork: "cynapsa-nat-a-test", privateAddress: "172.20.0.2",
		serviceNetwork: "cynapsa-service-test", serviceAddress: "172.21.0.5",
	}
}

func testGatewayInspection(t *testing.T, id string, specification gatewayLifecycleSpec, status string, running, attached bool) string {
	t.Helper()
	return testGatewayInspectionWithOwner(t, id, specification, specification.owner, status, running, attached)
}

func testGatewayInspectionWithOwner(t *testing.T, id string, specification gatewayLifecycleSpec, owner, status string, running, attached bool) string {
	t.Helper()
	networks := map[string]gatewayNetworkAttachment{
		specification.privateNetwork: testGatewayAttachment(specification.privateAddress, running),
	}
	if attached {
		networks[specification.serviceNetwork] = testGatewayAttachment(specification.serviceAddress, running)
	}
	encoded, err := json.Marshal(networks)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s|/%s|%s|%s|%t|0|%s", id, specification.name, owner, status, running, encoded)
}

func testGatewayAttachment(address string, active bool) gatewayNetworkAttachment {
	attachment := gatewayNetworkAttachment{}
	attachment.IPAMConfig = &struct {
		IPv4Address string `json:"IPv4Address"`
	}{IPv4Address: address}
	if active {
		attachment.IPAddress = address
	}
	return attachment
}

func testOwnedNetworkInspection(id, name, owner string) string {
	return fmt.Sprintf("%s|%s|%s", id, name, owner)
}

func noGatewayRetry() func() bool {
	return func() bool { return false }
}

func gatewayCommandOrder(calls [][]string) string {
	order := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call) == 0 {
			continue
		}
		command := call[0]
		if len(call) > 1 && command == "network" {
			command += "-" + call[1]
		}
		order = append(order, command)
	}
	return strings.Join(order, ",")
}

func containsRemoval(calls [][]string) bool {
	for _, call := range calls {
		if len(call) > 0 && (call[0] == "rm" || (len(call) > 1 && call[0] == "network" && call[1] == "rm")) {
			return true
		}
	}
	return false
}
