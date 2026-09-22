package coturn_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/test/internal/productionlock"
)

const pinnedAgentImage = "ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8"
const pinnedGatewayImage = "nicolaka/netshoot@sha256:47b907d662d139d1e2f22bfe14f4efca1e3f1feed283572f47c970c780c03b61"

const (
	agentRouteReadyTimeout    = 60 * time.Second
	agentRouteAttemptTimeout  = 8 * time.Second
	agentRoutePollInterval    = 250 * time.Millisecond
	containerRecoveryTimeout  = 15 * time.Second
	recoveryFaultReadyTimeout = 110 * time.Second
	turnModeTransitionTimeout = 90 * time.Second
	gatewayOwnerLabel         = "com.cynapsa.qualification.gateway"
)

type agentReport struct {
	Event     string `json:"event"`
	Role      string `json:"role"`
	Path      string `json:"path"`
	From      string `json:"from"`
	Bytes     int    `json:"bytes"`
	Available bool   `json:"available"`
}

type coturnEnvironment struct {
	repository, harness, state, childState, serviceNetwork string
	credentials                                            map[string]string
	turnSecret                                             string
	ejabberdID, stunID, turnID, volume, agent              string
	stopped                                                bool
}

func startCoturnEnvironment(t *testing.T) *coturnEnvironment {
	t.Helper()
	lockContext, cancelLock := context.WithTimeout(context.Background(), 10*time.Minute)
	release, err := productionlock.Acquire(lockContext)
	cancelLock()
	if err != nil {
		t.Fatalf("acquire production environment: %v", err)
	}
	t.Cleanup(release)

	environment := &coturnEnvironment{repository: repositoryRoot(t)}
	environment.harness = filepath.Join(environment.repository, "test", "production", "coturn", "run.sh")
	stateReport := filepath.Join(t.TempDir(), "coturn-state")
	start := runCommandAllowFailure(environment.repository, 180*time.Second, environment.harness, "start", stateReport)
	reportedState := readOptionalFirst(stateReport)
	if start.exitCode != 0 {
		if validCoturnState(reportedState) {
			_ = runCommandAllowFailure(environment.repository, 30*time.Second, environment.harness, "stop", reportedState)
		}
		t.Fatalf("%s start: exit=%d\n%s", environment.harness, start.exitCode, start.output)
	}
	environment.state = strings.TrimSpace(start.output)
	if reportedState != environment.state {
		if validCoturnState(reportedState) {
			_ = runCommandAllowFailure(environment.repository, 30*time.Second, environment.harness, "stop", reportedState)
		}
		t.Fatalf("state report=%q, output=%q", reportedState, environment.state)
	}
	if !validCoturnState(environment.state) {
		t.Fatalf("invalid environment state %q", environment.state)
	}
	t.Cleanup(func() {
		if !environment.stopped {
			_ = runCommandAllowFailure(environment.repository, 30*time.Second, environment.harness, "stop", environment.state)
		}
	})
	for _, command := range [][]string{{"create", "e2e-mesh"}, {"add", "agent-a", "e2e-mesh"}, {"add", "agent-b", "e2e-mesh"}} {
		arguments := append([]string{"admin", environment.state}, command...)
		runCommand(t, environment.repository, 30*time.Second, environment.harness, arguments...)
	}

	environment.credentials = readEnvironment(t, filepath.Join(environment.state, "credentials.env"))
	environment.turnSecret = readFirst(t, filepath.Join(environment.state, "turn-rest-secret"))
	if image := readFirst(t, filepath.Join(environment.state, "gateway-image.txt")); image != pinnedGatewayImage {
		t.Fatalf("gateway image=%q, want immutable %q", image, pinnedGatewayImage)
	}
	environment.childState = readFirst(t, filepath.Join(environment.state, "ejabberd-state"))
	environment.serviceNetwork = readFirst(t, filepath.Join(environment.childState, "network"))
	environment.ejabberdID = readFirst(t, filepath.Join(environment.childState, "container-id"))
	environment.stunID = readFirst(t, filepath.Join(environment.state, "stun-container-id"))
	environment.turnID = readFirst(t, filepath.Join(environment.state, "turn-container-id"))
	environment.volume = readFirst(t, filepath.Join(environment.childState, "volume"))
	environment.agent = filepath.Join(t.TempDir(), "cynapsa-production-agent")
	build := exec.Command("go", "build", "-trimpath", "-tags=cynapsa_test_evidence", "-o", environment.agent, "./test/production/multiprocess/agent")
	build.Dir = environment.repository
	build.Env = append(os.Environ(), "GOTOOLCHAIN=go1.26.6", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build Linux agent: %v\n%s", buildErr, output)
	}
	if err = os.Chmod(environment.agent, 0o555); err != nil {
		t.Fatal(err)
	}
	return environment
}

func (environment *coturnEnvironment) pairConfig(t *testing.T) agentPairConfig {
	t.Helper()
	return agentPairConfig{
		repository: environment.repository, state: environment.state, serviceNetwork: environment.serviceNetwork,
		agent: environment.agent, credentials: environment.credentials, turnSecret: environment.turnSecret,
		ejabberdID: environment.ejabberdID, turnID: environment.turnID,
		ejabberdAddress: containerNetworkIPv4(t, environment.repository, environment.ejabberdID, environment.serviceNetwork),
		stunAddress:     containerNetworkIPv4(t, environment.repository, environment.stunID, environment.serviceNetwork),
		turnAddress:     containerNetworkIPv4(t, environment.repository, environment.turnID, environment.serviceNetwork),
	}
}

func (environment *coturnEnvironment) assertReadyAndStop(t *testing.T) {
	t.Helper()
	readiness := readFirstLines(t, filepath.Join(environment.state, "readiness.log"))
	if !strings.Contains(readiness, "stun=ready") || !strings.Contains(readiness, "turn=ready listener=true") || !validExternalServiceReadiness(readiness) {
		t.Fatalf("protocol readiness evidence incomplete: %q", readiness)
	}
	runCommand(t, environment.repository, 30*time.Second, environment.harness, "stop", environment.state)
	environment.stopped = true
	for _, id := range []string{environment.ejabberdID, environment.stunID, environment.turnID} {
		if output := runCommandAllowFailure(environment.repository, 5*time.Second, "docker", "inspect", id); output.exitCode == 0 {
			t.Errorf("container remained after teardown: %s", id)
		}
	}
	if output := runCommandAllowFailure(environment.repository, 5*time.Second, "docker", "network", "inspect", environment.serviceNetwork); output.exitCode == 0 {
		t.Errorf("network remained after teardown: %s", environment.serviceNetwork)
	}
	if output := runCommandAllowFailure(environment.repository, 5*time.Second, "docker", "volume", "inspect", environment.volume); output.exitCode == 0 {
		t.Errorf("volume remained after teardown: %s", environment.volume)
	}
	for _, secret := range []string{"credentials.env", "extdisco-credential.json", "stun.conf", "turn.conf", "turn-mode", "turn-rest-secret"} {
		if _, err := os.Stat(filepath.Join(environment.state, secret)); !os.IsNotExist(err) {
			t.Errorf("private file remained after teardown: %s", secret)
		}
	}
	for _, secret := range []string{"ejabberd.yml", "ejabberd.yml.tmp", "ejabberd-extdisco-all.yml"} {
		if _, err := os.Stat(filepath.Join(environment.childState, secret)); !os.IsNotExist(err) {
			t.Errorf("secret-bearing server configuration remained after teardown: %s", secret)
		}
	}
	assertArtifactsSanitized(t, []string{filepath.Join(environment.state, "artifacts"), filepath.Join(environment.childState, "artifacts")}, []string{environment.credentials["CYNAPSA_EJABBERD_PASSWORD_A"], environment.credentials["CYNAPSA_EJABBERD_PASSWORD_B"], environment.turnSecret})
}

func validExternalServiceReadiness(readiness string) bool {
	const prefix = "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime="
	for _, line := range strings.Split(readiness, "\n") {
		if !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, "s") {
			continue
		}
		seconds, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line, prefix), "s"))
		return err == nil && seconds > 0 && seconds <= 120
	}
	return false
}

func TestPinnedServicesAcrossIndependentNATGateways(t *testing.T) {
	environment := startCoturnEnvironment(t)
	base := environment.pairConfig(t)
	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "stun")
	stunBefore := strings.Count(runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.stunID), "BINDING processed, success")
	directConfig := base
	directConfig.name = "direct"
	directConfig.natProfile = "full-cone"
	directConfig.expectLive = true
	direct := runAgentPair(t, directConfig)
	assertReports(t, direct.sender, true)
	assertReceiverReports(t, direct.receiver, true)
	if stunAfter := strings.Count(runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.stunID), "BINDING processed, success"); stunAfter <= stunBefore {
		t.Fatal("direct NAT traversal produced no new STUN binding evidence")
	}
	turnLog := runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)

	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-udp")
	relayConfig := base
	relayConfig.name = "relay"
	relayConfig.natProfile = "symmetric"
	relayConfig.expectLive = true
	relay := runAgentPair(t, relayConfig)
	assertReports(t, relay.sender, true)
	assertReceiverReports(t, relay.receiver, true)
	turnLog = runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	if !containsFold(turnLog, "allocate processed, success") || !containsFold(turnLog, "channel_bind processed, success") {
		t.Fatalf("relay success lacks authenticated allocation evidence:\n%s", turnLog)
	}

	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-tcp")
	tcpBefore := turnLog
	tcpConfig := base
	tcpConfig.name = "relay-tcp"
	tcpConfig.natProfile = "symmetric"
	tcpConfig.expectLive = true
	tcpConfig.initialUDPBlocked = true
	tcpRelay := runAgentPair(t, tcpConfig)
	assertReports(t, tcpRelay.sender, true)
	assertReceiverReports(t, tcpRelay.receiver, true)
	turnLog = runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	tcpEvidence := appendedLog(tcpBefore, turnLog)
	if !containsFold(tcpEvidence, "tcp or tls connected to:") || !containsFold(tcpEvidence, "allocate processed, success") || !containsFold(tcpEvidence, "channel_bind processed, success") {
		t.Fatalf("TCP relay success lacks authenticated TCP allocation evidence:\n%s", tcpEvidence)
	}

	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-udp")
	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "allocation-rejected")
	quotaBefore := runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	quotaConfig := relayConfig
	quotaConfig.name = "allocation-rejected"
	quotaConfig.expectLive = false
	quota := runAgentPair(t, quotaConfig)
	assertReports(t, quota.sender, false)
	assertReceiverReports(t, quota.receiver, false)
	turnLog = runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	quotaEvidence := appendedLog(quotaBefore, turnLog)
	if !containsFold(quotaEvidence, "allocate processed, error 442") || !containsFold(quotaEvidence, "udp transport is not allowed") {
		t.Fatalf("allocation rejection lacks exact server evidence:\n%s", quotaEvidence)
	}

	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "permission-rejected")
	permissionBefore := runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	permissionConfig := relayConfig
	permissionConfig.name = "permission-rejected"
	permissionConfig.expectLive = false
	permission := runAgentPair(t, permissionConfig)
	assertReports(t, permission.sender, false)
	assertReceiverReports(t, permission.receiver, false)
	turnLog = runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	permissionEvidence := appendedLog(permissionBefore, turnLog)
	if !containsFold(permissionEvidence, "create_permission processed, error 403: forbidden ip") {
		t.Fatalf("permission rejection lacks exact server evidence:\n%s", permissionEvidence)
	}
	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "normal")

	recoveryConfig := relayConfig
	recoveryConfig.name = "recovery"
	recoveryConfig.recovery = true
	recovery := runAgentPair(t, recoveryConfig)
	assertRecoveryReports(t, recovery.sender, recovery.receiver)

	runCommand(t, environment.repository, 15*time.Second, "docker", "stop", environment.turnID)
	unavailableConfig := relayConfig
	unavailableConfig.name = "unavailable"
	unavailableConfig.expectLive = false
	unavailable := runAgentPair(t, unavailableConfig)
	assertReports(t, unavailable.sender, false)
	assertReceiverReports(t, unavailable.receiver, false)
	runCommand(t, environment.repository, 30*time.Second, environment.harness, "restart", environment.state, "turn")
	recoveredConfig := relayConfig
	recoveredConfig.name = "recovered"
	recovered := runAgentPair(t, recoveredConfig)
	assertReports(t, recovered.sender, true)
	assertReceiverReports(t, recovered.receiver, true)

	environment.assertReadyAndStop(t)
}

func TestRemoteMeshSmoke(t *testing.T) {
	environment := startCoturnEnvironment(t)
	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-udp")
	turnBefore := runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID)
	config := environment.pairConfig(t)
	config.name = "remote-smoke"
	config.natProfile = "symmetric"
	config.expectLive = true
	config.remoteSmoke = true
	config.smokeNonce = "smoke-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	result := runAgentPair(t, config)
	assertRemoteSmokeReports(t, result, config.smokeNonce)
	turnEvidence := appendedLog(turnBefore, runCommand(t, environment.repository, 10*time.Second, "docker", "logs", environment.turnID))
	if !containsFold(turnEvidence, "allocate processed, success") || !containsFold(turnEvidence, "channel_bind processed, success") {
		t.Fatalf("remote smoke lacks authenticated TURN allocation and channel binding evidence:\n%s", turnEvidence)
	}
	environment.assertReadyAndStop(t)
}

func TestDiagnosticPermissionRejected(t *testing.T) {
	environment := startCoturnEnvironment(t)
	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-udp")
	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "permission-rejected")
	config := environment.pairConfig(t)
	config.name = "diagnostic-permission-rejected"
	config.natProfile = "symmetric"
	config.expectLive = false
	result := runAgentPair(t, config)
	assertReports(t, result.sender, false)
	assertReceiverReports(t, result.receiver, false)
}

func TestDiagnosticPermissionRejectedAfterAllocationRejected(t *testing.T) {
	environment := startCoturnEnvironment(t)
	runCommand(t, environment.repository, 90*time.Second, environment.harness, "external-service-mode", environment.state, "relay-udp")
	base := environment.pairConfig(t)
	base.natProfile = "symmetric"
	base.expectLive = false

	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "allocation-rejected")
	allocation := base
	allocation.name = "diagnostic-allocation-rejected"
	allocationResult := runAgentPair(t, allocation)
	assertReports(t, allocationResult.sender, false)
	assertReceiverReports(t, allocationResult.receiver, false)

	runCommand(t, environment.repository, turnModeTransitionTimeout, environment.harness, "turn-mode", environment.state, "permission-rejected")
	permission := base
	permission.name = "diagnostic-permission-after-allocation"
	permissionResult := runAgentPair(t, permission)
	assertReports(t, permissionResult.sender, false)
	assertReceiverReports(t, permissionResult.receiver, false)
}

type agentPairConfig struct {
	repository, state, serviceNetwork, agent  string
	credentials                               map[string]string
	turnSecret                                string
	ejabberdID, turnID                        string
	ejabberdAddress, stunAddress, turnAddress string
	name, natProfile                          string
	expectLive                                bool
	recovery                                  bool
	initialUDPBlocked                         bool
	remoteSmoke                               bool
	smokeNonce                                string
}

type agentPairResult struct {
	sender, receiver []agentReport
}

type natSide struct {
	network, subnet, gatewayAddress, agentAddress string
	owner, networkID, gatewayName, gatewayID      string
	evidence                                      string
}

func runAgentPair(t *testing.T, config agentPairConfig) agentPairResult {
	t.Helper()
	pairStartedAt := time.Now().UTC()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	receiverName := "cynapsa-production-b-" + config.name + "-" + suffix
	senderName := "cynapsa-production-a-" + config.name + "-" + suffix
	sideA := startNATSide(t, config, "a", suffix)
	sideB := startNATSide(t, config, "b", suffix)
	defer func() {
		if t.Failed() {
			observeAgentsAfterHarnessFailure(t, config, senderName, receiverName, 15*time.Second)
			logNATFailureEvidence(t, config, sideA, sideB)
			logServiceFailureEvidence(t, config, pairStartedAt)
		}
	}()
	if config.initialUDPBlocked {
		for _, side := range []natSide{sideA, sideB} {
			runCommand(t, config.repository, 10*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "fault-udp")
		}
	}
	agentArtifacts := filepath.Join(config.state, "artifacts", "agents", config.name)
	if err := os.MkdirAll(agentArtifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	for role, name := range map[string]string{"receiver": receiverName, "sender": senderName} {
		role, name := role, name
		t.Cleanup(func() {
			err := persistThenRemoveAgent(
				func() error {
					return persistAgentLog(config.repository, name, filepath.Join(agentArtifacts, role+".log"), agentLogSecrets(config)...)
				},
				func() { _ = runCommandAllowFailure(config.repository, 5*time.Second, "docker", "rm", "-f", "-v", name) },
			)
			if err != nil {
				t.Errorf("persist %s agent log before cleanup: %v", role, err)
			}
		})
	}
	receiverID := startAgent(t, config, sideB, receiverName, "receiver", config.credentials["CYNAPSA_EJABBERD_AGENT_B"], config.credentials["CYNAPSA_EJABBERD_PASSWORD_B"], config.credentials["CYNAPSA_EJABBERD_AGENT_A"])
	waitForLog(t, config.repository, receiverName, `"event":"ready"`, 30*time.Second)
	senderID := startAgent(t, config, sideA, senderName, "sender", config.credentials["CYNAPSA_EJABBERD_AGENT_A"], config.credentials["CYNAPSA_EJABBERD_PASSWORD_A"], config.credentials["CYNAPSA_EJABBERD_AGENT_B"])
	assertOnlyNetwork(t, config.repository, senderID, sideA.network)
	assertOnlyNetwork(t, config.repository, receiverID, sideB.network)
	if config.remoteSmoke {
		assertAgentConfigurationIsAuthorityOnly(t, config, senderID, receiverID)
		waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"ready"`, 20*time.Second)
		waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"ready"`, 20*time.Second)
		writeControl(t, sideA, "establish-released")
		writeControl(t, sideB, "establish-released")
		waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"smoke-barrier-ready"`, 80*time.Second)
		waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"smoke-barrier-ready"`, 80*time.Second)
		for _, side := range []natSide{sideA, sideB} {
			runCommand(t, config.repository, 10*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "fault-xmpp")
		}
		writeControl(t, sideA, "smoke-released")
		writeControl(t, sideB, "smoke-released")
		waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"peer-before"`, 20*time.Second)
		waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"peer-before"`, 20*time.Second)
		writeControl(t, sideA, "request-released")
		writeControl(t, sideB, "request-released")
		waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"request-phase-complete"`, 20*time.Second)
		waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"request-phase-complete"`, 20*time.Second)
		writeControl(t, sideA, "reverse-released")
		writeControl(t, sideB, "reverse-released")
		waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"reverse-phase-complete"`, 20*time.Second)
		waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"reverse-phase-complete"`, 20*time.Second)
		writeControl(t, sideA, "post-health-released")
		writeControl(t, sideB, "post-health-released")
	}
	if config.recovery {
		waitForLog(t, config.repository, senderName, `"event":"fault-ready"`, recoveryFaultReadyTimeout)
		for _, side := range []natSide{sideA, sideB} {
			runCommand(t, config.repository, 10*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "fault-udp")
		}
		// Hold the UDP fault long enough to exceed the two-missed-probe health
		// threshold, while remaining strictly below the 30-second application
		// lane idle retirement. The following fallback request refreshes lane
		// activity before TURN is restored, so the 12-second recovery gate below
		// measures in-place ICE restart rather than full link replacement.
		time.Sleep(15 * time.Second)
		writeControl(t, sideA, "drop-released")
		waitForLog(t, config.repository, senderName, `"event":"fallback"`, 30*time.Second)
		for _, side := range []natSide{sideA, sideB} {
			runCommand(t, config.repository, 10*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "snapshot")
			faultRules, err := os.ReadFile(filepath.Join(side.evidence, "iptables.txt"))
			if err != nil {
				t.Fatal(err)
			}
			if !hasPositiveIPTablesCounter(string(faultRules), "-A CYNAPSA_FAULT", "-p udp", "-j DROP") {
				t.Fatalf("UDP fault lacks packet evidence on %s:\n%s", side.gatewayName, faultRules)
			}
			if err = os.WriteFile(filepath.Join(side.evidence, "fault-iptables.txt"), faultRules, 0o600); err != nil {
				t.Fatal(err)
			}
			runCommand(t, config.repository, 10*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "restore-udp")
		}
		writeControl(t, sideA, "restore-released")
	}
	terminalTimeout := 60 * time.Second
	if !config.expectLive {
		terminalTimeout = 100 * time.Second
	} else if config.recovery {
		terminalTimeout = 120 * time.Second
	}
	waitForAgentPairLog(t, config.repository, senderName, receiverName, `"event":"terminal-ready"`, terminalTimeout)
	waitForAgentPairLog(t, config.repository, receiverName, senderName, `"event":"terminal-ready"`, terminalTimeout)
	if config.remoteSmoke {
		for _, side := range []natSide{sideA, sideB} {
			runCommand(t, config.repository, 5*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "snapshot")
			assertXMPPFaultEvidence(t, side, config.ejabberdAddress)
			runCommand(t, config.repository, 5*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "restore-xmpp")
		}
	}
	writeControl(t, sideA, "finish-released")
	writeControl(t, sideB, "finish-released")
	waitTimeout := 80 * time.Second
	if !config.expectLive {
		waitTimeout = 120 * time.Second
	} else if config.recovery {
		waitTimeout = 140 * time.Second
	}
	for _, process := range []struct{ name, id string }{{senderName, senderID}, {receiverName, receiverID}} {
		waitResult := runCommandAllowFailure(config.repository, waitTimeout, "docker", "wait", process.id)
		status := strings.TrimSpace(waitResult.output)
		if waitResult.exitCode != 0 || status != "0" {
			senderFailure := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "logs", senderID)
			receiverFailure := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "logs", receiverID)
			t.Fatalf("agent %s wait exit=%d status=%q:\nsender:\n%s\nreceiver:\n%s", process.name, waitResult.exitCode, status, senderFailure.output, receiverFailure.output)
		}
	}
	senderLog := runCommand(t, config.repository, 5*time.Second, "docker", "logs", senderID)
	receiverLog := runCommand(t, config.repository, 5*time.Second, "docker", "logs", receiverID)
	if err := persistAgentLog(config.repository, senderID, filepath.Join(agentArtifacts, "sender.log"), agentLogSecrets(config)...); err != nil {
		t.Fatal(err)
	}
	if err := persistAgentLog(config.repository, receiverID, filepath.Join(agentArtifacts, "receiver.log"), agentLogSecrets(config)...); err != nil {
		t.Fatal(err)
	}
	for _, side := range []natSide{sideA, sideB} {
		runCommand(t, config.repository, 5*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "snapshot")
		assertNATEvidence(t, side, config.natProfile, !config.initialUDPBlocked)
		if config.initialUDPBlocked {
			assertUDPFaultEvidence(t, side)
		}
	}
	runCommand(t, config.repository, 10*time.Second, "docker", "rm", "-f", "-v", senderID, receiverID)
	stopNATSide(config.repository, sideA)
	stopNATSide(config.repository, sideB)
	for _, side := range []natSide{sideA, sideB} {
		if result := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "inspect", side.gatewayID); result.exitCode == 0 {
			t.Errorf("NAT gateway remained after pair teardown: %s", side.gatewayID)
		}
		if result := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "network", "inspect", side.network); result.exitCode == 0 {
			t.Errorf("agent private network remained after pair teardown: %s", side.network)
		}
	}
	for _, secret := range []string{config.credentials["CYNAPSA_EJABBERD_PASSWORD_A"], config.credentials["CYNAPSA_EJABBERD_PASSWORD_B"], config.turnSecret} {
		if secret != "" && (strings.Contains(senderLog, secret) || strings.Contains(receiverLog, secret)) {
			t.Fatal("agent output contains a generated credential")
		}
	}
	return agentPairResult{sender: decodeReports(t, senderLog), receiver: decodeReports(t, receiverLog)}
}

func agentLogSecrets(config agentPairConfig) []string {
	return []string{config.credentials["CYNAPSA_EJABBERD_PASSWORD_A"], config.credentials["CYNAPSA_EJABBERD_PASSWORD_B"], config.turnSecret}
}

func persistThenRemoveAgent(persist func() error, remove func()) error {
	err := persist()
	remove()
	return err
}

func persistAgentLog(repository, container, path string, secrets ...string) error {
	if repository == "" || container == "" || path == "" {
		return nil
	}
	output := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", container)
	if output.exitCode != 0 {
		if retainedAgentLogExists(path) {
			return nil
		}
		return fmt.Errorf("read agent log for %s: exit=%d: %s", container, output.exitCode, strings.TrimSpace(output.output))
	}
	return persistAgentLogOutput(path, output.output, secrets)
}

func retainedAgentLogExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}

func persistAgentLogOutput(path, output string, secrets []string) error {
	if path == "" || output == "" {
		return nil
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(output, secret) {
			return errors.New("agent output contains a generated credential")
		}
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".agent-log-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.WriteString(output)
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func assertAgentConfigurationIsAuthorityOnly(t *testing.T, config agentPairConfig, containers ...string) {
	t.Helper()
	for _, container := range containers {
		inspection := runCommand(t, config.repository, 5*time.Second, "docker", "inspect", "-f", "{{json .Config.Env}}|{{json .Mounts}}", container)
		for _, forbidden := range []string{"CYNAPSA_PRIVATE_CONNECTIVITY_PROFILE", "CYNAPSA_PRIVATE_CONNECTIVITY_PROFILE_FILE", "connectivity.json", "CYNAPSA_TURN_REST_SECRET", config.turnSecret} {
			if forbidden != "" && strings.Contains(inspection, forbidden) {
				t.Fatalf("agent %s retained forbidden private connectivity input", container)
			}
		}
	}
}

func startNATSide(t *testing.T, config agentPairConfig, side, suffix string) natSide {
	t.Helper()
	network := "cynapsa-nat-" + side + "-" + config.name + "-" + suffix
	gatewayName := "cynapsa-gateway-" + side + "-" + config.name + "-" + suffix
	owner, err := newNATSideOwner(gatewayName)
	if err != nil {
		t.Fatal(err)
	}
	result := natSide{network: network, owner: owner, gatewayName: gatewayName}
	runner := func(timeout time.Duration, arguments ...string) commandResult {
		return runCommandAllowFailure(config.repository, timeout, "docker", arguments...)
	}
	// Register cleanup before the first Docker mutation. A timed-out network or
	// container creation can succeed remotely while the host command reports a
	// failure. Cleanup only receives IDs after this invocation's unpredictable
	// owner label has been inspected; it never deletes a mutable name.
	t.Cleanup(func() { stopNATSideWithRunner(result, runner) })
	// Docker's internal-bridge host policy drops frames whose IP destination is
	// outside the bridge before a container router can forward them. Use an
	// ordinary isolated bridge, then replace and verify the unprivileged agent's
	// sole default route so its only usable egress is the dedicated gateway.
	result.networkID, err = createOwnedNetwork(network, owner, runner, newGatewayRetry)
	if err != nil {
		t.Fatal(err)
	}
	subnet := runCommand(t, config.repository, 5*time.Second, "docker", "network", "inspect", "-f", "{{(index .IPAM.Config 0).Subnet}}", result.networkID)
	serviceSubnet := runCommand(t, config.repository, 5*time.Second, "docker", "network", "inspect", "-f", "{{(index .IPAM.Config 0).Subnet}}", config.serviceNetwork)
	gatewayAddress := addressAt(t, subnet, 2)
	agentAddress := addressAt(t, subnet, 3)
	serviceOffset := 5
	switch side {
	case "a":
	case "b":
		serviceOffset = 6
	default:
		t.Fatalf("unknown NAT side %q", side)
	}
	serviceAddress := addressAt(t, serviceSubnet, serviceOffset)
	evidence := filepath.Join(config.state, "artifacts", "nat", config.name, side)
	if err := os.MkdirAll(evidence, 0o700); err != nil {
		t.Fatal(err)
	}
	gatewayScript := filepath.Join(config.repository, "test", "production", "coturn", "nat_gateway.sh")
	arguments := []string{
		"--name", gatewayName, "--network", network, "--ip", gatewayAddress,
		"--read-only", "--cpus", "0.25", "--memory", "96m", "--pids-limit", "64",
		"--security-opt", "no-new-privileges", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN",
		"--sysctl", "net.ipv4.ip_forward=1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=4m",
		"--mount", "type=bind,src=" + gatewayScript + ",dst=/gateway.sh,readonly",
		"--mount", "type=bind,src=" + evidence + ",dst=/evidence",
		"-e", "CYNAPSA_NAT_PROFILE=" + config.natProfile,
		"-e", "CYNAPSA_NAT_PRIVATE_ADDRESS=" + gatewayAddress,
		"-e", "CYNAPSA_NAT_PRIVATE_CIDR=" + subnet,
		"-e", "CYNAPSA_NAT_AGENT_ADDRESS=" + agentAddress,
		"-e", "CYNAPSA_NAT_STUN_ADDRESS=" + config.stunAddress,
		"-e", "CYNAPSA_NAT_XMPP_ADDRESS=" + config.ejabberdAddress,
		"--entrypoint", "/gateway.sh", pinnedGatewayImage, "configure",
	}
	specification := gatewayLifecycleSpec{
		name: gatewayName, owner: owner,
		privateNetwork: network, privateAddress: gatewayAddress,
		serviceNetwork: config.serviceNetwork, serviceAddress: serviceAddress,
	}
	gatewayID, err := createAttachStartGateway(specification, arguments, runner, newGatewayRetry)
	result.gatewayID = gatewayID
	if err != nil {
		t.Fatal(err)
	}
	result.subnet = subnet
	result.gatewayAddress = gatewayAddress
	result.agentAddress = agentAddress
	result.evidence = evidence
	waitForFile(t, filepath.Join(evidence, "ready"), 20*time.Second)
	return result
}

func newNATSideOwner(name string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate NAT side owner: %w", err)
	}
	return name + "-" + hex.EncodeToString(random), nil
}

type ownedNetworkInspection struct {
	id, name, owner string
}

const ownedNetworkInspectFormat = `{{.Id}}|{{.Name}}|{{index .Labels "` + gatewayOwnerLabel + `"}}`

func createOwnedNetwork(name, owner string, run gatewayCommandRunner, retryFactory gatewayRetryFactory) (string, error) {
	if name == "" || owner == "" || strings.ContainsAny(name+owner, "|\r\n") {
		return "", errors.New("owned network identity is invalid")
	}
	if run == nil || retryFactory == nil {
		return "", errors.New("owned network command runner is unavailable")
	}
	created := run(10*time.Second, "network", "create", "--label", gatewayOwnerLabel+"="+owner, "--driver", "bridge", name)
	target := name
	expectedID := ""
	if created.exitCode == 0 && !created.timedOut {
		fields := strings.Fields(created.output)
		if len(fields) != 1 {
			return "", fmt.Errorf("network %s create returned an invalid identity", name)
		}
		expectedID = fields[0]
		target = expectedID
	} else if !created.timedOut {
		return "", fmt.Errorf("network %s create failed with exit %d", name, created.exitCode)
	}
	inspection, err := awaitOwnedNetworkInspection(run, retryFactory(), target, expectedID, name, owner)
	if err != nil {
		return "", fmt.Errorf("network %s creation verification: %w", name, err)
	}
	return inspection.id, nil
}

func awaitOwnedNetworkInspection(run gatewayCommandRunner, retry func() bool, target, expectedID, name, owner string) (ownedNetworkInspection, error) {
	for {
		result := run(5*time.Second, "network", "inspect", "-f", ownedNetworkInspectFormat, target)
		if result.exitCode == 0 && !result.timedOut {
			inspection, err := parseOwnedNetworkInspection(result.output)
			if err != nil {
				return ownedNetworkInspection{}, err
			}
			if (expectedID != "" && inspection.id != expectedID) || inspection.name != name || inspection.owner != owner {
				return ownedNetworkInspection{}, errors.New("network identity or ownership label mismatch")
			}
			return inspection, nil
		}
		if !retry() {
			return ownedNetworkInspection{}, fmt.Errorf("network inspection of %s did not verify ownership", target)
		}
	}
}

func parseOwnedNetworkInspection(output string) (ownedNetworkInspection, error) {
	fields := strings.Split(strings.TrimSpace(output), "|")
	if len(fields) != 3 || fields[0] == "" || fields[1] == "" || fields[2] == "" {
		return ownedNetworkInspection{}, errors.New("malformed owned network inspection")
	}
	return ownedNetworkInspection{id: fields[0], name: fields[1], owner: fields[2]}, nil
}

type gatewayLifecycleSpec struct {
	name, owner                    string
	privateNetwork, privateAddress string
	serviceNetwork, serviceAddress string
}

type gatewayNetworkAttachment struct {
	IPAMConfig *struct {
		IPv4Address string `json:"IPv4Address"`
	} `json:"IPAMConfig"`
	IPAddress string `json:"IPAddress"`
}

type gatewayInspection struct {
	id, name, owner, status string
	running                 bool
	exitCode                string
	networks                map[string]gatewayNetworkAttachment
}

type gatewayInspectionPhase uint8

const (
	gatewayCreated gatewayInspectionPhase = iota
	gatewayAttached
	gatewayRunning
)

type gatewayCommandRunner func(timeout time.Duration, arguments ...string) commandResult
type gatewayRetryFactory func() func() bool

const gatewayInspectFormat = `{{.Id}}|{{.Name}}|{{index .Config.Labels "` + gatewayOwnerLabel + `"}}|{{.State.Status}}|{{.State.Running}}|{{.State.ExitCode}}|{{json .NetworkSettings.Networks}}`

func newGatewayRetry() func() bool {
	deadline := time.Now().Add(containerRecoveryTimeout)
	return func() bool {
		if time.Now().Add(agentRoutePollInterval).After(deadline) {
			return false
		}
		time.Sleep(agentRoutePollInterval)
		return true
	}
}

// createAttachStartGateway prevents the gateway process from observing a
// half-built topology. Timeout recovery is state-based, but never adopts a
// container unless its exact identity, ownership, lifecycle, and IP bindings
// match the phase that the host has completed.
func createAttachStartGateway(specification gatewayLifecycleSpec, createArguments []string, run gatewayCommandRunner, retryFactory gatewayRetryFactory) (string, error) {
	if err := validateGatewayLifecycleSpec(specification); err != nil {
		return "", err
	}
	if run == nil || retryFactory == nil {
		return "", errors.New("gateway command runner is unavailable")
	}
	arguments := []string{"create", "--label", gatewayOwnerLabel + "=" + specification.owner}
	arguments = append(arguments, createArguments...)
	created := run(15*time.Second, arguments...)
	lookup := specification.name
	expectedID := ""
	if created.exitCode == 0 && !created.timedOut {
		fields := strings.Fields(created.output)
		if len(fields) != 1 {
			return "", fmt.Errorf("gateway %s create returned an invalid container identity", specification.name)
		}
		expectedID = fields[0]
		lookup = expectedID
	} else if !created.timedOut {
		return "", fmt.Errorf("gateway %s create failed with exit %d", specification.name, created.exitCode)
	}

	inspection, err := awaitGatewayInspection(run, retryFactory(), lookup, expectedID, specification, gatewayCreated)
	if err != nil {
		return "", fmt.Errorf("gateway %s creation verification: %w", specification.name, err)
	}
	expectedID = inspection.id

	connected := run(10*time.Second, "network", "connect", "--ip", specification.serviceAddress, specification.serviceNetwork, expectedID)
	if connected.exitCode != 0 && !connected.timedOut {
		return expectedID, fmt.Errorf("gateway %s WAN attachment failed with exit %d", specification.name, connected.exitCode)
	}
	if _, err = awaitGatewayInspection(run, retryFactory(), expectedID, expectedID, specification, gatewayAttached); err != nil {
		return expectedID, fmt.Errorf("gateway %s WAN attachment verification: %w", specification.name, err)
	}

	started := run(10*time.Second, "start", expectedID)
	if started.exitCode != 0 && !started.timedOut {
		return expectedID, fmt.Errorf("gateway %s start failed with exit %d", specification.name, started.exitCode)
	}
	if _, err = awaitGatewayInspection(run, retryFactory(), expectedID, expectedID, specification, gatewayRunning); err != nil {
		return expectedID, fmt.Errorf("gateway %s start verification: %w", specification.name, err)
	}
	return expectedID, nil
}

func validateGatewayLifecycleSpec(specification gatewayLifecycleSpec) error {
	for label, value := range map[string]string{
		"name": specification.name, "owner": specification.owner,
		"private network": specification.privateNetwork, "service network": specification.serviceNetwork,
	} {
		if value == "" || strings.ContainsAny(value, "|\r\n") {
			return fmt.Errorf("gateway %s is invalid", label)
		}
	}
	if specification.privateNetwork == specification.serviceNetwork {
		return errors.New("gateway private and service networks must differ")
	}
	privateAddress, privateErr := netip.ParseAddr(specification.privateAddress)
	serviceAddress, serviceErr := netip.ParseAddr(specification.serviceAddress)
	if privateErr != nil || !privateAddress.Is4() || serviceErr != nil || !serviceAddress.Is4() || privateAddress == serviceAddress {
		return errors.New("gateway network addressing is invalid")
	}
	return nil
}

func awaitGatewayInspection(run gatewayCommandRunner, retry func() bool, target, expectedID string, specification gatewayLifecycleSpec, phase gatewayInspectionPhase) (gatewayInspection, error) {
	for {
		result := run(5*time.Second, "inspect", "-f", gatewayInspectFormat, target)
		if result.exitCode == 0 && !result.timedOut {
			inspection, err := parseGatewayInspection(result.output)
			if err != nil {
				return gatewayInspection{}, err
			}
			ready, retryable, validationErr := validateGatewayInspection(inspection, expectedID, specification, phase)
			if validationErr != nil {
				return gatewayInspection{}, validationErr
			}
			if ready {
				return inspection, nil
			}
			if !retryable {
				return gatewayInspection{}, errors.New("gateway inspection did not reach the required state")
			}
		}
		if !retry() {
			return gatewayInspection{}, fmt.Errorf("gateway inspection of %s did not reach phase %d", target, phase)
		}
	}
}

func parseGatewayInspection(output string) (gatewayInspection, error) {
	fields := strings.SplitN(strings.TrimSpace(output), "|", 7)
	if len(fields) != 7 || fields[0] == "" || fields[1] == "" || fields[2] == "" || fields[3] == "" || (fields[4] != "true" && fields[4] != "false") || fields[5] == "" {
		return gatewayInspection{}, errors.New("malformed gateway inspection")
	}
	inspection := gatewayInspection{
		id: fields[0], name: fields[1], owner: fields[2], status: fields[3],
		running: fields[4] == "true", exitCode: fields[5],
	}
	if err := json.Unmarshal([]byte(fields[6]), &inspection.networks); err != nil || inspection.networks == nil {
		return gatewayInspection{}, errors.New("malformed gateway network inspection")
	}
	return inspection, nil
}

func validateGatewayInspection(inspection gatewayInspection, expectedID string, specification gatewayLifecycleSpec, phase gatewayInspectionPhase) (ready, retryable bool, err error) {
	if expectedID != "" && inspection.id != expectedID {
		return false, false, fmt.Errorf("wrong gateway container identity %s", inspection.id)
	}
	if inspection.name != "/"+specification.name || inspection.owner != specification.owner {
		return false, false, errors.New("gateway container name or ownership label mismatch")
	}
	if inspection.exitCode != "0" {
		return false, false, fmt.Errorf("gateway container has exit code %s", inspection.exitCode)
	}

	private, exists := inspection.networks[specification.privateNetwork]
	if !exists || !gatewayAttachmentHasAddress(private, specification.privateAddress, phase == gatewayRunning) {
		return false, false, errors.New("gateway private network attachment mismatch")
	}
	service, serviceExists := inspection.networks[specification.serviceNetwork]
	switch phase {
	case gatewayCreated:
		if len(inspection.networks) != 1 || serviceExists {
			return false, false, errors.New("gateway had a WAN attachment before the controlled connect step")
		}
		if inspection.status != "created" || inspection.running {
			return false, false, errors.New("gateway was running before WAN verification")
		}
		return true, false, nil
	case gatewayAttached, gatewayRunning:
		if !serviceExists {
			if len(inspection.networks) == 1 && inspection.status == "created" && !inspection.running {
				return false, true, nil
			}
			return false, false, errors.New("gateway service network attachment is missing")
		}
		if len(inspection.networks) != 2 || !gatewayAttachmentHasAddress(service, specification.serviceAddress, phase == gatewayRunning) {
			return false, false, errors.New("gateway service network attachment mismatch")
		}
	default:
		return false, false, errors.New("unknown gateway inspection phase")
	}

	if phase == gatewayAttached {
		if inspection.status != "created" || inspection.running {
			return false, false, errors.New("gateway was running before WAN verification")
		}
		return true, false, nil
	}
	if inspection.status == "created" && !inspection.running {
		return false, true, nil
	}
	if inspection.status != "running" || !inspection.running {
		return false, false, fmt.Errorf("gateway stopped during startup with state %s", inspection.status)
	}
	return true, false, nil
}

func gatewayAttachmentHasAddress(attachment gatewayNetworkAttachment, expected string, requireActive bool) bool {
	if attachment.IPAMConfig == nil || attachment.IPAMConfig.IPv4Address != expected {
		return false
	}
	if requireActive {
		return attachment.IPAddress == expected
	}
	return attachment.IPAddress == "" || attachment.IPAddress == expected
}

func stopNATSide(repository string, side natSide) {
	runner := func(timeout time.Duration, arguments ...string) commandResult {
		return runCommandAllowFailure(repository, timeout, "docker", arguments...)
	}
	stopNATSideWithRunner(side, runner)
}

func stopNATSideWithRunner(side natSide, run gatewayCommandRunner) {
	if run == nil || side.owner == "" {
		return
	}
	if side.gatewayID != "" {
		result := run(5*time.Second, "inspect", "-f", gatewayInspectFormat, side.gatewayID)
		if result.exitCode == 0 && !result.timedOut {
			inspection, err := parseGatewayInspection(result.output)
			if err == nil && inspection.id == side.gatewayID && inspection.name == "/"+side.gatewayName && inspection.owner == side.owner {
				_ = run(5*time.Second, "rm", "-f", side.gatewayID)
			}
		}
	}
	if side.networkID != "" {
		result := run(5*time.Second, "network", "inspect", "-f", ownedNetworkInspectFormat, side.networkID)
		if result.exitCode == 0 && !result.timedOut {
			inspection, err := parseOwnedNetworkInspection(result.output)
			if err == nil && inspection.id == side.networkID && inspection.name == side.network && inspection.owner == side.owner {
				_ = run(5*time.Second, "network", "rm", side.networkID)
			}
		}
	}
}

func startAgent(t *testing.T, config agentPairConfig, side natSide, name, role, agentID, password, peer string) string {
	t.Helper()
	uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
	environmentLines := []string{
		"SSL_CERT_FILE=/ca.pem",
		"CYNAPSA_EJABBERD_ENDPOINT=mesh.test:5222",
		"CYNAPSA_MESH_ID=e2e-mesh",
		"CYNAPSA_AGENT_ID=" + agentID,
		"CYNAPSA_AGENT_PASSWORD=" + password,
		"CYNAPSA_PEER_ID=" + peer,
		"CYNAPSA_EXPECT_LIVE=" + strconv.FormatBool(config.expectLive),
		"CYNAPSA_RECOVERY=" + strconv.FormatBool(config.recovery),
		"CYNAPSA_REMOTE_SMOKE=" + strconv.FormatBool(config.remoteSmoke),
		"CYNAPSA_SMOKE_NONCE=" + config.smokeNonce,
		"CYNAPSA_NAT_GATEWAY=" + side.gatewayAddress,
		"CYNAPSA_AGENT_ROLE=" + role,
	}
	environmentFile := writePrivateAgentEnvironmentFile(t, environmentLines)
	// Route setup is performed from a short-lived privileged helper sharing this
	// container's network namespace. Docker Desktop may need several seconds to
	// publish that namespace, so keep the unprivileged agent dormant until the
	// host has independently verified the installed default route. Source the
	// mode-0600 bind-mounted environment so generated credentials never appear
	// in the host Docker command line or the retained Docker environment.
	waitScript := `set -a; . /run/cynapsa-agent.env; set +a; deadline=$(( $(date +%s) + 120 )); while [ ! -f /control/route-ready ]; do [ "$(date +%s)" -lt "$deadline" ] || exit 70; sleep 1; done; exec /agent "$CYNAPSA_AGENT_ROLE"`
	arguments := []string{
		"run", "-d", "--name", name, "--network", side.network, "--ip", side.agentAddress, "--read-only", "--user", uid + ":" + gid,
		"--cpus", "0.5", "--memory", "128m", "--pids-limit", "64", "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
		"--add-host", "mesh.test:" + config.ejabberdAddress, "--add-host", "stun.test:" + config.stunAddress, "--add-host", "turn.test:" + config.turnAddress,
		"--mount", "type=bind,src=" + config.agent + ",dst=/agent,readonly",
		"--mount", "type=bind,src=" + config.credentials["CYNAPSA_EJABBERD_CA"] + ",dst=/ca.pem,readonly",
		"--mount", "type=bind,src=" + side.evidence + ",dst=/control,readonly",
		"--mount", "type=bind,src=" + environmentFile + ",dst=/run/cynapsa-agent.env,readonly",
		"--entrypoint", "/bin/sh", pinnedAgentImage, "-c", waitScript,
	}
	id := startNamedContainer(t, config.repository, name, 15*time.Second, arguments...)
	route := waitForAgentRoute(t, config.repository, id, name, side)
	if err := os.WriteFile(filepath.Join(side.evidence, "agent-route-"+role+".txt"), []byte(route+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(side.evidence, "route-ready"), []byte("ready\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return id
}
func writePrivateAgentEnvironmentFile(t *testing.T, lines []string) string {
	t.Helper()
	for _, line := range lines {
		if line == "" || !strings.Contains(line, "=") || strings.ContainsAny(line, "\r\n") {
			t.Fatal("refusing to write malformed private agent environment entry")
		}
	}
	path := filepath.Join(t.TempDir(), "agent.env")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write private agent environment: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("inspect private agent environment: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private agent environment mode=%v, want 0600", info.Mode().Perm())
	}
	return path
}

func startNamedContainer(t *testing.T, repository, name string, timeout time.Duration, arguments ...string) string {
	t.Helper()
	result := runCommandAllowFailure(repository, timeout, "docker", arguments...)
	if result.exitCode == 0 {
		if id := strings.TrimSpace(result.output); id != "" {
			return id
		}
	}

	// Docker Desktop can time out the host CLI after `docker run -d` has
	// already created and started the named container. The name is the stable
	// idempotency key: recover daemon state instead of starting a duplicate or
	// reporting a false environment failure.
	deadline := time.Now().Add(containerRecoveryTimeout)
	last := commandResult{exitCode: -1}
	for {
		last = runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", "-f",
			"{{.Id}}|{{.State.Running}}|{{.State.ExitCode}}", name)
		if last.exitCode == 0 {
			id, running, exitCode, ok := parseContainerInspection(last.output)
			if ok {
				if running {
					return id
				}
				t.Fatalf("container %s stopped during startup: running=%s exit=%s (docker run exit=%d; command output omitted because it may contain credentials)",
					name, strconv.FormatBool(running), exitCode, result.exitCode)
			}
		}
		if time.Now().Add(agentRoutePollInterval).After(deadline) {
			t.Fatalf("container %s did not become inspectable after docker run exit=%d (inspect exit=%d; command output omitted because it may contain credentials)",
				name, result.exitCode, last.exitCode)
		}
		time.Sleep(agentRoutePollInterval)
	}
}

func parseContainerInspection(output string) (id string, running bool, exitCode string, ok bool) {
	fields := strings.Split(strings.TrimSpace(output), "|")
	if len(fields) != 3 || fields[0] == "" || (fields[1] != "true" && fields[1] != "false") || fields[2] == "" {
		return "", false, "", false
	}
	return fields[0], fields[1] == "true", fields[2], true
}

func waitForAgentRoute(t *testing.T, repository, agentID, agentName string, side natSide) string {
	t.Helper()
	controllerName := agentName + "-route-setup"
	inspectorName := agentName + "-route-inspect"
	cleanupHelper := func(name string) {
		_ = runCommandAllowFailure(repository, 5*time.Second, "docker", "rm", "-f", name)
	}
	t.Cleanup(func() {
		cleanupHelper(controllerName)
		cleanupHelper(inspectorName)
	})

	deadline := time.Now().Add(agentRouteReadyTimeout)
	attempts := 0
	var controller, inspection, state, gatewayState commandResult
	for {
		attempts++
		cleanupHelper(controllerName)
		cleanupHelper(inspectorName)

		controller = runCommandAllowFailure(repository, agentRouteAttemptTimeout, "docker", "run", "--rm", "--name", controllerName,
			"--network", "container:"+agentID, "--security-opt", "no-new-privileges", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN",
			"--entrypoint", "ip", pinnedGatewayImage, "route", "replace", "default", "via", side.gatewayAddress)
		// Always verify using a fresh helper. A Docker CLI timeout can occur after
		// the idempotent route replacement has already completed in the shared
		// namespace, and retrying a proven-successful operation only adds noise.
		inspection = runCommandAllowFailure(repository, agentRouteAttemptTimeout, "docker", "run", "--rm", "--name", inspectorName,
			"--network", "container:"+agentID, "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
			"--entrypoint", "ip", pinnedGatewayImage, "route", "show")
		if ordinaryRouteVerified(inspection, side.gatewayAddress) {
			cleanupHelper(controllerName)
			cleanupHelper(inspectorName)
			return strings.TrimSpace(inspection.output)
		}

		state = runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", "-f", "{{.Id}}|{{.State.Running}}|{{.State.ExitCode}}", agentID)
		gatewayState = runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", "-f", "{{.Id}}|{{.State.Running}}|{{.State.ExitCode}}", side.gatewayID)
		if timedOutRouteVerified(inspection, side.gatewayAddress, state, agentID, gatewayState, side.gatewayID) {
			cleanupHelper(controllerName)
			cleanupHelper(inspectorName)
			return strings.TrimSpace(inspection.output)
		}
		if exactStoppedContainer(state, agentID) {
			t.Fatalf("agent exited before route readiness after %d attempts (%s):\nconfigure exit=%d:\n%s\nverify exit=%d:\n%s\ngateway: %s",
				attempts, strings.TrimSpace(state.output), controller.exitCode, controller.output, inspection.exitCode, inspection.output, strings.TrimSpace(gatewayState.output))
		}
		if exactStoppedContainer(gatewayState, side.gatewayID) {
			t.Fatalf("gateway exited before route readiness after %d attempts (%s):\nconfigure exit=%d:\n%s\nverify exit=%d:\n%s\nagent: %s",
				attempts, strings.TrimSpace(gatewayState.output), controller.exitCode, controller.output, inspection.exitCode, inspection.output, strings.TrimSpace(state.output))
		}
		if time.Now().Add(agentRoutePollInterval).After(deadline) {
			t.Fatalf("agent route did not become ready after %d attempts: want default via %s\nagent: %s\ngateway: %s\nconfigure exit=%d:\n%s\nverify exit=%d:\n%s",
				attempts, side.gatewayAddress, strings.TrimSpace(state.output), strings.TrimSpace(gatewayState.output),
				controller.exitCode, controller.output, inspection.exitCode, inspection.output)
		}
		time.Sleep(agentRoutePollInterval)
	}
}

func routeUsesGateway(route, gateway string) bool {
	defaultRoutes := 0
	for _, line := range strings.Split(route, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "default" {
			continue
		}
		defaultRoutes++
		if len(fields) < 3 || fields[1] != "via" || fields[2] != gateway {
			return false
		}
	}
	return defaultRoutes == 1
}

func ordinaryRouteVerified(route commandResult, gateway string) bool {
	return route.exitCode == 0 && !route.timedOut && routeUsesGateway(route.output, gateway)
}

func timedOutRouteVerified(route commandResult, gateway string, agent commandResult, agentID string, gatewayState commandResult, gatewayID string) bool {
	return route.timedOut && routeUsesGateway(route.output, gateway) && exactRunningContainer(agent, agentID) && exactRunningContainer(gatewayState, gatewayID)
}

func exactRunningContainer(result commandResult, expectedID string) bool {
	if result.exitCode != 0 {
		return false
	}
	id, running, exitCode, ok := parseContainerInspection(result.output)
	return ok && id == expectedID && running && exitCode == "0"
}

func exactStoppedContainer(result commandResult, expectedID string) bool {
	if result.exitCode != 0 {
		return false
	}
	id, running, _, ok := parseContainerInspection(result.output)
	return ok && id == expectedID && !running
}

func writeControl(t *testing.T, side natSide, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(side.evidence, name), []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForLog(t *testing.T, repository, container, pattern string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", container)
		if strings.Contains(output.output, pattern) {
			return
		}
		state := runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", "-f", "running={{.State.Running}} exit={{.State.ExitCode}} error={{.State.Error}}", container)
		if state.exitCode == 0 && strings.Contains(state.output, "running=false") {
			t.Fatalf("container %s exited before readiness (%s):\n%s", container, strings.TrimSpace(state.output), output.output)
		}
		time.Sleep(100 * time.Millisecond)
	}
	output := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", container)
	if logOutputContains(output, pattern) {
		return
	}
	t.Fatalf("container %s did not produce %q within %s:\n%s", container, pattern, timeout, output.output)
}

func waitForAgentPairLog(t *testing.T, repository, container, peer, pattern string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", container)
		if strings.Contains(output.output, pattern) {
			return
		}
		state := runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", "-f", "running={{.State.Running}} exit={{.State.ExitCode}} error={{.State.Error}}", container)
		if state.exitCode == 0 && strings.Contains(state.output, "running=false") {
			peerOutput := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", peer)
			t.Fatalf("container %s exited before readiness (%s):\n%s\npeer %s:\n%s", container, strings.TrimSpace(state.output), output.output, peer, peerOutput.output)
		}
		time.Sleep(100 * time.Millisecond)
	}
	output := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", container)
	if logOutputContains(output, pattern) {
		return
	}
	peerOutput := runCommandAllowFailure(repository, 5*time.Second, "docker", "logs", peer)
	t.Fatalf("container %s did not produce %q within %s:\n%s\npeer %s:\n%s", container, pattern, timeout, output.output, peer, peerOutput.output)
}

func logOutputContains(output commandResult, pattern string) bool {
	return pattern != "" && strings.Contains(output.output, pattern)
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("file did not become ready: %s", path)
}

func addressAt(t *testing.T, subnet string, offset int) string {
	t.Helper()
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || !prefix.Addr().Is4() {
		t.Fatalf("invalid IPv4 subnet %q: %v", subnet, err)
	}
	address := prefix.Masked().Addr()
	for range offset {
		address = address.Next()
		if !address.IsValid() || !prefix.Contains(address) {
			t.Fatalf("subnet %s has no address at offset %d", subnet, offset)
		}
	}
	return address.String()
}

func containerNetworkIPv4(t *testing.T, repository, container, network string) string {
	t.Helper()
	template := fmt.Sprintf(`{{with index .NetworkSettings.Networks %q}}{{.IPAddress}}{{end}}`, network)
	address := runCommand(t, repository, 5*time.Second, "docker", "inspect", "-f", template, container)
	parsed, err := netip.ParseAddr(address)
	if err != nil || !parsed.Is4() {
		t.Fatalf("container %s has invalid IPv4 address %q on %s", container, address, network)
	}
	return address
}

func assertNATEvidence(t *testing.T, side natSide, profile string, requireUDPFlow bool) {
	t.Helper()
	profileText, err := os.ReadFile(filepath.Join(side.evidence, "profile.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(profileText), "profile="+profile) || !strings.Contains(string(profileText), "private_cidr="+side.subnet) {
		t.Fatalf("NAT profile evidence mismatch: %s", profileText)
	}
	rules, err := os.ReadFile(filepath.Join(side.evidence, "iptables.txt"))
	if err != nil {
		t.Fatal(err)
	}
	ruleText := string(rules)
	t.Logf("NAT evidence gateway=%s profile=%s pre-nat={%s} translations={%s}",
		side.gatewayName, profile, natPacketEvidenceSummary(ruleText), natTranslationEvidenceSummary(ruleText, string(profileText)))
	if !hasIPTablesRule(ruleText, "-A POSTROUTING", "-j SNAT") || !hasPositiveIPTablesCounter(ruleText, "-A FORWARD") {
		t.Fatalf("NAT forwarding evidence is incomplete:\n%s", rules)
	}
	switch profile {
	case "full-cone":
		if !hasIPTablesRule(ruleText, "-A PREROUTING", "-p udp", "-j DNAT", "--to-destination "+side.agentAddress) {
			t.Fatalf("full-cone NAT evidence omits the explicit inbound mapping:\n%s", rules)
		}
	case "symmetric":
		profileValues := parseEvidenceFields(t, profileText)
		stunAddress, wanAddress := profileValues["stun_address"], profileValues["wan_address"]
		stunTarget := "--to-source " + wanAddress + ":40000"
		otherTarget := "--to-source " + wanAddress + ":40001-40999"
		if stunAddress == "" || wanAddress == "" ||
			!hasIPTablesRule(ruleText, "-A POSTROUTING", "-d "+stunAddress+"/32", "-p udp", "--dport 3478", stunTarget) ||
			!hasIPTablesRule(ruleText, "-A POSTROUTING", "-p udp", otherTarget, "--random-fully") {
			t.Fatalf("symmetric NAT evidence omits disjoint STUN/non-STUN mappings:\n%s", rules)
		}
		if requireUDPFlow {
			// Relay-only server profiles need not emit a STUN binding request,
			// but UDP TURN traffic must exercise the disjoint non-STUN mapping.
			if !hasPositiveIPTablesCounter(ruleText, "-A POSTROUTING", "-p udp", otherTarget, "--random-fully") {
				t.Fatalf("symmetric NAT non-STUN mapping lacks positive UDP traffic evidence (%s):\n%s",
					natPacketEvidenceSummary(ruleText), rules)
			}
		} else if !hasPositiveIPTablesCounter(ruleText, "-A POSTROUTING", "-j SNAT", "--to-source "+wanAddress) {
			// TCP-relay profiles intentionally block every UDP packet. Their
			// generic SNAT rule must instead show positive TCP/service egress.
			t.Fatalf("symmetric NAT lacks positive non-UDP egress evidence:\n%s", rules)
		}
	default:
		t.Fatalf("unsupported NAT evidence profile %q", profile)
	}
}

func logNATFailureEvidence(t *testing.T, config agentPairConfig, sides ...natSide) {
	t.Helper()
	for _, side := range sides {
		snapshot := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "exec", side.gatewayID, "/gateway.sh", "snapshot")
		if snapshot.exitCode != 0 {
			t.Logf("failure NAT evidence pair=%s gateway=%s snapshot-exit=%d output=%q", config.name, side.gatewayName, snapshot.exitCode, strings.TrimSpace(snapshot.output))
			continue
		}
		rules, rulesErr := os.ReadFile(filepath.Join(side.evidence, "iptables.txt"))
		profile, profileErr := os.ReadFile(filepath.Join(side.evidence, "profile.txt"))
		if rulesErr != nil || profileErr != nil {
			t.Logf("failure NAT evidence pair=%s gateway=%s rules-error=%v profile-error=%v", config.name, side.gatewayName, rulesErr, profileErr)
			continue
		}
		t.Logf("failure NAT evidence pair=%s gateway=%s pre-nat={%s} translations={%s}",
			config.name, side.gatewayName, natPacketEvidenceSummary(string(rules)), natTranslationEvidenceSummary(string(rules), string(profile)))
	}
}

func observeAgentsAfterHarnessFailure(t *testing.T, config agentPairConfig, sender, receiver string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		logs := runCommandAllowFailure(config.repository, 3*time.Second, "docker", "logs", sender)
		state := runCommandAllowFailure(config.repository, 3*time.Second, "docker", "inspect", "-f", "{{.State.Running}}", sender)
		if strings.Contains(logs.output, `"event":"terminal-ready"`) || strings.TrimSpace(state.output) == "false" || state.exitCode != 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	for role, container := range map[string]string{"sender": sender, "receiver": receiver} {
		state := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "inspect", "-f", "id={{.Id}} status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} restart_count={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}", container)
		logs := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "logs", "--timestamps", container)
		t.Logf("failure agent evidence pair=%s role=%s inspect-exit=%d state=%q logs-exit=%d logs-tail:\n%s",
			config.name, role, state.exitCode, strings.TrimSpace(state.output), logs.exitCode,
			boundedLogTail(redactEvidence(logs.output, agentLogSecrets(config)), 120, 32<<10))
	}
}

func logServiceFailureEvidence(t *testing.T, config agentPairConfig, since time.Time) {
	t.Helper()
	for service, container := range map[string]string{"turn": config.turnID, "ejabberd": config.ejabberdID} {
		if container == "" {
			continue
		}
		state := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "inspect", "-f", "id={{.Id}} status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} restart_count={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}", container)
		logs := runCommandAllowFailure(config.repository, 5*time.Second, "docker", "logs", "--since", since.Format(time.RFC3339Nano), "--timestamps", container)
		redacted := redactEvidence(logs.output, agentLogSecrets(config))
		t.Logf("failure service evidence pair=%s service=%s inspect-exit=%d state=%q logs-exit=%d protocol-summary={%s} relevant-log-tail:\n%s",
			config.name, service, state.exitCode, strings.TrimSpace(state.output), logs.exitCode,
			serviceProtocolSummary(redacted), relevantServiceLogTail(redacted, 60, 24<<10))
	}
	readiness, err := os.ReadFile(filepath.Join(config.state, "readiness.log"))
	if err != nil {
		t.Logf("failure service evidence pair=%s readiness-error=%v", config.name, err)
		return
	}
	t.Logf("failure service evidence pair=%s readiness-tail:\n%s", config.name, boundedLogTail(string(readiness), 20, 8<<10))
}

func serviceProtocolSummary(logs string) string {
	lower := strings.ToLower(logs)
	patterns := []struct{ name, text string }{
		{name: "turn_allocate_success", text: "allocate processed, success"},
		{name: "turn_allocate_error", text: "allocate processed, error"},
		{name: "turn_permission_success", text: "create_permission processed, success"},
		{name: "turn_permission_error", text: "create_permission processed, error"},
		{name: "turn_channel_success", text: "channel_bind processed, success"},
		{name: "turn_channel_error", text: "channel_bind processed, error"},
		{name: "turn_refresh_success", text: "refresh processed, success"},
		{name: "turn_refresh_error", text: "refresh processed, error"},
		{name: "xmpp_auth_accepted", text: "accepted c2s scram"},
		{name: "xmpp_session_opened", text: "opened c2s session"},
		{name: "xmpp_connection_failed", text: "closing c2s connection"},
		{name: "xmpp_replaced_conflict", text: "replaced by new connection (conflict)"},
		{name: "xmpp_resume_wait", text: "waiting 300 seconds for stream resumption"},
		{name: "mailbox_admission", text: "cynapsa_mailbox admission"},
		{name: "mailbox_ack", text: "cynapsa_mailbox ack"},
		{name: "jingle", text: "jingle"},
		{name: "error", text: "error"},
		{name: "warning", text: "warning"},
	}
	parts := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		parts = append(parts, fmt.Sprintf("%s=%d", pattern.name, strings.Count(lower, pattern.text)))
	}
	return strings.Join(parts, " ")
}

func relevantServiceLogTail(logs string, maxLines, maxBytes int) string {
	needles := []string{"allocat", "permission", "channel", "refresh", "jingle", "stun", "turn", "session", "stream", "auth", "error", "warn", "agent-a", "agent-b"}
	lines := make([]string, 0)
	for _, line := range strings.Split(logs, "\n") {
		lower := strings.ToLower(line)
		for _, needle := range needles {
			if strings.Contains(lower, needle) {
				lines = append(lines, line)
				break
			}
		}
	}
	if len(lines) == 0 {
		return boundedLogTail(logs, maxLines, maxBytes)
	}
	return boundedLogTail(strings.Join(lines, "\n"), maxLines, maxBytes)
}

func boundedLogTail(logs string, maxLines, maxBytes int) string {
	logs = strings.TrimSpace(logs)
	lines := strings.Split(logs, "\n")
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	result := strings.Join(lines, "\n")
	if maxBytes > 0 && len(result) > maxBytes {
		result = "[truncated to trailing bytes]\n" + result[len(result)-maxBytes:]
	}
	return result
}

func redactEvidence(value string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

func natPacketEvidenceSummary(rules string) string {
	labels := []string{
		"cynapsa-egress-stun-turn",
		"cynapsa-egress-udp",
		"cynapsa-egress-xmpp",
		"cynapsa-egress-tcp",
		"cynapsa-egress-any",
		"cynapsa-ingress-stun-turn",
		"cynapsa-ingress-udp",
		"cynapsa-ingress-xmpp",
		"cynapsa-ingress-tcp",
		"cynapsa-ingress-any",
	}
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		packets, bytes, found := iptablesCommentCounter(rules, label)
		if !found {
			parts = append(parts, label+"=missing")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%d:%d", label, packets, bytes))
	}
	return strings.Join(parts, " ")
}

func iptablesCommentCounter(rules, label string) (uint64, uint64, bool) {
	if label == "" || strings.ContainsAny(label, " \t\r\n") {
		return 0, 0, false
	}
	for _, line := range strings.Split(rules, "\n") {
		fields, matched := matchIPTablesRule(line, "-A CYNAPSA_EVIDENCE", "--comment "+label)
		if !matched {
			continue
		}
		packets, bytes, ok := parseIPTablesCounter(fields[0])
		if !ok {
			return 0, 0, false
		}
		return packets, bytes, true
	}
	return 0, 0, false
}

func natTranslationEvidenceSummary(rules, profile string) string {
	values := evidenceFields(profile)
	wanAddress := values["wan_address"]
	parts := []string{iptablesCounterSummary(rules, "generic-snat", "-A POSTROUTING", "-j SNAT", "--to-source "+wanAddress)}
	if values["profile"] == "symmetric" {
		parts = append(parts,
			iptablesCounterSummary(rules, "stun-snat", "-A POSTROUTING", "-d "+values["stun_address"]+"/32", "-p udp", "--dport 3478", "--to-source "+wanAddress+":40000"),
			iptablesCounterSummary(rules, "non-stun-udp-snat", "-A POSTROUTING", "-p udp", "--to-source "+wanAddress+":40001-40999", "--random-fully"),
		)
	}
	return strings.Join(parts, " ")
}

func evidenceFields(data string) map[string]string {
	values := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok && key != "" && value != "" {
			values[key] = value
		}
	}
	return values
}

func iptablesCounterSummary(rules, name string, tokenSequences ...string) string {
	packets, bytes, found := iptablesRuleCounter(rules, tokenSequences...)
	if !found {
		return name + "=missing"
	}
	return fmt.Sprintf("%s=%d:%d", name, packets, bytes)
}

func iptablesRuleCounter(rules string, tokenSequences ...string) (uint64, uint64, bool) {
	for _, line := range strings.Split(rules, "\n") {
		fields, matched := matchIPTablesRule(line, tokenSequences...)
		if !matched {
			continue
		}
		packets, bytes, valid := parseIPTablesCounter(fields[0])
		if !valid {
			return 0, 0, false
		}
		return packets, bytes, true
	}
	return 0, 0, false
}

func hasIPTablesRule(rules string, tokenSequences ...string) bool {
	for _, line := range strings.Split(rules, "\n") {
		if _, matched := matchIPTablesRule(line, tokenSequences...); matched {
			return true
		}
	}
	return false
}

func matchIPTablesRule(line string, tokenSequences ...string) ([]string, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || len(tokenSequences) == 0 {
		return nil, false
	}
	ruleFields := fields[1:]
	for _, sequence := range tokenSequences {
		required := strings.Fields(sequence)
		if len(required) == 0 || !containsTokenSequence(ruleFields, required) {
			return nil, false
		}
	}
	return fields, true
}

func containsTokenSequence(fields, required []string) bool {
	if len(required) == 0 || len(required) > len(fields) {
		return false
	}
	for start := 0; start <= len(fields)-len(required); start++ {
		matched := true
		for offset := range required {
			if fields[start+offset] != required[offset] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func parseEvidenceFields(t *testing.T, data []byte) map[string]string {
	t.Helper()
	fields := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || value == "" {
			t.Fatalf("malformed NAT profile evidence line %q", line)
		}
		if _, duplicate := fields[key]; duplicate {
			t.Fatalf("duplicate NAT profile evidence field %q", key)
		}
		fields[key] = value
	}
	return fields
}

func hasPositiveIPTablesCounter(rules string, tokenSequences ...string) bool {
	for _, line := range strings.Split(rules, "\n") {
		fields, matched := matchIPTablesRule(line, tokenSequences...)
		if !matched {
			continue
		}
		packets, bytes, valid := parseIPTablesCounter(fields[0])
		if valid && packets > 0 && bytes > 0 {
			return true
		}
	}
	return false
}

func parseIPTablesCounter(field string) (uint64, uint64, bool) {
	if len(field) < 5 || field[0] != '[' || field[len(field)-1] != ']' {
		return 0, 0, false
	}
	packetText, byteText, found := strings.Cut(field[1:len(field)-1], ":")
	if !found || packetText == "" || byteText == "" || strings.Contains(byteText, ":") {
		return 0, 0, false
	}
	packets, packetErr := strconv.ParseUint(packetText, 10, 64)
	bytes, byteErr := strconv.ParseUint(byteText, 10, 64)
	return packets, bytes, packetErr == nil && byteErr == nil
}

func assertUDPFaultEvidence(t *testing.T, side natSide) {
	t.Helper()
	rules, err := os.ReadFile(filepath.Join(side.evidence, "iptables.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !hasIPTablesRule(string(rules), "-A CYNAPSA_FAULT", "-p udp", "-j DROP") {
		t.Fatalf("UDP-independent path lacks an installed UDP block on %s:\n%s", side.gatewayName, rules)
	}
}

func assertXMPPFaultEvidence(t *testing.T, side natSide, address string) {
	t.Helper()
	rules, err := os.ReadFile(filepath.Join(side.evidence, "iptables.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !hasExactXMPPFaultCounter(string(rules), address) {
		t.Fatalf("XMPP blackhole lacks exact positive packet evidence on %s:\n%s", side.gatewayName, rules)
	}
}

func hasExactXMPPFaultCounter(rules, address string) bool {
	return hasPositiveIPTablesCounter(rules,
		"-A CYNAPSA_XMPP", "-d "+address+"/32", "-p tcp", "-m tcp", "--dport 5222", "-j DROP")
}

func assertOnlyNetwork(t *testing.T, repository, container, want string) {
	t.Helper()
	got := strings.Fields(runCommand(t, repository, 5*time.Second, "docker", "inspect", "-f", "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}", container))
	if len(got) != 1 || got[0] != want {
		t.Fatalf("container %s networks=%v, want only %s", container, got, want)
	}
}

func assertReports(t *testing.T, reports []agentReport, live bool) {
	t.Helper()
	if !hasReport(reports, func(report agentReport) bool { return report.Event == "peer" && report.Available == live }) ||
		!hasReport(reports, func(report agentReport) bool {
			return report.Event == "response" && report.Path == "/p2p/reply" && report.From == "agent-b@mesh.test" && report.Bytes == len("relay-response")
		}) ||
		!hasReport(reports, func(report agentReport) bool { return report.Event == "complete" }) {
		t.Fatalf("sender reports=%#v", reports)
	}
}

func assertReceiverReports(t *testing.T, reports []agentReport, live bool) {
	t.Helper()
	want := map[string]int{"/p2p/establish": len("durable-bootstrap"), "/p2p/request": len("relay-request"), "/p2p/complete": len("complete")}
	if live {
		want["/p2p/done"] = len("done")
	}
	for path, bytes := range want {
		if !hasReport(reports, func(report agentReport) bool {
			return report.Event == "message" && report.Path == path && report.From == "agent-a@mesh.test" && report.Bytes == bytes
		}) {
			t.Fatalf("receiver missing %s: %#v", path, reports)
		}
	}
}

func assertRemoteSmokeReports(t *testing.T, result agentPairResult, nonce string) {
	t.Helper()
	for role, reports := range map[string][]agentReport{"sender": result.sender, "receiver": result.receiver} {
		for _, event := range []string{"smoke-barrier-ready", "peer-before", "peer-after", "terminal-ready", "complete"} {
			if !hasReport(reports, func(report agentReport) bool {
				return report.Event == event && (event != "peer-before" && event != "peer-after" || report.Available)
			}) {
				t.Errorf("%s missing %s: %#v", role, event, reports)
			}
		}
	}
	for _, expected := range []agentReport{
		{Event: "response", Path: "/remote-smoke/reply/" + nonce + "-b", From: "agent-b@mesh.test", Bytes: len("reply-b-to-a")},
		{Event: "message", Path: "/remote-smoke/message/" + nonce + "-b", From: "agent-b@mesh.test", Bytes: len("message-b-to-a")},
		{Event: "accepted", Path: "/remote-smoke/message/" + nonce + "-b", From: "agent-b@mesh.test", Bytes: len("message-b-to-a")},
	} {
		if !hasReport(result.sender, func(report agentReport) bool {
			return report.Event == expected.Event && report.Path == expected.Path && report.From == expected.From && report.Bytes == expected.Bytes
		}) {
			t.Errorf("sender missing %#v: %#v", expected, result.sender)
		}
	}
	for _, expected := range []agentReport{
		{Event: "message", Path: "/remote-smoke/request/" + nonce + "-a", From: "agent-a@mesh.test", Bytes: len("request-a-to-b")},
		{Event: "accepted", Path: "/remote-smoke/request/" + nonce + "-a", From: "agent-a@mesh.test", Bytes: len("request-a-to-b")},
	} {
		if !hasReport(result.receiver, func(report agentReport) bool {
			return report.Event == expected.Event && report.Path == expected.Path && report.From == expected.From && report.Bytes == expected.Bytes
		}) {
			t.Errorf("receiver missing %#v: %#v", expected, result.receiver)
		}
	}
}

func assertRecoveryReports(t *testing.T, sender, receiver []agentReport) {
	t.Helper()
	for _, expected := range []struct {
		event, path string
		bytes       int
	}{
		{event: "peer"},
		{event: "fault-ready"},
		{event: "response", path: "/p2p/before-fault-ack", bytes: len("before-fault-ack")},
		{event: "response", path: "/p2p/during-fault-ack", bytes: len("during-fault-ack")},
		{event: "fallback"},
		{event: "response", path: "/p2p/after-recovery-ack", bytes: len("after-recovery-ack")},
		{event: "recovered"},
		{event: "complete"},
	} {
		if !hasReport(sender, func(report agentReport) bool {
			if report.Event != expected.event || expected.path != "" && (report.Path != expected.path || report.Bytes != expected.bytes) {
				return false
			}
			return expected.event != "peer" && expected.event != "recovered" || report.Available
		}) {
			t.Fatalf("sender missing event=%s path=%s: %#v", expected.event, expected.path, sender)
		}
	}
	for _, expected := range []struct {
		path  string
		bytes int
	}{
		{path: "/p2p/establish", bytes: len("durable-bootstrap")},
		{path: "/p2p/before-fault", bytes: len("before-fault")},
		{path: "/p2p/during-fault", bytes: len("during-fault")},
		{path: "/p2p/recover-trigger", bytes: len("recover-trigger")},
		{path: "/p2p/after-recovery", bytes: len("after-recovery")},
		{path: "/p2p/complete", bytes: len("complete")},
	} {
		if !hasReport(receiver, func(report agentReport) bool {
			return report.Event == "message" && report.Path == expected.path && report.From == "agent-a@mesh.test" && report.Bytes == expected.bytes
		}) {
			t.Fatalf("receiver missing %s: %#v", expected.path, receiver)
		}
	}
}

func hasReport(reports []agentReport, predicate func(agentReport) bool) bool {
	for _, report := range reports {
		if predicate(report) {
			return true
		}
	}
	return false
}

func decodeReports(t *testing.T, output string) []agentReport {
	t.Helper()
	var reports []agentReport
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "CYNAPSA_RANK1_EVIDENCE ") || strings.HasPrefix(scanner.Text(), "CYNAPSA_RANK2_EVIDENCE ") || strings.HasPrefix(scanner.Text(), "CYNAPSA_CORE_EVIDENCE ") {
			continue
		}
		var report agentReport
		if err := json.Unmarshal(scanner.Bytes(), &report); err != nil {
			t.Fatalf("decode agent output %q: %v", scanner.Text(), err)
		}
		reports = append(reports, report)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return reports
}

func readEnvironment(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found || key == "" {
			t.Fatalf("malformed environment line")
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func readFirst(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(strings.SplitN(string(content), "\n", 2)[0])
}

func readOptionalFirst(path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.SplitN(string(content), "\n", 2)[0])
}

func validCoturnState(path string) bool {
	if path == "" || !filepath.IsAbs(path) || !strings.HasPrefix(filepath.Base(path), "cynapsa-p2p.") {
		return false
	}
	relative, err := filepath.Rel(os.TempDir(), filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0
}

func readFirstLines(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", "..", ".."))
}

func runCommand(t *testing.T, directory string, timeout time.Duration, command string, arguments ...string) string {
	t.Helper()
	result := runCommandAllowFailure(directory, timeout, command, arguments...)
	if result.exitCode != 0 {
		t.Fatalf("%s %s: exit=%d\n%s", command, strings.Join(arguments, " "), result.exitCode, result.output)
	}
	return strings.TrimSpace(result.output)
}

type commandResult struct {
	output   string
	exitCode int
	timedOut bool
}

func runCommandAllowFailure(directory string, timeout time.Duration, command string, arguments ...string) commandResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	process := exec.CommandContext(ctx, command, arguments...)
	process.Dir = directory
	// The shell harness owns nested Docker helpers and another shell harness.
	// Cancel the whole isolated process group with TERM so its traps can join
	// children and tear down resources; a bounded WaitDelay is the final guard.
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process.Cancel = func() error {
		if process.Process != nil {
			_ = syscall.Kill(-process.Process.Pid, syscall.SIGTERM)
		}
		return nil
	}
	process.WaitDelay = 30 * time.Second
	output, err := process.CombinedOutput()
	timedOut := ctx.Err() == context.DeadlineExceeded
	if err == nil {
		return commandResult{output: string(output), timedOut: timedOut}
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return commandResult{output: string(output), exitCode: exit.ExitCode(), timedOut: timedOut}
	}
	return commandResult{output: fmt.Sprintf("%s\n%v", output, err), exitCode: -1, timedOut: timedOut}
}

func TestCommandTimeoutTerminatesProcessGroupAndJoinsCleanup(t *testing.T) {
	directory := t.TempDir()
	cleanup := filepath.Join(directory, "cleanup")
	childFile := filepath.Join(directory, "child-pid")
	script := `trap 'printf cleaned >"$1"; exit 0' TERM; sleep 30 & child=$!; printf '%s' "$child" >"$2"; wait "$child"`
	started := time.Now()
	result := runCommandAllowFailure(directory, 500*time.Millisecond, "/bin/sh", "-c", script, "timeout-probe", cleanup, childFile)
	if result.exitCode == 0 || !result.timedOut || time.Since(started) > 5*time.Second {
		t.Fatalf("timeout result=%#v elapsed=%s", result, time.Since(started))
	}
	if got := readOptionalFirst(cleanup); got != "cleaned" {
		t.Fatalf("cleanup marker=%q output=%q", got, result.output)
	}
	pidText := readOptionalFirst(childFile)
	pid, err := strconv.Atoi(pidText)
	if err != nil {
		t.Fatalf("child pid=%q: %v", pidText, err)
	}
	deadline := time.Now().Add(time.Second)
	for syscall.Kill(pid, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if syscall.Kill(pid, 0) == nil {
		t.Fatalf("timed-out descendant %d survived process-group cleanup", pid)
	}
}

func containsFold(value, substring string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(substring))
}

func appendedLog(before, after string) string {
	if strings.HasPrefix(after, before) {
		return after[len(before):]
	}
	return after
}

func assertArtifactsSanitized(t *testing.T, roots, secrets []string) {
	t.Helper()
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, secret := range secrets {
				if secret != "" && strings.Contains(string(content), secret) {
					return fmt.Errorf("generated credential retained in %s", path)
				}
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
	}
}
