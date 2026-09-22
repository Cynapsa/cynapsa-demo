package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/test/internal/productionlock"
)

type observation struct {
	Scenario     string `json:"scenario"`
	Admitted     bool   `json:"admitted"`
	CompletionOK bool   `json:"completion_ok"`
	ResultKind   string `json:"result_kind"`
	Lifecycle    string `json:"lifecycle"`
	ErrorCode    string `json:"error_code"`
	ErrorStage   string `json:"error_stage"`
	PayloadBytes int    `json:"payload_bytes"`
	PayloadEOF   bool   `json:"payload_eof"`
	Count        int    `json:"count"`
	Value        string `json:"value"`
	Allowed      bool   `json:"allowed"`
	Queued       uint64 `json:"queued"`
	Paused       bool   `json:"paused"`
}

type traceabilityMatrix struct {
	SchemaVersion   int                        `json:"schema_version"`
	CandidateCommit string                     `json:"candidate_commit"`
	CommandCoverage map[string]commandCoverage `json:"command_coverage"`
	Rows            []traceabilityRow          `json:"rows"`
}

type commandCoverage struct {
	Positive []string `json:"positive"`
	Negative []string `json:"negative"`
}

type traceabilityRow struct {
	RequirementID     string `json:"requirement_id"`
	PositiveScenario  string `json:"positive_scenario"`
	NegativeScenario  string `json:"negative_scenario"`
	TestName          string `json:"test_name"`
	Environment       string `json:"environment_profile"`
	ExpectedPositive  string `json:"expected_positive"`
	ExpectedNegative  string `json:"expected_negative"`
	ObservedResult    string `json:"observed_result"`
	ArtifactReference string `json:"artifact_reference"`
}

const pinnedEjabberdImage = "ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8"
const runtimeCandidateBinding = "runtime"

func TestPublicFacadeLocalTraceability(t *testing.T) {
	repository := repositoryRoot(t)
	binary := buildGoAgent(t, repository)
	command := exec.Command(binary, "local-contract")
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external Go runner: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "local-contract.jsonl", output)

	observed := decodeObservations(t, output)
	assertObservation(t, observed, "lifecycle_start_positive", func(got observation) bool {
		return got.Lifecycle == "created"
	})
	assertObservation(t, observed, "capabilities_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "capabilities"
	})
	assertObservation(t, observed, "command_cancel_completed_negative", errorObservation("invalid_handle", "command"))
	assertObservation(t, observed, "config_get_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "config" && got.Value == "30000|30000"
	})
	assertObservation(t, observed, "config_update_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "config" && got.Value == "7000|11000"
	})
	assertObservation(t, observed, "config_immutable_update_negative", errorObservation("command_error", "command"))
	assertObservation(t, observed, "completion_channel_register_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "completion_channel" && got.Count == 3 && got.Value != ""
	})
	assertObservation(t, observed, "completion_channel_duplicate_negative", errorObservation("command_error", "command"))
	assertObservation(t, observed, "completion_channel_stale_clear_negative", errorObservation("invalid_handle", "command"))
	assertObservation(t, observed, "completion_channel_clear_positive", successfulEmpty)
	assertObservation(t, observed, "event_sink_register_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "event_sink" && got.Value != ""
	})
	assertObservation(t, observed, "event_sink_stale_bind_negative", errorObservation("invalid_handle", "command"))
	assertObservation(t, observed, "event_sink_bind_positive", successfulEmpty)
	assertObservation(t, observed, "event_sink_clear_positive", successfulEmpty)
	assertObservation(t, observed, "address_put_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "empty"
	})
	assertObservation(t, observed, "address_resolve_equivalent_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "address_resolution" && got.Value == "peer@example.test|/orders%2Fnew|x=1&x=2"
	})
	assertObservation(t, observed, "address_list_spelling_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "address_mappings" && got.Count == 1 && got.Value == "HTTPS://Service.Example:443"
	})
	assertObservation(t, observed, "address_remove_positive", successfulEmpty)
	assertObservation(t, observed, "address_remove_idempotent_positive", successfulEmpty)
	assertObservation(t, observed, "address_unknown_resolve_negative", errorObservation("command_error", "command"))
	assertObservation(t, observed, "address_malformed_origin_negative", func(got observation) bool {
		return !got.Admitted && got.ErrorCode == "malformed_input"
	})
	assertObservation(t, observed, "policy_set_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "policy"
	})
	assertObservation(t, observed, "policy_get_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "policy" && got.Count == 1
	})
	assertObservation(t, observed, "policy_test_allow_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "policy" && got.Count == 1 && got.Allowed
	})
	assertObservation(t, observed, "policy_test_deny_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "policy" && got.Count == 1 && !got.Allowed
	})
	assertObservation(t, observed, "policy_malformed_action_negative", func(got observation) bool {
		return !got.Admitted && got.ErrorCode == "malformed_input"
	})
	for _, scenario := range []string{
		"handler_register_positive", "handler_register_idempotent_positive", "handler_unregister_positive", "handler_unregister_idempotent_positive",
		"diagnostics_logs_enable_positive", "diagnostics_logs_disable_positive", "delivery_pause_positive", "delivery_resume_positive",
	} {
		assertObservation(t, observed, scenario, successfulEmpty)
	}
	assertObservation(t, observed, "delivery_status_paused_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "delivery_queue_status" && got.Queued == 0 && got.Paused
	})
	assertObservation(t, observed, "delivery_next_paused_negative", errorObservation("command_error", "delivery"))
	assertObservation(t, observed, "delivery_next_empty_negative", errorObservation("connectivity_unavailable", "delivery"))
	assertObservation(t, observed, "delivery_accept_unknown_negative", errorObservation("invalid_handle", "delivery"))
	assertObservation(t, observed, "payload_lifecycle_positive", func(got observation) bool {
		return got.PayloadBytes == 13 && got.PayloadEOF
	})
	assertObservation(t, observed, "payload_stale_release_negative", func(got observation) bool {
		return got.ErrorCode == "invalid_handle"
	})
	assertObservation(t, observed, "lifecycle_shutdown_positive", func(got observation) bool {
		return got.Lifecycle == "closed"
	})
	assertObservation(t, observed, "lifecycle_destroy_idempotent_positive", func(got observation) bool {
		return got.CompletionOK
	})
	assertObservation(t, observed, "lifecycle_post_destroy_negative", func(got observation) bool {
		return got.ErrorCode == "invalid_handle"
	})
}

func TestAuthenticationNegativeTraceability(t *testing.T) {
	repository := repositoryRoot(t)
	binary := buildGoAgent(t, repository)
	command := exec.Command(binary, "auth-negative")
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external Go auth runner: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "auth-negative.jsonl", output)
	observed := decodeObservations(t, output)
	for _, scenario := range []string{
		"auth_endpoint_scheme_negative",
		"auth_endpoint_trailing_dot_negative",
		"auth_endpoint_unicode_negative",
		"auth_endpoint_zone_negative",
		"auth_endpoint_missing_port_negative",
		"auth_endpoint_zero_port_negative",
		"auth_endpoint_large_port_negative",
	} {
		assertObservation(t, observed, scenario, func(got observation) bool {
			return !got.Admitted && got.ErrorCode == "malformed_input" && got.ErrorStage == "sdk"
		})
	}
	assertObservation(t, observed, "auth_server_unavailable_negative", func(got observation) bool {
		return got.Admitted && !got.CompletionOK && got.ErrorCode == "connectivity_unavailable" && got.ErrorStage == "auth"
	})
	assertObservation(t, observed, "auth_failure_state_positive", func(got observation) bool {
		return got.Lifecycle == "created" && got.ResultKind == "unset"
	})
}

func TestRealEjabberdPublicAuthenticationTraceability(t *testing.T) {
	acquireProductionEnvironment(t)
	repository := repositoryRoot(t)
	harness := filepath.Join(repository, "test", "e2e", "ejabberd", "run.sh")
	if _, err := os.Stat(harness); err != nil {
		t.Fatalf("required disposable service harness is unavailable: %v", err)
	}
	start := exec.Command("sh", "-c", `umask 077; exec "$@"`, "sh", harness, "start", "shared-group")
	start.Dir = repository
	started, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start disposable shared-group profile: %v\n%s", err, started)
	}
	state := strings.TrimSpace(string(started))
	if state == "" || !filepath.IsAbs(state) {
		t.Fatalf("harness returned invalid state directory %q", state)
	}
	t.Cleanup(func() {
		stop := exec.Command(harness, "stop", state)
		stop.Dir = repository
		if output, stopErr := stop.CombinedOutput(); stopErr != nil {
			t.Errorf("stop disposable shared-group profile: %v\n%s", stopErr, output)
		}
	})

	credentials := readEnvironmentFile(t, filepath.Join(state, "credentials.env"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wantAdmin(t, ctx, repository, harness, state, "ready", "ready\tmesh.test\tsingle_node_mnesia")
	wantAdmin(t, ctx, repository, harness, state, "create", "", "e2e-mesh")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "e2e-mesh")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-b", "e2e-mesh")
	wantAdmin(t, ctx, repository, harness, state, "snapshot", currentMembershipAdminSnapshot("agent-a@mesh.test", "agent-b@mesh.test"), "e2e-mesh")
	slotProbe := exec.Command(harness, "probe", state, "slot")
	slotProbe.Dir = repository
	slotProbe.Env = append(os.Environ(), "CYNAPSA_EJABBERD_MESH=e2e-mesh")
	slotOutput, slotErr := slotProbe.CombinedOutput()
	if slotErr != nil {
		t.Fatalf("production XEP-0363 slot probe: %v\n%s", slotErr, slotOutput)
	}
	assertSanitizedUploadSlotEvidence(t, slotOutput)
	containerID := readTrimmedFile(t, filepath.Join(state, "container-id"))
	network := readTrimmedFile(t, filepath.Join(state, "network"))
	inspect := exec.Command("docker", "inspect", "-f", "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID)
	addressBytes, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve disposable service address: %v\n%s", err, addressBytes)
	}
	address := strings.TrimSpace(string(addressBytes))
	if address == "" {
		t.Fatal("disposable service has no isolated-network address")
	}

	agent := filepath.Join(t.TempDir(), "cynapsa-e2e-go-agent-linux")
	build := exec.Command("go", "build", "-trimpath", "-o", agent, "./test/e2e/runner/go_agent")
	build.Dir = repository
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build static Linux public runner: %v\n%s", buildErr, output)
	}
	runnerEnvironment := writePrivateEnvironmentFile(t, "runner.env", []string{
		"SSL_CERT_FILE=/ca.pem",
		"CYNAPSA_EJABBERD_ENDPOINT=mesh.test:5222",
		"CYNAPSA_EJABBERD_AGENT_A=" + credentials["CYNAPSA_EJABBERD_AGENT_A"],
		"CYNAPSA_EJABBERD_PASSWORD_A=" + credentials["CYNAPSA_EJABBERD_PASSWORD_A"],
		"CYNAPSA_EJABBERD_AGENT_B=" + credentials["CYNAPSA_EJABBERD_AGENT_B"],
		"CYNAPSA_EJABBERD_PASSWORD_B=" + credentials["CYNAPSA_EJABBERD_PASSWORD_B"],
	})

	arguments := []string{
		"run", "--rm", "--read-only", "--network", network,
		"--entrypoint", "/agent",
		"--memory", "128m", "--pids-limit", "64", "--cpus", "0.5",
		"--security-opt", "no-new-privileges", "--cap-drop", "ALL",
		"--add-host", "mesh.test:" + address,
		"--mount", "type=bind,src=" + agent + ",dst=/agent,readonly",
		"--mount", "type=bind,src=" + credentials["CYNAPSA_EJABBERD_CA"] + ",dst=/ca.pem,readonly",
		"--env-file", runnerEnvironment,
		pinnedEjabberdImage, "auth-real",
	}
	command := exec.Command("docker", arguments...)
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external Linux public runner against disposable service: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "ejabberd-auth.jsonl", output)
	observed := decodeObservations(t, output)
	assertObservation(t, observed, "auth_bad_credentials_negative", errorObservation("authentication_failed", "auth"))
	assertObservation(t, observed, "auth_bad_credentials_state_positive", func(got observation) bool {
		return got.Lifecycle == "created" && got.ResultKind == "unset"
	})
	assertObservation(t, observed, "auth_connect_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "auth"
	})
	assertObservation(t, observed, "auth_receiver_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "auth"
	})
	assertObservation(t, observed, "auth_ready_status_positive", func(got observation) bool {
		return got.Lifecycle == "ready" && got.ResultKind == "native"
	})
	assertObservation(t, observed, "auth_agent_id_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "agent_id"
	})
	assertObservation(t, observed, "diagnostics_connectivity_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "connectivity_status" && got.Value == "available"
	})
	assertObservation(t, observed, "mesh_list_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "mesh_list" && got.Count == 1 && got.Value == "e2e-mesh|true"
	})
	assertObservation(t, observed, "diagnostics_snapshot_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "diagnostic_snapshot" && got.Value == "1|0|0|0" && got.Queued == 0
	})
	for _, scenario := range []string{"mesh_refresh_real_positive", "public_same_mesh_message_accept_positive", "public_same_mesh_request_accept_positive"} {
		assertObservation(t, observed, scenario, successfulEmpty)
	}
	for _, scenario := range []string{"sender_policy_real_positive", "receiver_policy_real_positive"} {
		assertObservation(t, observed, scenario, func(got observation) bool {
			return got.Admitted && got.CompletionOK && got.ResultKind == "policy"
		})
	}
	assertObservation(t, observed, "public_same_mesh_send_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "send" && got.Allowed
	})
	assertObservation(t, observed, "public_same_mesh_message_received_positive", func(got observation) bool {
		return got.ResultKind == "event" && got.Value == "agent-a@mesh.test|msg|/e2e/message" && got.PayloadBytes == 96<<10
	})
	assertObservation(t, observed, "diagnostics_peer_available_positive", func(got observation) bool {
		return got.ResultKind == "peer_status" && got.Value == "available" && got.Allowed
	})
	assertObservation(t, observed, "public_same_mesh_request_received_positive", func(got observation) bool {
		return got.ResultKind == "event" && got.Value == "agent-a@mesh.test|rpc|/e2e/request" && got.PayloadBytes == 16 && got.Allowed
	})
	assertObservation(t, observed, "diagnostics_pending_rpc_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "diagnostic_snapshot" && got.Value == "1|0|1|0"
	})
	assertObservation(t, observed, "public_same_mesh_reply_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "send" && got.Allowed
	})
	assertObservation(t, observed, "public_same_mesh_request_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "response" && got.Value == "agent-b@mesh.test|/e2e/reply" && got.PayloadBytes == 15
	})
	assertObservation(t, observed, "conversation_list_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "conversation_list" && got.Count == 1 && got.Value == "agent-b@mesh.test" && got.Allowed
	})
	assertObservation(t, observed, "conversation_status_real_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "conversation_status" && got.Value == "agent-b@mesh.test|ready" && got.Queued == 0 && got.Allowed
	})
	assertObservation(t, observed, "conversation_close_real_positive", successfulEmpty)
	assertObservation(t, observed, "conversation_closed_status_negative", errorObservation("command_error", "delivery"))
	assertObservation(t, observed, "delivery_retry_terminal_negative", errorObservation("command_error", "delivery"))
	assertObservation(t, observed, "delivery_drop_terminal_negative", errorObservation("command_error", "delivery"))
	assertObservation(t, observed, "auth_logout_completion_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "empty"
	})
	assertObservation(t, observed, "auth_logout_later_admission_negative", errorObservation("shutdown_in_progress", "shutdown"))
	assertObservation(t, observed, "auth_logout_closed_positive", func(got observation) bool { return got.Lifecycle == "closed" })
	assertObservation(t, observed, "auth_reconnect_new_core_positive", func(got observation) bool {
		return got.Admitted && got.CompletionOK && got.ResultKind == "auth"
	})
}

func TestRealEjabberdUploadSlotAuxiliary(t *testing.T) {
	acquireProductionEnvironment(t)
	repository := repositoryRoot(t)
	harness := filepath.Join(repository, "test", "e2e", "ejabberd", "run.sh")
	start := exec.Command("sh", "-c", `umask 077; exec "$@"`, "sh", harness, "start", "shared-group")
	start.Dir = repository
	started, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start disposable upload-slot profile: %v\n%s", err, started)
	}
	state := strings.TrimSpace(string(started))
	if state == "" || !filepath.IsAbs(state) {
		t.Fatalf("harness returned invalid state directory %q", state)
	}
	stopped := false
	t.Cleanup(func() {
		if stopped {
			return
		}
		stop := exec.Command(harness, "stop", state)
		stop.Dir = repository
		if output, stopErr := stop.CombinedOutput(); stopErr != nil {
			t.Errorf("fallback stop disposable upload-slot profile: %v\n%s", stopErr, output)
		}
	})

	containerID := readTrimmedFile(t, filepath.Join(state, "container-id"))
	network := readTrimmedFile(t, filepath.Join(state, "network"))
	volume := readTrimmedFile(t, filepath.Join(state, "volume"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wantAdmin(t, ctx, repository, harness, state, "ready", "ready\tmesh.test\tsingle_node_mnesia")
	wantAdmin(t, ctx, repository, harness, state, "create", "", "upload-e2e")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "upload-e2e")

	probe := exec.Command(harness, "probe", state, "slot")
	probe.Dir = repository
	probe.Env = append(os.Environ(), "CYNAPSA_EJABBERD_MESH=upload-e2e")
	output, probeErr := probe.CombinedOutput()
	if probeErr != nil {
		t.Fatalf("production XEP-0363 auxiliary probe: %v\n%s", probeErr, output)
	}
	evidence := assertSanitizedUploadSlotEvidence(t, output)
	t.Logf("sanitized upload-slot evidence: %s", evidence)

	stop := exec.Command(harness, "stop", state)
	stop.Dir = repository
	if stopOutput, stopErr := stop.CombinedOutput(); stopErr != nil {
		t.Fatalf("stop disposable upload-slot profile: %v\n%s", stopErr, stopOutput)
	}
	stopped = true
	assertDockerObjectAbsent(t, "container", containerID)
	assertDockerObjectAbsent(t, "network", network)
	assertDockerObjectAbsent(t, "volume", volume)
	for _, secret := range []string{"credentials.env", "ca-key.pem", "server-key.pem", "server.pem"} {
		if _, statErr := os.Stat(filepath.Join(state, secret)); !os.IsNotExist(statErr) {
			t.Fatalf("secret-bearing state survived teardown: %s (%v)", secret, statErr)
		}
	}
}

func assertSanitizedUploadSlotEvidence(t *testing.T, output []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.HasPrefix(line, "client-phase=upload-slot-complete ") {
			continue
		}
		if !strings.Contains(line, "put_scheme=https put_host=127.0.0.1:") ||
			!strings.Contains(line, "get_scheme=https get_host=127.0.0.1:") ||
			!strings.Contains(line, " headers=") ||
			!strings.Contains(line, " header_names=") ||
			strings.Contains(line, "/upload") || strings.Contains(line, "?") ||
			strings.Contains(line, "Bearer ") {
			t.Fatalf("production XEP-0363 slot evidence is incomplete or secret-bearing: %q", line)
		}
		return line
	}
	t.Fatalf("production XEP-0363 slot probe omitted sanitized evidence:\n%s", output)
	return ""
}

func acquireProductionEnvironment(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	release, err := productionlock.Acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("acquire production environment: %v", err)
	}
	t.Cleanup(release)
}

func TestPayloadHandleTraceability(t *testing.T) {
	repository := repositoryRoot(t)
	binary := buildGoAgent(t, repository)
	command := exec.Command(binary, "payload-contract")
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external Go payload runner: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "payload-contract.jsonl", output)
	observed := decodeObservations(t, output)
	assertObservation(t, observed, "payload_over_limit_negative", errorObservation("payload_too_large", "payload"))
	assertObservation(t, observed, "payload_cancel_idempotent_positive", func(got observation) bool { return got.CompletionOK })
	for _, scenario := range []string{
		"payload_write_after_cancel_negative",
		"payload_cancelled_double_release_negative",
		"payload_cross_core_negative",
		"payload_stale_read_negative",
		"payload_malformed_handle_negative",
	} {
		assertObservation(t, observed, scenario, errorObservation("invalid_handle", "payload"))
	}
	assertObservation(t, observed, "payload_retain_release_positive", func(got observation) bool {
		return got.PayloadBytes == 13 && got.PayloadEOF
	})
}

func TestLifecycleAndConfigurationTraceability(t *testing.T) {
	repository := repositoryRoot(t)
	binary := buildGoAgent(t, repository)
	command := exec.Command(binary, "lifecycle-contract")
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external Go lifecycle runner: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "lifecycle-contract.jsonl", output)
	observed := decodeObservations(t, output)
	for _, scenario := range []string{
		"config_zero_queue_negative",
		"config_large_queue_negative",
		"config_zero_payload_negative",
		"config_large_payload_negative",
		"config_negative_timeout_negative",
	} {
		assertObservation(t, observed, scenario, errorObservation("malformed_input", "sdk"))
	}
	assertObservation(t, observed, "lifecycle_repeat_positive", func(got observation) bool {
		return got.CompletionOK && got.PayloadBytes == 20
	})
	assertObservation(t, observed, "lifecycle_destroy_before_shutdown_negative", errorObservation("shutdown_in_progress", "shutdown"))
	assertObservation(t, observed, "lifecycle_shutdown_timeout_negative", errorObservation("shutdown_timeout", "shutdown"))
	assertObservation(t, observed, "lifecycle_admission_after_shutdown_negative", errorObservation("shutdown_in_progress", "shutdown"))
	assertObservation(t, observed, "lifecycle_shutdown_join_positive", func(got observation) bool { return got.CompletionOK })
	assertObservation(t, observed, "core_shutdown_command_completion_positive", successfulEmpty)
	assertObservation(t, observed, "core_shutdown_command_late_admission_negative", errorObservation("shutdown_in_progress", "shutdown"))
	assertObservation(t, observed, "core_shutdown_command_direct_payload_negative", errorObservation("shutdown_in_progress", "shutdown"))
	assertObservation(t, observed, "core_shutdown_command_closed_positive", func(got observation) bool { return got.Lifecycle == "closed" })
}

func TestE2ERunnersDoNotImportPrivatePackages(t *testing.T) {
	root := repositoryRoot(t)
	err := filepath.WalkDir(filepath.Join(root, "test", "e2e", "runner"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			pathValue, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if strings.Contains(pathValue, "/internal/") || strings.HasSuffix(pathValue, "/internal") {
				t.Errorf("private package import %q in %s", pathValue, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTraceabilityMatrixIsMachineReadable(t *testing.T) {
	repository := repositoryRoot(t)
	path := filepath.Join(repository, "test", "e2e", "fixtures", "traceability_v1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var matrix traceabilityMatrix
	if err := decoder.Decode(&matrix); err != nil {
		t.Fatalf("decode traceability matrix: %v", err)
	}
	if matrix.SchemaVersion != 1 || matrix.CandidateCommit != runtimeCandidateBinding {
		t.Fatalf("unexpected traceability identity: %#v", matrix)
	}
	bindTraceabilityCandidate(t, repository, "traceability-candidate.json")
	expectedCommands := []v1.CommandName{
		v1.CommandCoreInit, v1.CommandCoreCapabilities, v1.CommandCoreStatus, v1.CommandCoreShutdown,
		v1.CommandConfigGet, v1.CommandConfigUpdate, v1.CommandChannelRegister, v1.CommandChannelClear, v1.CommandCancel,
		v1.CommandEventSinkRegister, v1.CommandEventSinkClear, v1.CommandEventSinkBind,
		v1.CommandAuthLogin, v1.CommandAuthConnect, v1.CommandAuthTokenLogin, v1.CommandAuthTokenConnect, v1.CommandAuthInstallationLogin, v1.CommandAuthInstallationConnect, v1.CommandAuthLogout, v1.CommandAuthAgentID,
		v1.CommandMeshList, v1.CommandMeshRefresh,
		v1.CommandAddressPut, v1.CommandAddressRemove, v1.CommandAddressList, v1.CommandAddressResolve,
		v1.CommandMessageSend, v1.CommandMessageRequest, v1.CommandMessageReply,
		v1.CommandDeliveryNext, v1.CommandDeliveryAccept, v1.CommandDeliveryQueueStatus, v1.CommandDeliveryRetry,
		v1.CommandDeliveryPause, v1.CommandDeliveryResume, v1.CommandDeliveryDrop,
		v1.CommandHandlerRegister, v1.CommandHandlerUnregister,
		v1.CommandPayloadOpen, v1.CommandPayloadWrite, v1.CommandPayloadFinish, v1.CommandPayloadCancel,
		v1.CommandPayloadRead, v1.CommandPayloadClose, v1.CommandPayloadRetain, v1.CommandPayloadRelease,
		v1.CommandConversationList, v1.CommandConversationStatus, v1.CommandConversationClose,
		v1.CommandPolicySet, v1.CommandPolicyGet, v1.CommandPolicyTest,
		v1.CommandDiagnosticsPeer, v1.CommandDiagnosticsConnectivity, v1.CommandDiagnosticsSnapshot, v1.CommandDiagnosticsLogs,
	}
	if len(matrix.CommandCoverage) != len(expectedCommands) {
		t.Fatalf("command coverage count = %d, want %d", len(matrix.CommandCoverage), len(expectedCommands))
	}
	var referenceCorpus bytes.Buffer
	if err := filepath.WalkDir(repository, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if path == filepath.Join(repository, "test", "e2e", "fixtures", "traceability_v1.json") || !(strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".c")) {
			return nil
		}
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		referenceCorpus.Write(contents)
		referenceCorpus.WriteByte('\n')
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, command := range expectedCommands {
		coverage, ok := matrix.CommandCoverage[string(command)]
		if !ok || len(coverage.Positive) == 0 || len(coverage.Negative) == 0 {
			t.Errorf("command %q lacks positive/negative traceability: %#v", command, coverage)
		}
		for _, scenarios := range [][]string{coverage.Positive, coverage.Negative} {
			for _, scenario := range scenarios {
				if strings.TrimSpace(scenario) == "" {
					t.Errorf("command %q has an empty scenario", command)
					continue
				}
				reference := strings.SplitN(scenario, "/", 2)[0]
				if !bytes.Contains(referenceCorpus.Bytes(), []byte(reference)) {
					t.Errorf("command %q references missing test/scenario %q", command, scenario)
				}
			}
		}
	}
	if len(matrix.Rows) == 0 {
		t.Fatal("traceability matrix has no rows")
	}
	seen := make(map[string]struct{}, len(matrix.Rows))
	for index, row := range matrix.Rows {
		values := []string{row.RequirementID, row.PositiveScenario, row.NegativeScenario, row.TestName, row.Environment, row.ExpectedPositive, row.ExpectedNegative, row.ObservedResult, row.ArtifactReference}
		for field, value := range values {
			if strings.TrimSpace(value) == "" {
				t.Errorf("traceability row %d field %d is empty", index, field)
			}
		}
		if _, duplicate := seen[row.RequirementID]; duplicate {
			t.Errorf("duplicate requirement_id %q", row.RequirementID)
		}
		seen[row.RequirementID] = struct{}{}
	}
}

func TestNativeABITraceability(t *testing.T) {
	repository := repositoryRoot(t)
	temporary := t.TempDir()
	library := filepath.Join(temporary, "libcynapsacore.so")
	linkerFlag := "-extldflags=-Wl,--version-script," + filepath.Join(repository, "cmd", "cynapsacore-shared", "exports_linux.map")
	if runtime.GOOS == "darwin" {
		library = filepath.Join(temporary, "libcynapsacore.dylib")
		linkerFlag = "-extldflags=-Wl,-exported_symbols_list," + filepath.Join(repository, "cmd", "cynapsacore-shared", "exports_darwin.txt")
	}
	build := exec.Command("go", "build", "-trimpath", "-buildmode=c-shared", "-ldflags="+linkerFlag, "-o", library, "./cmd/cynapsacore-shared")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release-form native library: %v\n%s", err, output)
	}

	compiler := strings.Fields(goEnv(t, repository, "CC"))
	if len(compiler) == 0 {
		t.Fatal("go env CC returned no compiler")
	}
	compileHeaderC99(t, repository, compiler)
	assertExactNativeExports(t, library)
	native := filepath.Join(temporary, "cynapsa-e2e-native-agent")
	arguments := append(compiler[1:],
		"-std=c11", "-Wall", "-Wextra", "-Werror",
		"-I"+filepath.Join(repository, "cmd", "cynapsacore-shared"),
		filepath.Join(repository, "test", "e2e", "runner", "native_agent.c"),
		library,
		"-o", native,
	)
	compile := exec.Command(compiler[0], arguments...)
	compile.Dir = repository
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile external native runner: %v\n%s", err, output)
	}

	command := exec.Command(native)
	command.Dir = temporary
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external native runner: %v\n%s", err, output)
	}
	assertNoForbiddenPublicOutput(t, output)
	writeArtifact(t, "native-abi.jsonl", output)
	observed := decodeRawObservations(t, output)
	assertRaw(t, observed, "abi_version_positive", func(value map[string]any) bool {
		return value["version"] == float64(1)
	})
	assertRaw(t, observed, "abi_version_mismatch_negative", errorCodeIs("unsupported_version"))
	assertRaw(t, observed, "abi_invalid_core_handle_negative", errorCodeIs("invalid_handle"))
	assertRaw(t, observed, "abi_core_lifecycle_positive", boolField("started"))
	assertRaw(t, observed, "abi_poll_timeout_positive", boolField("wait_timeout"))
	assertRaw(t, observed, "capabilities_unknown_command_negative", errorCodeIs("malformed_input"))
	assertRaw(t, observed, "abi_duplicate_field_negative", errorCodeIs("malformed_input"))
	assertRaw(t, observed, "abi_submit_positive", func(value map[string]any) bool {
		document, ok := value["document"].(map[string]any)
		return ok && document["abi_version"] == float64(1) && document["accepted"] == true
	})
	assertRaw(t, observed, "abi_buffer_double_free_negative", errorCodeIs("invalid_handle"))
	assertRaw(t, observed, "abi_callback_positive", boolField("completion_seen"))
	assertRaw(t, observed, "abi_callback_reentrant_clear_negative", errorCodeIs("shutdown_in_progress"))
	assertRaw(t, observed, "abi_callback_clear_positive", boolField("cleared"))
	assertRaw(t, observed, "abi_payload_open_positive", func(value map[string]any) bool {
		document, ok := value["document"].(map[string]any)
		handle, handleOK := document["payload_handle"].(string)
		return ok && handleOK && strings.HasPrefix(handle, "payh_")
	})
	assertRaw(t, observed, "abi_payload_write_positive", documentFieldEquals("accepted", float64(13)))
	assertRaw(t, observed, "abi_payload_finish_positive", documentFieldEquals("size", float64(13)))
	assertRaw(t, observed, "abi_payload_read_positive", func(value map[string]any) bool {
		document, ok := value["document"].(map[string]any)
		return ok && document["chunk"] == "pQABAQACYANhLwRBeA==" && document["eof"] == true
	})
	assertRaw(t, observed, "abi_payload_double_release_negative", errorCodeIs("invalid_handle"))
	assertRaw(t, observed, "abi_payload_cancel_idempotent_positive", boolField("cancelled"))
	assertRaw(t, observed, "abi_shutdown_destroy_positive", boolField("destroyed"))
}

func decodeObservations(t *testing.T, output []byte) map[string]observation {
	t.Helper()
	result := make(map[string]observation)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		var value observation
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatalf("decode runner output %q: %v", scanner.Text(), err)
		}
		if value.Scenario == "" {
			t.Fatalf("runner emitted observation without scenario: %s", scanner.Text())
		}
		if _, exists := result[value.Scenario]; exists {
			t.Fatalf("runner emitted duplicate scenario %q", value.Scenario)
		}
		result[value.Scenario] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func decodeRawObservations(t *testing.T, output []byte) map[string]map[string]any {
	t.Helper()
	result := make(map[string]map[string]any)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		value := make(map[string]any)
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatalf("decode native runner output %q: %v", scanner.Text(), err)
		}
		scenario, ok := value["scenario"].(string)
		if !ok || scenario == "" {
			t.Fatalf("native runner emitted observation without scenario: %s", scanner.Text())
		}
		result[scenario] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertObservation(t *testing.T, observed map[string]observation, scenario string, valid func(observation) bool) {
	t.Helper()
	got, ok := observed[scenario]
	if !ok {
		t.Errorf("missing traceability scenario %q", scenario)
		return
	}
	if !valid(got) {
		t.Errorf("scenario %q mismatch: %#v", scenario, got)
	}
}

func assertRaw(t *testing.T, observed map[string]map[string]any, scenario string, valid func(map[string]any) bool) {
	t.Helper()
	got, ok := observed[scenario]
	if !ok {
		t.Errorf("missing native traceability scenario %q", scenario)
		return
	}
	if !valid(got) {
		t.Errorf("native scenario %q mismatch: %#v", scenario, got)
	}
}

func errorCodeIs(want string) func(map[string]any) bool {
	return func(value map[string]any) bool {
		document, ok := value["document"].(map[string]any)
		if !ok {
			return false
		}
		failure, ok := document["error"].(map[string]any)
		return ok && failure["code"] == want
	}
}

func boolField(name string) func(map[string]any) bool {
	return func(value map[string]any) bool { return value[name] == true }
}

func documentFieldEquals(name string, want any) func(map[string]any) bool {
	return func(value map[string]any) bool {
		document, ok := value["document"].(map[string]any)
		return ok && document[name] == want
	}
}

func errorObservation(code, stage string) func(observation) bool {
	return func(value observation) bool { return value.ErrorCode == code && value.ErrorStage == stage }
}

func successfulEmpty(value observation) bool {
	return value.Admitted && value.CompletionOK && value.ResultKind == "empty"
}

func assertNoForbiddenPublicOutput(t *testing.T, output []byte) {
	t.Helper()
	forbidden := map[string]struct{}{
		"xmpp": {}, "jid": {}, "jabber": {}, "webrtc": {}, "rtcdatachannel": {},
		"ice": {}, "stun": {}, "turn": {}, "jingle": {}, "xep": {},
		"rank1": {}, "rank2": {}, "pion": {}, "mellium": {},
	}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		var value any
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatalf("decode output for boundary scan: %v", err)
		}
		walkPublicStrings(t, value, forbidden)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func walkPublicStrings(t *testing.T, value any, forbidden map[string]struct{}) {
	t.Helper()
	switch typed := value.(type) {
	case string:
		lower := strings.ToLower(typed)
		for _, token := range strings.FieldsFunc(lower, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
			if _, found := forbidden[token]; found {
				t.Errorf("forbidden public token %q in output", token)
			}
		}
		compact := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return r
			}
			return -1
		}, lower)
		if _, found := forbidden[compact]; found {
			t.Errorf("forbidden public identifier %q in output", typed)
		}
	case []any:
		for _, item := range typed {
			walkPublicStrings(t, item, forbidden)
		}
	case map[string]any:
		for key, item := range typed {
			walkPublicStrings(t, key, forbidden)
			walkPublicStrings(t, item, forbidden)
		}
	}
}

func goEnv(t *testing.T, repository, name string) string {
	t.Helper()
	command := exec.Command("go", "env", name)
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go env %s: %v\n%s", name, err, output)
	}
	return strings.TrimSpace(string(output))
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func buildGoAgent(t *testing.T, repository string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "cynapsa-e2e-go-agent")
	build := exec.Command("go", "build", "-trimpath", "-o", binary, "./test/e2e/runner/go_agent")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build external Go runner: %v\n%s", err, output)
	}
	return binary
}

func writeArtifact(t *testing.T, name string, data []byte) {
	t.Helper()
	directory := os.Getenv("CYNAPSA_E2E_ARTIFACT_DIR")
	if directory == "" {
		directory = t.TempDir()
	} else if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create E2E artifact directory: %v", err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write sanitized artifact %s: %v", name, err)
	}
	t.Logf("sanitized E2E artifact: %s", path)
}

type candidateProvenance struct {
	TestedCommit  string `json:"tested_commit"`
	WorktreeClean bool   `json:"worktree_clean"`
}

func bindTraceabilityCandidate(t *testing.T, repository, artifactName string) candidateProvenance {
	t.Helper()
	provenance, err := resolveCandidateProvenance(repository, os.Getenv("CYNAPSA_E2E_ARTIFACT_DIR"), os.Getenv("CYNAPSA_CANDIDATE_COMMIT"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		t.Fatalf("encode candidate provenance: %v", err)
	}
	writeArtifact(t, artifactName, append(encoded, '\n'))
	return provenance
}

func resolveCandidateProvenance(repository, artifactDirectory, configuredCandidate string) (candidateProvenance, error) {
	repositoryRootBytes, err := runGit(repository, "rev-parse", "--show-toplevel")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("resolve repository root: %w", err)
	}
	repositoryRoot, err := filepath.Abs(strings.TrimSpace(string(repositoryRootBytes)))
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("resolve repository root: %w", err)
	}
	canonicalRepository, err := filepath.EvalSymlinks(repositoryRoot)
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("canonicalize repository root: %w", err)
	}
	if artifactDirectory != "" {
		artifactPath, absoluteErr := filepath.Abs(artifactDirectory)
		if absoluteErr != nil {
			return candidateProvenance{}, fmt.Errorf("resolve E2E artifact directory: %w", absoluteErr)
		}
		canonicalArtifact, canonicalErr := canonicalizeProspectivePath(artifactPath)
		if canonicalErr != nil {
			return candidateProvenance{}, fmt.Errorf("canonicalize E2E artifact directory: %w", canonicalErr)
		}
		if pathWithin(repositoryRoot, artifactPath) || pathWithin(canonicalRepository, canonicalArtifact) || pathDescendsFromSameDirectory(canonicalRepository, artifactPath) {
			return candidateProvenance{}, fmt.Errorf("E2E artifact directory must be outside the tested worktree: %s", artifactPath)
		}
	}

	headBytes, err := runGit(canonicalRepository, "rev-parse", "HEAD")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("resolve tested candidate: %w", err)
	}
	head := strings.TrimSpace(string(headBytes))
	decoded, decodeErr := hex.DecodeString(head)
	if decodeErr != nil || len(decoded) != 20 {
		return candidateProvenance{}, fmt.Errorf("tested candidate is not a full commit: %q", head)
	}
	configured := strings.TrimSpace(configuredCandidate)
	if configured != "" && configured != head {
		return candidateProvenance{}, fmt.Errorf("configured candidate %q does not match tested HEAD %q", configured, head)
	}

	statusBytes, err := runGit(canonicalRepository, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("inspect tested worktree: %w", err)
	}
	ignoredBytes, err := runGit(canonicalRepository, "ls-files", "--others", "--ignored", "--exclude-standard")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("inspect ignored build inputs: %w", err)
	}
	statusDirty := len(bytes.TrimSpace(statusBytes)) != 0
	ignoredDirty := len(bytes.TrimSpace(ignoredBytes)) != 0
	provenance := candidateProvenance{TestedCommit: head, WorktreeClean: !statusDirty && !ignoredDirty}
	if configured != "" && !provenance.WorktreeClean {
		return candidateProvenance{}, fmt.Errorf("configured candidate %s is not being tested from a clean worktree (status_dirty=%t ignored_inputs=%t)", head, statusDirty, ignoredDirty)
	}
	return provenance, nil
}

func runGit(repository string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", arguments...)
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err, bytes.TrimSpace(output))
	}
	return output, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func pathDescendsFromSameDirectory(root, candidate string) bool {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return false
	}
	current := filepath.Clean(candidate)
	for {
		if info, statErr := os.Stat(current); statErr == nil && os.SameFile(rootInfo, info) {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
		current = parent
	}
}

func canonicalizeProspectivePath(path string) (string, error) {
	current := filepath.Clean(path)
	var missing []string
	for {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		resolved = filepath.Join(resolved, missing[index])
	}
	return resolved, nil
}

func writePrivateEnvironmentFile(t *testing.T, name string, lines []string) string {
	t.Helper()
	for _, line := range lines {
		if line == "" || !strings.Contains(line, "=") || strings.ContainsAny(line, "\r\n") {
			t.Fatal("refusing to write malformed private environment entry")
		}
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write private environment file: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("inspect private environment file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private environment file mode = %v, want 0600", info.Mode().Perm())
	}
	return path
}

func readEnvironmentFile(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open disposable environment: %v", err)
	}
	defer file.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), "=")
		if !ok || name == "" || value == "" {
			t.Fatal("disposable environment contains malformed entry")
		}
		values[name] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read disposable environment: %v", err)
	}
	for _, required := range []string{"CYNAPSA_EJABBERD_CA", "CYNAPSA_EJABBERD_AGENT_A", "CYNAPSA_EJABBERD_PASSWORD_A"} {
		if values[required] == "" {
			t.Fatalf("disposable environment omits %s", required)
		}
	}
	return values
}

func readTrimmedFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read disposable state file: %v", err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		t.Fatal("disposable state file is empty")
	}
	return value
}

func compileHeaderC99(t *testing.T, repository string, compiler []string) {
	t.Helper()
	arguments := append(append([]string(nil), compiler[1:]...),
		"-std=c99", "-Wall", "-Wextra", "-Werror", "-fsyntax-only", "-x", "c",
		"-I"+filepath.Join(repository, "cmd", "cynapsacore-shared"), "-",
	)
	command := exec.Command(compiler[0], arguments...)
	command.Dir = repository
	command.Stdin = strings.NewReader("#include \"cynapsacore_v1.h\"\nint main(void) { return 0; }\n")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile public header as C99: %v\n%s", err, output)
	}
}

func assertExactNativeExports(t *testing.T, library string) {
	t.Helper()
	expected := map[string]struct{}{
		"cynapsa_v1_abi_version": {}, "cynapsa_v1_core_create": {}, "cynapsa_v1_core_start": {},
		"cynapsa_v1_core_submit": {}, "cynapsa_v1_core_cancel": {}, "cynapsa_v1_core_next_completion": {},
		"cynapsa_v1_core_next_event": {}, "cynapsa_v1_core_status": {}, "cynapsa_v1_core_shutdown": {},
		"cynapsa_v1_core_destroy": {}, "cynapsa_v1_payload_open": {}, "cynapsa_v1_payload_write": {},
		"cynapsa_v1_payload_finish": {}, "cynapsa_v1_payload_read": {}, "cynapsa_v1_payload_cancel": {},
		"cynapsa_v1_payload_retain": {}, "cynapsa_v1_payload_release": {}, "cynapsa_v1_callbacks_register": {},
		"cynapsa_v1_callbacks_clear": {}, "cynapsa_v1_buffer_read": {}, "cynapsa_v1_buffer_free": {},
	}
	arguments := []string{"-D", "--defined-only", library}
	if runtime.GOOS == "darwin" {
		arguments = []string{"-gU", library}
	}
	command := exec.Command("nm", arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect native exports: %v\n%s", err, output)
	}
	actual := make(map[string]struct{})
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		symbol := fields[len(fields)-1]
		if runtime.GOOS == "darwin" {
			symbol = strings.TrimPrefix(symbol, "_")
		}
		if strings.HasPrefix(symbol, "cynapsa") {
			actual[symbol] = struct{}{}
		}
	}
	if len(actual) != len(expected) {
		t.Fatalf("native export count = %d, want %d: %v", len(actual), len(expected), actual)
	}
	for symbol := range expected {
		if _, ok := actual[symbol]; !ok {
			t.Errorf("missing native export %s", symbol)
		}
	}
}
