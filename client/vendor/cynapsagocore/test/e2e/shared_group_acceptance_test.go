package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSharedGroupAuthorityAndRank2Recovery(t *testing.T) {
	acquireProductionEnvironment(t)
	repository := repositoryRoot(t)
	harness := filepath.Join(repository, "test", "e2e", "ejabberd", "run.sh")
	repositorySource := filepath.Join(repository, "server", "ejabberd", "mod_cynapsa_mesh", "src", "mod_cynapsa_mesh.erl")
	sourceBefore, sourceModeBefore := fixtureSnapshot(t, repositorySource)
	// The bounded 257-member pagination fixture performs one authenticated
	// ejabberd admin registration per member before protocol assertions begin.
	// Keep the whole harness bounded while allowing slower shared CI hosts to
	// finish setup and the subsequent revocation, storage, and recovery matrix
	// instead of turning host speed into a protocol result.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	state := runSharedGroupCommand(t, ctx, repository, harness, "start", "shared-group")
	if state == "" || !filepath.IsAbs(state) {
		t.Fatalf("harness returned invalid state directory %q", state)
	}
	credentials := readEnvironmentFile(t, filepath.Join(state, "credentials.env"))
	for name, mode := range map[string]fs.FileMode{
		"ejabberd.yml":                       0o444,
		"ca.pem":                             0o444,
		"server.pem":                         0o600,
		"credentials.env":                    0o600,
		"module-source/mod_cynapsa_mesh.erl": 0o444,
		"module/mod_cynapsa_mesh.beam":       0o444,
	} {
		assertFixtureMode(t, filepath.Join(state, name), mode)
	}
	assertFixtureMode(t, filepath.Join(state, "module-source"), 0o555)
	assertFixtureMode(t, filepath.Join(state, "module"), 0o555)
	stagedSource, _ := fixtureSnapshot(t, filepath.Join(state, "module-source", "mod_cynapsa_mesh.erl"))
	if stagedSource != sourceBefore {
		t.Fatal("staged shared-group source does not match the repository source")
	}
	container := readTrimmedFile(t, filepath.Join(state, "container-id"))
	network := readTrimmedFile(t, filepath.Join(state, "network"))
	volume := readTrimmedFile(t, filepath.Join(state, "volume"))
	readable := exec.CommandContext(ctx, "docker", "exec", "--user", "9000:9000", container, "/bin/sh", "-c",
		"test -r /opt/ejabberd/conf/ejabberd.yml && test -r /opt/ejabberd/conf/ca.pem && test -r /opt/ejabberd/conf/server.pem && test -r /opt/cynapsa/ebin/mod_cynapsa_mesh.beam")
	if output, err := readable.CombinedOutput(); err != nil {
		t.Fatalf("uid 9000 cannot read staged shared-group inputs: %v\n%s", err, output)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stopSharedGroupEnvironment(t, repository, harness, state)
		}
	})

	wantAdmin(t, ctx, repository, harness, state, "ready", "ready\tmesh.test\tsingle_node_mnesia")
	wantAdmin(t, ctx, repository, harness, state, "create", "", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "Mesh-A")
	// Idempotent membership changes leave the current snapshot unchanged.
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-b", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "snapshot", "agent-a@mesh.test\nagent-b@mesh.test", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "create", "", "Mesh-Preserve")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "Mesh-Preserve")

	groupProbe := buildSharedGroupBinary(t, repository, "./test/e2e/ejabberd/group_probe", "cynapsa-shared-group-adversary")
	wantAdmin(t, ctx, repository, harness, state, "create", "", "Mesh-Paged")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "Mesh-Paged")
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-b", "Mesh-Paged")
	wantAdmin(t, ctx, repository, harness, state, "seed", "", "page", "257", "Mesh-Paged")
	paged := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"paged-snapshot", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"], "Mesh-Paged", "page", "257")
	if !linePresent(paged, "paged-snapshot-pages=2 members=259") {
		t.Fatalf("paged snapshot probe omitted exact multi-page evidence:\n%s", paged)
	}
	writeArtifact(t, "shared-group-paged-snapshot.txt", paged)
	expiry := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"paged-expiry", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"],
		"Mesh-Paged", "page", "257")
	for _, line := range []string{"paged-expiry-stalled-session=closed", "paged-expiry-capacity-released-pages=2 members=259"} {
		if !linePresent(expiry, line) {
			t.Fatalf("paged expiry probe omitted %q:\n%s", line, expiry)
		}
	}
	writeArtifact(t, "shared-group-paged-expiry.txt", expiry)
	runPagedInvalidationScenario(t, ctx, repository, harness, state, groupProbe, credentials)
	runAuthorityRevalidationScenario(t, ctx, repository, harness, state, groupProbe, credentials)
	matrix := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"matrix", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"],
		credentials["CYNAPSA_EJABBERD_AGENT_C"], "Mesh-A")
	assertSharedGroupMatrix(t, matrix)
	writeArtifact(t, "shared-group-initial.txt", matrix)
	const expiredUploadToken = "DDDDDDDDDDDDDDDDDDDDDDDDDD"
	wantAdmin(t, ctx, repository, harness, state, "upload-expired-seed", "ok",
		"agent-a", "Mesh-A", expiredUploadToken)
	wantAdmin(t, ctx, repository, harness, state, "upload-count", "1", "agent-a", "Mesh-A")
	uploadReuse := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"upload-reused-id", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], "Mesh-A")
	for _, line := range []string{"upload-reused-id-objects=2", "upload-http-gate=passed"} {
		if !linePresent(uploadReuse, line) {
			t.Fatalf("upload ownership probe omitted %q:\n%s", line, uploadReuse)
		}
	}
	wantAdmin(t, ctx, repository, harness, state, "upload-count", "2", "agent-a", "Mesh-A")
	if got := runSharedGroupCommand(t, ctx, repository, harness, "admin", state,
		"upload-file", "agent-a", expiredUploadToken); got != "absent" {
		t.Fatalf("expired pending upload survived admission prune: %q", got)
	}
	writeArtifact(t, "shared-group-upload-ownership.txt", uploadReuse)
	wantAdmin(t, ctx, repository, harness, state, "offline-count", "0", "agent-b")
	mamBefore := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "mam-count", "agent-a")
	offlineNegative := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"offline-negative", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"], "Mesh-A")
	if !linePresent(offlineNegative, "offline-denied-no-store=sent") {
		t.Fatalf("offline negative probe omitted completion evidence:\n%s", offlineNegative)
	}
	wantAdmin(t, ctx, repository, harness, state, "offline-count", "0", "agent-b")
	wantAdmin(t, ctx, repository, harness, state, "mam-count", mamBefore, "agent-a")
	writeArtifact(t, "shared-group-offline-negative.txt", offlineNegative)

	offlineStored := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"send-offline", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_B"], "CYNAPSA_EJABBERD_PASSWORD_B",
		credentials["CYNAPSA_EJABBERD_AGENT_A"], "Mesh-A")
	if !linePresent(offlineStored, "offline-current-message=sent") {
		t.Fatalf("offline storage probe omitted completion evidence:\n%s", offlineStored)
	}
	// The dedicated authority plane is live-only: even currently authorized
	// offline traffic is never admitted to a mailbox/replay path.
	wantAdmin(t, ctx, repository, harness, state, "offline-count", "0", "agent-a")

	wantAdmin(t, ctx, repository, harness, state, "mam-seed", "ok", "agent-a", "agent-b", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "mam-count", "2", "agent-a")
	wantAdmin(t, ctx, repository, harness, state, "mam-prefs-legacy", "ok", "agent-a", "Mesh-A")
	legacyPrefs := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "mam-prefs-state", "agent-a")
	if !strings.Contains(legacyPrefs, "always") {
		t.Fatalf("legacy MAM preference seed not visible: %q", legacyPrefs)
	}
	const removedUploadToken = "AAAAAAAAAAAAAAAAAAAAAAAAAA"
	const retainedUploadToken = "BBBBBBBBBBBBBBBBBBBBBBBBBB"
	wantAdmin(t, ctx, repository, harness, state, "upload-seed", "ok", "agent-a", "Mesh-A", removedUploadToken)
	wantAdmin(t, ctx, repository, harness, state, "upload-seed", "ok", "agent-a", "Mesh-Preserve", retainedUploadToken)
	if got := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "upload-file", "agent-a", removedUploadToken); got != "present" {
		t.Fatalf("removed-mesh upload seed = %q", got)
	}
	if got := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "upload-file", "agent-a", retainedUploadToken); got != "present" {
		t.Fatalf("other-mesh upload seed = %q", got)
	}
	wantAdmin(t, ctx, repository, harness, state, "upload-count", "3", "agent-a", "Mesh-A")

	revocationLog := filepath.Join(state, "exact-resource-revocation.log")
	revocationFile, err := os.OpenFile(revocationLog, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create revocation artifact: %v", err)
	}
	revocationCtx, revocationCancel := context.WithCancel(ctx)
	defer revocationCancel()
	revocation := exec.CommandContext(revocationCtx, groupProbe,
		"wait-revoked",
		credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], "CYNAPSA_EJABBERD_PASSWORD_A", "Mesh-A")
	revocation.Dir = repository
	revocation.Env = append(os.Environ(),
		"CYNAPSA_EJABBERD_PASSWORD_A="+credentials["CYNAPSA_EJABBERD_PASSWORD_A"])
	revocation.Stdout, revocation.Stderr = revocationFile, revocationFile
	if err := revocation.Start(); err != nil {
		_ = revocationFile.Close()
		t.Fatalf("start exact-resource revocation probe: %v", err)
	}
	revocationDone := make(chan error, 1)
	go func() { revocationDone <- revocation.Wait() }()
	waitForSharedGroupLine(t, revocationLog, "revocation-wait-ready", revocationDone, 20*time.Second)

	preserveSignal := filepath.Join(state, "preserve-check")
	preserveLog := filepath.Join(state, "other-mesh-preserved.log")
	preserveFile, err := os.OpenFile(preserveLog, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create other-mesh artifact: %v", err)
	}
	preserveCtx, preserveCancel := context.WithCancel(ctx)
	defer preserveCancel()
	preserve := exec.CommandContext(preserveCtx, groupProbe,
		"wait-preserved", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], "CYNAPSA_EJABBERD_PASSWORD_A", "Mesh-Preserve", preserveSignal)
	preserve.Dir = repository
	preserve.Env = append(os.Environ(), "CYNAPSA_EJABBERD_PASSWORD_A="+credentials["CYNAPSA_EJABBERD_PASSWORD_A"])
	preserve.Stdout, preserve.Stderr = preserveFile, preserveFile
	if err := preserve.Start(); err != nil {
		_ = preserveFile.Close()
		t.Fatalf("start other-mesh preservation probe: %v", err)
	}
	preserveDone := make(chan error, 1)
	go func() { preserveDone <- preserve.Wait() }()
	waitForSharedGroupLine(t, preserveLog, "other-mesh-session-ready", preserveDone, 20*time.Second)

	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-c", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "snapshot", "agent-a@mesh.test\nagent-b@mesh.test\nagent-c@mesh.test", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "remove-many", "", "agent-a,agent-c", "Mesh-A")
	if err := os.WriteFile(preserveSignal, nil, 0o600); err != nil {
		t.Fatalf("signal other-mesh preservation probe: %v", err)
	}
	waitForSharedGroupLine(t, revocationLog, "removed-session-revoked", revocationDone, 20*time.Second)
	waitForSharedGroupLine(t, preserveLog, "other-mesh-session=preserved", preserveDone, 20*time.Second)
	if err := preserveFile.Close(); err != nil {
		t.Fatalf("close other-mesh artifact: %v", err)
	}
	if err := revocationFile.Close(); err != nil {
		t.Fatalf("close revocation artifact: %v", err)
	}
	wantAdmin(t, ctx, repository, harness, state, "snapshot", "agent-b@mesh.test", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "offline-count", "0", "agent-a")
	wantAdmin(t, ctx, repository, harness, state, "mam-count", "0", "agent-a")
	wantAdmin(t, ctx, repository, harness, state, "upload-count", "0", "agent-a", "Mesh-A")
	if got := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "upload-file", "agent-a", removedUploadToken); got != "absent" {
		t.Fatalf("removed owner upload survived purge: %q", got)
	}
	if got := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "upload-file", "agent-a", retainedUploadToken); got != "present" {
		t.Fatalf("other-mesh upload was deleted: %q", got)
	}
	denied := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"bind-denied", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], "CYNAPSA_EJABBERD_PASSWORD_A", "Mesh-A")
	if string(denied) != "bind=denied\n" {
		t.Fatalf("removed member bind result = %q", denied)
	}

	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", "Mesh-A")
	wantAdmin(t, ctx, repository, harness, state, "snapshot", "agent-a@mesh.test\nagent-b@mesh.test", "Mesh-A")
	revocationOutput, err := os.ReadFile(revocationLog)
	if err != nil {
		t.Fatalf("read revocation artifact: %v", err)
	}
	writeArtifact(t, "shared-group-exact-resource-revocation.txt", revocationOutput)
	serverLifecycle := runDockerLogs(t, container)
	if !bytes.Contains(serverLifecycle, []byte("Forbidden c2s session for agent-a@mesh.test/Mesh-A")) {
		t.Errorf("server did not record removal authorization fence:\n%s", serverLifecycle)
	}
	writeArtifact(t, "shared-group-server-lifecycle.log", serverLifecycle)

	runSharedGroupCommand(t, ctx, repository, harness, "restart", state)
	wantAdmin(t, ctx, repository, harness, state, "ready", "ready\tmesh.test\tsingle_node_mnesia")
	migratedPrefs := runSharedGroupCommand(t, ctx, repository, harness, "admin", state, "mam-prefs-state", "agent-a")
	if !strings.Contains(migratedPrefs, "never,[],") || strings.Contains(migratedPrefs, "always,") {
		t.Fatalf("legacy MAM preference was not migrated fail-closed: %q", migratedPrefs)
	}
	wantAdmin(t, ctx, repository, harness, state, "snapshot", "agent-a@mesh.test\nagent-b@mesh.test", "Mesh-A")
	postRestart := runGroupProbe(t, ctx, repository, groupProbe, credentials,
		"matrix", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"],
		credentials["CYNAPSA_EJABBERD_AGENT_C"], "Mesh-A")
	assertSharedGroupMatrix(t, postRestart)
	writeArtifact(t, "shared-group-post-restart.txt", postRestart)

	stopSharedGroupEnvironment(t, repository, harness, state)
	stopped = true
	assertDockerObjectAbsent(t, "container", container)
	assertDockerObjectAbsent(t, "network", network)
	assertDockerObjectAbsent(t, "volume", volume)
	for _, private := range []string{"credentials.env", "ca-key.pem", "server-key.pem", "server.pem"} {
		if _, err := os.Stat(filepath.Join(state, private)); !os.IsNotExist(err) {
			t.Errorf("private fixture survived teardown: %s (%v)", private, err)
		}
	}
	sourceAfter, sourceModeAfter := fixtureSnapshot(t, repositorySource)
	if sourceAfter != sourceBefore || sourceModeAfter != sourceModeBefore {
		t.Errorf("repository module source changed during qualification: digest %x→%x mode %v→%v", sourceBefore, sourceAfter, sourceModeBefore, sourceModeAfter)
	}
	assertNoSharedGroupSecrets(t, filepath.Join(state, "artifacts"), credentials)
}

func TestSharedGroupV1TraceabilityRecordsCompleteComposition(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "test", "e2e", "fixtures", "shared_group_traceability_v1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var matrix traceabilityMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatalf("decode shared-group traceability: %v", err)
	}
	if matrix.SchemaVersion != 1 || matrix.CandidateCommit != runtimeCandidateBinding {
		t.Fatalf("traceability provenance = version %d candidate %q", matrix.SchemaVersion, matrix.CandidateCommit)
	}
	bindTraceabilityCandidate(t, repositoryRoot(t), "shared-group-traceability-candidate.json")
	seen, covered := map[string]bool{}, map[string]bool{}
	for _, row := range matrix.Rows {
		if seen[row.RequirementID] || row.TestName == "" || row.ArtifactReference == "" {
			t.Fatalf("malformed or duplicate traceability row: %#v", row)
		}
		seen[row.RequirementID] = true
		covered[row.RequirementID] = strings.HasPrefix(row.ObservedResult, "covered-")
	}
	for _, required := range []string{"SG-01", "SG-02", "SG-03", "SG-04", "SG-05", "SG-06", "SG-07", "SG-08", "SG-09", "SG-10", "SG-11", "SG-12"} {
		if !seen[required] {
			t.Errorf("traceability omits %s", required)
		}
	}
	for _, requirement := range []string{"SG-09", "SG-10", "SG-11"} {
		if !covered[requirement] {
			t.Errorf("%s is not recorded as covered", requirement)
		}
	}
}

func buildSharedGroupBinary(t *testing.T, repository, packagePath, name string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-trimpath", "-o", binary, packagePath)
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, output)
	}
	return binary
}

func runSharedGroupCommand(t *testing.T, ctx context.Context, repository, harness string, arguments ...string) string {
	t.Helper()
	var command *exec.Cmd
	if len(arguments) > 0 && arguments[0] == "start" {
		commandArguments := append([]string{"-c", `umask 077; exec "$@"`, "sh", harness}, arguments...)
		command = exec.CommandContext(ctx, "sh", commandArguments...)
	} else {
		command = exec.CommandContext(ctx, harness, arguments...)
	}
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("shared-group harness %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func fixtureSnapshot(t *testing.T, path string) ([sha256.Size]byte, fs.FileMode) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(data), info.Mode().Perm()
}

func assertFixtureMode(t *testing.T, path string, want fs.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("fixture %s mode=%v, want %v", path, got, want)
	}
}

func wantAdmin(t *testing.T, ctx context.Context, repository, harness, state, action, expected string, arguments ...string) {
	t.Helper()
	actual := runSharedGroupCommand(t, ctx, repository, harness, append([]string{"admin", state, action}, arguments...)...)
	if actual != expected {
		t.Fatalf("admin %s output = %q, want %q", action, actual, expected)
	}
}

func runGroupProbe(t *testing.T, ctx context.Context, repository, binary string, credentials map[string]string, arguments ...string) []byte {
	t.Helper()
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"CYNAPSA_EJABBERD_PASSWORD_A="+credentials["CYNAPSA_EJABBERD_PASSWORD_A"],
		"CYNAPSA_EJABBERD_PASSWORD_B="+credentials["CYNAPSA_EJABBERD_PASSWORD_B"],
		"CYNAPSA_EJABBERD_PASSWORD_C="+credentials["CYNAPSA_EJABBERD_PASSWORD_C"],
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("independent group adversary %s: %v\n%s", arguments[0], err, output)
	}
	return output
}

func runAuthorityRevalidationScenario(t *testing.T, ctx context.Context, repository, harness, state, binary string, credentials map[string]string) {
	t.Helper()
	const mesh = "Mesh-Sync"
	wantAdmin(t, ctx, repository, harness, state, "create", "", mesh)
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-a", mesh)
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-b", mesh)
	wantAdmin(t, ctx, repository, harness, state, "add", "", "agent-c", mesh)

	logPath := filepath.Join(state, "authority-revalidation.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create authority revalidation artifact: %v", err)
	}
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	signal := filepath.Join(state, "authority-mutated")
	command := exec.CommandContext(probeCtx, binary,
		"authority-revalidation", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"],
		credentials["CYNAPSA_EJABBERD_AGENT_C"], mesh, signal)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"CYNAPSA_EJABBERD_PASSWORD_A="+credentials["CYNAPSA_EJABBERD_PASSWORD_A"],
		"CYNAPSA_EJABBERD_PASSWORD_B="+credentials["CYNAPSA_EJABBERD_PASSWORD_B"],
	)
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start authority revalidation probe: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waitForSharedGroupLine(t, logPath, "authority-mutation-ready", done, 20*time.Second)
	wantAdmin(t, ctx, repository, harness, state, "remove", "", "agent-c", mesh)
	if err := os.WriteFile(signal, nil, 0o600); err != nil {
		cancel()
		_ = logFile.Close()
		t.Fatalf("signal authority mutation: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			_ = logFile.Close()
			output, _ := os.ReadFile(logPath)
			t.Fatalf("authority revalidation probe: %v\n%s", err, output)
		}
	case <-time.After(25 * time.Second):
		cancel()
		_ = logFile.Close()
		t.Fatal("authority revalidation probe timed out")
	}
	if err := logFile.Close(); err != nil {
		t.Fatalf("close authority revalidation artifact: %v", err)
	}
	output, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"authority-pre-sync=blocked",
		"authority-initial-snapshot=complete",
		"authority-stale-snapshot=blocked",
		"authority-revalidated-snapshot=complete",
		"authority-current-membership=reconciled",
		"authority-reconnect=fresh-sync-required",
	} {
		if !linePresent(output, line) {
			t.Errorf("authority revalidation omitted %q:\n%s", line, output)
		}
	}
	writeArtifact(t, "shared-group-authority-revalidation.txt", output)
}

func runPagedInvalidationScenario(t *testing.T, ctx context.Context, repository, harness, state, binary string, credentials map[string]string) {
	t.Helper()
	logPath := filepath.Join(state, "paged-invalidation.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create paged invalidation artifact: %v", err)
	}
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	signal := filepath.Join(state, "paged-mutated")
	command := exec.CommandContext(probeCtx, binary,
		"paged-invalidation", credentials["CYNAPSA_EJABBERD_ENDPOINT"], credentials["CYNAPSA_EJABBERD_CA"],
		credentials["CYNAPSA_EJABBERD_AGENT_A"], credentials["CYNAPSA_EJABBERD_AGENT_B"],
		"Mesh-Paged", "page", "257", signal)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"CYNAPSA_EJABBERD_PASSWORD_A="+credentials["CYNAPSA_EJABBERD_PASSWORD_A"])
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start paged invalidation probe: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waitForSharedGroupLine(t, logPath, "paged-invalidation-mutation-ready", done, 20*time.Second)
	wantAdmin(t, ctx, repository, harness, state, "add", "", "page-257", "Mesh-Paged")
	if err := os.WriteFile(signal, nil, 0o600); err != nil {
		cancel()
		_ = logFile.Close()
		t.Fatalf("signal paged mutation: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			_ = logFile.Close()
			output, _ := os.ReadFile(logPath)
			t.Fatalf("paged invalidation probe: %v\n%s", err, output)
		}
	case <-time.After(25 * time.Second):
		cancel()
		_ = logFile.Close()
		t.Fatal("paged invalidation probe timed out")
	}
	if err := logFile.Close(); err != nil {
		t.Fatalf("close paged invalidation artifact: %v", err)
	}
	output, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"paged-invalidation-stale=conflict",
		"paged-invalidation-fresh-pages=2 members=260",
	} {
		if !linePresent(output, line) {
			t.Errorf("paged invalidation omitted %q:\n%s", line, output)
		}
	}
	writeArtifact(t, "shared-group-paged-invalidation.txt", output)
}

func assertSharedGroupMatrix(t *testing.T, output []byte) {
	t.Helper()
	for _, line := range []string{
		"allowed-bind=true", "nonmember-bind=denied", "cross-resource-bind=denied",
		"pre-sync-route=blocked", "recipient-unsynced-route=blocked",
		"authority-snapshot=complete", "authority-pages=1",
		"both-session-authority=synchronized",
		"local-authority-time=delivered",
		"same-resource-message=delivered", "same-resource-signal=delivered",
		"same-resource-iq-terminal=delivered", "same-resource-presence=delivered",
		"bare-message=blocked", "cross-resource-message=blocked",
		"bare-signal=blocked", "cross-resource-signal=blocked", "foreign-message=blocked",
		"foreign-service-message=blocked", "foreign-service-iq=blocked",
		"local-service-message=blocked", "local-service-iq-unknown=blocked",
		"bare-presence=blocked", "cross-resource-presence=blocked",
		"bare-presence-subscribe=blocked", "bare-presence-probe=blocked",
		"undirected-presence=blocked",
		"forwarded-wrapper=blocked", "carbon-wrapper=blocked", "mam-wrapper=blocked",
		"nested-stanza-wrapper=blocked",
		"inherited-namespace-wrapper=blocked", "prefixed-forwarded-wrapper=blocked",
		"mixed-namespace-declaration=blocked",
	} {
		if !linePresent(output, line) {
			t.Errorf("shared-group matrix omitted %q:\n%s", line, output)
		}
	}
}

func linePresent(output []byte, expected string) bool {
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		if string(line) == expected {
			return true
		}
	}
	return false
}

func waitForSharedGroupLine(t *testing.T, path, expected string, done <-chan error, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		if output, err := os.ReadFile(path); err == nil && linePresent(output, expected) {
			return
		}
		select {
		case err := <-done:
			output, _ := os.ReadFile(path)
			if linePresent(output, expected) {
				return
			}
			t.Fatalf("production recovery stopped before %q: %v\n%s", expected, err, output)
		case <-deadline.C:
			output, _ := os.ReadFile(path)
			t.Fatalf("timed out waiting for %q:\n%s", expected, output)
		case <-poll.C:
		}
	}
}

func stopSharedGroupEnvironment(t *testing.T, repository, harness, state string) {
	t.Helper()
	command := exec.Command(harness, "stop", state)
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Errorf("stop shared-group environment: %v\n%s", err, output)
	}
}

func assertDockerObjectAbsent(t *testing.T, kind, name string) {
	t.Helper()
	arguments := []string{kind, "inspect", name}
	if kind == "container" {
		arguments = []string{"inspect", name}
	}
	if err := exec.Command("docker", arguments...).Run(); err == nil {
		t.Errorf("disposable Docker %s leaked: %s", kind, name)
	}
}

func runDockerLogs(t *testing.T, container string) []byte {
	t.Helper()
	output, err := exec.Command("docker", "logs", container).CombinedOutput()
	if err != nil {
		t.Fatalf("capture shared-group server lifecycle: %v\n%s", err, output)
	}
	return output
}

func assertNoSharedGroupSecrets(t *testing.T, root string, credentials map[string]string) {
	t.Helper()
	secrets := [][]byte{
		[]byte(credentials["CYNAPSA_EJABBERD_PASSWORD_A"]),
		[]byte(credentials["CYNAPSA_EJABBERD_PASSWORD_B"]),
		[]byte(credentials["CYNAPSA_EJABBERD_PASSWORD_C"]),
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, secret := range secrets {
			if len(secret) > 0 && bytes.Contains(data, secret) {
				return fmt.Errorf("artifact contains generated credential: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
