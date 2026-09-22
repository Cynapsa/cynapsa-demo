package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRank2Connectivity will verify the reliable fallback transport against a disposable test service.
func TestRank2Connectivity(t *testing.T) {
	runner := filepath.Join("..", "test", "e2e", "ejabberd", "run.sh")
	state := startHarnessWithUmask(t, runner, "077")
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if output, err := exec.Command(runner, "stop", state).CombinedOutput(); err != nil {
				t.Errorf("stop disposable server: %v\n%s", err, output)
			}
		}
	})
	assertNormalHarnessModes(t, state)
	container := readHarnessState(t, state, "container-id")
	network := readHarnessState(t, state, "network")
	volume := readHarnessState(t, state, "volume")
	readable := exec.Command("docker", "exec", "--user", "9000:9000", container, "/bin/sh", "-c",
		"test -r /opt/ejabberd/conf/ejabberd.yml && test -r /opt/ejabberd/conf/ca.pem && test -r /opt/ejabberd/conf/server.pem")
	if output, readableErr := readable.CombinedOutput(); readableErr != nil {
		t.Fatalf("uid 9000 cannot read staged normal-profile inputs: %v\n%s", readableErr, output)
	}
	if output, err := exec.Command(runner, "probe", state, "control").CombinedOutput(); err != nil {
		t.Fatalf("standalone protocol control: %v\n%s", err, output)
	}
	if output, err := exec.Command(runner, "probe", state, "adapter").CombinedOutput(); err != nil {
		t.Fatalf("production rank-2 negotiation: %v\n%s", err, output)
	}
	if output, err := exec.Command(runner, "stop", state).CombinedOutput(); err != nil {
		t.Fatalf("stop disposable server: %v\n%s", err, output)
	}
	stopped = true
	for kind, name := range map[string]string{"container": container, "network": network, "volume": volume} {
		if output, inspectErr := exec.Command("docker", kind, "inspect", name).CombinedOutput(); inspectErr == nil {
			t.Errorf("%s survived teardown: %s\n%s", kind, name, output)
		}
	}
	for _, secret := range []string{"credentials.env", "ca-key.pem", "server-key.pem", "server.pem"} {
		if _, statErr := os.Stat(filepath.Join(state, secret)); !os.IsNotExist(statErr) {
			t.Errorf("secret survived teardown: %s (%v)", secret, statErr)
		}
	}

	permissiveState := startHarnessWithUmask(t, runner, "022")
	permissiveStopped := false
	t.Cleanup(func() {
		if !permissiveStopped {
			_, _ = exec.Command(runner, "stop", permissiveState).CombinedOutput()
		}
	})
	assertNormalHarnessModes(t, permissiveState)
	permissiveObjects := map[string]string{
		"container": readHarnessState(t, permissiveState, "container-id"),
		"network":   readHarnessState(t, permissiveState, "network"),
		"volume":    readHarnessState(t, permissiveState, "volume"),
	}
	if output, err := exec.Command(runner, "stop", permissiveState).CombinedOutput(); err != nil {
		t.Fatalf("stop permissive-umask disposable server: %v\n%s", err, output)
	}
	permissiveStopped = true
	for kind, name := range permissiveObjects {
		if output, inspectErr := exec.Command("docker", kind, "inspect", name).CombinedOutput(); inspectErr == nil {
			t.Errorf("permissive-umask %s survived teardown: %s\n%s", kind, name, output)
		}
	}
	for _, secret := range []string{"credentials.env", "ca-key.pem", "server-key.pem", "server.pem"} {
		if _, statErr := os.Stat(filepath.Join(permissiveState, secret)); !os.IsNotExist(statErr) {
			t.Errorf("permissive-umask secret survived teardown: %s (%v)", secret, statErr)
		}
	}
}

func startHarnessWithUmask(t *testing.T, runner, mask string) string {
	t.Helper()
	start := exec.Command("sh", "-c", `umask "$1"; shift; exec "$@"`, "sh", mask, runner, "start", "normal")
	output, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start disposable server with umask %s: %v\n%s", mask, err, output)
	}
	state := strings.TrimSpace(string(output))
	if state == "" {
		t.Fatal("disposable server returned empty state path")
	}
	return state
}

func assertNormalHarnessModes(t *testing.T, state string) {
	t.Helper()
	for name, mode := range map[string]os.FileMode{
		"ejabberd.yml":    0o444,
		"ca.pem":          0o444,
		"server.pem":      0o600,
		"credentials.env": 0o600,
	} {
		info, statErr := os.Stat(filepath.Join(state, name))
		if statErr != nil || info.Mode().Perm() != mode {
			t.Fatalf("staged %s mode=%v error=%v, want %v", name, infoMode(info), statErr, mode)
		}
	}
}

func readHarnessState(t *testing.T, state, name string) string {
	t.Helper()
	value, err := os.ReadFile(filepath.Join(state, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(value))
}

func infoMode(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode().Perm()
}
