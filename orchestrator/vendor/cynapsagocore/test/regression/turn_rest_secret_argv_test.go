package regression

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const syntheticTURNRestSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestTURNRestSecretRendererKeepsSecretOutOfProcessArgumentsAndOutput(t *testing.T) {
	root := repositoryRoot(t)
	harness := filepath.Join(root, "test", "e2e", "ejabberd", "run.sh")
	template := filepath.Join(root, "test", "e2e", "ejabberd", "ejabberd.yml.in")
	state := t.TempDir()
	secretFile := filepath.Join(state, "turn-rest-secret")
	if err := os.WriteFile(secretFile, []byte(syntheticTURNRestSecret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(state, "ejabberd.yml")

	realSed, err := exec.LookPath("sed")
	if err != nil {
		t.Skip("sed unavailable")
	}
	realAwk, err := exec.LookPath("awk")
	if err != nil {
		t.Skip("awk unavailable")
	}
	fakeBin := filepath.Join(state, "fake-bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	argvLog := filepath.Join(state, "renderer-argv.log")
	wrapper := `#!/bin/sh
set -eu
tool=${0##*/}
{
  printf 'tool=%s\n' "$tool"
  for argument do
    printf 'argument=%s\n' "$argument"
  done
  printf '%s\n' '--'
} >>"$CYNAPSA_TEST_ARGV_LOG"
case "$tool" in
  sed) exec "$CYNAPSA_TEST_REAL_SED" "$@" ;;
  awk) exec "$CYNAPSA_TEST_REAL_AWK" "$@" ;;
  *) exit 127 ;;
esac
`
	for _, tool := range []string{"sed", "awk"} {
		if err := os.WriteFile(filepath.Join(fakeBin, tool), []byte(wrapper), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	command := exec.Command(harness, "render-config", template, output, "shared-group-extdisco", "5443")
	command.Dir = root
	command.Env = append(filteredEnvironment("PATH", "CYNAPSA_EJABBERD_TURN_REST_SECRET", "CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE"),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE="+secretFile,
		"CYNAPSA_TEST_ARGV_LOG="+argvLog,
		"CYNAPSA_TEST_REAL_SED="+realSed,
		"CYNAPSA_TEST_REAL_AWK="+realAwk,
	)
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render extdisco configuration: %v\n%s", err, combined)
	}
	if bytes.Contains(combined, []byte(syntheticTURNRestSecret)) {
		t.Fatal("renderer exposed the TURN REST secret on stdout or stderr")
	}
	arguments, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(arguments, []byte(syntheticTURNRestSecret)) {
		t.Fatalf("renderer subprocess argv exposed the TURN REST secret:\n%s", arguments)
	}
	if !bytes.Contains(arguments, []byte("tool=sed")) || !bytes.Contains(arguments, []byte("tool=awk")) {
		t.Fatalf("fake renderer did not capture both sed and awk argv:\n%s", arguments)
	}

	rendered, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(rendered, []byte(syntheticTURNRestSecret)) != 1 || bytes.Contains(rendered, []byte("__TURN_REST_SECRET__")) {
		t.Fatal("rendered configuration does not contain exactly one injected secret")
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret-bearing rendered configuration mode=%o, want 600", info.Mode().Perm())
	}
}

func TestTURNRestSecretRendererRejectsMalformedInputWithoutExposureOrOutput(t *testing.T) {
	root := repositoryRoot(t)
	harness := filepath.Join(root, "test", "e2e", "ejabberd", "run.sh")
	template := filepath.Join(root, "test", "e2e", "ejabberd", "ejabberd.yml.in")
	state := t.TempDir()
	malformed := strings.Repeat("a", 63) + "Z"
	secretFile := filepath.Join(state, "turn-rest-secret")
	if err := os.WriteFile(secretFile, []byte(malformed+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(state, "ejabberd.yml")
	command := exec.Command(harness, "render-config", template, output, "shared-group-extdisco", "5443")
	command.Dir = root
	command.Env = append(filteredEnvironment("CYNAPSA_EJABBERD_TURN_REST_SECRET", "CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE"),
		"CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE="+secretFile,
	)
	combined, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("malformed TURN REST secret was accepted")
	}
	if bytes.Contains(combined, []byte(malformed)) {
		t.Fatal("malformed TURN REST secret was exposed on stdout or stderr")
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Fatalf("failed rendering retained output: %v", statErr)
	}
	for _, suffix := range []string{".tmp", ".with-turn-secret", ".no-group", ".no-extdisco", ".no-empty-extdisco"} {
		if _, statErr := os.Stat(output + suffix); !os.IsNotExist(statErr) {
			t.Fatalf("failed rendering retained temporary output %s: %v", suffix, statErr)
		}
	}
}

func TestGeneratedQualificationSecretsAvoidHostArgumentInterpolation(t *testing.T) {
	root := repositoryRoot(t)
	ejabberd := readFile(t, filepath.Join(root, "test", "e2e", "ejabberd", "run.sh"))
	coturn := readFile(t, filepath.Join(root, "test", "production", "coturn", "run.sh"))
	combined := ejabberd + "\n" + coturn
	for _, forbidden := range []string{
		`CYNAPSA_EJABBERD_TURN_REST_SECRET="$rest_secret"`,
		`s|__TURN_REST_SECRET__|${turn_rest_secret}`,
		`awk -v secret=`,
		`ejabberdctl register agent-a mesh.test "$password_a"`,
		`ejabberdctl register agent-b mesh.test "$password_b"`,
		`ejabberdctl register agent-c mesh.test "$password_c"`,
	} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("generated secret is interpolated into a host process argument via %q", forbidden)
		}
	}
	for _, required := range []string{
		"CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE",
		`inject_turn_rest_secret "$turn_rest_secret_file"`,
		`openssl rand -hex 32 >"$rest_secret_file"`,
		`rm -f "$state/credentials.env"`,
		`"$state/turn-rest-secret"`,
	} {
		if !strings.Contains(combined, required) {
			t.Fatalf("private generated-secret handoff is missing %q", required)
		}
	}
	for _, line := range strings.Split(combined, "\n") {
		if (strings.Contains(line, "docker ") || strings.Contains(line, "sed ") || strings.Contains(line, "awk ") || strings.Contains(line, "python3 ")) &&
			(strings.Contains(line, "$rest_secret") || strings.Contains(line, "$turn_rest_secret") || strings.Contains(line, "$password_a") || strings.Contains(line, "$password_b") || strings.Contains(line, "$password_c")) {
			t.Fatalf("generated secret variable appears on a host process command line: %s", line)
		}
	}
}

func filteredEnvironment(prefixes ...string) []string {
	result := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		keep := true
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix+"=") {
				keep = false
				break
			}
		}
		if keep {
			result = append(result, entry)
		}
	}
	return result
}
