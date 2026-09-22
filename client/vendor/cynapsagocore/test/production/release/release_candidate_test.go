package release_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

type abiContract struct {
	SchemaVersion int      `json:"schema_version"`
	ABIVersion    int      `json:"abi_version"`
	Exports       []string `json:"exports"`
}

type artifactManifest struct {
	SchemaVersion   int    `json:"schema_version"`
	ArtifactVersion string `json:"artifact_version"`
	ABIVersion      int    `json:"abi_version"`
	CoreCommit      string `json:"core_commit"`
	SourceDateEpoch int64  `json:"source_date_epoch"`
	GoVersion       string `json:"go_version"`
	Target          struct {
		OS, Arch string
	} `json:"target"`
	Library struct {
		Path, SHA256 string
	} `json:"library"`
	Header struct {
		Path, SHA256 string
	} `json:"header"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestV1ABIContractMatchesHeaderAndLinkerAllowLists(t *testing.T) {
	root := repositoryRoot(t)
	contract := readABIContract(t, filepath.Join(root, "conformance", "v1", "abi.json"))
	if contract.SchemaVersion != 1 || contract.ABIVersion != 1 || len(contract.Exports) != 21 || !sort.StringsAreSorted(contract.Exports) {
		t.Fatalf("invalid ABI contract: %#v", contract)
	}

	header := readFile(t, filepath.Join(root, "cmd", "cynapsacore-shared", "cynapsacore_v1.h"))
	darwin := readFile(t, filepath.Join(root, "cmd", "cynapsacore-shared", "exports_darwin.txt"))
	linux := readFile(t, filepath.Join(root, "cmd", "cynapsacore-shared", "exports_linux.map"))
	for name, actual := range map[string][]string{
		"C header":         headerExports(header),
		"Darwin allowlist": darwinExports(t, darwin),
		"Linux allowlist":  linuxExports(t, linux),
	} {
		if err := requireExactExportSet(contract.Exports, actual); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestExactExportSetRejectsMutants(t *testing.T) {
	expected := []string{"cynapsa_v1_a", "cynapsa_v1_b"}
	for name, mutant := range map[string][]string{
		"missing":   {"cynapsa_v1_a"},
		"extra":     {"cynapsa_v1_a", "cynapsa_v1_b", "cynapsa_v1_private"},
		"duplicate": {"cynapsa_v1_a", "cynapsa_v1_b", "cynapsa_v1_b"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := requireExactExportSet(expected, mutant); err == nil {
				t.Fatal("mutated export set was accepted")
			}
		})
	}
	if err := requireExactExportSet(expected, append([]string(nil), expected...)); err != nil {
		t.Fatalf("exact export set rejected: %v", err)
	}
}

func TestNativeReleaseCandidateIsReproducibleAndSelfDescribing(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("release builder supports POSIX targets")
	}
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("cgo disabled")
	}
	for _, tool := range []string{"cc", "git", "nm", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s unavailable", tool)
		}
	}

	source := committedRepositorySnapshot(t, repositoryRoot(t))
	commit := gitOutput(t, source, "rev-parse", "HEAD")
	commitEpoch := gitOutput(t, source, "show", "-s", "--format=%ct", commit)
	version := "0.1.0-test"
	cleanPackage := runRelease(t, source, version, []string{"CYNAPSA_RELEASE_ALLOW_DIRTY_FOR_TESTS=0"})

	// A strict build must reject every untracked path before compilation.
	untracked := filepath.Join(source, "untracked-release-mutant.go")
	if err := os.WriteFile(untracked, []byte("package mutant\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertReleaseRejected(t, source, version+"-untracked", "worktree must be clean")
	if err := os.Remove(untracked); err != nil {
		t.Fatal(err)
	}

	// Even the explicitly test-only dirty mode must build the exact committed
	// archive. Invalid dirty Go source would fail compilation if the builder
	// read from the checkout instead of the recorded commit.
	mutatedPath := filepath.Join(source, "cmd", "cynapsacore-shared", "main.go")
	original, err := os.ReadFile(mutatedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mutatedPath, append(original, []byte("\nthis is not valid Go\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	assertReleaseRejected(t, source, version+"-tracked", "worktree must be clean")
	dirtyPackage := runRelease(t, source, version, []string{"CYNAPSA_RELEASE_ALLOW_DIRTY_FOR_TESTS=1"})

	cleanManifest := readManifest(t, cleanPackage)
	dirtyManifest := readManifest(t, dirtyPackage)
	if cleanManifest != dirtyManifest {
		t.Fatalf("manifest differs across exact-commit builds:\n%#v\n%#v", cleanManifest, dirtyManifest)
	}
	if cleanManifest.SchemaVersion != 1 || cleanManifest.ArtifactVersion != version || cleanManifest.ABIVersion != 1 || cleanManifest.CoreCommit != commit || fmt.Sprint(cleanManifest.SourceDateEpoch) != commitEpoch || cleanManifest.GoVersion != "go1.26.6" || cleanManifest.Target.OS != runtime.GOOS || cleanManifest.Target.Arch != runtime.GOARCH {
		t.Fatalf("unexpected manifest: %#v", cleanManifest)
	}

	cleanLibrary := filepath.Join(cleanPackage, filepath.FromSlash(cleanManifest.Library.Path))
	dirtyLibrary := filepath.Join(dirtyPackage, filepath.FromSlash(dirtyManifest.Library.Path))
	cleanBytes := readFile(t, cleanLibrary)
	dirtyBytes := readFile(t, dirtyLibrary)
	if !bytes.Equal(cleanBytes, dirtyBytes) {
		t.Fatal("native libraries from the same commit are not byte reproducible")
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(cleanBytes)); got != cleanManifest.Library.SHA256 {
		t.Fatalf("library digest = %s, manifest = %s", got, cleanManifest.Library.SHA256)
	}
	headerBytes := readFile(t, filepath.Join(cleanPackage, filepath.FromSlash(cleanManifest.Header.Path)))
	if got := fmt.Sprintf("%x", sha256.Sum256(headerBytes)); got != cleanManifest.Header.SHA256 {
		t.Fatalf("header digest = %s, manifest = %s", got, cleanManifest.Header.SHA256)
	}

	contract := readABIContract(t, filepath.Join(cleanPackage, "conformance", "v1", "abi.json"))
	if err := requireExactExportSet(contract.Exports, nativeExports(t, cleanLibrary)); err != nil {
		t.Fatalf("native dynamic exports: %v", err)
	}
	for _, directory := range []string{cleanPackage, dirtyPackage} {
		verifyPackageChecksums(t, directory)
	}
}

func committedRepositorySnapshot(t *testing.T, root string) string {
	t.Helper()
	// Build the candidate from every tracked and non-ignored untracked input in
	// the current worktree. The destination commit then becomes the exact clean
	// source tree used by the reproducibility assertions below.
	command := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	command.Dir = root
	encoded, err := command.Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}
	deletedCommand := exec.Command("git", "ls-files", "--deleted", "-z")
	deletedCommand.Dir = root
	deletedEncoded, err := deletedCommand.Output()
	if err != nil {
		t.Fatalf("list tracked deletions: %v", err)
	}
	deleted := make(map[string]struct{})
	for _, relative := range bytes.Split(deletedEncoded, []byte{0}) {
		if len(relative) != 0 {
			deleted[string(relative)] = struct{}{}
		}
	}
	destination := filepath.Join(t.TempDir(), "repository")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, relative := range bytes.Split(encoded, []byte{0}) {
		if len(relative) == 0 {
			continue
		}
		name := string(relative)
		// The snapshot represents the current tracked worktree and then commits
		// that exact state. Preserve a deletion only when Git itself reports the
		// indexed path as deleted; an unrelated lstat failure remains fatal.
		if _, intentionallyDeleted := deleted[name]; intentionallyDeleted {
			continue
		}
		sourcePath := filepath.Join(root, filepath.FromSlash(name))
		destinationPath := filepath.Join(destination, filepath.FromSlash(name))
		info, err := os.Lstat(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, destinationPath); err != nil {
				t.Fatal(err)
			}
			continue
		}
		contents := readFile(t, sourcePath)
		if err := os.WriteFile(destinationPath, contents, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
	}
	for _, arguments := range [][]string{
		{"init", "-q"},
		{"config", "user.name", "Cynapsa Release Test"},
		{"config", "user.email", "release-test@invalid.example"},
		{"add", "-f", "-A"},
		{"commit", "-q", "-m", "test snapshot"},
	} {
		gitOutput(t, destination, arguments...)
	}
	return destination
}

func TestCommittedRepositorySnapshotPreservesOnlyGitReportedDeletions(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, arguments := range [][]string{
		{"init", "-q"},
		{"config", "user.name", "Cynapsa Release Test"},
		{"config", "user.email", "release-test@invalid.example"},
	} {
		gitOutput(t, source, arguments...)
	}
	for name, contents := range map[string]string{"kept.txt": "original\n", "deleted.txt": "remove me\n"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitOutput(t, source, "add", "-A")
	gitOutput(t, source, "commit", "-q", "-m", "fixture")
	if err := os.WriteFile(filepath.Join(source, "kept.txt"), []byte("modified\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "new.txt"), []byte("new candidate input\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "deleted.txt")); err != nil {
		t.Fatal(err)
	}

	snapshot := committedRepositorySnapshot(t, source)
	if got := string(readFile(t, filepath.Join(snapshot, "kept.txt"))); got != "modified\n" {
		t.Fatalf("tracked modification = %q", got)
	}
	if _, err := os.Lstat(filepath.Join(snapshot, "deleted.txt")); !os.IsNotExist(err) {
		t.Fatalf("tracked deletion was not preserved: %v", err)
	}
	if got := string(readFile(t, filepath.Join(snapshot, "new.txt"))); got != "new candidate input\n" {
		t.Fatalf("untracked candidate input = %q", got)
	}
	if got := gitOutput(t, snapshot, "status", "--porcelain"); got != "" {
		t.Fatalf("snapshot is dirty: %q", got)
	}
}

func runRelease(t *testing.T, repository, version string, extraEnvironment []string) string {
	t.Helper()
	output := filepath.Join(t.TempDir(), "release")
	command := exec.Command(filepath.Join(repository, "scripts", "release-candidate-v1.sh"), output, version)
	command.Dir = repository
	command.Env = append(append([]string(nil), os.Environ()...), extraEnvironment...)
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("release build: %v\n%s", err, combined)
	}
	return strings.TrimSpace(string(combined))
}

func assertReleaseRejected(t *testing.T, repository, version, expected string) {
	t.Helper()
	output := filepath.Join(t.TempDir(), "release")
	command := exec.Command(filepath.Join(repository, "scripts", "release-candidate-v1.sh"), output, version)
	command.Dir = repository
	command.Env = append(os.Environ(), "CYNAPSA_RELEASE_ALLOW_DIRTY_FOR_TESTS=0")
	combined, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("dirty release build unexpectedly succeeded: %s", combined)
	}
	if !strings.Contains(string(combined), expected) {
		t.Fatalf("release rejection = %q, want substring %q", combined, expected)
	}
}

func verifyPackageChecksums(t *testing.T, directory string) {
	t.Helper()
	encoded := readFile(t, filepath.Join(directory, "SHA256SUMS"))
	scanner := bufio.NewScanner(bytes.NewReader(encoded))
	listed := make(map[string]bool)
	var ordered []string
	for scanner.Scan() {
		line := scanner.Text()
		separator := strings.Index(line, "  ")
		if separator != 64 || len(line) <= separator+2 {
			t.Fatalf("malformed checksum line %q", line)
		}
		digest, path := line[:separator], line[separator+2:]
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			t.Fatalf("invalid SHA-256 %q for %q", digest, path)
		}
		if filepath.IsAbs(path) || strings.Contains(path, "\\") || filepath.ToSlash(filepath.Clean(filepath.FromSlash(path))) != path || path == "." || strings.HasPrefix(path, "../") || listed[path] {
			t.Fatalf("unsafe or duplicate checksum path %q", path)
		}
		listed[path] = true
		ordered = append(ordered, path)
		contents := readFile(t, filepath.Join(directory, filepath.FromSlash(path)))
		if got := fmt.Sprintf("%x", sha256.Sum256(contents)); got != digest {
			t.Fatalf("checksum for %s = %s, want %s", path, got, digest)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !sort.StringsAreSorted(ordered) {
		t.Fatalf("SHA256SUMS paths are not sorted: %v", ordered)
	}
	var actual []string
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "SHA256SUMS" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("release entry %s is not a regular file", relative)
		}
		actual = append(actual, relative)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(actual)
	if len(actual) != len(ordered) {
		t.Fatalf("checksum coverage count = %d, release file count = %d", len(ordered), len(actual))
	}
	for index := range actual {
		if actual[index] != ordered[index] {
			t.Fatalf("checksum coverage differs: listed=%v actual=%v", ordered, actual)
		}
	}
}

func readABIContract(t *testing.T, path string) abiContract {
	t.Helper()
	var contract abiContract
	if err := json.Unmarshal(readFile(t, path), &contract); err != nil {
		t.Fatal(err)
	}
	return contract
}

func headerExports(header []byte) []string {
	expression := regexp.MustCompile(`(?m)^CYNAPSA_API\s+\S+\s+(cynapsa_v1_[A-Za-z0-9_]+)\s*\(`)
	matches := expression.FindAllSubmatch(header, -1)
	exports := make([]string, 0, len(matches))
	for _, match := range matches {
		exports = append(exports, string(match[1]))
	}
	return exports
}

func darwinExports(t *testing.T, source []byte) []string {
	t.Helper()
	var exports []string
	for _, line := range strings.Split(strings.TrimSpace(string(source)), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "_cynapsa_v1_") || strings.ContainsAny(line, " \t;") {
			t.Fatalf("invalid Darwin export line %q", line)
		}
		exports = append(exports, strings.TrimPrefix(line, "_"))
	}
	return exports
}

func linuxExports(t *testing.T, source []byte) []string {
	t.Helper()
	text := string(source)
	global := strings.Index(text, "global:")
	local := strings.Index(text, "local:")
	if global < 0 || local <= global {
		t.Fatal("Linux linker map lacks ordered global/local sections")
	}
	var exports []string
	for _, field := range strings.Fields(text[global+len("global:") : local]) {
		if !strings.HasPrefix(field, "cynapsa_v1_") || !strings.HasSuffix(field, ";") || strings.Count(field, ";") != 1 {
			t.Fatalf("invalid Linux global export token %q", field)
		}
		exports = append(exports, strings.TrimSuffix(field, ";"))
	}
	return exports
}

func nativeExports(t *testing.T, library string) []string {
	t.Helper()
	var command *exec.Cmd
	if runtime.GOOS == "darwin" {
		command = exec.Command("nm", "-gU", library)
	} else {
		command = exec.Command("nm", "-D", "--defined-only", "--format=posix", library)
	}
	output, err := command.Output()
	if err != nil {
		t.Fatalf("enumerate native exports: %v", err)
	}
	var exports []string
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		name := fields[0]
		if runtime.GOOS == "darwin" {
			name = fields[len(fields)-1]
			name = strings.TrimPrefix(name, "_")
		} else if at := strings.IndexByte(name, '@'); at >= 0 {
			name = name[:at]
		}
		exports = append(exports, name)
	}
	return exports
}

func requireExactExportSet(expected, actual []string) error {
	normalize := func(values []string) ([]string, error) {
		seen := make(map[string]bool, len(values))
		result := append([]string(nil), values...)
		for _, value := range result {
			if value == "" || seen[value] {
				return nil, fmt.Errorf("empty or duplicate export %q", value)
			}
			seen[value] = true
		}
		sort.Strings(result)
		return result, nil
	}
	want, err := normalize(expected)
	if err != nil {
		return fmt.Errorf("expected set: %w", err)
	}
	got, err := normalize(actual)
	if err != nil {
		return fmt.Errorf("actual set: %w", err)
	}
	if len(want) != len(got) {
		return fmt.Errorf("export count = %d, want %d: %v", len(got), len(want), got)
	}
	for index := range want {
		if want[index] != got[index] {
			return fmt.Errorf("exports = %v, want %v", got, want)
		}
	}
	return nil
}

func gitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readManifest(t *testing.T, directory string) artifactManifest {
	t.Helper()
	var manifest artifactManifest
	if err := json.Unmarshal(readFile(t, filepath.Join(directory, "manifest.json")), &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}
