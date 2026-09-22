package e2e_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCandidateProvenanceAcceptsOnlyCleanExactHeadWithExternalArtifacts(t *testing.T) {
	root := t.TempDir()
	repository := initializeProvenanceRepository(t, filepath.Join(root, "repository"))
	head := provenanceRepositoryHead(t, repository)
	artifactDirectory := filepath.Join(root, "artifacts")
	provenance, err := resolveCandidateProvenance(repository, artifactDirectory, head)
	if err != nil {
		t.Fatal(err)
	}
	if provenance.TestedCommit != head || !provenance.WorktreeClean {
		t.Fatalf("provenance=%#v, want exact clean HEAD %s", provenance, head)
	}
}

func TestCandidateProvenanceRejectsEveryInWorktreeArtifactDirectory(t *testing.T) {
	repository := initializeProvenanceRepository(t, filepath.Join(t.TempDir(), "repository"))
	head := provenanceRepositoryHead(t, repository)
	trackedDirectory := filepath.Join(repository, "tracked-artifacts")
	if err := os.Mkdir(trackedDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trackedDirectory, "evidence.txt"), []byte("tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provenanceGit(t, repository, "add", "tracked-artifacts/evidence.txt")
	provenanceGit(t, repository, "commit", "-m", "track artifact mutant")
	head = provenanceRepositoryHead(t, repository)

	for _, artifactDirectory := range []string{
		repository,
		filepath.Join(repository, "not-created-yet", "e2e"),
		trackedDirectory,
	} {
		if _, err := resolveCandidateProvenance(repository, artifactDirectory, head); err == nil || !strings.Contains(err.Error(), "outside the tested worktree") {
			t.Fatalf("artifact directory %q error=%v, want in-worktree rejection", artifactDirectory, err)
		}
	}
}

func TestCandidateProvenanceRejectsIgnoredGoBuildInput(t *testing.T) {
	root := t.TempDir()
	repository := initializeProvenanceRepository(t, filepath.Join(root, "repository"))
	if err := os.WriteFile(filepath.Join(repository, ".gitignore"), []byte("generated.go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provenanceGit(t, repository, "add", ".gitignore")
	provenanceGit(t, repository, "commit", "-m", "ignore generated input")
	if err := os.WriteFile(filepath.Join(repository, "generated.go"), []byte("package ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	head := provenanceRepositoryHead(t, repository)
	if _, err := resolveCandidateProvenance(repository, filepath.Join(root, "artifacts"), head); err == nil || !strings.Contains(err.Error(), "ignored_inputs=true") {
		t.Fatalf("ignored Go input error=%v, want explicit rejection", err)
	}
}

func TestCandidateProvenanceRejectsGlobalExcludeInput(t *testing.T) {
	root := t.TempDir()
	repository := initializeProvenanceRepository(t, filepath.Join(root, "repository"))
	excludes := filepath.Join(root, "global-excludes")
	if err := os.WriteFile(excludes, []byte("global-generated.go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	globalConfig := filepath.Join(root, "gitconfig")
	if err := os.WriteFile(globalConfig, []byte("[core]\n\texcludesfile = "+excludes+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", globalConfig)
	if err := os.WriteFile(filepath.Join(repository, "global-generated.go"), []byte("package ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	head := provenanceRepositoryHead(t, repository)
	if _, err := resolveCandidateProvenance(repository, filepath.Join(root, "artifacts"), head); err == nil || !strings.Contains(err.Error(), "ignored_inputs=true") {
		t.Fatalf("global-excluded input error=%v, want explicit rejection", err)
	}
}

func TestCandidateProvenanceOverridesConfiguredIgnoredDirtySubmodule(t *testing.T) {
	root := t.TempDir()
	submoduleSource := initializeProvenanceRepository(t, filepath.Join(root, "submodule-source"))
	repository := initializeProvenanceRepository(t, filepath.Join(root, "repository"))
	provenanceGit(t, repository, "-c", "protocol.file.allow=always", "submodule", "add", submoduleSource, "modules/child")
	provenanceGit(t, repository, "config", "-f", ".gitmodules", "submodule.modules/child.ignore", "all")
	provenanceGit(t, repository, "add", ".gitmodules", "modules/child")
	provenanceGit(t, repository, "commit", "-m", "add ignored submodule")
	if err := os.WriteFile(filepath.Join(repository, "modules", "child", "tracked.txt"), []byte("dirty submodule\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := runGit(repository, "status", "--porcelain=v1"); err != nil || len(strings.TrimSpace(string(output))) != 0 {
		t.Fatalf("configured ignored submodule is unexpectedly visible without override: output=%q err=%v", output, err)
	}
	head := provenanceRepositoryHead(t, repository)
	if _, err := resolveCandidateProvenance(repository, filepath.Join(root, "artifacts"), head); err == nil || !strings.Contains(err.Error(), "status_dirty=true") {
		t.Fatalf("ignored dirty submodule error=%v, want explicit status rejection", err)
	}
}

func initializeProvenanceRepository(t *testing.T, repository string) string {
	t.Helper()
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	provenanceGit(t, repository, "init", "-q")
	provenanceGit(t, repository, "config", "user.name", "Cynapsa Qualification")
	provenanceGit(t, repository, "config", "user.email", "qualification@example.invalid")
	if err := os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provenanceGit(t, repository, "add", "tracked.txt")
	provenanceGit(t, repository, "commit", "-q", "-m", "initial")
	return repository
}

func provenanceRepositoryHead(t *testing.T, repository string) string {
	t.Helper()
	output, err := runGit(repository, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

func provenanceGit(t *testing.T, repository string, arguments ...string) {
	t.Helper()
	if _, err := runGit(repository, arguments...); err != nil {
		t.Fatal(err)
	}
}
