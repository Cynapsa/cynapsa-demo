package doccontract_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPrivilegedQualificationCannotRunPullRequestCode(t *testing.T) {
	repository := repositoryRoot(t)
	fast := readContractFile(t, filepath.Join(repository, ".github", "workflows", "ci.yml"))
	trusted := readContractFile(t, filepath.Join(repository, ".github", "workflows", "qualification.yml"))
	documentation := readContractFile(t, filepath.Join(repository, "docs", "development", "CI.md"))

	if !strings.Contains(fast, "pull_request:") {
		t.Fatal("hosted fast workflow no longer validates pull requests")
	}
	for _, forbidden := range []string{"self-hosted", "cynapsa-docker-qualification", "docker info"} {
		if strings.Contains(fast, forbidden) {
			t.Errorf("pull-request workflow contains privileged-runner marker %q", forbidden)
		}
	}
	assertSelfHostedWorkflowsRejectPullRequestTriggers(t, repository)
	assertUntrustedWorkflowsCannotReachSelfHosted(t, repository)
	for _, required := range []string{
		"branches:\n      - main",
		"workflow_dispatch:",
		"github.event_name == 'push' && github.ref == 'refs/heads/main'",
		"github.repository == 'Cynapsa/cynapsagocore'",
		"runs-on: [self-hosted, linux, x64, cynapsa-docker-qualification]",
		"environment: docker-qualification",
		"ref: ${{ github.sha }}",
	} {
		if !strings.Contains(trusted, required) {
			t.Errorf("trusted qualification workflow is missing guard %q", required)
		}
	}
	for _, required := range []string{"protected `main`", "required reviewers", "ephemeral", "never a pull-request head"} {
		if !strings.Contains(documentation, required) {
			t.Errorf("CI runner contract is missing %q", required)
		}
	}
}

type workflowSecurityDefinition struct {
	triggers      []*yaml.Node
	selfHosted    bool
	workflowCall  bool
	localCalls    []string
	nonlocalCalls []string
	dynamicCall   bool
	dynamicRunner bool
}

func assertUntrustedWorkflowsCannotReachSelfHosted(t *testing.T, repository string) {
	t.Helper()
	workflowDirectory := filepath.Join(repository, ".github", "workflows")
	entries, err := os.ReadDir(workflowDirectory)
	if err != nil {
		t.Fatalf("read workflow directory: %v", err)
	}
	sources := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" && filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		name := filepath.ToSlash(filepath.Join(".github", "workflows", entry.Name()))
		sources[name] = readContractFile(t, filepath.Join(workflowDirectory, entry.Name()))
	}
	for _, violation := range workflowReachabilityViolations(sources) {
		t.Error(violation)
	}
}

func workflowReachabilityViolations(sources map[string]string) []string {
	definitions := make(map[string]workflowSecurityDefinition, len(sources))
	var violations []string
	for name, source := range sources {
		definition, err := parseWorkflowSecurity(source)
		if err != nil {
			violations = append(violations, fmt.Sprintf("workflow %s cannot be parsed safely: %v", name, err))
			continue
		}
		definitions[filepath.ToSlash(filepath.Clean(name))] = definition
	}
	var roots []string
	for name, definition := range definitions {
		if workflowHasTrigger(definition, "pull_request") || workflowHasTrigger(definition, "pull_request_target") {
			roots = append(roots, name)
		}
	}
	sort.Strings(roots)
	for _, root := range roots {
		states := make(map[string]uint8)
		var visit func(string, []string)
		visit = func(name string, path []string) {
			switch states[name] {
			case 1:
				violations = append(violations, fmt.Sprintf("untrusted workflow call cycle: %s", strings.Join(append(path, name), " -> ")))
				return
			case 2:
				return
			}
			definition, exists := definitions[name]
			if !exists {
				violations = append(violations, fmt.Sprintf("untrusted workflow path %s references missing local workflow %s", strings.Join(path, " -> "), name))
				return
			}
			states[name] = 1
			path = append(path, name)
			if definition.selfHosted {
				violations = append(violations, fmt.Sprintf("untrusted workflow reaches self-hosted job: %s", strings.Join(path, " -> ")))
			}
			if definition.dynamicCall {
				violations = append(violations, fmt.Sprintf("untrusted workflow path contains dynamic reusable-workflow target: %s", strings.Join(path, " -> ")))
			}
			for _, target := range definition.nonlocalCalls {
				violations = append(violations, fmt.Sprintf("untrusted workflow path contains uninspectable nonlocal reusable-workflow target %q: %s", target, strings.Join(path, " -> ")))
			}
			if definition.dynamicRunner {
				violations = append(violations, fmt.Sprintf("untrusted workflow path contains dynamic runner selection: %s", strings.Join(path, " -> ")))
			}
			for _, targetValue := range definition.localCalls {
				target, err := normalizeLocalWorkflowTarget(targetValue)
				if err != nil {
					violations = append(violations, fmt.Sprintf("untrusted workflow %s has unsafe local target %q", name, targetValue))
					continue
				}
				called, exists := definitions[target]
				if !exists {
					violations = append(violations, fmt.Sprintf("untrusted workflow path %s references missing local workflow %s", strings.Join(path, " -> "), target))
					continue
				}
				if !called.workflowCall {
					violations = append(violations, fmt.Sprintf("untrusted workflow %s calls %s without workflow_call", name, target))
					continue
				}
				visit(target, path)
			}
			states[name] = 2
		}
		visit(root, nil)
	}
	sort.Strings(violations)
	return violations
}

func parseWorkflowSecurity(source string) (workflowSecurityDefinition, error) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		return workflowSecurityDefinition{}, err
	}
	root := yamlDocumentRoot(&document)
	if root == nil || root.Kind != yaml.MappingNode {
		return workflowSecurityDefinition{}, fmt.Errorf("root is not a mapping")
	}
	definition := workflowSecurityDefinition{triggers: yamlMappingValues(root, "on")}
	for _, triggers := range definition.triggers {
		if yamlTriggerContains(triggers, "workflow_call") {
			definition.workflowCall = true
		}
	}
	for _, jobs := range yamlMappingValues(root, "jobs") {
		if jobs.Kind != yaml.MappingNode {
			return workflowSecurityDefinition{}, fmt.Errorf("jobs is not a mapping")
		}
		for index := 0; index+1 < len(jobs.Content); index += 2 {
			job := jobs.Content[index+1]
			if job.Kind != yaml.MappingNode {
				return workflowSecurityDefinition{}, fmt.Errorf("job %q is not a mapping", jobs.Content[index].Value)
			}
			for _, runsOn := range yamlMappingValues(job, "runs-on") {
				if yamlRunnerIsSelfHosted(runsOn) {
					definition.selfHosted = true
				}
				if yamlContainsExpression(runsOn) {
					definition.dynamicRunner = true
				}
			}
			for _, uses := range yamlMappingValues(job, "uses") {
				if uses.Kind != yaml.ScalarNode || strings.Contains(uses.Value, "${{") {
					definition.dynamicCall = true
					continue
				}
				if strings.HasPrefix(strings.TrimSpace(uses.Value), "./") {
					definition.localCalls = append(definition.localCalls, strings.TrimSpace(uses.Value))
				} else {
					definition.nonlocalCalls = append(definition.nonlocalCalls, strings.TrimSpace(uses.Value))
				}
			}
		}
	}
	return definition, nil
}

func workflowHasTrigger(definition workflowSecurityDefinition, trigger string) bool {
	for _, triggers := range definition.triggers {
		if yamlTriggerContains(triggers, trigger) {
			return true
		}
	}
	return false
}

func normalizeLocalWorkflowTarget(value string) (string, error) {
	if !strings.HasPrefix(value, "./") || strings.Contains(value, "@") || strings.Contains(value, "${{") {
		return "", fmt.Errorf("dynamic or non-local target")
	}
	target := filepath.ToSlash(filepath.Clean(strings.TrimPrefix(value, "./")))
	if !strings.HasPrefix(target, ".github/workflows/") || target == ".github/workflows" || filepath.Ext(target) != ".yml" && filepath.Ext(target) != ".yaml" {
		return "", fmt.Errorf("target escapes workflow directory")
	}
	return target, nil
}

func assertSelfHostedWorkflowsRejectPullRequestTriggers(t *testing.T, repository string) {
	t.Helper()
	workflowDirectory := filepath.Join(repository, ".github", "workflows")
	entries, err := os.ReadDir(workflowDirectory)
	if err != nil {
		t.Fatalf("read workflow directory: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" && filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		path := filepath.Join(workflowDirectory, entry.Name())
		source := readContractFile(t, path)
		forbidden, selfHosted, err := forbiddenPullRequestTrigger(source)
		if err != nil {
			t.Errorf("parse workflow %s: %v", entry.Name(), err)
			continue
		}
		if !selfHosted {
			continue
		}
		if forbidden != "" {
			t.Errorf("self-hosted workflow %s contains forbidden semantic trigger %q", entry.Name(), forbidden)
		}
	}
}

func TestSemanticWorkflowTriggerMutants(t *testing.T) {
	for name, source := range map[string]string{
		"mapping whitespace":  "on:\n  pull_request :\njobs:\n  qualification:\n    runs-on: [self-hosted]\n",
		"mapping target":      "on: {workflow_dispatch: {}, pull_request_target: {}}\njobs:\n  qualification:\n    runs-on: self-hosted\n",
		"sequence":            "on: [workflow_dispatch, pull_request]\njobs:\n  qualification:\n    runs-on: self-hosted\n",
		"scalar":              "on: pull_request_target\njobs:\n  qualification:\n    runs-on: self-hosted\n",
		"duplicate mapping":   "on: push\non:\n  pull_request: {}\njobs:\n  qualification:\n    runs-on: self-hosted\n",
		"uppercase runner":    "on: pull_request\njobs:\n  qualification:\n    runs-on: SELF-HOSTED\n",
		"mixed-case runner":   "on: pull_request_target\njobs:\n  qualification:\n    runs-on: [SeLf-HoStEd, linux]\n",
		"runner group":        "on: pull_request\njobs:\n  qualification:\n    runs-on: {group: privileged-runners}\n",
		"runner group labels": "on: pull_request_target\njobs:\n  qualification:\n    runs-on:\n      group: privileged-runners\n      labels: [linux, x64]\n",
	} {
		t.Run(name, func(t *testing.T) {
			forbidden, selfHosted, err := forbiddenPullRequestTrigger(source)
			if err != nil {
				t.Fatalf("parse mutant: %v", err)
			}
			if !selfHosted || forbidden == "" {
				t.Fatalf("mutant was accepted: self-hosted=%v forbidden=%q", selfHosted, forbidden)
			}
		})
	}

	const safe = "on: [push, workflow_dispatch]\njobs:\n  qualification:\n    runs-on: [self-hosted, linux]\n"
	forbidden, selfHosted, err := forbiddenPullRequestTrigger(safe)
	if err != nil || !selfHosted || forbidden != "" {
		t.Fatalf("trusted triggers rejected: self-hosted=%v forbidden=%q err=%v", selfHosted, forbidden, err)
	}
}

func TestUntrustedReusableWorkflowReachabilityMutants(t *testing.T) {
	const pullRequest = "on: pull_request\njobs:\n  call:\n    uses: ./.github/workflows/reusable.yml\n"
	const pullRequestTarget = "on: pull_request_target\njobs:\n  call:\n    uses: ./.github/workflows/middle.yml\n"
	const reusableSelfHosted = "on: workflow_call\njobs:\n  qualification:\n    runs-on: [self-hosted, linux]\n    steps:\n      - run: true\n"
	for name, sources := range map[string]map[string]string{
		"direct reusable": {
			".github/workflows/ci.yml":       pullRequest,
			".github/workflows/reusable.yml": reusableSelfHosted,
		},
		"nested reusable": {
			".github/workflows/ci.yml":       pullRequestTarget,
			".github/workflows/middle.yml":   "on: workflow_call\njobs:\n  next:\n    uses: ./.github/workflows/reusable.yml\n",
			".github/workflows/reusable.yml": reusableSelfHosted,
		},
		"cycle": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  call:\n    uses: ./.github/workflows/a.yml\n",
			".github/workflows/a.yml":  "on: workflow_call\njobs:\n  call:\n    uses: ./.github/workflows/b.yml\n",
			".github/workflows/b.yml":  "on: workflow_call\njobs:\n  call:\n    uses: ./.github/workflows/a.yml\n",
		},
		"missing": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  call:\n    uses: ./.github/workflows/missing.yml\n",
		},
		"dynamic": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  call:\n    uses: ${{ inputs.workflow }}\n",
		},
		"dynamic runner": {
			".github/workflows/ci.yml":       pullRequest,
			".github/workflows/reusable.yml": "on: workflow_call\njobs:\n  qualification:\n    runs-on: ${{ inputs.runner }}\n    steps:\n      - run: true\n",
		},
		"uppercase self hosted": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  qualification:\n    runs-on: [SELF-HOSTED, linux]\n    steps:\n      - run: true\n",
		},
		"mixed case nested self hosted": {
			".github/workflows/ci.yml":       pullRequest,
			".github/workflows/reusable.yml": "on: workflow_call\njobs:\n  qualification:\n    runs-on: [SeLf-HoStEd, linux]\n    steps:\n      - run: true\n",
		},
		"mapping runner group": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  qualification:\n    runs-on: {group: privileged-runners}\n    steps:\n      - run: true\n",
		},
		"external reusable": {
			".github/workflows/ci.yml": "on: pull_request\njobs:\n  call:\n    uses: owner/repository/.github/workflows/qualification.yml@v1\n",
		},
		"nested external reusable": {
			".github/workflows/ci.yml":       pullRequest,
			".github/workflows/reusable.yml": "on: workflow_call\njobs:\n  call:\n    uses: Cynapsa/shared/.github/workflows/qualification.yml@main\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if violations := workflowReachabilityViolations(sources); len(violations) == 0 {
				t.Fatal("untrusted reusable-workflow mutant was accepted")
			}
		})
	}

	safe := map[string]string{
		".github/workflows/ci.yml":            "on: pull_request\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n",
		".github/workflows/qualification.yml": "on: [push, workflow_dispatch]\njobs:\n  qualification:\n    runs-on: [self-hosted, linux]\n    steps:\n      - run: true\n  external:\n    uses: owner/repository/.github/workflows/qualification.yml@v1\n",
	}
	if violations := workflowReachabilityViolations(safe); len(violations) != 0 {
		t.Fatalf("trusted workflow separation rejected: %v", violations)
	}
}

func forbiddenPullRequestTrigger(source string) (forbidden string, selfHosted bool, err error) {
	definition, err := parseWorkflowSecurity(source)
	if err != nil {
		return "", false, err
	}
	selfHosted = definition.selfHosted
	if !selfHosted {
		return "", false, nil
	}
	triggerNodes := definition.triggers
	if len(triggerNodes) == 0 {
		return "missing on", true, nil
	}
	for _, trigger := range []string{"pull_request", "pull_request_target"} {
		for _, triggers := range triggerNodes {
			if yamlTriggerContains(triggers, trigger) {
				return trigger, true, nil
			}
		}
	}
	return "", true, nil
}

// GitHub's object-form runs-on syntax selects a self-hosted runner group even
// when the literal label "self-hosted" is absent. Treat every mapping form as
// self-hosted and fail closed on expressions separately in the reachability
// analysis.
func yamlRunnerIsSelfHosted(node *yaml.Node) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.AliasNode {
		return yamlRunnerIsSelfHosted(node.Alias)
	}
	return node.Kind == yaml.MappingNode || yamlContainsScalar(node, "self-hosted")
}

func yamlDocumentRoot(document *yaml.Node) *yaml.Node {
	if document != nil && document.Kind == yaml.DocumentNode && len(document.Content) == 1 {
		return document.Content[0]
	}
	return document
}

func yamlMappingValues(mapping *yaml.Node, key string) []*yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	var values []*yaml.Node
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			values = append(values, mapping.Content[index+1])
		}
	}
	return values
}

func yamlTriggerContains(node *yaml.Node, trigger string) bool {
	return yamlTriggerContainsSeen(node, trigger, make(map[*yaml.Node]bool))
}

func yamlTriggerContainsSeen(node *yaml.Node, trigger string, seen map[*yaml.Node]bool) bool {
	if node == nil {
		return false
	}
	if seen[node] {
		return false
	}
	seen[node] = true
	if node.Kind == yaml.AliasNode {
		return yamlTriggerContainsSeen(node.Alias, trigger, seen)
	}
	switch node.Kind {
	case yaml.ScalarNode:
		return node.Value == trigger
	case yaml.SequenceNode:
		for _, child := range node.Content {
			if yamlTriggerContainsSeen(child, trigger, seen) {
				return true
			}
		}
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Value == trigger {
				return true
			}
		}
	}
	return false
}

func yamlContainsScalar(node *yaml.Node, value string) bool {
	return yamlContainsScalarSeen(node, value, make(map[*yaml.Node]bool))
}

func yamlContainsExpression(node *yaml.Node) bool {
	return yamlContainsExpressionSeen(node, make(map[*yaml.Node]bool))
}

func yamlContainsExpressionSeen(node *yaml.Node, seen map[*yaml.Node]bool) bool {
	if node == nil || seen[node] {
		return false
	}
	seen[node] = true
	if node.Kind == yaml.AliasNode {
		return yamlContainsExpressionSeen(node.Alias, seen)
	}
	if node.Kind == yaml.ScalarNode && strings.Contains(node.Value, "${{") {
		return true
	}
	for _, child := range node.Content {
		if yamlContainsExpressionSeen(child, seen) {
			return true
		}
	}
	return false
}

func yamlContainsScalarSeen(node *yaml.Node, value string, seen map[*yaml.Node]bool) bool {
	if node == nil {
		return false
	}
	if seen[node] {
		return false
	}
	seen[node] = true
	if node.Kind == yaml.AliasNode {
		return yamlContainsScalarSeen(node.Alias, value, seen)
	}
	if node.Kind == yaml.ScalarNode && strings.EqualFold(node.Value, value) {
		return true
	}
	for _, child := range node.Content {
		if yamlContainsScalarSeen(child, value, seen) {
			return true
		}
	}
	return false
}

func TestFastAndQualificationPackageSetsStaySeparated(t *testing.T) {
	repository := repositoryRoot(t)
	fast := readContractFile(t, filepath.Join(repository, ".github", "workflows", "ci.yml"))
	trusted := readContractFile(t, filepath.Join(repository, ".github", "workflows", "qualification.yml"))

	const packageFilter = "grep -Ev '(/integration|/test/e2e|/test/production/(coturn|faults|release))$'"
	if count := strings.Count(fast, packageFilter); count != 2 {
		t.Fatalf("hosted workflow uses non-Docker package derivation %d times, want normal and race", count)
	}
	if strings.Contains(fast, "go test -count=1 ./integration") || strings.Contains(fast, "go test -race -count=1 ./integration") {
		t.Fatal("hosted workflow executes Docker-backed integration package")
	}
	if !strings.Contains(fast, `go test -race -count=1 "${packages[@]}"`) {
		t.Fatal("race job is not derived from the complete non-Docker package list")
	}
	for _, command := range []string{
		"go test -v -count=1 ./integration",
		"go test -v -count=1 -timeout=20m ./test/e2e",
		"go test -v -count=1 ./test/production/faults",
		"go test -v -count=1 -timeout=30m ./test/production/coturn",
		"go test -v -count=1 ./test/production/release",
	} {
		executes, err := workflowExecutesExactCommand(trusted, command)
		if err != nil {
			t.Fatalf("parse trusted workflow commands: %v", err)
		}
		if !executes {
			t.Errorf("trusted serialized qualification is missing %q", command)
		}
	}
}

func TestQualificationArtifactsStayOutsideTestedWorktree(t *testing.T) {
	repository := repositoryRoot(t)
	trusted := readContractFile(t, filepath.Join(repository, ".github", "workflows", "qualification.yml"))
	valid, err := qualificationArtifactConfigurationIsExternal(trusted)
	if err != nil || !valid {
		t.Fatalf("qualification artifact configuration is not externally rooted: valid=%t err=%v", valid, err)
	}

	const external = "${{ runner.temp }}/cynapsa-qualification/${{ github.run_id }}-${{ github.run_attempt }}/e2e"
	const viaEnvironment = "${{ env.CYNAPSA_E2E_ARTIFACT_DIR }}"
	for name, mutant := range map[string]string{
		"workspace environment": qualificationArtifactWorkflowMutant("${{ github.workspace }}/qualification-artifacts/e2e", viaEnvironment),
		"relative environment":  qualificationArtifactWorkflowMutant("qualification-artifacts/e2e", viaEnvironment),
		"tracked upload path":   qualificationArtifactWorkflowMutant(external, "qualification-artifacts/e2e"),
		"workspace upload path": qualificationArtifactWorkflowMutant(external, "${{ github.workspace }}/qualification-artifacts/e2e"),
	} {
		t.Run(name, func(t *testing.T) {
			valid, err := qualificationArtifactConfigurationIsExternal(mutant)
			if err != nil {
				t.Fatal(err)
			}
			if valid {
				t.Fatal("tracked-worktree artifact mutant was accepted")
			}
		})
	}
}

func qualificationArtifactWorkflowMutant(environmentPath, uploadPath string) string {
	return fmt.Sprintf("env:\n  CYNAPSA_E2E_ARTIFACT_DIR: %q\njobs:\n  qualification:\n    runs-on: self-hosted\n    steps:\n      - uses: actions/upload-artifact@pinned\n        with:\n          path: %q\n", environmentPath, uploadPath)
}

func qualificationArtifactConfigurationIsExternal(source string) (bool, error) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		return false, err
	}
	root := yamlDocumentRoot(&document)
	if root == nil || root.Kind != yaml.MappingNode {
		return false, fmt.Errorf("workflow root is not a mapping")
	}
	const external = "${{ runner.temp }}/cynapsa-qualification/${{ github.run_id }}-${{ github.run_attempt }}/e2e"
	environments := yamlMappingValues(root, "env")
	if len(environments) != 1 {
		return false, nil
	}
	artifactValues := yamlMappingValues(environments[0], "CYNAPSA_E2E_ARTIFACT_DIR")
	if len(artifactValues) != 1 || artifactValues[0].Kind != yaml.ScalarNode || artifactValues[0].Value != external {
		return false, nil
	}
	jobs := yamlMappingValues(root, "jobs")
	if len(jobs) != 1 || jobs[0].Kind != yaml.MappingNode {
		return false, nil
	}
	for index := 0; index+1 < len(jobs[0].Content); index += 2 {
		job := jobs[0].Content[index+1]
		steps := yamlMappingValues(job, "steps")
		if len(steps) != 1 || steps[0].Kind != yaml.SequenceNode {
			continue
		}
		for _, step := range steps[0].Content {
			uses := yamlMappingValues(step, "uses")
			if len(uses) != 1 || uses[0].Kind != yaml.ScalarNode || !strings.HasPrefix(uses[0].Value, "actions/upload-artifact@") {
				continue
			}
			with := yamlMappingValues(step, "with")
			if len(with) != 1 {
				return false, nil
			}
			paths := yamlMappingValues(with[0], "path")
			return len(paths) == 1 && paths[0].Kind == yaml.ScalarNode && paths[0].Value == "${{ env.CYNAPSA_E2E_ARTIFACT_DIR }}", nil
		}
	}
	return false, nil
}

func TestQualificationCommandsMustBeExecutableMutants(t *testing.T) {
	const command = "go test -v -count=1 -timeout=20m ./test/e2e"
	for name, run := range map[string]string{
		"inline comment":  "true # " + command,
		"comment line":    "# " + command,
		"echoed text":     "echo '" + command + "'",
		"substring":       command + "-disabled",
		"false branch":    "if false; then\n  " + command + "\nfi",
		"here document":   "cat <<'EOF'\n" + command + "\nEOF",
		"unused function": "unused() {\n  " + command + "\n}",
	} {
		t.Run(name, func(t *testing.T) {
			source := "on: push\njobs:\n  qualification:\n    runs-on: ubuntu-latest\n    steps:\n      - run: " + fmt.Sprintf("%q", run) + "\n"
			executes, err := workflowExecutesExactCommand(source, command)
			if err != nil {
				t.Fatalf("parse mutant: %v", err)
			}
			if executes {
				t.Fatal("non-executable qualification command was accepted")
			}
		})
	}

	safe := "on: push\njobs:\n  qualification:\n    runs-on: ubuntu-latest\n    steps:\n      - run: " + command + "\n"
	executes, err := workflowExecutesExactCommand(safe, command)
	if err != nil || !executes {
		t.Fatalf("executable command rejected: executes=%t err=%v", executes, err)
	}
}

func workflowExecutesExactCommand(source, command string) (bool, error) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		return false, err
	}
	root := yamlDocumentRoot(&document)
	if root == nil || root.Kind != yaml.MappingNode {
		return false, fmt.Errorf("root is not a mapping")
	}
	for _, jobs := range yamlMappingValues(root, "jobs") {
		if jobs.Kind != yaml.MappingNode {
			return false, fmt.Errorf("jobs is not a mapping")
		}
		for index := 0; index+1 < len(jobs.Content); index += 2 {
			job := jobs.Content[index+1]
			if job.Kind != yaml.MappingNode {
				return false, fmt.Errorf("job %q is not a mapping", jobs.Content[index].Value)
			}
			for _, steps := range yamlMappingValues(job, "steps") {
				if steps.Kind != yaml.SequenceNode {
					return false, fmt.Errorf("job %q steps is not a sequence", jobs.Content[index].Value)
				}
				for _, step := range steps.Content {
					if step.Kind != yaml.MappingNode {
						return false, fmt.Errorf("job %q has a non-mapping step", jobs.Content[index].Value)
					}
					for _, run := range yamlMappingValues(step, "run") {
						if run.Kind != yaml.ScalarNode {
							return false, fmt.Errorf("job %q has a non-scalar run command", jobs.Content[index].Value)
						}
						if exactShellRunCommand(run.Value) == command {
							return true, nil
						}
					}
				}
			}
		}
	}
	return false, nil
}

func exactShellRunCommand(run string) string {
	lines := strings.Split(run, "\n")
	for index := range lines {
		lines[index] = stripShellComment(lines[index])
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func stripShellComment(line string) string {
	var singleQuoted, doubleQuoted, escaped bool
	for index, value := range line {
		if escaped {
			escaped = false
			continue
		}
		if value == '\\' && !singleQuoted {
			escaped = true
			continue
		}
		switch value {
		case '\'':
			if !doubleQuoted {
				singleQuoted = !singleQuoted
			}
		case '"':
			if !singleQuoted {
				doubleQuoted = !doubleQuoted
			}
		case '#':
			if !singleQuoted && !doubleQuoted && (index == 0 || line[index-1] == ' ' || line[index-1] == '\t') {
				return line[:index]
			}
		}
	}
	return line
}

func readContractFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
