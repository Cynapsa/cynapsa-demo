package regression

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

type manifest struct {
	SchemaVersion int     `json:"schema_version"`
	Repository    string  `json:"repository"`
	Purpose       string  `json:"purpose"`
	Stages        []stage `json:"stages"`
	Issues        []issue `json:"issues"`
}

type stage struct {
	ID          string `json:"id"`
	Command     string `json:"command"`
	Description string `json:"description"`
}

type issue struct {
	IssueNumber int              `json:"issue_number"`
	IssueURL    string           `json:"issue_url"`
	Title       string           `json:"title"`
	Tests       []regressionTest `json:"tests"`
}

type regressionTest struct {
	TestName string `json:"test_name"`
	Package  string `json:"package"`
	Stage    string `json:"stage"`
	Command  string `json:"command"`
	Proof    string `json:"proof"`
}

func TestRegressionManifestContract(t *testing.T) {
	root := repositoryRoot(t)
	encoded, err := os.ReadFile(filepath.Join(root, "test", "regression", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = validateManifest(root, encoded); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionManifestRejectsNonExecutableEvidence(t *testing.T) {
	root := repositoryRoot(t)
	valid, err := os.ReadFile(filepath.Join(root, "test", "regression", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = validateManifest(root, append(append([]byte(nil), valid...), []byte("\n{}\n")...)); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}

	for _, test := range []struct {
		name   string
		mutate func(*manifest)
	}{
		{name: "helper no tests to run", mutate: func(value *manifest) {
			value.Issues[0].Tests[0] = regressionTest{
				TestName: "hasReport", Package: "./test/production/coturn", Stage: "remote-mesh",
				Command: "go test -v -count=1 ./test/production/coturn -run '^hasReport$'", Proof: "mutant",
			}
		}},
		{name: "echo prefix", mutate: func(value *manifest) {
			value.Issues[0].Tests[0].Command = "echo ok && " + value.Issues[0].Tests[0].Command
		}},
		{name: "shell operator", mutate: func(value *manifest) { value.Issues[0].Tests[0].Command += " || true" }},
		{name: "extra option", mutate: func(value *manifest) {
			value.Issues[0].Tests[0].Command = strings.Replace(value.Issues[0].Tests[0].Command, "go test ", "go test -shuffle=on ", 1)
		}},
		{name: "environment prefix", mutate: func(value *manifest) {
			value.Issues[0].Tests[0].Command = "IGNORED=1 " + value.Issues[0].Tests[0].Command
		}},
		{name: "stage chaining", mutate: func(value *manifest) { value.Stages[0].Command += " && echo accepted" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var value manifest
			if err := json.Unmarshal(valid, &value); err != nil {
				t.Fatal(err)
			}
			test.mutate(&value)
			mutant, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err = validateManifest(root, mutant); err == nil {
				t.Fatal("non-executable manifest evidence was accepted")
			}
		})
	}
}

func TestRunnableGoTestSignature(t *testing.T) {
	for _, test := range []struct {
		name, source string
		want         bool
	}{
		{name: "standard", source: `package p; import "testing"; func TestExact(t *testing.T) {}`, want: true},
		{name: "aliased", source: `package p; import qa "testing"; func TestExact(t *qa.T) {}`, want: true},
		{name: "dot import", source: `package p; import . "testing"; func TestExact(t *T) {}`, want: true},
		{name: "helper", source: `package p; func hasReport() {}`},
		{name: "lowercase suffix", source: `package p; import "testing"; func Testexact(t *testing.T) {}`},
		{name: "missing parameter", source: `package p; func TestExact() {}`},
		{name: "two parameters", source: `package p; import "testing"; func TestExact(a, b *testing.T) {}`},
		{name: "wrong type", source: `package p; type T struct{}; func TestExact(t *T) {}`},
		{name: "result", source: `package p; import "testing"; func TestExact(t *testing.T) error { return nil }`},
		{name: "type parameter", source: `package p; import "testing"; func TestExact[V any](t *testing.T) {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "mutant_test.go", test.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			var got bool
			for _, declaration := range file.Decls {
				if function, ok := declaration.(*ast.FuncDecl); ok {
					got = runnableTestDeclaration(file, function)
				}
			}
			if got != test.want {
				t.Fatalf("runnableTestDeclaration()=%t, want %t", got, test.want)
			}
		})
	}
}

func validateManifest(root string, encoded []byte) error {
	var value manifest
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode manifest: trailing JSON value")
		}
		return fmt.Errorf("decode manifest trailing content: %w", err)
	}
	if value.SchemaVersion != 1 || value.Repository != "Cynapsa/cynapsagocore" || strings.TrimSpace(value.Purpose) == "" {
		return fmt.Errorf("invalid manifest header: %#v", value)
	}
	wantedStages := map[string]bool{"manifest": false, "fast": false, "race": false, "ejabberd": false, "remote-mesh": false}
	for _, value := range value.Stages {
		if _, exists := wantedStages[value.ID]; !exists || wantedStages[value.ID] {
			return fmt.Errorf("unknown or duplicate stage %q", value.ID)
		}
		wantedStages[value.ID] = true
		if value.Command != "test/regression/run.sh "+value.ID || strings.TrimSpace(value.Description) == "" {
			return fmt.Errorf("stage %q has noncanonical command or empty description", value.ID)
		}
	}
	for id, seen := range wantedStages {
		if !seen {
			return fmt.Errorf("required stage %q is absent", id)
		}
	}

	seen := make(map[int]bool)
	for _, issue := range value.Issues {
		if issue.IssueNumber < 2 || issue.IssueNumber > 13 || seen[issue.IssueNumber] {
			return fmt.Errorf("invalid or duplicate issue number %d", issue.IssueNumber)
		}
		seen[issue.IssueNumber] = true
		wantURL := fmt.Sprintf("https://github.com/Cynapsa/cynapsagocore/issues/%d", issue.IssueNumber)
		if issue.IssueURL != wantURL || strings.TrimSpace(issue.Title) == "" || len(issue.Tests) == 0 {
			return fmt.Errorf("issue %d has incomplete traceability", issue.IssueNumber)
		}
		for _, regression := range issue.Tests {
			if !wantedStages[regression.Stage] || strings.TrimSpace(regression.Proof) == "" {
				return fmt.Errorf("issue %d test %q has an invalid stage or proof", issue.IssueNumber, regression.TestName)
			}
			canonical, canonicalErr := canonicalRegressionCommand(regression)
			if canonicalErr != nil {
				return fmt.Errorf("issue %d test %q: %w", issue.IssueNumber, regression.TestName, canonicalErr)
			}
			if regression.Command != canonical {
				return fmt.Errorf("issue %d command is noncanonical: got %q want %q", issue.IssueNumber, regression.Command, canonical)
			}
			if testErr := runnableTestExists(root, regression.Package, regression.TestName); testErr != nil {
				return fmt.Errorf("issue %d: %w", issue.IssueNumber, testErr)
			}
		}
	}
	for number := 2; number <= 13; number++ {
		if !seen[number] {
			return fmt.Errorf("GitHub issue #%d is absent", number)
		}
	}
	return nil
}

func TestRegressionWorkflowRouting(t *testing.T) {
	root := repositoryRoot(t)
	ci := readFile(t, filepath.Join(root, ".github", "workflows", "ci.yml"))
	qualification := readFile(t, filepath.Join(root, ".github", "workflows", "qualification.yml"))
	if strings.Count(ci, "test/regression/run.sh manifest") != 1 {
		t.Fatal("hosted CI must run the manifest contract exactly once without replacing complete jobs")
	}
	smoke := "go test -v -count=1 ./test/production/coturn -run '^TestRemoteMeshSmoke$'"
	full := "go test -v -count=1 -timeout=30m ./test/production/coturn"
	smokeAt, fullAt := strings.Index(qualification, smoke), strings.LastIndex(qualification, full)
	if smokeAt < 0 || fullAt < 0 || smokeAt >= fullAt {
		t.Fatal("trusted qualification must run focused remote smoke before the separate full Coturn matrix")
	}
	for _, required := range []string{"cron: '17 2 * * 0'", "(github.event_name == 'schedule' && github.ref == 'refs/heads/main')", "environment: docker-qualification", "runs-on: [self-hosted, linux, x64, cynapsa-docker-qualification]"} {
		if !strings.Contains(qualification, required) {
			t.Errorf("trusted qualification missing %q", required)
		}
	}
	if strings.Contains(qualification, "pull_request:") || strings.Contains(qualification, "pull_request_target:") {
		t.Fatal("privileged qualification exposes an untrusted pull-request trigger")
	}
}

func TestManifestStageRunsEntireContractPackage(t *testing.T) {
	runner := readFile(t, filepath.Join(repositoryRoot(t), "test", "regression", "run.sh"))
	const completeCommand = "go test -count=1 ./test/regression"
	if strings.Count(runner, completeCommand) != 1 {
		t.Fatalf("manifest stage must run the complete contract package exactly once")
	}
	manifestFunction := runner
	if start := strings.Index(runner, "run_manifest() {"); start >= 0 {
		manifestFunction = runner[start:]
		if end := strings.Index(manifestFunction, "\n}\n"); end >= 0 {
			manifestFunction = manifestFunction[:end]
		}
	}
	if strings.Contains(manifestFunction, "-run") {
		t.Fatal("manifest stage uses a selector and can omit contract tests or silently match zero tests")
	}
}

func TestRemoteSmokeUsesCurrentAuthorityAndBoundedReadiness(t *testing.T) {
	root := repositoryRoot(t)
	environment := readFile(t, filepath.Join(root, "test", "production", "coturn", "environment_test.go"))
	runner := readFile(t, filepath.Join(root, "test", "production", "coturn", "run.sh"))
	regressionRunner := readFile(t, filepath.Join(root, "test", "regression", "run.sh"))
	gateway := readFile(t, filepath.Join(root, "test", "production", "coturn", "nat_gateway.sh"))
	for _, required := range []string{"startCoturnEnvironment(t)", "productionlock.Acquire", "CYNAPSA_REMOTE_SMOKE", "fault-xmpp", "assertXMPPFaultEvidence", "assertAgentConfigurationIsAuthorityOnly"} {
		if !strings.Contains(environment, required) {
			t.Errorf("remote smoke harness missing %q", required)
		}
	}
	for _, forbidden := range []string{"connectivity.json", "CYNAPSA_PRIVATE_CONNECTIVITY_PROFILE"} {
		if containsFunctionStringLiteral(t, filepath.Join(root, "test", "production", "coturn", "environment_test.go"), "startAgent", forbidden) {
			t.Errorf("agent launch retains forbidden input %q", forbidden)
		}
	}
	for _, required := range []string{"lifetime=%ss", "authenticated XEP-0215 credential lifetime is not short-lived", "wait_container", "state_report"} {
		if !strings.Contains(runner, required) {
			t.Errorf("service readiness missing %q", required)
		}
	}
	if strings.Contains(regressionRunner, "ejabberd/run.sh run") {
		t.Fatal("self-contained ejabberd regressions must not be nested in another server harness")
	}
	for _, required := range []string{"CYNAPSA_NAT_XMPP_ADDRESS", "--dport 5222 -j DROP", "fault-xmpp", "restore-xmpp"} {
		if !strings.Contains(gateway, required) {
			t.Errorf("gateway XMPP fence missing %q", required)
		}
	}
}

func canonicalRegressionCommand(regression regressionTest) (string, error) {
	if !strings.HasPrefix(regression.Package, "./") || strings.ContainsAny(regression.Package, " \t\r\n'\";$&|()<>\\") {
		return "", fmt.Errorf("invalid package %q", regression.Package)
	}
	relativePackage := strings.TrimPrefix(regression.Package, "./")
	if relativePackage == "" || filepath.Clean(relativePackage) != relativePackage || relativePackage == ".." || strings.HasPrefix(relativePackage, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("nonlocal package %q", regression.Package)
	}
	if !runnableTestName(regression.TestName) {
		return "", fmt.Errorf("invalid Go test name %q", regression.TestName)
	}
	selector := "-run '^" + regression.TestName + "$'"
	switch regression.Stage {
	case "fast":
		return "go test -count=1 " + regression.Package + " " + selector, nil
	case "race":
		return "go test -race -count=1 " + regression.Package + " " + selector, nil
	case "ejabberd":
		return "(umask 077; go test -count=1 " + regression.Package + " " + selector + ")", nil
	case "remote-mesh":
		return "go test -v -count=1 " + regression.Package + " " + selector, nil
	default:
		return "", fmt.Errorf("invalid executable stage %q", regression.Stage)
	}
}

func runnableTestExists(root, packagePath, name string) error {
	if !runnableTestName(name) {
		return fmt.Errorf("%s is not a runnable Go test name", name)
	}
	directory := filepath.Join(root, strings.TrimPrefix(packagePath, "./"))
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read %s for %s: %w", packagePath, name, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		matched, matchErr := build.Default.MatchFile(directory, entry.Name())
		if matchErr != nil {
			return fmt.Errorf("match %s/%s: %w", packagePath, entry.Name(), matchErr)
		}
		if !matched {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, entry.Name()), nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s/%s: %w", packagePath, entry.Name(), parseErr)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Name.Name == name && runnableTestDeclaration(file, function) {
				return nil
			}
		}
	}
	return fmt.Errorf("%s does not contain runnable test %s", packagePath, name)
}

func runnableTestName(name string) bool {
	if !strings.HasPrefix(name, "Test") || len(name) == len("Test") {
		return false
	}
	next, _ := utf8.DecodeRuneInString(strings.TrimPrefix(name, "Test"))
	return next != utf8.RuneError && !unicode.IsLower(next)
}

func runnableTestDeclaration(file *ast.File, function *ast.FuncDecl) bool {
	if file == nil || function == nil || function.Recv != nil || !runnableTestName(function.Name.Name) || function.Type == nil || function.Type.Params == nil || len(function.Type.Params.List) != 1 || function.Type.Results != nil && len(function.Type.Results.List) != 0 || function.Type.TypeParams != nil && len(function.Type.TypeParams.List) != 0 {
		return false
	}
	parameter := function.Type.Params.List[0]
	if len(parameter.Names) > 1 {
		return false
	}
	pointer, ok := parameter.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	testingAliases := make(map[string]bool)
	dotTesting := false
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil || path != "testing" {
			continue
		}
		alias := "testing"
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		switch alias {
		case ".":
			dotTesting = true
		case "_":
		default:
			testingAliases[alias] = true
		}
	}
	switch value := pointer.X.(type) {
	case *ast.SelectorExpr:
		alias, ok := value.X.(*ast.Ident)
		return ok && testingAliases[alias.Name] && value.Sel.Name == "T"
	case *ast.Ident:
		return dotTesting && value.Name == "T"
	default:
		return false
	}
}

func containsFunctionStringLiteral(t *testing.T, path, functionName, substring string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName {
			continue
		}
		found := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr == nil && strings.Contains(value, substring) {
				found = true
			}
			return !found
		})
		return found
	}
	t.Fatalf("function %s not found in %s", functionName, path)
	return false
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(directory, "..", ".."))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
