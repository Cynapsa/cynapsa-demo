package sdkboundary

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

func TestSDKVisibleSourcesUsePublicVocabulary(t *testing.T) {
	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
	paths := []string{
		filepath.Join(repositoryRoot, "api", "v1"),
		filepath.Join(repositoryRoot, "cmd", "cynapsacore-shared"),
		repositoryRoot,
	}

	var inputs []vocabularyInput
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat public surface %s: %v", path, err)
		}
		if info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatalf("read public surface %s: %v", path, err)
			}
			for _, entry := range entries {
				if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
					continue
				}
				inputs = append(inputs, vocabularyInput{path: filepath.Join(path, entry.Name()), goSource: true})
			}
		}
	}
	inputs = append(inputs,
		vocabularyInput{path: filepath.Join(repositoryRoot, "README.md")},
		vocabularyInput{path: filepath.Join(repositoryRoot, "SECURITY.md")},
		vocabularyInput{path: filepath.Join(repositoryRoot, "AZTM_SDK.md")},
		vocabularyInput{path: filepath.Join(repositoryRoot, "AZTM_SDK_BOUNDARY.md")},
		vocabularyInput{path: filepath.Join(repositoryRoot, "cmd", "cynapsacore-shared", "cynapsacore_v1.h")},
	)
	vectorFiles, err := filepath.Glob(filepath.Join(repositoryRoot, "conformance", "v1", "*.json"))
	if err != nil {
		t.Fatalf("list public conformance vectors: %v", err)
	}
	for _, path := range vectorFiles {
		inputs = append(inputs, vocabularyInput{path: path})
	}

	for _, input := range inputs {
		input := input
		t.Run(filepath.ToSlash(input.path), func(t *testing.T) {
			var values []string
			if input.goSource {
				values = publicGoVocabulary(t, input.path)
			} else {
				values = textVocabulary(t, input.path)
			}
			for _, value := range values {
				if forbidden, ok := forbiddenPublicVocabulary(value); ok {
					t.Errorf("%s contains forbidden SDK-visible vocabulary %q", input.path, forbidden)
				}
			}
		})
	}
}

func TestForbiddenPublicVocabularyIsTokenAware(t *testing.T) {
	tests := []struct {
		value     string
		forbidden bool
	}{
		{value: "ordinary return value", forbidden: false},
		{value: "service unavailable", forbidden: false},
		{value: "private implementation detail", forbidden: false},
		{value: "joined with separators: RTC_data-channel", forbidden: true},
		{value: "mixed case: XmPp", forbidden: true},
		{value: "compatibility characters: ＸＭＰＰ", forbidden: true},
		{value: "numbered fallback: rank-2", forbidden: true},
	}

	for _, test := range tests {
		_, forbidden := forbiddenPublicVocabulary(test.value)
		if forbidden != test.forbidden {
			t.Errorf("forbiddenPublicVocabulary(%q) = %v, want %v", test.value, forbidden, test.forbidden)
		}
	}
}

func TestBoundaryVocabularyDeclarationExclusionIsExact(t *testing.T) {
	const declaration = "Forbidden SDK-visible vocabulary includes:"
	path := filepath.Join(t.TempDir(), "AZTM_SDK_BOUNDARY.md")
	content := "before\n" + declaration + "\n```text\nXMPP\n```\n\n## Mandatory Architecture\nafter\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := textVocabulary(t, path)
	if got := strings.Join(lines, "\n"); got != "before\n## Mandatory Architecture\nafter" {
		t.Fatalf("scanned boundary text = %q", got)
	}
}

type vocabularyInput struct {
	path     string
	goSource bool
}

func publicGoVocabulary(t *testing.T, path string) []string {
	t.Helper()

	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var values []string
	for _, group := range parsed.Comments {
		values = append(values, group.Text())
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			if value.IsExported() {
				values = append(values, value.Name)
			}
		case *ast.BasicLit:
			if value.Kind == token.STRING {
				decoded, err := strconv.Unquote(value.Value)
				if err == nil {
					values = append(values, decoded)
				}
			}
		case *ast.Field:
			if value.Tag != nil {
				decoded, err := strconv.Unquote(value.Tag.Value)
				if err == nil {
					values = append(values, decoded)
				}
			}
		}
		return true
	})
	return values
}

func textVocabulary(t *testing.T, path string) []string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	}()

	var lines []string
	boundaryDeclaration := filepath.Base(path) == "AZTM_SDK_BOUNDARY.md"
	foundDeclaration := false
	omittingDeclaration := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if boundaryDeclaration && line == "Forbidden SDK-visible vocabulary includes:" {
			foundDeclaration = true
			omittingDeclaration = true
			continue
		}
		if omittingDeclaration {
			if line == "## Mandatory Architecture" {
				omittingDeclaration = false
				lines = append(lines, line)
			}
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if omittingDeclaration {
		t.Fatalf("forbidden-vocabulary declaration in %s has no closing section", path)
	}
	if boundaryDeclaration && !foundDeclaration {
		t.Fatalf("forbidden-vocabulary declaration in %s is missing", path)
	}
	return lines
}

func forbiddenPublicVocabulary(value string) (string, bool) {
	words := splitVocabulary(value)
	forbidden := map[string]struct{}{
		"ice":            {},
		"jabber":         {},
		"jid":            {},
		"jingle":         {},
		"rtcdatachannel": {},
		"stun":           {},
		"turn":           {},
		"webrtc":         {},
		"xep":            {},
		"xmpp":           {},
	}

	for start := range words {
		var joined string
		for end := start; end < len(words) && end < start+4; end++ {
			joined += words[end]
			if _, found := forbidden[joined]; found {
				return joined, true
			}
		}
		if start+1 < len(words) && words[start] == "rank" && (words[start+1] == "1" || words[start+1] == "2") {
			return fmt.Sprintf("rank %s", words[start+1]), true
		}
	}
	return "", false
}

func splitVocabulary(value string) []string {
	value = strings.Map(func(r rune) rune {
		switch {
		case r == '\u3000':
			return ' '
		case r >= '\uff01' && r <= '\uff5e':
			return unicode.ToLower(r - 0xfee0)
		case unicode.Is(unicode.Mn, r):
			return -1
		default:
			return unicode.ToLower(r)
		}
	}, value)
	return strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
