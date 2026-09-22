package doccontract_test

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var aztmRootDocuments = []string{
	"AZTM_COMMAND_GATE.md",
	"AZTM_ENVELOPE_PAYLOAD_PATCHING.md",
	"AZTM_FUTURE_ISSUES.md",
	"AZTM_GO_CORE.md",
	"AZTM_NETWORKING.md",
	"AZTM_NETWORKING_SHORT.md",
	"AZTM_SDK.md",
	"AZTM_SDK_BOUNDARY.md",
}

var obsoleteAZTMTerm = regexp.MustCompile(`(?i)(ordered_delivery|delivery\.ordering_blocked|delivery\.unknown|(^|[^[:alnum:]_])(ordered|unordered|ordering|seq|sequence|sequencer|generation|reorder|reordering|gap|head)([^[:alnum:]_]|$))`)

func TestAZTMRootDocumentManifestAndVocabulary(t *testing.T) {
	root := repositoryRoot(t)
	manifestPath := filepath.Join(root, "AZTM_DOCS_MANIFEST.sha256")
	manifest, err := os.Open(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Close()

	var listed []string
	scanner := bufio.NewScanner(manifest)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || len(fields[0]) != sha256.Size*2 {
			t.Fatalf("invalid manifest line %q", scanner.Text())
		}
		data, readErr := os.ReadFile(filepath.Join(root, fields[1]))
		if readErr != nil {
			t.Fatal(readErr)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != fields[0] {
			t.Errorf("%s does not match the manifest", fields[1])
		}
		if match := obsoleteAZTMTerm.Find(data); match != nil {
			t.Errorf("%s contains obsolete public term %q", fields[1], match)
		}
		listed = append(listed, fields[1])
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	actual, err := filepath.Glob(filepath.Join(root, "AZTM_*.md"))
	if err != nil {
		t.Fatal(err)
	}
	for index := range actual {
		actual[index] = filepath.Base(actual[index])
	}
	sort.Strings(actual)
	if strings.Join(listed, "\n") != strings.Join(aztmRootDocuments, "\n") {
		t.Fatalf("manifest files = %v, want %v", listed, aztmRootDocuments)
	}
	if strings.Join(actual, "\n") != strings.Join(aztmRootDocuments, "\n") {
		t.Fatalf("root AZTM files = %v, want %v", actual, aztmRootDocuments)
	}
}
