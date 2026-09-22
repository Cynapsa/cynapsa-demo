package sdkboundary

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestClosedResultCatalogsRemainOneToOne prevents an admitted private result
// from lacking a public projection, and prevents a frozen public discriminator
// from lacking a private ResultValue variant.
func TestClosedResultCatalogsRemainOneToOne(t *testing.T) {
	expected := frozenResultCatalog()
	private := discoverMarkerReceivers(t, filepath.Join("..", "model"), "resultValue")
	public := discoverMarkerReceivers(t, filepath.Join("..", "..", "api", "v1"), "resultType")
	if len(private) != len(expected) || len(public) != len(expected) {
		t.Fatalf("closed result catalog sizes: private=%d public=%d expected=%d\nprivate=%v\npublic=%v", len(private), len(public), len(expected), private, public)
	}
	seenPublic := make(map[string]bool, len(expected))
	for privateName, publicName := range expected {
		if !private[privateName] {
			t.Errorf("private ResultValue is missing %s", privateName)
		}
		if !public[publicName] {
			t.Errorf("public Result is missing %s", publicName)
		}
		if seenPublic[publicName] {
			t.Errorf("public Result %s has multiple private mappings", publicName)
		}
		seenPublic[publicName] = true
	}
}

func frozenResultCatalog() map[string]string {
	return map[string]string{
		"EmptyResult":             "EmptyResult",
		"CapabilitiesResult":      "Capabilities",
		"StatusResult":            "Status",
		"ConfigResult":            "ConfigResult",
		"AuthResult":              "AuthResult",
		"AgentIDResult":           "AgentIDResult",
		"MeshListResult":          "MeshListResult",
		"AddressMappingsResult":   "AddressMappingsResult",
		"AddressResolution":       "AddressResolution",
		"SendResult":              "SendResult",
		"ResponseResult":          "ResponseResult",
		"EventResult":             "EventResult",
		"DeliveryQueueStatus":     "DeliveryQueueStatus",
		"PayloadHandleResult":     "PayloadHandleResult",
		"ConversationStatus":      "ConversationStatus",
		"ConversationListResult":  "ConversationListResult",
		"PolicyResult":            "PolicyResult",
		"PeerStatus":              "PeerStatus",
		"ConnectivityStatus":      "ConnectivityStatus",
		"DiagnosticSnapshot":      "DiagnosticSnapshot",
		"CoreInitResult":          "CoreInitResult",
		"CompletionChannelResult": "CompletionChannelResult",
		"EventSinkResult":         "EventSinkResult",
	}
}

func discoverMarkerReceivers(t *testing.T, directory, marker string) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(directory, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Name.Name != marker || function.Recv == nil || len(function.Recv.List) != 1 {
				continue
			}
			name := receiverTypeName(function.Recv.List[0].Type)
			if name == "" {
				t.Fatalf("unsupported %s receiver in %s", marker, file)
			}
			if result[name] {
				t.Fatalf("duplicate %s receiver %s", marker, name)
			}
			result[name] = true
		}
	}
	return result
}

func receiverTypeName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return receiverTypeName(value.X)
	default:
		return ""
	}
}
