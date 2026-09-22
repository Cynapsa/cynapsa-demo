package v1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestPublicContractDoesNotImportPrivatePackages(t *testing.T) {
	t.Helper()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list api/v1 sources: %v", err)
	}

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", file, err)
			}
			if strings.Contains(path, "/internal/") || strings.HasSuffix(path, "/internal") {
				t.Errorf("public contract %s imports private package %q", file, path)
			}
		}
	}
}

func TestMaximumPayloadBytesIsFrozen(t *testing.T) {
	const want uint64 = 134_217_696
	if MaximumPayloadBytes != want {
		t.Fatalf("MaximumPayloadBytes = %d, want %d", MaximumPayloadBytes, want)
	}
}

func TestMeshScopeIsSessionAuthenticationContext(t *testing.T) {
	for _, test := range []struct {
		name      string
		typeOf    reflect.Type
		forbidden []string
	}{
		{name: "core config", typeOf: reflect.TypeOf(Config{}), forbidden: []string{"MeshEndpoint", "Username", "Password", "MeshID"}},
		{name: "address mapping", typeOf: reflect.TypeOf(AddressMapping{}), forbidden: []string{"MeshID", "MeshName"}},
		{name: "message send", typeOf: reflect.TypeOf(MessageSendCommand{}), forbidden: []string{"MeshID"}},
		{name: "message request", typeOf: reflect.TypeOf(MessageRequestCommand{}), forbidden: []string{"MeshID"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, field := range test.forbidden {
				if _, exists := test.typeOf.FieldByName(field); exists {
					t.Errorf("%s must not contain %s", test.name, field)
				}
			}
		})
	}
	for _, field := range []string{"MeshEndpoint", "Username", "Password", "MeshID"} {
		if _, exists := reflect.TypeOf(AuthInput{}).FieldByName(field); !exists {
			t.Errorf("AuthInput must contain %s", field)
		}
	}
}

func TestPublicContractHasNoCatchAllFields(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list api/v1 sources: %v", err)
	}

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok {
				return true
			}
			if containsCatchAllType(field.Type) {
				t.Errorf("public field in %s uses a catch-all map, interface, or any", file)
			}
			return true
		})
	}
}

func containsCatchAllType(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.MapType, *ast.InterfaceType:
		return true
	case *ast.Ident:
		return value.Name == "any"
	case *ast.ArrayType:
		return containsCatchAllType(value.Elt)
	case *ast.StarExpr:
		return containsCatchAllType(value.X)
	default:
		return false
	}
}
